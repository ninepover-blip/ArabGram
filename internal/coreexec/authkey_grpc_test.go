package coreexec

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"telesrv/internal/coreexec/coreexecpb"
	"telesrv/internal/store"
	"telesrv/internal/store/memory"
)

func TestGRPCRemoteRequiresAuthKeyAuthorityCapability(t *testing.T) {
	remote, cleanup := newBufGRPCRemoteWithServer(t, missingAuthKeyAuthorityInfoServer{}, nil, "secret", "secret")
	defer cleanup()

	if err := remote.Check(context.Background()); !errors.Is(err, ErrGRPCProtocolVersionMismatch) {
		t.Fatalf("Check err = %v, want ErrGRPCProtocolVersionMismatch", err)
	}
}

func TestCachedAuthKeyStoreCachesPositiveOnlyAndStripsMutableMetadata(t *testing.T) {
	ctx := context.Background()
	raw := &countingAuthKeyStore{AuthKeyStore: memory.NewAuthKeyStore()}
	cache := NewCachedAuthKeyStore(raw, AuthKeyCacheConfig{MaxEntries: 8, TTL: time.Minute})
	authKeyID := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	value := testCoreExecAuthKeyValue(0x41)
	if err := raw.Save(ctx, store.AuthKeyData{
		ID:                 authKeyID,
		Value:              value,
		ServerSalt:         99,
		CreatedAt:          123,
		Layer:              228,
		LayerObservationID: 55,
		DeviceModel:        "Desktop",
		Platform:           "windows",
		SystemVersion:      "11",
		APIID:              2040,
		AppVersion:         "5.0",
	}); err != nil {
		t.Fatal(err)
	}

	got, found, err := cache.Get(ctx, authKeyID)
	if err != nil || !found {
		t.Fatalf("first Get = found:%v err:%v", found, err)
	}
	if got.Value != value || got.ServerSalt != 99 || got.CreatedAt != 123 {
		t.Fatalf("hot auth key protocol fields = salt:%d created:%d value_match:%v", got.ServerSalt, got.CreatedAt, got.Value == value)
	}
	if got.Layer != 0 || got.LayerObservationID != 0 || got.DeviceModel != "" || got.Platform != "" || got.APIID != 0 {
		t.Fatalf("cache leaked mutable metadata: %+v", got)
	}
	if got := raw.gets(); got != 1 {
		t.Fatalf("remote Get calls after first hit = %d, want 1", got)
	}
	if _, found, err := cache.Get(ctx, authKeyID); err != nil || !found {
		t.Fatalf("second Get = found:%v err:%v", found, err)
	}
	if got := raw.gets(); got != 1 {
		t.Fatalf("remote Get calls after cached hit = %d, want 1", got)
	}

	missing := [8]byte{8, 7, 6, 5, 4, 3, 2, 1}
	for i := 0; i < 2; i++ {
		if _, found, err := cache.Get(ctx, missing); err != nil || found {
			t.Fatalf("missing Get #%d = found:%v err:%v", i+1, found, err)
		}
	}
	if got := raw.gets(); got != 3 {
		t.Fatalf("remote Get calls after missing retries = %d, want 3", got)
	}
}

func TestGRPCRemoteAuthKeyAuthorityRoundTripAndDeleteFencing(t *testing.T) {
	ctx := context.Background()
	keys := memory.NewAuthKeyStore()
	broker := &recordingAuthInvalidationBroker{}
	server := newCoreExecGRPCServerWithRuntime(nil, "core-a", keys, broker)
	remote, cleanup := newBufGRPCRemoteWithServer(t, server, nil, "secret", "secret")
	defer cleanup()
	remote.instanceID = "edge-a"

	authKeyID := [8]byte{0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8}
	value := testCoreExecAuthKeyValue(0x77)
	if err := remote.Save(ctx, store.AuthKeyData{ID: authKeyID, Value: value, ServerSalt: 44, CreatedAt: 55}); err != nil {
		t.Fatalf("remote Save: %v", err)
	}
	got, found, err := remote.Get(ctx, authKeyID)
	if err != nil || !found {
		t.Fatalf("remote Get = found:%v err:%v", found, err)
	}
	if got.Value != value || got.ServerSalt != 44 || got.CreatedAt != 55 {
		t.Fatalf("remote Get data = %+v value_match:%v", got, got.Value == value)
	}

	broker.publishErr = errors.New("redis down")
	if err := remote.Delete(store.WithAuthKeyDeleteOrigin(ctx, store.AuthKeyDeleteOrigin{ExceptSessionID: 77}), authKeyID); err == nil {
		t.Fatal("remote Delete succeeded despite invalidation publish failure")
	}
	if _, found, err := keys.Get(ctx, authKeyID); err != nil || !found {
		t.Fatalf("auth key deleted despite publish failure: found=%v err=%v", found, err)
	}

	broker.publishErr = nil
	if err := remote.Delete(store.WithAuthKeyDeleteOrigin(ctx, store.AuthKeyDeleteOrigin{ExceptSessionID: 77}), authKeyID); err != nil {
		t.Fatalf("remote Delete: %v", err)
	}
	if _, found, err := keys.Get(ctx, authKeyID); err != nil || found {
		t.Fatalf("auth key remains after delete: found=%v err=%v", found, err)
	}
	events := broker.events()
	if len(events) != 1 {
		t.Fatalf("published events = %d, want 1", len(events))
	}
	event := events[0]
	if event.Kind != store.AuthInvalidationKindProtocolAuthKeyDeleted || event.SourceID != "core-a" ||
		event.OriginSourceID != "edge-a" || event.OriginSessionID != 77 ||
		len(event.AuthKeyIDs) != 1 || event.AuthKeyIDs[0] != authKeyID {
		t.Fatalf("delete invalidation event = %+v", event)
	}
}

func TestCachedAuthKeyStoreProtocolInvalidationClosesExceptOriginSession(t *testing.T) {
	ctx := context.Background()
	raw := &countingAuthKeyStore{AuthKeyStore: memory.NewAuthKeyStore()}
	cache := NewCachedAuthKeyStore(raw, AuthKeyCacheConfig{MaxEntries: 8, TTL: time.Minute})
	authKeyID := [8]byte{9, 9, 9, 9, 9, 9, 9, 9}
	if err := cache.Save(ctx, store.AuthKeyData{ID: authKeyID, Value: testCoreExecAuthKeyValue(0x19)}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := cache.Get(ctx, authKeyID); err != nil || !found {
		t.Fatalf("seeded cache Get = found:%v err:%v", found, err)
	}
	before := raw.gets()
	closer := &captureRawAuthKeyCloser{}
	cache.handleAuthKeyInvalidation(ctx, store.AuthInvalidationEvent{
		Kind:            store.AuthInvalidationKindProtocolAuthKeyDeleted,
		SourceID:        "core-a",
		AuthKeyIDs:      [][8]byte{authKeyID},
		DateUnix:        time.Now().Unix(),
		OriginSourceID:  "edge-a",
		OriginSessionID: 77,
	}, "edge-a", closer, nil)
	if len(closer.calls) != 1 || closer.calls[0].authKeyID != authKeyID || closer.calls[0].exceptSessionID != 77 {
		t.Fatalf("closer calls = %+v", closer.calls)
	}
	if _, found, err := cache.Get(ctx, authKeyID); err != nil || !found {
		t.Fatalf("Get after invalidation = found:%v err:%v", found, err)
	}
	if got := raw.gets(); got != before+1 {
		t.Fatalf("remote Get calls after invalidation = %d, want %d", got, before+1)
	}
}

func TestCachedAuthKeyStoreSubscriberStopFailsClosed(t *testing.T) {
	ctx := context.Background()
	raw := &countingAuthKeyStore{AuthKeyStore: memory.NewAuthKeyStore()}
	cache := NewCachedAuthKeyStore(raw, AuthKeyCacheConfig{MaxEntries: 8, TTL: time.Minute})
	authKeyID := [8]byte{6, 6, 6, 6, 6, 6, 6, 6}
	if err := cache.Save(ctx, store.AuthKeyData{ID: authKeyID, Value: testCoreExecAuthKeyValue(0x66)}); err != nil {
		t.Fatal(err)
	}
	if _, found, err := cache.Get(ctx, authKeyID); err != nil || !found {
		t.Fatalf("seeded cache Get = found:%v err:%v", found, err)
	}
	before := raw.gets()
	closer := &captureRawAuthKeyCloser{active: [][8]byte{authKeyID}}
	cache.failClosedAfterInvalidationSubscriberStop(closer, nil)
	if len(closer.calls) != 1 || closer.calls[0].authKeyID != authKeyID || closer.calls[0].exceptSessionID != 0 {
		t.Fatalf("closer calls = %+v", closer.calls)
	}
	if _, found, err := cache.Get(ctx, authKeyID); err != nil || !found {
		t.Fatalf("Get after fail-closed flush = found:%v err:%v", found, err)
	}
	if got := raw.gets(); got != before+1 {
		t.Fatalf("remote Get calls after fail-closed flush = %d, want %d", got, before+1)
	}
}

type countingAuthKeyStore struct {
	store.AuthKeyStore

	mu       sync.Mutex
	getCalls int
}

func (s *countingAuthKeyStore) Get(ctx context.Context, id [8]byte) (store.AuthKeyData, bool, error) {
	s.mu.Lock()
	s.getCalls++
	s.mu.Unlock()
	return s.AuthKeyStore.Get(ctx, id)
}

func (s *countingAuthKeyStore) gets() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getCalls
}

type recordingAuthInvalidationBroker struct {
	mu         sync.Mutex
	publishErr error
	published  []store.AuthInvalidationEvent
}

func (b *recordingAuthInvalidationBroker) PublishAuthInvalidation(_ context.Context, event store.AuthInvalidationEvent) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.publishErr != nil {
		return b.publishErr
	}
	b.published = append(b.published, event)
	return nil
}

func (b *recordingAuthInvalidationBroker) SubscribeAuthInvalidations(context.Context, func(context.Context, store.AuthInvalidationEvent)) error {
	return errors.New("not implemented")
}

func (b *recordingAuthInvalidationBroker) events() []store.AuthInvalidationEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]store.AuthInvalidationEvent(nil), b.published...)
}

type rawAuthKeyCloseCall struct {
	authKeyID       [8]byte
	exceptSessionID int64
}

type captureRawAuthKeyCloser struct {
	active [][8]byte
	calls  []rawAuthKeyCloseCall
}

func (c *captureRawAuthKeyCloser) CloseSessionsForRawAuthKeyExcept(authKeyID [8]byte, exceptSessionID int64) int {
	c.calls = append(c.calls, rawAuthKeyCloseCall{authKeyID: authKeyID, exceptSessionID: exceptSessionID})
	return 1
}

func (c *captureRawAuthKeyCloser) ActiveRawAuthKeyIDs() [][8]byte {
	return append([][8]byte(nil), c.active...)
}

func testCoreExecAuthKeyValue(fill byte) [256]byte {
	var value [256]byte
	for i := range value {
		value[i] = fill
	}
	return value
}

type missingAuthKeyAuthorityInfoServer struct {
	coreexecpb.UnimplementedCoreExecServiceServer
}

func (missingAuthKeyAuthorityInfoServer) GetInfo(context.Context, *coreexecpb.CoreExecInfoRequest) (*coreexecpb.CoreExecInfoResponse, error) {
	return &coreexecpb.CoreExecInfoResponse{
		ProtocolVersion:             coreExecGRPCProtocolVersion,
		MinSupportedProtocolVersion: coreExecGRPCMinSupportedProtocolVersion,
		Capabilities:                []string{"tl-wire-passthrough", "dispatch-admitted"},
		Implementation:              "missing-auth-key-authority-test",
	}, nil
}
