package redisstore

import (
	"context"
	"crypto/rand"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"telesrv/internal/store"
)

type redisLayerIdentitySource struct {
	mu    sync.Mutex
	items map[[8]byte]store.AuthKeyLayerIdentity
	reads int
	fail  error
}

func (s *redisLayerIdentitySource) ResolveAuthKeyLayerIdentity(
	_ context.Context,
	raw [8]byte,
) (store.AuthKeyLayerIdentity, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	if s.fail != nil {
		return store.AuthKeyLayerIdentity{}, false, s.fail
	}
	value, found := s.items[raw]
	return value, found, nil
}

func TestAuthKeySessionLayerRedisCrossInstanceAndBindMerge(t *testing.T) {
	addr := os.Getenv("TELESRV_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TELESRV_TEST_REDIS_ADDR to run redis integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clientA, err := Open(ctx, addr, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer clientA.Close()
	clientB, err := Open(ctx, addr, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer clientB.Close()

	rawA, rawB, permanent := randomLayerAuthKeyID(t), randomLayerAuthKeyID(t), randomLayerAuthKeyID(t)
	source := &redisLayerIdentitySource{items: map[[8]byte]store.AuthKeyLayerIdentity{
		rawA:      {EffectiveAuthKeyID: rawA},
		rawB:      {EffectiveAuthKeyID: rawB, RawExpiresAt: int(time.Now().Add(time.Hour).Unix())},
		permanent: {EffectiveAuthKeyID: permanent},
	}}
	a, err := NewAuthKeySessionLayerStore(clientA, source)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewAuthKeySessionLayerStore(clientB, source)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = clientA.Del(context.Background(),
			authKeyLayerIdentityKey(rawA), authKeyLayerIdentityKey(rawB), authKeyLayerIdentityKey(permanent),
			authKeyLayerSessionKey(rawA, 11), authKeyLayerSessionKey(rawB, 22),
			authKeyLayerDefaultPrefix+authKeyLayerID(rawA),
			authKeyLayerDefaultPrefix+authKeyLayerID(rawB),
			authKeyLayerDefaultPrefix+authKeyLayerID(permanent),
		).Err()
	})

	firstMsgID := freshLayerMessageID()
	first, applied, err := a.AdvanceSessionLayer(ctx, rawA, 11, 225, firstMsgID)
	if err != nil || !applied || first.Layer != 225 || !first.SharedDefault {
		t.Fatalf("first advance = %+v applied=%v err=%v", first, applied, err)
	}
	newerMsgID := firstMsgID + 4
	newer, applied, err := b.AdvanceSessionLayer(ctx, rawA, 11, 227, newerMsgID)
	if err != nil || !applied || newer.Layer != 227 || newer.MessageID != newerMsgID {
		t.Fatalf("newer advance = %+v applied=%v err=%v", newer, applied, err)
	}
	older, applied, err := a.AdvanceSessionLayer(ctx, rawA, 11, 225, firstMsgID)
	if err != nil || applied || older.Layer != 227 || older.MessageID != newerMsgID {
		t.Fatalf("older advance = %+v applied=%v err=%v", older, applied, err)
	}
	if _, _, err := a.AdvanceSessionLayer(ctx, rawA, 11, 225, newerMsgID); !errors.Is(err, store.ErrAuthKeySessionLayerConflict) {
		t.Fatalf("equal-msg conflict err=%v", err)
	}
	resolved, found, err := b.GetSessionLayer(ctx, rawA, 11)
	if err != nil || !found || resolved.Layer != 227 || resolved.ObservationID != newer.ObservationID {
		t.Fatalf("cross-instance get = %+v found=%v err=%v", resolved, found, err)
	}
	defaultValue, found, err := b.GetAuthKeyLayerDefault(ctx, rawA)
	if err != nil || !found || defaultValue.Layer != 227 || defaultValue.ObservationID != newer.ObservationID {
		t.Fatalf("default = %+v found=%v err=%v", defaultValue, found, err)
	}
	identity, found, err := b.ResolveCachedAuthKeyLayerIdentity(ctx, rawA)
	if err != nil || !found || identity.EffectiveAuthKeyID != rawA || identity.RawExpiresAt != 0 {
		t.Fatalf("permanent Redis identity = %+v found=%v err=%v", identity, found, err)
	}

	permMsgID := newerMsgID + 4
	permValue, applied, err := a.AdvanceSessionLayer(ctx, permanent, 33, 228, permMsgID)
	if err != nil || !applied {
		t.Fatalf("permanent advance = %+v applied=%v err=%v", permValue, applied, err)
	}
	if _, _, err := a.AdvanceSessionLayer(ctx, rawB, 22, 225, permMsgID+4); err != nil {
		t.Fatalf("temporary advance: %v", err)
	}
	if err := a.BindAuthKeyLayerIdentity(ctx, rawB, permanent, source.items[rawB].RawExpiresAt); err != nil {
		t.Fatalf("bind identity: %v", err)
	}
	identity, found, err = b.ResolveCachedAuthKeyLayerIdentity(ctx, rawB)
	if err != nil || !found || identity.EffectiveAuthKeyID != permanent || int64(identity.RawExpiresAt) <= time.Now().Unix() {
		t.Fatalf("bound Redis identity = %+v found=%v err=%v", identity, found, err)
	}
	boundDefault, found, err := b.GetAuthKeyLayerDefault(ctx, rawB)
	if err != nil || !found || boundDefault.Layer != 225 {
		t.Fatalf("bound default = %+v found=%v err=%v, want latest observation Layer 225", boundDefault, found, err)
	}
	if deleted, err := b.DeleteSessionLayer(ctx, rawA, 11); err != nil || !deleted {
		t.Fatalf("delete session deleted=%v err=%v", deleted, err)
	}
	if _, found, err := a.GetSessionLayer(ctx, rawA, 11); err != nil || found {
		t.Fatalf("deleted session found=%v err=%v", found, err)
	}

	source.mu.Lock()
	reads := source.reads
	source.mu.Unlock()
	if reads != 3 {
		t.Fatalf("identity cold reads=%d, want exactly one per raw key", reads)
	}
}

func TestAuthKeySessionLayerRedisFailureNeverFallsBackToIdentitySource(t *testing.T) {
	addr := os.Getenv("TELESRV_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TELESRV_TEST_REDIS_ADDR to run redis integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Open(ctx, addr, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	raw := randomLayerAuthKeyID(t)
	source := &redisLayerIdentitySource{items: map[[8]byte]store.AuthKeyLayerIdentity{
		raw: {EffectiveAuthKeyID: raw},
	}}
	layers, err := NewAuthKeySessionLayerStore(client, source)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := layers.AdvanceSessionLayer(ctx, raw, 44, 227, freshLayerMessageID()); err != nil {
		t.Fatalf("seed: %v", err)
	}
	source.mu.Lock()
	readsBefore := source.reads
	source.fail = errors.New("identity fallback was invoked")
	source.mu.Unlock()
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := layers.AdvanceSessionLayer(ctx, raw, 44, 227, freshLayerMessageID()+4); err == nil || errors.Is(err, source.fail) {
		t.Fatalf("advance after Redis close err=%v, want Redis failure without identity fallback", err)
	}
	if _, _, err := layers.ResolveCachedAuthKeyLayerIdentity(ctx, raw); err == nil || errors.Is(err, source.fail) {
		t.Fatalf("identity resolve after Redis close err=%v, want Redis failure without PG fallback", err)
	}
	source.mu.Lock()
	readsAfter := source.reads
	source.mu.Unlock()
	if readsAfter != readsBefore {
		t.Fatalf("identity source reads after Redis failure=%d, before=%d", readsAfter, readsBefore)
	}
}

func freshLayerMessageID() int64 {
	return (time.Now().Unix() << 32) | 0x10000000
}

func randomLayerAuthKeyID(t *testing.T) [8]byte {
	t.Helper()
	var id [8]byte
	if _, err := rand.Read(id[:]); err != nil {
		t.Fatal(err)
	}
	if id == ([8]byte{}) {
		id[0] = 1
	}
	return id
}
