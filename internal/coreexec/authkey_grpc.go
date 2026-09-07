package coreexec

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"telesrv/internal/coreexec/coreexecpb"
	"telesrv/internal/store"
)

const (
	defaultAuthKeyCacheMaxEntries     = 262144
	defaultAuthKeyCacheTTL            = 30 * time.Minute
	authKeyInvalidationSubscribeRetry = time.Second
	authKeyValueBytes                 = 256
)

var (
	errCoreExecAuthKeyStoreUnavailable     = errors.New("coreexec grpc: auth key store unavailable")
	errCoreExecAuthInvalidationUnavailable = errors.New("coreexec grpc: auth invalidation broker unavailable")
)

type AuthKeyCacheConfig struct {
	MaxEntries int
	TTL        time.Duration
	Logger     *zap.Logger
}

type RawAuthKeySessionCloser interface {
	CloseSessionsForRawAuthKeyExcept(authKeyID [8]byte, exceptSessionID int64) int
}

type ActiveRawAuthKeyProvider interface {
	ActiveRawAuthKeyIDs() [][8]byte
}

type CachedAuthKeyStore struct {
	remote     store.AuthKeyStore
	maxEntries int
	ttl        time.Duration
	log        *zap.Logger
	now        func() time.Time

	mu      sync.Mutex
	entries map[[8]byte]*list.Element
	order   *list.List
	sf      singleflight.Group
}

type cachedAuthKeyEntry struct {
	id        [8]byte
	data      store.AuthKeyData
	expiresAt time.Time
}

type authKeyLoadResult struct {
	data  store.AuthKeyData
	found bool
}

var (
	_ store.AuthKeyStore = (*GRPCRemote)(nil)
	_ store.AuthKeyStore = (*CachedAuthKeyStore)(nil)
)

func NewCachedAuthKeyStore(remote store.AuthKeyStore, cfg AuthKeyCacheConfig) *CachedAuthKeyStore {
	maxEntries := cfg.MaxEntries
	if maxEntries <= 0 {
		maxEntries = defaultAuthKeyCacheMaxEntries
	}
	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = defaultAuthKeyCacheTTL
	}
	log := cfg.Logger
	if log == nil {
		log = zap.NewNop()
	}
	return &CachedAuthKeyStore{
		remote:     remote,
		maxEntries: maxEntries,
		ttl:        ttl,
		log:        log,
		now:        time.Now,
		entries:    make(map[[8]byte]*list.Element),
		order:      list.New(),
	}
}

func (s *CachedAuthKeyStore) Save(ctx context.Context, k store.AuthKeyData) error {
	if s == nil || s.remote == nil {
		return ErrRemoteRPCUnavailable
	}
	if err := s.remote.Save(ctx, k); err != nil {
		return err
	}
	s.store(authKeyHotCacheData(k))
	return nil
}

func (s *CachedAuthKeyStore) Get(ctx context.Context, id [8]byte) (store.AuthKeyData, bool, error) {
	if s == nil || s.remote == nil {
		return store.AuthKeyData{}, false, ErrRemoteRPCUnavailable
	}
	if data, ok := s.lookup(id); ok {
		return data, true, nil
	}
	loaded, err, _ := s.sf.Do(string(id[:]), func() (any, error) {
		if data, ok := s.lookup(id); ok {
			return authKeyLoadResult{data: data, found: true}, nil
		}
		data, found, err := s.remote.Get(ctx, id)
		if err != nil || !found {
			return authKeyLoadResult{found: found}, err
		}
		data = authKeyHotCacheData(data)
		s.store(data)
		return authKeyLoadResult{data: data, found: true}, nil
	})
	if err != nil {
		return store.AuthKeyData{}, false, err
	}
	res, ok := loaded.(authKeyLoadResult)
	if !ok {
		return store.AuthKeyData{}, false, fmt.Errorf("coreexec auth key cache: invalid load result %T", loaded)
	}
	return res.data, res.found, nil
}

// Revalidate deliberately bypasses the Edge hot cache at the activation
// boundary. The remote Core read is authoritative; a successful value then
// refreshes the cache used by subsequent encrypted frames.
func (s *CachedAuthKeyStore) Revalidate(ctx context.Context, id [8]byte) (store.AuthKeyData, bool, error) {
	if s == nil || s.remote == nil {
		return store.AuthKeyData{}, false, ErrRemoteRPCUnavailable
	}
	data, found, err := s.remote.Revalidate(ctx, id)
	if err != nil || !found {
		if !found {
			s.Invalidate(id)
		}
		return data, found, err
	}
	s.store(authKeyHotCacheData(data))
	return data, true, nil
}

func (s *CachedAuthKeyStore) LoadBindingKeys(ctx context.Context, tempID, permID [8]byte) (store.AuthKeyBindingKeys, error) {
	if s == nil || s.remote == nil {
		return store.AuthKeyBindingKeys{}, ErrRemoteRPCUnavailable
	}
	return s.remote.LoadBindingKeys(ctx, tempID, permID)
}

func (s *CachedAuthKeyStore) UpdateClientInfo(ctx context.Context, id [8]byte, info store.AuthKeyClientInfo) error {
	if s == nil || s.remote == nil {
		return ErrRemoteRPCUnavailable
	}
	return s.remote.UpdateClientInfo(ctx, id, info)
}

func (s *CachedAuthKeyStore) Delete(ctx context.Context, id [8]byte) error {
	if s == nil || s.remote == nil {
		return ErrRemoteRPCUnavailable
	}
	if err := s.remote.Delete(ctx, id); err != nil {
		return err
	}
	s.Invalidate(id)
	return nil
}

func (s *CachedAuthKeyStore) Invalidate(id [8]byte) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleteLocked(id)
}

func (s *CachedAuthKeyStore) Flush() {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = make(map[[8]byte]*list.Element)
	s.order = list.New()
}

func (s *CachedAuthKeyStore) RunAuthKeyInvalidationSubscriber(ctx context.Context, broker store.AuthInvalidationBroker, localSourceID string, closer RawAuthKeySessionCloser, logger *zap.Logger) {
	if s == nil || broker == nil {
		return
	}
	localSourceID = strings.TrimSpace(localSourceID)
	if logger == nil {
		logger = s.log
	}
	for {
		err := broker.SubscribeAuthInvalidations(ctx, func(ctx context.Context, event store.AuthInvalidationEvent) {
			s.handleAuthKeyInvalidation(ctx, event, localSourceID, closer, logger)
		})
		if ctx.Err() != nil {
			return
		}
		if err != nil && logger != nil {
			logger.Warn("auth key invalidation subscriber stopped", zap.Error(err))
		}
		s.failClosedAfterInvalidationSubscriberStop(closer, logger)
		select {
		case <-ctx.Done():
			return
		case <-time.After(authKeyInvalidationSubscribeRetry):
		}
	}
}

func (s *CachedAuthKeyStore) failClosedAfterInvalidationSubscriberStop(closer RawAuthKeySessionCloser, logger *zap.Logger) {
	s.Flush()
	provider, ok := closer.(ActiveRawAuthKeyProvider)
	if !ok {
		if logger != nil {
			logger.Warn("auth key invalidation subscriber fail-closed without active raw-key provider")
		}
		return
	}
	active := compactAuthKeyInvalidationIDs(provider.ActiveRawAuthKeyIDs())
	closed := 0
	for _, id := range active {
		if closer != nil {
			closed += closer.CloseSessionsForRawAuthKeyExcept(id, 0)
		}
	}
	if logger != nil {
		logger.Warn("auth key invalidation subscriber failed closed",
			zap.Int("active_raw_auth_keys", len(active)),
			zap.Int("closed_sessions", closed),
		)
	}
}

func (s *CachedAuthKeyStore) handleAuthKeyInvalidation(_ context.Context, event store.AuthInvalidationEvent, localSourceID string, closer RawAuthKeySessionCloser, logger *zap.Logger) {
	if event.Kind.Normalized() != store.AuthInvalidationKindProtocolAuthKeyDeleted {
		return
	}
	ids := compactAuthKeyInvalidationIDs(event.AuthKeyIDs)
	if len(ids) == 0 {
		return
	}
	exceptSessionID := int64(0)
	if localSourceID != "" && strings.TrimSpace(event.OriginSourceID) == localSourceID {
		exceptSessionID = event.OriginSessionID
	}
	for _, id := range ids {
		s.Invalidate(id)
		closed := 0
		if closer != nil {
			closed = closer.CloseSessionsForRawAuthKeyExcept(id, exceptSessionID)
		}
		if closed > 0 && logger != nil {
			logger.Info("closed sessions after protocol auth key invalidation",
				zap.String("auth_key_id", fmt.Sprintf("%x", id)),
				zap.Int("sessions", closed),
				zap.Int64("except_session_id", exceptSessionID),
			)
		}
	}
}

func (s *CachedAuthKeyStore) lookup(id [8]byte) (store.AuthKeyData, bool) {
	now := s.now()
	s.mu.Lock()
	defer s.mu.Unlock()
	elem := s.entries[id]
	if elem == nil {
		return store.AuthKeyData{}, false
	}
	entry := elem.Value.(*cachedAuthKeyEntry)
	if !entry.expiresAt.IsZero() && !entry.expiresAt.After(now) {
		s.removeElementLocked(elem)
		return store.AuthKeyData{}, false
	}
	s.order.MoveToFront(elem)
	return entry.data, true
}

func (s *CachedAuthKeyStore) store(data store.AuthKeyData) {
	if s.maxEntries <= 0 {
		return
	}
	now := s.now()
	entry := &cachedAuthKeyEntry{
		id:        data.ID,
		data:      authKeyHotCacheData(data),
		expiresAt: now.Add(s.ttl),
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if elem := s.entries[data.ID]; elem != nil {
		elem.Value = entry
		s.order.MoveToFront(elem)
		return
	}
	elem := s.order.PushFront(entry)
	s.entries[data.ID] = elem
	for len(s.entries) > s.maxEntries {
		s.removeElementLocked(s.order.Back())
	}
}

func (s *CachedAuthKeyStore) deleteLocked(id [8]byte) {
	if elem := s.entries[id]; elem != nil {
		s.removeElementLocked(elem)
	}
}

func (s *CachedAuthKeyStore) removeElementLocked(elem *list.Element) {
	if elem == nil {
		return
	}
	entry, _ := elem.Value.(*cachedAuthKeyEntry)
	if entry != nil {
		delete(s.entries, entry.id)
	}
	s.order.Remove(elem)
}

func (r *GRPCRemote) Save(ctx context.Context, k store.AuthKeyData) error {
	if r == nil || r.client == nil {
		return ErrRemoteRPCUnavailable
	}
	if !store.ValidNewAuthKeyProtocolExpiry(k.ExpiresAt) {
		return store.ErrInvalidAuthKeyProtocolExpiry
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	start := time.Now()
	res, err := r.client.SaveAuthKey(callCtx, authKeyDataToPB(k))
	if err != nil {
		callErr := grpcRemoteCallError("save_auth_key", err)
		observeCoreExecGRPCCall(r.metrics, "client", "save_auth_key", start, grpcRemoteCallOutcome(callErr))
		return callErr
	}
	callErr := errorFromResponse(res.GetError())
	observeCoreExecGRPCCall(r.metrics, "client", "save_auth_key", start, grpcRemoteCallOutcome(callErr))
	return callErr
}

func (r *GRPCRemote) Get(ctx context.Context, id [8]byte) (store.AuthKeyData, bool, error) {
	if r == nil || r.client == nil {
		return store.AuthKeyData{}, false, ErrRemoteRPCUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	start := time.Now()
	res, err := r.client.GetAuthKey(callCtx, &coreexecpb.AuthKeyRequest{
		RawAuthKeyId: append([]byte(nil), id[:]...),
	})
	if err != nil {
		callErr := grpcRemoteCallError("get_auth_key", err)
		observeCoreExecGRPCCall(r.metrics, "client", "get_auth_key", start, grpcRemoteCallOutcome(callErr))
		return store.AuthKeyData{}, false, callErr
	}
	if err := errorFromResponse(res.GetError()); err != nil {
		observeCoreExecGRPCCall(r.metrics, "client", "get_auth_key", start, grpcRemoteCallOutcome(err))
		return store.AuthKeyData{}, false, err
	}
	if !res.GetFound() {
		observeCoreExecGRPCCall(r.metrics, "client", "get_auth_key", start, "not_found")
		return store.AuthKeyData{}, false, nil
	}
	data, err := authKeyDataFromPB(res.GetAuthKey())
	if err != nil {
		observeCoreExecGRPCCall(r.metrics, "client", "get_auth_key", start, "invalid")
		return store.AuthKeyData{}, false, err
	}
	observeCoreExecGRPCCall(r.metrics, "client", "get_auth_key", start, "ok")
	return data, true, nil
}

func (r *GRPCRemote) Revalidate(ctx context.Context, id [8]byte) (store.AuthKeyData, bool, error) {
	// CoreExec's GetAuthKey is the only Edge-facing authoritative auth-key read.
	// Revalidation occurs once at activation and intentionally bypasses the
	// CachedAuthKeyStore above this remote.
	return r.Get(ctx, id)
}

func (r *GRPCRemote) LoadBindingKeys(context.Context, [8]byte, [8]byte) (store.AuthKeyBindingKeys, error) {
	// Temp-key proof validation is Core business logic and must never execute on
	// Edge through the auth-key transport facade.
	return store.AuthKeyBindingKeys{}, errCoreExecAuthKeyStoreUnavailable
}

func (r *GRPCRemote) UpdateClientInfo(ctx context.Context, id [8]byte, info store.AuthKeyClientInfo) error {
	if r == nil || r.client == nil {
		return ErrRemoteRPCUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	start := time.Now()
	res, err := r.client.UpdateAuthKeyClientInfo(callCtx, authKeyClientInfoToPB(id, info))
	if err != nil {
		callErr := grpcRemoteCallError("update_auth_key_client_info", err)
		observeCoreExecGRPCCall(r.metrics, "client", "update_auth_key_client_info", start, grpcRemoteCallOutcome(callErr))
		return callErr
	}
	callErr := errorFromResponse(res.GetError())
	observeCoreExecGRPCCall(r.metrics, "client", "update_auth_key_client_info", start, grpcRemoteCallOutcome(callErr))
	return callErr
}

func (r *GRPCRemote) Delete(ctx context.Context, id [8]byte) error {
	if r == nil || r.client == nil {
		return ErrRemoteRPCUnavailable
	}
	req := &coreexecpb.AuthKeyRequest{
		RawAuthKeyId: append([]byte(nil), id[:]...),
		SourceId:     strings.TrimSpace(r.instanceID),
	}
	if origin, ok := store.AuthKeyDeleteOriginFrom(ctx); ok {
		req.SessionId = origin.ExceptSessionID
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	start := time.Now()
	res, err := r.client.DeleteAuthKey(callCtx, req)
	if err != nil {
		callErr := grpcRemoteCallError("delete_auth_key", err)
		observeCoreExecGRPCCall(r.metrics, "client", "delete_auth_key", start, grpcRemoteCallOutcome(callErr))
		return callErr
	}
	callErr := errorFromResponse(res.GetError())
	observeCoreExecGRPCCall(r.metrics, "client", "delete_auth_key", start, grpcRemoteCallOutcome(callErr))
	return callErr
}

func (s *coreExecGRPCServer) SaveAuthKey(ctx context.Context, req *coreexecpb.AuthKeyRecord) (*coreexecpb.ErrorResponse, error) {
	if s == nil || s.authKeys == nil {
		return &coreexecpb.ErrorResponse{Error: errCoreExecAuthKeyStoreUnavailable.Error()}, nil
	}
	data, err := authKeyDataFromPB(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if !store.ValidNewAuthKeyProtocolExpiry(data.ExpiresAt) {
		return nil, status.Error(codes.InvalidArgument, store.ErrInvalidAuthKeyProtocolExpiry.Error())
	}
	if err := s.authKeys.Save(ctx, data); err != nil {
		return &coreexecpb.ErrorResponse{Error: err.Error()}, nil
	}
	return &coreexecpb.ErrorResponse{}, nil
}

func (s *coreExecGRPCServer) GetAuthKey(ctx context.Context, req *coreexecpb.AuthKeyRequest) (*coreexecpb.GetAuthKeyResponse, error) {
	if s == nil || s.authKeys == nil {
		return &coreexecpb.GetAuthKeyResponse{Error: errCoreExecAuthKeyStoreUnavailable.Error()}, nil
	}
	id, err := authKeyIDFromBytes(req.GetRawAuthKeyId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "bad raw_auth_key_id")
	}
	data, found, err := s.authKeys.Get(ctx, id)
	res := &coreexecpb.GetAuthKeyResponse{Found: found}
	if err != nil {
		res.Error = err.Error()
		return res, nil
	}
	if found {
		res.AuthKey = authKeyDataToPB(data)
	}
	return res, nil
}

func (s *coreExecGRPCServer) UpdateAuthKeyClientInfo(ctx context.Context, req *coreexecpb.AuthKeyClientInfoRequest) (*coreexecpb.ErrorResponse, error) {
	if s == nil || s.authKeys == nil {
		return &coreexecpb.ErrorResponse{Error: errCoreExecAuthKeyStoreUnavailable.Error()}, nil
	}
	id, info, err := authKeyClientInfoFromPB(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.authKeys.UpdateClientInfo(ctx, id, info); err != nil {
		return &coreexecpb.ErrorResponse{Error: err.Error()}, nil
	}
	return &coreexecpb.ErrorResponse{}, nil
}

func (s *coreExecGRPCServer) DeleteAuthKey(ctx context.Context, req *coreexecpb.AuthKeyRequest) (*coreexecpb.ErrorResponse, error) {
	if s == nil || s.authKeys == nil {
		return &coreexecpb.ErrorResponse{Error: errCoreExecAuthKeyStoreUnavailable.Error()}, nil
	}
	id, err := authKeyIDFromBytes(req.GetRawAuthKeyId())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "bad raw_auth_key_id")
	}
	if s.authInvalidations == nil {
		return &coreexecpb.ErrorResponse{Error: errCoreExecAuthInvalidationUnavailable.Error()}, nil
	}
	if err := s.authInvalidations.PublishAuthInvalidation(ctx, store.AuthInvalidationEvent{
		Kind:            store.AuthInvalidationKindProtocolAuthKeyDeleted,
		SourceID:        s.authInvalidationSourceID(),
		AuthKeyIDs:      [][8]byte{id},
		DateUnix:        time.Now().Unix(),
		OriginSourceID:  strings.TrimSpace(req.GetSourceId()),
		OriginSessionID: req.GetSessionId(),
	}); err != nil {
		return &coreexecpb.ErrorResponse{Error: err.Error()}, nil
	}
	if err := s.authKeys.Delete(ctx, id); err != nil {
		return &coreexecpb.ErrorResponse{Error: err.Error()}, nil
	}
	return &coreexecpb.ErrorResponse{}, nil
}

func (s *coreExecGRPCServer) authInvalidationSourceID() string {
	if s == nil {
		return "coreexec-grpc"
	}
	if sourceID := strings.TrimSpace(s.instanceID); sourceID != "" {
		return sourceID
	}
	return "coreexec-grpc"
}

func authKeyDataToPB(k store.AuthKeyData) *coreexecpb.AuthKeyRecord {
	value := make([]byte, len(k.Value))
	copy(value, k.Value[:])
	return &coreexecpb.AuthKeyRecord{
		AuthKeyId:          append([]byte(nil), k.ID[:]...),
		Value:              value,
		ServerSalt:         k.ServerSalt,
		CreatedAt:          k.CreatedAt,
		ExpiresAt:          int32(k.ExpiresAt),
		Layer:              int32(k.Layer),
		LayerObservationId: k.LayerObservationID,
		DeviceModel:        k.DeviceModel,
		Platform:           k.Platform,
		SystemVersion:      k.SystemVersion,
		ApiId:              int32(k.APIID),
		AppVersion:         k.AppVersion,
	}
}

func authKeyDataFromPB(in *coreexecpb.AuthKeyRecord) (store.AuthKeyData, error) {
	if in == nil {
		return store.AuthKeyData{}, errors.New("missing auth key record")
	}
	id, err := authKeyIDFromBytes(in.GetAuthKeyId())
	if err != nil {
		return store.AuthKeyData{}, fmt.Errorf("bad auth_key_id: %w", err)
	}
	value := in.GetValue()
	if len(value) != authKeyValueBytes {
		return store.AuthKeyData{}, fmt.Errorf("bad auth key value length %d", len(value))
	}
	layer := int(in.GetLayer())
	if layer < 0 || in.GetLayerObservationId() < 0 || in.GetApiId() < 0 || in.GetExpiresAt() < -1 {
		return store.AuthKeyData{}, errors.New("negative auth key metadata")
	}
	var out store.AuthKeyData
	out.ID = id
	copy(out.Value[:], value)
	out.ServerSalt = in.GetServerSalt()
	out.CreatedAt = in.GetCreatedAt()
	out.ExpiresAt = int(in.GetExpiresAt())
	out.Layer = layer
	out.LayerObservationID = in.GetLayerObservationId()
	out.DeviceModel = in.GetDeviceModel()
	out.Platform = in.GetPlatform()
	out.SystemVersion = in.GetSystemVersion()
	out.APIID = int(in.GetApiId())
	out.AppVersion = in.GetAppVersion()
	return out, nil
}

func authKeyClientInfoToPB(id [8]byte, info store.AuthKeyClientInfo) *coreexecpb.AuthKeyClientInfoRequest {
	return &coreexecpb.AuthKeyClientInfoRequest{
		AuthKeyId:     append([]byte(nil), id[:]...),
		Layer:         int32(info.Layer),
		DeviceModel:   info.DeviceModel,
		Platform:      info.Platform,
		SystemVersion: info.SystemVersion,
		ApiId:         int32(info.APIID),
		AppVersion:    info.AppVersion,
	}
}

func authKeyClientInfoFromPB(in *coreexecpb.AuthKeyClientInfoRequest) ([8]byte, store.AuthKeyClientInfo, error) {
	if in == nil {
		return [8]byte{}, store.AuthKeyClientInfo{}, errors.New("missing auth key client info")
	}
	id, err := authKeyIDFromBytes(in.GetAuthKeyId())
	if err != nil {
		return [8]byte{}, store.AuthKeyClientInfo{}, fmt.Errorf("bad auth_key_id: %w", err)
	}
	if in.GetLayer() < 0 || in.GetApiId() < 0 {
		return [8]byte{}, store.AuthKeyClientInfo{}, errors.New("negative auth key client metadata")
	}
	return id, store.AuthKeyClientInfo{
		Layer:         int(in.GetLayer()),
		DeviceModel:   in.GetDeviceModel(),
		Platform:      in.GetPlatform(),
		SystemVersion: in.GetSystemVersion(),
		APIID:         int(in.GetApiId()),
		AppVersion:    in.GetAppVersion(),
	}, nil
}

func authKeyHotCacheData(k store.AuthKeyData) store.AuthKeyData {
	k.Layer = 0
	k.LayerObservationID = 0
	k.DeviceModel = ""
	k.Platform = ""
	k.SystemVersion = ""
	k.APIID = 0
	k.AppVersion = ""
	return k
}

func compactAuthKeyInvalidationIDs(in [][8]byte) [][8]byte {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[[8]byte]struct{}, len(in))
	out := make([][8]byte, 0, len(in))
	for _, id := range in {
		if id == ([8]byte{}) {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
