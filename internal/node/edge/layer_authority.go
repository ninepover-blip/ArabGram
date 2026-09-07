package edge

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"telesrv/internal/store"
	"telesrv/internal/store/redisstore"
)

type authKeyLayerIdentityPrimer interface {
	PrimeAuthKeyLayerIdentity(context.Context, [8]byte) error
}

// requiredAuthKeySessionLayerStore breaks the startup dependency cycle between
// the Edge codec shell and the CoreExec client. It is bound exactly once before
// the MTProto listener starts; every pre-bind call fails closed.
type requiredAuthKeySessionLayerStore struct {
	delegate atomic.Pointer[authKeySessionLayerStoreDelegate]
}

type authKeySessionLayerStoreDelegate struct {
	store.AuthKeySessionLayerStore
	identity store.AuthKeyLayerIdentityResolver
}

func (s *requiredAuthKeySessionLayerStore) Bind(value store.AuthKeySessionLayerStore) error {
	if s == nil || value == nil {
		return store.ErrAuthKeySessionLayerStoreRequired
	}
	identity, ok := value.(store.AuthKeyLayerIdentityResolver)
	if !ok {
		return fmt.Errorf("edge Layer authority requires identity resolver")
	}
	bound := &authKeySessionLayerStoreDelegate{
		AuthKeySessionLayerStore: value,
		identity:                 identity,
	}
	if !s.delegate.CompareAndSwap(nil, bound) {
		return fmt.Errorf("edge Layer authority already bound")
	}
	return nil
}

func (s *requiredAuthKeySessionLayerStore) target() (*authKeySessionLayerStoreDelegate, error) {
	if s == nil {
		return nil, store.ErrAuthKeySessionLayerStoreRequired
	}
	target := s.delegate.Load()
	if target == nil || target.AuthKeySessionLayerStore == nil || target.identity == nil {
		return nil, store.ErrAuthKeySessionLayerStoreRequired
	}
	return target, nil
}

func (s *requiredAuthKeySessionLayerStore) GetSessionLayer(ctx context.Context, raw [8]byte, sessionID int64) (store.AuthKeySessionLayer, bool, error) {
	target, err := s.target()
	if err != nil {
		return store.AuthKeySessionLayer{}, false, err
	}
	return target.GetSessionLayer(ctx, raw, sessionID)
}

func (s *requiredAuthKeySessionLayerStore) AdvanceSessionLayer(ctx context.Context, raw [8]byte, sessionID int64, layer int, msgID int64) (store.AuthKeySessionLayer, bool, error) {
	target, err := s.target()
	if err != nil {
		return store.AuthKeySessionLayer{}, false, err
	}
	return target.AdvanceSessionLayer(ctx, raw, sessionID, layer, msgID)
}

func (s *requiredAuthKeySessionLayerStore) GetAuthKeyLayerDefault(ctx context.Context, raw [8]byte) (store.AuthKeyLayerDefault, bool, error) {
	target, err := s.target()
	if err != nil {
		return store.AuthKeyLayerDefault{}, false, err
	}
	return target.GetAuthKeyLayerDefault(ctx, raw)
}

func (s *requiredAuthKeySessionLayerStore) DeleteSessionLayer(ctx context.Context, raw [8]byte, sessionID int64) (bool, error) {
	target, err := s.target()
	if err != nil {
		return false, err
	}
	return target.DeleteSessionLayer(ctx, raw, sessionID)
}

func (s *requiredAuthKeySessionLayerStore) DeleteExpiredSessionLayers(ctx context.Context, limit int) (int, error) {
	target, err := s.target()
	if err != nil {
		return 0, err
	}
	return target.DeleteExpiredSessionLayers(ctx, limit)
}

func (s *requiredAuthKeySessionLayerStore) ResolveCachedAuthKeyLayerIdentity(ctx context.Context, raw [8]byte) (store.AuthKeyLayerIdentity, bool, error) {
	target, err := s.target()
	if err != nil {
		return store.AuthKeyLayerIdentity{}, false, err
	}
	return target.identity.ResolveCachedAuthKeyLayerIdentity(ctx, raw)
}

type edgeAuthKeyLayerIdentitySource struct {
	authKeys store.AuthKeyStore
	primer   authKeyLayerIdentityPrimer
	redis    redis.UniversalClient
	now      func() time.Time
}

func newEdgeAuthKeyLayerIdentitySource(
	authKeys store.AuthKeyStore,
	primer authKeyLayerIdentityPrimer,
	redisClient redis.UniversalClient,
) (*edgeAuthKeyLayerIdentitySource, error) {
	if authKeys == nil || primer == nil || redisClient == nil {
		return nil, store.ErrAuthKeySessionLayerStoreRequired
	}
	return &edgeAuthKeyLayerIdentitySource{
		authKeys: authKeys,
		primer:   primer,
		redis:    redisClient,
		now:      time.Now,
	}, nil
}

func (s *edgeAuthKeyLayerIdentitySource) ResolveAuthKeyLayerIdentity(
	ctx context.Context,
	raw [8]byte,
) (store.AuthKeyLayerIdentity, bool, error) {
	if s == nil || s.authKeys == nil || s.primer == nil || s.redis == nil || raw == ([8]byte{}) {
		return store.AuthKeyLayerIdentity{}, false, store.ErrAuthKeySessionLayerStoreRequired
	}
	key, found, err := s.authKeys.Get(ctx, raw)
	if err != nil || !found {
		return store.AuthKeyLayerIdentity{}, found, err
	}
	switch {
	case key.ExpiresAt < 0:
		return store.AuthKeyLayerIdentity{}, false, store.ErrAuthKeyBindingInvalid
	case key.ExpiresAt == 0:
		return store.AuthKeyLayerIdentity{EffectiveAuthKeyID: raw}, true, nil
	case !s.now().Before(time.Unix(int64(key.ExpiresAt), 0)):
		return store.AuthKeyLayerIdentity{}, false, store.ErrAuthKeyNotFound
	}
	if err := s.primer.PrimeAuthKeyLayerIdentity(ctx, raw); err != nil {
		return store.AuthKeyLayerIdentity{}, false, err
	}
	identity, found, err := redisstore.ReadInstalledAuthKeyLayerIdentity(ctx, s.redis, raw)
	if err != nil {
		return store.AuthKeyLayerIdentity{}, false, err
	}
	if !found || identity.EffectiveAuthKeyID == ([8]byte{}) || identity.RawExpiresAt <= 0 {
		return store.AuthKeyLayerIdentity{}, false, store.ErrAuthKeyBindingInvalid
	}
	identity.RawExpiresAt = key.ExpiresAt
	return identity, true, nil
}

var (
	_ store.AuthKeySessionLayerStore     = (*requiredAuthKeySessionLayerStore)(nil)
	_ store.AuthKeyLayerIdentityResolver = (*requiredAuthKeySessionLayerStore)(nil)
	_ store.AuthKeyLayerIdentitySource   = (*edgeAuthKeyLayerIdentitySource)(nil)
)
