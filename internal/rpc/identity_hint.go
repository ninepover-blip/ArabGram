package rpc

import (
	"context"
	"time"

	"telesrv/internal/rpcidentity"
	"telesrv/internal/store"
)

type layerRPCIdentityHintKey struct{}

const redisAuthKeyIdentitySingleflightPrefix = "redis-auth-identity:"

func (r *Router) WithLayerRPCIdentityHint(ctx context.Context, hint rpcidentity.LayerRPCIdentityHint) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, layerRPCIdentityHintKey{}, hint)
}

func layerRPCIdentityHintFromContext(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64) (rpcidentity.LayerRPCIdentityHint, bool) {
	if ctx == nil {
		return rpcidentity.LayerRPCIdentityHint{}, false
	}
	hint, ok := ctx.Value(layerRPCIdentityHintKey{}).(rpcidentity.LayerRPCIdentityHint)
	if !ok || hint.RawAuthKeyID != rawAuthKeyID || hint.SessionID != sessionID {
		return rpcidentity.LayerRPCIdentityHint{}, false
	}
	return hint, true
}

func (r *Router) permanentAuthKeyIDFromIdentityHint(ctx context.Context, rawAuthKeyID [8]byte, sessionID int64) ([8]byte, bool) {
	hint, ok := layerRPCIdentityHintFromContext(ctx, rawAuthKeyID, sessionID)
	if !ok ||
		!hint.BusinessAuthKeyResolved ||
		hint.BusinessAuthKeyID != rawAuthKeyID ||
		!hint.RawAuthKeyExpiresAtResolved ||
		hint.RawAuthKeyExpiresAt != 0 {
		return [8]byte{}, false
	}
	return rawAuthKeyID, true
}

func (r *Router) redisVerifiedTempAuthKeyIDFromIdentityHint(
	ctx context.Context,
	rawAuthKeyID [8]byte,
	sessionID int64,
) ([8]byte, bool, error) {
	if r == nil || r.cfg.TempKeyResolveCacheTTL <= 0 {
		return [8]byte{}, false, nil
	}
	hint, ok := layerRPCIdentityHintFromContext(ctx, rawAuthKeyID, sessionID)
	if !ok || !hint.BusinessAuthKeyResolved || !hint.RawAuthKeyExpiresAtResolved ||
		hint.BusinessAuthKeyID == rawAuthKeyID || hint.BusinessAuthKeyID == ([8]byte{}) ||
		hint.RawAuthKeyExpiresAt <= 0 {
		return [8]byte{}, false, nil
	}
	if perm, ok := r.tempKeyResolveCache.Get(rawAuthKeyID, hint.BusinessAuthKeyID, r.clock.Now()); ok {
		return perm, true, nil
	}
	v, err, _ := r.authUserSF.Do(redisAuthKeyIdentitySingleflightPrefix+string(rawAuthKeyID[:]), func() (any, error) {
		if perm, ok := r.tempKeyResolveCache.Get(rawAuthKeyID, hint.BusinessAuthKeyID, r.clock.Now()); ok {
			return perm, nil
		}
		resolver, ok := r.deps.AuthKeySessionLayers.(store.AuthKeyLayerIdentityResolver)
		if !ok {
			return [8]byte{}, store.ErrAuthKeySessionLayerStoreRequired
		}
		identity, found, err := resolver.ResolveCachedAuthKeyLayerIdentity(ctx, rawAuthKeyID)
		if err != nil {
			return [8]byte{}, err
		}
		if !found {
			return [8]byte{}, store.ErrAuthKeyNotFound
		}
		if identity.EffectiveAuthKeyID != hint.BusinessAuthKeyID || identity.RawExpiresAt <= 0 {
			return [8]byte{}, store.ErrAuthKeyBindingInvalid
		}
		now := r.clock.Now()
		expiresAt := time.Unix(int64(identity.RawExpiresAt), 0)
		if !now.Before(expiresAt) {
			return [8]byte{}, store.ErrAuthKeyNotFound
		}
		cacheUntil := now.Add(r.cfg.TempKeyResolveCacheTTL)
		if expiresAt.Before(cacheUntil) {
			cacheUntil = expiresAt
		}
		r.tempKeyResolveCache.Store(rawAuthKeyID, identity.EffectiveAuthKeyID, cacheUntil, now)
		return identity.EffectiveAuthKeyID, nil
	})
	if err != nil {
		return [8]byte{}, true, err
	}
	return v.([8]byte), true, nil
}

func (r *Router) userIDFromIdentityHint(ctx context.Context, rawAuthKeyID, authKeyID [8]byte, sessionID int64) (int64, bool) {
	hint, ok := layerRPCIdentityHintFromContext(ctx, rawAuthKeyID, sessionID)
	if !ok ||
		!hint.BusinessAuthKeyResolved ||
		hint.BusinessAuthKeyID != authKeyID ||
		!hint.UserIDResolved ||
		hint.UserID == 0 {
		return 0, false
	}
	cachedUserID, ok := r.positiveCachedAuthUser(authKeyID)
	if !ok || cachedUserID != hint.UserID {
		return 0, false
	}
	return hint.UserID, true
}
