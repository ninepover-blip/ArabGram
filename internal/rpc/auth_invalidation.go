package rpc

import (
	"context"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/store"
)

const authInvalidationSubscribeRetry = time.Second

func (r *Router) publishAuthInvalidation(ctx context.Context, authKeyIDs ...[8]byte) {
	if r == nil || r.deps.AuthInvalidations == nil {
		return
	}
	ids := compactAuthInvalidationIDs(authKeyIDs)
	if len(ids) == 0 {
		return
	}
	event := store.AuthInvalidationEvent{
		SourceID:   r.instanceID,
		Kind:       store.AuthInvalidationKindAuthorization,
		AuthKeyIDs: ids,
		DateUnix:   r.clock.Now().Unix(),
	}
	if err := r.deps.AuthInvalidations.PublishAuthInvalidation(ctx, event); err != nil && r.log != nil {
		r.log.Warn("publish auth invalidation",
			zap.Int("auth_keys", len(ids)),
			zap.Error(err),
		)
	}
}

func (r *Router) RunAuthInvalidationSubscriber(ctx context.Context) {
	if r == nil || r.deps.AuthInvalidations == nil {
		return
	}
	for {
		err := r.deps.AuthInvalidations.SubscribeAuthInvalidations(ctx, func(ctx context.Context, event store.AuthInvalidationEvent) {
			r.handleRemoteAuthInvalidation(ctx, event)
		})
		if ctx.Err() != nil {
			return
		}
		if err != nil && r.log != nil {
			r.log.Warn("auth invalidation subscriber stopped", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(authInvalidationSubscribeRetry):
		}
	}
}

func (r *Router) handleRemoteAuthInvalidation(_ context.Context, event store.AuthInvalidationEvent) {
	if r == nil || event.SourceID == "" {
		return
	}
	kind := event.Kind.Normalized()
	if kind == "" || (kind == store.AuthInvalidationKindAuthorization && event.SourceID == r.instanceID) {
		return
	}
	for _, authKeyID := range compactAuthInvalidationIDs(event.AuthKeyIDs) {
		r.invalidateAuthUserCache(authKeyID)
		for _, rawAuthKeyID := range r.invalidateTempAuthKeyCacheForPerm(authKeyID) {
			r.invalidateAuthUserCache(rawAuthKeyID)
		}
	}
}

func compactAuthInvalidationIDs(in [][8]byte) [][8]byte {
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
