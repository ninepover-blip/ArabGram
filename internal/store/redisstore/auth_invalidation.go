package redisstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	"telesrv/internal/store"
)

const (
	authInvalidationChannel         = "telesrv:auth:invalidation:v1"
	maxAuthInvalidationAuthKeyIDs   = 256
	maxAuthInvalidationPayloadBytes = 16 << 10
)

type AuthInvalidationBroker struct {
	c redis.UniversalClient
}

func NewAuthInvalidationBroker(c redis.UniversalClient) *AuthInvalidationBroker {
	return &AuthInvalidationBroker{c: c}
}

func (b *AuthInvalidationBroker) PublishAuthInvalidation(ctx context.Context, event store.AuthInvalidationEvent) error {
	if b == nil || b.c == nil {
		return errors.New("redis auth invalidation broker is not configured")
	}
	if !validAuthInvalidation(event) {
		return errors.New("invalid auth invalidation event")
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal auth invalidation: %w", err)
	}
	if len(raw) > maxAuthInvalidationPayloadBytes {
		return errors.New("auth invalidation event exceeds encoded size limit")
	}
	if err := b.c.Publish(ctx, authInvalidationChannel, raw).Err(); err != nil {
		return fmt.Errorf("redis publish auth invalidation: %w", err)
	}
	return nil
}

func (b *AuthInvalidationBroker) SubscribeAuthInvalidations(ctx context.Context, handle func(context.Context, store.AuthInvalidationEvent)) error {
	if b == nil || b.c == nil {
		return errors.New("redis auth invalidation broker is not configured")
	}
	if handle == nil {
		return errors.New("auth invalidation handler is nil")
	}
	pubsub := b.c.Subscribe(ctx, authInvalidationChannel)
	defer func() { _ = pubsub.Close() }()
	if _, err := pubsub.Receive(ctx); err != nil {
		return fmt.Errorf("redis subscribe auth invalidations: %w", err)
	}
	messages := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case item, ok := <-messages:
			if !ok {
				return nil
			}
			if len(item.Payload) > maxAuthInvalidationPayloadBytes {
				continue
			}
			var event store.AuthInvalidationEvent
			if err := json.Unmarshal([]byte(item.Payload), &event); err != nil || !validAuthInvalidation(event) {
				continue
			}
			handle(ctx, event)
		}
	}
}

func validAuthInvalidation(event store.AuthInvalidationEvent) bool {
	if event.SourceID == "" || event.DateUnix <= 0 || len(event.AuthKeyIDs) == 0 || len(event.AuthKeyIDs) > maxAuthInvalidationAuthKeyIDs {
		return false
	}
	if !event.Kind.Valid() {
		return false
	}
	seen := make(map[[8]byte]struct{}, len(event.AuthKeyIDs))
	for _, authKeyID := range event.AuthKeyIDs {
		if authKeyID == ([8]byte{}) {
			return false
		}
		if _, ok := seen[authKeyID]; ok {
			return false
		}
		seen[authKeyID] = struct{}{}
	}
	return true
}
