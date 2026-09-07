package redisstore

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

const (
	readModelInvalidationChannel = "telesrv:read-model-invalidated:v1"
	maxReadModelInvalidations    = 256
	readModelBusInitialBackoff   = 100 * time.Millisecond
	readModelBusMaxBackoff       = 5 * time.Second
)

type readModelInvalidationWire struct {
	Model       string `json:"model"`
	OwnerUserID int64  `json:"owner_user_id"`
	PeerType    string `json:"peer_type"`
	PeerID      int64  `json:"peer_id"`
	Version     int64  `json:"version"`
	Hash        int64  `json:"hash"`
}

type readModelInvalidationEnvelope struct {
	Items []readModelInvalidationWire `json:"items"`
}

// ReadModelInvalidationBus transports bounded exact-key cache invalidations.
// The durable version, claim lease, and relay retry live in PostgreSQL.
type ReadModelInvalidationBus struct {
	client *redis.Client
	log    *zap.Logger
}

func NewReadModelInvalidationBus(client *redis.Client, log *zap.Logger) *ReadModelInvalidationBus {
	if log == nil {
		log = zap.NewNop()
	}
	return &ReadModelInvalidationBus{client: client, log: log}
}

func (b *ReadModelInvalidationBus) PublishReadModelInvalidations(ctx context.Context, items []store.ReadModelInvalidation) error {
	if b == nil || b.client == nil {
		return fmt.Errorf("redis read-model invalidation bus is required")
	}
	if len(items) == 0 || len(items) > maxReadModelInvalidations {
		return fmt.Errorf("read-model invalidation batch must be in [1,%d]", maxReadModelInvalidations)
	}
	wire := make([]readModelInvalidationWire, len(items))
	businessOwners := make([]int64, 0)
	for i, item := range items {
		if err := validateReadModelInvalidation(item); err != nil {
			return fmt.Errorf("read-model invalidation %d: %w", i, err)
		}
		wire[i] = readModelInvalidationWire{
			Model: item.Key.Model, OwnerUserID: item.Key.OwnerUserID,
			PeerType: string(item.Key.PeerType), PeerID: item.Key.PeerID,
			Version: item.Version, Hash: item.Hash,
		}
		if item.Key.Model == "business_automation" && item.Key.OwnerUserID > 0 {
			businessOwners = append(businessOwners, item.Key.OwnerUserID)
		}
	}
	// Delete the shared L2 before Pub/Sub and before the durable relay ACK.
	// Therefore even a subscriber that was disconnected during Publish will
	// reload an exact value after its reconnect callback flushes L1.
	if len(businessOwners) > 0 {
		cache := NewBusinessAutomationGateCache(b.client, DefaultBusinessAutomationGateTTL)
		if err := cache.DeleteBusinessAutomationGate(ctx, businessOwners...); err != nil {
			return fmt.Errorf("invalidate Redis business automation gates: %w", err)
		}
	}
	raw, err := json.Marshal(readModelInvalidationEnvelope{Items: wire})
	if err != nil {
		return fmt.Errorf("encode read-model invalidations: %w", err)
	}
	if err := b.client.Publish(ctx, readModelInvalidationChannel, raw).Err(); err != nil {
		return fmt.Errorf("publish read-model invalidations: %w", err)
	}
	return nil
}

// Run subscribes until ctx is canceled. reconnect is called after every
// successful subscribe before messages are consumed, closing missed-Pub/Sub
// windows by flushing only the affected cache family.
func (b *ReadModelInvalidationBus) Run(
	ctx context.Context,
	reconnect func(),
	handle func([]store.ReadModelInvalidation),
) {
	if b == nil || b.client == nil || handle == nil {
		return
	}
	backoff := readModelBusInitialBackoff
	for ctx.Err() == nil {
		pubsub := b.client.Subscribe(ctx, readModelInvalidationChannel)
		stopClose := context.AfterFunc(ctx, func() { _ = pubsub.Close() })
		if _, err := pubsub.Receive(ctx); err != nil {
			stopClose()
			_ = pubsub.Close()
			if !waitReadModelBus(ctx, backoff) {
				return
			}
			backoff = nextReadModelBusBackoff(backoff)
			continue
		}
		if reconnect != nil {
			reconnect()
		}
		backoff = readModelBusInitialBackoff
		for ctx.Err() == nil {
			message, err := pubsub.ReceiveMessage(ctx)
			if err != nil {
				if ctx.Err() == nil {
					b.log.Warn("Redis read-model invalidation subscriber disconnected", zap.Error(err))
				}
				break
			}
			items, err := decodeReadModelInvalidations(message.Payload)
			if err != nil {
				b.log.Warn("reject Redis read-model invalidation payload", zap.Error(err))
				continue
			}
			handle(items)
		}
		stopClose()
		_ = pubsub.Close()
		if !waitReadModelBus(ctx, backoff) {
			return
		}
		backoff = nextReadModelBusBackoff(backoff)
	}
}

func decodeReadModelInvalidations(payload string) ([]store.ReadModelInvalidation, error) {
	var envelope readModelInvalidationEnvelope
	if err := json.Unmarshal([]byte(payload), &envelope); err != nil {
		return nil, err
	}
	if len(envelope.Items) == 0 || len(envelope.Items) > maxReadModelInvalidations {
		return nil, fmt.Errorf("item count must be in [1,%d]", maxReadModelInvalidations)
	}
	items := make([]store.ReadModelInvalidation, len(envelope.Items))
	for i, wire := range envelope.Items {
		item := store.ReadModelInvalidation{
			Key: store.ReadModelKey{
				Model: wire.Model, OwnerUserID: wire.OwnerUserID,
				PeerType: domain.PeerType(wire.PeerType), PeerID: wire.PeerID,
			},
			Version: wire.Version, Hash: wire.Hash,
		}
		if err := validateReadModelInvalidation(item); err != nil {
			return nil, fmt.Errorf("item %d: %w", i, err)
		}
		items[i] = item
	}
	return items, nil
}

func validateReadModelInvalidation(item store.ReadModelInvalidation) error {
	if strings.TrimSpace(item.Key.Model) == "" || item.Key.Model != strings.TrimSpace(item.Key.Model) || len(item.Key.Model) > 64 {
		return fmt.Errorf("invalid model")
	}
	if item.Key.OwnerUserID < 0 || item.Key.PeerID < 0 || item.Version <= 0 || item.Hash == 0 {
		return fmt.Errorf("invalid identity/version/hash")
	}
	switch item.Key.PeerType {
	case "", domain.PeerTypeUser, domain.PeerTypeChannel:
	default:
		return fmt.Errorf("invalid peer type %q", item.Key.PeerType)
	}
	return nil
}

func waitReadModelBus(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func nextReadModelBusBackoff(delay time.Duration) time.Duration {
	delay *= 2
	if delay > readModelBusMaxBackoff {
		return readModelBusMaxBackoff
	}
	return delay
}

var _ store.ReadModelInvalidationPublisher = (*ReadModelInvalidationBus)(nil)
