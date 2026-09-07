package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"telesrv/internal/store"
	"telesrv/internal/store/postgres/sqlcgen"
)

var errInvalidDeliveryOutboxExclusionPair = errors.New("delivery outbox exclusion requires both raw auth key and session id")

type DeliveryOutboxStore struct {
	durableOutboxState
}

type DeliveryOutboxOption func(*DeliveryOutboxStore)

func WithDeliveryLeaseTimeout(d time.Duration) DeliveryOutboxOption {
	return func(s *DeliveryOutboxStore) {
		if d > 0 {
			s.defaultLease = d
		}
	}
}

func NewDeliveryOutboxStore(db sqlcgen.DBTX, opts ...DeliveryOutboxOption) *DeliveryOutboxStore {
	s := &DeliveryOutboxStore{
		durableOutboxState: newDurableOutboxState(db, deliveryOutboxStateConfig, defaultOutboxLease),
	}
	for _, option := range opts {
		if option != nil {
			option(s)
		}
	}
	return s
}

func (s *DeliveryOutboxStore) Enqueue(ctx context.Context, item store.DeliveryOutboxEnqueue) (store.DeliveryOutboxItem, error) {
	if err := validateDeliveryOutboxEnqueue(item); err != nil {
		return store.DeliveryOutboxItem{}, err
	}
	var id int64
	err := s.db.QueryRow(ctx, `
INSERT INTO edge_delivery_outbox (
  target_user_id, payload, exclude_auth_key_id, exclude_session_id, recovery_policy
)
VALUES ($1, $2, $3, $4, 'absolute_reload')
RETURNING id`, item.TargetUserID, item.Payload,
		item.ExcludeAuthKeyID[:], item.ExcludeSessionID).Scan(&id)
	if err != nil {
		return store.DeliveryOutboxItem{}, fmt.Errorf("enqueue absolute delivery outbox: %w", err)
	}
	return store.DeliveryOutboxItem{
		ID: id, TargetUserID: item.TargetUserID,
		ExcludeAuthKeyID: item.ExcludeAuthKeyID, ExcludeSessionID: item.ExcludeSessionID,
		Payload: append([]byte(nil), item.Payload...), RecoveryPolicy: item.RecoveryPolicy,
	}, nil
}

func validateDeliveryOutboxEnqueue(item store.DeliveryOutboxEnqueue) error {
	if item.TargetUserID <= 0 {
		return fmt.Errorf("delivery outbox target user id is required")
	}
	if len(item.Payload) == 0 {
		return fmt.Errorf("delivery outbox payload is required")
	}
	if len(item.Payload) > store.MaxDeliveryBatchBytes {
		return fmt.Errorf("delivery outbox payload exceeds %d bytes", store.MaxDeliveryBatchBytes)
	}
	if item.RecoveryPolicy != store.OutboxRecoveryAbsoluteReload {
		return fmt.Errorf("delivery outbox recovery policy must be absolute_reload")
	}
	hasAuthKey := item.ExcludeAuthKeyID != ([8]byte{})
	hasSession := item.ExcludeSessionID != 0
	if hasAuthKey != hasSession {
		return errInvalidDeliveryOutboxExclusionPair
	}
	return nil
}
