package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/store"
	"telesrv/internal/store/postgres/sqlcgen"
)

// applyDeliveryEffectsTx persists every intent before the owning aggregate
// transaction commits. Callers must return its error so mutation and delivery
// can never be observed independently.
func applyDeliveryEffectsTx(ctx context.Context, tx pgx.Tx, effects []store.DeliveryEffect) ([]store.DeliveryEffect, error) {
	if len(effects) == 0 {
		return effects, nil
	}
	for i := range effects {
		if err := effects[i].Validate(); err != nil {
			return nil, fmt.Errorf("delivery effect %d: %w", i, err)
		}
	}
	qtx := sqlcgen.New(tx)
	events := NewUpdateEventStore(tx)
	outbox := NewDeliveryOutboxStore(tx)
	for i := range effects {
		effect := &effects[i]
		switch effect.Kind {
		case store.DeliveryEffectAbsolute:
			if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
				TargetUserID: effect.TargetUserID, ExcludeAuthKeyID: effect.ExcludeAuthKeyID,
				ExcludeSessionID: effect.ExcludeSessionID, Payload: effect.Payload,
				RecoveryPolicy: effect.RecoveryPolicy,
			}); err != nil {
				return nil, fmt.Errorf("apply absolute delivery effect %d: %w", i, err)
			}
		case store.DeliveryEffectAccountPTS:
			event, err := events.appendInTx(ctx, tx, qtx, effect.TargetUserID, effect.Event, true, effect.ExcludeAuthKeyID, effect.ExcludeSessionID, true)
			if err != nil {
				return nil, fmt.Errorf("apply account PTS delivery effect %d: %w", i, err)
			}
			effect.Event = event
		}
	}
	return effects, nil
}

// applyAbsoluteDeliveryEffectsTx persists a bounded multi-target absolute
// delivery set with one INSERT. Besides avoiding one round trip per Community
// requester/channel nudge, the statement-level lane marker trigger observes
// the complete immutable append set at once. The caller owns tx and must return
// any error so the aggregate mutation rolls back with the delivery rows.
func applyAbsoluteDeliveryEffectsTx(ctx context.Context, tx pgx.Tx, effects []store.DeliveryEffect) error {
	if len(effects) == 0 {
		return nil
	}
	targets := make([]int64, 0, len(effects))
	authKeys := make([][]byte, 0, len(effects))
	sessions := make([]int64, 0, len(effects))
	payloads := make([][]byte, 0, len(effects))
	for i := range effects {
		effect := effects[i]
		if err := effect.Validate(); err != nil {
			return fmt.Errorf("absolute delivery effect %d: %w", i, err)
		}
		if effect.Kind != store.DeliveryEffectAbsolute {
			return fmt.Errorf("absolute delivery effect %d has kind %d", i, effect.Kind)
		}
		targets = append(targets, effect.TargetUserID)
		authKeys = append(authKeys, append([]byte(nil), effect.ExcludeAuthKeyID[:]...))
		sessions = append(sessions, effect.ExcludeSessionID)
		payloads = append(payloads, append([]byte(nil), effect.Payload...))
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO edge_delivery_outbox (
  target_user_id, exclude_auth_key_id, exclude_session_id, payload, recovery_policy
)
SELECT input.target_user_id, input.exclude_auth_key_id,
       input.exclude_session_id, input.payload, 'absolute_reload'
FROM unnest($1::bigint[], $2::bytea[], $3::bigint[], $4::bytea[])
  AS input(target_user_id, exclude_auth_key_id, exclude_session_id, payload)`,
		targets, authKeys, sessions, payloads); err != nil {
		return fmt.Errorf("insert absolute delivery effects: %w", err)
	}
	return nil
}
