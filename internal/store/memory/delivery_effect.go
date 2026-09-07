package memory

import (
	"context"
	"fmt"
	"time"

	"telesrv/internal/store"
)

func applyDeliveryEffects(ctx context.Context, effects []store.DeliveryEffect, outbox *DeliveryOutboxStore, events *UpdateEventStore) ([]store.DeliveryEffect, error) {
	for i := range effects {
		if err := effects[i].Validate(); err != nil {
			return nil, fmt.Errorf("delivery effect %d: %w", i, err)
		}
	}
	for i := range effects {
		effect := &effects[i]
		switch effect.Kind {
		case store.DeliveryEffectAbsolute:
			if outbox == nil {
				return nil, store.ErrDeliveryOutboxRequired
			}
			if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
				TargetUserID: effect.TargetUserID, ExcludeAuthKeyID: effect.ExcludeAuthKeyID,
				ExcludeSessionID: effect.ExcludeSessionID, Payload: effect.Payload,
				RecoveryPolicy: effect.RecoveryPolicy,
			}); err != nil {
				return nil, err
			}
		case store.DeliveryEffectAccountPTS:
			if events == nil {
				return nil, store.ErrDeliveryOutboxRequired
			}
			event, err := events.AppendAllocatedWithDispatch(ctx, effect.TargetUserID, effect.Event, effect.ExcludeAuthKeyID, effect.ExcludeSessionID)
			if err != nil {
				return nil, err
			}
			effect.Event = event
		}
	}
	return effects, nil
}

// appendAbsoluteDeliveryEffectsLocked installs already validated absolute
// intents while the caller holds outbox.mu. It is the commit primitive used by
// memory aggregates that also hold their state mutex; no fallible work remains
// after the first item is installed.
func appendAbsoluteDeliveryEffectsLocked(outbox *DeliveryOutboxStore, effects []store.DeliveryEffect, now time.Time) error {
	if outbox == nil || len(effects) == 0 {
		return store.ErrDeliveryOutboxRequired
	}
	for i := range effects {
		if err := effects[i].Validate(); err != nil {
			return fmt.Errorf("delivery effect %d: %w", i, err)
		}
		if effects[i].Kind != store.DeliveryEffectAbsolute {
			return fmt.Errorf("delivery effect %d is not absolute", i)
		}
	}
	for _, effect := range effects {
		item := store.DeliveryOutboxItem{
			ID: outbox.nextID, TargetUserID: effect.TargetUserID,
			ExcludeAuthKeyID: effect.ExcludeAuthKeyID, ExcludeSessionID: effect.ExcludeSessionID,
			Payload: append([]byte(nil), effect.Payload...), RecoveryPolicy: effect.RecoveryPolicy,
		}
		outbox.nextID++
		outbox.items[item.ID] = cloneDeliveryOutboxItem(item)
		if _, exists := outbox.lanes[item.TargetUserID]; !exists {
			outbox.lanes[item.TargetUserID] = &memoryOutboxLane{
				streamID: item.TargetUserID, headItemID: item.ID, state: "ready", readyAt: now,
			}
		}
	}
	return nil
}
