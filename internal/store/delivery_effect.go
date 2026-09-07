package store

import (
	"fmt"

	"telesrv/internal/domain"
)

// DeliveryEffectKind identifies the durable fact a business transaction must
// commit. It is deliberately independent of TL types: RPC builds opaque bytes,
// while stores only validate and persist the resulting intent.
type DeliveryEffectKind uint8

const (
	DeliveryEffectInvalid DeliveryEffectKind = iota
	DeliveryEffectAbsolute
	DeliveryEffectAccountPTS
)

// DeliveryEffect is an immutable delivery intent produced from the aggregate's
// post-mutation snapshot and applied before that aggregate transaction commits.
type DeliveryEffect struct {
	Kind DeliveryEffectKind

	TargetUserID     int64
	ExcludeAuthKeyID [8]byte
	ExcludeSessionID int64

	Payload        []byte
	RecoveryPolicy OutboxRecoveryPolicy
	Event          domain.UpdateEvent
}

// DeliveryEffectsBuilder runs while the aggregate transaction is open. It may
// only project the supplied immutable snapshot; stores treat any builder error
// as a mutation error and roll back the whole transaction.
type DeliveryEffectsBuilder[T any] func(T) ([]DeliveryEffect, error)

func AbsoluteDeliveryEffect(item DeliveryOutboxEnqueue) DeliveryEffect {
	return DeliveryEffect{
		Kind:         DeliveryEffectAbsolute,
		TargetUserID: item.TargetUserID, ExcludeAuthKeyID: item.ExcludeAuthKeyID,
		ExcludeSessionID: item.ExcludeSessionID, Payload: append([]byte(nil), item.Payload...),
		RecoveryPolicy: item.RecoveryPolicy,
	}
}

func AccountPTSDeliveryEffect(userID int64, event domain.UpdateEvent, excludeAuthKeyID [8]byte, excludeSessionID int64) DeliveryEffect {
	return DeliveryEffect{
		Kind: DeliveryEffectAccountPTS, TargetUserID: userID, Event: event,
		ExcludeAuthKeyID: excludeAuthKeyID, ExcludeSessionID: excludeSessionID,
	}
}

func (e DeliveryEffect) Validate() error {
	if e.TargetUserID <= 0 {
		return fmt.Errorf("delivery effect target user id is required")
	}
	hasAuthKey := e.ExcludeAuthKeyID != ([8]byte{})
	hasSession := e.ExcludeSessionID != 0
	if hasAuthKey != hasSession {
		return fmt.Errorf("delivery effect exclusion requires both raw auth key and session id")
	}
	switch e.Kind {
	case DeliveryEffectAbsolute:
		if len(e.Payload) == 0 {
			return fmt.Errorf("absolute delivery effect payload is required")
		}
		if len(e.Payload) > MaxDeliveryBatchBytes {
			return fmt.Errorf("absolute delivery effect payload exceeds %d bytes", MaxDeliveryBatchBytes)
		}
		if e.RecoveryPolicy != OutboxRecoveryAbsoluteReload {
			return fmt.Errorf("absolute delivery effect requires absolute_reload recovery")
		}
	case DeliveryEffectAccountPTS:
		if e.Event.PtsCount <= 0 || e.Event.Pts != 0 {
			return fmt.Errorf("account PTS delivery effect requires unallocated positive pts_count")
		}
	default:
		return fmt.Errorf("unknown delivery effect kind %d", e.Kind)
	}
	return nil
}
