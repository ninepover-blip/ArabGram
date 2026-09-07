package store

import (
	"context"
	"errors"

	"telesrv/internal/deliverycontract"
)

var ErrDeliveryOutboxRequired = errors.New("absolute delivery outbox is required")

// MaxDeliveryBatchBytes is the hard payload boundary shared by durable
// absolute delivery persistence and the Edge delivery protocol. Window byte
// budgets may be smaller, but a legal head item is always claimable alone.
const (
	MaxDeliveryBatchBytes      = deliverycontract.MaxBatchBytes
	MaxDeliveryBatchItems      = deliverycontract.MaxBatchItems
	MaxDeliveryTargets         = deliverycontract.MaxTargets
	MaxDeliveryInstanceIDBytes = deliverycontract.MaxInstanceIDBytes
	MaxDeliveryLeaseOwnerBytes = deliverycontract.MaxLeaseOwnerBytes
)

// DeliveryOutboxItem is a durable non-PTS online delivery task. Payload is an
// already encoded TL updates object; the queue layer treats it as opaque bytes.
type DeliveryOutboxItem struct {
	ID               int64
	TargetUserID     int64
	ExcludeAuthKeyID [8]byte
	ExcludeSessionID int64
	Payload          []byte
	RecoveryPolicy   OutboxRecoveryPolicy
}

type DeliveryOutboxEnqueue struct {
	TargetUserID     int64
	ExcludeAuthKeyID [8]byte
	ExcludeSessionID int64
	Payload          []byte
	RecoveryPolicy   OutboxRecoveryPolicy
}

// DeliveryOutboxStore persists non-PTS delivery facts for independent Egress.
type DeliveryOutboxStore interface {
	Enqueue(ctx context.Context, item DeliveryOutboxEnqueue) (DeliveryOutboxItem, error)
	DurableOutboxStateStore
}
