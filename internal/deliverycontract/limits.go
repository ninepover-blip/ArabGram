// Package deliverycontract owns the hard resource limits shared by the
// durable store, Egress and the Edge delivery wire. Keeping these values below
// any transport- or schema-specific limit prevents a command from being
// accepted by one boundary and rejected after its target ledger is frozen.
package deliverycontract

import "time"

const (
	MaxBatchItems      = 128
	MaxBatchBytes      = 16 << 20
	MaxTargets         = 1024
	MaxInstanceIDBytes = 255
	MaxLeaseOwnerBytes = 255

	MaxNotAfterHorizon = 70 * time.Second
)
