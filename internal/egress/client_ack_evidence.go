package egress

import (
	"context"
	"sync/atomic"

	"telesrv/internal/edgecontrol"
)

const clientAckObservationShards = 16

// ClientAckObservationSink is deliberately volatile. Client ACK corroborates
// a physical receipt but never authorizes completion, so retaining it must not
// extend a PostgreSQL attempt or add a durable write to the delivery path.
type ClientAckObservationSink interface {
	Observe(edgecontrol.ClientAckObservation)
	Stats() ClientAckObservationStats
}

type ClientAckObservationStats struct {
	Capacity    uint64
	Written     uint64
	Overwritten uint64
}

type clientAckObservationSlot struct {
	value edgecontrol.ClientAckObservation
}

type clientAckObservationShard struct {
	next  atomic.Uint64
	slots []atomic.Pointer[clientAckObservationSlot]
}

type clientAckObservationRing struct {
	shards      [clientAckObservationShards]clientAckObservationShard
	capacity    uint64
	written     atomic.Uint64
	overwritten atomic.Uint64
}

// NewClientAckObservationRing creates a fixed-capacity, lock-free diagnostic
// ring. Capacity is rounded up across stable ordering-domain shards; writes
// overwrite the oldest shard-local observation instead of applying backpressure
// to msgs_ack or the physical delivery plane.
func NewClientAckObservationRing(capacity int) ClientAckObservationSink {
	if capacity < clientAckObservationShards {
		capacity = clientAckObservationShards
	}
	perShard := (capacity + clientAckObservationShards - 1) / clientAckObservationShards
	ring := &clientAckObservationRing{}
	for i := range ring.shards {
		ring.shards[i].slots = make([]atomic.Pointer[clientAckObservationSlot], perShard)
		ring.capacity += uint64(perShard)
	}
	return ring
}

func (r *clientAckObservationRing) Observe(observation edgecontrol.ClientAckObservation) {
	if r == nil || !observation.Valid() {
		return
	}
	shard := &r.shards[edgecontrol.OrderingDomainHash(observation.Tracking.Ref.Domain)&(clientAckObservationShards-1)]
	index := shard.next.Add(1) - 1
	copy := &clientAckObservationSlot{value: observation}
	if shard.slots[index%uint64(len(shard.slots))].Swap(copy) != nil {
		r.overwritten.Add(1)
	}
	r.written.Add(1)
}

func (r *clientAckObservationRing) Stats() ClientAckObservationStats {
	if r == nil {
		return ClientAckObservationStats{}
	}
	return ClientAckObservationStats{
		Capacity: r.capacity, Written: r.written.Load(), Overwritten: r.overwritten.Load(),
	}
}

func applyClientAckBatch(
	_ context.Context,
	stores deliveryEvidenceStores,
	observations []edgecontrol.ClientAckObservation,
) []edgecontrol.ClientAckObservationResult {
	results := make([]edgecontrol.ClientAckObservationResult, len(observations))
	for i, observation := range observations {
		if !observation.Valid() {
			results[i] = edgecontrol.ClientAckObservationResult{
				Outcome: edgecontrol.ClientAckObservationRejected,
				Detail:  edgecontrol.DetailInvalidIdentity,
			}
			continue
		}
		if stores.clientAcks == nil {
			results[i] = edgecontrol.ClientAckObservationResult{Outcome: edgecontrol.ClientAckObservationRetryable}
			continue
		}
		stores.clientAcks.Observe(observation)
		results[i] = edgecontrol.ClientAckObservationResult{Outcome: edgecontrol.ClientAckObservationApplied}
	}
	return results
}
