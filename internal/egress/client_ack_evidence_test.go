package egress

import (
	"context"
	"strings"
	"testing"
	"time"

	"telesrv/internal/edgecontrol"
)

func TestClientAckObservationNeverFinalizesDelivery(t *testing.T) {
	ring := NewClientAckObservationRing(16).(*clientAckObservationRing)
	var batch edgecontrol.BatchID
	var command edgecontrol.CommandID
	batch[0], command[0] = 1, 2
	observation := edgecontrol.ClientAckObservation{
		Tracking: edgecontrol.DeliveryTracking{
			BatchID: batch, CommandID: command, SourceInstanceID: "egress-a",
			Ref: edgecontrol.DeliveryRef{
				Domain:   edgecontrol.OrderingDomain{Kind: edgecontrol.QueueAccountPTS, StreamID: 41},
				OutboxID: 51, TargetUserID: 41, PTS: 61, LeaseFence: 71, Attempt: 1,
			},
		},
		TargetInstanceID: "edge-a",
		AuthKeyID:        [8]byte{1},
		SessionID:        81,
		ServerMsgID:      91,
		ObservedAt:       time.Now().UTC(),
	}

	results := applyClientAckBatch(context.Background(), deliveryEvidenceStores{clientAcks: ring}, []edgecontrol.ClientAckObservation{observation})
	if len(results) != 1 || results[0].Outcome != edgecontrol.ClientAckObservationApplied {
		t.Fatalf("results = %+v, want one applied observation", results)
	}
	shard := &ring.shards[edgecontrol.OrderingDomainHash(observation.Tracking.Ref.Domain)&(clientAckObservationShards-1)]
	got := shard.slots[0].Load()
	if got == nil || got.value != observation {
		t.Fatalf("ring observation = %+v, want %+v", got, observation)
	}
	ring.Observe(observation)
	if stats := ring.Stats(); stats.Capacity != 16 || stats.Written != 2 || stats.Overwritten != 1 {
		t.Fatalf("ring stats = %+v, want capacity=16 written=2 overwritten=1", stats)
	}
}

func TestPhysicalReceiptRejectsInstanceIdentityBeyondWireBound(t *testing.T) {
	receipt := edgecontrol.PhysicalReceipt{
		BatchID: edgecontrol.BatchID{1}, CommandID: edgecontrol.CommandID{2},
		SourceInstanceID: "egress-a", TargetInstanceID: "edge-a",
		Ref: edgecontrol.DeliveryRef{
			Domain:   edgecontrol.OrderingDomain{Kind: edgecontrol.QueueAccountPTS, StreamID: 41},
			OutboxID: 51, TargetUserID: 41, PTS: 61, LeaseFence: 71, Attempt: 1,
		},
		Outcome: edgecontrol.PhysicalWritten, EligibleSessions: 1, WrittenSessions: 1,
		FirstServerMsgID: 91, ObservedAt: time.Now().UTC(),
	}
	if err := validatePhysicalReceipt(receipt); err != nil {
		t.Fatalf("valid physical receipt rejected: %v", err)
	}
	receipt.SourceInstanceID = strings.Repeat("s", edgecontrol.MaxDeliveryInstanceIDBytes+1)
	if err := validatePhysicalReceipt(receipt); err == nil {
		t.Fatal("oversized physical receipt source instance identity was accepted")
	}
	receipt.SourceInstanceID = "egress-a "
	if err := validatePhysicalReceipt(receipt); err == nil {
		t.Fatal("non-canonical physical receipt source instance identity was accepted")
	}
	receipt.SourceInstanceID = "egress-a"
	receipt.Outcome = edgecontrol.PhysicalIndeterminate
	receipt.WrittenSessions = 0
	if err := validatePhysicalReceipt(receipt); err == nil {
		t.Fatal("indeterminate physical receipt with a server message id was accepted")
	}
}
