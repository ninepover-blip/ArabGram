package memory

import (
	"context"
	"testing"
	"time"

	"telesrv/internal/store"
)

func TestDeliveryOutboxNextReadyUsesEvidenceDeadlineBeforeLaneLease(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	enqueueAbsoluteForTest(t, ctx, outbox, 515, []byte{0x01})
	ready, ok, err := outbox.NextReadyAt(ctx, store.OutboxQueueAbsoluteDelivery, 0, nil)
	if err != nil || !ok || ready.Kind != store.OutboxReadyClaim || ready.Delay() > time.Second {
		t.Fatalf("NextReadyAt ready = %+v ok=%v err=%v", ready, ok, err)
	}
	window := claimAbsoluteForTest(t, ctx, outbox, "egress-a/worker-1", 1, 1)
	if len(window) != 1 {
		t.Fatalf("claimed = %+v", window)
	}
	ready, ok, err = outbox.NextReadyAt(ctx, store.OutboxQueueAbsoluteDelivery, 0, nil)
	// Claim freezes CommandNotAfter at +10s and evidence at +1s skew. Retry
	// scheduling must wake on that database/store-clock recovery boundary, not
	// wait for the unrelated one-minute ownership lease.
	if err != nil || !ok || ready.Kind != store.OutboxReadyRecoverLease || ready.Delay() < 10*time.Second || ready.Delay() > 12*time.Second {
		t.Fatalf("NextReadyAt evidence deadline = %+v ok=%v err=%v", ready, ok, err)
	}
	ref := window[0].Items[0].Ref
	bindAuthoritativeEmptyForTest(t, ctx, outbox, ref)
	recordNoTargetsForTest(t, ctx, outbox, ref)
	ready, ok, err = outbox.NextReadyAt(ctx, store.OutboxQueueAbsoluteDelivery, 0, nil)
	if err != nil || ok {
		t.Fatalf("NextReadyAt finalizable = %+v ok=%v err=%v, want no timer deadline", ready, ok, err)
	}
}
