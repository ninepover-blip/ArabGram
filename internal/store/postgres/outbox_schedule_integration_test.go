package postgres

import (
	"context"
	"testing"
	"time"

	"telesrv/internal/store"
)

func TestDeliveryOutboxNextReadyUsesDurableLaneDeadlinePostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	user := createTestUser(t, ctx, NewUserStore(pool), "+1885"+randomSuffix(t)+"01", "DeliverySchedule", "")
	cleanupOutboxUser(t, pool, user.ID)
	outbox := NewDeliveryOutboxStore(pool)
	if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
		TargetUserID: user.ID, Payload: []byte{0x01}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
	}); err != nil {
		t.Fatal(err)
	}
	ready, ok, err := outbox.NextReadyAt(ctx, store.OutboxQueueAbsoluteDelivery, 0, nil)
	if err != nil || !ok || ready.Kind != store.OutboxReadyClaim || ready.Delay() > time.Second {
		t.Fatalf("ready deadline = %+v ok=%v err=%v", ready, ok, err)
	}
	windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-a/worker-1",
		LaneLimit: 1, WindowSize: 1, WindowByteLimit: 1024, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != 1 || len(windows[0].Items) != 1 {
		t.Fatal(err)
	}
	ready, ok, err = outbox.NextReadyAt(ctx, store.OutboxQueueAbsoluteDelivery, 0, nil)
	if err != nil || !ok || ready.Kind != store.OutboxReadyRecoverLease || ready.ReadyAt.After(windows[0].Items[0].EvidenceDeadline) ||
		!ready.ReadyAt.Before(windows[0].LeaseUntil) {
		t.Fatalf("leased evidence deadline = %+v ok=%v err=%v window=%+v", ready, ok, err, windows[0])
	}
	ref := windows[0].Items[0].Ref
	if results, err := outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{
		Ref: ref, SourceInstanceID: "egress-a", Targets: []store.OutboxAttemptTarget{},
	}}); err != nil || results[0].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind empty = %+v err=%v", results, err)
	}
	ready, ok, err = outbox.NextReadyAt(ctx, store.OutboxQueueAbsoluteDelivery, 0, nil)
	if err != nil || ok {
		t.Fatalf("finalizable deadline = %+v ok=%v err=%v, want no timer deadline", ready, ok, err)
	}
}
