package postgres

import (
	"context"
	"testing"
	"time"

	"telesrv/internal/store"
)

func TestDeliveryOutboxPhysicalEvidenceDuplicateMustBeExactPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	user := createTestUser(t, ctx, NewUserStore(pool), "+1887"+randomSuffix(t)+"98", "OutboxExactEvidence", "")
	cleanupOutboxUser(t, pool, user.ID)
	outbox := NewDeliveryOutboxStore(pool)
	if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
		TargetUserID: user.ID, Payload: []byte{1}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
	}); err != nil {
		t.Fatal(err)
	}
	windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-exact/q2/w0",
		LaneLimit: 1, WindowSize: 1, WindowByteLimit: 1024, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != 1 || len(windows[0].Items) != 1 {
		t.Fatalf("claim = %+v err=%v", windows, err)
	}
	ref := windows[0].Items[0].Ref
	target := store.OutboxAttemptTarget{
		TargetInstanceID: "edge-exact", TargetUserID: user.ID,
		BatchID: testOutboxID(71), CommandID: testOutboxID(72),
	}
	if got, err := outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{
		Ref: ref, SourceInstanceID: "egress-exact", Targets: []store.OutboxAttemptTarget{target},
	}}); err != nil || got[0].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind = %+v err=%v", got, err)
	}
	written := store.OutboxAttemptEvidence{
		Ref: ref, Kind: store.OutboxEvidenceEdgeWritten, SourceInstanceID: "egress-exact",
		TargetInstanceID: target.TargetInstanceID, TargetUserID: target.TargetUserID,
		BatchID: target.BatchID, CommandID: target.CommandID,
		EligibleSessions: 2, WrittenSessions: 2, ServerMsgID: 7301, ObservedAt: time.Now(),
	}
	conflicting := written
	conflicting.Kind = store.OutboxEvidenceEdgeNoEligible
	conflicting.EligibleSessions, conflicting.WrittenSessions, conflicting.ServerMsgID = 0, 0, 0
	if _, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{written, conflicting}); err == nil {
		t.Fatal("conflicting evidence in one batch was not rejected before SQL mutation")
	}
	if got, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{written}); err != nil || got[0].Outcome != store.OutboxEvidenceRecorded {
		t.Fatalf("record written = %+v err=%v", got, err)
	}
	if got, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{written}); err != nil || got[0].Outcome != store.OutboxEvidenceDuplicate {
		t.Fatalf("exact duplicate = %+v err=%v", got, err)
	}
	if got, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{conflicting}); err != nil || got[0].Outcome != store.OutboxEvidenceRejected {
		t.Fatalf("conflicting outcome = %+v err=%v", got, err)
	}
	differentMessage := written
	differentMessage.ServerMsgID++
	if got, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{differentMessage}); err != nil || got[0].Outcome != store.OutboxEvidenceRejected {
		t.Fatalf("conflicting server message = %+v err=%v", got, err)
	}
}

func TestDeliveryOutboxResolutionBatchRejectsConflictingDecisionBeforeMutationPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	user := createTestUser(t, ctx, NewUserStore(pool), "+1887"+randomSuffix(t)+"97", "OutboxExactResolution", "")
	cleanupOutboxUser(t, pool, user.ID)
	outbox := NewDeliveryOutboxStore(pool)
	if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
		TargetUserID: user.ID, Payload: []byte{2}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
	}); err != nil {
		t.Fatal(err)
	}
	windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-exact/q2/w0",
		LaneLimit: 1, WindowSize: 1, WindowByteLimit: 1024, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != 1 || len(windows[0].Items) != 1 {
		t.Fatalf("claim = %+v err=%v", windows, err)
	}
	ref := windows[0].Items[0].Ref
	target := store.OutboxAttemptTarget{
		TargetInstanceID: "edge-exact", TargetUserID: user.ID,
		BatchID: testOutboxID(73), CommandID: testOutboxID(74),
	}
	if got, err := outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{
		Ref: ref, SourceInstanceID: "egress-exact", Targets: []store.OutboxAttemptTarget{target},
	}}); err != nil || got[0].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind = %+v err=%v", got, err)
	}
	retry := store.OutboxTargetAttemptResolution{
		Ref: ref, SourceInstanceID: "egress-exact",
		TargetInstanceID: target.TargetInstanceID, TargetUserID: target.TargetUserID,
		BatchID: target.BatchID, CommandID: target.CommandID,
		Kind: store.OutboxResolutionRetry, RetryDelay: time.Millisecond, LastError: "retry",
	}
	abandon := retry
	abandon.Kind, abandon.RetryDelay, abandon.LastError = store.OutboxResolutionAbandoned, 0, "abandon"
	if _, err := outbox.ResolveTargetAttemptBatch(ctx, []store.OutboxTargetAttemptResolution{retry, abandon}); err == nil {
		t.Fatal("conflicting resolution batch was not rejected before SQL mutation")
	}
	if got, err := outbox.ResolveTargetAttemptBatch(ctx, []store.OutboxTargetAttemptResolution{retry}); err != nil || got[0].Outcome != store.OutboxResolutionRecorded {
		t.Fatalf("record retry after rejected batch = %+v err=%v", got, err)
	}
}
