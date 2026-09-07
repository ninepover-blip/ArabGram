package memory

import (
	"context"
	"testing"
	"time"

	"telesrv/internal/store"
)

func TestDeliveryOutboxPhysicalEvidenceDuplicateMustBeExact(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	enqueueAbsoluteForTest(t, ctx, outbox, 9201, []byte{1})
	window := claimAbsoluteForTest(t, ctx, outbox, "egress-a/q2/w0", 1, 1)[0]
	ref := window.Items[0].Ref
	target := store.OutboxAttemptTarget{
		TargetInstanceID: "edge-a", TargetUserID: ref.StreamID,
		BatchID: bytes16(71), CommandID: bytes16(72),
	}
	if got, err := outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{
		Ref: ref, SourceInstanceID: "egress-a", Targets: []store.OutboxAttemptTarget{target},
	}}); err != nil || got[0].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind = %+v err=%v", got, err)
	}
	written := store.OutboxAttemptEvidence{
		Ref: ref, Kind: store.OutboxEvidenceEdgeWritten, SourceInstanceID: "egress-a",
		TargetInstanceID: target.TargetInstanceID, TargetUserID: target.TargetUserID,
		BatchID: target.BatchID, CommandID: target.CommandID,
		EligibleSessions: 2, WrittenSessions: 2, ServerMsgID: 7301, ObservedAt: time.Now(),
	}
	conflicting := written
	conflicting.Kind = store.OutboxEvidenceEdgeNoEligible
	conflicting.EligibleSessions, conflicting.WrittenSessions, conflicting.ServerMsgID = 0, 0, 0
	if _, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{written, conflicting}); err == nil {
		t.Fatal("conflicting evidence in one batch was not rejected before mutation")
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

func TestDeliveryOutboxResolutionBatchRejectsConflictingDecisionBeforeMutation(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	enqueueAbsoluteForTest(t, ctx, outbox, 9202, []byte{2})
	window := claimAbsoluteForTest(t, ctx, outbox, "egress-a/q2/w0", 1, 1)[0]
	ref := window.Items[0].Ref
	target := store.OutboxAttemptTarget{
		TargetInstanceID: "edge-a", TargetUserID: ref.StreamID,
		BatchID: bytes16(73), CommandID: bytes16(74),
	}
	if got, err := outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{
		Ref: ref, SourceInstanceID: "egress-a", Targets: []store.OutboxAttemptTarget{target},
	}}); err != nil || got[0].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind = %+v err=%v", got, err)
	}
	retry := store.OutboxTargetAttemptResolution{
		Ref: ref, SourceInstanceID: "egress-a",
		TargetInstanceID: target.TargetInstanceID, TargetUserID: target.TargetUserID,
		BatchID: target.BatchID, CommandID: target.CommandID,
		Kind: store.OutboxResolutionRetry, RetryDelay: time.Millisecond, LastError: "retry",
	}
	abandon := retry
	abandon.Kind, abandon.RetryDelay, abandon.LastError = store.OutboxResolutionAbandoned, 0, "abandon"
	if _, err := outbox.ResolveTargetAttemptBatch(ctx, []store.OutboxTargetAttemptResolution{retry, abandon}); err == nil {
		t.Fatal("conflicting resolution batch was not rejected before mutation")
	}
	if got, err := outbox.ResolveTargetAttemptBatch(ctx, []store.OutboxTargetAttemptResolution{retry}); err != nil || got[0].Outcome != store.OutboxResolutionRecorded {
		t.Fatalf("record retry after rejected batch = %+v err=%v", got, err)
	}
}
