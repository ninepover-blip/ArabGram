package memory

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"telesrv/internal/store"
)

func TestDeliveryOutboxRejectsIdentitiesOutsideDurableContract(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	enqueueAbsoluteForTest(t, ctx, outbox, 9011, []byte{1})
	if _, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery,
		Owner:     strings.Repeat("o", store.MaxDeliveryLeaseOwnerBytes+1),
	}); err == nil {
		t.Fatal("oversized durable lease owner was accepted")
	}
	window := claimAbsoluteForTest(t, ctx, outbox, "egress-a/q2/w0", 1, 1)[0]
	ref := window.Items[0].Ref
	target := store.OutboxAttemptTarget{
		TargetInstanceID: "edge-a", TargetUserID: ref.StreamID,
		BatchID: bytes16(1), CommandID: bytes16(2),
	}
	result, err := outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{
		Ref: ref, SourceInstanceID: strings.Repeat("s", store.MaxDeliveryInstanceIDBytes+1),
		Targets: []store.OutboxAttemptTarget{target},
	}})
	if err != nil || result[0].Outcome != store.OutboxBindTargetRejected {
		t.Fatalf("oversized source identity result=%+v err=%v", result, err)
	}
	target.TargetInstanceID = strings.Repeat("t", store.MaxDeliveryInstanceIDBytes+1)
	result, err = outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{
		Ref: ref, SourceInstanceID: "egress-a", Targets: []store.OutboxAttemptTarget{target},
	}})
	if err != nil || result[0].Outcome != store.OutboxBindTargetRejected {
		t.Fatalf("oversized target identity result=%+v err=%v", result, err)
	}
	result, err = outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{
		Ref: ref, SourceInstanceID: "egress-a",
		Targets: make([]store.OutboxAttemptTarget, store.MaxDeliveryTargets+1),
	}})
	if err != nil || result[0].Outcome != store.OutboxBindTargetRejected {
		t.Fatalf("oversized target ledger result=%+v err=%v", result, err)
	}
}

func TestDeliveryOutboxWindowGapAndEvidenceCrashRecovery(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	for i := 0; i < 3; i++ {
		enqueueAbsoluteForTest(t, ctx, outbox, 1001, []byte{byte(i + 1)})
	}
	windows := claimAbsoluteForTest(t, ctx, outbox, "egress-a/worker-1", 1, 3)
	if len(windows) != 1 || len(windows[0].Items) != 3 {
		t.Fatalf("ClaimWindows = %+v, want one 3-item window", windows)
	}
	refs := []store.OutboxAttemptRef{
		windows[0].Items[0].Ref, windows[0].Items[1].Ref, windows[0].Items[2].Ref,
	}
	target := store.OutboxAttemptTarget{
		TargetInstanceID: "edge-a", TargetUserID: refs[0].StreamID,
		BatchID: bytes16(11), CommandID: bytes16(12),
	}
	bound, err := outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{
		{Ref: refs[0], SourceInstanceID: "egress-a", Targets: []store.OutboxAttemptTarget{target}},
		{Ref: refs[1], SourceInstanceID: "egress-a", Targets: []store.OutboxAttemptTarget{}},
		{Ref: refs[2], SourceInstanceID: "egress-a", Targets: []store.OutboxAttemptTarget{target}},
	})
	if err != nil || len(bound) != 3 || bound[0].Outcome != store.OutboxBindTargetBound ||
		bound[1].Outcome != store.OutboxBindTargetBound || bound[2].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind gap targets = %+v err=%v", bound, err)
	}

	// A finalizer on any process may consume durable evidence, but cannot skip
	// the unresolved predecessor.
	batch, err := outbox.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{Ref: refs[1]}})
	if err != nil || len(batch.Results) != 1 || batch.Results[0].Outcome != store.OutboxFinalizeWaitingForPredecessor {
		t.Fatalf("FinalizeAttempts gap = %+v err=%v", batch, err)
	}
	if got := len(outbox.Snapshot()); got != 3 {
		t.Fatalf("items after gap finalize = %d, want 3", got)
	}

	// Evidence survives the delivery executor. A separate finalizer advances
	// the exact contiguous prefix without waiting for lease expiry.
	evidence, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{{
		Ref: refs[0], Kind: store.OutboxEvidenceEdgeWritten,
		SourceInstanceID: "egress-a", TargetInstanceID: target.TargetInstanceID,
		TargetUserID: target.TargetUserID, BatchID: target.BatchID, CommandID: target.CommandID,
		EligibleSessions: 1, WrittenSessions: 1, ServerMsgID: 1001, ObservedAt: time.Now(),
	}})
	if err != nil || len(evidence) != 1 || evidence[0].Outcome != store.OutboxEvidenceRecorded {
		t.Fatalf("record predecessor evidence = %+v err=%v", evidence, err)
	}
	batch, err = outbox.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{Ref: refs[0]}, {Ref: refs[1]}})
	if err != nil || len(batch.Results) != 2 ||
		batch.Results[0].Outcome != store.OutboxFinalizeApplied ||
		batch.Results[1].Outcome != store.OutboxFinalizeApplied {
		t.Fatalf("FinalizeAttempts prefix = %+v err=%v", batch, err)
	}
	remaining := outbox.Snapshot()
	if len(remaining) != 1 || remaining[0].ID != refs[2].ItemID {
		t.Fatalf("remaining = %+v, want third item", remaining)
	}
}

func TestDeliveryOutboxFrozenTargetsRequireEveryCommand(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	enqueueAbsoluteForTest(t, ctx, outbox, 2001, []byte{0x01})
	window := claimAbsoluteForTest(t, ctx, outbox, "egress-a/worker-1", 1, 1)[0]
	ref := window.Items[0].Ref
	targets := []store.OutboxAttemptTarget{
		{TargetInstanceID: "edge-a", TargetUserID: ref.StreamID, BatchID: bytes16(1), CommandID: bytes16(11)},
		{TargetInstanceID: "edge-b", TargetUserID: ref.StreamID, BatchID: bytes16(1), CommandID: bytes16(12)},
	}
	bound, err := outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{Ref: ref, SourceInstanceID: "egress-a", Targets: targets}})
	if err != nil || bound[0].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("BindAttemptTargets = %+v err=%v", bound, err)
	}
	wrongSource, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{{
		Ref: ref, Kind: store.OutboxEvidenceEdgeWritten, SourceInstanceID: "egress-b",
		TargetInstanceID: targets[0].TargetInstanceID, TargetUserID: targets[0].TargetUserID,
		BatchID: targets[0].BatchID, CommandID: targets[0].CommandID,
		EligibleSessions: 1, WrittenSessions: 1, ServerMsgID: 101, ObservedAt: time.Now(),
	}})
	if err != nil || wrongSource[0].Outcome != store.OutboxEvidenceFenced {
		t.Fatalf("wrong-source evidence = %+v err=%v", wrongSource, err)
	}
	recorded, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{{
		Ref: ref, Kind: store.OutboxEvidenceEdgeWritten, SourceInstanceID: "egress-a",
		EligibleSessions: 1, WrittenSessions: 1, ServerMsgID: 102,
		TargetInstanceID: targets[0].TargetInstanceID, TargetUserID: targets[0].TargetUserID, BatchID: targets[0].BatchID,
		CommandID: targets[0].CommandID, ObservedAt: time.Now(),
	}})
	if err != nil || recorded[0].Outcome != store.OutboxEvidenceRecorded {
		t.Fatalf("first evidence = %+v err=%v", recorded, err)
	}
	batch, err := outbox.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{Ref: ref}})
	if err != nil || batch.Results[0].Outcome != store.OutboxFinalizeWaitingForPredecessor {
		t.Fatalf("partial target finalize = %+v err=%v", batch, err)
	}
	recorded, err = outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{{
		Ref: ref, Kind: store.OutboxEvidenceEdgeNoEligible, SourceInstanceID: "egress-a",
		TargetInstanceID: targets[1].TargetInstanceID, TargetUserID: targets[1].TargetUserID, BatchID: targets[1].BatchID,
		CommandID: targets[1].CommandID, ObservedAt: time.Now(),
	}})
	if err != nil || recorded[0].Outcome != store.OutboxEvidenceRecorded {
		t.Fatalf("second evidence = %+v err=%v", recorded, err)
	}
	batch, err = outbox.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{Ref: ref}})
	if err != nil || batch.Results[0].Outcome != store.OutboxFinalizeApplied || len(outbox.Snapshot()) != 0 {
		t.Fatalf("complete target finalize = %+v err=%v snapshot=%+v", batch, err, outbox.Snapshot())
	}
}

func TestDeliveryOutboxResolutionRequiresExactTargetOrLiveOwner(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	enqueueAbsoluteForTest(t, ctx, outbox, 2101, []byte{0x01})
	enqueueAbsoluteForTest(t, ctx, outbox, 2101, []byte{0x02})
	window := claimAbsoluteForTest(t, ctx, outbox, "egress-owner", 1, 2)[0]
	targets := []store.OutboxAttemptTarget{
		{TargetInstanceID: "edge-a", TargetUserID: 2101, BatchID: bytes16(31), CommandID: bytes16(41)},
		{TargetInstanceID: "edge-b", TargetUserID: 2101, BatchID: bytes16(32), CommandID: bytes16(42)},
	}
	sets := make([]store.OutboxAttemptTargetSet, len(window.Items))
	for i, item := range window.Items {
		sets[i] = store.OutboxAttemptTargetSet{Ref: item.Ref, SourceInstanceID: "egress-source", Targets: []store.OutboxAttemptTarget{targets[i]}}
	}
	if bound, err := outbox.BindAttemptTargets(ctx, sets); err != nil ||
		bound[0].Outcome != store.OutboxBindTargetBound || bound[1].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind resolution targets = %+v err=%v", bound, err)
	}
	first := window.Items[0].Ref
	wrongSource, err := outbox.ResolveTargetAttemptBatch(ctx, []store.OutboxTargetAttemptResolution{{
		Ref: first, SourceInstanceID: "forged-source",
		TargetInstanceID: targets[0].TargetInstanceID, TargetUserID: targets[0].TargetUserID,
		BatchID: targets[0].BatchID, CommandID: targets[0].CommandID,
		Kind: store.OutboxResolutionRetry, RetryDelay: time.Millisecond,
	}})
	if err != nil || wrongSource[0].Outcome != store.OutboxResolutionFenced {
		t.Fatalf("wrong-source target resolution = %+v err=%v", wrongSource, err)
	}
	wrongCommand := targets[0].CommandID
	wrongCommand[0]++
	unknown, err := outbox.ResolveTargetAttemptBatch(ctx, []store.OutboxTargetAttemptResolution{{
		Ref: first, SourceInstanceID: "egress-source",
		TargetInstanceID: targets[0].TargetInstanceID, TargetUserID: targets[0].TargetUserID,
		BatchID: targets[0].BatchID, CommandID: wrongCommand,
		Kind: store.OutboxResolutionRetry, RetryDelay: time.Millisecond,
	}})
	if err != nil || unknown[0].Outcome != store.OutboxResolutionRejected {
		t.Fatalf("unknown-target resolution = %+v err=%v", unknown, err)
	}
	recorded, err := outbox.ResolveTargetAttemptBatch(ctx, []store.OutboxTargetAttemptResolution{{
		Ref: first, SourceInstanceID: "egress-source",
		TargetInstanceID: targets[0].TargetInstanceID, TargetUserID: targets[0].TargetUserID,
		BatchID: targets[0].BatchID, CommandID: targets[0].CommandID,
		Kind: store.OutboxResolutionRetry, RetryDelay: time.Millisecond,
	}})
	if err != nil || recorded[0].Outcome != store.OutboxResolutionRecorded {
		t.Fatalf("exact target resolution = %+v err=%v", recorded, err)
	}
	second := window.Items[1].Ref
	wrongOwner, err := outbox.ResolveOwnedAttemptBatch(ctx, []store.OutboxOwnedAttemptResolution{{
		Ref: second, Owner: "other-owner", Kind: store.OutboxResolutionAbandoned,
	}})
	if err != nil || wrongOwner[0].Outcome != store.OutboxResolutionFenced {
		t.Fatalf("wrong-owner resolution = %+v err=%v", wrongOwner, err)
	}
	owned, err := outbox.ResolveOwnedAttemptBatch(ctx, []store.OutboxOwnedAttemptResolution{{
		Ref: second, Owner: window.Owner, Kind: store.OutboxResolutionAbandoned,
	}})
	if err != nil || owned[0].Outcome != store.OutboxResolutionRecorded {
		t.Fatalf("live-owner resolution = %+v err=%v", owned, err)
	}
}

func TestDeliveryOutboxLiveAttemptFenceIsTerminal(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	enqueueAbsoluteForTest(t, ctx, outbox, 3001, []byte{0x01})
	first := claimAbsoluteForTest(t, ctx, outbox, "egress-a/worker-1", 1, 1)[0].Items[0].Ref

	outbox.mu.Lock()
	outbox.lanes[first.StreamID].leaseUntil = time.Now().Add(-time.Second)
	outbox.mu.Unlock()
	reclaimed := claimAbsoluteForTest(t, ctx, outbox, "egress-b/worker-2", 1, 1)[0].Items[0].Ref
	if reclaimed.LeaseFence == first.LeaseFence || reclaimed.Attempt == first.Attempt {
		t.Fatalf("reclaimed ref = %+v, first = %+v", reclaimed, first)
	}
	staleBind, err := outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{Ref: first, SourceInstanceID: "egress-a", Targets: nil}})
	if err != nil || staleBind[0].Outcome != store.OutboxBindTargetFenced {
		t.Fatalf("stale bind = %+v err=%v", staleBind, err)
	}

	bindAuthoritativeEmptyForTest(t, ctx, outbox, reclaimed)
	recordNoTargetsForTest(t, ctx, outbox, reclaimed)
	batch, err := outbox.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{Ref: reclaimed}})
	if err != nil || batch.Results[0].Outcome != store.OutboxFinalizeApplied {
		t.Fatalf("finalize reclaimed = %+v err=%v", batch, err)
	}
	batch, err = outbox.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{Ref: reclaimed}})
	if err != nil || batch.Results[0].Outcome != store.OutboxFinalizeFenced {
		t.Fatalf("completed live-attempt replay = %+v err=%v", batch, err)
	}
	enqueueAbsoluteForTest(t, ctx, outbox, 3001, []byte{0x02})
	newGeneration := claimAbsoluteForTest(t, ctx, outbox, "egress-c/worker-3", 1, 1)[0].Items[0].Ref
	if newGeneration.LeaseFence <= reclaimed.LeaseFence || newGeneration.ItemID == reclaimed.ItemID {
		t.Fatalf("new lane generation reused identity: old=%+v new=%+v", reclaimed, newGeneration)
	}
	wrong := reclaimed
	wrong.Attempt++
	batch, err = outbox.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{Ref: wrong}})
	if err != nil || batch.Results[0].Outcome != store.OutboxFinalizeFenced {
		t.Fatalf("wrong ref = %+v err=%v", batch, err)
	}
}

func TestDeliveryOutboxContinuousHandoffKeepsFixedExecutorLease(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	enqueueAbsoluteForTest(t, ctx, outbox, 4001, []byte{0x01})
	enqueueAbsoluteForTest(t, ctx, outbox, 4001, []byte{0x02})
	firstWindow := claimAbsoluteForTest(t, ctx, outbox, "egress-a/worker-7", 1, 1)[0]
	first := firstWindow.Items[0].Ref
	bindAuthoritativeEmptyForTest(t, ctx, outbox, first)
	recordNoTargetsForTest(t, ctx, outbox, first)
	batch, err := outbox.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{
		Ref: first, Owner: firstWindow.Owner, RetainLease: true,
		WindowSize: 1, WindowByteLimit: 1024, LeaseDuration: time.Minute,
	}})
	if err != nil || batch.Results[0].Outcome != store.OutboxFinalizeApplied ||
		len(batch.Next) != 1 || len(batch.Next[0].Items) != 1 {
		t.Fatalf("handoff = %+v err=%v", batch, err)
	}
	next := batch.Next[0]
	if next.Owner != firstWindow.Owner || next.LeaseFence != firstWindow.LeaseFence || next.Items[0].Ref.ItemID == first.ItemID {
		t.Fatalf("handoff window = %+v, first = %+v", next, firstWindow)
	}
}

func TestDeliveryOutboxRecoverBoundWindowPreservesPlanAndFencesTakeover(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	enqueueAbsoluteForTest(t, ctx, outbox, 5001, []byte{0x01})
	claimed := claimAbsoluteForTest(t, ctx, outbox, "egress-a/worker-1", 1, 1)[0]
	ref := claimed.Items[0].Ref
	target := store.OutboxAttemptTarget{
		TargetInstanceID: "edge-a", TargetUserID: ref.StreamID,
		BatchID: bytes16(21), CommandID: bytes16(22),
	}
	bound, err := outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{
		Ref: ref, SourceInstanceID: "egress-source-a", Targets: []store.OutboxAttemptTarget{target},
	}})
	if err != nil || bound[0].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("BindAttemptTargets = %+v err=%v", bound, err)
	}
	live, err := outbox.RecoverBoundWindows(ctx, store.OutboxRecoverBoundRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Mode: store.OutboxBoundRecoveryExactOwnerLive, Owner: claimed.Owner,
		LaneLimit: 1, LeaseDuration: time.Minute,
	})
	if err != nil || len(live) != 1 || live[0].Items[0].Ref != ref ||
		live[0].Items[0].SourceInstanceID != "egress-source-a" ||
		!live[0].Items[0].CommandNotAfter.Equal(claimed.Items[0].CommandNotAfter) ||
		!live[0].Items[0].EvidenceDeadline.Equal(claimed.Items[0].EvidenceDeadline) ||
		len(live[0].Items[0].Targets) != 1 || live[0].Items[0].Targets[0] != target {
		t.Fatalf("live RecoverBoundWindows = %+v err=%v", live, err)
	}

	outbox.mu.Lock()
	attempt := outbox.attempts[memoryAttemptKey{itemID: ref.ItemID, fence: ref.LeaseFence}]
	attempt.commandNotAfter = time.Now().Add(-2 * time.Second)
	attempt.evidenceDeadline = time.Now().Add(-time.Second)
	outbox.mu.Unlock()
	if got := claimAbsoluteForTest(t, ctx, outbox, "egress-b/worker-2", 1, 1); len(got) != 0 {
		t.Fatalf("ClaimWindows stole bound plan = %+v", got)
	}
	expired, err := outbox.ExpireEvidenceDeadlines(ctx, store.OutboxEvidenceExpiryRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, LaneLimit: 1, MaxAttempts: 5,
	})
	if err != nil || len(expired) != 1 || expired[0].Ref != ref {
		t.Fatalf("expired plan must resolve retry, got=%+v err=%v", expired, err)
	}
	requests, err := outbox.RecoverFinalizableAttempts(ctx, store.OutboxRecoverFinalizableRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, LaneLimit: 1, AttemptLimit: store.MaxDeliveryBatchItems,
	})
	if err != nil || len(requests) != 1 || requests[0].Ref != ref {
		t.Fatalf("expired finalizable = %+v err=%v", requests, err)
	}
	batch, err := outbox.FinalizeAttempts(ctx, requests)
	if err != nil || batch.Results[0].Outcome != store.OutboxFinalizeScheduledRetry {
		t.Fatalf("finalize expired retry = %+v err=%v", batch, err)
	}
	fresh := claimAbsoluteForTest(t, ctx, outbox, "egress-b/worker-2", 1, 1)[0].Items[0]
	if fresh.Ref.LeaseFence == ref.LeaseFence || fresh.Ref.Attempt == ref.Attempt ||
		fresh.SourceInstanceID != "" || len(fresh.Targets) != 0 {
		t.Fatalf("fresh claim reused expired plan identity = %+v old=%+v", fresh, ref)
	}
	stale, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{{
		Ref: ref, Kind: store.OutboxEvidenceEdgeWritten, SourceInstanceID: "egress-source-a",
		EligibleSessions: 1, WrittenSessions: 1, ServerMsgID: 103,
		TargetInstanceID: target.TargetInstanceID, TargetUserID: target.TargetUserID,
		BatchID: target.BatchID, CommandID: target.CommandID, ObservedAt: time.Now(),
	}})
	if err != nil || stale[0].Outcome != store.OutboxEvidenceFenced {
		t.Fatalf("stale evidence = %+v err=%v", stale, err)
	}
}

func TestDeliveryOutboxRecoverFinalizableAfterEvidenceCrash(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	enqueueAbsoluteForTest(t, ctx, outbox, 6001, []byte{0x01})
	ref := claimAbsoluteForTest(t, ctx, outbox, "egress-a/worker-1", 1, 1)[0].Items[0].Ref
	bindAuthoritativeEmptyForTest(t, ctx, outbox, ref)
	recordNoTargetsForTest(t, ctx, outbox, ref)
	requests, err := outbox.RecoverFinalizableAttempts(ctx, store.OutboxRecoverFinalizableRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, LaneLimit: 1, AttemptLimit: store.MaxDeliveryBatchItems,
	})
	if err != nil || len(requests) != 1 || requests[0].Ref != ref || requests[0].RetainLease {
		t.Fatalf("RecoverFinalizableAttempts = %+v err=%v", requests, err)
	}
	batch, err := outbox.FinalizeAttempts(ctx, requests)
	if err != nil || batch.Results[0].Outcome != store.OutboxFinalizeApplied || len(outbox.Snapshot()) != 0 {
		t.Fatalf("FinalizeAttempts recovered = %+v err=%v", batch, err)
	}
}

func TestDeliveryOutboxRecoverFinalizableBoundsAttemptsAcrossLanes(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	for lane := 0; lane < 2; lane++ {
		for item := 0; item < store.MaxDeliveryBatchItems; item++ {
			enqueueAbsoluteForTest(t, ctx, outbox, int64(6101+lane), []byte{byte(item + 1)})
		}
	}
	windows := claimAbsoluteForTest(t, ctx, outbox, "egress-recovery-limit", 2, store.MaxDeliveryBatchItems)
	if len(windows) != 2 {
		t.Fatalf("claimed windows = %d, want 2", len(windows))
	}
	for _, window := range windows {
		if len(window.Items) != store.MaxDeliveryBatchItems {
			t.Fatalf("claimed lane %d items = %d, want %d", window.StreamID, len(window.Items), store.MaxDeliveryBatchItems)
		}
		sets := make([]store.OutboxAttemptTargetSet, len(window.Items))
		evidence := make([]store.OutboxAttemptEvidence, len(window.Items))
		for i, item := range window.Items {
			sets[i] = store.OutboxAttemptTargetSet{Ref: item.Ref, SourceInstanceID: "egress-recovery-limit", Targets: []store.OutboxAttemptTarget{}}
			evidence[i] = store.OutboxAttemptEvidence{
				Ref: item.Ref, Kind: store.OutboxEvidenceAuthoritativeNoTargets,
				SourceInstanceID: "egress-recovery-limit", ObservedAt: time.Now(),
			}
		}
		if bound, err := outbox.BindAttemptTargets(ctx, sets); err != nil || len(bound) != len(sets) {
			t.Fatalf("bind lane %d = %d err=%v", window.StreamID, len(bound), err)
		}
		if recorded, err := outbox.RecordAttemptEvidenceBatch(ctx, evidence); err != nil || len(recorded) != len(evidence) {
			t.Fatalf("record lane %d = %d err=%v", window.StreamID, len(recorded), err)
		}
	}

	request := store.OutboxRecoverFinalizableRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, LaneLimit: 2, AttemptLimit: store.MaxDeliveryBatchItems,
	}
	for batchIndex := 0; batchIndex < 2; batchIndex++ {
		requests, err := outbox.RecoverFinalizableAttempts(ctx, request)
		if err != nil || len(requests) != store.MaxDeliveryBatchItems {
			t.Fatalf("recover batch %d = %d err=%v, want %d", batchIndex, len(requests), err, store.MaxDeliveryBatchItems)
		}
		finalized, err := outbox.FinalizeAttempts(ctx, requests)
		if err != nil || len(finalized.Results) != len(requests) {
			t.Fatalf("finalize batch %d = %d err=%v", batchIndex, len(finalized.Results), err)
		}
	}
	requests, err := outbox.RecoverFinalizableAttempts(ctx, request)
	if err != nil || len(requests) != 0 || len(outbox.Snapshot()) != 0 {
		t.Fatalf("terminal recovery = %d remaining=%d err=%v", len(requests), len(outbox.Snapshot()), err)
	}
}

func TestDeliveryOutboxRepeatedCrashSupersedesUnboundAttempts(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	enqueueAbsoluteForTest(t, ctx, outbox, 7001, []byte{0x01})
	first := claimAbsoluteForTest(t, ctx, outbox, "egress/0", 1, 1)[0].Items[0].Ref
	latest := first
	for i := 1; i <= 100; i++ {
		outbox.mu.Lock()
		outbox.lanes[latest.StreamID].leaseUntil = time.Now().Add(-time.Second)
		outbox.mu.Unlock()
		latest = claimAbsoluteForTest(t, ctx, outbox, fmt.Sprintf("egress/%d", i), 1, 1)[0].Items[0].Ref
	}
	outbox.mu.Lock()
	liveAttempts := len(outbox.attempts)
	outbox.mu.Unlock()
	if liveAttempts != 1 || latest.Attempt != 101 || latest.LeaseFence != 101 {
		t.Fatalf("live attempts/latest = %d/%+v want 1 and attempt/fence 101", liveAttempts, latest)
	}
	if batch, err := outbox.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{Ref: first}}); err != nil ||
		batch.Results[0].Outcome != store.OutboxFinalizeFenced {
		t.Fatalf("superseded live attempt = %+v err=%v", batch, err)
	}
}

func TestDeliveryOutboxExpiredHeadRetryDeletesConfirmedSuffixAttempt(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	enqueueAbsoluteForTest(t, ctx, outbox, 7101, []byte{0x01})
	enqueueAbsoluteForTest(t, ctx, outbox, 7101, []byte{0x02})
	window := claimAbsoluteForTest(t, ctx, outbox, "egress-a", 1, 2)[0]
	refs := []store.OutboxAttemptRef{window.Items[0].Ref, window.Items[1].Ref}
	targets := []store.OutboxAttemptTarget{
		{TargetInstanceID: "edge-a", TargetUserID: 7101, BatchID: bytes16(10), CommandID: bytes16(11)},
		{TargetInstanceID: "edge-a", TargetUserID: 7101, BatchID: bytes16(10), CommandID: bytes16(12)},
	}
	sets := make([]store.OutboxAttemptTargetSet, len(refs))
	for i := range refs {
		sets[i] = store.OutboxAttemptTargetSet{Ref: refs[i], SourceInstanceID: "egress-a", Targets: []store.OutboxAttemptTarget{targets[i]}}
	}
	bound, err := outbox.BindAttemptTargets(ctx, sets)
	if err != nil || bound[0].Outcome != store.OutboxBindTargetBound || bound[1].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind window = %+v err=%v", bound, err)
	}
	receipt, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{{
		Ref: refs[1], Kind: store.OutboxEvidenceEdgeWritten, SourceInstanceID: "egress-a",
		TargetInstanceID: targets[1].TargetInstanceID, TargetUserID: targets[1].TargetUserID,
		BatchID: targets[1].BatchID, CommandID: targets[1].CommandID,
		EligibleSessions: 2, WrittenSessions: 2, ServerMsgID: 104, ObservedAt: time.Now(),
	}})
	if err != nil || receipt[0].Outcome != store.OutboxEvidenceRecorded {
		t.Fatalf("record suffix = %+v err=%v", receipt, err)
	}
	outbox.mu.Lock()
	headAttempt := outbox.attempts[memoryAttemptKey{itemID: refs[0].ItemID, fence: refs[0].LeaseFence}]
	headAttempt.commandNotAfter = time.Now().Add(-2 * time.Second)
	headAttempt.evidenceDeadline = time.Now().Add(-time.Second)
	outbox.mu.Unlock()
	if expired, err := outbox.ExpireEvidenceDeadlines(ctx, store.OutboxEvidenceExpiryRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, LaneLimit: 1, MaxAttempts: 5,
	}); err != nil || len(expired) != 1 || expired[0].Ref != refs[0] {
		t.Fatalf("expire head = %+v err=%v", expired, err)
	}
	requests, err := outbox.RecoverFinalizableAttempts(ctx, store.OutboxRecoverFinalizableRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, LaneLimit: 1, AttemptLimit: store.MaxDeliveryBatchItems,
	})
	if err != nil || len(requests) != 2 {
		t.Fatalf("finalizable = %+v err=%v", requests, err)
	}
	finalized, err := outbox.FinalizeAttempts(ctx, requests)
	if err != nil || finalized.Results[0].Outcome != store.OutboxFinalizeScheduledRetry ||
		finalized.Results[1].Outcome != store.OutboxFinalizeFenced {
		t.Fatalf("finalize retry/suffix = %+v err=%v", finalized, err)
	}
	stale, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{{
		Ref: refs[1], Kind: store.OutboxEvidenceEdgeWritten, SourceInstanceID: "egress-a",
		TargetInstanceID: targets[1].TargetInstanceID, TargetUserID: targets[1].TargetUserID,
		BatchID: targets[1].BatchID, CommandID: targets[1].CommandID,
		EligibleSessions: 2, WrittenSessions: 2, ServerMsgID: 104, ObservedAt: time.Now(),
	}})
	if err != nil || stale[0].Outcome != store.OutboxEvidenceFenced {
		t.Fatalf("stale suffix receipt = %+v err=%v", stale, err)
	}
	fresh := claimAbsoluteForTest(t, ctx, outbox, "egress-b", 1, 2)
	if len(fresh) != 1 || len(fresh[0].Items) != 2 || fresh[0].LeaseFence == window.LeaseFence {
		t.Fatalf("fresh retry window = %+v", fresh)
	}
}

func TestDeliveryOutboxUnboundEvidenceDeadlineCreatesFreshFence(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	enqueueAbsoluteForTest(t, ctx, outbox, 7201, []byte{0x01})
	windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-crash",
		LaneLimit: 1, WindowSize: 1, WindowByteLimit: 1024, LeaseDuration: time.Second,
		PhysicalDuration: 20 * time.Millisecond, ClockSkewAllowance: 30 * time.Millisecond,
	})
	if err != nil || len(windows) != 1 {
		t.Fatalf("claim unbound crash window = %+v err=%v", windows, err)
	}
	old := windows[0].Items[0]
	next, ok, err := outbox.NextReadyAt(ctx, store.OutboxQueueAbsoluteDelivery, 0, nil)
	if err != nil || !ok || next.ReadyAt.After(old.EvidenceDeadline) || !next.ReadyAt.Before(windows[0].LeaseUntil) {
		t.Fatalf("next ready must arm evidence deadline: next=%+v ok=%v err=%v item=%+v", next, ok, err, old)
	}
	if wait := time.Until(old.EvidenceDeadline) + 15*time.Millisecond; wait > 0 {
		time.Sleep(wait)
	}
	expired, err := outbox.ExpireEvidenceDeadlines(ctx, store.OutboxEvidenceExpiryRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, LaneLimit: 1, MaxAttempts: 5,
	})
	if err != nil || len(expired) != 1 || expired[0].Ref != old.Ref {
		t.Fatalf("expire unbound evidence = %+v err=%v", expired, err)
	}
	finalized, err := outbox.FinalizeAttempts(ctx, expired)
	if err != nil || finalized.Results[0].Outcome != store.OutboxFinalizeScheduledRetry {
		t.Fatalf("finalize unbound evidence timeout = %+v err=%v", finalized, err)
	}
	fresh := claimAbsoluteForTest(t, ctx, outbox, "egress-fresh", 1, 1)[0].Items[0]
	if fresh.Ref.LeaseFence <= old.Ref.LeaseFence || fresh.Ref.Attempt <= old.Ref.Attempt {
		t.Fatalf("fresh claim did not advance fence/attempt: old=%+v fresh=%+v", old, fresh)
	}
}

func TestDeliveryOutboxBoundResolutionWaitsForEvidenceDeadline(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	enqueueAbsoluteForTest(t, ctx, outbox, 7301, []byte{0x01})
	windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-hold",
		LaneLimit: 1, WindowSize: 1, WindowByteLimit: 1024, LeaseDuration: time.Second,
		PhysicalDuration: 25 * time.Millisecond, ClockSkewAllowance: 35 * time.Millisecond,
	})
	if err != nil || len(windows) != 1 {
		t.Fatalf("claim hold window = %+v err=%v", windows, err)
	}
	item := windows[0].Items[0]
	targets := []store.OutboxAttemptTarget{
		{TargetInstanceID: "edge-a", TargetUserID: item.Ref.StreamID, BatchID: bytes16(71), CommandID: bytes16(72)},
		{TargetInstanceID: "edge-b", TargetUserID: item.Ref.StreamID, BatchID: bytes16(73), CommandID: bytes16(74)},
	}
	if bound, err := outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{
		Ref: item.Ref, SourceInstanceID: "egress-source", Targets: targets,
	}}); err != nil || bound[0].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind hold targets = %+v err=%v", bound, err)
	}
	resolved, err := outbox.ResolveTargetAttemptBatch(ctx, []store.OutboxTargetAttemptResolution{{
		Ref: item.Ref, SourceInstanceID: "egress-source",
		TargetInstanceID: targets[0].TargetInstanceID, TargetUserID: targets[0].TargetUserID,
		BatchID: targets[0].BatchID, CommandID: targets[0].CommandID,
		Kind: store.OutboxResolutionRetry, RetryDelay: time.Millisecond, LastError: "early reject",
	}})
	if err != nil || resolved[0].Outcome != store.OutboxResolutionRecorded {
		t.Fatalf("resolve early target = %+v err=%v", resolved, err)
	}
	if early, err := outbox.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{Ref: item.Ref}}); err != nil ||
		early.Results[0].Outcome != store.OutboxFinalizeWaitingForPredecessor || len(early.Next) != 0 {
		t.Fatalf("early finalize crossed evidence hold = %+v err=%v", early, err)
	}
	late, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{{
		Ref: item.Ref, Kind: store.OutboxEvidenceEdgeWritten, SourceInstanceID: "egress-source",
		TargetInstanceID: targets[1].TargetInstanceID, TargetUserID: targets[1].TargetUserID,
		BatchID: targets[1].BatchID, CommandID: targets[1].CommandID,
		EligibleSessions: 1, WrittenSessions: 1, ServerMsgID: 7301, ObservedAt: time.Now(),
	}})
	if err != nil || late[0].Outcome != store.OutboxEvidenceRejected {
		t.Fatalf("late target observation = %+v err=%v", late, err)
	}
	if wait := time.Until(item.EvidenceDeadline) + 15*time.Millisecond; wait > 0 {
		time.Sleep(wait)
	}
	ready, err := outbox.RecoverFinalizableAttempts(ctx, store.OutboxRecoverFinalizableRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, LaneLimit: 1, AttemptLimit: store.MaxDeliveryBatchItems,
	})
	if err != nil || len(ready) != 1 || ready[0].Ref != item.Ref {
		t.Fatalf("recover held resolution = %+v err=%v", ready, err)
	}
	finalized, err := outbox.FinalizeAttempts(ctx, ready)
	if err != nil || finalized.Results[0].Outcome != store.OutboxFinalizeScheduledRetry {
		t.Fatalf("finalize held retry = %+v err=%v", finalized, err)
	}
	fresh := claimAbsoluteForTest(t, ctx, outbox, "egress-after-hold", 1, 1)[0].Items[0]
	if fresh.Ref.LeaseFence <= item.Ref.LeaseFence || time.Now().Before(item.EvidenceDeadline) {
		t.Fatalf("fresh attempt overlapped old command horizon: old=%+v fresh=%+v", item, fresh)
	}
}

func TestDeliveryOutboxLateCompletionEvidenceUsesStoreClock(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	enqueueAbsoluteForTest(t, ctx, outbox, 7351, []byte{0x01})
	window := claimAbsoluteForTest(t, ctx, outbox, "egress-deadline", 1, 1)[0]
	target := store.OutboxAttemptTarget{
		TargetInstanceID: "edge-a", TargetUserID: 7351,
		BatchID: bytes16(81), CommandID: bytes16(82),
	}
	if bound, err := outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{
		{Ref: window.Items[0].Ref, SourceInstanceID: "egress-source", Targets: []store.OutboxAttemptTarget{target}},
	}); err != nil || bound[0].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind deadline targets = %+v err=%v", bound, err)
	}
	outbox.mu.Lock()
	for _, item := range window.Items {
		attempt := outbox.attempts[memoryAttemptKey{itemID: item.Ref.ItemID, fence: item.Ref.LeaseFence}]
		attempt.commandNotAfter = time.Now().Add(-2 * time.Second)
		attempt.evidenceDeadline = time.Now().Add(-time.Second)
	}
	outbox.mu.Unlock()

	// A forged future Edge timestamp cannot revive completion authority after
	// the store's own deadline.
	late, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{
		{
			Ref: window.Items[0].Ref, Kind: store.OutboxEvidenceEdgeWritten,
			SourceInstanceID: "egress-source", TargetInstanceID: target.TargetInstanceID,
			TargetUserID: target.TargetUserID, BatchID: target.BatchID, CommandID: target.CommandID,
			EligibleSessions: 1, WrittenSessions: 1, ServerMsgID: 7351,
			ObservedAt: time.Now().Add(time.Hour),
		},
	})
	if err != nil || late[0].Outcome != store.OutboxEvidenceFenced {
		t.Fatalf("late completion evidence = %+v err=%v, want Fenced", late, err)
	}
	outbox.mu.Lock()
	defer outbox.mu.Unlock()
	first := outbox.attempts[memoryAttemptKey{itemID: window.Items[0].Ref.ItemID, fence: window.Items[0].Ref.LeaseFence}]
	if first.resolution != 0 || first.targets[memoryTargetKey{
		instance: target.TargetInstanceID, userID: target.TargetUserID, batch: target.BatchID, command: target.CommandID,
	}].evidence != 0 {
		t.Fatalf("late completion mutated attempt state: first=%+v", first)
	}
}

func TestDeliveryOutboxOversizedWindowHeadClaimsAlone(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	headPayload := make([]byte, 2048)
	enqueueAbsoluteForTest(t, ctx, outbox, 7401, headPayload)
	enqueueAbsoluteForTest(t, ctx, outbox, 7401, []byte{0x02})

	windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-budget",
		LaneLimit: 1, WindowSize: 16, WindowByteLimit: 1024, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != 1 || len(windows[0].Items) != 1 {
		t.Fatalf("oversized head claim = %+v err=%v, want one-item window", windows, err)
	}
	payload, ok := windows[0].Items[0].Payload.(store.AbsoluteDeliveryPayload)
	if !ok || len(payload.TL) != len(headPayload) {
		t.Fatalf("claimed head payload = %#v", windows[0].Items[0].Payload)
	}
	if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
		TargetUserID: 7402, Payload: make([]byte, store.MaxDeliveryBatchBytes+1),
		RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
	}); err == nil {
		t.Fatal("oversized durable payload was accepted")
	}
	if err := (store.DeliveryEffect{
		Kind: store.DeliveryEffectAbsolute, TargetUserID: 7402,
		Payload:        make([]byte, store.MaxDeliveryBatchBytes+1),
		RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
	}).Validate(); err == nil {
		t.Fatal("oversized delivery effect was accepted")
	}
}

func TestDeliveryOutboxClaimsOnlyContiguousExclusionPrefix(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	const itemCount = 32
	for i := 0; i < itemCount; i++ {
		input := store.DeliveryOutboxEnqueue{
			TargetUserID: 7501, Payload: []byte{byte(i + 1)},
			RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		}
		if i%2 == 1 {
			input.ExcludeAuthKeyID = [8]byte{byte(i + 1)}
			input.ExcludeSessionID = int64(i + 1)
		}
		if _, err := outbox.Enqueue(ctx, input); err != nil {
			t.Fatalf("enqueue item %d: %v", i, err)
		}
	}

	for i := 0; i < itemCount; i++ {
		windows := claimAbsoluteForTest(t, ctx, outbox, fmt.Sprintf("egress-exclusion-%d", i), 1, itemCount)
		if len(windows) != 1 || len(windows[0].Items) != 1 {
			t.Fatalf("claim %d crossed exclusion boundary: %+v", i, windows)
		}
		ref := windows[0].Items[0].Ref
		if ref.Attempt != 1 {
			t.Fatalf("item %d attempt = %d, want first attempt", i, ref.Attempt)
		}
		bindAuthoritativeEmptyForTest(t, ctx, outbox, ref)
		recordNoTargetsForTest(t, ctx, outbox, ref)
		finalized, err := outbox.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{Ref: ref}})
		if err != nil || finalized.Results[0].Outcome != store.OutboxFinalizeApplied {
			t.Fatalf("finalize item %d = %+v err=%v", i, finalized, err)
		}
	}
	if remaining := outbox.Snapshot(); len(remaining) != 0 {
		t.Fatalf("remaining exclusion items = %d", len(remaining))
	}
}

func TestDeliveryOutboxFinalizationDeletesLiveAttempts(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	for i := 0; i < 3; i++ {
		enqueueAbsoluteForTest(t, ctx, outbox, 7601, []byte{byte(i + 1)})
	}
	window := claimAbsoluteForTest(t, ctx, outbox, "egress-retention", 1, 3)[0]
	refs := make([]store.OutboxAttemptRef, 0, len(window.Items))
	for _, item := range window.Items {
		refs = append(refs, item.Ref)
	}
	bindAuthoritativeEmptyForTest(t, ctx, outbox, refs...)
	for _, ref := range refs {
		recordNoTargetsForTest(t, ctx, outbox, ref)
	}
	requests := make([]store.OutboxFinalizeRequest, len(refs))
	for i, ref := range refs {
		requests[i] = store.OutboxFinalizeRequest{Ref: ref}
	}
	if finalized, err := outbox.FinalizeAttempts(ctx, requests); err != nil || len(finalized.Results) != len(refs) {
		t.Fatalf("finalize retention window = %+v err=%v", finalized, err)
	}
	late, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{{
		Ref: refs[0], Kind: store.OutboxEvidenceAuthoritativeNoTargets,
		SourceInstanceID: "egress-a", ObservedAt: time.Now(),
	}})
	if err != nil || late[0].Outcome != store.OutboxEvidenceFenced {
		t.Fatalf("late evidence after live-attempt deletion = %+v err=%v", late, err)
	}
	if len(outbox.Snapshot()) != 0 {
		t.Fatalf("finalization retained item state: %+v", outbox.Snapshot())
	}
	outbox.mu.Lock()
	liveAttempts := len(outbox.attempts)
	outbox.mu.Unlock()
	if liveAttempts != 0 {
		t.Fatalf("finalization retained live attempt state: attempts=%d", liveAttempts)
	}
}

func enqueueAbsoluteForTest(t *testing.T, ctx context.Context, outbox *DeliveryOutboxStore, userID int64, payload []byte) store.DeliveryOutboxItem {
	t.Helper()
	item, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
		TargetUserID: userID, Payload: payload, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	return item
}

func claimAbsoluteForTest(t *testing.T, ctx context.Context, outbox *DeliveryOutboxStore, owner string, lanes, window int) []store.OutboxClaimWindow {
	t.Helper()
	claimed, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: owner,
		LaneLimit: lanes, WindowSize: window, WindowByteLimit: 1 << 20, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil {
		t.Fatalf("ClaimWindows: %v", err)
	}
	return claimed
}

func bindAuthoritativeEmptyForTest(t *testing.T, ctx context.Context, outbox *DeliveryOutboxStore, refs ...store.OutboxAttemptRef) {
	t.Helper()
	sets := make([]store.OutboxAttemptTargetSet, len(refs))
	for i, ref := range refs {
		sets[i] = store.OutboxAttemptTargetSet{Ref: ref, SourceInstanceID: "egress-a", Targets: []store.OutboxAttemptTarget{}}
	}
	results, err := outbox.BindAttemptTargets(ctx, sets)
	if err != nil {
		t.Fatalf("BindAttemptTargets: %v", err)
	}
	for i, result := range results {
		if result.Outcome != store.OutboxBindTargetBound {
			t.Fatalf("BindAttemptTargets[%d] = %+v", i, result)
		}
	}
}

func recordNoTargetsForTest(t *testing.T, ctx context.Context, outbox *DeliveryOutboxStore, ref store.OutboxAttemptRef) {
	t.Helper()
	results, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{{
		Ref: ref, Kind: store.OutboxEvidenceAuthoritativeNoTargets,
		SourceInstanceID: "egress-a", ObservedAt: time.Now(),
	}})
	if err != nil || len(results) != 1 || (results[0].Outcome != store.OutboxEvidenceRecorded && results[0].Outcome != store.OutboxEvidenceDuplicate) {
		t.Fatalf("RecordAttemptEvidenceBatch = %+v err=%v", results, err)
	}
}

func bytes16(value byte) [16]byte {
	var out [16]byte
	out[15] = value
	return out
}
