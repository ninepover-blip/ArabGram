package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"telesrv/internal/store"
)

func TestDeliveryOutboxAppendDoesNotWaitForUncommittedActiveLaneUpdatePostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	user := createTestUser(t, ctx, NewUserStore(pool), "+1887"+randomSuffix(t)+"00", "AbsoluteUpdatedLane", "")
	cleanupOutboxUser(t, pool, user.ID)
	outbox := NewDeliveryOutboxStore(pool)
	firstID, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
		TargetUserID: user.ID, Payload: []byte{0x01}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
	})
	if err != nil {
		t.Fatalf("enqueue head: %v", err)
	}

	consumer, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = consumer.Rollback(context.Background()) }()
	if _, err := consumer.Exec(ctx, `
UPDATE edge_delivery_outbox_lanes
SET lease_owner = 'uncommitted-consumer', updated_at = clock_timestamp()
WHERE stream_id = $1`, user.ID); err != nil {
		t.Fatalf("update active lane: %v", err)
	}

	appendCtx, cancelAppend := context.WithTimeout(ctx, time.Second)
	defer cancelAppend()
	appendDone := make(chan error, 1)
	go func() {
		_, err := outbox.Enqueue(appendCtx, store.DeliveryOutboxEnqueue{
			TargetUserID: user.ID, Payload: []byte{0x02}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		})
		appendDone <- err
	}()
	select {
	case err := <-appendDone:
		if err != nil {
			t.Fatalf("append behind uncommitted lane update: %v", err)
		}
	case <-appendCtx.Done():
		_ = consumer.Rollback(context.Background())
		t.Fatalf("append waited for consumer lane update: %v", appendCtx.Err())
	}
	if err := consumer.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	var headID, count int64
	if err := pool.QueryRow(ctx, `SELECT head_item_id FROM edge_delivery_outbox_lanes WHERE stream_id = $1`, user.ID).Scan(&headID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = $1`, user.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if headID != firstID.ID || count != 2 {
		t.Fatalf("head id=%d rows=%d want %d/2", headID, count, firstID.ID)
	}
}

func TestDeliveryOutboxDurableAttemptRecoveryPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	user := createTestUser(t, ctx, NewUserStore(pool), "+1887"+randomSuffix(t)+"01", "OutboxRecovery", "")
	cleanupOutboxUser(t, pool, user.ID)
	outbox := NewDeliveryOutboxStore(pool, WithDeliveryLeaseTimeout(time.Minute))
	if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
		TargetUserID: user.ID, Payload: []byte{0x01}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
	}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-a/worker-1",
		LaneLimit: 1, WindowSize: 1, WindowByteLimit: 1024, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != 1 || len(windows[0].Items) != 1 {
		t.Fatalf("claim = %+v err=%v", windows, err)
	}
	ref := windows[0].Items[0].Ref
	target := store.OutboxAttemptTarget{
		TargetInstanceID: "edge-a", TargetUserID: user.ID,
		BatchID: testOutboxID(1), CommandID: testOutboxID(2),
	}
	bound, err := outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{
		Ref: ref, SourceInstanceID: "egress-source-a", Targets: []store.OutboxAttemptTarget{target},
	}})
	if err != nil || bound[0].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind = %+v err=%v", bound, err)
	}
	recovered, err := outbox.RecoverBoundWindows(ctx, store.OutboxRecoverBoundRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Mode: store.OutboxBoundRecoveryExactOwnerLive, Owner: windows[0].Owner,
		LaneLimit: 1, LeaseDuration: time.Minute,
	})
	if err != nil || len(recovered) != 1 || len(recovered[0].Items) != 1 {
		t.Fatalf("recover live = %+v err=%v", recovered, err)
	}
	got := recovered[0].Items[0]
	if got.Ref != ref || got.SourceInstanceID != "egress-source-a" ||
		!got.CommandNotAfter.Equal(windows[0].Items[0].CommandNotAfter) ||
		!got.EvidenceDeadline.Equal(windows[0].Items[0].EvidenceDeadline) ||
		len(got.Targets) != 1 || got.Targets[0] != target {
		t.Fatalf("recovered item = %+v", got)
	}
	if _, err := pool.Exec(ctx, `UPDATE edge_delivery_outbox_lanes SET lease_until = now() - interval '1 second' WHERE stream_id = $1`, user.ID); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE edge_delivery_outbox_attempts
SET issued_at = now() - interval '3 seconds',
    command_not_after = now() - interval '2 seconds',
    evidence_deadline = now() - interval '1 second'
WHERE item_id = $1 AND lease_fence = $2`, ref.ItemID, int64(ref.LeaseFence)); err != nil {
		t.Fatalf("expire evidence deadline: %v", err)
	}
	if stolen, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-b/worker-2",
		LaneLimit: 1, WindowSize: 1, WindowByteLimit: 1024, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	}); err != nil || len(stolen) != 0 {
		t.Fatalf("ordinary claim stole bound ledger = %+v err=%v", stolen, err)
	}
	expiredRefs, err := outbox.ExpireEvidenceDeadlines(ctx, store.OutboxEvidenceExpiryRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, LaneLimit: 1, MaxAttempts: 5,
	})
	if err != nil || len(expiredRefs) != 1 || expiredRefs[0].Ref != ref {
		t.Fatalf("expired plan must resolve retry, refs=%+v err=%v", expiredRefs, err)
	}
	expired, err := outbox.RecoverFinalizableAttempts(ctx, store.OutboxRecoverFinalizableRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, LaneLimit: 1, AttemptLimit: store.MaxDeliveryBatchItems,
	})
	if err != nil || len(expired) != 1 || expired[0].Ref != ref {
		t.Fatalf("expired finalizable = %+v err=%v", expired, err)
	}
	if batch, err := outbox.FinalizeAttempts(ctx, expired); err != nil ||
		batch.Results[0].Outcome != store.OutboxFinalizeScheduledRetry {
		t.Fatalf("finalize expired retry = %+v err=%v", batch, err)
	}
	freshWindows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-b/worker-2",
		LaneLimit: 1, WindowSize: 1, WindowByteLimit: 1024, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(freshWindows) != 1 || len(freshWindows[0].Items) != 1 {
		t.Fatalf("fresh claim = %+v err=%v", freshWindows, err)
	}
	fresh := freshWindows[0].Items[0]
	if fresh.Ref.LeaseFence == ref.LeaseFence || fresh.Ref.Attempt == ref.Attempt ||
		fresh.SourceInstanceID != "" || len(fresh.Targets) != 0 {
		t.Fatalf("fresh claim reused expired identity = %+v", fresh)
	}
	newTarget := target
	newTarget.BatchID = testOutboxID(3)
	newTarget.CommandID = testOutboxID(4)
	if results, err := outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{
		Ref: fresh.Ref, SourceInstanceID: "egress-source-b", Targets: []store.OutboxAttemptTarget{newTarget},
	}}); err != nil || results[0].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind fresh identity = %+v err=%v", results, err)
	}
	stale, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{{
		Ref: ref, Kind: store.OutboxEvidenceEdgeWritten, SourceInstanceID: "egress-source-a",
		EligibleSessions: 1, WrittenSessions: 1, ServerMsgID: 301,
		TargetInstanceID: target.TargetInstanceID, TargetUserID: target.TargetUserID,
		BatchID: target.BatchID, CommandID: target.CommandID, ObservedAt: time.Now(),
	}})
	if err != nil || stale[0].Outcome != store.OutboxEvidenceFenced {
		t.Fatalf("stale evidence = %+v err=%v", stale, err)
	}
	wrongSource, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{{
		Ref: fresh.Ref, Kind: store.OutboxEvidenceEdgeWritten, SourceInstanceID: "egress-source-a",
		TargetInstanceID: newTarget.TargetInstanceID, TargetUserID: newTarget.TargetUserID,
		BatchID: newTarget.BatchID, CommandID: newTarget.CommandID,
		EligibleSessions: 1, WrittenSessions: 1, ServerMsgID: 302, ObservedAt: time.Now(),
	}})
	if err != nil || wrongSource[0].Outcome != store.OutboxEvidenceFenced {
		t.Fatalf("wrong-source evidence = %+v err=%v", wrongSource, err)
	}
	current, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{{
		Ref: fresh.Ref, Kind: store.OutboxEvidenceEdgeWritten, SourceInstanceID: "egress-source-b",
		EligibleSessions: 1, WrittenSessions: 1, ServerMsgID: 303,
		TargetInstanceID: newTarget.TargetInstanceID, TargetUserID: newTarget.TargetUserID,
		BatchID: newTarget.BatchID, CommandID: newTarget.CommandID, ObservedAt: time.Now(),
	}})
	if err != nil || current[0].Outcome != store.OutboxEvidenceRecorded {
		t.Fatalf("current evidence = %+v err=%v", current, err)
	}
	var evidenceKind string
	var eligibleSessions, writtenSessions int
	if err := pool.QueryRow(ctx, `
SELECT evidence_kind, eligible_sessions, written_sessions
FROM edge_delivery_outbox_attempt_targets
WHERE item_id = $1 AND lease_fence = $2
  AND target_instance_id = $3 AND target_user_id = $4
  AND batch_id = $5 AND command_id = $6`, fresh.Ref.ItemID, int64(fresh.Ref.LeaseFence),
		newTarget.TargetInstanceID, newTarget.TargetUserID, newTarget.BatchID[:], newTarget.CommandID[:]).Scan(
		&evidenceKind, &eligibleSessions, &writtenSessions); err != nil {
		t.Fatalf("load physical evidence ledger: %v", err)
	}
	if evidenceKind != "edge_written" || eligibleSessions != 1 || writtenSessions != 1 {
		t.Fatalf("physical ledger = %q/%d/%d", evidenceKind, eligibleSessions, writtenSessions)
	}
	finalizable, err := outbox.RecoverFinalizableAttempts(ctx, store.OutboxRecoverFinalizableRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, LaneLimit: 1, AttemptLimit: store.MaxDeliveryBatchItems,
	})
	if err != nil || len(finalizable) != 1 || finalizable[0].Ref != fresh.Ref {
		t.Fatalf("recover finalizable = %+v err=%v", finalizable, err)
	}
	finalized, err := outbox.FinalizeAttempts(ctx, finalizable)
	if err != nil || finalized.Results[0].Outcome != store.OutboxFinalizeApplied {
		t.Fatalf("finalize = %+v err=%v", finalized, err)
	}
}

func TestDeliveryOutboxRecoveryBoundsAttemptsAcrossLanesPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users := []int64{
		createTestUser(t, ctx, NewUserStore(pool), "+1887"+randomSuffix(t)+"31", "OutboxBatchA", "").ID,
		createTestUser(t, ctx, NewUserStore(pool), "+1887"+randomSuffix(t)+"32", "OutboxBatchB", "").ID,
	}
	for _, userID := range users {
		cleanupOutboxUser(t, pool, userID)
	}
	outbox := NewDeliveryOutboxStore(pool, WithDeliveryLeaseTimeout(time.Minute))
	for _, userID := range users {
		for item := 0; item < store.MaxDeliveryBatchItems; item++ {
			if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
				TargetUserID: userID, Payload: []byte{byte(item + 1)}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
			}); err != nil {
				t.Fatalf("enqueue lane %d item %d: %v", userID, item, err)
			}
		}
	}
	windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-recovery-limit",
		LaneLimit: 2, WindowSize: store.MaxDeliveryBatchItems, WindowByteLimit: 1 << 20,
		LeaseDuration: time.Minute, PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != 2 {
		t.Fatalf("claim recovery lanes = %d err=%v, want 2", len(windows), err)
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
	if err != nil || len(requests) != 0 {
		t.Fatalf("terminal recovery = %d err=%v", len(requests), err)
	}
}

func TestDeliveryOutboxFinalizeBatchesIndependentLanesIntoBoundedTransactionsPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	const laneCount = outboxFinalizeTransactionLanes + 1
	userIDs := make([]int64, 0, laneCount)
	users := NewUserStore(pool)
	for i := 0; i < laneCount; i++ {
		user := createTestUser(t, ctx, users, fmt.Sprintf("+1888%s%02d", randomSuffix(t), i), "OutboxFinalizeBatch", "")
		userIDs = append(userIDs, user.ID)
		cleanupOutboxUser(t, pool, user.ID)
	}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		for _, userID := range userIDs {
			cleanupOutboxUser(t, pool, userID)
		}
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM users WHERE id = ANY($1::bigint[])`, userIDs)
	})

	countingDB := &countingBeginDB{Pool: pool}
	outbox := NewDeliveryOutboxStore(countingDB, WithDeliveryLeaseTimeout(time.Minute))
	for _, userID := range userIDs {
		if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
			TargetUserID: userID, Payload: []byte{0x01}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		}); err != nil {
			t.Fatalf("enqueue user %d: %v", userID, err)
		}
		if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
			TargetUserID: userID, Payload: []byte{0x02}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		}); err != nil {
			t.Fatalf("enqueue successor for user %d: %v", userID, err)
		}
	}
	windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-finalize-batch",
		LaneLimit: laneCount, WindowSize: 1, WindowByteLimit: 1024, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != laneCount {
		t.Fatalf("claim lanes = %d err=%v, want %d", len(windows), err, laneCount)
	}
	sets := make([]store.OutboxAttemptTargetSet, 0, laneCount)
	evidence := make([]store.OutboxAttemptEvidence, 0, laneCount)
	finalize := make([]store.OutboxFinalizeRequest, 0, laneCount)
	for _, window := range windows {
		if len(window.Items) != 1 {
			t.Fatalf("lane %d items=%d want=1", window.StreamID, len(window.Items))
		}
		ref := window.Items[0].Ref
		sets = append(sets, store.OutboxAttemptTargetSet{
			Ref: ref, SourceInstanceID: "egress-finalize-batch-source", Targets: []store.OutboxAttemptTarget{},
		})
		evidence = append(evidence, store.OutboxAttemptEvidence{
			Ref: ref, Kind: store.OutboxEvidenceAuthoritativeNoTargets,
			SourceInstanceID: "egress-finalize-batch-source", ObservedAt: time.Now(),
		})
		finalize = append(finalize, store.OutboxFinalizeRequest{Ref: ref})
	}
	if bound, err := outbox.BindAttemptTargets(ctx, sets); err != nil || len(bound) != laneCount {
		t.Fatalf("bind lanes = %d err=%v", len(bound), err)
	}
	if recorded, err := outbox.RecordAttemptEvidenceBatch(ctx, evidence); err != nil || len(recorded) != laneCount {
		t.Fatalf("record evidence = %d err=%v", len(recorded), err)
	}

	countingDB.resetBeginCount()
	countingDB.resetQueryCount()
	batch, err := outbox.FinalizeAttempts(ctx, finalize)
	if err != nil || len(batch.Results) != laneCount {
		t.Fatalf("finalize lanes = %d err=%v", len(batch.Results), err)
	}
	for i, result := range batch.Results {
		if result.Outcome != store.OutboxFinalizeApplied {
			t.Fatalf("finalize result %d = %+v", i, result)
		}
	}
	if got := countingDB.begins(); got != 2 {
		t.Fatalf("finalize begin count=%d want=2 for %d lanes", got, laneCount)
	}
	if got := countingDB.queries(); got != 6 {
		t.Fatalf("finalize transaction query count=%d want=6 (lock/window/mutation per group)", got)
	}
}

func TestDeliveryOutboxLateCompletionEvidenceUsesDatabaseClockPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	user := createTestUser(t, ctx, NewUserStore(pool), "+1887"+randomSuffix(t)+"11", "OutboxDeadline", "")
	cleanupOutboxUser(t, pool, user.ID)
	outbox := NewDeliveryOutboxStore(pool)
	for i := 0; i < 2; i++ {
		if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
			TargetUserID: user.ID, Payload: []byte{byte(i + 1)}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		}); err != nil {
			t.Fatal(err)
		}
	}
	windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-deadline",
		LaneLimit: 1, WindowSize: 2, WindowByteLimit: 1024, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != 1 || len(windows[0].Items) != 2 {
		t.Fatalf("claim deadline window = %+v err=%v", windows, err)
	}
	target := store.OutboxAttemptTarget{
		TargetInstanceID: "edge-deadline", TargetUserID: user.ID,
		BatchID: testOutboxID(91), CommandID: testOutboxID(92),
	}
	if bound, err := outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{
		{Ref: windows[0].Items[0].Ref, SourceInstanceID: "egress-source", Targets: []store.OutboxAttemptTarget{target}},
		{Ref: windows[0].Items[1].Ref, SourceInstanceID: "egress-source", Targets: []store.OutboxAttemptTarget{target}},
	}); err != nil || bound[0].Outcome != store.OutboxBindTargetBound || bound[1].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind deadline targets = %+v err=%v", bound, err)
	}
	if _, err := pool.Exec(ctx, `
UPDATE edge_delivery_outbox_attempts
SET issued_at = clock_timestamp() - interval '3 seconds',
    command_not_after = clock_timestamp() - interval '2 seconds',
    evidence_deadline = clock_timestamp() - interval '1 second'
WHERE stream_id = $1 AND lease_fence = $2`, user.ID, int64(windows[0].LeaseFence)); err != nil {
		t.Fatalf("expire evidence authority: %v", err)
	}
	late, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{
		{
			Ref: windows[0].Items[0].Ref, Kind: store.OutboxEvidenceEdgeWritten,
			SourceInstanceID: "egress-source", TargetInstanceID: target.TargetInstanceID,
			TargetUserID: target.TargetUserID, BatchID: target.BatchID, CommandID: target.CommandID,
			EligibleSessions: 1, WrittenSessions: 1, ServerMsgID: 911,
			// Edge time is intentionally forged into the future; DB time owns authority.
			ObservedAt: time.Now().Add(time.Hour),
		},
		{
			Ref: windows[0].Items[1].Ref, Kind: store.OutboxEvidenceEdgeWritten,
			SourceInstanceID: "egress-source", TargetInstanceID: target.TargetInstanceID,
			TargetUserID: target.TargetUserID, BatchID: target.BatchID, CommandID: target.CommandID,
			EligibleSessions: 1, WrittenSessions: 1, ServerMsgID: 912,
			ObservedAt: time.Now().Add(time.Hour),
		},
	})
	if err != nil || late[0].Outcome != store.OutboxEvidenceFenced || late[1].Outcome != store.OutboxEvidenceFenced {
		t.Fatalf("late completion evidence = %+v err=%v, want Fenced/Fenced", late, err)
	}
	var completedAttempts, physicalTargets int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FILTER (WHERE resolution IS NOT NULL)
FROM edge_delivery_outbox_attempts
WHERE stream_id = $1 AND lease_fence = $2`, user.ID, int64(windows[0].LeaseFence)).Scan(&completedAttempts); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
SELECT count(*) FROM edge_delivery_outbox_attempt_targets
WHERE item_id = $1 AND lease_fence = $2 AND evidence_kind IS NOT NULL`,
		windows[0].Items[0].Ref.ItemID, int64(windows[0].LeaseFence)).Scan(&physicalTargets); err != nil {
		t.Fatal(err)
	}
	if completedAttempts != 0 || physicalTargets != 0 {
		t.Fatalf("late completion mutated durable state: attempts=%d targets=%d", completedAttempts, physicalTargets)
	}
}

func TestDeliveryOutboxResolutionRequiresExactTargetOrLiveOwnerPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	user := createTestUser(t, ctx, NewUserStore(pool), "+1887"+randomSuffix(t)+"09", "OutboxResolveAuth", "")
	cleanupOutboxUser(t, pool, user.ID)
	outbox := NewDeliveryOutboxStore(pool)
	for i := 0; i < 2; i++ {
		if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
			TargetUserID: user.ID, Payload: []byte{byte(i + 1)}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `UPDATE edge_delivery_outbox_lanes SET ready_at = '-infinity'::timestamptz WHERE stream_id = $1`, user.ID); err != nil {
		t.Fatal(err)
	}
	shard := int(user.ID % store.DispatchOutboxLogicalShards)
	windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-owner",
		LogicalShardCount: store.DispatchOutboxLogicalShards, LogicalShardIDs: []int{shard},
		LaneLimit: 1, WindowSize: 2, WindowByteLimit: 1024, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != 1 || windows[0].StreamID != user.ID || len(windows[0].Items) != 2 {
		t.Fatalf("claim resolution authority window = %+v err=%v", windows, err)
	}
	targets := []store.OutboxAttemptTarget{
		{TargetInstanceID: "edge-a", TargetUserID: user.ID, BatchID: testOutboxID(41), CommandID: testOutboxID(51)},
		{TargetInstanceID: "edge-b", TargetUserID: user.ID, BatchID: testOutboxID(42), CommandID: testOutboxID(52)},
	}
	sets := make([]store.OutboxAttemptTargetSet, 2)
	for i := range sets {
		sets[i] = store.OutboxAttemptTargetSet{Ref: windows[0].Items[i].Ref, SourceInstanceID: "egress-source", Targets: []store.OutboxAttemptTarget{targets[i]}}
	}
	if bound, err := outbox.BindAttemptTargets(ctx, sets); err != nil ||
		bound[0].Outcome != store.OutboxBindTargetBound || bound[1].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind resolution targets = %+v err=%v", bound, err)
	}
	first := windows[0].Items[0].Ref
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
	second := windows[0].Items[1].Ref
	wrongOwner, err := outbox.ResolveOwnedAttemptBatch(ctx, []store.OutboxOwnedAttemptResolution{{
		Ref: second, Owner: "other-owner", Kind: store.OutboxResolutionAbandoned,
	}})
	if err != nil || wrongOwner[0].Outcome != store.OutboxResolutionFenced {
		t.Fatalf("wrong-owner resolution = %+v err=%v", wrongOwner, err)
	}
	owned, err := outbox.ResolveOwnedAttemptBatch(ctx, []store.OutboxOwnedAttemptResolution{{
		Ref: second, Owner: windows[0].Owner, Kind: store.OutboxResolutionAbandoned,
	}})
	if err != nil || owned[0].Outcome != store.OutboxResolutionRecorded {
		t.Fatalf("live-owner resolution = %+v err=%v", owned, err)
	}
}

func TestDeliveryOutboxFinalizeFreshSnapshotClosesEmptyRacePostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	user := createTestUser(t, ctx, NewUserStore(pool), "+1887"+randomSuffix(t)+"02", "OutboxRace", "")
	cleanupOutboxUser(t, pool, user.ID)
	outbox := NewDeliveryOutboxStore(pool, WithDeliveryLeaseTimeout(time.Minute))
	first, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
		TargetUserID: user.ID, Payload: []byte{0x01}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
	})
	if err != nil {
		t.Fatal(err)
	}
	windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-a/worker-1",
		LaneLimit: 1, WindowSize: 1, WindowByteLimit: 1024, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != 1 {
		t.Fatalf("claim = %+v err=%v", windows, err)
	}
	ref := windows[0].Items[0].Ref
	if results, err := outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{
		Ref: ref, SourceInstanceID: "egress-a", Targets: []store.OutboxAttemptTarget{},
	}}); err != nil || results[0].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind empty = %+v err=%v", results, err)
	}
	lockTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(ctx) }()
	if _, err := lockTx.Exec(ctx, `SELECT 1 FROM edge_delivery_outbox_lanes WHERE stream_id = $1 FOR UPDATE`, user.ID); err != nil {
		t.Fatal(err)
	}
	type finalizeResult struct {
		batch store.OutboxFinalizeBatch
		err   error
	}
	done := make(chan finalizeResult, 1)
	go func() {
		batch, err := outbox.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{Ref: ref}})
		done <- finalizeResult{batch: batch, err: err}
	}()
	time.Sleep(50 * time.Millisecond)
	select {
	case early := <-done:
		t.Fatalf("finalize returned before the lane lock was released: batch=%+v err=%v", early.batch, early.err)
	default:
	}
	second, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
		TargetUserID: user.ID, Payload: []byte{0x02}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
	})
	if err != nil {
		t.Fatalf("append while lane locked: %v", err)
	}
	if err := lockTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	result := <-done
	if result.err != nil || result.batch.Results[0].Outcome != store.OutboxFinalizeApplied {
		t.Fatalf("finalize = %+v err=%v", result.batch, result.err)
	}
	var head int64
	if err := pool.QueryRow(ctx, `SELECT head_item_id FROM edge_delivery_outbox_lanes WHERE stream_id = $1`, user.ID).Scan(&head); err != nil {
		t.Fatalf("load promoted lane: %v", err)
	}
	if head != second.ID || head == first.ID {
		t.Fatalf("head=%d want successor=%d", head, second.ID)
	}
}

func TestDeliveryOutboxFinalizeWaitsForUncommittedAppendMarkerPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	user := createTestUser(t, ctx, NewUserStore(pool), "+1887"+randomSuffix(t)+"15", "OutboxCommitFence", "")
	cleanupOutboxUser(t, pool, user.ID)
	outbox := NewDeliveryOutboxStore(pool)
	if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
		TargetUserID: user.ID, Payload: []byte{0x01}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
	}); err != nil {
		t.Fatal(err)
	}
	windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-commit-fence",
		LaneLimit: 1, WindowSize: 1, WindowByteLimit: 1024, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != 1 || len(windows[0].Items) != 1 {
		t.Fatalf("claim = %+v err=%v", windows, err)
	}
	ref := windows[0].Items[0].Ref
	if results, err := outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{
		Ref: ref, SourceInstanceID: "egress-commit-fence", Targets: []store.OutboxAttemptTarget{},
	}}); err != nil || results[0].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind = %+v err=%v", results, err)
	}

	producer, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = producer.Rollback(ctx) }()
	var successorID int64
	if err := producer.QueryRow(ctx, `
INSERT INTO edge_delivery_outbox (target_user_id, payload, recovery_policy)
VALUES ($1, decode('02', 'hex'), 'absolute_reload')
RETURNING id`, user.ID).Scan(&successorID); err != nil {
		t.Fatalf("insert uncommitted successor: %v", err)
	}

	type finalizeResult struct {
		batch store.OutboxFinalizeBatch
		err   error
	}
	done := make(chan finalizeResult, 1)
	go func() {
		batch, err := outbox.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{Ref: ref}})
		done <- finalizeResult{batch: batch, err: err}
	}()
	select {
	case result := <-done:
		t.Fatalf("finalizer crossed an uncommitted append: batch=%+v err=%v", result.batch, result.err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := producer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	result := <-done
	if result.err != nil || len(result.batch.Results) != 1 || result.batch.Results[0].Outcome != store.OutboxFinalizeApplied {
		t.Fatalf("finalize = %+v err=%v", result.batch, result.err)
	}
	var head int64
	if err := pool.QueryRow(ctx, `SELECT head_item_id FROM edge_delivery_outbox_lanes WHERE stream_id = $1`, user.ID).Scan(&head); err != nil {
		t.Fatal(err)
	}
	if head != successorID {
		t.Fatalf("head=%d want uncommitted successor=%d", head, successorID)
	}
}

func TestDeliveryOutboxTenThousandAppendsDoNotLockActiveLanePostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	user := createTestUser(t, ctx, NewUserStore(pool), "+1887"+randomSuffix(t)+"03", "OutboxAppend", "")
	cleanupOutboxUser(t, pool, user.ID)
	outbox := NewDeliveryOutboxStore(pool)
	first, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
		TargetUserID: user.ID, Payload: []byte{0x01}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
	})
	if err != nil {
		t.Fatal(err)
	}
	lockTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(ctx) }()
	if _, err := lockTx.Exec(ctx, `SELECT 1 FROM edge_delivery_outbox_lanes WHERE stream_id = $1 FOR UPDATE`, user.ID); err != nil {
		t.Fatal(err)
	}
	appendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := pool.Exec(appendCtx, `
INSERT INTO edge_delivery_outbox (target_user_id, payload, recovery_policy)
SELECT $1, decode('01', 'hex'), 'absolute_reload'
FROM generate_series(1, 10000)`, user.ID); err != nil {
		t.Fatalf("10k append waited on active lane: %v", err)
	}
	var head, rows int64
	if err := pool.QueryRow(ctx, `SELECT head_item_id FROM edge_delivery_outbox_lanes WHERE stream_id = $1`, user.ID).Scan(&head); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = $1`, user.ID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if head != first.ID || rows != 10001 {
		t.Fatalf("head=%d rows=%d want head=%d rows=10001", head, rows, first.ID)
	}
}

func TestDurableOutboxTargetBindingIsSetExactAndFreezesMembershipPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	user := createTestUser(t, ctx, NewUserStore(pool), "+1887"+randomSuffix(t)+"12", "OutboxTargets", "")
	cleanupOutboxUser(t, pool, user.ID)
	outbox := NewDeliveryOutboxStore(pool)
	if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
		TargetUserID: user.ID, Payload: []byte{1}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
	}); err != nil {
		t.Fatal(err)
	}
	windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-targets",
		LaneLimit: 1, WindowSize: 1, WindowByteLimit: 1024, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != 1 {
		t.Fatalf("claim target test = %+v err=%v", windows, err)
	}
	ref := windows[0].Items[0].Ref
	targets := make([]store.OutboxAttemptTarget, 64)
	for i := range targets {
		targets[i] = store.OutboxAttemptTarget{
			TargetInstanceID: fmt.Sprintf("edge-%02d", i), TargetUserID: user.ID,
			BatchID: testOutboxID(byte(i + 1)), CommandID: testOutboxID(byte(i + 65)),
		}
	}
	if bound, err := outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{
		Ref: ref, SourceInstanceID: "egress-source", Targets: targets,
	}}); err != nil || bound[0].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind 64 frozen targets = %+v err=%v", bound, err)
	}
	var parentBound bool
	var expectedCount, actualCount int
	if err := pool.QueryRow(ctx, `
SELECT a.targets_bound, a.target_count, count(t.*)::integer
FROM edge_delivery_outbox_attempts a
LEFT JOIN edge_delivery_outbox_attempt_targets t
  ON t.item_id = a.item_id AND t.lease_fence = a.lease_fence
WHERE a.item_id = $1 AND a.lease_fence = $2
GROUP BY a.targets_bound, a.target_count`, ref.ItemID, int64(ref.LeaseFence)).
		Scan(&parentBound, &expectedCount, &actualCount); err != nil {
		t.Fatal(err)
	}
	if !parentBound || expectedCount != len(targets) || actualCount != len(targets) {
		t.Fatalf("bound=%t expected=%d actual=%d, want true/%d/%d",
			parentBound, expectedCount, actualCount, len(targets), len(targets))
	}
	var countTriggers int
	if err := pool.QueryRow(ctx, `
SELECT count(*)
FROM pg_trigger
WHERE tgrelid IN (
  'dispatch_outbox_attempts'::regclass,
  'edge_delivery_outbox_attempts'::regclass,
  'channel_delivery_attempts'::regclass
)
  AND NOT tgisinternal AND tgname LIKE '%target_count%'`).Scan(&countTriggers); err != nil {
		t.Fatal(err)
	}
	if countTriggers != 0 {
		t.Fatalf("per-attempt target count validators=%d, want zero set-based bind duplicates", countTriggers)
	}
	var fenceIndex string
	if err := pool.QueryRow(ctx, `SELECT to_regclass('public.channel_delivery_attempts_fence_idx')::text`).Scan(&fenceIndex); err != nil {
		t.Fatal(err)
	}
	if fenceIndex == "" {
		t.Fatal("channel live-attempt fence index is missing")
	}
	extraBatch, extraCommand := testOutboxID(201), testOutboxID(202)
	if _, err := pool.Exec(ctx, `
INSERT INTO edge_delivery_outbox_attempt_targets (
  item_id, lease_fence, target_instance_id, target_user_id, batch_id, command_id
) VALUES ($1, $2, 'edge-forged', $3, $4, $5)`,
		ref.ItemID, int64(ref.LeaseFence), user.ID, extraBatch[:], extraCommand[:]); err == nil {
		t.Fatal("direct INSERT mutated frozen target membership")
	}
	if _, err := pool.Exec(ctx, `
DELETE FROM edge_delivery_outbox_attempt_targets
WHERE item_id = $1 AND lease_fence = $2 AND target_instance_id = $3`,
		ref.ItemID, int64(ref.LeaseFence), targets[0].TargetInstanceID); err == nil {
		t.Fatal("direct DELETE mutated frozen target membership")
	}
	// FK cascade must remain available to the same-transaction live-attempt delete.
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM edge_delivery_outbox_attempts WHERE item_id = $1 AND lease_fence = $2`,
		ref.ItemID, int64(ref.LeaseFence)); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("attempt cascade was blocked by membership guard: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDeliveryOutboxExpiredHeadRetryDeletesConfirmedSuffixAttemptPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	user := createTestUser(t, ctx, NewUserStore(pool), "+1887"+randomSuffix(t)+"04", "OutboxSuffix", "")
	cleanupOutboxUser(t, pool, user.ID)
	outbox := NewDeliveryOutboxStore(pool)
	for i := 0; i < 2; i++ {
		if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
			TargetUserID: user.ID, Payload: []byte{byte(i + 1)}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		}); err != nil {
			t.Fatal(err)
		}
	}
	windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-a",
		LaneLimit: 1, WindowSize: 2, WindowByteLimit: 1024, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != 1 || len(windows[0].Items) != 2 {
		t.Fatalf("claim = %+v err=%v", windows, err)
	}
	refs := []store.OutboxAttemptRef{windows[0].Items[0].Ref, windows[0].Items[1].Ref}
	targets := []store.OutboxAttemptTarget{
		{TargetInstanceID: "edge-a", TargetUserID: user.ID, BatchID: testOutboxID(20), CommandID: testOutboxID(21)},
		{TargetInstanceID: "edge-a", TargetUserID: user.ID, BatchID: testOutboxID(20), CommandID: testOutboxID(22)},
	}
	sets := make([]store.OutboxAttemptTargetSet, 2)
	for i := range refs {
		sets[i] = store.OutboxAttemptTargetSet{Ref: refs[i], SourceInstanceID: "egress-a", Targets: []store.OutboxAttemptTarget{targets[i]}}
	}
	bound, err := outbox.BindAttemptTargets(ctx, sets)
	if err != nil || bound[0].Outcome != store.OutboxBindTargetBound || bound[1].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind = %+v err=%v", bound, err)
	}
	physical, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{{
		Ref: refs[1], Kind: store.OutboxEvidenceEdgeWritten, SourceInstanceID: "egress-a",
		TargetInstanceID: targets[1].TargetInstanceID, TargetUserID: targets[1].TargetUserID,
		BatchID: targets[1].BatchID, CommandID: targets[1].CommandID,
		EligibleSessions: 3, WrittenSessions: 3, ServerMsgID: 401, ObservedAt: time.Now(),
	}})
	if err != nil || physical[0].Outcome != store.OutboxEvidenceRecorded {
		t.Fatalf("record suffix = %+v err=%v", physical, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE edge_delivery_outbox_lanes
SET lease_until = now() - interval '1 second' WHERE stream_id = $1`, user.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE edge_delivery_outbox_attempts
SET issued_at = now() - interval '3 seconds',
    command_not_after = now() - interval '2 seconds',
    evidence_deadline = now() - interval '1 second'
WHERE item_id = $1 AND lease_fence = $2`, refs[0].ItemID, int64(refs[0].LeaseFence)); err != nil {
		t.Fatal(err)
	}
	if expired, err := outbox.ExpireEvidenceDeadlines(ctx, store.OutboxEvidenceExpiryRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, LaneLimit: 1, MaxAttempts: 5,
	}); err != nil || len(expired) != 1 || expired[0].Ref != refs[0] {
		t.Fatalf("expire head = %+v err=%v", expired, err)
	}
	requests, err := outbox.RecoverFinalizableAttempts(ctx, store.OutboxRecoverFinalizableRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, LaneLimit: 1, AttemptLimit: store.MaxDeliveryBatchItems,
	})
	if err != nil || len(requests) != 2 {
		t.Fatalf("recover finalizable = %+v err=%v", requests, err)
	}
	finalized, err := outbox.FinalizeAttempts(ctx, requests)
	if err != nil || finalized.Results[0].Outcome != store.OutboxFinalizeScheduledRetry ||
		finalized.Results[1].Outcome != store.OutboxFinalizeFenced {
		t.Fatalf("finalize retry/suffix = %+v err=%v", finalized, err)
	}
	var suffixAttempts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM edge_delivery_outbox_attempts
WHERE item_id = $1 AND lease_fence = $2`, refs[1].ItemID, int64(refs[1].LeaseFence)).Scan(&suffixAttempts); err != nil {
		t.Fatal(err)
	}
	if suffixAttempts != 0 {
		t.Fatalf("suffix live attempts=%d want 0", suffixAttempts)
	}
	fresh, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-b",
		LaneLimit: 1, WindowSize: 2, WindowByteLimit: 1024, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(fresh) != 1 || len(fresh[0].Items) != 2 || fresh[0].LeaseFence == windows[0].LeaseFence {
		t.Fatalf("fresh retry claim = %+v err=%v", fresh, err)
	}
}

func TestDeliveryOutboxLatePhysicalReceiptCannotCrossRetryResolutionPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	user := createTestUser(t, ctx, NewUserStore(pool), "+1887"+randomSuffix(t)+"05", "OutboxReceiptRace", "")
	cleanupOutboxUser(t, pool, user.ID)
	outbox := NewDeliveryOutboxStore(pool)
	if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
		TargetUserID: user.ID, Payload: []byte{0x01}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
	}); err != nil {
		t.Fatal(err)
	}
	windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-a",
		LaneLimit: 1, WindowSize: 1, WindowByteLimit: 1024, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != 1 || len(windows[0].Items) != 1 {
		t.Fatalf("claim = %+v err=%v", windows, err)
	}
	ref := windows[0].Items[0].Ref
	target := store.OutboxAttemptTarget{
		TargetInstanceID: "edge-a", TargetUserID: user.ID,
		BatchID: testOutboxID(30), CommandID: testOutboxID(31),
	}
	if results, err := outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{
		Ref: ref, SourceInstanceID: "egress-a", Targets: []store.OutboxAttemptTarget{target},
	}}); err != nil || results[0].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind = %+v err=%v", results, err)
	}
	lockTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(ctx) }()
	if _, err := lockTx.Exec(ctx, `SELECT 1 FROM edge_delivery_outbox_attempts
WHERE item_id = $1 AND lease_fence = $2 FOR UPDATE`, ref.ItemID, int64(ref.LeaseFence)); err != nil {
		t.Fatal(err)
	}
	type evidenceResult struct {
		results []store.OutboxEvidenceResult
		err     error
	}
	done := make(chan evidenceResult, 1)
	go func() {
		results, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{{
			Ref: ref, Kind: store.OutboxEvidenceEdgeWritten, SourceInstanceID: "egress-a",
			TargetInstanceID: target.TargetInstanceID, TargetUserID: target.TargetUserID,
			BatchID: target.BatchID, CommandID: target.CommandID,
			EligibleSessions: 1, WrittenSessions: 1, ServerMsgID: 304, ObservedAt: time.Now(),
		}})
		done <- evidenceResult{results: results, err: err}
	}()
	time.Sleep(50 * time.Millisecond)
	lockedStore := NewDeliveryOutboxStore(lockTx)
	resolved, err := lockedStore.ResolveOwnedAttemptBatch(ctx, []store.OutboxOwnedAttemptResolution{{
		Ref: ref, Owner: windows[0].Owner, Kind: store.OutboxResolutionRetry,
		RetryDelay: time.Millisecond, LastError: "receipt deadline",
	}})
	if err != nil || resolved[0].Outcome != store.OutboxResolutionRecorded {
		t.Fatalf("resolve under attempt lock = %+v err=%v", resolved, err)
	}
	if err := lockTx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		if result.err != nil || result.results[0].Outcome != store.OutboxEvidenceRejected {
			t.Fatalf("late physical evidence = %+v err=%v, want rejected", result.results, result.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("late physical evidence remained blocked")
	}
	if early, err := outbox.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{Ref: ref}}); err != nil ||
		early.Results[0].Outcome != store.OutboxFinalizeWaitingForPredecessor || len(early.Next) != 0 {
		t.Fatalf("retry crossed frozen evidence deadline = %+v err=%v", early, err)
	}
	var physicalRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM edge_delivery_outbox_attempt_targets
WHERE item_id = $1 AND lease_fence = $2 AND evidence_kind IS NOT NULL`,
		ref.ItemID, int64(ref.LeaseFence)).Scan(&physicalRows); err != nil {
		t.Fatal(err)
	}
	if physicalRows != 0 {
		t.Fatalf("late physical evidence rows=%d want=0", physicalRows)
	}
}

func TestDeliveryOutboxFenceNeverReusesEmptyLaneGenerationPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	user := createTestUser(t, ctx, NewUserStore(pool), "+1887"+randomSuffix(t)+"06", "OutboxFenceGeneration", "")
	cleanupOutboxUser(t, pool, user.ID)
	outbox := NewDeliveryOutboxStore(pool)
	enqueueAndClaim := func(payload byte, owner string) store.OutboxAttemptRef {
		t.Helper()
		if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
			TargetUserID: user.ID, Payload: []byte{payload}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		}); err != nil {
			t.Fatal(err)
		}
		windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
			QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: owner,
			LaneLimit: 1, WindowSize: 1, WindowByteLimit: 1024, LeaseDuration: time.Minute,
			PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
		})
		if err != nil || len(windows) != 1 || len(windows[0].Items) != 1 {
			t.Fatalf("claim generation = %+v err=%v", windows, err)
		}
		return windows[0].Items[0].Ref
	}
	first := enqueueAndClaim(1, "egress-a")
	if results, err := outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{
		Ref: first, SourceInstanceID: "egress-a", Targets: []store.OutboxAttemptTarget{},
	}}); err != nil || results[0].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind first = %+v err=%v", results, err)
	}
	if batch, err := outbox.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{Ref: first}}); err != nil ||
		batch.Results[0].Outcome != store.OutboxFinalizeApplied {
		t.Fatalf("finalize first = %+v err=%v", batch, err)
	}
	second := enqueueAndClaim(2, "egress-b")
	if second.ItemID == first.ItemID || second.LeaseFence <= first.LeaseFence {
		t.Fatalf("lane generation reused fence: first=%+v second=%+v", first, second)
	}
	if batch, err := outbox.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{Ref: first}}); err != nil ||
		batch.Results[0].Outcome != store.OutboxFinalizeFenced {
		t.Fatalf("old live attempt against new generation = %+v err=%v", batch, err)
	}
	var currentFence int64
	if err := pool.QueryRow(ctx, `SELECT lease_fence FROM edge_delivery_outbox_lanes WHERE stream_id = $1`, user.ID).Scan(&currentFence); err != nil {
		t.Fatal(err)
	}
	if uint64(currentFence) != second.LeaseFence {
		t.Fatalf("stale finalize changed new generation fence=%d want=%d", currentFence, second.LeaseFence)
	}
}

func TestDeliveryOutboxByteBudgetPreservesContiguousWindowAndHandoffPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	user := createTestUser(t, ctx, NewUserStore(pool), "+1887"+randomSuffix(t)+"07", "OutboxByteBudget", "")
	cleanupOutboxUser(t, pool, user.ID)
	outbox := NewDeliveryOutboxStore(pool)
	for _, size := range []int{600, 600, 100} {
		if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
			TargetUserID: user.ID, Payload: make([]byte, size), RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		}); err != nil {
			t.Fatal(err)
		}
	}
	windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-budget",
		LaneLimit: 1, WindowSize: 3, WindowByteLimit: 1000, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != 1 || len(windows[0].Items) != 1 {
		t.Fatalf("initial byte window = %+v err=%v, want head only", windows, err)
	}
	ref := windows[0].Items[0].Ref
	if results, err := outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{
		Ref: ref, SourceInstanceID: "egress-budget", Targets: []store.OutboxAttemptTarget{},
	}}); err != nil || results[0].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind head = %+v err=%v", results, err)
	}
	batch, err := outbox.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{
		Ref: ref, Owner: windows[0].Owner, RetainLease: true,
		WindowSize: 3, WindowByteLimit: 1000, LeaseDuration: time.Minute,
	}})
	if err != nil || batch.Results[0].Outcome != store.OutboxFinalizeApplied ||
		len(batch.Next) != 1 || len(batch.Next[0].Items) != 2 {
		t.Fatalf("byte-budget handoff = %+v err=%v", batch, err)
	}
	if got := len(batch.Next[0].Items[0].Payload.(store.AbsoluteDeliveryPayload).TL) +
		len(batch.Next[0].Items[1].Payload.(store.AbsoluteDeliveryPayload).TL); got != 700 {
		t.Fatalf("handoff bytes=%d want=700", got)
	}
}

func TestDeliveryOutboxOversizedWindowHeadClaimsAlonePostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	user := createTestUser(t, ctx, NewUserStore(pool), "+1887"+randomSuffix(t)+"08", "OutboxOversizedHead", "")
	cleanupOutboxUser(t, pool, user.ID)
	outbox := NewDeliveryOutboxStore(pool)
	for _, size := range []int{2048, 100} {
		if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
			TargetUserID: user.ID, Payload: make([]byte, size), RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		}); err != nil {
			t.Fatal(err)
		}
	}
	windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-oversized-head",
		LaneLimit: 1, WindowSize: 16, WindowByteLimit: 1024, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != 1 || len(windows[0].Items) != 1 {
		t.Fatalf("oversized head claim = %+v err=%v, want one-item window", windows, err)
	}
	if got := len(windows[0].Items[0].Payload.(store.AbsoluteDeliveryPayload).TL); got != 2048 {
		t.Fatalf("oversized head payload bytes = %d", got)
	}
	if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
		TargetUserID: user.ID, Payload: make([]byte, store.MaxDeliveryBatchBytes+1),
		RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
	}); err == nil {
		t.Fatal("payload above the hard delivery boundary was accepted")
	}
}

func TestDeliveryOutboxClaimsOnlyContiguousExclusionPrefixPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	user := createTestUser(t, ctx, NewUserStore(pool), "+1887"+randomSuffix(t)+"09", "OutboxExclusionPrefix", "")
	cleanupOutboxUser(t, pool, user.ID)
	outbox := NewDeliveryOutboxStore(pool)
	const itemCount = 32
	for i := 0; i < itemCount; i++ {
		input := store.DeliveryOutboxEnqueue{
			TargetUserID: user.ID, Payload: []byte{byte(i + 1)},
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
		windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
			QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: fmt.Sprintf("egress-exclusion-%d", i),
			LaneLimit: 1, WindowSize: itemCount, WindowByteLimit: 1 << 20, LeaseDuration: time.Minute,
			PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
		})
		if err != nil || len(windows) != 1 || len(windows[0].Items) != 1 {
			t.Fatalf("claim %d crossed exclusion boundary: %+v err=%v", i, windows, err)
		}
		ref := windows[0].Items[0].Ref
		if ref.Attempt != 1 {
			t.Fatalf("item %d attempt = %d, want 1", i, ref.Attempt)
		}
		if results, err := outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{
			Ref: ref, SourceInstanceID: "egress-exclusion", Targets: []store.OutboxAttemptTarget{},
		}}); err != nil || results[0].Outcome != store.OutboxBindTargetBound {
			t.Fatalf("bind item %d = %+v err=%v", i, results, err)
		}
		finalized, err := outbox.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{Ref: ref}})
		if err != nil || finalized.Results[0].Outcome != store.OutboxFinalizeApplied {
			t.Fatalf("finalize item %d = %+v err=%v", i, finalized, err)
		}
	}
}

func TestDeliveryOutboxFinalizationDeletesLiveAttemptsPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	user := createTestUser(t, ctx, NewUserStore(pool), "+1887"+randomSuffix(t)+"10", "OutboxAttemptRetention", "")
	cleanupOutboxUser(t, pool, user.ID)
	outbox := NewDeliveryOutboxStore(pool)
	for i := 0; i < 3; i++ {
		if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
			TargetUserID: user.ID, Payload: []byte{byte(i + 1)}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		}); err != nil {
			t.Fatal(err)
		}
	}
	windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-retention",
		LaneLimit: 1, WindowSize: 3, WindowByteLimit: 1024, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != 1 || len(windows[0].Items) != 3 {
		t.Fatalf("claim retention window = %+v err=%v", windows, err)
	}
	refs := []store.OutboxAttemptRef{windows[0].Items[0].Ref, windows[0].Items[1].Ref, windows[0].Items[2].Ref}
	target := store.OutboxAttemptTarget{
		TargetInstanceID: "edge-retention", TargetUserID: user.ID,
		BatchID: testOutboxID(81), CommandID: testOutboxID(82),
	}
	sets := []store.OutboxAttemptTargetSet{
		{Ref: refs[0], SourceInstanceID: "egress-retention", Targets: []store.OutboxAttemptTarget{target}},
		{Ref: refs[1], SourceInstanceID: "egress-retention", Targets: []store.OutboxAttemptTarget{}},
		{Ref: refs[2], SourceInstanceID: "egress-retention", Targets: []store.OutboxAttemptTarget{}},
	}
	if bound, err := outbox.BindAttemptTargets(ctx, sets); err != nil || len(bound) != 3 {
		t.Fatalf("bind retention window = %+v err=%v", bound, err)
	}
	physical := store.OutboxAttemptEvidence{
		Ref: refs[0], Kind: store.OutboxEvidenceEdgeWritten, SourceInstanceID: "egress-retention",
		TargetInstanceID: target.TargetInstanceID, TargetUserID: target.TargetUserID,
		BatchID: target.BatchID, CommandID: target.CommandID,
		EligibleSessions: 1, WrittenSessions: 1, ServerMsgID: 7601, ObservedAt: time.Now(),
	}
	evidence := []store.OutboxAttemptEvidence{
		physical,
		{Ref: refs[1], Kind: store.OutboxEvidenceAuthoritativeNoTargets, SourceInstanceID: "egress-retention", ObservedAt: time.Now()},
		{Ref: refs[2], Kind: store.OutboxEvidenceAuthoritativeNoTargets, SourceInstanceID: "egress-retention", ObservedAt: time.Now()},
	}
	if recorded, err := outbox.RecordAttemptEvidenceBatch(ctx, evidence); err != nil || len(recorded) != 3 {
		t.Fatalf("record retention evidence = %+v err=%v", recorded, err)
	}
	requests := []store.OutboxFinalizeRequest{{Ref: refs[0]}, {Ref: refs[1]}, {Ref: refs[2]}}
	if finalized, err := outbox.FinalizeAttempts(ctx, requests); err != nil || len(finalized.Results) != 3 {
		t.Fatalf("finalize retention window = %+v err=%v", finalized, err)
	}
	late, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{physical})
	if err != nil || (late[0].Outcome != store.OutboxEvidenceFenced && late[0].Outcome != store.OutboxEvidenceRejected) {
		t.Fatalf("late physical evidence after live attempt deletion = %+v err=%v", late, err)
	}
	var targetRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM edge_delivery_outbox_attempt_targets WHERE item_id = $1 AND lease_fence = $2`,
		refs[0].ItemID, int64(refs[0].LeaseFence)).Scan(&targetRows); err != nil || targetRows != 0 {
		t.Fatalf("cascaded target rows = %d err=%v", targetRows, err)
	}
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM edge_delivery_outbox_attempts WHERE stream_id = $1`, user.ID).Scan(&attempts); err != nil || attempts != 0 {
		t.Fatalf("live attempts after finalization = %d err=%v", attempts, err)
	}
}

func cleanupOutboxUser(t *testing.T, pool *pgxpool.Pool, userID int64) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = pool.Exec(ctx, `DELETE FROM edge_delivery_outbox_attempts WHERE stream_id = $1`, userID)
		_, _ = pool.Exec(ctx, `DELETE FROM edge_delivery_outbox_lanes WHERE stream_id = $1`, userID)
		_, _ = pool.Exec(ctx, `DELETE FROM edge_delivery_outbox WHERE target_user_id = $1`, userID)
		_, _ = pool.Exec(ctx, `DELETE FROM users WHERE id = $1`, userID)
	})
}

func testOutboxID(last byte) [16]byte {
	var out [16]byte
	out[15] = last
	return out
}
