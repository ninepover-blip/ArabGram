package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestDispatchOutboxFinalizeBatchesIndependentLanesIntoBoundedTransactionsPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	const laneCount = outboxFinalizeTransactionLanes + 1
	userIDs := make([]int64, 0, laneCount)
	users := NewUserStore(pool)
	events := NewUpdateEventStore(pool)
	for i := 0; i < laneCount; i++ {
		user := createTestUser(t, ctx, users, fmt.Sprintf("+1883%s%02d", randomSuffix(t), i), "DispatchFinalizeBatch", "")
		userIDs = append(userIDs, user.ID)
		cleanupDispatchUser(t, pool, user.ID)
		for eventIndex := 0; eventIndex < 2; eventIndex++ {
			if _, err := events.AppendAllocatedWithDispatch(ctx, user.ID, domain.UpdateEvent{
				Type: domain.UpdateEventDialogPinned, PtsCount: 1, Date: 1700001900 + eventIndex,
				Peer: domain.Peer{Type: domain.PeerTypeUser, ID: user.ID}, Bool: eventIndex == 0,
			}, [8]byte{}, 0); err != nil {
				t.Fatalf("append user %d event %d: %v", user.ID, eventIndex, err)
			}
		}
	}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		for _, userID := range userIDs {
			cleanupDispatchUser(t, pool, userID)
		}
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM users WHERE id = ANY($1::bigint[])`, userIDs)
	})

	countingDB := &countingBeginDB{Pool: pool}
	outbox := NewDispatchOutboxStore(countingDB, WithLeaseTimeout(time.Minute))
	windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueDispatchPTS, Owner: "egress-dispatch-finalize-batch",
		LaneLimit: laneCount, WindowSize: 1, WindowByteLimit: ptsEventByteEstimate,
		LeaseDuration: time.Minute, PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != laneCount {
		t.Fatalf("claim lanes = %d err=%v, want %d", len(windows), err, laneCount)
	}
	sets := make([]store.OutboxAttemptTargetSet, 0, laneCount)
	evidence := make([]store.OutboxAttemptEvidence, 0, laneCount)
	finalize := make([]store.OutboxFinalizeRequest, 0, laneCount)
	for _, window := range windows {
		if len(window.Items) != 1 || window.Items[0].Ref.Sequence != 1 {
			t.Fatalf("lane %d items=%+v, want one PTS=1 item", window.StreamID, window.Items)
		}
		ref := window.Items[0].Ref
		sets = append(sets, store.OutboxAttemptTargetSet{
			Ref: ref, SourceInstanceID: "egress-dispatch-finalize-batch-source", Targets: []store.OutboxAttemptTarget{},
		})
		evidence = append(evidence, store.OutboxAttemptEvidence{
			Ref: ref, Kind: store.OutboxEvidenceAuthoritativeNoTargets,
			SourceInstanceID: "egress-dispatch-finalize-batch-source", ObservedAt: time.Now(),
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
	for _, userID := range userIDs {
		var headPTS int64
		if err := pool.QueryRow(ctx, `SELECT head_sequence FROM dispatch_outbox_lanes WHERE stream_id = $1`, userID).Scan(&headPTS); err != nil {
			t.Fatal(err)
		}
		if headPTS != 2 {
			t.Fatalf("user %d head pts=%d want=2", userID, headPTS)
		}
	}
}

func TestDispatchOutboxPTSWindowIsStrictlyContiguousPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	user := createTestUser(t, ctx, NewUserStore(pool), "+1884"+randomSuffix(t)+"01", "DispatchWindow", "")
	cleanupDispatchUser(t, pool, user.ID)
	events := NewUpdateEventStore(pool)
	for i := 0; i < 3; i++ {
		if _, err := events.AppendAllocatedWithDispatch(ctx, user.ID, domain.UpdateEvent{
			Type: domain.UpdateEventDialogPinned, PtsCount: 1, Date: 1700002000 + i,
			Peer: domain.Peer{Type: domain.PeerTypeUser, ID: user.ID}, Bool: true,
		}, [8]byte{}, 0); err != nil {
			t.Fatalf("append event %d: %v", i, err)
		}
	}
	outbox := NewDispatchOutboxStore(pool, WithLeaseTimeout(time.Minute))
	shard := int(user.ID % store.DispatchOutboxLogicalShards)
	wrongShard := (shard + 1) % store.DispatchOutboxLogicalShards
	wrong, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueDispatchPTS, Owner: "egress-a/worker-1",
		LogicalShardCount: store.DispatchOutboxLogicalShards, LogicalShardIDs: []int{wrongShard},
		LaneLimit: 1, WindowSize: 3, WindowByteLimit: 3 * ptsEventByteEstimate, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(wrong) != 0 {
		t.Fatalf("wrong shard claim = %+v err=%v", wrong, err)
	}
	windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueDispatchPTS, Owner: "egress-a/worker-1",
		LogicalShardCount: store.DispatchOutboxLogicalShards, LogicalShardIDs: []int{shard},
		LaneLimit: 1, WindowSize: 3, WindowByteLimit: 3 * ptsEventByteEstimate, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != 1 || len(windows[0].Items) != 3 {
		t.Fatalf("claim = %+v err=%v", windows, err)
	}
	refs := make([]store.OutboxAttemptRef, 3)
	sets := make([]store.OutboxAttemptTargetSet, 3)
	target := store.OutboxAttemptTarget{
		TargetInstanceID: "edge-a", TargetUserID: user.ID,
		BatchID: [16]byte{11}, CommandID: [16]byte{12},
	}
	for i, item := range windows[0].Items {
		refs[i] = item.Ref
		if item.Ref.Sequence != int64(i+1) {
			t.Fatalf("item %d sequence=%d want=%d", i, item.Ref.Sequence, i+1)
		}
		sets[i] = store.OutboxAttemptTargetSet{Ref: item.Ref, SourceInstanceID: "egress-a"}
		if i == 1 {
			sets[i].Targets = []store.OutboxAttemptTarget{}
		} else {
			sets[i].Targets = []store.OutboxAttemptTarget{target}
		}
	}
	if results, err := outbox.BindAttemptTargets(ctx, sets); err != nil ||
		results[0].Outcome != store.OutboxBindTargetBound ||
		results[1].Outcome != store.OutboxBindTargetBound ||
		results[2].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind = %+v err=%v", results, err)
	}
	if batch, err := outbox.FinalizeAttempts(ctx, []store.OutboxFinalizeRequest{{Ref: refs[1]}}); err != nil ||
		batch.Results[0].Outcome != store.OutboxFinalizeWaitingForPredecessor {
		t.Fatalf("gap finalize = %+v err=%v", batch, err)
	}
	if results, err := outbox.RecordAttemptEvidenceBatch(ctx, []store.OutboxAttemptEvidence{{
		Ref: refs[0], Kind: store.OutboxEvidenceEdgeWritten,
		SourceInstanceID: "egress-a", TargetInstanceID: target.TargetInstanceID,
		TargetUserID: target.TargetUserID, BatchID: target.BatchID, CommandID: target.CommandID,
		EligibleSessions: 1, WrittenSessions: 1, ServerMsgID: 101, ObservedAt: time.Now(),
	}}); err != nil || results[0].Outcome != store.OutboxEvidenceRecorded {
		t.Fatalf("record first = %+v err=%v", results, err)
	}
	requests, err := outbox.RecoverFinalizableAttempts(ctx, store.OutboxRecoverFinalizableRequest{
		QueueKind: store.OutboxQueueDispatchPTS, LaneLimit: 1, AttemptLimit: store.MaxDeliveryBatchItems,
	})
	if err != nil || len(requests) != 2 {
		t.Fatalf("recover finalizable = %+v err=%v", requests, err)
	}
	batch, err := outbox.FinalizeAttempts(ctx, requests)
	if err != nil || batch.Results[0].Outcome != store.OutboxFinalizeApplied || batch.Results[1].Outcome != store.OutboxFinalizeApplied {
		t.Fatalf("finalize prefix = %+v err=%v", batch, err)
	}
	var headPTS int64
	if err := pool.QueryRow(ctx, `SELECT head_sequence FROM dispatch_outbox_lanes WHERE stream_id = $1`, user.ID).Scan(&headPTS); err != nil {
		t.Fatal(err)
	}
	if headPTS != 3 {
		t.Fatalf("head pts=%d want=3", headPTS)
	}
}

func TestDispatchOutboxTenThousandAppendsDoNotLockActiveLanePostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	user := createTestUser(t, ctx, NewUserStore(pool), "+1884"+randomSuffix(t)+"02", "DispatchAppend", "")
	cleanupDispatchUser(t, pool, user.ID)
	if _, err := pool.Exec(ctx, `
INSERT INTO user_update_events (user_id, pts, pts_count, date, event_type)
SELECT $1, n, 1, 1700002100, 'noop'
FROM generate_series(1, 10001) AS n`, user.ID); err != nil {
		t.Fatalf("insert events: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO dispatch_outbox (target_user_id, pts, event_type)
VALUES ($1, 1, 'noop')`, user.ID); err != nil {
		t.Fatalf("insert head: %v", err)
	}
	lockTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lockTx.Rollback(ctx) }()
	if _, err := lockTx.Exec(ctx, `SELECT 1 FROM dispatch_outbox_lanes WHERE stream_id = $1 FOR UPDATE`, user.ID); err != nil {
		t.Fatal(err)
	}
	appendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := pool.Exec(appendCtx, `
INSERT INTO dispatch_outbox (target_user_id, pts, event_type)
SELECT $1, n, 'noop' FROM generate_series(2, 10001) AS n`, user.ID); err != nil {
		t.Fatalf("10k append waited on active lane: %v", err)
	}
	var headPTS, count int64
	if err := pool.QueryRow(ctx, `SELECT head_sequence FROM dispatch_outbox_lanes WHERE stream_id = $1`, user.ID).Scan(&headPTS); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM dispatch_outbox WHERE target_user_id = $1`, user.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if headPTS != 1 || count != 10001 {
		t.Fatalf("head pts=%d rows=%d want 1/10001", headPTS, count)
	}
}

func TestDispatchOutboxAppendDoesNotWaitForUncommittedActiveLaneUpdatePostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	user := createTestUser(t, ctx, NewUserStore(pool), "+1884"+randomSuffix(t)+"07", "DispatchUpdatedLane", "")
	cleanupDispatchUser(t, pool, user.ID)
	if _, err := pool.Exec(ctx, `
INSERT INTO user_update_events (user_id, pts, pts_count, date, event_type)
VALUES ($1, 1, 1, 1700002700, 'noop'), ($1, 2, 1, 1700002701, 'noop')`, user.ID); err != nil {
		t.Fatalf("insert events: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO dispatch_outbox (target_user_id, pts, event_type)
VALUES ($1, 1, 'noop')`, user.ID); err != nil {
		t.Fatalf("insert head: %v", err)
	}

	consumer, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = consumer.Rollback(context.Background()) }()
	if _, err := consumer.Exec(ctx, `
UPDATE dispatch_outbox_lanes
SET lease_owner = 'uncommitted-consumer', updated_at = clock_timestamp()
WHERE stream_id = $1`, user.ID); err != nil {
		t.Fatalf("update active lane: %v", err)
	}

	appendCtx, cancelAppend := context.WithTimeout(ctx, time.Second)
	defer cancelAppend()
	appendDone := make(chan error, 1)
	go func() {
		_, err := pool.Exec(appendCtx, `
INSERT INTO dispatch_outbox (target_user_id, pts, event_type)
VALUES ($1, 2, 'noop')`, user.ID)
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

	var headPTS, count int64
	if err := pool.QueryRow(ctx, `SELECT head_sequence FROM dispatch_outbox_lanes WHERE stream_id = $1`, user.ID).Scan(&headPTS); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM dispatch_outbox WHERE target_user_id = $1`, user.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if headPTS != 1 || count != 2 {
		t.Fatalf("head pts=%d rows=%d want 1/2", headPTS, count)
	}
}

func TestDispatchOutboxFinalizeWaitsForUncommittedAppendMarkerPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	user := createTestUser(t, ctx, NewUserStore(pool), "+1884"+randomSuffix(t)+"05", "DispatchCommitFence", "")
	cleanupDispatchUser(t, pool, user.ID)
	events := NewUpdateEventStore(pool)
	if _, err := events.AppendAllocatedWithDispatch(ctx, user.ID, domain.UpdateEvent{
		Type: domain.UpdateEventDialogPinned, PtsCount: 1, Date: 1700002500,
		Peer: domain.Peer{Type: domain.PeerTypeUser, ID: user.ID}, Bool: true,
	}, [8]byte{}, 0); err != nil {
		t.Fatal(err)
	}
	outbox := NewDispatchOutboxStore(pool)
	windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueDispatchPTS, Owner: "egress-dispatch-commit-fence",
		LaneLimit: 1, WindowSize: 1, WindowByteLimit: ptsEventByteEstimate, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != 1 || len(windows[0].Items) != 1 {
		t.Fatalf("claim = %+v err=%v", windows, err)
	}
	ref := windows[0].Items[0].Ref
	if results, err := outbox.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{
		Ref: ref, SourceInstanceID: "egress-dispatch-commit-fence", Targets: []store.OutboxAttemptTarget{},
	}}); err != nil || results[0].Outcome != store.OutboxBindTargetBound {
		t.Fatalf("bind = %+v err=%v", results, err)
	}

	producer, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = producer.Rollback(ctx) }()
	if _, err := producer.Exec(ctx, `
INSERT INTO user_update_events (user_id, pts, pts_count, date, event_type)
VALUES ($1, 2, 1, 1700002501, 'noop')`, user.ID); err != nil {
		t.Fatalf("insert uncommitted event: %v", err)
	}
	var successorID int64
	if err := producer.QueryRow(ctx, `
INSERT INTO dispatch_outbox (target_user_id, pts, event_type)
VALUES ($1, 2, 'noop')
RETURNING id`, user.ID).Scan(&successorID); err != nil {
		t.Fatalf("insert uncommitted dispatch: %v", err)
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
	var headID, headPTS int64
	if err := pool.QueryRow(ctx, `
SELECT head_item_id, head_sequence
FROM dispatch_outbox_lanes
WHERE stream_id = $1`, user.ID).Scan(&headID, &headPTS); err != nil {
		t.Fatal(err)
	}
	if headID != successorID || headPTS != 2 {
		t.Fatalf("head=%d/%d want successor=%d/2", headID, headPTS, successorID)
	}
}

func TestDispatchOutboxPTSWindowHonorsConservativeByteBudgetPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	user := createTestUser(t, ctx, NewUserStore(pool), "+1884"+randomSuffix(t)+"03", "DispatchBudget", "")
	cleanupDispatchUser(t, pool, user.ID)
	for pts := 1; pts <= 3; pts++ {
		if _, err := pool.Exec(ctx, `INSERT INTO user_update_events (user_id, pts, pts_count, date, event_type)
VALUES ($1, $2, 1, 1700002200, 'noop')`, user.ID, pts); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO dispatch_outbox (target_user_id, pts, event_type)
VALUES ($1, $2, 'noop')`, user.ID, pts); err != nil {
			t.Fatal(err)
		}
	}
	outbox := NewDispatchOutboxStore(pool)
	windows, err := outbox.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueDispatchPTS, Owner: "egress-budget",
		LaneLimit: 1, WindowSize: 3, WindowByteLimit: 1, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != 1 || len(windows[0].Items) != 1 || windows[0].Items[0].Ref.Sequence != 1 {
		t.Fatalf("one-byte conservative claim = %+v err=%v, want head only", windows, err)
	}
}

func cleanupDispatchUser(t *testing.T, pool *pgxpool.Pool, userID int64) {
	t.Helper()
	t.Cleanup(func() {
		ctx := context.Background()
		deleteDispatchOutboxStateForUser(t, ctx, pool, userID)
		_, _ = pool.Exec(ctx, `DELETE FROM users WHERE id = $1`, userID)
	})
}

func deleteDispatchOutboxStateForUser(t *testing.T, ctx context.Context, pool *pgxpool.Pool, userID int64) {
	t.Helper()
	for _, statement := range []string{
		`DELETE FROM dispatch_outbox_attempts WHERE stream_id = $1`,
		`DELETE FROM dispatch_outbox_lanes WHERE stream_id = $1`,
		`DELETE FROM dispatch_outbox WHERE target_user_id = $1`,
	} {
		if _, err := pool.Exec(ctx, statement, userID); err != nil {
			t.Fatalf("delete dispatch outbox state for user %d: %v", userID, err)
		}
	}
}
