package postgres

import (
	"context"
	"testing"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/observability/dbtrace"
	storepkg "telesrv/internal/store"
)

func TestLockDispatchOutboxAppendFencesUsesConsumerGlobalOrderPostgres(t *testing.T) {
	pool := testPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const lowStreamID int64 = 8_802_508_100_001
	const highStreamID int64 = 8_802_508_100_002

	consumer, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Rollback(context.Background())
	if _, err := consumer.Exec(ctx, `
SELECT pg_advisory_xact_lock(outbox_lane_advisory_key('account_pts', $1))`, lowStreamID); err != nil {
		t.Fatalf("lock consumer low stream: %v", err)
	}

	producer, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Rollback(context.Background())
	var producerPID int
	if err := producer.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&producerPID); err != nil {
		t.Fatal(err)
	}
	locked := make(chan error, 1)
	// Reverse input is deliberate. The producer must wait on low before taking
	// high; otherwise a consumer holding low and waiting on high can deadlock it.
	go func() {
		locked <- lockDispatchOutboxAppendFences(ctx, producer, []int64{highStreamID, lowStreamID})
	}()

	waiting := false
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
		if err := pool.QueryRow(ctx, `
SELECT COALESCE((
  SELECT wait_event_type = 'Lock' AND wait_event = 'advisory'
  FROM pg_stat_activity
  WHERE pid = $1
), false)`, producerPID).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !waiting {
		t.Fatal("producer did not wait on consumer's low-stream fence")
	}

	inspector, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var highAvailable bool
	if err := inspector.QueryRow(ctx, `
SELECT pg_try_advisory_xact_lock(outbox_lane_advisory_key('account_pts', $1))`, highStreamID).Scan(&highAvailable); err != nil {
		_ = inspector.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := inspector.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if !highAvailable {
		t.Fatal("producer acquired high stream before blocked low stream; global lane order is broken")
	}

	if err := consumer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-locked; err != nil {
		t.Fatalf("producer append fences: %v", err)
	}
	if err := producer.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSelectPlainPrivateSendBatchPreservesConflictingFIFOAndCombinesDisjointScopes(t *testing.T) {
	messageStore := &MessageStore{}
	task := func(sender, recipient int64) *plainPrivateSendBatchTask {
		return &plainPrivateSendBatchTask{
			ctx: context.Background(), store: messageStore,
			req: domain.SendPrivateTextRequest{SenderUserID: sender, RecipientUserID: recipient},
		}
	}
	first := task(1, 2)
	blockedFollower := task(2, 3)
	disjoint := task(4, 5)
	batch, remaining, canceled := selectPlainPrivateSendBatch(
		[]*plainPrivateSendBatchTask{first, blockedFollower, disjoint},
		map[plainPrivateSendScope]struct{}{},
	)
	if len(canceled) != 0 || len(batch) != 2 || batch[0] != first || batch[1] != disjoint ||
		len(remaining) != 1 || remaining[0] != blockedFollower {
		t.Fatalf("batch=%p/%p remaining=%p canceled=%d", batch[0], batch[1], remaining[0], len(canceled))
	}

	busy := map[plainPrivateSendScope]struct{}{{store: messageStore, userID: 1}: {}}
	batch, remaining, canceled = selectPlainPrivateSendBatch(
		[]*plainPrivateSendBatchTask{first, blockedFollower, disjoint}, busy,
	)
	if len(canceled) != 0 || len(batch) != 1 || batch[0] != disjoint ||
		len(remaining) != 2 || remaining[0] != first || remaining[1] != blockedFollower {
		t.Fatalf("busy batch=%v remaining=%v canceled=%d", batch, remaining, len(canceled))
	}
}

func TestExecutePlainPrivateSendBatchCommitsDisjointMessagesTogetherPostgres(t *testing.T) {
	pool := testPool(t)
	baseCtx := context.Background()
	users := NewUserStore(pool)
	suffix := randomSuffix(t)
	const count = 12
	created := make([]int64, 0, count*2)
	requests := make([]domain.SendPrivateTextRequest, 0, count)
	for i := range count {
		sender := createTestUser(t, baseCtx, users, "+1671"+suffix+batchTestSuffix(i, 0), "BatchSender", "")
		recipient := createTestUser(t, baseCtx, users, "+1671"+suffix+batchTestSuffix(i, 1), "BatchRecipient", "")
		created = append(created, sender.ID, recipient.ID)
		requests = append(requests, domain.SendPrivateTextRequest{
			SenderUserID: sender.ID, RecipientUserID: recipient.ID,
			RandomID: int64(2508251000 + i), Message: "batched private send", Date: 1800001000 + i,
			IdempotencyPreflighted: true,
		})
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(baseCtx, "DELETE FROM users WHERE id = ANY($1::bigint[])", created)
	})
	messageStore := newTestMessageStore(pool, WithMessageAllocators(&perUserCounterAllocator{}))
	ctx, stats := dbtrace.WithStats(baseCtx)
	tasks := make([]*plainPrivateSendBatchTask, len(requests))
	for i, req := range requests {
		fingerprint, err := storepkg.PrivateSendFingerprint(req)
		if err != nil {
			t.Fatal(err)
		}
		tasks[i] = &plainPrivateSendBatchTask{ctx: ctx, store: messageStore, req: req, fingerprint: fingerprint}
	}
	results := executePlainPrivateSendBatch(tasks)
	for i, result := range results {
		if result.err != nil {
			t.Fatalf("result %d: %v", i, result.err)
		}
		if result.result.Duplicate || result.result.SenderMessage.Pts != 1 || result.result.RecipientMessage.Pts != 1 {
			t.Fatalf("result %d = %+v", i, result.result)
		}
	}
	// One allocator call, BEGIN, one ordered account lock, one globally ordered
	// dispatch-lane append fence, one trigger-defer, one set-based logical insert,
	// one set-based durable projection and one COMMIT. The transaction query count
	// is constant regardless of logical batch width.
	wantQueries := int64(7)
	if snapshot := stats.Snapshot(); snapshot.Queries != wantQueries || snapshot.Errors != 0 {
		t.Fatalf("query stats=%+v want queries=%d", snapshot, wantQueries)
	}
	var boxes, events, outbox int
	if err := pool.QueryRow(baseCtx, `
SELECT
  (SELECT count(*) FROM message_boxes WHERE owner_user_id = ANY($1::bigint[])),
  (SELECT count(*) FROM user_update_events WHERE user_id = ANY($1::bigint[])),
  (SELECT count(*) FROM dispatch_outbox WHERE target_user_id = ANY($1::bigint[]))`, created).Scan(&boxes, &events, &outbox); err != nil {
		t.Fatal(err)
	}
	if boxes != count*2 || events != count*2 || outbox != count*2 {
		t.Fatalf("durable rows boxes/events/outbox=%d/%d/%d want=%d", boxes, events, outbox, count*2)
	}
}

func TestExecutePlainPrivateSendBatchPreservesNormalBlockedSelfAndReplayPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users := NewUserStore(pool)
	suffix := randomSuffix(t)
	createdUsers := make([]int64, 0, 6)
	makeUser := func(index int) int64 {
		user := createTestUser(t, ctx, users, "+1672"+suffix+batchTestSuffix(index, 0), "BatchMode", "")
		createdUsers = append(createdUsers, user.ID)
		return user.ID
	}
	normalSender, normalRecipient := makeUser(1), makeUser(2)
	blockedSender, blockedRecipient := makeUser(3), makeUser(4)
	selfUser := makeUser(5)
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", createdUsers)
	})
	messages := newTestMessageStore(pool, WithMessageAllocators(&perUserCounterAllocator{}))
	requests := []domain.SendPrivateTextRequest{
		{SenderUserID: normalSender, RecipientUserID: normalRecipient, RandomID: 2508252001, Message: "normal", Date: 1800002001, IdempotencyPreflighted: true},
		{SenderUserID: blockedSender, RecipientUserID: blockedRecipient, RandomID: 2508252002, Message: "blocked", Date: 1800002002, RecipientBlocked: true, IdempotencyPreflighted: true},
		{SenderUserID: selfUser, RecipientUserID: selfUser, RandomID: 2508252003, Message: "self", Date: 1800002003, IdempotencyPreflighted: true},
	}
	makeTasks := func(reqs []domain.SendPrivateTextRequest) []*plainPrivateSendBatchTask {
		tasks := make([]*plainPrivateSendBatchTask, len(reqs))
		for i, req := range reqs {
			fingerprint, err := storepkg.PrivateSendFingerprint(req)
			if err != nil {
				t.Fatal(err)
			}
			tasks[i] = &plainPrivateSendBatchTask{ctx: ctx, store: messages, req: req, fingerprint: fingerprint}
		}
		return tasks
	}
	results := executePlainPrivateSendBatch(makeTasks(requests))
	for i, result := range results {
		if result.err != nil || result.result.Duplicate {
			t.Fatalf("first result %d = %+v error=%v", i, result.result, result.err)
		}
	}
	if results[0].result.RecipientMessage.ID <= 0 || results[0].result.RecipientMessage.Pts <= 0 {
		t.Fatalf("normal recipient = %+v", results[0].result.RecipientMessage)
	}
	if results[1].result.RecipientMessage.ID != 0 || results[1].result.RecipientEvent.Pts != 0 {
		t.Fatalf("blocked recipient leaked durable projection: %+v", results[1].result)
	}
	if results[2].result.RecipientMessage.ID != results[2].result.SenderMessage.ID ||
		results[2].result.RecipientMessage.Pts != results[2].result.SenderMessage.Pts ||
		results[2].result.RecipientMessage.OwnerUserID != results[2].result.SenderMessage.OwnerUserID {
		t.Fatalf("self projection differs sender=%+v recipient=%+v", results[2].result.SenderMessage, results[2].result.RecipientMessage)
	}

	replay := executePlainPrivateSendBatch(makeTasks(requests[:1]))
	if len(replay) != 1 || replay[0].err != nil || !replay[0].result.Duplicate ||
		replay[0].result.SenderMessage.ID != results[0].result.SenderMessage.ID ||
		replay[0].result.SenderMessage.Pts != results[0].result.SenderMessage.Pts {
		t.Fatalf("replay = %+v first=%+v", replay, results[0])
	}

	var boxes, events, outbox int
	if err := pool.QueryRow(ctx, `
SELECT
  (SELECT count(*) FROM message_boxes WHERE owner_user_id = ANY($1::bigint[])),
  (SELECT count(*) FROM user_update_events WHERE user_id = ANY($1::bigint[])),
  (SELECT count(*) FROM dispatch_outbox WHERE target_user_id = ANY($1::bigint[]))`, createdUsers).Scan(&boxes, &events, &outbox); err != nil {
		t.Fatal(err)
	}
	if boxes != 4 || events != 4 || outbox != 4 {
		t.Fatalf("durable rows boxes/events/outbox=%d/%d/%d want=4/4/4", boxes, events, outbox)
	}
}

func batchTestSuffix(index, side int) string {
	return string([]byte{
		byte('0' + (index/10)%10),
		byte('0' + index%10),
		byte('0' + side),
	})
}
