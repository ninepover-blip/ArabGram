package postgres

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/observability/dbtrace"
	"telesrv/internal/store"
)

func TestPlainPrivateSendHotPathDurableFactsAndQueryCount(t *testing.T) {
	pool := testPool(t)
	baseCtx := context.Background()
	users := NewUserStore(pool)
	suffix := randomSuffix(t)
	sender := createTestUser(t, baseCtx, users, "+1667"+suffix+"01", "HotSender", "")
	recipient := createTestUser(t, baseCtx, users, "+1667"+suffix+"02", "HotRecipient", "")
	t.Cleanup(func() {
		_, _ = pool.Exec(baseCtx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{sender.ID, recipient.ID})
	})
	allocator := &perUserCounterAllocator{}
	messages := newTestMessageStore(pool, WithMessageAllocators(allocator))
	if _, err := messages.SendPrivateText(baseCtx, domain.SendPrivateTextRequest{
		SenderUserID: sender.ID, RecipientUserID: sender.ID,
		RandomID: 240824000, Message: "seed sender pts", Date: 1800000000,
	}); err != nil {
		t.Fatalf("seed sender pts: %v", err)
	}
	deleteDispatchOutboxStateForUser(t, baseCtx, pool, sender.ID)

	ctx, stats := dbtrace.WithStats(baseCtx)
	highBitAuthKeyID := [8]byte{0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x80}
	result, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:           sender.ID,
		RecipientUserID:        recipient.ID,
		RandomID:               240824001,
		Message:                "plain hot path",
		Date:                   1800000001,
		IdempotencyPreflighted: true,
		OriginAuthKeyID:        highBitAuthKeyID,
		OriginSessionID:        240824,
	})
	if err != nil {
		t.Fatalf("SendPrivateText: %v", err)
	}
	rows, err := pool.Query(baseCtx, `
SELECT target_user_id, exclude_auth_key_id, exclude_session_id,
       read_model_peer_type, read_model_peer_id
FROM dispatch_outbox
WHERE target_user_id = ANY($1::bigint[])
ORDER BY target_user_id`, []int64{sender.ID, recipient.ID})
	if err != nil {
		t.Fatalf("load hot-path dispatch exclusions: %v", err)
	}
	defer rows.Close()
	seen := 0
	for rows.Next() {
		var userID, sessionID, readModelPeerID int64
		var raw []byte
		var readModelPeerType string
		if err := rows.Scan(&userID, &raw, &sessionID, &readModelPeerType, &readModelPeerID); err != nil {
			t.Fatalf("scan hot-path dispatch exclusion: %v", err)
		}
		if userID == sender.ID {
			if string(raw) != string(highBitAuthKeyID[:]) || sessionID != 240824 {
				t.Fatalf("sender exclusion = %x/%d, want exact %x/240824", raw, sessionID, highBitAuthKeyID)
			}
			if readModelPeerType != "user" || readModelPeerID != recipient.ID {
				t.Fatalf("sender read-model peer = %s/%d, want user/%d", readModelPeerType, readModelPeerID, recipient.ID)
			}
		} else if userID == recipient.ID {
			if string(raw) != string(make([]byte, 8)) || sessionID != 0 {
				t.Fatalf("recipient exclusion = %x/%d, want zero", raw, sessionID)
			}
			if readModelPeerType != "user" || readModelPeerID != sender.ID {
				t.Fatalf("recipient read-model peer = %s/%d, want user/%d", readModelPeerType, readModelPeerID, sender.ID)
			}
		} else {
			t.Fatalf("unexpected dispatch user %d", userID)
		}
		seen++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate hot-path dispatch exclusions: %v", err)
	}
	if seen != 2 {
		t.Fatalf("hot-path dispatch exclusions = %d, want 2", seen)
	}
	if result.SenderMessage.ID != 2 || result.RecipientMessage.ID != 1 ||
		result.SenderMessage.Pts != 2 || result.RecipientMessage.Pts != 1 {
		t.Fatalf("result = %+v, want boxes 2/1 and distinct PTS 2/1", result)
	}
	// BEGIN + one ordered account-lock statement + one globally ordered dispatch
	// append-fence statement + one local trigger-defer statement +
	// one TTL/logical-message CTE + one combined PTS/projection CTE + COMMIT. The preflight lookup
	// and PostgreSQL box-id query are deliberately absent from this contract.
	if snapshot := stats.Snapshot(); snapshot.Queries != 7 || snapshot.Errors != 0 {
		t.Fatalf("query stats = %+v, want exactly 7 successful statements", snapshot)
	}
	var hotDialogVersionRows int
	if err := pool.QueryRow(baseCtx, `
SELECT count(*)
FROM read_model_versions
WHERE model='dialog_light'
  AND ((owner_user_id=$1 AND peer_type='user' AND peer_id=$2)
    OR (owner_user_id=$2 AND peer_type='user' AND peer_id=$1))`, sender.ID, recipient.ID).Scan(&hotDialogVersionRows); err != nil {
		t.Fatalf("count hot dialog versions: %v", err)
	}
	if hotDialogVersionRows != 0 {
		t.Fatalf("plain send wrote %d dialog_light version rows; hot PG token writes are forbidden", hotDialogVersionRows)
	}

	for _, fact := range []struct {
		name  string
		query string
		want  int
	}{
		{"boxes", `SELECT count(*) FROM message_boxes WHERE private_message_id=$1`, 2},
		{"events", `SELECT count(*) FROM user_update_events e JOIN message_boxes m ON (m.owner_user_id,m.box_id)=(e.user_id,e.message_box_id) WHERE m.private_message_id=$1`, 2},
		{"outbox", `SELECT count(*) FROM dispatch_outbox d JOIN user_update_events e ON (e.user_id,e.pts)=(d.target_user_id,d.pts) JOIN message_boxes m ON (m.owner_user_id,m.box_id)=(e.user_id,e.message_box_id) WHERE m.private_message_id=$1`, 2},
	} {
		var got int
		err := pool.QueryRow(baseCtx, fact.query, result.SenderMessage.UID).Scan(&got)
		if err != nil {
			t.Fatalf("count %s: %v", fact.name, err)
		}
		if got != fact.want {
			t.Fatalf("%s rows = %d, want %d", fact.name, got, fact.want)
		}
	}

	var senderBox, senderPts, recipientBox, recipientPts int
	var senderSnapshot []byte
	if err := pool.QueryRow(baseCtx, `
SELECT sender_box_id, sender_pts, recipient_box_id, recipient_pts, sender_snapshot
FROM private_messages
WHERE sender_user_id=$1 AND random_id=$2`, sender.ID, int64(240824001)).Scan(
		&senderBox, &senderPts, &recipientBox, &recipientPts, &senderSnapshot,
	); err != nil {
		t.Fatalf("load immutable receipt: %v", err)
	}
	if senderBox != 2 || senderPts != 2 || recipientBox != 1 || recipientPts != 1 {
		t.Fatalf("receipt = %d/%d %d/%d, want 2/2 1/1", senderBox, senderPts, recipientBox, recipientPts)
	}
	snapshotMessage, err := store.DecodePrivateSendSnapshot(senderSnapshot)
	if err != nil {
		t.Fatalf("decode immutable receipt: %v", err)
	}
	if snapshotMessage.Pts != 2 || snapshotMessage.ID != 2 || snapshotMessage.RandomID != 240824001 {
		t.Fatalf("snapshot = %+v, want SQL-patched sender PTS 2", snapshotMessage)
	}
}

func TestPlainPrivateSendReplayKeepsRecipientPtsWhenLocalBoxIDsMatch(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users := NewUserStore(pool)
	suffix := randomSuffix(t)
	sender := createTestUser(t, ctx, users, "+1669"+suffix+"01", "ReceiptSender", "")
	recipient := createTestUser(t, ctx, users, "+1669"+suffix+"02", "ReceiptRecipient", "")
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{sender.ID, recipient.ID})
	})
	if _, err := pool.Exec(ctx, `
INSERT INTO user_update_watermarks (user_id, contiguous_pts)
VALUES ($1, 7)
ON CONFLICT (user_id) DO UPDATE SET contiguous_pts = EXCLUDED.contiguous_pts`, sender.ID); err != nil {
		t.Fatalf("seed sender pts: %v", err)
	}
	messages := newTestMessageStore(pool, WithMessageAllocators(&perUserCounterAllocator{}))
	req := domain.SendPrivateTextRequest{
		SenderUserID: sender.ID, RecipientUserID: recipient.ID,
		RandomID: 240824050, Message: "equal local box ids", Date: 1800000050,
		IdempotencyPreflighted: true,
	}
	first, err := messages.SendPrivateText(ctx, req)
	if err != nil {
		t.Fatalf("first send: %v", err)
	}
	if first.SenderMessage.ID != 1 || first.RecipientMessage.ID != 1 ||
		first.SenderMessage.Pts != 8 || first.RecipientMessage.Pts != 1 {
		t.Fatalf("first result = %+v, want equal box ids with pts 8/1", first)
	}
	req.IdempotencyPreflighted = false
	replayed, err := messages.SendPrivateText(ctx, req)
	if err != nil {
		t.Fatalf("replay send: %v", err)
	}
	if !replayed.Duplicate || replayed.SenderMessage.Pts != 8 || replayed.RecipientMessage.Pts != 1 {
		t.Fatalf("replayed receipt = %+v, want duplicate pts 8/1", replayed)
	}
}

func TestMessageBoxIndexesAreConsolidatedPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	var replyMissing, privateMissing, dialogPresent bool
	if err := pool.QueryRow(ctx, `
SELECT
  to_regclass('public.message_boxes_reply_lookup_idx') IS NULL,
  to_regclass('public.message_boxes_private_lookup_idx') IS NULL,
  to_regclass('public.message_boxes_dialog_seek_idx') IS NOT NULL`).Scan(
		&replyMissing, &privateMissing, &dialogPresent,
	); err != nil {
		t.Fatal(err)
	}
	if !replyMissing || !privateMissing || !dialogPresent {
		t.Fatalf("consolidated indexes reply_missing=%t private_missing=%t dialog_present=%t",
			replyMissing, privateMissing, dialogPresent)
	}
	var definition string
	if err := pool.QueryRow(ctx, `
SELECT pg_get_constraintdef(oid)
FROM pg_constraint
WHERE conrelid = 'public.message_boxes'::regclass
  AND conname = 'message_boxes_owner_user_id_private_message_id_key'`).Scan(&definition); err != nil {
		t.Fatal(err)
	}
	if definition != "UNIQUE (private_message_id, owner_user_id)" {
		t.Fatalf("message box pair uniqueness=%q", definition)
	}
}

func BenchmarkPlainPrivateSendHotPath(b *testing.B) {
	dsn := os.Getenv("TELESRV_TEST_POSTGRES_DSN")
	if dsn == "" {
		b.Skip("set TELESRV_TEST_POSTGRES_DSN to run postgres benchmark")
	}
	if err := Migrate(dsn); err != nil {
		b.Fatalf("migrate: %v", err)
	}
	baseCtx := context.Background()
	pool, err := Open(baseCtx, dsn)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer pool.Close()
	users := NewUserStore(pool)
	suffix := time.Now().UnixNano()
	sender, err := users.Create(baseCtx, domain.User{AccessHash: suffix, Phone: "+17771" + randomDigits(suffix), FirstName: "BenchSender"})
	if err != nil {
		b.Fatalf("create sender: %v", err)
	}
	recipient, err := users.Create(baseCtx, domain.User{AccessHash: suffix + 1, Phone: "+17772" + randomDigits(suffix), FirstName: "BenchRecipient"})
	if err != nil {
		b.Fatalf("create recipient: %v", err)
	}
	defer pool.Exec(baseCtx, "DELETE FROM users WHERE id=ANY($1::bigint[])", []int64{sender.ID, recipient.ID})

	ctx, stats := dbtrace.WithStats(baseCtx)
	messages := newTestMessageStore(pool, WithMessageAllocators(&perUserCounterAllocator{}))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
			SenderUserID:           sender.ID,
			RecipientUserID:        recipient.ID,
			RandomID:               suffix + int64(i) + 1,
			Message:                "plain benchmark",
			Date:                   1800000100 + i,
			IdempotencyPreflighted: true,
		}); err != nil {
			b.Fatalf("SendPrivateText %d: %v", i, err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(stats.Snapshot().Queries)/float64(b.N), "pg-queries/op")
	b.ReportMetric(4, "business-sql/op")
}

func randomDigits(v int64) string {
	if v < 0 {
		v = -v
	}
	return strconv.FormatInt(v, 10)
}

func TestPlainPrivateSendHotPathClassifier(t *testing.T) {
	plain := domain.SendPrivateTextRequest{Message: "plain"}
	if !plainPrivateSendHotPath(plain, privateSendTxHooks{}) {
		t.Fatal("plain request did not select hot path")
	}
	cases := []struct {
		name string
		req  domain.SendPrivateTextRequest
	}{
		{"entities", domain.SendPrivateTextRequest{Message: "x", Entities: []domain.MessageEntity{{Type: domain.MessageEntityBold, Length: 1}}}},
		{"reply", domain.SendPrivateTextRequest{Message: "x", ReplyTo: &domain.MessageReply{MessageID: 1}}},
		{"forward", domain.SendPrivateTextRequest{Message: "x", Forward: &domain.MessageForward{Date: 1}}},
		{"silent", domain.SendPrivateTextRequest{Message: "x", Silent: true}},
		{"effect", domain.SendPrivateTextRequest{Message: "x", Effect: 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if plainPrivateSendHotPath(tc.req, privateSendTxHooks{}) {
				t.Fatal("rich request selected plain hot path")
			}
		})
	}
}

func TestPlainPrivateSendHotPathPreservesHistoryTTL(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users := NewUserStore(pool)
	suffix := randomSuffix(t)
	sender := createTestUser(t, ctx, users, "+1668"+suffix+"01", "TTLSender", "")
	recipient := createTestUser(t, ctx, users, "+1668"+suffix+"02", "TTLRecipient", "")
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id=ANY($1::bigint[])", []int64{sender.ID, recipient.ID})
	})
	if _, err := pool.Exec(ctx, `UPDATE users SET default_history_ttl_period=60 WHERE id=$1`, sender.ID); err != nil {
		t.Fatalf("set default ttl: %v", err)
	}
	messages := newTestMessageStore(pool, WithMessageAllocators(&perUserCounterAllocator{}))
	first, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID: sender.ID, RecipientUserID: recipient.ID,
		RandomID: 240824101, Message: "default ttl", Date: 1800000200,
	})
	if err != nil {
		t.Fatalf("send default ttl: %v", err)
	}
	if first.SenderMessage.TTLPeriod != 60 || first.SenderMessage.ExpiresAt != 1800000260 ||
		first.RecipientMessage.TTLPeriod != 60 || first.RecipientMessage.ExpiresAt != 1800000260 {
		t.Fatalf("default ttl projection = sender %d/%d recipient %d/%d",
			first.SenderMessage.TTLPeriod, first.SenderMessage.ExpiresAt,
			first.RecipientMessage.TTLPeriod, first.RecipientMessage.ExpiresAt)
	}
	if _, err := pool.Exec(ctx, `
UPDATE dialogs SET ttl_period=120
WHERE user_id=$1 AND peer_type='user' AND peer_id=$2`, sender.ID, recipient.ID); err != nil {
		t.Fatalf("set dialog ttl: %v", err)
	}
	second, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID: sender.ID, RecipientUserID: recipient.ID,
		RandomID: 240824102, Message: "dialog ttl", Date: 1800000300,
	})
	if err != nil {
		t.Fatalf("send dialog ttl: %v", err)
	}
	if second.SenderMessage.TTLPeriod != 120 || second.SenderMessage.ExpiresAt != 1800000420 {
		t.Fatalf("dialog ttl projection = %d/%d, want 120/1800000420",
			second.SenderMessage.TTLPeriod, second.SenderMessage.ExpiresAt)
	}
}

func TestPlainPrivateSendReadsTTLAfterWaitingForOrderedUserLocks(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users := NewUserStore(pool)
	suffix := randomSuffix(t)
	sender := createTestUser(t, ctx, users, "+1670"+suffix+"01", "TTLWaitSender", "")
	recipient := createTestUser(t, ctx, users, "+1670"+suffix+"02", "TTLWaitRecipient", "")
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id=ANY($1::bigint[])", []int64{sender.ID, recipient.ID})
	})

	ttlTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin ttl tx: %v", err)
	}
	defer ttlTx.Rollback(ctx)
	if err := lockUsersForUpdate(ctx, ttlTx, sender.ID, recipient.ID); err != nil {
		t.Fatalf("lock ttl users: %v", err)
	}
	if err := upsertPrivateDialogTTL(ctx, ttlTx, sender.ID, recipient.ID, 180); err != nil {
		t.Fatalf("write pending ttl: %v", err)
	}
	if err := upsertPrivateDialogTTL(ctx, ttlTx, recipient.ID, sender.ID, 180); err != nil {
		t.Fatalf("write reciprocal pending ttl: %v", err)
	}

	type sendResult struct {
		value domain.SendPrivateTextResult
		err   error
	}
	resultCh := make(chan sendResult, 1)
	messages := newTestMessageStore(pool, WithMessageAllocators(&perUserCounterAllocator{}))
	go func() {
		value, sendErr := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
			SenderUserID: sender.ID, RecipientUserID: recipient.ID,
			RandomID: 240824151, Message: "ttl after lock", Date: 1800000400,
		})
		resultCh <- sendResult{value: value, err: sendErr}
	}()
	select {
	case early := <-resultCh:
		t.Fatalf("send did not wait for ttl writer: result=%+v err=%v", early.value, early.err)
	case <-time.After(100 * time.Millisecond):
	}
	if err := ttlTx.Commit(ctx); err != nil {
		t.Fatalf("commit ttl tx: %v", err)
	}
	select {
	case completed := <-resultCh:
		if completed.err != nil {
			t.Fatalf("send after ttl commit: %v", completed.err)
		}
		if completed.value.SenderMessage.TTLPeriod != 180 || completed.value.RecipientMessage.TTLPeriod != 180 {
			t.Fatalf("send ttl = %d/%d, want 180/180", completed.value.SenderMessage.TTLPeriod, completed.value.RecipientMessage.TTLPeriod)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("send remained blocked after ttl commit")
	}
}
