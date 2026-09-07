package postgres

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	broadcastapp "telesrv/internal/app/broadcast"
	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestBroadcastDeliveryPreservesAutoEntitiesInHistoryAndUpdates(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	recipient := createLoginCodeDeliveryTestUser(t, ctx, pool, "BroadcastEntities")

	broadcasts := NewBroadcastStore(pool)
	messages := newTestMessageStore(pool)
	service := broadcastapp.NewService(broadcasts, messages, nil)
	const text = "اعلان 🚀\n\n@matrixG"
	want := []domain.MessageEntity{{Type: domain.MessageEntityMention, Offset: 10, Length: 8}}
	ptsBefore := broadcastWatermark(t, ctx, pool, recipient.ID)

	campaign, err := service.Create(ctx, text, domain.BroadcastTargetSelected, []int64{recipient.ID}, "integration-test")
	if err != nil {
		t.Fatalf("Create broadcast: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM broadcasts WHERE id = $1`, campaign.ID)
	})
	if campaign.Message != text || !reflect.DeepEqual(campaign.Entities, want) {
		t.Fatalf("created campaign message/entities = %q/%+v, want %q/%+v", campaign.Message, campaign.Entities, text, want)
	}

	claims, err := broadcasts.ClaimBroadcastRecipients(ctx, "entity-worker", 1, time.Minute)
	if err != nil {
		t.Fatalf("ClaimBroadcastRecipients: %v", err)
	}
	if len(claims) != 1 || claims[0].BroadcastID != campaign.ID || claims[0].UserID != recipient.ID {
		t.Fatalf("claims = %+v, want campaign %d recipient %d", claims, campaign.ID, recipient.ID)
	}
	msg, err := messages.DeliverBroadcastRecipient(ctx, claims[0])
	if err != nil {
		t.Fatalf("DeliverBroadcastRecipient: %v", err)
	}
	if msg.Body != text || !reflect.DeepEqual(msg.Entities, want) {
		t.Fatalf("delivered message body/entities = %q/%+v, want %q/%+v", msg.Body, msg.Entities, text, want)
	}
	if msg.Pts != ptsBefore+1 {
		t.Fatalf("delivered pts = %d, want %d", msg.Pts, ptsBefore+1)
	}

	entityRows := []struct {
		name  string
		query string
		args  []any
	}{
		{name: "campaign", query: `SELECT entities::text FROM broadcasts WHERE id = $1`, args: []any{campaign.ID}},
		{name: "logical message", query: `SELECT entities::text FROM private_messages WHERE sender_user_id = $1 AND id = $2`, args: []any{domain.OfficialSystemUserID, msg.UID}},
		{name: "recipient box", query: `SELECT entities::text FROM message_boxes WHERE owner_user_id = $1 AND box_id = $2`, args: []any{recipient.ID, msg.ID}},
	}
	for _, row := range entityRows {
		var raw string
		if err := pool.QueryRow(ctx, row.query, row.args...).Scan(&raw); err != nil {
			t.Fatalf("load %s entities: %v", row.name, err)
		}
		got, err := decodeMessageEntities(raw)
		if err != nil {
			t.Fatalf("decode %s entities: %v", row.name, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("%s entities = %+v, want %+v", row.name, got, want)
		}
	}

	history, err := messages.ListByUser(ctx, recipient.ID, domain.MessageFilter{
		HasPeer: true,
		Peer:    domain.Peer{Type: domain.PeerTypeUser, ID: domain.OfficialSystemUserID},
		Limit:   10,
	})
	if err != nil {
		t.Fatalf("ListByUser: %v", err)
	}
	if len(history.Messages) != 1 || history.Messages[0].ID != msg.ID || !reflect.DeepEqual(history.Messages[0].Entities, want) {
		t.Fatalf("history messages = %+v, want message %d with entities %+v", history.Messages, msg.ID, want)
	}

	updates := NewUpdateEventStore(pool)
	events, err := updates.ListAfter(ctx, recipient.ID, ptsBefore, 10)
	if err != nil {
		t.Fatalf("ListAfter: %v", err)
	}
	if len(events) != 1 || events[0].Type != domain.UpdateEventNewMessage || events[0].Pts != msg.Pts ||
		events[0].Message.ID != msg.ID || !reflect.DeepEqual(events[0].Message.Entities, want) {
		t.Fatalf("difference events = %+v, want new message %d/%d with entities %+v", events, msg.ID, msg.Pts, want)
	}
	batch, err := updates.BatchByCursor(ctx, []store.EventCursor{{UserID: recipient.ID, Pts: msg.Pts}})
	if err != nil {
		t.Fatalf("BatchByCursor: %v", err)
	}
	if len(batch) != 1 || batch[0].Message.ID != msg.ID || !reflect.DeepEqual(batch[0].Message.Entities, want) {
		t.Fatalf("outbox events = %+v, want message %d with entities %+v", batch, msg.ID, want)
	}
}

func TestBroadcastSelectedClaimsAreDisjointAndDeliveryIsIncomingOnly(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	firstUser := createLoginCodeDeliveryTestUser(t, ctx, pool, "BroadcastFirst")
	secondUser := createLoginCodeDeliveryTestUser(t, ctx, pool, "BroadcastSecond")

	broadcasts := NewBroadcastStore(pool)
	campaign, err := broadcasts.CreateBroadcast(ctx, "maintenance complete", nil, domain.BroadcastTargetSelected, []int64{firstUser.ID, secondUser.ID}, "integration-test")
	if err != nil {
		t.Fatalf("CreateBroadcast: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM broadcasts WHERE id = $1`, campaign.ID)
	})

	firstClaims, err := broadcasts.ClaimBroadcastRecipients(ctx, "worker-one", 1, time.Minute)
	if err != nil {
		t.Fatalf("ClaimBroadcastRecipients worker one: %v", err)
	}
	secondClaims, err := broadcasts.ClaimBroadcastRecipients(ctx, "worker-two", 10, time.Minute)
	if err != nil {
		t.Fatalf("ClaimBroadcastRecipients worker two: %v", err)
	}
	if len(firstClaims) != 1 || len(secondClaims) != 1 {
		t.Fatalf("claim sizes = %d/%d, want 1/1", len(firstClaims), len(secondClaims))
	}
	if firstClaims[0].RecipientID == secondClaims[0].RecipientID || firstClaims[0].UserID == secondClaims[0].UserID {
		t.Fatalf("claims overlap: first=%+v second=%+v", firstClaims[0], secondClaims[0])
	}

	recipientPtsBefore := broadcastWatermark(t, ctx, pool, firstClaims[0].UserID)
	systemPtsBefore := broadcastWatermark(t, ctx, pool, domain.OfficialSystemUserID)
	systemBoxesBefore := broadcastFactCount(t, ctx, pool, `SELECT count(*)::int FROM message_boxes WHERE owner_user_id = $1`, domain.OfficialSystemUserID)
	systemEventsBefore := broadcastFactCount(t, ctx, pool, `SELECT count(*)::int FROM user_update_events WHERE user_id = $1`, domain.OfficialSystemUserID)
	systemOutboxBefore := broadcastFactCount(t, ctx, pool, `SELECT count(*)::int FROM dispatch_outbox WHERE target_user_id = $1`, domain.OfficialSystemUserID)
	msg, err := newTestMessageStore(pool).DeliverBroadcastRecipient(ctx, firstClaims[0])
	if err != nil {
		t.Fatalf("DeliverBroadcastRecipient: %v", err)
	}
	if msg.OwnerUserID != firstClaims[0].UserID || msg.Out || msg.Peer.ID != domain.OfficialSystemUserID || msg.From.ID != domain.OfficialSystemUserID {
		t.Fatalf("delivered message viewpoint = %+v", msg)
	}
	if msg.Pts != recipientPtsBefore+1 {
		t.Fatalf("recipient pts = %d, want %d", msg.Pts, recipientPtsBefore+1)
	}
	if got := broadcastWatermark(t, ctx, pool, domain.OfficialSystemUserID); got != systemPtsBefore {
		t.Fatalf("system user pts = %d, want unchanged %d", got, systemPtsBefore)
	}
	if got := broadcastFactCount(t, ctx, pool, `SELECT count(*)::int FROM message_boxes WHERE owner_user_id = $1`, domain.OfficialSystemUserID); got != systemBoxesBefore {
		t.Fatalf("system user box count = %d, want unchanged %d", got, systemBoxesBefore)
	}
	if got := broadcastFactCount(t, ctx, pool, `SELECT count(*)::int FROM user_update_events WHERE user_id = $1`, domain.OfficialSystemUserID); got != systemEventsBefore {
		t.Fatalf("system user event count = %d, want unchanged %d", got, systemEventsBefore)
	}
	if got := broadcastFactCount(t, ctx, pool, `SELECT count(*)::int FROM dispatch_outbox WHERE target_user_id = $1`, domain.OfficialSystemUserID); got != systemOutboxBefore {
		t.Fatalf("system user outbox count = %d, want unchanged %d", got, systemOutboxBefore)
	}
	assertBroadcastDeliveryFacts(t, ctx, pool, firstClaims[0], msg)

	if _, err := newTestMessageStore(pool).DeliverBroadcastRecipient(ctx, firstClaims[0]); !errors.Is(err, domain.ErrBroadcastLeaseLost) {
		t.Fatalf("replay delivery error = %v, want lease lost", err)
	}
	if got := broadcastWatermark(t, ctx, pool, firstClaims[0].UserID); got != msg.Pts {
		t.Fatalf("recipient pts after replay = %d, want %d", got, msg.Pts)
	}

	if _, err := pool.Exec(ctx, `UPDATE broadcast_recipients SET lease_until = now() - interval '1 second' WHERE id = $1`, secondClaims[0].RecipientID); err != nil {
		t.Fatalf("expire second recipient lease: %v", err)
	}
	reclaimed, err := broadcasts.ClaimBroadcastRecipients(ctx, "worker-three", 10, time.Minute)
	if err != nil {
		t.Fatalf("reclaim stale recipient: %v", err)
	}
	if len(reclaimed) != 1 || reclaimed[0].RecipientID != secondClaims[0].RecipientID || reclaimed[0].Attempts != 2 {
		t.Fatalf("reclaimed claims = %+v, want second recipient attempt 2", reclaimed)
	}
	if _, err := newTestMessageStore(pool).DeliverBroadcastRecipient(ctx, secondClaims[0]); !errors.Is(err, domain.ErrBroadcastLeaseLost) {
		t.Fatalf("stale claimant delivery error = %v, want lease lost", err)
	}
	if _, err := newTestMessageStore(pool).DeliverBroadcastRecipient(ctx, reclaimed[0]); err != nil {
		t.Fatalf("deliver second recipient: %v", err)
	}
	var sentCount int64
	if err := pool.QueryRow(ctx, `SELECT sent_count FROM broadcasts WHERE id = $1`, campaign.ID).Scan(&sentCount); err != nil {
		t.Fatalf("load sent_count: %v", err)
	}
	if sentCount != 2 {
		t.Fatalf("sent_count = %d, want 2", sentCount)
	}
}

func TestBroadcastAllMaterializationUsesCreationHighWaterAndBoundedKeysets(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	firstUser := createLoginCodeDeliveryTestUser(t, ctx, pool, "BroadcastAllFirst")
	secondUser := createLoginCodeDeliveryTestUser(t, ctx, pool, "BroadcastAllSecond")

	broadcasts := NewBroadcastStore(pool)
	campaign, err := broadcasts.CreateBroadcast(ctx, "all users", nil, domain.BroadcastTargetAll, nil, "integration-test")
	if err != nil {
		t.Fatalf("CreateBroadcast all: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM broadcasts WHERE id = $1`, campaign.ID)
	})
	lateUser := createLoginCodeDeliveryTestUser(t, ctx, pool, "BroadcastAllLate")

	for i := 0; i < 10000; i++ {
		inserted, err := broadcasts.MaterializeBroadcastRecipients(ctx, 1)
		if err != nil {
			t.Fatalf("MaterializeBroadcastRecipients iteration %d: %v", i, err)
		}
		if inserted < 0 || inserted > 1 {
			t.Fatalf("materialized batch = %d, want 0..1", inserted)
		}
		var done bool
		if err := pool.QueryRow(ctx, `SELECT enumeration_done FROM broadcasts WHERE id = $1`, campaign.ID).Scan(&done); err != nil {
			t.Fatalf("load enumeration state: %v", err)
		}
		if done {
			break
		}
		if i == 9999 {
			t.Fatal("materialization did not finish")
		}
	}

	for _, userID := range []int64{firstUser.ID, secondUser.ID} {
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM broadcast_recipients WHERE broadcast_id = $1 AND user_id = $2`, campaign.ID, userID).Scan(&count); err != nil {
			t.Fatalf("count materialized user %d: %v", userID, err)
		}
		if count != 1 {
			t.Fatalf("materialized user %d count = %d, want 1", userID, count)
		}
	}
	for _, excludedID := range []int64{lateUser.ID, domain.OfficialSystemUserID} {
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*)::int FROM broadcast_recipients WHERE broadcast_id = $1 AND user_id = $2`, campaign.ID, excludedID).Scan(&count); err != nil {
			t.Fatalf("count excluded user %d: %v", excludedID, err)
		}
		if count != 0 {
			t.Fatalf("excluded user %d was materialized", excludedID)
		}
	}

	var targetCount, materializedCount, actualCount int64
	if err := pool.QueryRow(ctx, `
SELECT b.target_count, b.materialized_count, count(r.id)
FROM broadcasts b
LEFT JOIN broadcast_recipients r ON r.broadcast_id = b.id
WHERE b.id = $1
GROUP BY b.id`, campaign.ID).Scan(&targetCount, &materializedCount, &actualCount); err != nil {
		t.Fatalf("load final materialization counters: %v", err)
	}
	if targetCount != materializedCount || materializedCount != actualCount {
		t.Fatalf("materialization counters target/materialized/actual = %d/%d/%d", targetCount, materializedCount, actualCount)
	}
}

func broadcastWatermark(t *testing.T, ctx context.Context, pool *pgxpool.Pool, userID int64) int {
	t.Helper()
	var pts int
	if err := pool.QueryRow(ctx, `
SELECT COALESCE((SELECT contiguous_pts FROM user_update_watermarks WHERE user_id = $1), 0)`, userID).Scan(&pts); err != nil {
		t.Fatalf("load user %d watermark: %v", userID, err)
	}
	return pts
}

func broadcastFactCount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, query, args...).Scan(&count); err != nil {
		t.Fatalf("count broadcast fact: %v", err)
	}
	return count
}

func assertBroadcastDeliveryFacts(t *testing.T, ctx context.Context, pool *pgxpool.Pool, claim store.BroadcastRecipientClaim, msg domain.Message) {
	t.Helper()
	var status string
	var privateMessageID int64
	var boxID, recipientPts int
	if err := pool.QueryRow(ctx, `
SELECT status, private_message_id, message_box_id, pts
FROM broadcast_recipients
WHERE id = $1`, claim.RecipientID).Scan(&status, &privateMessageID, &boxID, &recipientPts); err != nil {
		t.Fatalf("load broadcast recipient receipt: %v", err)
	}
	if status != string(domain.BroadcastRecipientSent) || privateMessageID != msg.UID || boxID != msg.ID || recipientPts != msg.Pts {
		t.Fatalf("broadcast receipt = %s/%d/%d/%d, message=%+v", status, privateMessageID, boxID, recipientPts, msg)
	}

	queries := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "recipient message box",
			sql:  `SELECT count(*)::int FROM message_boxes WHERE owner_user_id = $1 AND box_id = $2 AND NOT outgoing AND from_user_id = $3`,
			args: []any{claim.UserID, msg.ID, domain.OfficialSystemUserID},
		},
		{
			name: "recipient update event",
			sql:  `SELECT count(*)::int FROM user_update_events WHERE user_id = $1 AND pts = $2 AND event_type = 'new_message' AND message_box_id = $3`,
			args: []any{claim.UserID, msg.Pts, msg.ID},
		},
		{
			name: "recipient dispatch outbox",
			sql:  `SELECT count(*)::int FROM dispatch_outbox WHERE target_user_id = $1 AND pts = $2 AND event_type = 'new_message'`,
			args: []any{claim.UserID, msg.Pts},
		},
	}
	for _, query := range queries {
		var count int
		if err := pool.QueryRow(ctx, query.sql, query.args...).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", query.name, err)
		}
		if count != 1 {
			t.Fatalf("%s count = %d, want 1", query.name, count)
		}
	}
}
