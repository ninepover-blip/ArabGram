package postgres

import (
	"context"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/observability/dbtrace"
	storepkg "telesrv/internal/store"
)

func TestUpdateEventQuickReplyBatchHydrationPostgres(t *testing.T) {
	pool := testPool(t)
	baseCtx := context.Background()
	suffix := randomSuffix(t)
	user, err := NewUserStore(pool).Create(baseCtx, domain.User{
		AccessHash: 8109,
		Phone:      "+1676" + suffix + "01",
		FirstName:  "QuickReplyBatch",
	})
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DELETE FROM users WHERE id = $1", user.ID) })

	events := NewUpdateEventStore(pool)
	replies := []domain.QuickReply{
		{OwnerUserID: user.ID, ID: 11, Shortcut: "first", TopMessage: 101, Count: 1, SortOrder: 1},
		{OwnerUserID: user.ID, ID: 12, Shortcut: "second", TopMessage: 102, Count: 2, SortOrder: 2},
	}
	message := domain.QuickReplyMessage{
		OwnerUserID: user.ID,
		ShortcutID:  12,
		ID:          102,
		RandomID:    9000102,
		Date:        1700001102,
		Message:     "durable quick reply",
	}
	appended := make([]domain.UpdateEvent, 0, 4)
	for _, event := range []domain.UpdateEvent{
		{Type: domain.UpdateEventQuickReplies, Date: 1700001100, QuickReplies: replies, PtsCount: 1},
		{Type: domain.UpdateEventNewQuickReply, Date: 1700001101, MaxID: replies[1].ID, QuickReplies: replies, PtsCount: 1},
		{Type: domain.UpdateEventQuickReplyMessage, Date: 1700001102, MaxID: replies[1].ID, QuickReplyMessage: message, PtsCount: 1},
		{Type: domain.UpdateEventNoop, Date: 1700001103, PtsCount: 1},
	} {
		got, err := events.AppendAllocated(baseCtx, user.ID, event)
		if err != nil {
			t.Fatalf("append %s: %v", event.Type, err)
		}
		appended = append(appended, got)
	}
	for i := range appended {
		if appended[i].PtsCount != 1 || (i > 0 && appended[i].Pts != appended[i-1].Pts+1) {
			t.Fatalf("unexpected PTS allocation at %d: %+v", i, appended)
		}
	}

	listCtx, listStats := dbtrace.WithStats(baseCtx)
	listed, err := events.ListAfter(listCtx, user.ID, 0, 50)
	if err != nil {
		t.Fatalf("ListAfter: %v", err)
	}
	if snapshot := listStats.Snapshot(); snapshot.Queries != 2 || snapshot.Errors != 0 {
		t.Fatalf("ListAfter query stats = %+v, want one event query plus one batch payload query", snapshot)
	}
	assertQuickReplyBatchPayloads(t, listed, replies, message)

	cursors := make([]storepkg.EventCursor, 0, 3)
	for _, event := range appended[:3] {
		cursors = append(cursors, storepkg.EventCursor{UserID: user.ID, Pts: event.Pts})
	}
	batchCtx, batchStats := dbtrace.WithStats(baseCtx)
	batched, err := events.BatchByCursor(batchCtx, cursors)
	if err != nil {
		t.Fatalf("BatchByCursor: %v", err)
	}
	if snapshot := batchStats.Snapshot(); snapshot.Queries != 2 || snapshot.Errors != 0 {
		t.Fatalf("BatchByCursor query stats = %+v, want one event query plus one batch payload query", snapshot)
	}
	assertQuickReplyBatchPayloads(t, batched, replies, message)

	plainCtx, plainStats := dbtrace.WithStats(baseCtx)
	plain, err := events.BatchByCursor(plainCtx, []storepkg.EventCursor{{UserID: user.ID, Pts: appended[3].Pts}})
	if err != nil {
		t.Fatalf("BatchByCursor noop: %v", err)
	}
	if len(plain) != 1 || plain[0].Type != domain.UpdateEventNoop {
		t.Fatalf("plain event = %+v, want one noop", plain)
	}
	if snapshot := plainStats.Snapshot(); snapshot.Queries != 1 || snapshot.Errors != 0 {
		t.Fatalf("plain BatchByCursor query stats = %+v, want no payload query", snapshot)
	}

	if _, err := pool.Exec(baseCtx, `
UPDATE user_update_events
SET quick_replies = '{}'::jsonb
WHERE user_id = $1 AND pts = $2`, user.ID, appended[0].Pts); err != nil {
		t.Fatalf("corrupt quick reply payload: %v", err)
	}
	corruptCtx, corruptStats := dbtrace.WithStats(baseCtx)
	if _, err := events.ListAfter(corruptCtx, user.ID, 0, 50); err == nil {
		t.Fatal("ListAfter accepted malformed relevant quick reply payload")
	}
	if snapshot := corruptStats.Snapshot(); snapshot.Queries != 2 || snapshot.Errors != 0 {
		t.Fatalf("corrupt ListAfter query stats = %+v, want one event query plus one batch payload query", snapshot)
	}
}

func assertQuickReplyBatchPayloads(t *testing.T, events []domain.UpdateEvent, replies []domain.QuickReply, message domain.QuickReplyMessage) {
	t.Helper()
	byType := make(map[domain.UpdateEventType]domain.UpdateEvent, len(events))
	for _, event := range events {
		byType[event.Type] = event
	}
	list := byType[domain.UpdateEventQuickReplies]
	if len(list.QuickReplies) != len(replies) || list.QuickReplies[0].Shortcut != replies[0].Shortcut || list.QuickReplies[1].Shortcut != replies[1].Shortcut {
		t.Fatalf("quick reply list = %+v, want %+v", list.QuickReplies, replies)
	}
	created := byType[domain.UpdateEventNewQuickReply]
	if created.QuickReply.ID != replies[1].ID || created.QuickReply.Shortcut != replies[1].Shortcut {
		t.Fatalf("new quick reply = %+v, want %+v", created.QuickReply, replies[1])
	}
	messageEvent := byType[domain.UpdateEventQuickReplyMessage]
	if messageEvent.QuickReplyMessage.ID != message.ID || messageEvent.QuickReplyMessage.ShortcutID != message.ShortcutID || messageEvent.QuickReplyMessage.Message != message.Message {
		t.Fatalf("quick reply message = %+v, want %+v", messageEvent.QuickReplyMessage, message)
	}
}
