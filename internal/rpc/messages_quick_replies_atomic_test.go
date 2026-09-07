package rpc

import (
	"context"
	"testing"

	"github.com/iamxvbaba/td/clock"
	"github.com/iamxvbaba/td/tg"
	"go.uber.org/zap/zaptest"

	accountapp "telesrv/internal/app/account"
	usersapp "telesrv/internal/app/users"
	"telesrv/internal/domain"
	"telesrv/internal/store/memory"
)

func TestQuickReplyRPCHardcutUsesOwningAggregate(t *testing.T) {
	ctx := context.Background()
	users := memory.NewUserStore()
	self, err := users.Create(ctx, domain.User{AccessHash: 991, Phone: "+15559910001", FirstName: "Quick"})
	if err != nil {
		t.Fatal(err)
	}
	events := memory.NewUpdateEventStore()
	quickReplies := memory.NewPasswordStore()
	quickReplies.AttachUpdateEventStore(events)
	router := New(Config{}, Deps{
		Account: accountapp.NewService(quickReplies, accountapp.WithBusinessAutomation(quickReplies)),
		Users:   usersapp.NewService(users),
	}, zaptest.NewLogger(t), clock.System)
	requestCtx := WithSessionID(WithAuthKeyID(WithUserID(ctx, self.ID), [8]byte{1}), 44)

	createdClass, err := router.onMessagesSaveQuickReplyText(requestCtx, &tg.MessagesSendMessageRequest{
		Peer: &tg.InputPeerSelf{}, Message: "template", RandomID: 71,
		QuickReplyShortcut: &tg.InputQuickReplyShortcut{Shortcut: "alpha"},
	})
	if err != nil {
		t.Fatal(err)
	}
	created, ok := createdClass.(*tg.Updates)
	if !ok {
		t.Fatalf("create result=%T", createdClass)
	}
	var shortcutID, messageID int
	for _, update := range created.Updates {
		switch value := update.(type) {
		case *tg.UpdateNewQuickReply:
			shortcutID = value.QuickReply.ShortcutID
		case *tg.UpdateMessageID:
			messageID = value.ID
		}
	}
	if shortcutID <= 0 || messageID <= 0 {
		t.Fatalf("create updates=%+v", created.Updates)
	}
	if ok, err := router.onMessagesEditQuickReplyShortcut(requestCtx, &tg.MessagesEditQuickReplyShortcutRequest{
		ShortcutID: shortcutID, Shortcut: "alpha",
	}); err != nil || !ok {
		t.Fatalf("same-name rename: ok=%v err=%v", ok, err)
	}
	if ok, err := router.onMessagesEditQuickReplyShortcut(requestCtx, &tg.MessagesEditQuickReplyShortcutRequest{
		ShortcutID: shortcutID, Shortcut: "renamed",
	}); err != nil || !ok {
		t.Fatalf("rename: ok=%v err=%v", ok, err)
	}
	if ok, err := router.onMessagesReorderQuickReplies(requestCtx, []int{shortcutID}); err != nil || !ok {
		t.Fatalf("same-order reorder: ok=%v err=%v", ok, err)
	}
	deleted, err := router.onMessagesDeleteQuickReplyMessages(requestCtx, &tg.MessagesDeleteQuickReplyMessagesRequest{
		ShortcutID: shortcutID, ID: []int{messageID},
	})
	if err != nil {
		t.Fatal(err)
	}
	updates, ok := deleted.(*tg.Updates)
	if !ok {
		t.Fatalf("delete messages result=%T", deleted)
	}
	var sawDelete bool
	for _, update := range updates.Updates {
		if value, ok := update.(*tg.UpdateDeleteQuickReplyMessages); ok && value.ShortcutID == shortcutID && len(value.Messages) == 1 && value.Messages[0] == messageID {
			sawDelete = true
		}
	}
	if !sawDelete {
		t.Fatalf("delete updates=%+v", updates.Updates)
	}
	if ok, err := router.onMessagesDeleteQuickReplyShortcut(requestCtx, shortcutID); err != nil || !ok {
		t.Fatalf("delete shortcut: ok=%v err=%v", ok, err)
	}

	durable, err := events.ListAfter(ctx, self.ID, 0, 10)
	if err != nil || len(durable) != 4 {
		t.Fatalf("durable events=%+v err=%v", durable, err)
	}
	wantTypes := []domain.UpdateEventType{
		domain.UpdateEventNewQuickReply,
		domain.UpdateEventQuickReplies,
		domain.UpdateEventDeleteQuickReplyMessages,
		domain.UpdateEventDeleteQuickReply,
	}
	for i, event := range durable {
		if event.Pts != i+1 || event.PtsCount != 1 || event.Type != wantTypes[i] {
			t.Fatalf("event[%d]=%+v, want pts=%d type=%s", i, event, i+1, wantTypes[i])
		}
	}
	if durable[0].QuickReply.ID != shortcutID || durable[0].QuickReplyMessage.ID != messageID ||
		len(durable[1].QuickReplies) != 1 || durable[1].QuickReplies[0].Shortcut != "renamed" ||
		len(durable[2].MessageIDs) != 1 || durable[2].MessageIDs[0] != messageID || durable[3].MaxID != shortcutID {
		t.Fatalf("durable payloads are not exact: %+v", durable)
	}
}
