package postgres

import (
	"context"
	"testing"

	"telesrv/internal/domain"
)

// TestUpdateEventListAfterCarriesPinServiceReply 复现离线差分场景：
// 接收方经 ListAfter 读到的 pin 服务消息事件必须携带 reply_to（指向
// 接收方视角的被置顶消息），否则 TDesktop 渲染 "pinned Deleted message"。
func TestUpdateEventListAfterCarriesPinServiceReply(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	alice, err := users.Create(ctx, domain.User{AccessHash: 95, Phone: "+1674" + suffix + "01", FirstName: "DiffPinA"})
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, err := users.Create(ctx, domain.User{AccessHash: 96, Phone: "+1674" + suffix + "02", FirstName: "DiffPinB"})
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{alice.ID, bob.ID})
	})

	messages := newTestMessageStore(pool)
	sent, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID: alice.ID, RecipientUserID: bob.ID, RandomID: 841001,
		Message: "OfflinePinTarget", Date: 1700000800,
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, err := messages.PinPrivateMessage(ctx, domain.PinPrivateMessageRequest{
		OwnerUserID: alice.ID,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: bob.ID},
		MessageID:   sent.SenderMessage.ID,
		Pinned:      true,
		Date:        1700000801,
	}); err != nil {
		t.Fatalf("pin: %v", err)
	}
	svc, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID: alice.ID, RecipientUserID: bob.ID, RandomID: 841002,
		Media: &domain.MessageMedia{
			Kind:          domain.MessageMediaKindService,
			ServiceAction: &domain.MessageServiceAction{Kind: domain.MessageServiceActionPinMessage},
		},
		ReplyTo: &domain.MessageReply{MessageID: sent.SenderMessage.ID, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: bob.ID}},
		Date:    1700000802,
	})
	if err != nil {
		t.Fatalf("send pin service: %v", err)
	}
	if svc.RecipientMessage.ReplyTo == nil || svc.RecipientMessage.ReplyTo.MessageID != sent.RecipientMessage.ID {
		t.Fatalf("recipient service reply = %+v, want bob 视角 id %d", svc.RecipientMessage.ReplyTo, sent.RecipientMessage.ID)
	}

	events, err := NewUpdateEventStore(pool).ListAfter(ctx, bob.ID, 0, 50)
	if err != nil {
		t.Fatalf("list bob events: %v", err)
	}
	foundService := false
	for _, event := range events {
		if event.Type != domain.UpdateEventNewMessage || event.Message.Media == nil {
			continue
		}
		if event.Message.Media.Kind != domain.MessageMediaKindService {
			continue
		}
		foundService = true
		if event.Message.Media.ServiceAction == nil || event.Message.Media.ServiceAction.Kind != domain.MessageServiceActionPinMessage {
			t.Fatalf("service event action = %+v, want pin_message", event.Message.Media.ServiceAction)
		}
		if event.Message.ReplyTo == nil {
			t.Fatalf("difference 路径服务消息事件丢失 reply_to（TDesktop 将渲染 pinned Deleted message）：%+v", event.Message)
		}
		if event.Message.ReplyTo.MessageID != sent.RecipientMessage.ID {
			t.Fatalf("service event reply id = %d, want bob 视角 %d", event.Message.ReplyTo.MessageID, sent.RecipientMessage.ID)
		}
	}
	if !foundService {
		t.Fatalf("bob events lack pin service message: %+v", events)
	}

	// pinned_messages 事件也必须可经 difference 回放。
	foundPinned := false
	for _, event := range events {
		if event.Type == domain.UpdateEventPinnedMessages {
			foundPinned = true
			if !event.Bool || len(event.MessageIDs) != 1 || event.MessageIDs[0] != sent.RecipientMessage.ID {
				t.Fatalf("pinned event = %+v, want pinned=true with bob 视角 id", event)
			}
		}
	}
	if !foundPinned {
		t.Fatalf("bob events lack pinned_messages: %+v", events)
	}

	// getDialogs 的 top message 必须带全量元数据：TDesktop 把它先入缓存
	// 且后续完整版不覆盖，缺 reply 头会让 pin 服务消息永久渲染成
	// "pinned Deleted message"（实测离线场景抓到的真 bug）。
	dialogs := NewDialogStore(pool)
	list, err := dialogs.ListByPeers(ctx, bob.ID, []domain.Peer{{Type: domain.PeerTypeUser, ID: alice.ID}})
	if err != nil {
		t.Fatalf("list dialogs by peers: %v", err)
	}
	if len(list.Messages) != 1 {
		t.Fatalf("dialog top messages = %d, want 1", len(list.Messages))
	}
	top := list.Messages[0]
	if top.ID != svc.RecipientMessage.ID {
		t.Fatalf("dialog top message id = %d, want pin service %d", top.ID, svc.RecipientMessage.ID)
	}
	if top.Media == nil || top.Media.ServiceAction == nil || top.Media.ServiceAction.Kind != domain.MessageServiceActionPinMessage {
		t.Fatalf("dialog top message media = %+v, want pin service action", top.Media)
	}
	if top.ReplyTo == nil || top.ReplyTo.MessageID != sent.RecipientMessage.ID {
		t.Fatalf("getDialogs top message 丢失 reply 头：%+v, want reply_to=%d", top.ReplyTo, sent.RecipientMessage.ID)
	}
	byUser, err := dialogs.ListByUser(ctx, bob.ID, domain.DialogFilter{Limit: 10})
	if err != nil {
		t.Fatalf("list dialogs by user: %v", err)
	}
	foundTop := false
	for _, msg := range byUser.Messages {
		if msg.ID == svc.RecipientMessage.ID && msg.Peer == (domain.Peer{Type: domain.PeerTypeUser, ID: alice.ID}) {
			foundTop = true
			if msg.ReplyTo == nil || msg.ReplyTo.MessageID != sent.RecipientMessage.ID {
				t.Fatalf("ListByUser top message 丢失 reply 头：%+v", msg.ReplyTo)
			}
		}
	}
	if !foundTop {
		t.Fatalf("ListByUser lacks pin service top message: %+v", byUser.Messages)
	}
}
