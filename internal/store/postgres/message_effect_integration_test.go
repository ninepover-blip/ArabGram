package postgres

import (
	"context"
	"testing"
	"time"

	"telesrv/internal/domain"
)

// TestSendPrivateEffectSurvivesReadPaths 回归迁移 0023：消息特效 effect 经私聊全部读路径
// 保真——发送结果双盒、history、GetByIDs、以及更新事件重建路径（离线 difference / 在线推送
// 共用）。漏一处读站点 = 特效静默丢失（发送方播了、接收方/重载看不到）。
func TestSendPrivateEffectSurvivesReadPaths(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	sender, err := users.Create(ctx, domain.User{AccessHash: 91, Phone: "+1996" + suffix + "01", FirstName: "EffectSender"})
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	recipient, err := users.Create(ctx, domain.User{AccessHash: 92, Phone: "+1996" + suffix + "02", FirstName: "EffectRecipient"})
	if err != nil {
		t.Fatalf("create recipient: %v", err)
	}
	ids := []int64{sender.ID, recipient.ID}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM dispatch_outbox WHERE target_user_id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(ctx, "DELETE FROM user_update_events WHERE user_id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(ctx, "DELETE FROM message_boxes WHERE owner_user_id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(ctx, "DELETE FROM private_messages WHERE sender_user_id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(ctx, "DELETE FROM dialogs WHERE user_id = ANY($1::bigint[])", ids)
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", ids)
	})

	messages := newTestMessageStore(pool, WithMessageAllocators(&perUserCounterAllocator{}))
	const effect = int64(5104841169784993603)

	res, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:    sender.ID,
		RecipientUserID: recipient.ID,
		RandomID:        time.Now().UnixNano(),
		Message:         "🎉 with effect",
		Effect:          effect,
		Date:            int(time.Now().Unix()),
	})
	if err != nil {
		t.Fatalf("send effect message: %v", err)
	}
	// 发送结果双盒：发送方与接收方各自的副本都带特效。
	if res.SenderMessage.Effect != effect || res.RecipientMessage.Effect != effect {
		t.Fatalf("send result effect = sender %d recipient %d, want %d", res.SenderMessage.Effect, res.RecipientMessage.Effect, effect)
	}

	// history：接收方拉取携带 effect。
	hist, err := messages.ListByUser(ctx, recipient.ID, domain.MessageFilter{Limit: 10})
	if err != nil {
		t.Fatalf("recipient history: %v", err)
	}
	found := false
	for _, m := range hist.Messages {
		if m.ID == res.RecipientMessage.ID {
			found = true
			if m.Effect != effect {
				t.Fatalf("history effect = %d, want %d", m.Effect, effect)
			}
		}
	}
	if !found {
		t.Fatal("recipient history missing the effect message")
	}

	// GetByIDs 带 effect。
	byID, err := messages.GetByIDs(ctx, sender.ID, []int{res.SenderMessage.ID})
	if err != nil {
		t.Fatalf("sender get by ids: %v", err)
	}
	if len(byID.Messages) != 1 {
		t.Fatalf("get by ids = %d, want 1", len(byID.Messages))
	}
	if byID.Messages[0].Effect != effect {
		t.Fatalf("get-by-id effect = %d, want %d", byID.Messages[0].Effect, effect)
	}

	// 更新事件重建路径（关键：getDifference/推送共用）带 effect。
	events := NewUpdateEventStore(pool)
	got, err := events.ListAfter(ctx, recipient.ID, 0, 20)
	if err != nil {
		t.Fatalf("list recipient events: %v", err)
	}
	rebuilt := false
	for _, ev := range got {
		if ev.Type == domain.UpdateEventNewMessage && ev.Message.ID == res.RecipientMessage.ID {
			if ev.Message.Effect != effect {
				t.Fatalf("update-event effect = %d, want %d（重建路径丢失特效）", ev.Message.Effect, effect)
			}
			rebuilt = true
		}
	}
	if !rebuilt {
		t.Fatal("recipient new_message event missing effect rebuild")
	}

	// getDialogs 的 top message 必须带 effect：TDesktop 把 getDialogs 返回的消息入缓存
	// 且不被后续 getHistory 覆盖，dialog top message 漏 effect = 最新那条特效消息永久按
	// 「无特效」渲染（真机实测的 bug：getDialogs 返回的最新消息没有消息特效）。
	dialogs := NewDialogStore(pool)
	for _, owner := range []struct {
		id    int64
		boxID int
		label string
	}{
		{recipient.ID, res.RecipientMessage.ID, "recipient"},
		{sender.ID, res.SenderMessage.ID, "sender"},
	} {
		list, err := dialogs.ListByUser(ctx, owner.id, domain.DialogFilter{Limit: 20})
		if err != nil {
			t.Fatalf("%s getDialogs: %v", owner.label, err)
		}
		top := false
		for _, m := range list.Messages {
			if m.ID == owner.boxID {
				top = true
				if m.Effect != effect {
					t.Fatalf("%s dialog top message effect = %d, want %d（getDialogs 漏 effect）", owner.label, m.Effect, effect)
				}
			}
		}
		if !top {
			t.Fatalf("%s getDialogs missing top message box %d", owner.label, owner.boxID)
		}
	}
}
