package postgres

import (
	"context"
	"testing"
	"time"

	"telesrv/internal/domain"
)

// TestDialogTopMessageCarriesFullMetadata 回归 getDialogs/getPeerDialogs 的 top-message
// 投影完整性：dialog 列表的 top message 是独立查询（dialog.sql，不复用 message.sql），
// 必须带 grouped_id / effect / reply_markup / rich_message —— TDesktop 把 getDialogs
// 返回的消息入缓存且不被后续 getHistory 覆盖，缺任一字段都让最新那条消息永久按缺失态渲染。
func TestDialogTopMessageCarriesFullMetadata(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	sender, err := users.Create(ctx, domain.User{AccessHash: 71, Phone: "+1997" + suffix + "01", FirstName: "MetaSender"})
	if err != nil {
		t.Fatalf("create sender: %v", err)
	}
	recipient, err := users.Create(ctx, domain.User{AccessHash: 72, Phone: "+1997" + suffix + "02", FirstName: "MetaRecipient"})
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
	const groupedID = int64(990000000777)
	const effect = int64(5104841169784993603)
	markup := &domain.MessageReplyMarkup{Inline: [][]domain.MarkupButton{{
		{Type: domain.MarkupButtonCallback, Text: "Press", Data: []byte{0x00, 0x01, 0xFF, 'x'}},
	}}}
	rich := &domain.MessageRichMessage{Rtl: true, Blocks: []byte{0x10, 0x20, 0x30}}

	res, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:    sender.ID,
		RecipientUserID: recipient.ID,
		RandomID:        time.Now().UnixNano(),
		Message:         "full metadata top",
		GroupedID:       groupedID,
		Effect:          effect,
		ReplyMarkup:     markup,
		RichMessage:     rich,
		Date:            int(time.Now().Unix()),
	})
	if err != nil {
		t.Fatalf("send message: %v", err)
	}

	dialogs := NewDialogStore(pool)
	check := func(label string, ownerID int64, boxID int) {
		t.Helper()
		list, err := dialogs.ListByUser(ctx, ownerID, domain.DialogFilter{Limit: 20})
		if err != nil {
			t.Fatalf("%s getDialogs: %v", label, err)
		}
		var top *domain.Message
		for i := range list.Messages {
			if list.Messages[i].ID == boxID {
				top = &list.Messages[i]
			}
		}
		if top == nil {
			t.Fatalf("%s getDialogs missing top message box %d", label, boxID)
		}
		if top.GroupedID != groupedID {
			t.Fatalf("%s dialog top grouped_id = %d, want %d", label, top.GroupedID, groupedID)
		}
		if top.Effect != effect {
			t.Fatalf("%s dialog top effect = %d, want %d", label, top.Effect, effect)
		}
		if top.ReplyMarkup == nil || len(top.ReplyMarkup.Inline) != 1 || len(top.ReplyMarkup.Inline[0]) != 1 ||
			top.ReplyMarkup.Inline[0][0].Text != "Press" {
			t.Fatalf("%s dialog top reply_markup not preserved: %+v", label, top.ReplyMarkup)
		}
		if top.RichMessage == nil || top.RichMessage.IsZero() || !top.RichMessage.Rtl {
			t.Fatalf("%s dialog top rich_message not preserved: %+v", label, top.RichMessage)
		}
	}
	check("sender", sender.ID, res.SenderMessage.ID)
	check("recipient", recipient.ID, res.RecipientMessage.ID)
}

// TestSavedDialogTopMessageCarriesFullMetadata 回归 getSavedDialogs 的 top-message 投影：
// 收藏夹子会话(Saved Messages)的 top message 是另一组独立查询(saved_dialog.sql,3 条),
// 同样不复用 message.sql，必须带 grouped_id/effect/reply_markup/rich_message(转发相册/
// bot 键盘/富文本进收藏夹后子会话最新一条会缺失)。
func TestSavedDialogTopMessageCarriesFullMetadata(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	alice, err := users.Create(ctx, domain.User{AccessHash: 73, Phone: "+1998" + suffix + "01", FirstName: "SavedMeta"})
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM message_boxes WHERE owner_user_id = $1", alice.ID)
		_, _ = pool.Exec(ctx, "DELETE FROM private_messages WHERE sender_user_id = $1", alice.ID)
		_, _ = pool.Exec(ctx, "DELETE FROM dialogs WHERE user_id = $1", alice.ID)
		_, _ = pool.Exec(ctx, "DELETE FROM saved_dialog_pins WHERE user_id = $1", alice.ID)
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", alice.ID)
	})

	messages := newTestMessageStore(pool, WithMessageAllocators(&perUserCounterAllocator{}))
	const groupedID = int64(990000000888)
	const effect = int64(5104841169784993603)
	markup := &domain.MessageReplyMarkup{Inline: [][]domain.MarkupButton{{
		{Type: domain.MarkupButtonCallback, Text: "Saved", Data: []byte{0x09}},
	}}}
	rich := &domain.MessageRichMessage{Part: true, Blocks: []byte{0x44, 0x55}}

	// 直发笔记 → saved_peer = self；子会话 top 即这条。
	note, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:    alice.ID,
		RecipientUserID: alice.ID,
		RandomID:        time.Now().UnixNano(),
		Message:         "self note full meta",
		GroupedID:       groupedID,
		Effect:          effect,
		ReplyMarkup:     markup,
		RichMessage:     rich,
		Date:            int(time.Now().Unix()),
	})
	if err != nil {
		t.Fatalf("send self note: %v", err)
	}

	list, err := messages.ListSavedDialogs(ctx, alice.ID, domain.SavedDialogsFilter{Limit: 20})
	if err != nil {
		t.Fatalf("ListSavedDialogs: %v", err)
	}
	var top *domain.Message
	for i := range list.Messages {
		if list.Messages[i].ID == note.SenderMessage.ID {
			top = &list.Messages[i]
		}
	}
	if top == nil {
		t.Fatalf("getSavedDialogs missing top message box %d (got %d messages)", note.SenderMessage.ID, len(list.Messages))
	}
	if top.GroupedID != groupedID {
		t.Fatalf("saved dialog top grouped_id = %d, want %d", top.GroupedID, groupedID)
	}
	if top.Effect != effect {
		t.Fatalf("saved dialog top effect = %d, want %d", top.Effect, effect)
	}
	if top.ReplyMarkup == nil || len(top.ReplyMarkup.Inline) != 1 || top.ReplyMarkup.Inline[0][0].Text != "Saved" {
		t.Fatalf("saved dialog top reply_markup not preserved: %+v", top.ReplyMarkup)
	}
	if top.RichMessage == nil || top.RichMessage.IsZero() || !top.RichMessage.Part {
		t.Fatalf("saved dialog top rich_message not preserved: %+v", top.RichMessage)
	}
}
