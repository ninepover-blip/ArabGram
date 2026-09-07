package postgres

import (
	"context"
	"testing"
	"time"

	"telesrv/internal/domain"
)

// TestSetMessageReactionsPeerViewpoint 回归：chosen/My 是 per-viewer 字段，
// SetMessageReactions 返回的双方 owner 副本必须各按自己的视角解析——
// 修复前统一用发起者视角，对端推送会把发起者的 reaction 标成"对端自己选的"
// （TDesktop 非 min 更新用 chosen_order 直接覆盖本地 my 状态）。
func TestSetMessageReactionsPeerViewpoint(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{AccessHash: 1, Phone: "+1666" + suffix + "01", FirstName: "Owner"})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	friend, err := users.Create(ctx, domain.User{AccessHash: 2, Phone: "+1666" + suffix + "02", FirstName: "Friend"})
	if err != nil {
		t.Fatalf("create friend: %v", err)
	}
	t.Cleanup(func() {
		for _, id := range []int64{owner.ID, friend.ID} {
			_, _ = pool.Exec(ctx, "DELETE FROM private_message_reactions WHERE user_id = $1 OR message_sender_id = $1", id)
			_, _ = pool.Exec(ctx, "DELETE FROM message_boxes WHERE owner_user_id = $1", id)
			_, _ = pool.Exec(ctx, "DELETE FROM private_messages WHERE sender_user_id = $1", id)
			_, _ = pool.Exec(ctx, "DELETE FROM user_update_events WHERE user_id = $1", id)
			_, _ = pool.Exec(ctx, "DELETE FROM dialogs WHERE user_id = $1", id)
			_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", id)
		}
	})

	messages := newTestMessageStore(pool)
	now := int(time.Now().Unix())
	sent, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:    owner.ID,
		RecipientUserID: friend.ID,
		RandomID:        time.Now().UnixNano(),
		Message:         "react to me",
		Date:            now,
	})
	if err != nil {
		t.Fatalf("send private text: %v", err)
	}
	if sent.RecipientMessage.ID == 0 {
		t.Fatal("recipient box id missing")
	}

	// friend 给 owner 的消息点 👍（用 friend 自己视角的 box id 与 peer）。
	res, err := messages.SetMessageReactions(ctx, domain.SetPrivateMessageReactionsRequest{
		UserID:    friend.ID,
		Peer:      domain.Peer{Type: domain.PeerTypeUser, ID: owner.ID},
		MessageID: sent.RecipientMessage.ID,
		Reactions: []domain.MessageReaction{{Type: domain.MessageReactionEmoji, Emoticon: "\U0001F44D"}},
		Date:      now + 1,
	}, testPrivateReactionEffects)

	if err != nil {
		t.Fatalf("set message reactions: %v", err)
	}
	if len(res.Messages) != 2 {
		t.Fatalf("reaction result messages = %d, want both owners' copies", len(res.Messages))
	}
	for _, msg := range res.Messages {
		if msg.Reactions == nil || len(msg.Reactions.Results) != 1 {
			t.Fatalf("owner %d reactions = %+v, want one aggregate", msg.OwnerUserID, msg.Reactions)
		}
		result := msg.Reactions.Results[0]
		switch msg.OwnerUserID {
		case friend.ID:
			if result.ChosenOrder == 0 {
				t.Errorf("reactor's own copy should carry chosen_order, got %+v", result)
			}
			if len(msg.Reactions.Recent) != 1 || !msg.Reactions.Recent[0].My {
				t.Errorf("reactor's recent entry should be My, got %+v", msg.Reactions.Recent)
			}
		case owner.ID:
			if result.ChosenOrder != 0 {
				t.Errorf("peer copy must NOT carry reactor's chosen_order (viewpoint bleed), got %+v", result)
			}
			if len(msg.Reactions.Recent) != 1 || msg.Reactions.Recent[0].My {
				t.Errorf("peer copy recent entry must NOT be My, got %+v", msg.Reactions.Recent)
			}
		default:
			t.Errorf("unexpected owner %d in reaction result", msg.OwnerUserID)
		}
	}
}
