package postgres

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestPrivateReactionAndAbsoluteDeliveryCommitAtomicallyPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users := NewUserStore(pool)
	suffix := randomSuffix(t)
	alice, err := users.Create(ctx, domain.User{AccessHash: 121, Phone: "+1898" + suffix + "01", FirstName: "Reaction Alice"})
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, err := users.Create(ctx, domain.User{AccessHash: 122, Phone: "+1898" + suffix + "02", FirstName: "Reaction Bob"})
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}
	userIDs := []int64{alice.ID, bob.ID}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM edge_delivery_outbox WHERE target_user_id = ANY($1::bigint[])", userIDs)
		_, _ = pool.Exec(ctx, "DELETE FROM dialogs WHERE user_id = ANY($1::bigint[])", userIDs)
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", userIDs)
	})
	messages := newTestMessageStore(pool)
	sent, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID: alice.ID, RecipientUserID: bob.ID, RandomID: 830001,
		Message: "atomic reaction", Date: 1_700_020_000,
	})
	if err != nil {
		t.Fatalf("send private text: %v", err)
	}
	var beforeDelivery, beforeAlicePTS, beforeBobPTS int
	if err := pool.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = ANY($1::bigint[])),
  COALESCE((SELECT contiguous_pts FROM user_update_watermarks WHERE user_id = $2), 0),
  COALESCE((SELECT contiguous_pts FROM user_update_watermarks WHERE user_id = $3), 0)`, userIDs, alice.ID, bob.ID).Scan(&beforeDelivery, &beforeAlicePTS, &beforeBobPTS); err != nil {
		t.Fatalf("read reaction baselines: %v", err)
	}
	reaction := domain.MessageReaction{Type: domain.MessageReactionEmoji, Emoticon: "👍"}
	request := domain.SetPrivateMessageReactionsRequest{
		UserID: bob.ID, Peer: domain.Peer{Type: domain.PeerTypeUser, ID: alice.ID},
		MessageID: sent.RecipientMessage.ID, Reactions: []domain.MessageReaction{reaction}, Date: 1_700_020_001,
	}
	projectionErr := errors.New("reaction projection failed")
	if _, err := messages.SetMessageReactions(ctx, request, func(domain.PrivateMessageReactionsResult) ([]store.DeliveryEffect, error) {
		return nil, projectionErr
	}); !errors.Is(err, projectionErr) {
		t.Fatalf("failed reaction err = %v, want projection error", err)
	}
	var reactionRows int
	var aliceUnread bool
	if err := pool.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM private_message_reactions WHERE message_sender_id = $1 AND private_message_id = $2),
  (SELECT reaction_unread FROM message_boxes WHERE owner_user_id = $1 AND box_id = $3)`, alice.ID, sent.SenderMessage.UID, sent.SenderMessage.ID).Scan(&reactionRows, &aliceUnread); err != nil {
		t.Fatalf("read reaction rollback facts: %v", err)
	}
	if reactionRows != 0 || aliceUnread {
		t.Fatalf("reaction survived failed delivery builder: rows=%d unread=%v", reactionRows, aliceUnread)
	}

	res, err := messages.SetMessageReactions(ctx, request, func(result domain.PrivateMessageReactionsResult) ([]store.DeliveryEffect, error) {
		effects := make([]store.DeliveryEffect, 0, len(result.Messages))
		for _, message := range result.Messages {
			effects = append(effects, store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
				TargetUserID: message.OwnerUserID, Payload: []byte{1}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
			}))
		}
		return effects, nil
	})
	if err != nil {
		t.Fatalf("set private reaction: %v", err)
	}
	if len(res.Messages) != 2 {
		t.Fatalf("reaction owner projections = %d, want 2", len(res.Messages))
	}
	var afterDelivery, afterAlicePTS, afterBobPTS int
	if err := pool.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = ANY($1::bigint[])),
	  COALESCE((SELECT contiguous_pts FROM user_update_watermarks WHERE user_id = $2), 0),
	  COALESCE((SELECT contiguous_pts FROM user_update_watermarks WHERE user_id = $3), 0),
  (SELECT count(*) FROM private_message_reactions WHERE message_sender_id = $2 AND private_message_id = $4)`, userIDs, alice.ID, bob.ID, sent.SenderMessage.UID).
		Scan(&afterDelivery, &afterAlicePTS, &afterBobPTS, &reactionRows); err != nil {
		t.Fatalf("read committed reaction facts: %v", err)
	}
	if reactionRows != 1 || afterDelivery-beforeDelivery != 2 {
		t.Fatalf("reaction committed rows/delivery delta = %d/%d, want 1/2", reactionRows, afterDelivery-beforeDelivery)
	}
	if afterAlicePTS != beforeAlicePTS || afterBobPTS != beforeBobPTS {
		t.Fatalf("reaction changed account pts alice %d->%d bob %d->%d", beforeAlicePTS, afterAlicePTS, beforeBobPTS, afterBobPTS)
	}
}
