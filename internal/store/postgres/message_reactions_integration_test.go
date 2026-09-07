package postgres

import (
	"context"
	"reflect"
	"testing"

	"telesrv/internal/domain"
)

func TestMessageStorePrivateMessageReactionsUseOwnerVisibleBoxIDs(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	sender := createTestUser(t, ctx, users, "+1994"+suffix+"01", "ReactionSender", "")
	recipient := createTestUser(t, ctx, users, "+1994"+suffix+"02", "ReactionRecipient", "")
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{sender.ID, recipient.ID})
	})

	messages := newTestMessageStore(pool)
	sent, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:    sender.ID,
		RecipientUserID: recipient.ID,
		RandomID:        99401,
		Message:         "react to me",
		Date:            1700000940,
	})
	if err != nil {
		t.Fatalf("SendPrivateText: %v", err)
	}

	reaction := domain.MessageReaction{Type: domain.MessageReactionEmoji, Emoticon: "\U0001f44d"}
	res, err := messages.SetMessageReactions(ctx, domain.SetPrivateMessageReactionsRequest{
		UserID:    recipient.ID,
		Peer:      domain.Peer{Type: domain.PeerTypeUser, ID: sender.ID},
		MessageID: sent.RecipientMessage.ID,
		Reactions: []domain.MessageReaction{
			reaction,
		},
		Big:  true,
		Date: 1700000950,
	}, testPrivateReactionEffects)

	if err != nil {
		t.Fatalf("SetMessageReactions: %v", err)
	}
	if len(res.Messages) != 2 {
		t.Fatalf("reaction result messages = %d, want both owner boxes", len(res.Messages))
	}
	byOwner := make(map[int64]domain.Message, len(res.Messages))
	for _, msg := range res.Messages {
		byOwner[msg.OwnerUserID] = msg
	}
	senderMsg, ok := byOwner[sender.ID]
	if !ok {
		t.Fatalf("reaction result = %+v, want sender owner box", res.Messages)
	}
	if senderMsg.ID != sent.SenderMessage.ID || !senderMsg.ReactionUnread {
		t.Fatalf("sender reaction box = %+v, want sender msg id %d with reaction_unread", senderMsg, sent.SenderMessage.ID)
	}
	recipientMsg, ok := byOwner[recipient.ID]
	if !ok {
		t.Fatalf("reaction result = %+v, want recipient owner box", res.Messages)
	}
	if recipientMsg.ID != sent.RecipientMessage.ID || recipientMsg.ReactionUnread {
		t.Fatalf("recipient reaction box = %+v, want recipient msg id %d without reaction_unread", recipientMsg, sent.RecipientMessage.ID)
	}

	senderReactions, err := messages.GetMessageReactions(ctx, domain.PrivateMessageReactionsRequest{
		OwnerUserID: sender.ID,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: recipient.ID},
		IDs:         []int{sent.SenderMessage.ID},
	})
	if err != nil {
		t.Fatalf("sender GetMessageReactions: %v", err)
	}
	if len(senderReactions.Messages) != 1 || senderReactions.Messages[0].Reactions == nil {
		t.Fatalf("sender reactions = %+v, want one enriched message", senderReactions)
	}
	if got := senderReactions.Messages[0].Reactions.Results; len(got) != 1 || got[0].Reaction != reaction || got[0].Count != 1 || got[0].ChosenOrder != 0 {
		t.Fatalf("sender reaction counts = %+v, want one peer reaction without chosen order", got)
	}
	if got := senderReactions.Messages[0].Reactions.Recent; len(got) != 1 || got[0].UserID != recipient.ID || got[0].SenderUserID != sender.ID || !got[0].Unread || !got[0].Big || got[0].My {
		t.Fatalf("sender recent reactions = %+v, want recipient non-my big reaction", got)
	}
	senderBox, err := messages.GetByIDs(ctx, sender.ID, []int{sent.SenderMessage.ID})
	if err != nil {
		t.Fatalf("sender GetByIDs after reaction: %v", err)
	}
	if len(senderBox.Messages) != 1 || !senderBox.Messages[0].ReactionUnread {
		t.Fatalf("sender box after reaction = %+v, want reaction_unread", senderBox.Messages)
	}
	read, err := messages.ReadMessageContents(ctx, domain.ReadMessageContentsRequest{
		OwnerUserID: sender.ID,
		IDs:         []int{sent.SenderMessage.ID},
		Date:        1700000960,
	})
	if err != nil {
		t.Fatalf("ReadMessageContents reaction: %v", err)
	}
	if !reflect.DeepEqual(read.MessageIDs, []int{sent.SenderMessage.ID}) || read.Event.Type != domain.UpdateEventReadMessageContents || read.Event.Pts == 0 {
		t.Fatalf("read reaction contents = %+v, want one read_message_contents event", read)
	}
	senderBox, err = messages.GetByIDs(ctx, sender.ID, []int{sent.SenderMessage.ID})
	if err != nil {
		t.Fatalf("sender GetByIDs after read reaction: %v", err)
	}
	if len(senderBox.Messages) != 1 || senderBox.Messages[0].ReactionUnread {
		t.Fatalf("sender box after read reaction = %+v, want reaction_unread cleared", senderBox.Messages)
	}
	if got := senderBox.Messages[0].Reactions.Recent; len(got) != 1 || got[0].Unread || got[0].SenderUserID != sender.ID {
		t.Fatalf("sender recent reactions after read = %+v, want unread flag cleared", got)
	}

	recipientReactions, err := messages.GetMessageReactions(ctx, domain.PrivateMessageReactionsRequest{
		OwnerUserID: recipient.ID,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: sender.ID},
		IDs:         []int{sent.RecipientMessage.ID},
	})
	if err != nil {
		t.Fatalf("recipient GetMessageReactions: %v", err)
	}
	if len(recipientReactions.Messages) != 1 || recipientReactions.Messages[0].Reactions == nil {
		t.Fatalf("recipient reactions = %+v, want one enriched message", recipientReactions)
	}
	if got := recipientReactions.Messages[0].Reactions.Results; len(got) != 1 || got[0].Reaction != reaction || got[0].ChosenOrder != 1 {
		t.Fatalf("recipient reaction counts = %+v, want own chosen order", got)
	}
	if got := recipientReactions.Messages[0].Reactions.Recent; len(got) != 1 || got[0].UserID != recipient.ID || got[0].SenderUserID != sender.ID || got[0].Unread || !got[0].My {
		t.Fatalf("recipient recent reactions = %+v, want my reaction", got)
	}
}

func TestDialogTopMessagesCarryPrivateMessageReactions(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	alice := createTestUser(t, ctx, users, "+1995"+suffix+"01", "DialogReactionAlice", "")
	bob := createTestUser(t, ctx, users, "+1995"+suffix+"02", "DialogReactionBob", "")
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", []int64{alice.ID, bob.ID})
	})

	messages := newTestMessageStore(pool)
	dialogs := NewDialogStore(pool)
	sent, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID:    alice.ID,
		RecipientUserID: bob.ID,
		RandomID:        99501,
		Message:         "latest from alice",
		Date:            1700001950,
	})
	if err != nil {
		t.Fatalf("SendPrivateText: %v", err)
	}
	reaction := domain.MessageReaction{Type: domain.MessageReactionEmoji, Emoticon: "\u2764"}
	if _, err := messages.SetMessageReactions(ctx, domain.SetPrivateMessageReactionsRequest{
		UserID:    bob.ID,
		Peer:      domain.Peer{Type: domain.PeerTypeUser, ID: alice.ID},
		MessageID: sent.RecipientMessage.ID,
		Reactions: []domain.MessageReaction{
			reaction,
		},
		Date: 1700001960,
	}, testPrivateReactionEffects); err != nil {
		t.Fatalf("SetMessageReactions: %v", err)
	}

	aliceDialogs, err := dialogs.ListByUser(ctx, alice.ID, domain.DialogFilter{Limit: 10})
	if err != nil {
		t.Fatalf("alice ListByUser: %v", err)
	}
	if len(aliceDialogs.Dialogs) != 1 || aliceDialogs.Dialogs[0].TopMessage != sent.SenderMessage.ID || aliceDialogs.Dialogs[0].UnreadReactions != 1 {
		t.Fatalf("alice dialog = %+v, want top message with one unread reaction", aliceDialogs.Dialogs)
	}
	if len(aliceDialogs.Messages) != 1 || aliceDialogs.Messages[0].ID != sent.SenderMessage.ID || aliceDialogs.Messages[0].Reactions == nil {
		t.Fatalf("alice dialog messages = %+v, want enriched top message", aliceDialogs.Messages)
	}
	if got := aliceDialogs.Messages[0].Reactions.Recent; len(got) != 1 || got[0].UserID != bob.ID || got[0].SenderUserID != alice.ID || !got[0].Unread || got[0].My {
		t.Fatalf("alice dialog recent reactions = %+v, want bob unread non-my reaction", got)
	}

	bobDialogs, err := dialogs.ListByPeers(ctx, bob.ID, []domain.Peer{{Type: domain.PeerTypeUser, ID: alice.ID}})
	if err != nil {
		t.Fatalf("bob ListByPeers: %v", err)
	}
	if len(bobDialogs.Messages) != 1 || bobDialogs.Messages[0].ID != sent.RecipientMessage.ID || bobDialogs.Messages[0].Reactions == nil {
		t.Fatalf("bob peer dialog messages = %+v, want enriched top message", bobDialogs.Messages)
	}
	if got := bobDialogs.Messages[0].Reactions.Recent; len(got) != 1 || got[0].UserID != bob.ID || got[0].SenderUserID != alice.ID || got[0].Unread || !got[0].My {
		t.Fatalf("bob peer dialog recent reactions = %+v, want my read reaction", got)
	}

	if _, err := messages.ReadMessageContents(ctx, domain.ReadMessageContentsRequest{
		OwnerUserID: alice.ID,
		IDs:         []int{sent.SenderMessage.ID},
		Date:        1700001970,
	}); err != nil {
		t.Fatalf("ReadMessageContents: %v", err)
	}
	aliceDialogs, err = dialogs.ListByUser(ctx, alice.ID, domain.DialogFilter{Limit: 10})
	if err != nil {
		t.Fatalf("alice ListByUser after read: %v", err)
	}
	if len(aliceDialogs.Dialogs) != 1 || aliceDialogs.Dialogs[0].UnreadReactions != 0 {
		t.Fatalf("alice dialog after read = %+v, want no unread reactions", aliceDialogs.Dialogs)
	}
	if got := aliceDialogs.Messages[0].Reactions.Recent; len(got) != 1 || got[0].Unread {
		t.Fatalf("alice dialog recent reactions after read = %+v, want unread cleared", got)
	}
}
