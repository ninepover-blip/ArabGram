package memory

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
	storepkg "telesrv/internal/store"
)

func TestMemoryChannelSendClearDraftCommitsAccountEvent(t *testing.T) {
	ctx := context.Background()
	dialogs := NewDialogStore()
	events := NewUpdateEventStore()
	dialogs.AttachUpdateEventStore(events)
	channels := newChannelStoreWithDelivery()
	dialogs.BindDialogAggregateStores(channels, nil)
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 101, Title: "draft aggregate", Megagroup: true, Date: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	peer := domain.Peer{Type: domain.PeerTypeChannel, ID: created.Channel.ID}
	if err := dialogs.SaveDraft(ctx, 101, domain.DialogDraft{Peer: peer, Message: "pending", Date: 11}); err != nil {
		t.Fatal(err)
	}
	result, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID: 101, ChannelID: peer.ID, RandomID: 99, Message: "sent", Date: 12, ClearDraft: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.DraftEvent.Type != domain.UpdateEventDraftMessage || result.DraftEvent.Pts != 1 || result.DraftEvent.Peer != peer {
		t.Fatalf("unexpected draft event: %+v", result.DraftEvent)
	}
	if _, found, err := dialogs.GetDraft(ctx, 101, peer, 0); err != nil || found {
		t.Fatalf("draft survived channel send: found=%v err=%v", found, err)
	}
	accountEvents, _ := events.ListAfter(ctx, 101, 0, 10)
	if len(accountEvents) != 1 || accountEvents[0].Pts != 1 || accountEvents[0].Type != domain.UpdateEventDraftMessage {
		t.Fatalf("unexpected account event stream: %+v", accountEvents)
	}
	replay, err := channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID: 101, ChannelID: peer.ID, RandomID: 99, Message: "sent", Date: 13, ClearDraft: true,
	})
	if err != nil || !replay.Duplicate {
		t.Fatalf("channel replay: duplicate=%v err=%v", replay.Duplicate, err)
	}
	accountEvents, _ = events.ListAfter(ctx, 101, 0, 10)
	if len(accountEvents) != 1 {
		t.Fatalf("channel replay appended account event: %+v", accountEvents)
	}
}

func TestMemoryChannelSendClearDraftFailsClosedBeforeChannelWrite(t *testing.T) {
	ctx := context.Background()
	channels := newChannelStoreWithDelivery()
	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: 101, Title: "unbound", Megagroup: true, Date: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	beforeMessages := len(channels.messages[created.Channel.ID])
	beforePTS := channels.ptsSeq[created.Channel.ID]
	_, err = channels.SendChannelMessage(ctx, domain.SendChannelMessageRequest{
		UserID: 101, ChannelID: created.Channel.ID, RandomID: 100, Message: "must fail", Date: 12, ClearDraft: true,
	})
	if !errors.Is(err, storepkg.ErrDeliveryOutboxRequired) {
		t.Fatalf("error=%v, want aggregate dependency failure", err)
	}
	if len(channels.messages[created.Channel.ID]) != beforeMessages || channels.ptsSeq[created.Channel.ID] != beforePTS {
		t.Fatalf("failed clear-draft send mutated channel state")
	}
}

func TestMemoryMonoforumSendClearDraftCommitsAccountEvent(t *testing.T) {
	ctx := context.Background()
	dialogs := NewDialogStore()
	events := NewUpdateEventStore()
	dialogs.AttachUpdateEventStore(events)
	channels := newChannelStoreWithDelivery()
	dialogs.BindDialogAggregateStores(channels, nil)
	broadcast, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{CreatorUserID: 1, Title: "DM", Broadcast: true, Date: 10})
	if err != nil {
		t.Fatal(err)
	}
	enabled, err := channels.SetPaidMessagesPrice(ctx, 1, broadcast.Channel.ID, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	peer := domain.Peer{Type: domain.PeerTypeChannel, ID: enabled.Channel.LinkedMonoforumID}
	if err := dialogs.SaveDraft(ctx, 42, domain.DialogDraft{Peer: peer, Message: "pending", Date: 11}); err != nil {
		t.Fatal(err)
	}
	result, err := channels.SendMonoforumMessage(ctx, domain.SendMonoforumMessageRequest{
		MonoforumID: peer.ID, SenderUserID: 42, SavedPeer: domain.Peer{Type: domain.PeerTypeUser, ID: 42},
		RandomID: 101, Message: "sent", Date: 12, ClearDraft: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.DraftEvent.Pts != 1 || result.DraftEvent.Peer != peer {
		t.Fatalf("unexpected monoforum draft event: %+v", result.DraftEvent)
	}
	if _, found, _ := dialogs.GetDraft(ctx, 42, peer, 0); found {
		t.Fatal("monoforum draft survived committed send")
	}
}
