package memory

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestMemoryPrivateSendClearDraftCommitsOneContiguousAggregate(t *testing.T) {
	ctx := context.Background()
	dialogs := NewDialogStore()
	events := NewUpdateEventStore()
	dialogs.AttachUpdateEventStore(events)
	messages := NewMessageStore(dialogs)
	messages.AttachUpdateEventStore(events)
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: 202}
	if err := dialogs.SaveDraft(ctx, 101, domain.DialogDraft{Peer: peer, Message: "pending", Date: 7}); err != nil {
		t.Fatal(err)
	}

	request := domain.SendPrivateTextRequest{
		SenderUserID: 101, RecipientUserID: peer.ID, RandomID: 9001,
		Message: "sent", Date: 10, ClearDraft: true,
	}
	result, err := messages.SendPrivateText(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if result.SenderEvent.Pts != 1 || result.DraftEvent.Pts != 2 || result.DraftEvent.Type != domain.UpdateEventDraftMessage {
		t.Fatalf("unexpected allocated events: sender=%+v draft=%+v", result.SenderEvent, result.DraftEvent)
	}
	if _, found, err := dialogs.GetDraft(ctx, 101, peer, 0); err != nil || found {
		t.Fatalf("draft survived committed send: found=%v err=%v", found, err)
	}
	got, err := events.ListAfter(ctx, 101, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Type != domain.UpdateEventNewMessage || got[0].Pts != 1 || got[1].Type != domain.UpdateEventDraftMessage || got[1].Pts != 2 {
		t.Fatalf("non-contiguous durable event stream: %+v", got)
	}
	events.mu.RLock()
	dispatches := append([]memoryUpdateDispatch(nil), events.dispatches[101]...)
	events.mu.RUnlock()
	if len(dispatches) != 2 || dispatches[0].Pts != 1 || dispatches[1].Pts != 2 {
		t.Fatalf("unexpected dispatch stream: %+v", dispatches)
	}

	replay, err := messages.SendPrivateText(ctx, request)
	if err != nil || !replay.Duplicate {
		t.Fatalf("exact replay: duplicate=%v err=%v", replay.Duplicate, err)
	}
	got, _ = events.ListAfter(ctx, 101, 0, 10)
	if len(got) != 2 {
		t.Fatalf("replay appended events: %+v", got)
	}
}

func TestMemoryPrivateSendClearDraftFailsBeforeMessageWithoutDialogAggregate(t *testing.T) {
	ctx := context.Background()
	messages := NewMessageStore()
	_, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID: 101, RecipientUserID: 202, RandomID: 9002,
		Message: "must not commit", Date: 10, ClearDraft: true,
	})
	if !errors.Is(err, store.ErrDeliveryOutboxRequired) {
		t.Fatalf("error=%v, want durable aggregate dependency failure", err)
	}
	messages.mu.RLock()
	defer messages.mu.RUnlock()
	if len(messages.m[101]) != 0 || messages.nextUID != 1 || messages.nextPts[101] != 0 {
		t.Fatalf("failed send mutated memory state: messages=%+v next_uid=%d pts=%d", messages.m[101], messages.nextUID, messages.nextPts[101])
	}
}

func TestMemoryPrivateSendClearDraftNoopDoesNotConsumeExtraPTS(t *testing.T) {
	ctx := context.Background()
	dialogs := NewDialogStore()
	events := NewUpdateEventStore()
	messages := NewMessageStore(dialogs)
	messages.AttachUpdateEventStore(events)
	result, err := messages.SendPrivateText(ctx, domain.SendPrivateTextRequest{
		SenderUserID: 101, RecipientUserID: 202, RandomID: 9003,
		Message: "sent", Date: 10, ClearDraft: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.SenderEvent.Pts != 1 || result.DraftEvent.Pts != 0 {
		t.Fatalf("no-op clear consumed PTS: sender=%+v draft=%+v", result.SenderEvent, result.DraftEvent)
	}
	got, _ := events.ListAfter(ctx, 101, 0, 10)
	if len(got) != 1 || got[0].Pts != 1 {
		t.Fatalf("unexpected events after no-op clear: %+v", got)
	}
}
