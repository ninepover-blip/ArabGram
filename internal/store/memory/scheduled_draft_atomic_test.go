package memory

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
	storepkg "telesrv/internal/store"
)

func TestMemoryScheduleMessageClearDraftIsOneAggregate(t *testing.T) {
	ctx := context.Background()
	dialogs := NewDialogStore()
	events := NewUpdateEventStore()
	dialogs.AttachUpdateEventStore(events)
	messages := NewMessageStore(dialogs)
	messages.AttachUpdateEventStore(events)
	outbox := NewDeliveryOutboxStore()
	messages.AttachDeliveryOutbox(outbox)
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: 202}
	if err := dialogs.SaveDraft(ctx, 101, domain.DialogDraft{Peer: peer, Message: "pending", Date: 5}); err != nil {
		t.Fatal(err)
	}
	request := domain.ScheduleMessageRequest{
		OwnerUserID: 101, Peer: peer, RandomID: 77, Message: "later",
		ScheduleDate: 1000, Date: 10, ClearDraft: true,
	}
	build := func(msg domain.ScheduledMessage) ([]storepkg.DeliveryEffect, error) {
		return []storepkg.DeliveryEffect{memoryAbsoluteEffect(msg.OwnerUserID)}, nil
	}
	if _, err := messages.CreateScheduledMessage(ctx, request, build); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := dialogs.GetDraft(ctx, 101, peer, 0); found {
		t.Fatal("draft survived scheduled-message commit")
	}
	got, _ := events.ListAfter(ctx, 101, 0, 10)
	if len(got) != 1 || got[0].Type != domain.UpdateEventDraftMessage || got[0].Pts != 1 {
		t.Fatalf("unexpected scheduled draft events: %+v", got)
	}
	if items := outbox.Snapshot(); len(items) != 1 {
		t.Fatalf("scheduled absolute outbox=%+v, want one", items)
	}
}

func TestMemoryScheduleMessageClearDraftBuilderFailureRollsBackEverything(t *testing.T) {
	ctx := context.Background()
	dialogs := NewDialogStore()
	events := NewUpdateEventStore()
	messages := NewMessageStore(dialogs)
	messages.AttachUpdateEventStore(events)
	outbox := NewDeliveryOutboxStore()
	messages.AttachDeliveryOutbox(outbox)
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: 202}
	if err := dialogs.SaveDraft(ctx, 101, domain.DialogDraft{Peer: peer, Message: "pending", Date: 5}); err != nil {
		t.Fatal(err)
	}
	want := errors.New("projection failed")
	_, err := messages.CreateScheduledMessage(ctx, domain.ScheduleMessageRequest{
		OwnerUserID: 101, Peer: peer, RandomID: 78, Message: "later",
		ScheduleDate: 1000, Date: 10, ClearDraft: true,
	}, func(domain.ScheduledMessage) ([]storepkg.DeliveryEffect, error) { return nil, want })
	if !errors.Is(err, want) {
		t.Fatalf("error=%v, want builder failure", err)
	}
	if _, found, _ := dialogs.GetDraft(ctx, 101, peer, 0); !found {
		t.Fatal("failed scheduled aggregate deleted draft")
	}
	list, _ := messages.ListScheduledMessages(ctx, domain.ScheduledMessageFilter{OwnerUserID: 101, Peer: peer})
	if len(list.Messages) != 0 || len(outbox.Snapshot()) != 0 {
		t.Fatalf("failed scheduled aggregate leaked state: messages=%+v outbox=%+v", list.Messages, outbox.Snapshot())
	}
	got, _ := events.ListAfter(ctx, 101, 0, 10)
	if len(got) != 0 {
		t.Fatalf("failed scheduled aggregate leaked events: %+v", got)
	}
}
