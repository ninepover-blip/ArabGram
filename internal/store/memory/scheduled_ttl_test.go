package memory

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
	storepkg "telesrv/internal/store"
)

func TestScheduledMessageMemoryStoreRequiresAtomicDelivery(t *testing.T) {
	ctx := context.Background()
	dialogs := NewDialogStore()
	messages := NewMessageStore(dialogs)
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: 2002}
	req := domain.ScheduleMessageRequest{
		OwnerUserID: 2001, Peer: peer, RandomID: 91, Message: "later",
		ScheduleDate: 1_700_003_600, Date: 1_700_000_000,
	}
	build := func(msg domain.ScheduledMessage) ([]storepkg.DeliveryEffect, error) {
		return []storepkg.DeliveryEffect{memoryAbsoluteEffect(msg.OwnerUserID)}, nil
	}
	if _, err := messages.CreateScheduledMessage(ctx, req, build); !errors.Is(err, storepkg.ErrDeliveryOutboxRequired) {
		t.Fatalf("create without outbox err = %v, want ErrDeliveryOutboxRequired", err)
	}
	if list, err := messages.ListScheduledMessages(ctx, domain.ScheduledMessageFilter{OwnerUserID: req.OwnerUserID, Peer: peer}); err != nil || len(list.Messages) != 0 {
		t.Fatalf("failed create leaked state: list=%+v err=%v", list, err)
	}

	outbox := NewDeliveryOutboxStore()
	messages.AttachDeliveryOutbox(outbox)
	created, err := messages.CreateScheduledMessage(ctx, req, build)
	if err != nil {
		t.Fatalf("create scheduled message: %v", err)
	}
	if created.ID <= 0 || created.State != "pending" {
		t.Fatalf("created = %+v, want pending id", created)
	}
	if has, err := messages.HasScheduledMessages(ctx, req.OwnerUserID, peer); err != nil || !has {
		t.Fatalf("has scheduled = %v, err=%v", has, err)
	}
	dialogList, err := dialogs.ListByPeers(ctx, req.OwnerUserID, []domain.Peer{peer})
	if err != nil || len(dialogList.Dialogs) != 1 || !dialogList.Dialogs[0].HasScheduled {
		t.Fatalf("dialog scheduled projection = %+v, err=%v", dialogList.Dialogs, err)
	}

	if _, err := messages.EditScheduledMessage(ctx, domain.EditScheduledMessageRequest{
		OwnerUserID: req.OwnerUserID, Peer: peer, ID: created.ID,
		SetMessage: true, Message: "must roll back", ScheduleDate: req.ScheduleDate + 60, Date: req.Date + 1,
	}, func(domain.ScheduledMessage) ([]storepkg.DeliveryEffect, error) { return nil, nil }); !errors.Is(err, storepkg.ErrDeliveryOutboxRequired) {
		t.Fatalf("edit without effect err = %v, want ErrDeliveryOutboxRequired", err)
	}
	got, err := messages.GetScheduledMessages(ctx, domain.ScheduledMessageFilter{OwnerUserID: req.OwnerUserID, Peer: peer, IDs: []int{created.ID}})
	if err != nil || len(got.Messages) != 1 || got.Messages[0].Message != req.Message || got.Messages[0].ScheduleDate != req.ScheduleDate {
		t.Fatalf("failed edit changed state: %+v err=%v", got.Messages, err)
	}
	if items := outbox.Snapshot(); len(items) != 1 {
		t.Fatalf("outbox items = %+v, want create only", items)
	}
}

func TestPrivateHistoryTTLMemoryStoreCommitsBothOwnersWithDelivery(t *testing.T) {
	ctx := context.Background()
	dialogs := NewDialogStore()
	messages := NewMessageStore(dialogs)
	outbox := NewDeliveryOutboxStore()
	messages.AttachDeliveryOutbox(outbox)
	const ownerID int64 = 3001
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: 3002}

	oneEffect := func(domain.PrivateHistoryTTLResult) ([]storepkg.DeliveryEffect, error) {
		return []storepkg.DeliveryEffect{memoryAbsoluteEffect(ownerID)}, nil
	}
	if err := messages.SetPrivateHistoryTTL(ctx, ownerID, peer, 3600, oneEffect); !errors.Is(err, storepkg.ErrDeliveryOutboxRequired) {
		t.Fatalf("one-sided TTL delivery err = %v, want ErrDeliveryOutboxRequired", err)
	}
	if period, err := messages.GetPrivateHistoryTTL(ctx, ownerID, peer); err != nil || period != 0 {
		t.Fatalf("failed TTL mutation leaked owner state: period=%d err=%v", period, err)
	}
	if items := outbox.Snapshot(); len(items) != 0 {
		t.Fatalf("invalid TTL effect count enqueued items: %+v", items)
	}

	twoEffects := func(res domain.PrivateHistoryTTLResult) ([]storepkg.DeliveryEffect, error) {
		return []storepkg.DeliveryEffect{memoryAbsoluteEffect(res.OwnerUserID), memoryAbsoluteEffect(res.Peer.ID)}, nil
	}
	if err := messages.SetPrivateHistoryTTL(ctx, ownerID, peer, 3600, twoEffects); err != nil {
		t.Fatalf("set private TTL: %v", err)
	}
	if period, err := messages.GetPrivateHistoryTTL(ctx, ownerID, peer); err != nil || period != 3600 {
		t.Fatalf("owner TTL period=%d err=%v, want 3600", period, err)
	}
	reverse := domain.Peer{Type: domain.PeerTypeUser, ID: ownerID}
	if period, err := messages.GetPrivateHistoryTTL(ctx, peer.ID, reverse); err != nil || period != 3600 {
		t.Fatalf("peer TTL period=%d err=%v, want 3600", period, err)
	}
	if items := outbox.Snapshot(); len(items) != 2 {
		t.Fatalf("TTL outbox items=%+v, want both owners", items)
	}
}

func memoryAbsoluteEffect(userID int64) storepkg.DeliveryEffect {
	return storepkg.AbsoluteDeliveryEffect(storepkg.DeliveryOutboxEnqueue{
		TargetUserID: userID, Payload: []byte{1}, RecoveryPolicy: storepkg.OutboxRecoveryAbsoluteReload,
	})
}
