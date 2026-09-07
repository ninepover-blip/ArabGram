package memory

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestMemoryQuickReplyAccountAggregateExactRollbackNoopAndPTS(t *testing.T) {
	ctx := context.Background()
	events := NewUpdateEventStore()
	quickReplies := NewPasswordStore()
	quickReplies.AttachUpdateEventStore(events)
	userID := int64(501)
	firstSave := store.QuickReplyAccountMutation{
		Kind: store.QuickReplyAccountSaveText, UserID: userID, Date: 101, Shortcut: "alpha",
		Message: domain.QuickReplyMessage{RandomID: 11, Date: 101, Message: "first"},
	}

	if _, err := quickReplies.MutateQuickReplies(ctx, firstSave, nil); !errors.Is(err, store.ErrDeliveryOutboxRequired) {
		t.Fatalf("missing builder err=%v", err)
	}
	wantErr := errors.New("encode failed")
	if _, err := quickReplies.MutateQuickReplies(ctx, firstSave, func(store.QuickReplyAccountMutationSnapshot) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("builder failure err=%v", err)
	}
	if _, err := quickReplies.MutateQuickReplies(ctx, firstSave, func(snapshot store.QuickReplyAccountMutationSnapshot) ([]store.DeliveryEffect, error) {
		effects, err := memoryQuickReplyAccountEffects(snapshot)
		if len(effects) == 1 {
			effects[0].TargetUserID++
		}
		return effects, err
	}); err == nil {
		t.Fatal("wrong-target effect was accepted")
	}
	if list, err := quickReplies.ListQuickReplies(ctx, userID, true); err != nil || len(list.QuickReplies) != 0 {
		t.Fatalf("rejected save changed state: %+v err=%v", list, err)
	}
	if got, _ := events.ListAfter(ctx, userID, 0, 10); len(got) != 0 {
		t.Fatalf("rejected save appended events: %+v", got)
	}

	apply := func(mutation store.QuickReplyAccountMutation) store.QuickReplyAccountMutationSnapshot {
		t.Helper()
		snapshot, err := quickReplies.MutateQuickReplies(ctx, mutation, memoryQuickReplyAccountEffects)
		if err != nil {
			t.Fatalf("mutate quick replies (%s): %v", mutation.Kind, err)
		}
		return snapshot
	}
	first := apply(firstSave)
	if !first.Changed || first.Result.ShortcutID != 1 || first.Result.Message.ID != 1 || len(first.Effects) != 1 || first.Effects[0].Event.Pts != 1 {
		t.Fatalf("first save=%+v", first)
	}
	second := apply(store.QuickReplyAccountMutation{
		Kind: store.QuickReplyAccountSaveText, UserID: userID, Date: 102, Shortcut: "beta",
		Message: domain.QuickReplyMessage{RandomID: 12, Date: 102, Message: "second"},
	})
	if second.Result.ShortcutID != 2 || second.Result.Message.ID != 2 || second.Effects[0].Event.Pts != 2 {
		t.Fatalf("second save=%+v", second)
	}
	noopRename := apply(store.QuickReplyAccountMutation{
		Kind: store.QuickReplyAccountRenameShortcut, UserID: userID, Date: 103, ShortcutID: 1, Shortcut: "alpha",
	})
	if noopRename.Changed || len(noopRename.Effects) != 0 {
		t.Fatalf("same-name rename consumed PTS: %+v", noopRename)
	}
	renamed := apply(store.QuickReplyAccountMutation{
		Kind: store.QuickReplyAccountRenameShortcut, UserID: userID, Date: 104, ShortcutID: 1, Shortcut: "renamed",
	})
	if !renamed.Changed || renamed.Effects[0].Event.Pts != 3 || renamed.Effects[0].Event.Type != domain.UpdateEventQuickReplies {
		t.Fatalf("rename=%+v", renamed)
	}
	reordered := apply(store.QuickReplyAccountMutation{
		Kind: store.QuickReplyAccountReorder, UserID: userID, Date: 105, Order: []int{2, 1},
	})
	if !reordered.Changed || reordered.Effects[0].Event.Pts != 4 || len(reordered.Result.List.QuickReplies) != 2 ||
		reordered.Result.List.QuickReplies[0].ID != 2 || reordered.Result.List.QuickReplies[1].ID != 1 {
		t.Fatalf("reorder=%+v", reordered)
	}
	noopReorder := apply(store.QuickReplyAccountMutation{
		Kind: store.QuickReplyAccountReorder, UserID: userID, Date: 106, Order: []int{2, 1},
	})
	if noopReorder.Changed || len(noopReorder.Effects) != 0 {
		t.Fatalf("same order consumed PTS: %+v", noopReorder)
	}

	_, err := quickReplies.MutateQuickReplies(ctx, store.QuickReplyAccountMutation{
		Kind: store.QuickReplyAccountDeleteMessages, UserID: userID, Date: 107,
		ShortcutID: 1, MessageIDs: []int{1, 999},
	}, memoryQuickReplyAccountEffects)
	if !errors.Is(err, domain.ErrShortcutInvalid) {
		t.Fatalf("mixed valid/invalid delete err=%v", err)
	}
	if messages, err := quickReplies.GetQuickReplyMessages(ctx, userID, 1, nil); err != nil || len(messages.Messages) != 1 || messages.Messages[0].ID != 1 {
		t.Fatalf("failed delete partially removed messages: %+v err=%v", messages, err)
	}
	deletedMessages := apply(store.QuickReplyAccountMutation{
		Kind: store.QuickReplyAccountDeleteMessages, UserID: userID, Date: 108,
		ShortcutID: 1, MessageIDs: []int{1},
	})
	if deletedMessages.Effects[0].Event.Pts != 5 || deletedMessages.Effects[0].Event.Type != domain.UpdateEventDeleteQuickReplyMessages {
		t.Fatalf("delete messages=%+v", deletedMessages)
	}
	deletedShortcut := apply(store.QuickReplyAccountMutation{
		Kind: store.QuickReplyAccountDeleteShortcut, UserID: userID, Date: 109, ShortcutID: 2,
	})
	if deletedShortcut.Effects[0].Event.Pts != 6 || deletedShortcut.Effects[0].Event.Type != domain.UpdateEventDeleteQuickReply {
		t.Fatalf("delete shortcut=%+v", deletedShortcut)
	}

	got, err := events.ListAfter(ctx, userID, 0, 10)
	if err != nil || len(got) != 6 {
		t.Fatalf("events=%+v err=%v", got, err)
	}
	for i, event := range got {
		if event.Pts != i+1 || event.PtsCount != 1 {
			t.Fatalf("non-contiguous event[%d]=%+v", i, event)
		}
	}
	events.mu.RLock()
	dispatches := append([]memoryUpdateDispatch(nil), events.dispatches[userID]...)
	events.mu.RUnlock()
	if len(dispatches) != 6 {
		t.Fatalf("dispatches=%+v", dispatches)
	}
	for i, dispatch := range dispatches {
		if dispatch.Pts != i+1 || dispatch.ExcludeAuthKeyID != ([8]byte{}) || dispatch.ExcludeSessionID != 0 {
			t.Fatalf("dispatch[%d]=%+v", i, dispatch)
		}
	}
}

func memoryQuickReplyAccountEffects(snapshot store.QuickReplyAccountMutationSnapshot) ([]store.DeliveryEffect, error) {
	if !snapshot.Changed {
		return nil, nil
	}
	result := snapshot.Result
	event := domain.UpdateEvent{
		Date: snapshot.Mutation.Date, PtsCount: 1, MaxID: result.ShortcutID,
		QuickReplies: append([]domain.QuickReply(nil), result.List.QuickReplies...),
		QuickReply:   result.QuickReply, QuickReplyMessage: result.Message,
		MessageIDs: append([]int(nil), result.MessageIDs...),
	}
	switch result.Kind {
	case domain.QuickReplyMutationNew:
		event.Type = domain.UpdateEventNewQuickReply
	case domain.QuickReplyMutationMessage:
		event.Type = domain.UpdateEventQuickReplyMessage
	case domain.QuickReplyMutationDelete:
		event.Type = domain.UpdateEventDeleteQuickReply
	case domain.QuickReplyMutationIDs:
		event.Type = domain.UpdateEventDeleteQuickReplyMessages
	default:
		event.Type = domain.UpdateEventQuickReplies
	}
	return []store.DeliveryEffect{store.AccountPTSDeliveryEffect(snapshot.Mutation.UserID, event, [8]byte{}, 0)}, nil
}
