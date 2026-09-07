package memory

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestMemorySavedDialogMutationIsAtomicExactAndContinuous(t *testing.T) {
	ctx := context.Background()
	events := NewUpdateEventStore()
	messages := NewMessageStore()
	messages.AttachUpdateEventStore(events)
	userID := int64(401)
	first := domain.Peer{Type: domain.PeerTypeUser, ID: 402}
	second := domain.Peer{Type: domain.PeerTypeChannel, ID: 403}

	failed := store.SavedDialogMutation{
		Kind: store.SavedDialogSetPinned, UserID: userID, Date: 91, Peer: first, Pinned: true,
	}
	wantErr := errors.New("encode failed")
	if _, err := messages.MutateSavedDialogs(ctx, failed, func(store.SavedDialogMutationSnapshot) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("builder failure err=%v", err)
	}
	if _, err := messages.MutateSavedDialogs(ctx, failed, func(snapshot store.SavedDialogMutationSnapshot) ([]store.DeliveryEffect, error) {
		return []store.DeliveryEffect{store.AccountPTSDeliveryEffect(userID+1, domain.UpdateEvent{
			Type: domain.UpdateEventSavedDialogPinned, Peer: first, Bool: true, Date: failed.Date, PtsCount: 1,
		}, [8]byte{}, 0)}, nil
	}); err == nil {
		t.Fatal("wrong target effect was accepted")
	}
	messages.mu.RLock()
	if len(messages.savedPins[userID]) != 0 {
		t.Fatalf("rejected mutation changed pins: %+v", messages.savedPins[userID])
	}
	messages.mu.RUnlock()
	if got, err := events.ListAfter(ctx, userID, 0, 10); err != nil || len(got) != 0 {
		t.Fatalf("rejected mutation appended events: %+v err=%v", got, err)
	}

	apply := func(mutation store.SavedDialogMutation) store.SavedDialogMutationSnapshot {
		snapshot, err := messages.MutateSavedDialogs(ctx, mutation, memorySavedDialogEffects)
		if err != nil {
			t.Fatalf("mutate saved dialogs (%s): %v", mutation.Kind, err)
		}
		return snapshot
	}
	one := apply(failed)
	if !one.Changed || len(one.Effects) != 1 || one.Effects[0].Event.Pts != 1 {
		t.Fatalf("first pin result=%+v", one)
	}
	noop := apply(store.SavedDialogMutation{
		Kind: store.SavedDialogSetPinned, UserID: userID, Date: 92, Peer: first, Pinned: true,
	})
	if noop.Changed || len(noop.Effects) != 0 {
		t.Fatalf("no-op consumed an effect: %+v", noop)
	}
	two := apply(store.SavedDialogMutation{
		Kind: store.SavedDialogSetPinned, UserID: userID, Date: 93, Peer: second, Pinned: true,
	})
	if len(two.Effects) != 1 || two.Effects[0].Event.Pts != 2 {
		t.Fatalf("second pin result=%+v", two)
	}
	reordered := apply(store.SavedDialogMutation{
		Kind: store.SavedDialogReorder, UserID: userID, Date: 94,
		Order: []domain.Peer{first, second}, Force: true,
	})
	if !reordered.Changed || len(reordered.Effects) != 1 || reordered.Effects[0].Event.Pts != 3 ||
		len(reordered.Effects[0].Event.Peers) != 2 || reordered.Effects[0].Event.Peers[0] != first || reordered.Effects[0].Event.Peers[1] != second {
		t.Fatalf("reorder result=%+v", reordered)
	}

	got, err := events.ListAfter(ctx, userID, 0, 10)
	if err != nil || len(got) != 3 || got[0].Pts != 1 || got[1].Pts != 2 || got[2].Pts != 3 {
		t.Fatalf("event stream=%+v err=%v", got, err)
	}
	events.mu.RLock()
	dispatches := append([]memoryUpdateDispatch(nil), events.dispatches[userID]...)
	events.mu.RUnlock()
	if len(dispatches) != 3 || dispatches[0].Pts != 1 || dispatches[1].Pts != 2 || dispatches[2].Pts != 3 {
		t.Fatalf("dispatch stream=%+v", dispatches)
	}
}

func memorySavedDialogEffects(snapshot store.SavedDialogMutationSnapshot) ([]store.DeliveryEffect, error) {
	if !snapshot.Changed {
		return nil, nil
	}
	event := domain.UpdateEvent{Date: snapshot.Mutation.Date, PtsCount: 1}
	switch snapshot.Mutation.Kind {
	case store.SavedDialogSetPinned:
		event.Type = domain.UpdateEventSavedDialogPinned
		event.Peer = snapshot.Mutation.Peer
		event.Bool = snapshot.Mutation.Pinned
	case store.SavedDialogReorder:
		event.Type = domain.UpdateEventPinnedSavedDialogs
		event.Peers = append([]domain.Peer(nil), snapshot.Order...)
	}
	return []store.DeliveryEffect{store.AccountPTSDeliveryEffect(snapshot.Mutation.UserID, event, [8]byte{}, 0)}, nil
}
