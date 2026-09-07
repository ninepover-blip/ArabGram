package memory

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestDialogAccountDraftMutationIsAtomicAndContinuous(t *testing.T) {
	ctx := context.Background()
	dialogs := NewDialogStore()
	events := NewUpdateEventStore()
	dialogs.AttachUpdateEventStore(events)
	mutation := store.DialogAccountMutation{
		Kind: store.DialogAccountSaveDraft, UserID: 101, Date: 77,
		Draft: domain.DialogDraft{Peer: domain.Peer{Type: domain.PeerTypeUser, ID: 202}, Date: 77, Message: "draft"},
	}
	if _, err := dialogs.MutateAccountDialogs(ctx, mutation, nil); !errors.Is(err, store.ErrDeliveryOutboxRequired) {
		t.Fatalf("missing builder err = %v", err)
	}
	if _, found, err := dialogs.GetDraft(ctx, mutation.UserID, mutation.Draft.Peer, 0); err != nil || found {
		t.Fatalf("draft survived missing builder: found=%v err=%v", found, err)
	}
	wantErr := errors.New("encode failed")
	if _, err := dialogs.MutateAccountDialogs(ctx, mutation, func(store.DialogAccountMutationSnapshot) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("builder failure err = %v", err)
	}
	if _, found, err := dialogs.GetDraft(ctx, mutation.UserID, mutation.Draft.Peer, 0); err != nil || found {
		t.Fatalf("draft survived builder failure: found=%v err=%v", found, err)
	}

	build := func(snapshot store.DialogAccountMutationSnapshot) ([]store.DeliveryEffect, error) {
		if !snapshot.Changed {
			return nil, nil
		}
		return []store.DeliveryEffect{store.AccountPTSDeliveryEffect(mutation.UserID, domain.UpdateEvent{
			Type: domain.UpdateEventDraftMessage, Peer: mutation.Draft.Peer,
			MaxID: mutation.Draft.TopMessageID, PtsCount: 1, Date: mutation.Date,
		}, [8]byte{}, 0)}, nil
	}
	result, err := dialogs.MutateAccountDialogs(ctx, mutation, build)
	if err != nil {
		t.Fatalf("save draft aggregate: %v", err)
	}
	if !result.Changed || len(result.Effects) != 1 || result.Effects[0].Event.Pts != 1 {
		t.Fatalf("save result = %+v", result)
	}
	if _, found, err := dialogs.GetDraft(ctx, mutation.UserID, mutation.Draft.Peer, 0); err != nil || !found {
		t.Fatalf("draft missing after commit: found=%v err=%v", found, err)
	}

	// Date is not draft content. The same value is a no-op and cannot allocate
	// another event or dispatch row.
	mutation.Draft.Date++
	result, err = dialogs.MutateAccountDialogs(ctx, mutation, build)
	if err != nil || result.Changed || len(result.Effects) != 0 {
		t.Fatalf("same draft replay = %+v err=%v", result, err)
	}
	got, err := events.ListAfter(ctx, mutation.UserID, 0, 10)
	if err != nil || len(got) != 1 || got[0].Pts != 1 {
		t.Fatalf("events = %+v err=%v", got, err)
	}
	events.mu.RLock()
	dispatches := append([]memoryUpdateDispatch(nil), events.dispatches[mutation.UserID]...)
	events.mu.RUnlock()
	if len(dispatches) != 1 || dispatches[0].ExcludeAuthKeyID != ([8]byte{}) || dispatches[0].ExcludeSessionID != 0 {
		t.Fatalf("dispatches = %+v", dispatches)
	}
}

func TestDialogAccountRejectsNonExactEffectBeforeMutation(t *testing.T) {
	ctx := context.Background()
	dialogs := NewDialogStore()
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: 302}
	if err := dialogs.Upsert(ctx, 301, domain.Dialog{Peer: peer}); err != nil {
		t.Fatal(err)
	}
	mutation := store.DialogAccountMutation{
		Kind: store.DialogAccountSetUnreadMark, UserID: 301, Peer: peer, Value: true, Date: 88,
	}
	_, err := dialogs.MutateAccountDialogs(ctx, mutation, func(store.DialogAccountMutationSnapshot) ([]store.DeliveryEffect, error) {
		return []store.DeliveryEffect{store.AccountPTSDeliveryEffect(999, domain.UpdateEvent{
			Type: domain.UpdateEventDialogUnreadMark, Peer: peer, Bool: true, PtsCount: 1,
		}, [8]byte{}, 0)}, nil
	})
	if err == nil {
		t.Fatal("wrong target effect was accepted")
	}
	list, err := dialogs.ListByPeers(ctx, mutation.UserID, []domain.Peer{peer})
	if err != nil || len(list.Dialogs) != 1 || list.Dialogs[0].UnreadMark {
		t.Fatalf("state changed after rejected effect: %+v err=%v", list, err)
	}
}
