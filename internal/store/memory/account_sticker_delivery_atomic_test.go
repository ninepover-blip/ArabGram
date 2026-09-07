package memory

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestStickerCollectionMutationRollsBackWhenDeliveryFails(t *testing.T) {
	ctx := context.Background()
	passwords := NewPasswordStore()
	outbox := NewDeliveryOutboxStore()
	passwords.AttachDeliveryOutbox(outbox)
	mutation := domain.StickerCollectionMutation{
		OwnerUserID: 101, Kind: domain.StickerCollectionGif,
		Mutation: domain.StickerCollectionMutationSave, DocumentID: 9001, Date: 77,
	}
	wantErr := errors.New("projection failed")
	err := passwords.MutateStickerCollection(ctx, mutation, func(domain.StickerCollectionMutation) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("mutation error = %v, want projection failure", err)
	}
	items, err := passwords.ListStickerCollection(ctx, mutation.OwnerUserID, mutation.Kind, 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("collection after rollback = %+v, err %v", items, err)
	}
	if got := outbox.Snapshot(); len(got) != 0 {
		t.Fatalf("outbox after rollback = %+v", got)
	}
}

func TestStickerCollectionMutationWithoutOutboxFailsClosed(t *testing.T) {
	ctx := context.Background()
	passwords := NewPasswordStore()
	mutation := domain.StickerCollectionMutation{
		OwnerUserID: 111, Kind: domain.StickerCollectionFaved,
		Mutation: domain.StickerCollectionMutationSave, DocumentID: 7001, Date: 88,
	}
	err := passwords.MutateStickerCollection(ctx, mutation, absoluteStickerTestEffects[domain.StickerCollectionMutation](mutation.OwnerUserID, []byte{1}))
	if !errors.Is(err, store.ErrDeliveryOutboxRequired) {
		t.Fatalf("mutation error = %v, want ErrDeliveryOutboxRequired", err)
	}
	items, listErr := passwords.ListStickerCollection(ctx, mutation.OwnerUserID, mutation.Kind, 10)
	if listErr != nil || len(items) != 0 {
		t.Fatalf("collection after missing outbox = %+v, err %v", items, listErr)
	}
}

func TestUserStickerSetBatchMutationAndDeliveryAreAtomic(t *testing.T) {
	ctx := context.Background()
	passwords := NewPasswordStore()
	outbox := NewDeliveryOutboxStore()
	passwords.AttachDeliveryOutbox(outbox)
	mutation := domain.UserStickerSetMutation{
		OwnerUserID: 202, Mutation: domain.UserStickerSetMutationInstall, Date: 123,
		Items: []domain.UserStickerSetMutationItem{
			{StickerSetID: 11, Kind: domain.StickerSetKindStickers},
			{StickerSetID: 22, Kind: domain.StickerSetKindEmoji},
			{StickerSetID: 33, Kind: domain.StickerSetKindMasks},
		},
	}
	payload := []byte{9, 8, 7}
	if err := passwords.MutateUserStickerSets(ctx, mutation, absoluteStickerTestEffects[domain.UserStickerSetMutation](mutation.OwnerUserID, payload)); err != nil {
		t.Fatalf("batch install: %v", err)
	}
	for _, item := range mutation.Items {
		sets, total, err := passwords.ListUserStickerSets(ctx, mutation.OwnerUserID, item.Kind, nil, 0, 10)
		if err != nil || total != 1 || len(sets) != 1 || sets[0].StickerSetID != item.StickerSetID {
			t.Fatalf("kind %q sets=%+v total=%d err=%v", item.Kind, sets, total, err)
		}
	}
	queued := outbox.Snapshot()
	if len(queued) != 1 || string(queued[0].Payload) != string(payload) {
		t.Fatalf("outbox = %+v, want one batch delivery", queued)
	}
}

func TestUserStickerSetBatchRollsBackBeforeAnyOutboxWrite(t *testing.T) {
	ctx := context.Background()
	passwords := NewPasswordStore()
	outbox := NewDeliveryOutboxStore()
	passwords.AttachDeliveryOutbox(outbox)
	mutation := domain.UserStickerSetMutation{
		OwnerUserID: 303, Mutation: domain.UserStickerSetMutationInstall, Date: 456,
		Items: []domain.UserStickerSetMutationItem{
			{StickerSetID: 44, Kind: domain.StickerSetKindStickers},
			{StickerSetID: 55, Kind: domain.StickerSetKindEmoji},
		},
	}
	err := passwords.MutateUserStickerSets(ctx, mutation, func(domain.UserStickerSetMutation) ([]store.DeliveryEffect, error) {
		return []store.DeliveryEffect{
			store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
				TargetUserID: mutation.OwnerUserID, Payload: []byte{1}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
			}),
			store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
				TargetUserID: mutation.OwnerUserID, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
			}),
		}, nil
	})
	if err == nil {
		t.Fatal("batch mutation succeeded with invalid second delivery effect")
	}
	if got := outbox.Snapshot(); len(got) != 0 {
		t.Fatalf("partial outbox writes = %+v", got)
	}
	for _, item := range mutation.Items {
		sets, total, listErr := passwords.ListUserStickerSets(ctx, mutation.OwnerUserID, item.Kind, nil, 0, 10)
		if listErr != nil || total != 0 || len(sets) != 0 {
			t.Fatalf("kind %q survived rollback: sets=%+v total=%d err=%v", item.Kind, sets, total, listErr)
		}
	}
}

func absoluteStickerTestEffects[T any](userID int64, payload []byte) store.DeliveryEffectsBuilder[T] {
	return func(T) ([]store.DeliveryEffect, error) {
		return []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: userID, Payload: append([]byte(nil), payload...), RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		})}, nil
	}
}
