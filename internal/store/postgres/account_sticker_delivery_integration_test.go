package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestStickerCollectionDeliveryEffectFailureRollsBackPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	passwords := NewPasswordStore(pool)
	owner := createTestUser(t, ctx, NewUserStore(pool), "+1886"+randomSuffix(t)+"01", "StickerRollback", "").ID
	cleanupAccountStickerDeliveryTest(t, pool, owner)

	wantErr := errors.New("encode failed")
	err := passwords.MutateStickerCollection(ctx, domain.StickerCollectionMutation{
		OwnerUserID: owner, Kind: domain.StickerCollectionGif,
		Mutation: domain.StickerCollectionMutationSave, DocumentID: 7001, Date: 100,
	}, func(domain.StickerCollectionMutation) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("mutation error = %v, want encode failure", err)
	}
	var collectionRows, deliveryRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM user_sticker_collections WHERE owner_user_id = $1`, owner).Scan(&collectionRows); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = $1`, owner).Scan(&deliveryRows); err != nil {
		t.Fatal(err)
	}
	if collectionRows != 0 || deliveryRows != 0 {
		t.Fatalf("rollback left collection=%d delivery=%d rows", collectionRows, deliveryRows)
	}
}

func TestUserStickerSetVectorMutationUsesOneAtomicDeliveryPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	passwords := NewPasswordStore(pool)
	owner := createTestUser(t, ctx, NewUserStore(pool), "+1886"+randomSuffix(t)+"02", "StickerVector", "").ID
	cleanupAccountStickerDeliveryTest(t, pool, owner)

	mutation := domain.UserStickerSetMutation{
		OwnerUserID: owner, Mutation: domain.UserStickerSetMutationInstall, Date: 200,
		Items: []domain.UserStickerSetMutationItem{
			{StickerSetID: 101, Kind: domain.StickerSetKindStickers},
			{StickerSetID: 202, Kind: domain.StickerSetKindEmoji},
			{StickerSetID: 303, Kind: domain.StickerSetKindMasks},
		},
	}
	if err := passwords.MutateUserStickerSets(ctx, mutation, func(domain.UserStickerSetMutation) ([]store.DeliveryEffect, error) {
		return []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: owner, Payload: []byte{4, 5, 6}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		})}, nil
	}); err != nil {
		t.Fatalf("batch install: %v", err)
	}
	var installedRows, deliveryRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM user_sticker_sets WHERE owner_user_id = $1`, owner).Scan(&installedRows); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = $1`, owner).Scan(&deliveryRows); err != nil {
		t.Fatal(err)
	}
	if installedRows != len(mutation.Items) || deliveryRows != 1 {
		t.Fatalf("batch rows installed=%d delivery=%d, want %d/1", installedRows, deliveryRows, len(mutation.Items))
	}
}

func TestUserStickerSetInvalidEffectRollsBackWholeVectorPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	passwords := NewPasswordStore(pool)
	owner := createTestUser(t, ctx, NewUserStore(pool), "+1886"+randomSuffix(t)+"03", "StickerInvalid", "").ID
	cleanupAccountStickerDeliveryTest(t, pool, owner)

	mutation := domain.UserStickerSetMutation{
		OwnerUserID: owner, Mutation: domain.UserStickerSetMutationInstall, Date: 300,
		Items: []domain.UserStickerSetMutationItem{
			{StickerSetID: 404, Kind: domain.StickerSetKindStickers},
			{StickerSetID: 505, Kind: domain.StickerSetKindEmoji},
		},
	}
	err := passwords.MutateUserStickerSets(ctx, mutation, func(domain.UserStickerSetMutation) ([]store.DeliveryEffect, error) {
		return []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: owner, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		})}, nil
	})
	if err == nil {
		t.Fatal("batch install succeeded with empty delivery payload")
	}
	var installedRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM user_sticker_sets WHERE owner_user_id = $1`, owner).Scan(&installedRows); err != nil {
		t.Fatal(err)
	}
	if installedRows != 0 {
		t.Fatalf("rollback left %d installed rows", installedRows)
	}
}

func cleanupAccountStickerDeliveryTest(t *testing.T, pool *pgxpool.Pool, owner int64) {
	t.Helper()
	ctx := context.Background()
	cleanupRows := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM edge_delivery_outbox_lanes WHERE stream_id = $1`, owner)
		_, _ = pool.Exec(ctx, `DELETE FROM edge_delivery_outbox WHERE target_user_id = $1`, owner)
		_, _ = pool.Exec(ctx, `DELETE FROM user_sticker_collections WHERE owner_user_id = $1`, owner)
		_, _ = pool.Exec(ctx, `DELETE FROM user_sticker_sets WHERE owner_user_id = $1`, owner)
	}
	cleanupRows()
	t.Cleanup(func() {
		cleanupRows()
		_, _ = pool.Exec(ctx, `DELETE FROM users WHERE id = $1`, owner)
	})
}
