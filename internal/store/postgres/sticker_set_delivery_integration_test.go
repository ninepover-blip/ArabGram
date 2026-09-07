package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestCreateInstalledStickerSetAndDeliveryCommitTogetherPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	media := NewMediaStore(pool)
	owner := createTestUser(t, ctx, NewUserStore(pool), "+1886"+randomSuffix(t)+"11", "StickerCreate", "").ID
	setID := int64(8_881_001)
	cleanupStickerSetDeliveryTest(t, pool, owner, setID)
	set := domain.StickerSet{
		ID: setID, AccessHash: setID + 1, CreatorUserID: owner, Creator: true,
		Kind: domain.StickerSetKindStickers, ShortName: fmt.Sprintf("atomic_pack_%d", time.Now().UnixNano()),
		Title: "Atomic Pack", Hash: 1, Installed: true, InstalledDate: 123,
	}
	installation := domain.UserStickerSet{
		OwnerUserID: owner, StickerSetID: set.ID, Kind: set.Kind, InstalledDate: 123,
	}
	if err := media.CreateInstalledStickerSetWithDelivery(ctx, set, nil, installation, func(store.StickerSetMutation) ([]store.DeliveryEffect, error) {
		return []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: owner, Payload: []byte{7}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		})}, nil
	}); err != nil {
		t.Fatalf("create installed sticker set: %v", err)
	}
	var sets, installs, deliveries int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM sticker_sets WHERE id = $1`, set.ID).Scan(&sets)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM user_sticker_sets WHERE owner_user_id = $1 AND sticker_set_id = $2`, owner, set.ID).Scan(&installs)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = $1`, owner).Scan(&deliveries)
	if sets != 1 || installs != 1 || deliveries != 1 {
		t.Fatalf("committed rows set/install/delivery = %d/%d/%d", sets, installs, deliveries)
	}
}

func TestCreateInstalledStickerSetInvalidDeliveryRollsBackPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	media := NewMediaStore(pool)
	owner := createTestUser(t, ctx, NewUserStore(pool), "+1886"+randomSuffix(t)+"12", "StickerCreateBad", "").ID
	setID := int64(8_881_002)
	cleanupStickerSetDeliveryTest(t, pool, owner, setID)
	set := domain.StickerSet{
		ID: setID, AccessHash: setID + 1, CreatorUserID: owner, Creator: true,
		Kind: domain.StickerSetKindEmoji, Emojis: true,
		ShortName: fmt.Sprintf("atomic_emoji_%d", time.Now().UnixNano()), Title: "Atomic Emoji", Hash: 2,
	}
	installation := domain.UserStickerSet{OwnerUserID: owner, StickerSetID: set.ID, Kind: set.Kind, InstalledDate: 456}
	err := media.CreateInstalledStickerSetWithDelivery(ctx, set, nil, installation, func(store.StickerSetMutation) ([]store.DeliveryEffect, error) {
		return []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: owner, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		})}, nil
	})
	if err == nil {
		t.Fatal("create succeeded with invalid delivery payload")
	}
	var sets, installs, deliveries int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM sticker_sets WHERE id = $1`, set.ID).Scan(&sets)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM user_sticker_sets WHERE owner_user_id = $1 AND sticker_set_id = $2`, owner, set.ID).Scan(&installs)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = $1`, owner).Scan(&deliveries)
	if sets != 0 || installs != 0 || deliveries != 0 {
		t.Fatalf("rollback rows set/install/delivery = %d/%d/%d", sets, installs, deliveries)
	}
}

func cleanupStickerSetDeliveryTest(t *testing.T, pool *pgxpool.Pool, owner, setID int64) {
	t.Helper()
	ctx := context.Background()
	cleanupRows := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM edge_delivery_outbox_lanes WHERE stream_id = $1`, owner)
		_, _ = pool.Exec(ctx, `DELETE FROM edge_delivery_outbox WHERE target_user_id = $1`, owner)
		_, _ = pool.Exec(ctx, `DELETE FROM user_sticker_sets WHERE owner_user_id = $1 OR sticker_set_id = $2`, owner, setID)
		_, _ = pool.Exec(ctx, `DELETE FROM sticker_sets WHERE id = $1`, setID)
	}
	cleanupRows()
	t.Cleanup(func() {
		cleanupRows()
		_, _ = pool.Exec(ctx, `DELETE FROM users WHERE id = $1`, owner)
	})
}
