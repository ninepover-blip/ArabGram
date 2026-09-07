package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"telesrv/internal/domain"
	storepkg "telesrv/internal/store"
)

func userProjectionFactHash(t *testing.T, model string, userID int64) int64 {
	t.Helper()
	var hash int64
	if err := testPool(t).QueryRow(context.Background(), `
SELECT hash FROM read_model_versions
WHERE model=$1 AND owner_user_id=0 AND peer_type='user' AND peer_id=$2`, model, userID).Scan(&hash); err != nil {
		t.Fatalf("read %s hash for user %d: %v", model, userID, err)
	}
	return hash
}

func TestUserProjectionFactVersionsSeedAndTrackCollectiblePhoneOwners(t *testing.T) {
	pool := testPool(t)
	seed := time.Now().UnixNano() & 0x1fffffff
	first := collectibleTestUser(t, pool, 6_300_000_000+seed, "")
	second := collectibleTestUser(t, pool, 6_600_000_000+seed, "")

	// The users INSERT trigger must seed negative facts for accounts created
	// after migration rollout, not only for rows present during backfill.
	_ = userProjectionFactHash(t, "user_visibility", first.ID)
	initialFirst := userProjectionFactHash(t, "user_collectible_phone", first.ID)
	initialSecond := userProjectionFactHash(t, "user_collectible_phone", second.ID)

	phone := fmt.Sprintf("888%09d", seed%1_000_000_000)
	store := NewCollectiblePhoneStore(pool)
	payload := []byte(fmt.Sprintf("projection-fact-%d", seed))
	asset, created, err := store.MintCollectiblePhoneWithDelivery(context.Background(), domain.MintCollectiblePhoneRequest{
		Phone:          phone,
		Tier:           domain.CollectiblePhoneTierStandard,
		OwnerUserID:    first.ID,
		Currency:       domain.CollectibleCurrencyUSD,
		Amount:         100,
		CryptoCurrency: domain.CollectibleCryptoCurrencyTON,
		CryptoAmount:   1000,
		Actor:          "test",
		CommandKey:     fmt.Sprintf("projection-fact-mint-%d", seed),
	}, pgCollectiblePhoneEffects(payload))
	if err != nil || !created {
		t.Fatalf("MintCollectiblePhone: created=%v err=%v", created, err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM collectible_phones WHERE id=$1`, asset.ID)
	})
	afterMint := userProjectionFactHash(t, "user_collectible_phone", first.ID)
	if afterMint == initialFirst {
		t.Fatalf("first owner hash did not change on mint: %d", initialFirst)
	}

	transferred, changed, err := store.TransferCollectiblePhoneWithDelivery(context.Background(), domain.TransferCollectiblePhoneRequest{
		Phone:      phone,
		ToUserID:   second.ID,
		Actor:      "test",
		CommandKey: fmt.Sprintf("projection-fact-transfer-%d", seed),
	}, pgCollectiblePhoneEffects(payload))
	if err != nil || !changed || transferred.OwnerUserID != second.ID {
		t.Fatalf("TransferCollectiblePhone: asset=%+v changed=%v err=%v", transferred, changed, err)
	}
	afterTransferFirst := userProjectionFactHash(t, "user_collectible_phone", first.ID)
	afterTransferSecond := userProjectionFactHash(t, "user_collectible_phone", second.ID)
	if afterTransferFirst == afterMint || afterTransferSecond == initialSecond {
		t.Fatalf("transfer hashes old/new = %d/%d, before=%d/%d", afterTransferFirst, afterTransferSecond, afterMint, initialSecond)
	}

	deleted, err := store.DeleteCollectiblePhoneWithDelivery(context.Background(), domain.DeleteCollectiblePhoneRequest{
		Phone:      phone,
		Actor:      "test",
		CommandKey: fmt.Sprintf("projection-fact-delete-%d", seed),
	}, func(snapshot storepkg.CollectiblePhoneDeliverySnapshot) ([]storepkg.DeliveryEffect, error) {
		return pgCollectiblePhoneEffects(payload)(snapshot)
	})
	if err != nil || !deleted {
		t.Fatalf("DeleteCollectiblePhone: deleted=%v err=%v", deleted, err)
	}
	afterDeleteSecond := userProjectionFactHash(t, "user_collectible_phone", second.ID)
	if afterDeleteSecond == afterTransferSecond {
		t.Fatalf("second owner hash did not change on delete: %d", afterTransferSecond)
	}
}
