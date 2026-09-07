package postgres

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func pgAudienceEffects(payload []byte) store.DeliveryEffectsBuilder[store.UserAudienceDeliverySnapshot] {
	return func(snapshot store.UserAudienceDeliverySnapshot) ([]store.DeliveryEffect, error) {
		effects := make([]store.DeliveryEffect, 0, len(snapshot.Audience))
		for _, viewerID := range snapshot.Audience {
			effects = append(effects, store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
				TargetUserID: viewerID, Payload: payload, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
			}))
		}
		return effects, nil
	}
}

func TestAdminUserMutationAndAudienceDeliveryAreAtomicPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users := NewUserStore(pool)
	target := createTestUser(t, ctx, users, "+1778"+randomSuffix(t)+"01", "AdminBefore", "")
	if _, err := users.AdminUpdateProfileWithDelivery(ctx, target.ID, "MissingBuilder", "", "", nil); !errors.Is(err, store.ErrDeliveryOutboxRequired) {
		t.Fatalf("missing profile builder error = %v", err)
	}
	if _, err := users.AdminUpdateProfileWithDelivery(ctx, target.ID, "Excluded", "", "", func(snapshot store.UserAudienceDeliverySnapshot) ([]store.DeliveryEffect, error) {
		intents, err := pgAudienceEffects([]byte("excluded-admin-user"))(snapshot)
		if len(intents) > 0 {
			intents[0].ExcludeAuthKeyID = [8]byte{1}
			intents[0].ExcludeSessionID = 1
		}
		return intents, err
	}); err == nil {
		t.Fatal("excluded profile delivery unexpectedly succeeded")
	}
	if current, found, err := users.ByID(ctx, target.ID); err != nil || !found || current.FirstName != "AdminBefore" {
		t.Fatalf("fail-closed profile update leaked row: %+v found=%v err=%v", current, found, err)
	}
	wantErr := errors.New("projector failed")
	if _, err := users.AdminUpdateProfileWithDelivery(ctx, target.ID, "AdminAfter", "", "", func(store.UserAudienceDeliverySnapshot) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("failed profile update error = %v", err)
	}
	current, found, err := users.ByID(ctx, target.ID)
	if err != nil || !found || current.FirstName != "AdminBefore" {
		t.Fatalf("failed profile update leaked row: %+v found=%v err=%v", current, found, err)
	}
	payload := []byte("admin-user-" + randomSuffix(t))
	if _, err := users.AdminUpdateProfileWithDelivery(ctx, target.ID, "AdminAfter", "", "", pgAudienceEffects(payload)); err != nil {
		t.Fatal(err)
	}
	var deliveries int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM edge_delivery_outbox WHERE payload=$1`, payload).Scan(&deliveries); err != nil || deliveries != 1 {
		t.Fatalf("profile deliveries=%d err=%v", deliveries, err)
	}
	builds := 0
	if _, err := users.AdminUpdateProfileWithDelivery(ctx, target.ID, "AdminAfter", "", "", func(snapshot store.UserAudienceDeliverySnapshot) ([]store.DeliveryEffect, error) {
		builds++
		return pgAudienceEffects(payload)(snapshot)
	}); err != nil {
		t.Fatal(err)
	}
	if builds != 0 {
		t.Fatalf("no-op rebuilt delivery %d times", builds)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM edge_delivery_outbox WHERE payload=$1`, payload).Scan(&deliveries); err != nil || deliveries != 1 {
		t.Fatalf("no-op deliveries=%d err=%v", deliveries, err)
	}
}

func pgBotLifecycleEffects(payload []byte) store.DeliveryEffectsBuilder[store.BotLifecycleDeliverySnapshot] {
	return func(snapshot store.BotLifecycleDeliverySnapshot) ([]store.DeliveryEffect, error) {
		return []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: snapshot.OwnerUserID, Payload: payload, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		})}, nil
	}
}

func TestBotLifecycleAndOwnerDeliveryAreAtomicPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	users := NewUserStore(pool)
	owner := createTestUser(t, ctx, users, "+1779"+suffix+"01", "BotOwner", "")
	bots := NewBotStore(pool)
	wantErr := errors.New("projector failed")
	failedUsername := "admin_fail_" + suffix
	if _, _, err := bots.CreateBotAccountWithDelivery(ctx,
		domain.User{AccessHash: 1, FirstName: "Failed", Username: failedUsername, Bot: true, BotInfoVersion: 1},
		domain.BotProfile{OwnerUserID: owner.ID, TokenSecret: "failed-secret"},
		func(store.BotLifecycleDeliverySnapshot) ([]store.DeliveryEffect, error) { return nil, wantErr },
	); !errors.Is(err, wantErr) {
		t.Fatalf("failed create error = %v", err)
	}
	if _, found, err := users.ByUsername(ctx, failedUsername); err != nil || found {
		t.Fatalf("failed create leaked user: found=%v err=%v", found, err)
	}
	payload := []byte("admin-bot-" + suffix)
	bot, _, err := bots.CreateBotAccountWithDelivery(ctx,
		domain.User{AccessHash: 2, FirstName: "Created", Username: "admin_ok_" + suffix, Bot: true, BotInfoVersion: 1},
		domain.BotProfile{OwnerUserID: owner.ID, TokenSecret: "secret"}, pgBotLifecycleEffects(payload))
	if err != nil {
		t.Fatal(err)
	}
	var deliveries int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM edge_delivery_outbox WHERE payload=$1`, payload).Scan(&deliveries); err != nil || deliveries != 1 {
		t.Fatalf("create deliveries=%d err=%v", deliveries, err)
	}
	if _, err := bots.DeleteBotAccountWithDelivery(ctx, bot.ID, func(store.BotLifecycleDeliverySnapshot) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("failed delete error = %v", err)
	}
	if current, found, err := users.ByID(ctx, bot.ID); err != nil || !found || current.Deleted {
		t.Fatalf("failed delete leaked tombstone: %+v found=%v err=%v", current, found, err)
	}
	deleted, err := bots.DeleteBotAccountWithDelivery(ctx, bot.ID, pgBotLifecycleEffects(payload))
	if err != nil || !deleted.Deleted {
		t.Fatalf("delete=%+v err=%v", deleted, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM edge_delivery_outbox WHERE payload=$1`, payload).Scan(&deliveries); err != nil || deliveries != 2 {
		t.Fatalf("lifecycle deliveries=%d err=%v", deliveries, err)
	}
}

func pgCollectiblePhoneEffects(payload []byte) store.DeliveryEffectsBuilder[store.CollectiblePhoneDeliverySnapshot] {
	return func(snapshot store.CollectiblePhoneDeliverySnapshot) ([]store.DeliveryEffect, error) {
		var effects []store.DeliveryEffect
		for _, user := range snapshot.Users {
			for _, viewerID := range user.Audience {
				effects = append(effects, store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
					TargetUserID: viewerID, Payload: payload, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
				}))
			}
		}
		return effects, nil
	}
}

func TestCollectiblePhoneOwnerTransitionAndDeliveryAreAtomicPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	users := NewUserStore(pool)
	from := createTestUser(t, ctx, users, "+1780"+suffix+"01", "PhoneFrom", "")
	to := createTestUser(t, ctx, users, "+1780"+suffix+"02", "PhoneTo", "")
	phones := NewCollectiblePhoneStore(pool)
	numericSuffix, err := strconv.ParseUint(suffix, 16, 32)
	if err != nil {
		t.Fatalf("parse random phone suffix: %v", err)
	}
	phone := fmt.Sprintf("888%010d", numericSuffix)
	request := domain.MintCollectiblePhoneRequest{
		Phone: phone, Tier: domain.CollectiblePhoneTierStandard, OwnerUserID: from.ID,
		PurchaseDate: time.Now().UTC(), Currency: domain.CollectibleCurrencyUSD, Amount: 100,
		CryptoCurrency: domain.CollectibleCryptoCurrencyTON, CryptoAmount: 1000,
		Actor: "test", Reason: "atomic", CommandKey: "mint-phone-" + suffix,
	}
	if _, _, err := phones.MintCollectiblePhoneWithDelivery(ctx, request, nil); !errors.Is(err, store.ErrDeliveryOutboxRequired) {
		t.Fatalf("missing mint builder error = %v", err)
	}
	if _, _, err := phones.MintCollectiblePhoneWithDelivery(ctx, request, func(snapshot store.CollectiblePhoneDeliverySnapshot) ([]store.DeliveryEffect, error) {
		intents, err := pgCollectiblePhoneEffects([]byte("excluded-admin-phone"))(snapshot)
		if len(intents) > 0 {
			intents[0].ExcludeAuthKeyID = [8]byte{1}
			intents[0].ExcludeSessionID = 1
		}
		return intents, err
	}); err == nil {
		t.Fatal("excluded mint delivery unexpectedly succeeded")
	}
	if _, err := phones.CollectiblePhone(ctx, phone); !errors.Is(err, domain.ErrCollectiblePhoneNotFound) {
		t.Fatalf("fail-closed mint leaked asset: %v", err)
	}
	wantErr := errors.New("projector failed")
	if _, _, err := phones.MintCollectiblePhoneWithDelivery(ctx, request, func(store.CollectiblePhoneDeliverySnapshot) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("failed mint phone=%q owner=%d request=%+v error=%v", phone, from.ID, request, err)
	}
	if _, err := phones.CollectiblePhone(ctx, phone); !errors.Is(err, domain.ErrCollectiblePhoneNotFound) {
		t.Fatalf("failed mint leaked asset: %v", err)
	}
	payload := []byte("admin-phone-" + suffix)
	asset, created, err := phones.MintCollectiblePhoneWithDelivery(ctx, request, pgCollectiblePhoneEffects(payload))
	if err != nil || !created || asset.OwnerUserID != from.ID {
		t.Fatalf("mint=%+v created=%v err=%v", asset, created, err)
	}
	transfer := domain.TransferCollectiblePhoneRequest{
		Phone: phone, ToUserID: to.ID, Actor: "test", Reason: "atomic", CommandKey: "transfer-phone-" + suffix,
	}
	if _, _, err := phones.TransferCollectiblePhoneWithDelivery(ctx, transfer, func(store.CollectiblePhoneDeliverySnapshot) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("failed transfer error = %v", err)
	}
	current, err := phones.CollectiblePhone(ctx, phone)
	if err != nil || current.OwnerUserID != from.ID {
		t.Fatalf("failed transfer leaked owner: %+v err=%v", current, err)
	}
	transferred, changed, err := phones.TransferCollectiblePhoneWithDelivery(ctx, transfer, pgCollectiblePhoneEffects(payload))
	if err != nil || !changed || transferred.OwnerUserID != to.ID {
		t.Fatalf("transfer=%+v changed=%v err=%v", transferred, changed, err)
	}
	builds := 0
	if _, changed, err := phones.TransferCollectiblePhoneWithDelivery(ctx, transfer, func(snapshot store.CollectiblePhoneDeliverySnapshot) ([]store.DeliveryEffect, error) {
		builds++
		return pgCollectiblePhoneEffects(payload)(snapshot)
	}); err != nil || changed || builds != 0 {
		t.Fatalf("replay changed=%v builds=%d err=%v", changed, builds, err)
	}
}
