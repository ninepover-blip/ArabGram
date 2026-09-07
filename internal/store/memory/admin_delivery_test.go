package memory

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func absoluteUserAudienceEffects(snapshot store.UserAudienceDeliverySnapshot) ([]store.DeliveryEffect, error) {
	effects := make([]store.DeliveryEffect, 0, len(snapshot.Audience))
	for _, viewerID := range snapshot.Audience {
		effects = append(effects, store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: viewerID, Payload: []byte{1}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		}))
	}
	return effects, nil
}

func TestAdminUserDeliveryIsAtomicAndNoopDoesNotDuplicateMemory(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	users := NewUserStore()
	users.AttachDeliveryOutbox(outbox)
	user, err := users.Create(ctx, domain.User{Phone: "+15550001001", FirstName: "Before"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := users.AdminUpdateProfileWithDelivery(ctx, user.ID, "MissingBuilder", "", "", nil); !errors.Is(err, store.ErrDeliveryOutboxRequired) {
		t.Fatalf("missing builder error = %v", err)
	}
	if current, _, err := users.ByID(ctx, user.ID); err != nil || current.FirstName != "Before" || len(outbox.items) != 0 {
		t.Fatalf("missing builder leaked state: user=%+v outbox=%d err=%v", current, len(outbox.items), err)
	}
	if _, err := users.AdminUpdateProfileWithDelivery(ctx, user.ID, "Excluded", "", "", func(snapshot store.UserAudienceDeliverySnapshot) ([]store.DeliveryEffect, error) {
		intents, err := absoluteUserAudienceEffects(snapshot)
		if len(intents) > 0 {
			intents[0].ExcludeAuthKeyID = [8]byte{1}
			intents[0].ExcludeSessionID = 1
		}
		return intents, err
	}); err == nil {
		t.Fatal("excluded admin delivery unexpectedly succeeded")
	}
	if current, _, err := users.ByID(ctx, user.ID); err != nil || current.FirstName != "Before" || len(outbox.items) != 0 {
		t.Fatalf("excluded delivery leaked state: user=%+v outbox=%d err=%v", current, len(outbox.items), err)
	}
	wantErr := errors.New("projector failed")
	if _, err := users.AdminUpdateProfileWithDelivery(ctx, user.ID, "After", "", "", func(store.UserAudienceDeliverySnapshot) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("failed mutation error = %v", err)
	}
	unchanged, _, err := users.ByID(ctx, user.ID)
	if err != nil || unchanged.FirstName != "Before" || len(outbox.items) != 0 {
		t.Fatalf("failed mutation leaked state: user=%+v outbox=%d err=%v", unchanged, len(outbox.items), err)
	}
	updated, err := users.AdminUpdateProfileWithDelivery(ctx, user.ID, "After", "", "", absoluteUserAudienceEffects)
	if err != nil || updated.FirstName != "After" || len(outbox.items) != 1 {
		t.Fatalf("committed mutation = %+v outbox=%d err=%v", updated, len(outbox.items), err)
	}
	builds := 0
	if _, err := users.AdminUpdateProfileWithDelivery(ctx, user.ID, "After", "", "", func(snapshot store.UserAudienceDeliverySnapshot) ([]store.DeliveryEffect, error) {
		builds++
		return absoluteUserAudienceEffects(snapshot)
	}); err != nil {
		t.Fatal(err)
	}
	if builds != 0 || len(outbox.items) != 1 {
		t.Fatalf("no-op built or duplicated delivery: builds=%d outbox=%d", builds, len(outbox.items))
	}
}

func TestEveryAdminUserMutationRollsBackAndNoopSkipsDeliveryMemory(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	users := NewUserStore()
	users.AttachDeliveryOutbox(outbox)
	user, err := users.Create(ctx, domain.User{Phone: "+15550001003", FirstName: "Before"})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(store.DeliveryEffectsBuilder[store.UserAudienceDeliverySnapshot]) (domain.User, error)
	}{
		{name: "profile", mutate: func(builder store.DeliveryEffectsBuilder[store.UserAudienceDeliverySnapshot]) (domain.User, error) {
			return users.AdminUpdateProfileWithDelivery(ctx, user.ID, "Profile", "Changed", "About", builder)
		}},
		{name: "username", mutate: func(builder store.DeliveryEffectsBuilder[store.UserAudienceDeliverySnapshot]) (domain.User, error) {
			return users.AdminUpdateUsernameWithDelivery(ctx, user.ID, "delivery_user", builder)
		}},
		{name: "phone", mutate: func(builder store.DeliveryEffectsBuilder[store.UserAudienceDeliverySnapshot]) (domain.User, error) {
			return users.AdminUpdatePhoneWithDelivery(ctx, user.ID, "+15550001004", builder)
		}},
		{name: "color", mutate: func(builder store.DeliveryEffectsBuilder[store.UserAudienceDeliverySnapshot]) (domain.User, error) {
			return users.AdminUpdateColorWithDelivery(ctx, user.ID, false, domain.PeerColor{HasColor: true, Color: 2}, builder)
		}},
		{name: "emoji-status", mutate: func(builder store.DeliveryEffectsBuilder[store.UserAudienceDeliverySnapshot]) (domain.User, error) {
			return users.AdminUpdateEmojiStatusWithDelivery(ctx, user.ID, domain.UserEmojiStatus{DocumentID: 42}, builder)
		}},
	}
	wantErr := errors.New("projector failed")
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before, found, err := users.ByID(ctx, user.ID)
			if err != nil || !found {
				t.Fatalf("load before mutation: found=%v err=%v", found, err)
			}
			beforeOutbox := len(outbox.items)
			if _, err := test.mutate(func(store.UserAudienceDeliverySnapshot) ([]store.DeliveryEffect, error) {
				return nil, wantErr
			}); !errors.Is(err, wantErr) {
				t.Fatalf("builder failure error = %v", err)
			}
			afterFailure, found, err := users.ByID(ctx, user.ID)
			if err != nil || !found || !reflect.DeepEqual(afterFailure, before) || len(outbox.items) != beforeOutbox {
				t.Fatalf("builder failure leaked state: before=%+v after=%+v outbox=%d/%d found=%v err=%v", before, afterFailure, beforeOutbox, len(outbox.items), found, err)
			}
			if _, err := test.mutate(absoluteUserAudienceEffects); err != nil {
				t.Fatalf("commit mutation: %v", err)
			}
			if len(outbox.items) != beforeOutbox+1 {
				t.Fatalf("committed outbox=%d, want %d", len(outbox.items), beforeOutbox+1)
			}
			builds := 0
			if _, err := test.mutate(func(snapshot store.UserAudienceDeliverySnapshot) ([]store.DeliveryEffect, error) {
				builds++
				return absoluteUserAudienceEffects(snapshot)
			}); err != nil || builds != 0 || len(outbox.items) != beforeOutbox+1 {
				t.Fatalf("no-op err=%v builds=%d outbox=%d", err, builds, len(outbox.items))
			}
			if _, err := test.mutate(nil); !errors.Is(err, store.ErrDeliveryOutboxRequired) {
				t.Fatalf("no-op missing builder error = %v", err)
			}
		})
	}
}

func botLifecycleEffects(snapshot store.BotLifecycleDeliverySnapshot) ([]store.DeliveryEffect, error) {
	return []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
		TargetUserID: snapshot.OwnerUserID, Payload: []byte{2}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
	})}, nil
}

func TestBotLifecycleDeliveryRollsBackAndTargetsOwnerMemory(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	users := NewUserStore()
	owner, err := users.Create(ctx, domain.User{Phone: "+15550001002", FirstName: "Owner"})
	if err != nil {
		t.Fatal(err)
	}
	bots := NewBotStore(users)
	bots.AttachDeliveryDependencies(nil, outbox)
	if _, _, err := bots.CreateBotAccountWithDelivery(ctx,
		domain.User{FirstName: "Missing", Username: "missing_builder_bot", Bot: true, BotInfoVersion: 1},
		domain.BotProfile{OwnerUserID: owner.ID, TokenSecret: "secret"}, nil,
	); !errors.Is(err, store.ErrDeliveryOutboxRequired) {
		t.Fatalf("missing create builder error = %v", err)
	}
	if count, err := bots.CountBotsByOwner(ctx, owner.ID); err != nil || count != 0 || len(outbox.items) != 0 {
		t.Fatalf("missing create builder leaked state: count=%d outbox=%d err=%v", count, len(outbox.items), err)
	}
	wantErr := errors.New("projector failed")
	if _, _, err := bots.CreateBotAccountWithDelivery(ctx,
		domain.User{FirstName: "Bot", Username: "atomic_bot", Bot: true, BotInfoVersion: 1},
		domain.BotProfile{OwnerUserID: owner.ID, TokenSecret: "secret"},
		func(store.BotLifecycleDeliverySnapshot) ([]store.DeliveryEffect, error) { return nil, wantErr },
	); !errors.Is(err, wantErr) {
		t.Fatalf("failed create error = %v", err)
	}
	if count, err := bots.CountBotsByOwner(ctx, owner.ID); err != nil || count != 0 || len(outbox.items) != 0 {
		t.Fatalf("failed create leaked state: count=%d outbox=%d err=%v", count, len(outbox.items), err)
	}
	bot, _, err := bots.CreateBotAccountWithDelivery(ctx,
		domain.User{FirstName: "Bot", Username: "atomic_bot", Bot: true, BotInfoVersion: 1},
		domain.BotProfile{OwnerUserID: owner.ID, TokenSecret: "secret"}, botLifecycleEffects)
	if err != nil || bot.ID <= 0 || len(outbox.items) != 1 {
		t.Fatalf("create = %+v outbox=%d err=%v", bot, len(outbox.items), err)
	}
	if _, err := bots.DeleteBotAccountWithDelivery(ctx, bot.ID, func(store.BotLifecycleDeliverySnapshot) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("failed delete error = %v", err)
	}
	if current, found, _ := users.ByID(ctx, bot.ID); !found || current.Deleted || len(outbox.items) != 1 {
		t.Fatalf("failed delete leaked state: user=%+v found=%v outbox=%d", current, found, len(outbox.items))
	}
	deleted, err := bots.DeleteBotAccountWithDelivery(ctx, bot.ID, botLifecycleEffects)
	if err != nil || !deleted.Deleted || len(outbox.items) != 2 {
		t.Fatalf("delete = %+v outbox=%d err=%v", deleted, len(outbox.items), err)
	}
}
