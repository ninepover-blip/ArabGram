package rpc

import (
	"context"
	"testing"

	"github.com/iamxvbaba/td/clock"
	"github.com/iamxvbaba/td/tg"
	"go.uber.org/zap/zaptest"

	appusers "telesrv/internal/app/users"
	"telesrv/internal/domain"
	"telesrv/internal/store/memory"
)

func TestAccountUpdateBirthdayPersistsFullUserAndQueuesRefresh(t *testing.T) {
	ctx := context.Background()
	userStore := memory.NewUserStore()
	delivery := attachDeliveryOutbox(userStore)
	owner, err := userStore.Create(ctx, domain.User{AccessHash: 11, Phone: "15550003301", FirstName: "Owner"})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	router := New(Config{}, Deps{
		Users:          appusers.NewService(userStore),
		DeliveryOutbox: delivery,
	}, zaptest.NewLogger(t), clock.System)

	birthday := tg.Birthday{Day: 14, Month: 2}
	birthday.SetYear(1990)
	req := &tg.AccountUpdateBirthdayRequest{}
	req.SetBirthday(birthday)
	authKeyID := [8]byte{4, 2, 4, 2}
	callCtx := WithRawAuthKeyID(WithSessionID(WithUserID(ctx, owner.ID), 4242), authKeyID)
	ok, err := router.onAccountUpdateBirthday(callCtx, req)
	if err != nil || !ok {
		t.Fatalf("update birthday = ok %v err %v, want true/nil", ok, err)
	}

	saved, found, err := userStore.ByID(ctx, owner.ID)
	if err != nil || !found {
		t.Fatalf("load saved user found=%v err=%v", found, err)
	}
	if saved.Birthday != (domain.Birthday{Day: 14, Month: 2, Year: 1990}) {
		t.Fatalf("saved birthday = %+v, want 14/2/1990", saved.Birthday)
	}

	full, err := router.onUsersGetFullUser(WithUserID(ctx, owner.ID), &tg.InputUserSelf{})
	if err != nil {
		t.Fatalf("get full user: %v", err)
	}
	gotBirthday, ok := full.FullUser.GetBirthday()
	if !ok {
		t.Fatal("full user missing birthday")
	}
	gotYear, gotYearOK := gotBirthday.GetYear()
	if gotBirthday.Day != 14 || gotBirthday.Month != 2 || !gotYearOK || gotYear != 1990 {
		t.Fatalf("full user birthday = %+v yearOK=%v, want 14/2/1990", gotBirthday, gotYearOK)
	}

	items := delivery.Snapshot()
	if len(items) != 1 {
		t.Fatalf("delivery rows = %d, want 1", len(items))
	}
	if items[0].TargetUserID != owner.ID || items[0].ExcludeSessionID != 4242 || items[0].ExcludeAuthKeyID != authKeyID {
		t.Fatalf("delivery target/exclude = user %d session %d auth %v, want user %d session 4242 auth %v",
			items[0].TargetUserID, items[0].ExcludeSessionID, items[0].ExcludeAuthKeyID, owner.ID, authKeyID)
	}
	updates := lastQueuedDeliveryUpdates(t, delivery)
	hasUserUpdate := false
	for _, update := range updates.Updates {
		if u, ok := update.(*tg.UpdateUser); ok && u.UserID == owner.ID {
			hasUserUpdate = true
		}
	}
	if !hasUserUpdate {
		t.Fatalf("updates = %+v, want UpdateUser for self", updates.Updates)
	}
	if len(updates.Users) != 1 {
		t.Fatalf("pushed users = %d, want 1 self user", len(updates.Users))
	}
	pushedUser, ok := updates.Users[0].(*tg.User)
	if !ok || pushedUser.ID != owner.ID {
		t.Fatalf("pushed user = %T %+v, want self user", updates.Users[0], updates.Users[0])
	}
}
