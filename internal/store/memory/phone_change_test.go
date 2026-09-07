package memory

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestPhoneChangeWithDeliveryIsAtomicAndIdempotentMemory(t *testing.T) {
	ctx := context.Background()
	users := NewUserStore()
	outbox := NewDeliveryOutboxStore()
	users.AttachDeliveryOutbox(outbox)
	user, err := users.Create(ctx, domain.User{
		AccessHash: 7001,
		Phone:      "15550017001",
		FirstName:  "PhoneAtomic",
	})
	if err != nil {
		t.Fatal(err)
	}
	changes := NewPhoneChangeStore(users)
	authKeyID := [8]byte{7, 0, 0, 1}
	req := domain.PhoneChangeRequest{
		UserID: user.ID, Phone: "15550017002", Date: 1_800_000_001,
		ExcludeAuthKeyID: authKeyID, ExcludeSessionID: 71,
	}
	wantErr := errors.New("encode phone update failed")
	if _, err := changes.ChangePhoneWithDelivery(ctx, req, func(store.UserDeliverySnapshot) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("failed change error = %v, want %v", err, wantErr)
	}
	current, found, err := users.ByID(ctx, user.ID)
	if err != nil || !found || current.Phone != user.Phone {
		t.Fatalf("user after failed delivery = %+v found=%v err=%v", current, found, err)
	}
	if items := outbox.Snapshot(); len(items) != 0 {
		t.Fatalf("outbox after failed delivery = %+v, want empty", items)
	}

	buildCalls := 0
	build := func(snapshot store.UserDeliverySnapshot) ([]store.DeliveryEffect, error) {
		buildCalls++
		return []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: snapshot.User.ID, ExcludeAuthKeyID: authKeyID,
			ExcludeSessionID: 71, Payload: []byte{1},
			RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		})}, nil
	}
	result, err := changes.ChangePhoneWithDelivery(ctx, req, build)
	if err != nil || !result.Changed || result.User.Phone != req.Phone {
		t.Fatalf("successful change = %+v err=%v", result, err)
	}
	if buildCalls != 1 {
		t.Fatalf("successful delivery builder calls = %d, want 1", buildCalls)
	}
	items := outbox.Snapshot()
	if len(items) != 1 || items[0].TargetUserID != user.ID ||
		items[0].ExcludeAuthKeyID != authKeyID || items[0].ExcludeSessionID != 71 {
		t.Fatalf("successful outbox = %+v", items)
	}

	for retry := 0; retry < 2; retry++ {
		replayed, err := changes.ChangePhoneWithDelivery(ctx, req, func(store.UserDeliverySnapshot) ([]store.DeliveryEffect, error) {
			t.Fatalf("no-op replay %d invoked delivery builder", retry+1)
			return nil, nil
		})
		if err != nil || replayed.Changed || replayed.User.Phone != req.Phone {
			t.Fatalf("no-op replay %d = %+v err=%v", retry+1, replayed, err)
		}
	}
	if items := outbox.Snapshot(); len(items) != 1 {
		t.Fatalf("outbox after no-op replays = %+v, want one item", items)
	}
}
