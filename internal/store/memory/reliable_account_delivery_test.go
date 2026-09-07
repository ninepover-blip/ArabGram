package memory

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestStarsDeliveryBuilderFailureRollsBackLedgerMemory(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	stars := NewStarsStore()
	stars.AttachDeliveryOutbox(outbox)
	wantErr := errors.New("encode failed")
	if _, err := stars.CreditWithDelivery(ctx, 1001, 25, domain.StarsReasonAdjust, domain.Peer{}, 1, "admin", "", func(domain.StarsBalance) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("CreditWithDelivery error = %v", err)
	}
	if balance, err := stars.GetBalance(ctx, 1001); err != nil || balance.Balance != 0 || len(outbox.Snapshot()) != 0 {
		t.Fatalf("rolled back credit balance=%+v outbox=%+v err=%v", balance, outbox.Snapshot(), err)
	}
	if _, err := stars.DebitWithDelivery(ctx, 1001, 10, 50, domain.StarsReasonAdjust, domain.Peer{}, 2, "admin", "", func(domain.StarsBalance) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("DebitWithDelivery error = %v", err)
	}
	if balance, err := stars.GetBalance(ctx, 1001); err != nil || balance.Balance != 0 || balance.Granted || len(outbox.Snapshot()) != 0 {
		t.Fatalf("rolled back debit/grant balance=%+v outbox=%+v err=%v", balance, outbox.Snapshot(), err)
	}
}

func TestPremiumSweepAndModerationDeliveryRollbackMemory(t *testing.T) {
	ctx := context.Background()
	outbox := NewDeliveryOutboxStore()
	users := NewUserStore()
	users.AttachDeliveryOutbox(outbox)
	user, err := users.Create(ctx, domain.User{Phone: "15550009001", FirstName: "Alice", PremiumUntil: 100})
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("encode failed")
	if _, err := users.SweepExpiredPremiumWithDelivery(ctx, 100, 10, func([]domain.User) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("SweepExpiredPremiumWithDelivery error = %v", err)
	}
	current, _, _ := users.ByID(ctx, user.ID)
	if current.PremiumUntil != 100 || len(outbox.Snapshot()) != 0 {
		t.Fatalf("rolled back premium user=%+v outbox=%+v", current, outbox.Snapshot())
	}
	if _, err := users.SetVerifiedWithDelivery(ctx, user.ID, true, func(store.UserAudienceDeliverySnapshot) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("SetVerifiedWithDelivery error = %v", err)
	}
	current, _, _ = users.ByID(ctx, user.ID)
	if current.Verified || len(outbox.Snapshot()) != 0 {
		t.Fatalf("rolled back verified user=%+v outbox=%+v", current, outbox.Snapshot())
	}
}
