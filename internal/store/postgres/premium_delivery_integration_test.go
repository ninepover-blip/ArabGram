package postgres

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestPremiumSweepDeliveryFailureRollsBackUserPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users := NewUserStore(pool)
	user := createTestUser(t, ctx, users, "+1783"+randomSuffix(t)+"71", "PremiumRollback", "")
	user, err := users.SetPremiumUntil(ctx, user.ID, 100)
	if err != nil {
		t.Fatal(err)
	}
	wantErr := errors.New("encode failed")
	if _, err := users.SweepExpiredPremiumWithDelivery(ctx, 100, 10, func([]domain.User) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("SweepExpiredPremiumWithDelivery error = %v", err)
	}
	current, found, err := users.ByID(ctx, user.ID)
	if err != nil || !found || current.PremiumUntil != 100 {
		t.Fatalf("user after rollback = %+v found=%v err=%v", current, found, err)
	}
	var deliveries int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id=$1", user.ID).Scan(&deliveries); err != nil || deliveries != 0 {
		t.Fatalf("deliveries after rollback = %d err=%v", deliveries, err)
	}
}
