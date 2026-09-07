package postgres

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestAuthorizationBindAndDeliveryAreAtomicAndIdempotentPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	userID := createRevokeTestUser(t, ctx, pool, "delivery-bind")
	key := saveTempIdentityTestAuthKey(t, ctx, pool, NewAuthKeyStore(pool), 0)
	user, found, err := NewUserStore(pool).ByID(ctx, userID)
	if err != nil || !found {
		t.Fatalf("load authorization user found=%v err=%v", found, err)
	}
	auths := NewAuthorizationStore(pool)
	a := domain.Authorization{AuthKeyID: key, UserID: userID}
	wantErr := errors.New("projection failed")
	if err := auths.BindWithDelivery(ctx, a, user, func(store.AuthorizationDeliverySnapshot) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("BindWithDelivery projection error = %v, want %v", err, wantErr)
	}
	assertRevokeTestNoAuthorization(t, ctx, auths, key)
	assertAuthorizationDeliveryCount(t, ctx, pool, userID, 0)

	const goroutines = 12
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- auths.BindWithDelivery(ctx, a, user, postgresAuthorizationDelivery)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent BindWithDelivery: %v", err)
		}
	}
	assertRevokeTestPresentAuthorization(t, ctx, auths, key)
	assertAuthorizationDeliveryCount(t, ctx, pool, userID, 1)
}

func TestPasswordPromotionAndDeliveryAreAtomicAndIdempotentPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	userID := createRevokeTestUser(t, ctx, pool, "delivery-password")
	key := saveTempIdentityTestAuthKey(t, ctx, pool, NewAuthKeyStore(pool), 0)
	user, found, err := NewUserStore(pool).ByID(ctx, userID)
	if err != nil || !found {
		t.Fatalf("load authorization user found=%v err=%v", found, err)
	}
	auths := NewAuthorizationStore(pool)
	if err := auths.Bind(ctx, domain.Authorization{AuthKeyID: key, UserID: userID, PasswordPending: true}); err != nil {
		t.Fatalf("bind pending authorization: %v", err)
	}
	wantErr := errors.New("encode failed")
	if err := auths.MarkPasswordPassedWithDelivery(ctx, key, user, func(store.AuthorizationDeliverySnapshot) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("MarkPasswordPassedWithDelivery projection error = %v, want %v", err, wantErr)
	}
	stored, found, err := auths.ByAuthKey(ctx, key)
	if err != nil || !found || !stored.PasswordPending {
		t.Fatalf("pending authorization after failed builder=%+v found=%v err=%v", stored, found, err)
	}
	assertAuthorizationDeliveryCount(t, ctx, pool, userID, 0)
	if err := auths.MarkPasswordPassedWithDelivery(ctx, key, user, postgresAuthorizationDelivery); err != nil {
		t.Fatalf("promote password authorization: %v", err)
	}
	if err := auths.MarkPasswordPassedWithDelivery(ctx, key, user, postgresAuthorizationDelivery); err != nil {
		t.Fatalf("replay password promotion: %v", err)
	}
	stored, found, err = auths.ByAuthKey(ctx, key)
	if err != nil || !found || stored.PasswordPending {
		t.Fatalf("promoted authorization=%+v found=%v err=%v", stored, found, err)
	}
	assertAuthorizationDeliveryCount(t, ctx, pool, userID, 1)
}

func assertAuthorizationDeliveryCount(t *testing.T, ctx context.Context, db *pgxpool.Pool, userID int64, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = $1`, userID).Scan(&got); err != nil {
		t.Fatalf("count authorization delivery rows: %v", err)
	}
	if got != want {
		t.Fatalf("authorization delivery rows = %d, want %d", got, want)
	}
}
