package memory

import (
	"context"
	"errors"
	"sync"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func memoryAuthorizationDelivery(snapshot store.AuthorizationDeliverySnapshot) ([]store.DeliveryEffect, error) {
	return []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
		TargetUserID:     snapshot.User.ID,
		ExcludeAuthKeyID: [8]byte{0x91},
		ExcludeSessionID: 91,
		Payload:          []byte{1, 2, 3},
		RecoveryPolicy:   store.OutboxRecoveryAbsoluteReload,
	})}, nil
}

func TestAuthorizationDeliveryRequiresExplicitOutboxMemory(t *testing.T) {
	auths := NewAuthorizationStore()
	user := domain.User{ID: 9001, FirstName: "NoFallback"}
	authorization := domain.Authorization{AuthKeyID: [8]byte{9}, UserID: user.ID}
	if err := auths.BindWithDelivery(context.Background(), authorization, user, memoryAuthorizationDelivery); !errors.Is(err, store.ErrDeliveryOutboxRequired) {
		t.Fatalf("BindWithDelivery without outbox = %v, want %v", err, store.ErrDeliveryOutboxRequired)
	}
	if _, found, err := auths.ByAuthKey(context.Background(), authorization.AuthKeyID); err != nil || found {
		t.Fatalf("authorization after missing outbox found=%v err=%v, want absent", found, err)
	}
}

func TestAuthorizationBindAndDeliveryAreAtomicAndIdempotentMemory(t *testing.T) {
	ctx := context.Background()
	auths := NewAuthorizationStore()
	auths.AttachDeliveryOutbox(NewDeliveryOutboxStore())
	user := domain.User{ID: 1001, FirstName: "Alice"}
	a := domain.Authorization{AuthKeyID: [8]byte{1}, UserID: user.ID}
	wantErr := errors.New("projection failed")
	if err := auths.BindWithDelivery(ctx, a, user, func(store.AuthorizationDeliverySnapshot) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("BindWithDelivery projection error = %v, want %v", err, wantErr)
	}
	if _, found, err := auths.ByAuthKey(ctx, a.AuthKeyID); err != nil || found {
		t.Fatalf("authorization after failed builder found=%v err=%v, want absent", found, err)
	}
	if got := len(auths.DeliveryOutbox().items); got != 0 {
		t.Fatalf("outbox after failed builder = %d, want 0", got)
	}

	const goroutines = 16
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- auths.BindWithDelivery(ctx, a, user, memoryAuthorizationDelivery)
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent BindWithDelivery: %v", err)
		}
	}
	if got := len(auths.DeliveryOutbox().items); got != 1 {
		t.Fatalf("outbox after concurrent/replayed bind = %d, want 1", got)
	}
	if stored, found, err := auths.ByAuthKey(ctx, a.AuthKeyID); err != nil || !found || stored.UserID != user.ID || stored.PasswordPending {
		t.Fatalf("stored authorization=%+v found=%v err=%v", stored, found, err)
	}
}

func TestPasswordPromotionAndDeliveryAreAtomicAndIdempotentMemory(t *testing.T) {
	ctx := context.Background()
	auths := NewAuthorizationStore()
	auths.AttachDeliveryOutbox(NewDeliveryOutboxStore())
	user := domain.User{ID: 2001, FirstName: "Bob"}
	a := domain.Authorization{AuthKeyID: [8]byte{2}, UserID: user.ID, PasswordPending: true}
	if err := auths.Bind(ctx, a); err != nil {
		t.Fatalf("bind pending authorization: %v", err)
	}
	wantErr := errors.New("encode failed")
	if err := auths.MarkPasswordPassedWithDelivery(ctx, a.AuthKeyID, user, func(store.AuthorizationDeliverySnapshot) ([]store.DeliveryEffect, error) {
		return nil, wantErr
	}); !errors.Is(err, wantErr) {
		t.Fatalf("MarkPasswordPassedWithDelivery projection error = %v, want %v", err, wantErr)
	}
	if stored, _, _ := auths.ByAuthKey(ctx, a.AuthKeyID); !stored.PasswordPending {
		t.Fatal("password promotion survived failed builder")
	}
	if err := auths.MarkPasswordPassedWithDelivery(ctx, a.AuthKeyID, user, memoryAuthorizationDelivery); err != nil {
		t.Fatalf("promote password authorization: %v", err)
	}
	if err := auths.MarkPasswordPassedWithDelivery(ctx, a.AuthKeyID, user, memoryAuthorizationDelivery); err != nil {
		t.Fatalf("replay password promotion: %v", err)
	}
	if stored, _, _ := auths.ByAuthKey(ctx, a.AuthKeyID); stored.PasswordPending {
		t.Fatal("password authorization remained pending")
	}
	if got := len(auths.DeliveryOutbox().items); got != 1 {
		t.Fatalf("password promotion outbox = %d, want 1", got)
	}
}
