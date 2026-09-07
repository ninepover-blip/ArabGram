package redisstore

import (
	"context"
	"os"
	"testing"
	"time"

	"telesrv/internal/store"
)

func TestRedisAuthInvalidationBrokerCrossInstancePubSub(t *testing.T) {
	addr := os.Getenv("TELESRV_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TELESRV_TEST_REDIS_ADDR to run redis integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clientA, err := Open(ctx, addr, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer clientA.Close()
	clientB, err := Open(ctx, addr, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer clientB.Close()

	a, b := NewAuthInvalidationBroker(clientA), NewAuthInvalidationBroker(clientB)
	authKeyID := [8]byte{0xa1, 0xa2, 0xa3}
	event := store.AuthInvalidationEvent{
		SourceID:   "redis-auth-invalidation-test-a",
		AuthKeyIDs: [][8]byte{authKeyID},
		DateUnix:   time.Now().Unix(),
	}
	events := make(chan store.AuthInvalidationEvent, 1)
	subCtx, cancelSub := context.WithCancel(ctx)
	defer cancelSub()
	errCh := make(chan error, 1)
	go func() {
		errCh <- b.SubscribeAuthInvalidations(subCtx, func(_ context.Context, got store.AuthInvalidationEvent) {
			if got.SourceID != event.SourceID {
				return
			}
			select {
			case events <- got:
			default:
			}
		})
	}()

	time.Sleep(50 * time.Millisecond)
	if err := a.PublishAuthInvalidation(ctx, event); err != nil {
		t.Fatalf("publish auth invalidation: %v", err)
	}
	select {
	case got := <-events:
		if got.DateUnix != event.DateUnix || len(got.AuthKeyIDs) != 1 || got.AuthKeyIDs[0] != authKeyID {
			t.Fatalf("event = %+v, want %+v", got, event)
		}
	case <-ctx.Done():
		t.Fatal("missing cross-instance auth invalidation")
	}

	cancelSub()
	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled {
			t.Fatalf("subscriber err = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("auth invalidation subscriber did not stop")
	}
}
