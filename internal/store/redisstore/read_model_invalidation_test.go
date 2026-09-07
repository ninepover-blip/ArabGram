package redisstore

import (
	"context"
	"os"
	"testing"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestDecodeReadModelInvalidationsPreservesExactVersionedKeys(t *testing.T) {
	payload := `{"items":[{"model":"dialog_light","owner_user_id":42,"peer_type":"user","peer_id":84,"version":9,"hash":12345}]}`
	items, err := decodeReadModelInvalidations(payload)
	if err != nil {
		t.Fatalf("decode invalidations: %v", err)
	}
	want := store.ReadModelInvalidation{
		Key:     store.ReadModelKey{Model: "dialog_light", OwnerUserID: 42, PeerType: domain.PeerTypeUser, PeerID: 84},
		Version: 9, Hash: 12345,
	}
	if len(items) != 1 || items[0] != want {
		t.Fatalf("items = %+v, want %+v", items, want)
	}
}

func TestDecodeReadModelInvalidationsRejectsUnversionedSignal(t *testing.T) {
	if _, err := decodeReadModelInvalidations(`{"items":[{"model":"dialog_light","owner_user_id":42,"peer_type":"user","peer_id":84}]}`); err == nil {
		t.Fatal("unversioned Redis invalidation was accepted")
	}
}

func TestReadModelInvalidationBusCrossInstanceRedis(t *testing.T) {
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

	publisher := NewReadModelInvalidationBus(clientA, nil)
	subscriber := NewReadModelInvalidationBus(clientB, nil)
	want := store.ReadModelInvalidation{
		Key:     store.ReadModelKey{Model: "dialog_light", OwnerUserID: 42, PeerType: domain.PeerTypeUser, PeerID: 84},
		Version: 9, Hash: 12345,
	}
	businessOwner := int64(420042)
	businessCache := NewBusinessAutomationGateCache(clientA, time.Hour)
	if err := businessCache.PutBusinessAutomationGate(ctx, businessOwner, true); err != nil {
		t.Fatalf("seed business gate: %v", err)
	}
	subscribed := make(chan struct{}, 1)
	received := make(chan []store.ReadModelInvalidation, 1)
	done := make(chan struct{})
	subCtx, cancelSub := context.WithCancel(ctx)
	defer cancelSub()
	go func() {
		defer close(done)
		subscriber.Run(subCtx, func() {
			select {
			case subscribed <- struct{}{}:
			default:
			}
		}, func(items []store.ReadModelInvalidation) {
			select {
			case received <- items:
			default:
			}
		})
	}()
	select {
	case <-subscribed:
	case <-ctx.Done():
		t.Fatal("Redis invalidation subscriber did not become ready")
	}
	if err := publisher.PublishReadModelInvalidations(ctx, []store.ReadModelInvalidation{want}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	businessInvalidation := store.ReadModelInvalidation{
		Key:     store.ReadModelKey{Model: "business_automation", OwnerUserID: businessOwner, PeerType: domain.PeerTypeUser, PeerID: businessOwner},
		Version: 1, Hash: 67890,
	}
	if err := publisher.PublishReadModelInvalidations(ctx, []store.ReadModelInvalidation{businessInvalidation}); err != nil {
		t.Fatalf("publish business invalidation: %v", err)
	}
	if _, found, err := businessCache.GetBusinessAutomationGate(ctx, businessOwner); err != nil || found {
		t.Fatalf("business Redis gate after durable publish found=%v err=%v, want deleted", found, err)
	}
	select {
	case got := <-received:
		if len(got) != 1 || got[0] != want {
			t.Fatalf("received = %+v, want %+v", got, want)
		}
	case <-ctx.Done():
		t.Fatal("missing cross-instance Redis invalidation")
	}
	cancelSub()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Redis invalidation subscriber did not stop")
	}
}
