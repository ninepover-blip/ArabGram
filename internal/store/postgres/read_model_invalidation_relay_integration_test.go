package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"telesrv/internal/store"
)

type relayPublisherCapture struct{}

func (relayPublisherCapture) PublishReadModelInvalidations(context.Context, []store.ReadModelInvalidation) error {
	return nil
}

func TestDialogReadModelRelayClaimsAndAcknowledgesExactVersionPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	ownerID := time.Now().UnixNano() & 0x3fffffffffffffff
	peerID := ownerID + 1
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM read_model_versions WHERE model='dialog_light' AND owner_user_id=$1 AND peer_type='user' AND peer_id=$2`, ownerID, peerID)
	})
	if _, err := pool.Exec(ctx, `SELECT telesrv_bump_dialog_light($1, 'user', $2)`, ownerID, peerID); err != nil {
		t.Fatalf("bump dialog light: %v", err)
	}
	relay, err := NewReadModelInvalidationRelay(pool, relayPublisherCapture{}, fmt.Sprintf("relay-test-%d", ownerID), nil)
	if err != nil {
		t.Fatalf("new relay: %v", err)
	}
	items, err := relay.claim(ctx)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	var exact *store.ReadModelInvalidation
	for i := range items {
		item := &items[i]
		if item.Key.OwnerUserID == ownerID && item.Key.PeerID == peerID {
			exact = item
			break
		}
	}
	if exact == nil || exact.Key.Model != "dialog_light" || exact.Version != 1 || exact.Hash == 0 {
		t.Fatalf("exact claim = %+v in %+v", exact, items)
	}
	if err := relay.ack(ctx, items); err != nil {
		t.Fatalf("ack: %v", err)
	}
	var version, published int64
	var publishOwner string
	var lease *time.Time
	if err := pool.QueryRow(ctx, `SELECT version,published_version,publish_owner,publish_lease_until FROM read_model_versions WHERE model='dialog_light' AND owner_user_id=$1 AND peer_type='user' AND peer_id=$2`, ownerID, peerID).Scan(&version, &published, &publishOwner, &lease); err != nil {
		t.Fatalf("load ack state: %v", err)
	}
	if published != version || publishOwner != "" || lease != nil {
		t.Fatalf("ack state version=%d published=%d owner=%q lease=%v", version, published, publishOwner, lease)
	}
}

func TestDialogReadModelRelayLeaseExpiresForAnotherInstancePostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	ownerID := (time.Now().UnixNano() & 0x3fffffffffffffff) + 10
	peerID := ownerID + 1
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM read_model_versions WHERE model='dialog_light' AND owner_user_id=$1 AND peer_type='user' AND peer_id=$2`, ownerID, peerID)
	})
	if _, err := pool.Exec(ctx, `SELECT telesrv_bump_dialog_light($1, 'user', $2)`, ownerID, peerID); err != nil {
		t.Fatalf("bump dialog light: %v", err)
	}
	first, err := NewReadModelInvalidationRelay(pool, relayPublisherCapture{}, fmt.Sprintf("relay-first-%d", ownerID), nil)
	if err != nil {
		t.Fatalf("new first relay: %v", err)
	}
	second, err := NewReadModelInvalidationRelay(pool, relayPublisherCapture{}, fmt.Sprintf("relay-second-%d", ownerID), nil)
	if err != nil {
		t.Fatalf("new second relay: %v", err)
	}
	firstItems, err := first.claim(ctx)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if !containsReadModelInvalidation(firstItems, ownerID, peerID) {
		t.Fatalf("first relay did not claim exact key: %+v", firstItems)
	}
	secondItems, err := second.claim(ctx)
	if err != nil {
		t.Fatalf("second claim during lease: %v", err)
	}
	if containsReadModelInvalidation(secondItems, ownerID, peerID) {
		t.Fatalf("second relay stole an active lease: %+v", secondItems)
	}
	if _, err := pool.Exec(ctx, `
UPDATE read_model_versions
SET publish_lease_until = clock_timestamp() - interval '1 millisecond'
WHERE model='dialog_light' AND owner_user_id=$1 AND peer_type='user' AND peer_id=$2`, ownerID, peerID); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	secondItems, err = second.claim(ctx)
	if err != nil {
		t.Fatalf("second claim after lease expiry: %v", err)
	}
	if !containsReadModelInvalidation(secondItems, ownerID, peerID) {
		t.Fatalf("second relay did not recover expired lease: %+v", secondItems)
	}
	if err := second.ack(ctx, secondItems); err != nil {
		t.Fatalf("second ack: %v", err)
	}
	if err := first.ack(ctx, firstItems); err == nil {
		t.Fatal("stale first relay unexpectedly acknowledged a lease owned by the second relay")
	}
}

func containsReadModelInvalidation(items []store.ReadModelInvalidation, ownerID, peerID int64) bool {
	for _, item := range items {
		if item.Key.Model == "dialog_light" && item.Key.OwnerUserID == ownerID &&
			item.Key.PeerType == "user" && item.Key.PeerID == peerID {
			return true
		}
	}
	return false
}
