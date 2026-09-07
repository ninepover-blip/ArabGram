package postgres

import (
	"context"
	"errors"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestPrivateHistoryTTLAndAbsoluteDeliveryCommitAtomicallyPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users := NewUserStore(pool)
	suffix := randomSuffix(t)
	alice, err := users.Create(ctx, domain.User{AccessHash: 91, Phone: "+1895" + suffix + "01", FirstName: "TTL Alice"})
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, err := users.Create(ctx, domain.User{AccessHash: 92, Phone: "+1895" + suffix + "02", FirstName: "TTL Bob"})
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}
	userIDs := []int64{alice.ID, bob.ID}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM edge_delivery_outbox WHERE target_user_id = ANY($1::bigint[])", userIDs)
		_, _ = pool.Exec(ctx, "DELETE FROM dialogs WHERE user_id = ANY($1::bigint[])", userIDs)
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = ANY($1::bigint[])", userIDs)
	})

	messages := newTestMessageStore(pool)
	peer := domain.Peer{Type: domain.PeerTypeUser, ID: bob.ID}
	var beforePTS [2]int
	if err := pool.QueryRow(ctx, `SELECT
  COALESCE((SELECT contiguous_pts FROM user_update_watermarks WHERE user_id = $1), 0),
  COALESCE((SELECT contiguous_pts FROM user_update_watermarks WHERE user_id = $2), 0)`, alice.ID, bob.ID).Scan(&beforePTS[0], &beforePTS[1]); err != nil {
		t.Fatalf("read pts before ttl: %v", err)
	}

	projectionErr := errors.New("ttl projection failed")
	if err := messages.SetPrivateHistoryTTL(ctx, alice.ID, peer, 3600, func(domain.PrivateHistoryTTLResult) ([]store.DeliveryEffect, error) {
		return nil, projectionErr
	}); !errors.Is(err, projectionErr) {
		t.Fatalf("failed ttl mutation err = %v, want projection error", err)
	}
	for _, pair := range [][2]int64{{alice.ID, bob.ID}, {bob.ID, alice.ID}} {
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM dialogs WHERE user_id = $1 AND peer_type = 'user' AND peer_id = $2`, pair[0], pair[1]).Scan(&count); err != nil {
			t.Fatalf("count rolled back ttl dialog %v: %v", pair, err)
		}
		if count != 0 {
			t.Fatalf("ttl dialog %v survived failed delivery builder", pair)
		}
	}

	effects := func(result domain.PrivateHistoryTTLResult) ([]store.DeliveryEffect, error) {
		return []store.DeliveryEffect{
			store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{TargetUserID: result.OwnerUserID, Payload: []byte{1}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload}),
			store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{TargetUserID: result.Peer.ID, Payload: []byte{2}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload}),
		}, nil
	}
	if err := messages.SetPrivateHistoryTTL(ctx, alice.ID, peer, 7200, effects); err != nil {
		t.Fatalf("set private history ttl: %v", err)
	}
	for _, ownerPeer := range []struct {
		owner int64
		peer  domain.Peer
	}{{alice.ID, peer}, {bob.ID, domain.Peer{Type: domain.PeerTypeUser, ID: alice.ID}}} {
		period, err := messages.GetPrivateHistoryTTL(ctx, ownerPeer.owner, ownerPeer.peer)
		if err != nil || period != 7200 {
			t.Fatalf("ttl %d -> %d = %d err %v, want 7200", ownerPeer.owner, ownerPeer.peer.ID, period, err)
		}
	}
	var deliveryRows int
	var afterPTS [2]int
	if err := pool.QueryRow(ctx, `SELECT
  (SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = ANY($1::bigint[])),
  COALESCE((SELECT contiguous_pts FROM user_update_watermarks WHERE user_id = $2), 0),
  COALESCE((SELECT contiguous_pts FROM user_update_watermarks WHERE user_id = $3), 0)`, userIDs, alice.ID, bob.ID).Scan(&deliveryRows, &afterPTS[0], &afterPTS[1]); err != nil {
		t.Fatalf("read ttl delivery rows: %v", err)
	}
	if deliveryRows != 2 {
		t.Fatalf("ttl delivery rows = %d, want 2", deliveryRows)
	}
	if afterPTS != beforePTS {
		t.Fatalf("ttl changed account pts %v -> %v", beforePTS, afterPTS)
	}
}
