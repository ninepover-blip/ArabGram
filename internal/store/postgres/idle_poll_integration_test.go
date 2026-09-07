package postgres

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestIdleRelayAcknowledgesOnlyClaimedVersionPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	owner := time.Now().UnixNano() & 0x3fffffffffffffff
	peer := owner + 1
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM read_model_versions WHERE model='dialog_light' AND owner_user_id=$1 AND peer_type='user' AND peer_id=$2`, owner, peer)
	})
	bump := func() {
		t.Helper()
		if _, err := pool.Exec(ctx, `SELECT telesrv_bump_dialog_light($1, 'user', $2)`, owner, peer); err != nil {
			t.Fatal(err)
		}
	}
	bump()
	relay, err := NewReadModelInvalidationRelay(pool, relayPublisherCapture{}, "allocation-version-test", nil)
	if err != nil {
		t.Fatal(err)
	}
	claim := func(version int64) []store.ReadModelInvalidation {
		t.Helper()
		items, err := relay.claim(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, item := range items {
			if item.Key.OwnerUserID == owner && item.Key.PeerID == peer {
				if item.Version != version || item.Hash == 0 {
					t.Fatalf("claimed version: %+v", item)
				}
				return []store.ReadModelInvalidation{item}
			}
		}
		t.Fatal("owned invalidation missing")
		return nil
	}
	first := claim(1)
	bump()
	if err := relay.ack(ctx, first); err != nil {
		t.Fatal(err)
	}
	var version, published int64
	if err := pool.QueryRow(ctx, `SELECT version,published_version FROM read_model_versions WHERE model='dialog_light' AND owner_user_id=$1 AND peer_type='user' AND peer_id=$2`, owner, peer).Scan(&version, &published); err != nil {
		t.Fatal(err)
	}
	if version != 2 || published != 1 {
		t.Fatalf("old ACK swallowed new version: version=%d published=%d", version, published)
	}
	if err := relay.ack(ctx, claim(2)); err != nil {
		t.Fatal(err)
	}
	items, err := relay.claim(ctx)
	if err != nil || containsReadModelInvalidation(items, owner, peer) {
		t.Fatalf("acknowledged key remained claimable: %v %v", items, err)
	}
}

func TestIdleOutboxShardAndDeadlinePostgres(t *testing.T) {
	for _, cfg := range []outboxStateConfig{dispatchOutboxStateConfig, deliveryOutboxStateConfig} {
		t.Run(cfg.itemsTable, func(t *testing.T) {
			pool := testPool(t)
			ctx := context.Background()
			user := createTestUser(t, ctx, NewUserStore(pool), "+1886"+randomSuffix(t)+"91", "IdleDeadline", "")
			if cfg.kind == store.OutboxQueueDispatchPTS {
				cleanupDispatchUser(t, pool, user.ID)
				if _, err := NewUpdateEventStore(pool).AppendAllocatedWithDispatch(ctx, user.ID, domain.UpdateEvent{
					Type: domain.UpdateEventDialogPinned, PtsCount: 1, Date: 1700002000,
					Peer: domain.Peer{Type: domain.PeerTypeUser, ID: user.ID}, Bool: true,
				}, [8]byte{}, 0); err != nil {
					t.Fatal(err)
				}
			} else {
				cleanupOutboxUser(t, pool, user.ID)
				if _, err := NewDeliveryOutboxStore(pool).Enqueue(ctx, store.DeliveryOutboxEnqueue{
					TargetUserID: user.ID, Payload: []byte{1}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
				}); err != nil {
					t.Fatal(err)
				}
			}
			s := newDurableOutboxState(pool, cfg, time.Minute)
			shard := int(user.ID % store.DispatchOutboxLogicalShards)
			wrong := (shard + 1) % store.DispatchOutboxLogicalShards
			if _, ok, err := s.NextReadyAt(ctx, cfg.kind, 256, []int{wrong}); err != nil || ok {
				t.Fatalf("wrong shard saw lane: %v %v", ok, err)
			}
			first, ok, err := s.NextReadyAt(ctx, cfg.kind, 256, []int{shard, shard})
			if err != nil || !ok || first.Kind != store.OutboxReadyClaim || first.ReadyAt.After(first.ObservedAt) {
				t.Fatalf("new lane not ready: %+v %v", first, err)
			}
			var future time.Time
			query := fmt.Sprintf("UPDATE %s SET ready_at=clock_timestamp()+interval '1 hour' WHERE stream_id=$1 RETURNING ready_at", cfg.lanesTable)
			if err := pool.QueryRow(ctx, query, user.ID).Scan(&future); err != nil {
				t.Fatal(err)
			}
			for _, ids := range [][]int{nil, {shard}} {
				count := 0
				if ids != nil {
					count = 256
				}
				next, ok, err := s.NextReadyAt(ctx, cfg.kind, count, ids)
				if err != nil || !ok || !next.ReadyAt.Equal(future) || next.Kind != store.OutboxReadyClaim || !next.ReadyAt.After(next.ObservedAt.Add(59*time.Minute)) {
					t.Fatalf("future deadline stale/corrupt: %+v %v", next, err)
				}
			}
			canceled, cancel := context.WithCancel(ctx)
			cancel()
			if _, ok, err := s.NextReadyAt(canceled, cfg.kind, 0, nil); ok || !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation ignored: %v", err)
			}
			if _, err := pool.Exec(ctx, fmt.Sprintf("UPDATE %s SET ready_at=clock_timestamp()-interval '1 second' WHERE stream_id=$1", cfg.lanesTable), user.ID); err != nil {
				t.Fatal(err)
			}
			windows, err := s.ClaimWindows(ctx, store.OutboxClaimRequest{
				QueueKind: cfg.kind, Owner: "idle-shard-owner", LogicalShardCount: 256, LogicalShardIDs: []int{shard},
				LaneLimit: 1, WindowSize: 1, WindowByteLimit: ptsEventByteEstimate, LeaseDuration: time.Minute,
				PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
			})
			if err != nil || len(windows) != 1 || len(windows[0].Items) != 1 {
				t.Fatalf("claim = %+v %v", windows, err)
			}
			next, ok, err := s.NextReadyAt(ctx, cfg.kind, 256, []int{shard})
			if err != nil || !ok || next.Kind != store.OutboxReadyRecoverLease || !next.ReadyAt.Equal(windows[0].Items[0].EvidenceDeadline) {
				t.Fatalf("lease deadline changed: %+v %v", next, err)
			}
			if _, ok, err := s.NextReadyAt(ctx, cfg.kind, 256, []int{wrong}); err != nil || ok {
				t.Fatalf("wrong shard saw lease: %v %v", ok, err)
			}
			bound, err := s.BindAttemptTargets(ctx, []store.OutboxAttemptTargetSet{{Ref: windows[0].Items[0].Ref, SourceInstanceID: "idle-shard", Targets: []store.OutboxAttemptTarget{}}})
			if err != nil || len(bound) != 1 || bound[0].Outcome != store.OutboxBindTargetBound {
				t.Fatalf("bind = %+v %v", bound, err)
			}
			for _, ids := range [][]int{nil, {shard}} {
				count := 0
				if ids != nil {
					count = 256
				}
				if _, ok, err := s.NextReadyAt(ctx, cfg.kind, count, ids); err != nil || ok {
					t.Fatalf("finalizable lane retained timer: %v %v", ok, err)
				}
			}
		})
	}
}
