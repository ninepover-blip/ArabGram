package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestDeliveryClaimsAllocateOneFencePerStatementPostgres(t *testing.T) {
	t.Run("account_absolute", func(t *testing.T) {
		pool := testPool(t)
		ctx := context.Background()
		users := NewUserStore(pool)
		userIDs := []int64{
			createTestUser(t, ctx, users, "+1889"+randomSuffix(t)+"01", "FenceAbsoluteA", "").ID,
			createTestUser(t, ctx, users, "+1889"+randomSuffix(t)+"02", "FenceAbsoluteB", "").ID,
		}
		outbox := NewDeliveryOutboxStore(pool, WithDeliveryLeaseTimeout(time.Minute))
		for _, userID := range userIDs {
			cleanupOutboxUser(t, pool, userID)
			if _, err := outbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
				TargetUserID: userID, Payload: []byte{1}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
			}); err != nil {
				t.Fatalf("enqueue user %d: %v", userID, err)
			}
		}

		assertStatementFenceEpochs(t, ctx, pool, "edge_delivery_outbox_lease_fence_seq", outbox,
			store.OutboxQueueAbsoluteDelivery, "edge_delivery_outbox_lanes", "stream_id", userIDs)
	})

	t.Run("account_pts", func(t *testing.T) {
		pool := testPool(t)
		ctx := context.Background()
		users := NewUserStore(pool)
		userIDs := []int64{
			createTestUser(t, ctx, users, "+1889"+randomSuffix(t)+"11", "FencePTSA", "").ID,
			createTestUser(t, ctx, users, "+1889"+randomSuffix(t)+"12", "FencePTSB", "").ID,
		}
		for _, userID := range userIDs {
			cleanupDispatchUser(t, pool, userID)
			if _, err := pool.Exec(ctx, `
INSERT INTO user_update_events (user_id, pts, pts_count, date, event_type)
VALUES ($1, 1, 1, 1700003000, 'noop')`, userID); err != nil {
				t.Fatalf("insert event for user %d: %v", userID, err)
			}
			if _, err := pool.Exec(ctx, `
INSERT INTO dispatch_outbox (target_user_id, pts, event_type)
VALUES ($1, 1, 'noop')`, userID); err != nil {
				t.Fatalf("insert outbox for user %d: %v", userID, err)
			}
		}

		assertStatementFenceEpochs(t, ctx, pool, "dispatch_outbox_lease_fence_seq", NewDispatchOutboxStore(pool),
			store.OutboxQueueDispatchPTS, "dispatch_outbox_lanes", "stream_id", userIDs)
	})

	t.Run("channel", func(t *testing.T) {
		pool := testPool(t)
		ctx := context.Background()
		suffix := channelDeliveryRandomSuffix(t)
		users := NewUserStore(pool)
		owner := createTestUser(t, ctx, users, "+1889"+suffix+"21", "FenceChannelOwner", "")
		channels := newTestChannelStore(pool)
		channelIDs := make([]int64, 0, 2)
		for _, title := range []string{"fence-channel-a-", "fence-channel-b-"} {
			created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
				CreatorUserID: owner.ID, Title: title + suffix, Megagroup: true, Date: 1700003100,
			})
			if err != nil {
				t.Fatalf("create channel: %v", err)
			}
			channelIDs = append(channelIDs, created.Channel.ID)
		}
		t.Cleanup(func() {
			cleanupCtx := context.Background()
			_, _ = pool.Exec(cleanupCtx, `DELETE FROM channel_delivery_attempts WHERE channel_id = ANY($1::bigint[])`, channelIDs)
			_, _ = pool.Exec(cleanupCtx, `DELETE FROM channels WHERE id = ANY($1::bigint[])`, channelIDs)
			_, _ = pool.Exec(cleanupCtx, `DELETE FROM users WHERE id = $1`, owner.ID)
		})

		assertStatementFenceEpochs(t, ctx, pool, "channel_delivery_lease_fence_seq", NewChannelDeliveryStore(pool),
			store.OutboxQueueChannelPTS, "channel_delivery_lanes", "channel_id", channelIDs)
	})
}

type deliveryClaimStore interface {
	ClaimWindows(context.Context, store.OutboxClaimRequest) ([]store.OutboxClaimWindow, error)
	NextReadyAt(context.Context, store.OutboxQueueKind, int, []int) (store.OutboxNextReady, bool, error)
}

func assertStatementFenceEpochs(
	t *testing.T,
	ctx context.Context,
	db *pgxpool.Pool,
	sequence string,
	claims deliveryClaimStore,
	kind store.OutboxQueueKind,
	laneTable string,
	laneKey string,
	streamIDs []int64,
) {
	t.Helper()
	ready, ok, err := claims.NextReadyAt(ctx, kind, 0, nil)
	if err != nil || !ok || ready.Kind != store.OutboxReadyClaim {
		t.Fatalf("initial %s ready = %+v ok=%v err=%v", sequence, ready, ok, err)
	}
	var baseline int64
	if err := db.QueryRow(ctx, `SELECT nextval($1::regclass)`, "public."+sequence).Scan(&baseline); err != nil {
		t.Fatalf("prime %s: %v", sequence, err)
	}
	req := store.OutboxClaimRequest{
		QueueKind: kind, Owner: "egress-fence-statement", LaneLimit: len(streamIDs),
		WindowSize: 1, WindowByteLimit: ptsEventByteEstimate, LeaseDuration: time.Minute,
		PhysicalDuration: 10 * time.Second, ClockSkewAllowance: time.Second,
	}
	first, err := claims.ClaimWindows(ctx, req)
	if err != nil || len(first) != len(streamIDs) {
		t.Fatalf("first %s claim = %d err=%v, want %d", sequence, len(first), err, len(streamIDs))
	}
	firstFence := first[0].LeaseFence
	if firstFence != uint64(baseline+1) {
		t.Fatalf("first %s fence=%d, want baseline+1=%d", sequence, firstFence, baseline+1)
	}
	for i, window := range first {
		if window.LeaseFence != firstFence || len(window.Items) != 1 {
			t.Fatalf("first %s window %d = %+v, want shared fence %d and one item", sequence, i, window, firstFence)
		}
		if i > 0 && window.Items[0].Ref == first[i-1].Items[0].Ref {
			t.Fatalf("different streams share exact attempt ref: %+v", window.Items[0].Ref)
		}
	}
	ready, ok, err = claims.NextReadyAt(ctx, kind, 0, nil)
	if err != nil || !ok || ready.Kind != store.OutboxReadyRecoverLease {
		t.Fatalf("leased %s ready = %+v ok=%v err=%v", sequence, ready, ok, err)
	}

	expireSQL := "UPDATE " + laneTable + " SET lease_until = now() - interval '1 second' WHERE " + laneKey + " = ANY($1::bigint[])"
	if _, err := db.Exec(ctx, expireSQL, streamIDs); err != nil {
		t.Fatalf("expire %s lanes: %v", sequence, err)
	}
	second, err := claims.ClaimWindows(ctx, req)
	if err != nil || len(second) != len(streamIDs) {
		t.Fatalf("second %s claim = %d err=%v, want %d", sequence, len(second), err, len(streamIDs))
	}
	secondFence := second[0].LeaseFence
	if secondFence != firstFence+1 {
		t.Fatalf("second %s fence=%d, want %d", sequence, secondFence, firstFence+1)
	}
	for _, window := range second {
		if window.LeaseFence != secondFence {
			t.Fatalf("second %s stream %d fence=%d, want shared %d", sequence, window.StreamID, window.LeaseFence, secondFence)
		}
	}

	var beforeEmpty int64
	if err := db.QueryRow(ctx, "SELECT last_value FROM public."+sequence).Scan(&beforeEmpty); err != nil {
		t.Fatalf("read %s before empty poll: %v", sequence, err)
	}
	empty, err := claims.ClaimWindows(ctx, req)
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty %s claim = %d err=%v", sequence, len(empty), err)
	}
	var afterEmpty int64
	if err := db.QueryRow(ctx, "SELECT last_value FROM public."+sequence).Scan(&afterEmpty); err != nil {
		t.Fatalf("read %s after empty poll: %v", sequence, err)
	}
	if afterEmpty != beforeEmpty {
		t.Fatalf("empty %s claim advanced sequence %d -> %d", sequence, beforeEmpty, afterEmpty)
	}
}
