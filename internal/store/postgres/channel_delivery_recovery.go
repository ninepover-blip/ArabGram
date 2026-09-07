package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

// RecoverBoundWindows is process-start exact replay only. It never recomputes
// or extends either persisted physical deadline.
func (s *ChannelDeliveryStore) RecoverBoundWindows(ctx context.Context, req store.OutboxRecoverBoundRequest) ([]store.OutboxClaimWindow, error) {
	if req.QueueKind != store.OutboxQueueChannelPTS {
		return nil, fmt.Errorf("recover channel delivery windows: queue kind %d is not channel PTS", req.QueueKind)
	}
	shards, scoped, err := normalizeChannelDeliveryShards(req.LogicalShardCount, req.LogicalShardIDs)
	if err != nil {
		return nil, fmt.Errorf("recover channel delivery windows: %w", err)
	}
	if scoped && len(shards) == 0 {
		return []store.OutboxClaimWindow{}, nil
	}
	if req.LaneLimit <= 0 {
		req.LaneLimit = 64
	}
	if req.LaneLimit > maxChannelDeliveryLaneLimit {
		req.LaneLimit = maxChannelDeliveryLaneLimit
	}
	if req.Mode == store.OutboxBoundRecoveryExactOwnerLive {
		if strings.TrimSpace(req.Owner) == "" || strings.TrimSpace(req.Owner) != req.Owner || len(req.Owner) > store.MaxDeliveryLeaseOwnerBytes {
			return nil, fmt.Errorf("recover channel delivery windows: exact-owner mode requires owner")
		}
		if req.LeaseDuration <= 0 {
			req.LeaseDuration = defaultChannelDeliveryLease
		}
		if req.LeaseDuration.Microseconds() <= 0 {
			return nil, fmt.Errorf("recover channel delivery windows: lease duration is required")
		}
		return s.recoverExactOwnerLiveChannelWindows(ctx, req, shards, scoped)
	}
	return nil, fmt.Errorf("recover channel delivery windows: recovery mode is required")
}

func (s *ChannelDeliveryStore) recoverExactOwnerLiveChannelWindows(ctx context.Context, req store.OutboxRecoverBoundRequest, shards []int16, scoped bool) ([]store.OutboxClaimWindow, error) {
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
SELECT l.channel_id, l.lease_fence
FROM channel_delivery_lanes l
WHERE ($1::boolean = false OR l.logical_shard = ANY($2::smallint[]))
  AND l.state = 'leased' AND l.lease_owner = $3
  AND l.lease_until > clock_timestamp() AND l.window_tail_item_id IS NOT NULL
  AND EXISTS (
    SELECT 1 FROM channel_delivery_attempts head
    WHERE head.channel_id = l.channel_id AND head.item_id = l.head_item_id
      AND head.lease_fence = l.lease_fence AND head.targets_bound
      AND head.command_not_after > clock_timestamp()
      AND head.evidence_deadline > clock_timestamp()
      AND head.resolution = 'pending'
  )
  AND NOT EXISTS (
    SELECT 1 FROM channel_delivery_attempts invalid
    WHERE invalid.channel_id = l.channel_id AND invalid.lease_fence = l.lease_fence
      AND (NOT invalid.targets_bound OR invalid.resolution <> 'pending'
           OR invalid.command_not_after <= clock_timestamp()
           OR invalid.evidence_deadline <= clock_timestamp())
  )
ORDER BY l.lease_until, l.channel_id
LIMIT $4
FOR UPDATE OF l SKIP LOCKED`, scoped, shards, req.Owner, req.LaneLimit)
	if err != nil {
		return nil, fmt.Errorf("pick exact-owner channel delivery lanes: %w", err)
	}
	type pickedLane struct{ channelID, fence int64 }
	picked := make([]pickedLane, 0, req.LaneLimit)
	for rows.Next() {
		var lane pickedLane
		if err := rows.Scan(&lane.channelID, &lane.fence); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan exact-owner channel delivery lane: %w", err)
		}
		picked = append(picked, lane)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("iterate exact-owner channel delivery lanes: %w", err)
	}
	rows.Close()
	channelIDs := make([]int64, 0, len(picked))
	for _, lane := range picked {
		tag, err := tx.Exec(ctx, `
UPDATE channel_delivery_lanes
SET lease_until = GREATEST(lease_until, clock_timestamp() + ($3::bigint * interval '1 microsecond')),
    updated_at = clock_timestamp()
WHERE channel_id = $1 AND lease_fence = $2
  AND lease_owner = $4 AND state = 'leased'`,
			lane.channelID, lane.fence, req.LeaseDuration.Microseconds(), req.Owner)
		if err != nil {
			return nil, fmt.Errorf("renew exact-owner channel delivery lane: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return nil, fmt.Errorf("renew exact-owner channel delivery lane %d was fenced", lane.channelID)
		}
		if _, err := tx.Exec(ctx, `
UPDATE channel_delivery_attempts
SET lease_until = GREATEST(lease_until, clock_timestamp() + ($3::bigint * interval '1 microsecond'))
WHERE channel_id = $1 AND lease_fence = $2`,
			lane.channelID, lane.fence, req.LeaseDuration.Microseconds()); err != nil {
			return nil, fmt.Errorf("renew exact-owner channel delivery attempts: %w", err)
		}
		channelIDs = append(channelIDs, lane.channelID)
	}
	windows := []store.OutboxClaimWindow{}
	if len(channelIDs) > 0 {
		windows, err = loadRecoveredChannelDeliveryWindows(ctx, tx, req.Owner, channelIDs)
		if err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit exact-owner channel delivery recovery: %w", err)
	}
	return windows, nil
}

type recoveredChannelDeliveryItem struct {
	item        store.OutboxClaimedItem
	ordinal     int
	targetCount int
}

func loadRecoveredChannelDeliveryWindows(ctx context.Context, tx pgx.Tx, owner string, channelIDs []int64) ([]store.OutboxClaimWindow, error) {
	rows, err := tx.Query(ctx, `
SELECT l.channel_id, l.lease_fence, l.lease_until, l.head_item_id, l.head_sequence,
       a.item_id, a.min_pts, a.max_pts, a.attempt_no, a.window_ordinal,
       a.delivery_source_instance_id, a.target_count,
       a.command_not_after, a.evidence_deadline,
       e.projection_kind, e.audience_kind, e.audience_user_ids, e.affected_user_ids,
       t.target_instance_id, t.target_user_id, t.batch_id, t.command_id
FROM channel_delivery_lanes l
JOIN channel_delivery_attempts a
  ON a.channel_id = l.channel_id AND a.lease_fence = l.lease_fence
 AND a.targets_bound AND a.resolution = 'pending'
JOIN channel_delivery_events e ON e.channel_id = a.channel_id AND e.id = a.item_id
LEFT JOIN channel_delivery_attempt_targets t
  ON t.item_id = a.item_id AND t.lease_fence = a.lease_fence
WHERE l.channel_id = ANY($1::bigint[])
  AND l.state = 'leased' AND l.lease_owner = $2
ORDER BY l.channel_id, a.window_ordinal,
         t.target_instance_id, t.target_user_id, t.batch_id, t.command_id`, channelIDs, owner)
	if err != nil {
		return nil, fmt.Errorf("load recovered channel delivery windows: %w", err)
	}
	defer rows.Close()
	windows := make([]store.OutboxClaimWindow, 0, len(channelIDs))
	var currentWindow *store.OutboxClaimWindow
	var current *recoveredChannelDeliveryItem
	var laneHeadPTS int
	finishItem := func() error {
		if current == nil {
			return nil
		}
		if len(current.item.Targets) != current.targetCount {
			return fmt.Errorf("recover channel item %d: target ledger count %d, want %d", current.item.Ref.ItemID, len(current.item.Targets), current.targetCount)
		}
		if currentWindow == nil || current.ordinal != len(currentWindow.Items) {
			return fmt.Errorf("recover channel item %d: window ordinal %d is not contiguous", current.item.Ref.ItemID, current.ordinal)
		}
		payload := current.item.Payload.(store.ChannelDeliveryPayload)
		if len(currentWindow.Items) == 0 {
			if payload.MinPTS != laneHeadPTS {
				return fmt.Errorf("recover channel item %d: head PTS disagrees with lane", current.item.Ref.ItemID)
			}
		} else {
			previous := currentWindow.Items[len(currentWindow.Items)-1].Payload.(store.ChannelDeliveryPayload)
			if payload.MinPTS != previous.MaxPTS+1 {
				return fmt.Errorf("recover channel item %d: non-contiguous PTS range", current.item.Ref.ItemID)
			}
		}
		currentWindow.Items = append(currentWindow.Items, current.item)
		current = nil
		return nil
	}
	for rows.Next() {
		var channelID, fence, headItemID, itemID int64
		var leaseUntil, commandNotAfter, evidenceDeadline time.Time
		var headSequence, minPTS, maxPTS, attempt, ordinal, targetCount int
		var source, projection, audience string
		var audienceUserIDs, affectedUserIDs []int64
		var targetInstance *string
		var targetUserID *int64
		var batch, command []byte
		if err := rows.Scan(
			&channelID, &fence, &leaseUntil, &headItemID, &headSequence,
			&itemID, &minPTS, &maxPTS, &attempt, &ordinal, &source, &targetCount,
			&commandNotAfter, &evidenceDeadline,
			&projection, &audience, &audienceUserIDs, &affectedUserIDs,
			&targetInstance, &targetUserID, &batch, &command,
		); err != nil {
			return nil, fmt.Errorf("scan recovered channel delivery window: %w", err)
		}
		if currentWindow == nil || currentWindow.StreamID != channelID {
			if err := finishItem(); err != nil {
				return nil, err
			}
			if headItemID != itemID {
				return nil, fmt.Errorf("recover channel %d: first item %d is not lane head %d", channelID, itemID, headItemID)
			}
			laneHeadPTS = headSequence
			windows = append(windows, store.OutboxClaimWindow{
				QueueKind: store.OutboxQueueChannelPTS, StreamID: channelID, Owner: owner,
				LeaseFence: uint64(fence), LeaseUntil: leaseUntil,
			})
			currentWindow = &windows[len(windows)-1]
		}
		if current == nil || current.item.Ref.ItemID != itemID {
			if err := finishItem(); err != nil {
				return nil, err
			}
			current = &recoveredChannelDeliveryItem{
				ordinal: ordinal, targetCount: targetCount,
				item: store.OutboxClaimedItem{
					Ref: store.OutboxAttemptRef{
						QueueKind: store.OutboxQueueChannelPTS, StreamID: channelID,
						ItemID: itemID, Sequence: int64(maxPTS), LeaseFence: uint64(fence), Attempt: attempt,
					},
					RecoveryPolicy:   store.OutboxRecoveryDifference,
					SourceInstanceID: source,
					CommandNotAfter:  commandNotAfter,
					EvidenceDeadline: evidenceDeadline,
					Payload: store.ChannelDeliveryPayload{
						MinPTS: minPTS, MaxPTS: maxPTS,
						ProjectionKind:  domain.ChannelUpdateEventType(projection),
						AudienceKind:    store.ChannelDeliveryAudienceKind(audience),
						AudienceUserIDs: append([]int64(nil), audienceUserIDs...),
						AffectedUserIDs: append([]int64(nil), affectedUserIDs...),
					},
				},
			}
			if !commandNotAfter.Before(evidenceDeadline) || !evidenceDeadline.Before(leaseUntil) {
				return nil, fmt.Errorf("recover channel item %d: invalid persisted deadlines", itemID)
			}
		}
		if targetInstance == nil {
			if targetCount != 0 || targetUserID != nil || len(batch) != 0 || len(command) != 0 {
				return nil, fmt.Errorf("recover channel item %d: malformed empty target ledger", itemID)
			}
			continue
		}
		if targetUserID == nil || *targetUserID != 0 || len(batch) != 16 || len(command) != 16 {
			return nil, fmt.Errorf("recover channel item %d: malformed channel target ledger", itemID)
		}
		target := store.OutboxAttemptTarget{TargetInstanceID: *targetInstance}
		copy(target.BatchID[:], batch)
		copy(target.CommandID[:], command)
		current.item.Targets = append(current.item.Targets, target)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recovered channel delivery windows: %w", err)
	}
	if err := finishItem(); err != nil {
		return nil, err
	}
	if len(windows) != len(channelIDs) {
		return nil, fmt.Errorf("recover channel delivery windows: loaded %d lanes, want %d", len(windows), len(channelIDs))
	}
	return windows, nil
}

// RecoverFinalizableAttempts closes the evidence/resolve -> finalize crash
// window. FinalizeAttempts takes the blocking lane lock and revalidates fence.
func (s *ChannelDeliveryStore) RecoverFinalizableAttempts(ctx context.Context, req store.OutboxRecoverFinalizableRequest) ([]store.OutboxFinalizeRequest, error) {
	if req.QueueKind != store.OutboxQueueChannelPTS {
		return nil, fmt.Errorf("recover finalizable channel attempts: queue kind %d is not channel PTS", req.QueueKind)
	}
	if req.AttemptLimit <= 0 || req.AttemptLimit > store.MaxDeliveryBatchItems {
		return nil, fmt.Errorf("recover finalizable channel attempts: attempt limit must be between 1 and %d", store.MaxDeliveryBatchItems)
	}
	shards, scoped, err := normalizeChannelDeliveryShards(req.LogicalShardCount, req.LogicalShardIDs)
	if err != nil {
		return nil, fmt.Errorf("recover finalizable channel attempts: %w", err)
	}
	if scoped && len(shards) == 0 {
		return []store.OutboxFinalizeRequest{}, nil
	}
	if req.LaneLimit <= 0 {
		req.LaneLimit = 64
	}
	if req.LaneLimit > maxChannelDeliveryLaneLimit {
		req.LaneLimit = maxChannelDeliveryLaneLimit
	}
	rows, err := s.db.Query(ctx, `
WITH picked AS MATERIALIZED (
    SELECT l.channel_id, l.lease_fence
    FROM channel_delivery_lanes l
    WHERE ($1::boolean = false OR l.logical_shard = ANY($2::smallint[]))
      AND l.state = 'leased'
      AND EXISTS (
        SELECT 1 FROM channel_delivery_attempts ready
        WHERE ready.channel_id = l.channel_id AND ready.lease_fence = l.lease_fence
          AND ready.resolution <> 'pending'
          AND (ready.retry_at IS NULL OR ready.retry_at <= clock_timestamp())
      )
    ORDER BY l.updated_at, l.channel_id
    LIMIT $3
)
SELECT a.channel_id, a.item_id, a.max_pts, a.lease_fence, a.attempt_no
FROM picked p
JOIN channel_delivery_attempts a
  ON a.channel_id = p.channel_id AND a.lease_fence = p.lease_fence
WHERE a.resolution <> 'pending'
  AND (a.retry_at IS NULL OR a.retry_at <= clock_timestamp())
ORDER BY a.channel_id, a.window_ordinal
LIMIT $4`, scoped, shards, req.LaneLimit, req.AttemptLimit)
	if err != nil {
		return nil, fmt.Errorf("recover finalizable channel attempts: %w", err)
	}
	defer rows.Close()
	requests := make([]store.OutboxFinalizeRequest, 0)
	for rows.Next() {
		var ref store.OutboxAttemptRef
		var fence int64
		ref.QueueKind = store.OutboxQueueChannelPTS
		if err := rows.Scan(&ref.StreamID, &ref.ItemID, &ref.Sequence, &fence, &ref.Attempt); err != nil {
			return nil, fmt.Errorf("scan finalizable channel attempt: %w", err)
		}
		if fence <= 0 {
			return nil, fmt.Errorf("scan finalizable channel attempt: invalid fence %d", fence)
		}
		ref.LeaseFence = uint64(fence)
		requests = append(requests, store.OutboxFinalizeRequest{Ref: ref})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate finalizable channel attempts: %w", err)
	}
	return requests, nil
}

func (s *ChannelDeliveryStore) ExpireEvidenceDeadlines(ctx context.Context, req store.OutboxEvidenceExpiryRequest) ([]store.OutboxFinalizeRequest, error) {
	if req.QueueKind != store.OutboxQueueChannelPTS {
		return nil, fmt.Errorf("expire channel evidence deadlines: queue kind %d is not channel PTS", req.QueueKind)
	}
	if req.MaxAttempts <= 0 {
		return nil, fmt.Errorf("expire channel evidence deadlines: max attempts is required")
	}
	shards, scoped, err := normalizeChannelDeliveryShards(req.LogicalShardCount, req.LogicalShardIDs)
	if err != nil {
		return nil, fmt.Errorf("expire channel evidence deadlines: %w", err)
	}
	if scoped && len(shards) == 0 {
		return []store.OutboxFinalizeRequest{}, nil
	}
	if req.LaneLimit <= 0 {
		req.LaneLimit = 64
	}
	if req.LaneLimit > maxChannelDeliveryLaneLimit {
		req.LaneLimit = maxChannelDeliveryLaneLimit
	}
	rows, err := s.db.Query(ctx, `
WITH candidates AS MATERIALIZED (
    SELECT a.item_id, a.lease_fence, a.channel_id, a.max_pts, a.attempt_no,
           a.evidence_deadline
    FROM channel_delivery_attempts a
    JOIN channel_delivery_lanes l
      ON l.channel_id = a.channel_id AND l.lease_fence = a.lease_fence
     AND l.head_item_id = a.item_id AND l.state = 'leased'
    WHERE ($1::boolean = false OR l.logical_shard = ANY($2::smallint[]))
      AND a.evidence_deadline <= clock_timestamp()
      AND a.resolution = 'pending'
    ORDER BY a.evidence_deadline, a.channel_id, a.item_id
    LIMIT $3
    FOR UPDATE OF a SKIP LOCKED
), resolved AS (
    UPDATE channel_delivery_attempts a
    SET resolution = CASE WHEN c.attempt_no >= $4 THEN 'terminal_resync' ELSE 'retry' END,
        retry_at = CASE WHEN c.attempt_no >= $4 THEN NULL
                        ELSE GREATEST(clock_timestamp(), c.evidence_deadline) END,
        resolution_error = CASE WHEN c.attempt_no >= $4
          THEN 'physical evidence deadline exhausted'
          ELSE 'physical evidence deadline expired'
        END,
        resolved_at = clock_timestamp()
    FROM candidates c
    WHERE a.item_id = c.item_id AND a.lease_fence = c.lease_fence
      AND a.resolution = 'pending'
    RETURNING a.channel_id, a.item_id, a.max_pts, a.lease_fence, a.attempt_no
)
SELECT channel_id, item_id, max_pts, lease_fence, attempt_no
FROM resolved
ORDER BY channel_id, max_pts, item_id`, scoped, shards, req.LaneLimit, req.MaxAttempts)
	if err != nil {
		return nil, fmt.Errorf("expire channel evidence deadlines: %w", err)
	}
	defer rows.Close()
	requests := make([]store.OutboxFinalizeRequest, 0, req.LaneLimit)
	for rows.Next() {
		var ref store.OutboxAttemptRef
		var fence int64
		ref.QueueKind = store.OutboxQueueChannelPTS
		if err := rows.Scan(&ref.StreamID, &ref.ItemID, &ref.Sequence, &fence, &ref.Attempt); err != nil {
			return nil, fmt.Errorf("scan expired channel evidence deadline: %w", err)
		}
		if fence <= 0 {
			return nil, fmt.Errorf("scan expired channel evidence deadline: invalid fence %d", fence)
		}
		ref.LeaseFence = uint64(fence)
		requests = append(requests, store.OutboxFinalizeRequest{Ref: ref})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate expired channel evidence deadlines: %w", err)
	}
	return requests, nil
}

var _ store.ChannelDeliveryStore = (*ChannelDeliveryStore)(nil)
