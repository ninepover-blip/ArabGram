package postgres

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

type channelDeliveryLaneState struct {
	headItemID   int64
	headSequence int
	leaseFence   int64
	state        string
	owner        *string
	leaseUntil   *time.Time
}

type channelTerminalItem struct {
	itemID     int64
	minPTS     int
	maxPTS     int
	resolution string
}

func (s *ChannelDeliveryStore) FinalizeAttempts(ctx context.Context, requests []store.OutboxFinalizeRequest) (store.OutboxFinalizeBatch, error) {
	if len(requests) > store.MaxDeliveryBatchItems {
		return store.OutboxFinalizeBatch{}, fmt.Errorf("finalize channel attempts: batch exceeds %d items", store.MaxDeliveryBatchItems)
	}
	batch := store.OutboxFinalizeBatch{Results: make([]store.OutboxFinalizeResult, len(requests))}
	validByStream := make(map[int64][]int)
	for i := range requests {
		batch.Results[i] = store.OutboxFinalizeResult{Ref: requests[i].Ref, Outcome: store.OutboxFinalizeRejected}
		if err := validateChannelFinalizeRequest(requests[i]); err != nil {
			return store.OutboxFinalizeBatch{}, fmt.Errorf("finalize channel attempt: invalid request at index %d: %w", i, err)
		}
		validByStream[requests[i].Ref.StreamID] = append(validByStream[requests[i].Ref.StreamID], i)
	}
	if len(validByStream) == 0 {
		return batch, nil
	}
	streams := make([]int64, 0, len(validByStream))
	for streamID := range validByStream {
		streams = append(streams, streamID)
	}
	sort.Slice(streams, func(i, j int) bool { return streams[i] < streams[j] })

	for groupStart := 0; groupStart < len(streams); groupStart += outboxFinalizeTransactionLanes {
		groupEnd := min(groupStart+outboxFinalizeTransactionLanes, len(streams))
		group := streams[groupStart:groupEnd]
		err := func() error {
			tx, err := s.begin(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = tx.Rollback(ctx) }()
			// Match private/absolute outbox finalization: acquire lane locks in
			// stable order and amortize WAL flushes over a hard-bounded group. The
			// bound prevents a stalled channel from retaining an unbounded set of
			// unrelated lane locks.
			for _, streamID := range group {
				if err := finalizeChannelDeliveryStream(ctx, tx, streamID, requests, validByStream[streamID], &batch); err != nil {
					return err
				}
			}
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("finalize channel delivery lanes %d..%d: commit: %w", group[0], group[len(group)-1], err)
			}
			return nil
		}()
		if err != nil {
			return store.OutboxFinalizeBatch{}, err
		}
	}
	return batch, nil
}

func finalizeChannelDeliveryStream(ctx context.Context, tx pgx.Tx, streamID int64, requests []store.OutboxFinalizeRequest, indices []int, batch *store.OutboxFinalizeBatch) error {
	var lane channelDeliveryLaneState
	err := tx.QueryRow(ctx, `
SELECT head_item_id, head_sequence, lease_fence, state, lease_owner, lease_until
FROM channel_delivery_lanes
WHERE channel_id = $1
FOR UPDATE`, streamID).Scan(
		&lane.headItemID,
		&lane.headSequence,
		&lane.leaseFence,
		&lane.state,
		&lane.owner,
		&lane.leaseUntil,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return classifyChannelFinalizeWithoutLane(ctx, tx, requests, indices, batch.Results)
	}
	if err != nil {
		return fmt.Errorf("lock channel delivery lane %d: %w", streamID, err)
	}

	attempts := make(map[int]channelDeliveryAttemptState, len(indices))
	for _, idx := range indices {
		state, err := loadChannelDeliveryAttemptForUpdate(ctx, tx, requests[idx].Ref)
		if errors.Is(err, pgx.ErrNoRows) {
			batch.Results[idx].Outcome = store.OutboxFinalizeFenced
			continue
		}
		if err != nil {
			return fmt.Errorf("load channel finalize attempt: %w", err)
		}
		attempts[idx] = state
		if !exactChannelAttemptRef(requests[idx].Ref, state) {
			batch.Results[idx].Outcome = store.OutboxFinalizeRejected
			continue
		}
		if lane.leaseFence != int64(requests[idx].Ref.LeaseFence) {
			batch.Results[idx].Outcome = store.OutboxFinalizeFenced
			continue
		}
		if state.resolution == "pending" {
			if requests[idx].Ref.ItemID == lane.headItemID {
				batch.Results[idx].Outcome = store.OutboxFinalizeRejected
			} else {
				batch.Results[idx].Outcome = store.OutboxFinalizeWaitingForPredecessor
			}
		} else {
			batch.Results[idx].Outcome = store.OutboxFinalizeWaitingForPredecessor
		}
	}

	windowSize, byteLimit := normalizeChannelDeliveryWindow(requests[indices[0]].WindowSize, requests[indices[0]].WindowByteLimit)
	terminal, err := listChannelTerminalPrefix(ctx, tx, streamID, lane, windowSize, byteLimit)
	if err != nil {
		return err
	}
	if len(terminal) == 0 {
		return nil
	}
	if terminal[0].resolution == "retry" {
		if err := scheduleChannelRetry(ctx, tx, streamID, lane, terminal[0]); err != nil {
			return err
		}
		applyChannelFinalizeResults(requests, indices, attempts, map[int64]string{terminal[0].itemID: "scheduled_retry"}, batch.Results)
		return nil
	}
	completed := make(map[int64]string, len(terminal))
	itemIDs := make([]int64, 0, len(terminal))
	for _, item := range terminal {
		if item.resolution == "retry" {
			break
		}
		itemIDs = append(itemIDs, item.itemID)
		completed[item.itemID] = channelFinalOutcome(item.resolution)
	}
	if len(itemIDs) == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `
DELETE FROM channel_delivery_events
WHERE channel_id = $1 AND id = ANY($2::bigint[])`, streamID, itemIDs); err != nil {
		return fmt.Errorf("delete finalized channel delivery events: %w", err)
	}

	// This is intentionally a new statement after the blocking lane lock and
	// deletes. READ COMMITTED must observe any producer that committed while
	// waiting for this lane, closing the empty -> non-empty race.
	successorID, successorSequence, found, err := loadChannelDeliverySuccessor(ctx, tx, streamID)
	if err != nil {
		return err
	}
	if !found {
		if _, err := tx.Exec(ctx, `
SELECT pg_advisory_xact_lock(outbox_lane_advisory_key('channel_pts', $1))`, streamID); err != nil {
			return fmt.Errorf("lock empty channel delivery lane transition: %w", err)
		}
		successorID, successorSequence, found, err = loadChannelDeliverySuccessor(ctx, tx, streamID)
		if err != nil {
			return err
		}
	}
	if !found {
		if _, err := tx.Exec(ctx, `DELETE FROM channel_delivery_lanes WHERE channel_id = $1`, streamID); err != nil {
			return fmt.Errorf("delete empty channel delivery lane: %w", err)
		}
	} else {
		if _, err := tx.Exec(ctx, `
UPDATE channel_delivery_lanes
SET head_item_id = $2, head_sequence = $3, updated_at = clock_timestamp()
WHERE channel_id = $1`, streamID, successorID, successorSequence); err != nil {
			return fmt.Errorf("advance channel delivery lane: %w", err)
		}
		var successorResolution string
		err := tx.QueryRow(ctx, `
SELECT resolution
FROM channel_delivery_attempts
WHERE item_id = $1 AND lease_fence = $2`, successorID, lane.leaseFence).Scan(&successorResolution)
		switch {
		case err == nil && successorResolution == "retry":
			successor := channelTerminalItem{itemID: successorID, minPTS: successorSequence, maxPTS: successorSequence, resolution: "retry"}
			if err := scheduleChannelRetry(ctx, tx, streamID, lane, successor); err != nil {
				return err
			}
			completed[successorID] = "scheduled_retry"
		case err == nil:
			// The successor belongs to the already-issued window and therefore to
			// this exact fence.  Releasing the lane here would create a forbidden
			// ready+bound-attempt state: ClaimWindows must reject that attempt while
			// NextReadyAt would otherwise advertise immediate claim work forever.
			// Keep the lease until the successor's evidence/finalization state
			// advances; its persisted deadline is the durable scheduler authority.
		case errors.Is(err, pgx.ErrNoRows):
			if _, err := tx.Exec(ctx, `
UPDATE channel_delivery_lanes
SET retry_count = 0, updated_at = clock_timestamp()
WHERE channel_id = $1`, streamID); err != nil {
				return fmt.Errorf("reset channel successor attempt counter: %w", err)
			}
			if requests[indices[0]].RetainLease {
				next, err := claimChannelSuccessorTx(ctx, tx, streamID, requests[indices[0]])
				if err != nil {
					return err
				}
				if len(next.Items) > 0 {
					batch.Next = append(batch.Next, next)
				}
			} else if err := releaseChannelLane(ctx, tx, streamID); err != nil {
				return err
			}
		default:
			return fmt.Errorf("load channel successor attempt: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `
DELETE FROM channel_delivery_attempts
WHERE lease_fence = $2 AND item_id = ANY($1::bigint[])`, itemIDs, lane.leaseFence); err != nil {
		return fmt.Errorf("delete finalized channel delivery attempts: %w", err)
	}
	applyChannelFinalizeResults(requests, indices, attempts, completed, batch.Results)
	return nil
}

func loadChannelDeliverySuccessor(ctx context.Context, tx pgx.Tx, streamID int64) (int64, int, bool, error) {
	var itemID int64
	var sequence int
	err := tx.QueryRow(ctx, `
SELECT id, min_pts
FROM channel_delivery_events
WHERE channel_id = $1
ORDER BY min_pts, id
LIMIT 1`, streamID).Scan(&itemID, &sequence)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, false, nil
	}
	if err != nil {
		return 0, 0, false, fmt.Errorf("load channel delivery successor: %w", err)
	}
	return itemID, sequence, true, nil
}

func validateChannelFinalizeRequest(req store.OutboxFinalizeRequest) error {
	if err := validateChannelAttemptRef(req.Ref); err != nil {
		return err
	}
	if req.RetainLease {
		owner := strings.TrimSpace(req.Owner)
		if owner == "" || owner != req.Owner || len(owner) > store.MaxDeliveryLeaseOwnerBytes {
			return errors.New("channel successor handoff requires canonical owner")
		}
	}
	return nil
}

func classifyChannelFinalizeWithoutLane(ctx context.Context, tx pgx.Tx, requests []store.OutboxFinalizeRequest, indices []int, results []store.OutboxFinalizeResult) error {
	for _, idx := range indices {
		var streamID, sequence int64
		var attempt int
		err := tx.QueryRow(ctx, `
SELECT channel_id, max_pts, attempt_no
FROM channel_delivery_attempts
WHERE item_id = $1 AND lease_fence = $2`, requests[idx].Ref.ItemID, int64(requests[idx].Ref.LeaseFence)).Scan(
			&streamID, &sequence, &attempt,
		)
		if errors.Is(err, pgx.ErrNoRows) {
			results[idx].Outcome = store.OutboxFinalizeFenced
			continue
		}
		if err != nil {
			return fmt.Errorf("classify finalized channel attempt: %w", err)
		}
		if streamID != requests[idx].Ref.StreamID || sequence != requests[idx].Ref.Sequence || attempt != requests[idx].Ref.Attempt {
			results[idx].Outcome = store.OutboxFinalizeRejected
		} else {
			results[idx].Outcome = store.OutboxFinalizeFenced
		}
	}
	return nil
}

func listChannelTerminalPrefix(ctx context.Context, tx pgx.Tx, streamID int64, lane channelDeliveryLaneState, windowSize, byteLimit int) ([]channelTerminalItem, error) {
	rows, err := tx.Query(ctx, `
WITH RECURSIVE prefix AS (
    SELECT e.id, e.min_pts, e.max_pts, a.resolution,
           1 AS ordinal,
		(32 + octet_length(e.projection_kind) + octet_length(e.audience_kind)
				+ 8 * cardinality(e.audience_user_ids)
				+ 8 * cardinality(e.affected_user_ids))::bigint AS used_bytes
    FROM channel_delivery_events e
    JOIN channel_delivery_attempts a
      ON a.item_id = e.id AND a.lease_fence = $3
    WHERE e.channel_id = $1 AND e.id = $2
      AND a.resolution IN ('confirmed', 'retry', 'terminal_resync')
      AND (a.retry_at IS NULL OR a.retry_at <= clock_timestamp())
  UNION ALL
    SELECT n.id, n.min_pts, n.max_pts, a.resolution,
           p.ordinal + 1,
		   p.used_bytes + 32 + octet_length(n.projection_kind)
			 + octet_length(n.audience_kind) + 8 * cardinality(n.audience_user_ids)
			 + 8 * cardinality(n.affected_user_ids)
    FROM prefix p
    JOIN LATERAL (
        SELECT e.id, e.min_pts, e.max_pts, e.projection_kind,
		       e.audience_kind, e.audience_user_ids, e.affected_user_ids
        FROM channel_delivery_events e
        WHERE e.channel_id = $1 AND e.min_pts = p.max_pts + 1
        ORDER BY e.id
        LIMIT 1
    ) n ON p.resolution <> 'retry'
    JOIN channel_delivery_attempts a
      ON a.item_id = n.id AND a.lease_fence = $3
     AND a.resolution IN ('confirmed', 'retry', 'terminal_resync')
     AND (a.retry_at IS NULL OR a.retry_at <= clock_timestamp())
    WHERE p.ordinal < $4
		  AND p.used_bytes + 32 + octet_length(n.projection_kind)
			+ octet_length(n.audience_kind) + 8 * cardinality(n.audience_user_ids)
			+ 8 * cardinality(n.affected_user_ids) <= $5
)
SELECT id, min_pts, max_pts, resolution
FROM prefix
ORDER BY ordinal`, streamID, lane.headItemID, lane.leaseFence, windowSize, byteLimit)
	if err != nil {
		return nil, fmt.Errorf("list channel terminal prefix: %w", err)
	}
	defer rows.Close()
	var out []channelTerminalItem
	for rows.Next() {
		var item channelTerminalItem
		if err := rows.Scan(&item.itemID, &item.minPTS, &item.maxPTS, &item.resolution); err != nil {
			return nil, fmt.Errorf("scan channel terminal prefix: %w", err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate channel terminal prefix: %w", err)
	}
	return out, nil
}

func scheduleChannelRetry(ctx context.Context, tx pgx.Tx, streamID int64, lane channelDeliveryLaneState, item channelTerminalItem) error {
	var retryAt time.Time
	if err := tx.QueryRow(ctx, `
SELECT retry_at
FROM channel_delivery_attempts
WHERE item_id = $1 AND lease_fence = $2 AND resolution = 'retry'
FOR UPDATE`, item.itemID, lane.leaseFence).Scan(&retryAt); err != nil {
		return fmt.Errorf("load channel scheduled retry: %w", err)
	}
	if _, err := tx.Exec(ctx, `
DELETE FROM channel_delivery_attempts
WHERE channel_id = $1 AND lease_fence = $2`, streamID, lane.leaseFence); err != nil {
		return fmt.Errorf("delete channel retry window attempts: %w", err)
	}
	if _, err := tx.Exec(ctx, `
UPDATE channel_delivery_lanes
SET head_item_id = $2,
    head_sequence = $3,
    state = 'ready',
    ready_at = $4,
    lease_owner = NULL,
    lease_until = NULL,
    window_tail_item_id = NULL,
    window_tail_sequence = NULL,
    updated_at = clock_timestamp()
WHERE channel_id = $1`, streamID, item.itemID, item.minPTS, retryAt); err != nil {
		return fmt.Errorf("schedule channel lane retry: %w", err)
	}
	return nil
}

func releaseChannelLane(ctx context.Context, tx pgx.Tx, streamID int64) error {
	_, err := tx.Exec(ctx, `
UPDATE channel_delivery_lanes
SET state = 'ready',
    ready_at = clock_timestamp(),
    lease_owner = NULL,
    lease_until = NULL,
    window_tail_item_id = NULL,
    window_tail_sequence = NULL,
    updated_at = clock_timestamp()
WHERE channel_id = $1`, streamID)
	if err != nil {
		return fmt.Errorf("release channel delivery lane: %w", err)
	}
	return nil
}

func channelFinalOutcome(resolution string) string {
	switch resolution {
	case "terminal_resync":
		return "terminal_resync"
	default:
		return "applied"
	}
}

func applyChannelFinalizeResults(requests []store.OutboxFinalizeRequest, indices []int, attempts map[int]channelDeliveryAttemptState, completed map[int64]string, results []store.OutboxFinalizeResult) {
	for _, idx := range indices {
		state, ok := attempts[idx]
		if !ok || !exactChannelAttemptRef(requests[idx].Ref, state) {
			continue
		}
		outcome, ok := completed[requests[idx].Ref.ItemID]
		if !ok {
			if state.resolution != "pending" {
				results[idx].Outcome = store.OutboxFinalizeWaitingForPredecessor
			}
			continue
		}
		switch outcome {
		case "scheduled_retry":
			results[idx].Outcome = store.OutboxFinalizeScheduledRetry
		case "terminal_resync":
			results[idx].Outcome = store.OutboxFinalizeTerminalResync
		default:
			results[idx].Outcome = store.OutboxFinalizeApplied
		}
	}
}

func claimChannelSuccessorTx(ctx context.Context, tx pgx.Tx, streamID int64, req store.OutboxFinalizeRequest) (store.OutboxClaimWindow, error) {
	windowSize, byteLimit := normalizeChannelDeliveryWindow(req.WindowSize, req.WindowByteLimit)
	lease := req.LeaseDuration
	if lease <= 0 {
		lease = defaultChannelDeliveryLease
	}
	var issuedAt, previousCommandNotAfter, previousEvidenceDeadline time.Time
	if err := tx.QueryRow(ctx, `
SELECT issued_at, command_not_after, evidence_deadline
FROM channel_delivery_attempts
WHERE channel_id = $1 AND lease_fence = $2
ORDER BY window_ordinal
LIMIT 1`, streamID, int64(req.Ref.LeaseFence)).Scan(
		&issuedAt, &previousCommandNotAfter, &previousEvidenceDeadline,
	); err != nil {
		return store.OutboxClaimWindow{}, fmt.Errorf("load channel successor deadlines: %w", err)
	}
	physicalDuration := previousCommandNotAfter.Sub(issuedAt)
	clockSkewAllowance := previousEvidenceDeadline.Sub(previousCommandNotAfter)
	if physicalDuration <= 0 || clockSkewAllowance <= 0 ||
		lease <= outboxLeaseSafetyMargin ||
		physicalDuration >= lease-outboxLeaseSafetyMargin ||
		clockSkewAllowance >= lease-outboxLeaseSafetyMargin-physicalDuration {
		return store.OutboxClaimWindow{}, fmt.Errorf("claim channel successor: persisted deadline intervals exceed retained lease")
	}
	rows, err := tx.Query(ctx, `
WITH current_lane AS MATERIALIZED (
    SELECT channel_id, head_item_id,
           nextval('public.channel_delivery_lease_fence_seq') AS lease_fence,
           1::integer AS attempt_no,
           statement_timestamp() + ($3::bigint * interval '1 microsecond') AS lease_until,
           statement_timestamp() + ($6::bigint * interval '1 microsecond') AS command_not_after,
           statement_timestamp() + (($6::bigint + $7::bigint) * interval '1 microsecond') AS evidence_deadline
    FROM channel_delivery_lanes
    WHERE channel_id = $1
), window_items AS MATERIALIZED (
    SELECT l.channel_id, l.lease_fence, l.attempt_no, l.lease_until,
           l.command_not_after, l.evidence_deadline,
           e.id AS item_id, e.min_pts, e.max_pts, e.projection_kind,
	           e.audience_kind, e.audience_user_ids, e.affected_user_ids, e.ordinal
    FROM current_lane l
    CROSS JOIN LATERAL (
        WITH RECURSIVE chain AS (
            SELECT h.id, h.min_pts, h.max_pts, h.projection_kind, h.audience_kind,
	                   h.audience_user_ids, h.affected_user_ids,
                   0 AS ordinal,
                   (32 + octet_length(h.projection_kind) + octet_length(h.audience_kind)
	                       + 8 * cardinality(h.audience_user_ids)
	                       + 8 * cardinality(h.affected_user_ids))::bigint AS used_bytes
            FROM channel_delivery_events h
            WHERE h.id = l.head_item_id AND h.channel_id = l.channel_id
          UNION ALL
            SELECT n.id, n.min_pts, n.max_pts, n.projection_kind, n.audience_kind,
	                   n.audience_user_ids, n.affected_user_ids,
                   c.ordinal + 1,
                   c.used_bytes + 32 + octet_length(n.projection_kind)
	                     + octet_length(n.audience_kind) + 8 * cardinality(n.audience_user_ids)
	                     + 8 * cardinality(n.affected_user_ids)
            FROM chain c
            JOIN LATERAL (
                SELECT x.id, x.min_pts, x.max_pts, x.projection_kind,
	                       x.audience_kind, x.audience_user_ids, x.affected_user_ids
                FROM channel_delivery_events x
                WHERE x.channel_id = l.channel_id AND x.min_pts = c.max_pts + 1
                ORDER BY x.id LIMIT 1
            ) n ON true
            WHERE c.ordinal + 1 < $4
              AND c.used_bytes + 32 + octet_length(n.projection_kind)
	                    + octet_length(n.audience_kind) + 8 * cardinality(n.audience_user_ids)
	                    + 8 * cardinality(n.affected_user_ids) <= $5
        )
	        SELECT id, min_pts, max_pts, projection_kind, audience_kind, audience_user_ids,
	               affected_user_ids, ordinal FROM chain
    ) e
), numbered AS MATERIALIZED (
    SELECT w.*
    FROM window_items w
), planned_tail AS MATERIALIZED (
    SELECT item_id, max_pts
    FROM window_items
    ORDER BY ordinal DESC
    LIMIT 1
), leased AS MATERIALIZED (
    UPDATE channel_delivery_lanes l
    SET state = 'leased',
        lease_fence = c.lease_fence,
        retry_count = c.attempt_no,
        lease_owner = $2,
        lease_until = c.lease_until,
        window_tail_item_id = tail.item_id,
        window_tail_sequence = tail.max_pts,
        updated_at = clock_timestamp()
    FROM current_lane c
    CROSS JOIN planned_tail tail
    WHERE l.channel_id = c.channel_id
    RETURNING l.channel_id, l.lease_fence, l.lease_until
), issued AS MATERIALIZED (
    INSERT INTO channel_delivery_attempts (
        item_id, lease_fence, channel_id, min_pts, max_pts,
        attempt_no, window_ordinal, owner, lease_until, command_not_after, evidence_deadline
    )
    SELECT item_id, lease_fence, channel_id, min_pts, max_pts,
           attempt_no, ordinal, $2, lease_until, command_not_after, evidence_deadline
    FROM numbered
    JOIN leased USING (channel_id, lease_fence, lease_until)
    RETURNING item_id, lease_fence, attempt_no
)
SELECT n.lease_fence, n.lease_until, n.command_not_after, n.evidence_deadline,
       n.item_id, n.min_pts, n.max_pts,
       n.projection_kind, n.audience_kind, n.audience_user_ids, n.affected_user_ids, n.attempt_no
FROM numbered n
JOIN issued i ON i.item_id = n.item_id AND i.lease_fence = n.lease_fence
JOIN leased l ON l.channel_id = n.channel_id AND l.lease_fence = n.lease_fence
ORDER BY n.ordinal`, streamID, req.Owner, lease.Microseconds(), windowSize, byteLimit,
		physicalDuration.Microseconds(), clockSkewAllowance.Microseconds())
	if err != nil {
		return store.OutboxClaimWindow{}, fmt.Errorf("claim channel successor: %w", err)
	}
	defer rows.Close()
	window := store.OutboxClaimWindow{QueueKind: store.OutboxQueueChannelPTS, StreamID: streamID, Owner: req.Owner}
	for rows.Next() {
		var fence, itemID int64
		var leaseUntil, commandNotAfter, evidenceDeadline time.Time
		var minPTS, maxPTS, attempt int
		var projection, audience string
		var audienceUserIDs []int64
		var affectedUserIDs []int64
		if err := rows.Scan(&fence, &leaseUntil, &commandNotAfter, &evidenceDeadline,
			&itemID, &minPTS, &maxPTS, &projection, &audience, &audienceUserIDs, &affectedUserIDs, &attempt); err != nil {
			return store.OutboxClaimWindow{}, fmt.Errorf("scan channel successor: %w", err)
		}
		window.LeaseFence = uint64(fence)
		window.LeaseUntil = leaseUntil
		window.Items = append(window.Items, store.OutboxClaimedItem{
			Ref: store.OutboxAttemptRef{
				QueueKind:  store.OutboxQueueChannelPTS,
				StreamID:   streamID,
				ItemID:     itemID,
				Sequence:   int64(maxPTS),
				LeaseFence: uint64(fence),
				Attempt:    attempt,
			},
			RecoveryPolicy:   store.OutboxRecoveryDifference,
			CommandNotAfter:  commandNotAfter,
			EvidenceDeadline: evidenceDeadline,
			Payload: store.ChannelDeliveryPayload{
				MinPTS:          minPTS,
				MaxPTS:          maxPTS,
				ProjectionKind:  domain.ChannelUpdateEventType(projection),
				AudienceKind:    store.ChannelDeliveryAudienceKind(audience),
				AudienceUserIDs: append([]int64(nil), audienceUserIDs...),
				AffectedUserIDs: append([]int64(nil), affectedUserIDs...),
			},
		})
	}
	if err := rows.Err(); err != nil {
		return store.OutboxClaimWindow{}, fmt.Errorf("iterate channel successor: %w", err)
	}
	return window, nil
}
