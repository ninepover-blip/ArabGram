package postgres

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	"telesrv/internal/store"
	"telesrv/internal/store/postgres/sqlcgen"
)

const (
	defaultOutboxLease           = 30 * time.Second
	defaultOutboxWindowSize      = 16
	defaultOutboxWindowByteLimit = 1 << 20
	maxOutboxLaneLimit           = 1000
	maxOutboxWindowSize          = 128
	maxOutboxWindowByteLimit     = 16 << 20
	outboxLeaseSafetyMargin      = 100 * time.Millisecond
	// PTS projection bytes do not exist until Egress builds the update. Reserve
	// a deliberately conservative budget per durable event when sizing a claim.
	ptsEventByteEstimate = 64 << 10
	// Bound lock hold time while amortizing BEGIN/COMMIT and WAL flush across
	// independent ordering domains. All stream ids in a group are processed in
	// ascending order, matching the global durable-lane lock order.
	outboxFinalizeTransactionLanes = 16
)

type outboxStateConfig struct {
	kind                             store.OutboxQueueKind
	itemsTable                       string
	lanesTable                       string
	attemptsTable                    string
	targetsTable                     string
	fenceSequence                    string
	laneLockScope                    string
	sequenceSQL                      string
	orderSQL                         string
	payloadSQL                       string
	recoverySQL                      string
	nextReadyAllSQL                  string
	nextReadyShardSQL                string
	claimAllSQL                      string
	claimShardSQL                    string
	bindTargetsSQL                   string
	recordEvidenceSQL                string
	finalizeLockLaneGroupSQL         string
	finalizeLoadCurrentWindowSQL     string
	finalizeApplyTerminalGroupSQL    string
	finalizeLoadSuccessorGroupSQL    string
	finalizeReleaseSuccessorGroupSQL string
	finalizeDeleteEmptyLaneGroupSQL  string
}

// The two fixed queue configurations share immutable query text. Only text is
// prepared at initialization; every poll still reads fresh PostgreSQL deadlines.
func withOutboxNextReadyQueries(cfg outboxStateConfig) outboxStateConfig {
	const query = `
WITH db_clock AS MATERIALIZED (
  SELECT clock_timestamp() AS observed
), ready_candidate AS MATERIALIZED (
  SELECT c.observed, l.ready_at, false AS recover_lease
  FROM %s l
  CROSS JOIN db_clock c
  WHERE %s
    AND l.state = 'ready'
    AND l.ready_at IS NOT NULL
  ORDER BY l.ready_at, l.stream_id
  LIMIT 1
), leased_candidate AS MATERIALIZED (
  SELECT
    c.observed,
    LEAST(l.lease_until, COALESCE(
      CASE
        WHEN deadline.resolution IS NULL THEN deadline.evidence_deadline
        ELSE deadline.retry_at
      END,
      l.lease_until
    )) AS ready_at,
    true AS recover_lease
  FROM %s l
  CROSS JOIN db_clock c
  LEFT JOIN %s deadline
    ON deadline.stream_id = l.stream_id
   AND deadline.item_id = l.head_item_id
   AND deadline.lease_fence = l.lease_fence
  WHERE %s
    AND l.state = 'leased'
    AND l.lease_until IS NOT NULL
    AND NOT (
      deadline.resolution IS NOT NULL
      AND (deadline.retry_at IS NULL OR deadline.retry_at <= c.observed)
    )
  ORDER BY ready_at, l.stream_id
  LIMIT 1
), candidates AS MATERIALIZED (
  SELECT * FROM ready_candidate
  UNION ALL
  SELECT * FROM leased_candidate
)
SELECT observed, ready_at, recover_lease
FROM candidates
WHERE ready_at IS NOT NULL
ORDER BY ready_at, recover_lease DESC
LIMIT 1`
	cfg.nextReadyAllSQL = fmt.Sprintf(query, cfg.lanesTable, "TRUE", cfg.lanesTable, cfg.attemptsTable, "TRUE")
	const predicate = "l.logical_shard = ANY($1::smallint[])"
	cfg.nextReadyShardSQL = fmt.Sprintf(query, cfg.lanesTable, predicate, cfg.lanesTable, cfg.attemptsTable, predicate)
	return cfg
}

var (
	dispatchOutboxStateConfig = withOutboxQueries(outboxStateConfig{
		kind:          store.OutboxQueueDispatchPTS,
		itemsTable:    "dispatch_outbox",
		lanesTable:    "dispatch_outbox_lanes",
		attemptsTable: "dispatch_outbox_attempts",
		targetsTable:  "dispatch_outbox_attempt_targets",
		fenceSequence: "dispatch_outbox_lease_fence_seq",
		laneLockScope: "account_pts",
		sequenceSQL:   "i.pts::bigint",
		orderSQL:      "i.pts ASC, i.id ASC",
		payloadSQL:    "concat(i.event_type::text, chr(31), i.read_model_peer_type::text, chr(31), i.read_model_peer_id::text)",
		recoverySQL:   "'difference'::text",
	})
	deliveryOutboxStateConfig = withOutboxQueries(outboxStateConfig{
		kind:          store.OutboxQueueAbsoluteDelivery,
		itemsTable:    "edge_delivery_outbox",
		lanesTable:    "edge_delivery_outbox_lanes",
		attemptsTable: "edge_delivery_outbox_attempts",
		targetsTable:  "edge_delivery_outbox_attempt_targets",
		fenceSequence: "edge_delivery_outbox_lease_fence_seq",
		laneLockScope: "account_absolute",
		sequenceSQL:   "0::bigint",
		orderSQL:      "i.id ASC",
		payloadSQL:    "i.payload",
		recoverySQL:   "i.recovery_policy::text",
	})
)

type durableOutboxState struct {
	db           sqlcgen.DBTX
	cfg          outboxStateConfig
	defaultLease time.Duration
}

func newDurableOutboxState(db sqlcgen.DBTX, cfg outboxStateConfig, lease time.Duration) durableOutboxState {
	if lease <= 0 {
		lease = defaultOutboxLease
	}
	return durableOutboxState{db: db, cfg: cfg, defaultLease: lease}
}

func (s *durableOutboxState) ClaimWindows(ctx context.Context, req store.OutboxClaimRequest) ([]store.OutboxClaimWindow, error) {
	req, shardIDs, scoped, err := s.normalizeClaimRequest(req)
	if err != nil {
		return nil, err
	}
	if scoped && len(shardIDs) == 0 {
		return []store.OutboxClaimWindow{}, nil
	}

	args := []any{
		req.Owner,
		durationMillis(req.LeaseDuration),
		int32(req.LaneLimit),
		int32(req.WindowSize),
		int64(req.WindowByteLimit),
		durationMillis(req.PhysicalDuration),
		durationMillis(req.ClockSkewAllowance),
	}
	query := s.cfg.claimAllSQL
	if scoped {
		query = s.cfg.claimShardSQL
		args = append(args, shardIDs)
	}

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("claim %s windows: %w", s.queueName(), err)
	}
	defer rows.Close()

	var windows []store.OutboxClaimWindow
	var current *store.OutboxClaimWindow
	for rows.Next() {
		var (
			streamID, fence, itemID, sequence, excludeSession int64
			excludeAuth                                       []byte
			owner, recovery                                   string
			leaseUntil, commandNotAfter, evidenceDeadline     time.Time
			payloadRaw                                        any
			ordinal, attempt                                  int
		)
		if err := rows.Scan(&streamID, &fence, &owner, &leaseUntil, &itemID, &sequence,
			&excludeAuth, &excludeSession, &payloadRaw, &recovery,
			&commandNotAfter, &evidenceDeadline, &ordinal, &attempt); err != nil {
			return nil, fmt.Errorf("scan %s claim window: %w", s.queueName(), err)
		}
		if fence <= 0 {
			return nil, fmt.Errorf("scan %s claim window: invalid fence %d", s.queueName(), fence)
		}
		if current == nil || current.StreamID != streamID {
			windows = append(windows, store.OutboxClaimWindow{
				QueueKind: s.cfg.kind, StreamID: streamID, Owner: owner,
				LeaseFence: uint64(fence), LeaseUntil: leaseUntil,
			})
			current = &windows[len(windows)-1]
		}
		item, err := s.claimedItem(streamID, itemID, sequence, uint64(fence), attempt,
			excludeAuth, excludeSession, payloadRaw, recovery)
		if err != nil {
			return nil, err
		}
		if ordinal != len(current.Items) {
			return nil, fmt.Errorf("scan %s claim window: ordinal %d, want %d", s.queueName(), ordinal, len(current.Items))
		}
		item.CommandNotAfter = commandNotAfter
		item.EvidenceDeadline = evidenceDeadline
		current.Items = append(current.Items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s claim windows: %w", s.queueName(), err)
	}
	return windows, nil
}

// RecoverBoundWindows is process-start exact replay only. The frozen physical
// evidence deadline is never recomputed or extended here.
func (s *durableOutboxState) RecoverBoundWindows(ctx context.Context, req store.OutboxRecoverBoundRequest) ([]store.OutboxClaimWindow, error) {
	if req.QueueKind != s.cfg.kind {
		return nil, fmt.Errorf("recover %s windows: queue kind %d", s.queueName(), req.QueueKind)
	}
	req.Owner = strings.TrimSpace(req.Owner)
	if req.Mode != store.OutboxBoundRecoveryExactOwnerLive {
		return nil, fmt.Errorf("recover %s windows: recovery mode is required", s.queueName())
	}
	if req.Owner == "" || strings.TrimSpace(req.Owner) != req.Owner || len(req.Owner) > store.MaxDeliveryLeaseOwnerBytes {
		return nil, fmt.Errorf("recover %s windows: exact-owner mode requires owner", s.queueName())
	}
	if req.LeaseDuration <= 0 {
		req.LeaseDuration = s.defaultLease
	}
	if durationMillis(req.LeaseDuration) <= 0 {
		return nil, fmt.Errorf("recover %s windows: lease duration is required", s.queueName())
	}
	if req.LaneLimit <= 0 {
		req.LaneLimit = 100
	}
	if req.LaneLimit > maxOutboxLaneLimit {
		req.LaneLimit = maxOutboxLaneLimit
	}
	shardIDs, scoped, err := normalizeOutboxScheduleShards(req.LogicalShardCount, req.LogicalShardIDs)
	if err != nil {
		return nil, fmt.Errorf("recover %s windows: %w", s.queueName(), err)
	}
	if scoped && len(shardIDs) == 0 {
		return []store.OutboxClaimWindow{}, nil
	}

	var windows []store.OutboxClaimWindow
	err = withOutboxTx(ctx, s.db, func(tx pgx.Tx) error {
		predicate := "TRUE"
		args := []any{req.Owner, int32(req.LaneLimit)}
		if scoped {
			predicate = "l.logical_shard = ANY($3::smallint[])"
			args = append(args, shardIDs)
		}
		recoveryPredicate := "l.lease_until > now() AND l.lease_owner = $1"
		attemptPredicate := fmt.Sprintf(`NOT EXISTS (
    SELECT 1 FROM %s invalid_attempt
    WHERE invalid_attempt.stream_id = l.stream_id
      AND invalid_attempt.lease_fence = l.lease_fence
      AND (NOT invalid_attempt.targets_bound OR invalid_attempt.resolution IS NOT NULL
           OR invalid_attempt.command_not_after <= now()
           OR invalid_attempt.evidence_deadline <= now())
  )`, s.cfg.attemptsTable)
		rows, err := tx.Query(ctx, fmt.Sprintf(`
SELECT l.stream_id, l.lease_fence
FROM %s l
WHERE %s
  AND l.state = 'leased'
  AND l.lease_until IS NOT NULL
  AND l.window_end_item_id IS NOT NULL
  AND (%s)
  AND EXISTS (
    SELECT 1 FROM %s head_attempt
    WHERE head_attempt.stream_id = l.stream_id
      AND head_attempt.item_id = l.head_item_id
      AND head_attempt.lease_fence = l.lease_fence
      AND head_attempt.targets_bound
      AND head_attempt.command_not_after > now()
      AND head_attempt.evidence_deadline > now()
      AND head_attempt.resolution IS NULL
  )
  AND (%s)
ORDER BY l.lease_until, l.stream_id
LIMIT $2
FOR UPDATE OF l SKIP LOCKED`, s.cfg.lanesTable, predicate, recoveryPredicate,
			s.cfg.attemptsTable, attemptPredicate), args...)
		if err != nil {
			return fmt.Errorf("pick recoverable %s windows: %w", s.queueName(), err)
		}
		type pickedLane struct {
			streamID int64
			oldFence int64
		}
		picked := make([]pickedLane, 0, req.LaneLimit)
		for rows.Next() {
			var lane pickedLane
			if err := rows.Scan(&lane.streamID, &lane.oldFence); err != nil {
				rows.Close()
				return fmt.Errorf("scan recoverable %s lane: %w", s.queueName(), err)
			}
			picked = append(picked, lane)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("iterate recoverable %s lanes: %w", s.queueName(), err)
		}
		rows.Close()
		if len(picked) == 0 {
			windows = []store.OutboxClaimWindow{}
			return nil
		}

		leaseMillis := durationMillis(req.LeaseDuration)
		streamIDs := make([]int64, 0, len(picked))
		for _, lane := range picked {
			streamIDs = append(streamIDs, lane.streamID)
			if _, err := tx.Exec(ctx, fmt.Sprintf(`
UPDATE %s
SET lease_until = GREATEST(lease_until, now() + ($3::bigint * interval '1 millisecond')),
    updated_at = now()
WHERE stream_id = $1 AND lease_fence = $2 AND lease_owner = $4 AND state = 'leased'`,
				s.cfg.lanesTable), lane.streamID, lane.oldFence, leaseMillis, req.Owner); err != nil {
				return fmt.Errorf("renew recoverable %s lane: %w", s.queueName(), err)
			}
			if _, err := tx.Exec(ctx, fmt.Sprintf(`
UPDATE %s
SET lease_until = GREATEST(lease_until, now() + ($3::bigint * interval '1 millisecond'))
WHERE stream_id = $1 AND lease_fence = $2`,
				s.cfg.attemptsTable), lane.streamID, lane.oldFence, leaseMillis); err != nil {
				return fmt.Errorf("renew recoverable %s attempts: %w", s.queueName(), err)
			}
		}
		if len(streamIDs) == 0 {
			windows = []store.OutboxClaimWindow{}
			return nil
		}
		windows, err = s.loadRecoveredWindows(ctx, tx, req.Owner, streamIDs)
		return err
	})
	if err != nil {
		return nil, err
	}
	return windows, nil
}

type recoveredTargetJSON struct {
	TargetInstanceID string `json:"target_instance_id"`
	TargetUserID     int64  `json:"target_user_id"`
	BatchID          string `json:"batch_id"`
	CommandID        string `json:"command_id"`
}

func (s *durableOutboxState) loadRecoveredWindows(ctx context.Context, tx pgx.Tx, owner string, streamIDs []int64) ([]store.OutboxClaimWindow, error) {
	rows, err := tx.Query(ctx, fmt.Sprintf(`
SELECT l.stream_id, l.lease_fence, l.lease_until,
       a.item_id, a.sequence, a.attempt, a.window_ordinal,
       a.delivery_source_instance_id, a.command_not_after, a.evidence_deadline,
       i.exclude_auth_key_id, i.exclude_session_id, %s AS payload_value,
       %s AS recovery_policy,
       COALESCE(jsonb_agg(jsonb_build_object(
         'target_instance_id', t.target_instance_id,
         'target_user_id', t.target_user_id,
         'batch_id', encode(t.batch_id, 'hex'),
         'command_id', encode(t.command_id, 'hex')
       ) ORDER BY t.target_instance_id, t.target_user_id, t.command_id)
       FILTER (WHERE t.item_id IS NOT NULL), '[]'::jsonb) AS targets
FROM %s l
JOIN %s a
  ON a.stream_id = l.stream_id AND a.lease_fence = l.lease_fence
 AND a.targets_bound AND a.resolution IS NULL
JOIN %s i ON i.id = a.item_id AND i.target_user_id = a.stream_id
LEFT JOIN %s t ON t.item_id = a.item_id AND t.lease_fence = a.lease_fence
WHERE l.stream_id = ANY($1::bigint[]) AND l.state = 'leased' AND l.lease_owner = $2
GROUP BY l.stream_id, l.lease_fence, l.lease_until,
         a.item_id, a.sequence, a.attempt, a.window_ordinal,
         a.delivery_source_instance_id, a.command_not_after, a.evidence_deadline,
         i.exclude_auth_key_id, i.exclude_session_id, payload_value, recovery_policy
ORDER BY l.stream_id, a.window_ordinal`, s.cfg.payloadSQL, s.cfg.recoverySQL,
		s.cfg.lanesTable, s.cfg.attemptsTable, s.cfg.itemsTable, s.cfg.targetsTable), streamIDs, owner)
	if err != nil {
		return nil, fmt.Errorf("load recovered %s windows: %w", s.queueName(), err)
	}
	defer rows.Close()
	var windows []store.OutboxClaimWindow
	var current *store.OutboxClaimWindow
	for rows.Next() {
		var (
			streamID, fence, itemID, sequence, excludeSession int64
			excludeAuth                                       []byte
			attempt, ordinal                                  int
			leaseUntil, commandNotAfter, evidenceDeadline     time.Time
			source, recovery                                  string
			payloadRaw                                        any
			targetsRaw                                        []byte
		)
		if err := rows.Scan(&streamID, &fence, &leaseUntil, &itemID, &sequence, &attempt, &ordinal,
			&source, &commandNotAfter, &evidenceDeadline,
			&excludeAuth, &excludeSession, &payloadRaw, &recovery, &targetsRaw); err != nil {
			return nil, fmt.Errorf("scan recovered %s window: %w", s.queueName(), err)
		}
		if current == nil || current.StreamID != streamID {
			windows = append(windows, store.OutboxClaimWindow{
				QueueKind: s.cfg.kind, StreamID: streamID, Owner: owner,
				LeaseFence: uint64(fence), LeaseUntil: leaseUntil,
			})
			current = &windows[len(windows)-1]
		}
		item, err := s.claimedItem(streamID, itemID, sequence, uint64(fence), attempt,
			excludeAuth, excludeSession, payloadRaw, recovery)
		if err != nil {
			return nil, err
		}
		if ordinal != len(current.Items) {
			return nil, fmt.Errorf("scan recovered %s ordinal %d, want %d", s.queueName(), ordinal, len(current.Items))
		}
		item.SourceInstanceID = source
		item.CommandNotAfter = commandNotAfter
		item.EvidenceDeadline = evidenceDeadline
		var targets []recoveredTargetJSON
		if err := json.Unmarshal(targetsRaw, &targets); err != nil {
			return nil, fmt.Errorf("decode recovered %s targets: %w", s.queueName(), err)
		}
		item.Targets = make([]store.OutboxAttemptTarget, 0, len(targets))
		for _, target := range targets {
			batchID, err := parseBytes16Hex(target.BatchID)
			if err != nil {
				return nil, fmt.Errorf("decode recovered %s batch id: %w", s.queueName(), err)
			}
			commandID, err := parseBytes16Hex(target.CommandID)
			if err != nil {
				return nil, fmt.Errorf("decode recovered %s command id: %w", s.queueName(), err)
			}
			item.Targets = append(item.Targets, store.OutboxAttemptTarget{
				TargetInstanceID: target.TargetInstanceID, TargetUserID: target.TargetUserID,
				BatchID: batchID, CommandID: commandID,
			})
		}
		current.Items = append(current.Items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recovered %s windows: %w", s.queueName(), err)
	}
	return windows, nil
}

func (s *durableOutboxState) RecoverFinalizableAttempts(ctx context.Context, req store.OutboxRecoverFinalizableRequest) ([]store.OutboxFinalizeRequest, error) {
	if req.QueueKind != s.cfg.kind {
		return nil, fmt.Errorf("recover finalizable %s attempts: queue kind %d", s.queueName(), req.QueueKind)
	}
	if req.AttemptLimit <= 0 || req.AttemptLimit > store.MaxDeliveryBatchItems {
		return nil, fmt.Errorf("recover finalizable %s attempts: attempt limit must be between 1 and %d", s.queueName(), store.MaxDeliveryBatchItems)
	}
	if req.LaneLimit <= 0 {
		req.LaneLimit = 100
	}
	if req.LaneLimit > maxOutboxLaneLimit {
		req.LaneLimit = maxOutboxLaneLimit
	}
	shardIDs, scoped, err := normalizeOutboxScheduleShards(req.LogicalShardCount, req.LogicalShardIDs)
	if err != nil {
		return nil, fmt.Errorf("recover finalizable %s attempts: %w", s.queueName(), err)
	}
	if scoped && len(shardIDs) == 0 {
		return []store.OutboxFinalizeRequest{}, nil
	}
	predicate := "TRUE"
	args := []any{int32(req.LaneLimit), int32(req.AttemptLimit)}
	if scoped {
		predicate = "l.logical_shard = ANY($3::smallint[])"
		args = append(args, shardIDs)
	}
	rows, err := s.db.Query(ctx, fmt.Sprintf(`
WITH picked AS MATERIALIZED (
  SELECT l.stream_id, l.lease_fence
  FROM %s l
  WHERE %s AND l.state = 'leased'
    AND EXISTS (
      SELECT 1 FROM %s ready
      WHERE ready.stream_id = l.stream_id
        AND ready.lease_fence = l.lease_fence
        AND ready.resolution IS NOT NULL
        AND (ready.retry_at IS NULL OR ready.retry_at <= now())
    )
  ORDER BY l.updated_at, l.stream_id
  LIMIT $1
)
SELECT a.stream_id, a.item_id, a.sequence, a.lease_fence, a.attempt
FROM picked p
JOIN %s a
  ON a.stream_id = p.stream_id AND a.lease_fence = p.lease_fence
WHERE a.resolution IS NOT NULL
  AND (a.retry_at IS NULL OR a.retry_at <= now())
ORDER BY a.stream_id, a.window_ordinal
LIMIT $2`, s.cfg.lanesTable, predicate,
		s.cfg.attemptsTable, s.cfg.attemptsTable), args...)
	if err != nil {
		return nil, fmt.Errorf("recover finalizable %s attempts: %w", s.queueName(), err)
	}
	defer rows.Close()
	requests := make([]store.OutboxFinalizeRequest, 0)
	for rows.Next() {
		var ref store.OutboxAttemptRef
		var fence int64
		ref.QueueKind = s.cfg.kind
		if err := rows.Scan(&ref.StreamID, &ref.ItemID, &ref.Sequence, &fence, &ref.Attempt); err != nil {
			return nil, fmt.Errorf("scan finalizable %s attempt: %w", s.queueName(), err)
		}
		if fence <= 0 {
			return nil, fmt.Errorf("scan finalizable %s attempt: invalid fence %d", s.queueName(), fence)
		}
		ref.LeaseFence = uint64(fence)
		requests = append(requests, store.OutboxFinalizeRequest{Ref: ref})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate finalizable %s attempts: %w", s.queueName(), err)
	}
	return requests, nil
}

func (s *durableOutboxState) ExpireEvidenceDeadlines(ctx context.Context, req store.OutboxEvidenceExpiryRequest) ([]store.OutboxFinalizeRequest, error) {
	if req.QueueKind != s.cfg.kind {
		return nil, fmt.Errorf("expire %s evidence deadlines: queue kind %d", s.queueName(), req.QueueKind)
	}
	if req.MaxAttempts <= 0 {
		return nil, fmt.Errorf("expire %s evidence deadlines: max attempts is required", s.queueName())
	}
	if req.LaneLimit <= 0 {
		req.LaneLimit = 100
	}
	if req.LaneLimit > maxOutboxLaneLimit {
		req.LaneLimit = maxOutboxLaneLimit
	}
	shards, scoped, err := normalizeOutboxScheduleShards(req.LogicalShardCount, req.LogicalShardIDs)
	if err != nil {
		return nil, fmt.Errorf("expire %s evidence deadlines: %w", s.queueName(), err)
	}
	if scoped && len(shards) == 0 {
		return []store.OutboxFinalizeRequest{}, nil
	}
	terminalSQL := "'terminal_resync'"
	if s.cfg.kind == store.OutboxQueueAbsoluteDelivery {
		terminalSQL = "'abandoned'"
	}
	predicate := "TRUE"
	args := []any{int32(req.MaxAttempts), int32(req.LaneLimit)}
	if scoped {
		predicate = "l.logical_shard = ANY($3::smallint[])"
		args = append(args, shards)
	}
	rows, err := s.db.Query(ctx, fmt.Sprintf(`
WITH candidates AS MATERIALIZED (
  SELECT a.item_id, a.lease_fence, a.stream_id, a.sequence, a.attempt
  FROM %s a
  JOIN %s l
    ON l.stream_id = a.stream_id AND l.lease_fence = a.lease_fence
   AND l.head_item_id = a.item_id AND l.state = 'leased'
  WHERE %s
    AND a.evidence_deadline <= now()
    AND a.resolution IS NULL
  ORDER BY a.evidence_deadline, a.stream_id, a.item_id
  LIMIT $2
  FOR UPDATE OF a SKIP LOCKED
), resolved AS (
  UPDATE %s a
  SET resolution = CASE WHEN c.attempt >= $1 THEN %s ELSE 'retry' END,
      retry_at = CASE WHEN c.attempt >= $1 THEN NULL ELSE now() END,
      last_error = CASE WHEN c.attempt >= $1
        THEN 'physical evidence deadline exhausted'
        ELSE 'physical evidence deadline expired'
      END
  FROM candidates c
  WHERE a.item_id = c.item_id AND a.lease_fence = c.lease_fence
    AND a.resolution IS NULL
  RETURNING a.stream_id, a.item_id, a.sequence, a.lease_fence, a.attempt
)
SELECT stream_id, item_id, sequence, lease_fence, attempt
FROM resolved
ORDER BY stream_id, sequence, item_id`, s.cfg.attemptsTable, s.cfg.lanesTable, predicate,
		s.cfg.attemptsTable, terminalSQL), args...)
	if err != nil {
		return nil, fmt.Errorf("expire %s evidence deadlines: %w", s.queueName(), err)
	}
	defer rows.Close()
	requests := make([]store.OutboxFinalizeRequest, 0, req.LaneLimit)
	for rows.Next() {
		var ref store.OutboxAttemptRef
		var fence int64
		ref.QueueKind = s.cfg.kind
		if err := rows.Scan(&ref.StreamID, &ref.ItemID, &ref.Sequence, &fence, &ref.Attempt); err != nil {
			return nil, fmt.Errorf("scan expired %s evidence deadline: %w", s.queueName(), err)
		}
		if fence <= 0 {
			return nil, fmt.Errorf("scan expired %s evidence deadline: invalid fence %d", s.queueName(), fence)
		}
		ref.LeaseFence = uint64(fence)
		requests = append(requests, store.OutboxFinalizeRequest{Ref: ref})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate expired %s evidence deadlines: %w", s.queueName(), err)
	}
	return requests, nil
}

func (s *durableOutboxState) normalizeClaimRequest(req store.OutboxClaimRequest) (store.OutboxClaimRequest, []int16, bool, error) {
	if req.QueueKind != s.cfg.kind {
		return req, nil, false, fmt.Errorf("claim %s windows: queue kind %d", s.queueName(), req.QueueKind)
	}
	if strings.TrimSpace(req.Owner) == "" || strings.TrimSpace(req.Owner) != req.Owner || len(req.Owner) > store.MaxDeliveryLeaseOwnerBytes {
		return req, nil, false, fmt.Errorf("claim %s windows: owner is required", s.queueName())
	}
	if req.LaneLimit <= 0 {
		req.LaneLimit = 100
	}
	if req.LaneLimit > maxOutboxLaneLimit {
		req.LaneLimit = maxOutboxLaneLimit
	}
	if req.WindowSize <= 0 {
		req.WindowSize = defaultOutboxWindowSize
	}
	if req.WindowSize > maxOutboxWindowSize {
		req.WindowSize = maxOutboxWindowSize
	}
	if req.WindowByteLimit <= 0 {
		req.WindowByteLimit = defaultOutboxWindowByteLimit
	}
	if req.WindowByteLimit > maxOutboxWindowByteLimit {
		req.WindowByteLimit = maxOutboxWindowByteLimit
	}
	if s.cfg.kind == store.OutboxQueueDispatchPTS {
		budgetCount := req.WindowByteLimit / ptsEventByteEstimate
		if budgetCount < 1 {
			budgetCount = 1
		}
		if req.WindowSize > budgetCount {
			req.WindowSize = budgetCount
		}
	}
	if req.LeaseDuration <= 0 {
		req.LeaseDuration = s.defaultLease
	}
	if durationMillis(req.LeaseDuration) <= 0 {
		return req, nil, false, fmt.Errorf("claim %s windows: lease duration is required", s.queueName())
	}
	if req.PhysicalDuration <= 0 {
		return req, nil, false, fmt.Errorf("claim %s windows: physical duration is required", s.queueName())
	}
	if req.ClockSkewAllowance <= 0 {
		return req, nil, false, fmt.Errorf("claim %s windows: clock skew allowance is required", s.queueName())
	}
	if req.LeaseDuration <= outboxLeaseSafetyMargin ||
		req.PhysicalDuration >= req.LeaseDuration-outboxLeaseSafetyMargin ||
		req.ClockSkewAllowance >= req.LeaseDuration-outboxLeaseSafetyMargin-req.PhysicalDuration {
		return req, nil, false, fmt.Errorf(
			"claim %s windows: physical duration plus clock skew allowance and lease safety must be shorter than lease duration",
			s.queueName(),
		)
	}
	ids, scoped, err := normalizeOutboxScheduleShards(req.LogicalShardCount, req.LogicalShardIDs)
	if err != nil {
		return req, nil, false, fmt.Errorf("claim %s windows: %w", s.queueName(), err)
	}
	return req, ids, scoped, nil
}

func (s *durableOutboxState) claimedItem(streamID, itemID, sequence int64, fence uint64, attempt int,
	excludeAuth []byte, excludeSession int64, payloadRaw any, recovery string) (store.OutboxClaimedItem, error) {
	rawAuthKeyID, err := outboxAuthKeyID(excludeAuth)
	if err != nil {
		return store.OutboxClaimedItem{}, fmt.Errorf("scan %s exclusion auth key: %w", s.queueName(), err)
	}
	item := store.OutboxClaimedItem{
		Ref: store.OutboxAttemptRef{
			QueueKind: s.cfg.kind, StreamID: streamID, ItemID: itemID,
			Sequence: sequence, LeaseFence: fence, Attempt: attempt,
		},
		ExcludeAuthKeyID: rawAuthKeyID,
		ExcludeSessionID: excludeSession,
	}
	switch s.cfg.kind {
	case store.OutboxQueueDispatchPTS:
		encoded, ok := payloadRaw.(string)
		if !ok {
			return item, fmt.Errorf("scan dispatch claim payload %T", payloadRaw)
		}
		parts := strings.SplitN(encoded, string(rune(31)), 3)
		if len(parts) != 3 || strings.TrimSpace(parts[0]) == "" {
			return item, fmt.Errorf("scan dispatch claim payload %q", encoded)
		}
		peerID, err := strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			return item, fmt.Errorf("scan dispatch read-model peer id %q: %w", parts[2], err)
		}
		peer := domain.Peer{Type: domain.PeerType(parts[1]), ID: peerID}
		if (peer.Type == "") != (peer.ID == 0) ||
			(peer.Type != "" && peer.Type != domain.PeerTypeUser && peer.Type != domain.PeerTypeChannel) {
			return item, fmt.Errorf("scan dispatch read-model peer %+v", peer)
		}
		item.RecoveryPolicy = store.OutboxRecoveryDifference
		item.Payload = store.DispatchOutboxPayload{EventType: domain.UpdateEventType(parts[0]), ReadModelPeer: peer}
		if recovery != "difference" {
			return item, fmt.Errorf("scan dispatch recovery policy %q", recovery)
		}
	case store.OutboxQueueAbsoluteDelivery:
		payload, ok := payloadRaw.([]byte)
		if !ok || len(payload) == 0 {
			return item, fmt.Errorf("scan absolute delivery payload %T", payloadRaw)
		}
		if recovery != "absolute_reload" {
			return item, fmt.Errorf("scan absolute delivery recovery policy %q", recovery)
		}
		item.RecoveryPolicy = store.OutboxRecoveryAbsoluteReload
		item.Payload = store.AbsoluteDeliveryPayload{TL: append([]byte(nil), payload...)}
	default:
		return item, fmt.Errorf("unsupported outbox queue kind %d", s.cfg.kind)
	}
	return item, nil
}

func outboxAuthKeyID(raw []byte) ([8]byte, error) {
	var id [8]byte
	if len(raw) != len(id) {
		return id, fmt.Errorf("auth key id is %d bytes, want %d", len(raw), len(id))
	}
	copy(id[:], raw)
	return id, nil
}

func (s *durableOutboxState) NextReadyAt(ctx context.Context, kind store.OutboxQueueKind, shardCount int, shardIDs []int) (store.OutboxNextReady, bool, error) {
	if kind != s.cfg.kind {
		return store.OutboxNextReady{}, false, fmt.Errorf("next %s ready at: queue kind %d", s.queueName(), kind)
	}
	ids, scoped, err := normalizeOutboxScheduleShards(shardCount, shardIDs)
	if err != nil {
		return store.OutboxNextReady{}, false, fmt.Errorf("next %s ready at: %w", s.queueName(), err)
	}
	if scoped && len(ids) == 0 {
		return store.OutboxNextReady{}, false, nil
	}
	query := s.cfg.nextReadyAllSQL
	args := []any{}
	if scoped {
		query = s.cfg.nextReadyShardSQL
		args = append(args, ids)
	}
	var observed, ready time.Time
	var recoverLease bool
	err = s.db.QueryRow(ctx, query, args...).Scan(&observed, &ready, &recoverLease)
	if errors.Is(err, pgx.ErrNoRows) {
		return store.OutboxNextReady{}, false, nil
	}
	if err != nil {
		return store.OutboxNextReady{}, false, fmt.Errorf("next %s ready at: %w", s.queueName(), err)
	}
	readyKind := store.OutboxReadyClaim
	if recoverLease {
		readyKind = store.OutboxReadyRecoverLease
	}
	return store.OutboxNextReady{ObservedAt: observed, ReadyAt: ready, Kind: readyKind}, true, nil
}

type attemptTargetJSON struct {
	TargetInstanceID string `json:"target_instance_id"`
	TargetUserID     int64  `json:"target_user_id"`
	BatchID          string `json:"batch_id"`
	CommandID        string `json:"command_id"`
}

type targetSetJSON struct {
	attemptRefJSON
	SourceInstanceID string              `json:"source_instance_id"`
	Targets          []attemptTargetJSON `json:"targets"`
}

func (s *durableOutboxState) BindAttemptTargets(ctx context.Context, sets []store.OutboxAttemptTargetSet) ([]store.OutboxBindTargetResult, error) {
	if len(sets) == 0 {
		return []store.OutboxBindTargetResult{}, nil
	}
	if len(sets) > store.MaxDeliveryBatchItems {
		return nil, fmt.Errorf("bind %s targets: batch exceeds %d items", s.queueName(), store.MaxDeliveryBatchItems)
	}
	input := make([]targetSetJSON, len(sets))
	for i, set := range sets {
		if !validAttemptRef(set.Ref, s.cfg.kind) {
			return nil, fmt.Errorf("bind %s targets: invalid ref at index %d", s.queueName(), i)
		}
		input[i].attemptRefJSON = refJSON(i, set.Ref)
		sourceInstanceID := strings.TrimSpace(set.SourceInstanceID)
		if sourceInstanceID == "" || sourceInstanceID != set.SourceInstanceID || len(sourceInstanceID) > store.MaxDeliveryInstanceIDBytes {
			return nil, fmt.Errorf("bind %s targets: source instance is required at index %d", s.queueName(), i)
		}
		input[i].SourceInstanceID = sourceInstanceID
		if len(set.Targets) > store.MaxDeliveryTargets {
			return nil, fmt.Errorf("bind %s targets: target set exceeds %d at index %d", s.queueName(), store.MaxDeliveryTargets, i)
		}
		input[i].Targets = make([]attemptTargetJSON, 0, len(set.Targets))
		type targetIdentity struct {
			instance string
			userID   int64
		}
		seen := make(map[targetIdentity]struct{}, len(set.Targets))
		for targetIndex, target := range set.Targets {
			instanceID := strings.TrimSpace(target.TargetInstanceID)
			if instanceID == "" || instanceID != target.TargetInstanceID || len(instanceID) > store.MaxDeliveryInstanceIDBytes ||
				!validOutboxTargetUserID(s.cfg.kind, set.Ref.StreamID, target.TargetUserID) ||
				!validBytes16(target.BatchID) || !validBytes16(target.CommandID) {
				return nil, fmt.Errorf("bind %s targets: invalid target %d at index %d", s.queueName(), targetIndex, i)
			}
			// A frozen attempt has at most one command per exact Edge/user
			// target. Batch/command IDs identify that command; they must not be
			// used to smuggle multiple commands to the same target.
			key := targetIdentity{instance: instanceID, userID: target.TargetUserID}
			if _, ok := seen[key]; ok {
				return nil, fmt.Errorf("bind %s targets: duplicate target %d at index %d", s.queueName(), targetIndex, i)
			}
			seen[key] = struct{}{}
			input[i].Targets = append(input[i].Targets, attemptTargetJSON{
				TargetInstanceID: instanceID,
				TargetUserID:     target.TargetUserID,
				BatchID:          bytes16Hex(target.BatchID),
				CommandID:        bytes16Hex(target.CommandID),
			})
		}
	}
	payload, err := marshalJSONInput(input)
	if err != nil {
		return nil, err
	}

	rows, err := s.db.Query(ctx, s.cfg.bindTargetsSQL, payload)
	if err != nil {
		return nil, fmt.Errorf("bind %s targets: %w", s.queueName(), err)
	}
	defer rows.Close()
	results := make([]store.OutboxBindTargetResult, len(sets))
	for next := 0; rows.Next(); next++ {
		var ordinal int
		var outcome string
		var ignored int
		if err := rows.Scan(&ordinal, &outcome, &ignored); err != nil {
			return nil, fmt.Errorf("scan bind %s targets: %w", s.queueName(), err)
		}
		if next >= len(results) || ordinal != next {
			return nil, fmt.Errorf("bind %s targets: result ordinal %d, want %d", s.queueName(), ordinal, next)
		}
		results[next] = store.OutboxBindTargetResult{Ref: sets[next].Ref, Outcome: bindTargetOutcome(outcome)}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate bind %s targets: %w", s.queueName(), err)
	}
	for i := range results {
		if results[i].Outcome == 0 {
			return nil, fmt.Errorf("bind %s targets: result count mismatch", s.queueName())
		}
	}
	return results, nil
}

func bindTargetOutcome(value string) store.OutboxBindTargetOutcome {
	switch value {
	case "bound":
		return store.OutboxBindTargetBound
	case "duplicate":
		return store.OutboxBindTargetDuplicate
	case "fenced":
		return store.OutboxBindTargetFenced
	case "rejected":
		return store.OutboxBindTargetRejected
	default:
		return 0
	}
}

type evidenceJSON struct {
	attemptRefJSON
	Kind             string    `json:"kind"`
	SourceInstanceID string    `json:"source_instance_id"`
	TargetInstanceID string    `json:"target_instance_id"`
	TargetUserID     int64     `json:"target_user_id"`
	BatchID          string    `json:"batch_id"`
	CommandID        string    `json:"command_id"`
	EligibleSessions int       `json:"eligible_sessions"`
	WrittenSessions  int       `json:"written_sessions"`
	ServerMsgID      int64     `json:"server_msg_id"`
	ObservedAt       time.Time `json:"observed_at"`
}

func (s *durableOutboxState) RecordAttemptEvidenceBatch(ctx context.Context, evidence []store.OutboxAttemptEvidence) ([]store.OutboxEvidenceResult, error) {
	if len(evidence) == 0 {
		return []store.OutboxEvidenceResult{}, nil
	}
	if len(evidence) > store.MaxDeliveryBatchItems {
		return nil, fmt.Errorf("record %s evidence: batch exceeds %d items", s.queueName(), store.MaxDeliveryBatchItems)
	}
	input := make([]evidenceJSON, len(evidence))
	seen := make(map[string]store.OutboxAttemptEvidence, len(evidence))
	for i, item := range evidence {
		if !validAttemptRef(item.Ref, s.cfg.kind) || item.ObservedAt.IsZero() {
			return nil, fmt.Errorf("record %s evidence: invalid evidence at index %d", s.queueName(), i)
		}
		kind, targetRequired := evidenceKindString(item.Kind)
		if kind == "" {
			return nil, fmt.Errorf("record %s evidence: invalid kind at index %d", s.queueName(), i)
		}
		sourceInstanceID := strings.TrimSpace(item.SourceInstanceID)
		if sourceInstanceID == "" || sourceInstanceID != item.SourceInstanceID || len(sourceInstanceID) > store.MaxDeliveryInstanceIDBytes {
			return nil, fmt.Errorf("record %s evidence: source instance is required at index %d", s.queueName(), i)
		}
		instanceID := strings.TrimSpace(item.TargetInstanceID)
		if targetRequired {
			if instanceID == "" || instanceID != item.TargetInstanceID || len(instanceID) > store.MaxDeliveryInstanceIDBytes ||
				!validOutboxTargetUserID(s.cfg.kind, item.Ref.StreamID, item.TargetUserID) ||
				!validBytes16(item.BatchID) || !validBytes16(item.CommandID) {
				return nil, fmt.Errorf("record %s evidence: target identity is required at index %d", s.queueName(), i)
			}
		} else if instanceID != "" || item.TargetUserID != 0 || validBytes16(item.BatchID) || validBytes16(item.CommandID) {
			return nil, fmt.Errorf("record %s evidence: authoritative empty evidence has a target at index %d", s.queueName(), i)
		}
		switch item.Kind {
		case store.OutboxEvidenceEdgeWritten:
			if item.EligibleSessions <= 0 || item.WrittenSessions != item.EligibleSessions || item.ServerMsgID <= 0 {
				return nil, fmt.Errorf("record %s evidence: written sessions must equal eligible sessions at index %d", s.queueName(), i)
			}
		case store.OutboxEvidenceEdgeNoEligible, store.OutboxEvidenceAuthoritativeNoTargets:
			if item.EligibleSessions != 0 || item.WrittenSessions != 0 || item.ServerMsgID != 0 {
				return nil, fmt.Errorf("record %s evidence: unexpected physical evidence fields at index %d", s.queueName(), i)
			}
		}
		key := fmt.Sprintf("%d/%d/%s/%d/%s/%s", item.Ref.ItemID, item.Ref.LeaseFence,
			instanceID, item.TargetUserID, bytes16Hex(item.BatchID), bytes16Hex(item.CommandID))
		if previous, ok := seen[key]; ok && !sameOutboxCompletionEvidence(previous, item) {
			return nil, fmt.Errorf("record %s evidence: conflicting duplicate at index %d", s.queueName(), i)
		}
		seen[key] = item
		input[i] = evidenceJSON{
			attemptRefJSON: refJSON(i, item.Ref), Kind: kind,
			SourceInstanceID: sourceInstanceID,
			TargetInstanceID: instanceID, TargetUserID: item.TargetUserID, BatchID: bytes16Hex(item.BatchID),
			CommandID:        bytes16Hex(item.CommandID),
			EligibleSessions: item.EligibleSessions, WrittenSessions: item.WrittenSessions,
			ServerMsgID: item.ServerMsgID, ObservedAt: item.ObservedAt,
		}
	}
	payload, err := marshalJSONInput(input)
	if err != nil {
		return nil, err
	}

	rows, err := s.db.Query(ctx, s.cfg.recordEvidenceSQL, payload)
	if err != nil {
		return nil, fmt.Errorf("record %s evidence: %w", s.queueName(), err)
	}
	defer rows.Close()
	results := make([]store.OutboxEvidenceResult, len(evidence))
	for next := 0; rows.Next(); next++ {
		var ordinal, ignored int
		var outcome string
		if err := rows.Scan(&ordinal, &outcome, &ignored); err != nil {
			return nil, fmt.Errorf("scan %s evidence: %w", s.queueName(), err)
		}
		if next >= len(results) || ordinal != next {
			return nil, fmt.Errorf("record %s evidence: result ordinal %d, want %d", s.queueName(), ordinal, next)
		}
		results[next] = store.OutboxEvidenceResult{Ref: evidence[next].Ref, Outcome: evidenceOutcome(outcome)}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s evidence: %w", s.queueName(), err)
	}
	for i := range results {
		if results[i].Outcome == 0 {
			return nil, fmt.Errorf("record %s evidence: result count mismatch", s.queueName())
		}
	}
	return results, nil
}

func evidenceKindString(kind store.OutboxEvidenceKind) (string, bool) {
	switch kind {
	case store.OutboxEvidenceEdgeWritten:
		return "edge_written", true
	case store.OutboxEvidenceEdgeNoEligible:
		return "edge_no_eligible", true
	case store.OutboxEvidenceAuthoritativeNoTargets:
		return "authoritative_no_targets", false
	default:
		return "", false
	}
}

func sameOutboxCompletionEvidence(a, b store.OutboxAttemptEvidence) bool {
	if a.Ref != b.Ref || a.Kind != b.Kind || a.SourceInstanceID != b.SourceInstanceID ||
		a.TargetInstanceID != b.TargetInstanceID || a.TargetUserID != b.TargetUserID ||
		a.BatchID != b.BatchID || a.CommandID != b.CommandID {
		return false
	}
	return a.EligibleSessions == b.EligibleSessions && a.WrittenSessions == b.WrittenSessions &&
		a.ServerMsgID == b.ServerMsgID
}

func evidenceOutcome(value string) store.OutboxEvidenceOutcome {
	switch value {
	case "recorded":
		return store.OutboxEvidenceRecorded
	case "duplicate":
		return store.OutboxEvidenceDuplicate
	case "fenced":
		return store.OutboxEvidenceFenced
	case "rejected":
		return store.OutboxEvidenceRejected
	default:
		return 0
	}
}

type authorizedResolutionJSON struct {
	attemptRefJSON
	Authority      string `json:"authority"`
	Owner          string `json:"owner"`
	SourceInstance string `json:"source_instance_id"`
	TargetInstance string `json:"target_instance_id"`
	TargetUserID   int64  `json:"target_user_id"`
	BatchID        string `json:"batch_id"`
	CommandID      string `json:"command_id"`
	Kind           string `json:"kind"`
	RetryDelayMS   int64  `json:"retry_delay_ms"`
	LastError      string `json:"last_error"`
}

func (s *durableOutboxState) ResolveTargetAttemptBatch(ctx context.Context, resolutions []store.OutboxTargetAttemptResolution) ([]store.OutboxResolutionResult, error) {
	if len(resolutions) == 0 {
		return []store.OutboxResolutionResult{}, nil
	}
	if len(resolutions) > store.MaxDeliveryBatchItems {
		return nil, fmt.Errorf("resolve target %s attempt: batch exceeds %d items", s.queueName(), store.MaxDeliveryBatchItems)
	}
	input := make([]authorizedResolutionJSON, len(resolutions))
	refs := make([]store.OutboxAttemptRef, len(resolutions))
	type decision struct {
		kind       store.OutboxResolutionKind
		retryDelay time.Duration
		lastError  string
	}
	decisions := make(map[store.OutboxAttemptRef]decision, len(resolutions))
	for i, resolution := range resolutions {
		if !validAttemptRef(resolution.Ref, s.cfg.kind) {
			return nil, fmt.Errorf("resolve target %s attempt: invalid ref at index %d", s.queueName(), i)
		}
		source := strings.TrimSpace(resolution.SourceInstanceID)
		instance := strings.TrimSpace(resolution.TargetInstanceID)
		if source == "" || source != resolution.SourceInstanceID || len(source) > store.MaxDeliveryInstanceIDBytes ||
			instance == "" || instance != resolution.TargetInstanceID || len(instance) > store.MaxDeliveryInstanceIDBytes ||
			!validOutboxTargetUserID(s.cfg.kind, resolution.Ref.StreamID, resolution.TargetUserID) ||
			!validBytes16(resolution.BatchID) || !validBytes16(resolution.CommandID) {
			return nil, fmt.Errorf("resolve target %s attempt: invalid exact target identity at index %d", s.queueName(), i)
		}
		kind := s.resolutionKindString(resolution.Kind)
		if kind == "" || (resolution.Kind == store.OutboxResolutionRetry) != (resolution.RetryDelay > 0) {
			return nil, fmt.Errorf("resolve target %s attempt: invalid resolution at index %d", s.queueName(), i)
		}
		lastError := truncateOutboxError(resolution.LastError)
		if previous, ok := decisions[resolution.Ref]; ok {
			if previous.kind != resolution.Kind || previous.retryDelay != resolution.RetryDelay {
				return nil, fmt.Errorf("resolve target %s attempt: conflicting duplicate at index %d", s.queueName(), i)
			}
			// The decision is attempt-scoped while target failures can carry
			// different diagnostics. Persist the first observation deterministically
			// so UPDATE ... FROM never chooses a batch-dependent error string.
			lastError = previous.lastError
		} else {
			decisions[resolution.Ref] = decision{kind: resolution.Kind, retryDelay: resolution.RetryDelay, lastError: lastError}
		}
		refs[i] = resolution.Ref
		input[i] = authorizedResolutionJSON{
			attemptRefJSON: refJSON(i, resolution.Ref), Authority: "target",
			SourceInstance: source, TargetInstance: instance, TargetUserID: resolution.TargetUserID,
			BatchID: bytes16Hex(resolution.BatchID), CommandID: bytes16Hex(resolution.CommandID), Kind: kind,
			RetryDelayMS: durationMillis(resolution.RetryDelay), LastError: lastError,
		}
	}
	return s.resolveAuthorizedAttemptBatch(ctx, input, refs)
}

func (s *durableOutboxState) ResolveOwnedAttemptBatch(ctx context.Context, resolutions []store.OutboxOwnedAttemptResolution) ([]store.OutboxResolutionResult, error) {
	if len(resolutions) == 0 {
		return []store.OutboxResolutionResult{}, nil
	}
	if len(resolutions) > store.MaxDeliveryBatchItems {
		return nil, fmt.Errorf("resolve owned %s attempt: batch exceeds %d items", s.queueName(), store.MaxDeliveryBatchItems)
	}
	input := make([]authorizedResolutionJSON, len(resolutions))
	refs := make([]store.OutboxAttemptRef, len(resolutions))
	type decision struct {
		kind       store.OutboxResolutionKind
		retryDelay time.Duration
		lastError  string
	}
	decisions := make(map[store.OutboxAttemptRef]decision, len(resolutions))
	for i, resolution := range resolutions {
		if !validAttemptRef(resolution.Ref, s.cfg.kind) {
			return nil, fmt.Errorf("resolve owned %s attempt: invalid ref at index %d", s.queueName(), i)
		}
		owner := strings.TrimSpace(resolution.Owner)
		if owner == "" || owner != resolution.Owner || len(owner) > store.MaxDeliveryLeaseOwnerBytes {
			return nil, fmt.Errorf("resolve owned %s attempt: owner is required at index %d", s.queueName(), i)
		}
		kind := s.resolutionKindString(resolution.Kind)
		if kind == "" || (resolution.Kind == store.OutboxResolutionRetry) != (resolution.RetryDelay > 0) {
			return nil, fmt.Errorf("resolve owned %s attempt: invalid resolution at index %d", s.queueName(), i)
		}
		lastError := truncateOutboxError(resolution.LastError)
		if previous, ok := decisions[resolution.Ref]; ok {
			if previous.kind != resolution.Kind || previous.retryDelay != resolution.RetryDelay {
				return nil, fmt.Errorf("resolve owned %s attempt: conflicting duplicate at index %d", s.queueName(), i)
			}
			lastError = previous.lastError
		} else {
			decisions[resolution.Ref] = decision{kind: resolution.Kind, retryDelay: resolution.RetryDelay, lastError: lastError}
		}
		refs[i] = resolution.Ref
		input[i] = authorizedResolutionJSON{
			attemptRefJSON: refJSON(i, resolution.Ref), Authority: "owned", Owner: owner, Kind: kind,
			RetryDelayMS: durationMillis(resolution.RetryDelay), LastError: lastError,
		}
	}
	return s.resolveAuthorizedAttemptBatch(ctx, input, refs)
}

func (s *durableOutboxState) resolveAuthorizedAttemptBatch(ctx context.Context, input []authorizedResolutionJSON, refs []store.OutboxAttemptRef) ([]store.OutboxResolutionResult, error) {
	payload, err := marshalJSONInput(input)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.Query(ctx, fmt.Sprintf(`
WITH input AS MATERIALIZED (
  SELECT *
  FROM jsonb_to_recordset($1::jsonb) AS i(
    ordinal integer, stream_id bigint, item_id bigint, sequence bigint,
    lease_fence bigint, attempt integer, authority text, owner text,
    source_instance_id text, target_instance_id text, target_user_id bigint,
    batch_id text, command_id text, kind text,
    retry_delay_ms bigint, last_error text
  )
), locked_attempts AS MATERIALIZED (
  SELECT a.*
  FROM %s a
  JOIN (
    SELECT DISTINCT stream_id, item_id, sequence, lease_fence, attempt FROM input
  ) wanted
    ON wanted.stream_id = a.stream_id AND wanted.item_id = a.item_id
   AND wanted.sequence = a.sequence AND wanted.lease_fence = a.lease_fence
   AND wanted.attempt = a.attempt
  ORDER BY a.stream_id, a.item_id, a.lease_fence, a.attempt
  FOR UPDATE OF a
), exact AS MATERIALIZED (
  SELECT i.*, (a.item_id IS NOT NULL) AS attempt_exists,
         a.resolution, a.targets_bound,
         a.evidence_deadline,
		 a.delivery_source_instance_id, a.lease_owner AS attempt_owner,
         a.lease_until AS attempt_lease_until,
         l.lease_fence AS current_fence, l.state AS lane_state,
         l.lease_owner AS lane_owner, l.lease_until AS lane_lease_until,
         (t.item_id IS NOT NULL) AS target_exists,
         t.evidence_kind AS target_evidence_kind
  FROM input i
  LEFT JOIN locked_attempts a
    ON a.stream_id = i.stream_id AND a.item_id = i.item_id
   AND a.sequence = i.sequence AND a.lease_fence = i.lease_fence
   AND a.attempt = i.attempt
  LEFT JOIN %s l ON l.stream_id = i.stream_id
  LEFT JOIN %s t
    ON i.authority = 'target'
   AND t.item_id = i.item_id AND t.lease_fence = i.lease_fence
   AND t.target_instance_id = i.target_instance_id
   AND t.target_user_id = i.target_user_id
   AND t.batch_id = decode(i.batch_id, 'hex')
   AND t.command_id = decode(i.command_id, 'hex')
), resolved AS MATERIALIZED (
  UPDATE %s a
  SET resolution = e.kind,
      retry_at = CASE
		WHEN e.targets_bound THEN GREATEST(
		  now() + (e.retry_delay_ms * interval '1 millisecond'),
		  e.evidence_deadline
		)
        WHEN e.kind = 'retry' THEN now() + (e.retry_delay_ms * interval '1 millisecond')
        ELSE NULL
      END,
      last_error = left(e.last_error, 2000)
  FROM exact e
  WHERE a.item_id = e.item_id AND a.lease_fence = e.lease_fence
    AND a.resolution IS NULL
    AND e.current_fence = e.lease_fence AND e.lane_state = 'leased'
    AND (
      (e.authority = 'target'
       AND e.targets_bound
       AND e.delivery_source_instance_id = e.source_instance_id
       AND e.target_exists
       AND e.target_evidence_kind IS NULL)
      OR
      (e.authority = 'owned'
       AND e.owner <> ''
       AND e.attempt_owner = e.owner AND e.lane_owner = e.owner
       AND e.attempt_lease_until > now() AND e.lane_lease_until > now())
    )
  RETURNING a.item_id, a.lease_fence
)
SELECT e.ordinal,
  CASE
    WHEN NOT e.attempt_exists THEN 'fenced'
    WHEN e.current_fence IS DISTINCT FROM e.lease_fence OR e.lane_state <> 'leased' THEN 'fenced'
    WHEN e.authority = 'target' AND e.delivery_source_instance_id IS DISTINCT FROM e.source_instance_id THEN 'fenced'
    WHEN e.authority = 'target' AND (NOT e.targets_bound OR NOT e.target_exists OR e.target_evidence_kind IS NOT NULL) THEN 'rejected'
    WHEN e.authority = 'owned' AND (
      e.owner = '' OR e.attempt_owner IS DISTINCT FROM e.owner OR e.lane_owner IS DISTINCT FROM e.owner
      OR e.attempt_lease_until <= now() OR e.lane_lease_until <= now()
    ) THEN 'fenced'
    WHEN r.item_id IS NOT NULL THEN 'recorded'
    WHEN e.resolution = e.kind THEN 'duplicate'
    WHEN e.resolution IS NULL THEN 'fenced'
    ELSE 'rejected'
  END AS outcome
FROM exact e
LEFT JOIN resolved r USING (item_id, lease_fence)
ORDER BY e.ordinal`, s.cfg.attemptsTable, s.cfg.lanesTable, s.cfg.targetsTable, s.cfg.attemptsTable), payload)
	if err != nil {
		return nil, fmt.Errorf("resolve %s attempt: %w", s.queueName(), err)
	}
	defer rows.Close()
	results := make([]store.OutboxResolutionResult, len(refs))
	for next := 0; rows.Next(); next++ {
		var ordinal int
		var outcome string
		if err := rows.Scan(&ordinal, &outcome); err != nil {
			return nil, fmt.Errorf("scan resolve %s attempt: %w", s.queueName(), err)
		}
		if next >= len(results) || ordinal != next {
			return nil, fmt.Errorf("resolve %s attempt: result ordinal %d, want %d", s.queueName(), ordinal, next)
		}
		results[next] = store.OutboxResolutionResult{Ref: refs[next], Outcome: resolutionOutcome(outcome)}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate resolve %s attempt: %w", s.queueName(), err)
	}
	for i := range results {
		if results[i].Outcome == 0 {
			return nil, fmt.Errorf("resolve %s attempt: result count mismatch", s.queueName())
		}
	}
	return results, nil
}

func (s *durableOutboxState) resolutionKindString(kind store.OutboxResolutionKind) string {
	switch kind {
	case store.OutboxResolutionRetry:
		return "retry"
	case store.OutboxResolutionAbandoned:
		if s.cfg.kind == store.OutboxQueueAbsoluteDelivery {
			return "abandoned"
		}
	case store.OutboxResolutionTerminalResync:
		if s.cfg.kind == store.OutboxQueueDispatchPTS {
			return "terminal_resync"
		}
	default:
	}
	return ""
}

func resolutionOutcome(value string) store.OutboxResolutionOutcome {
	switch value {
	case "recorded":
		return store.OutboxResolutionRecorded
	case "duplicate":
		return store.OutboxResolutionDuplicate
	case "fenced":
		return store.OutboxResolutionFenced
	case "rejected":
		return store.OutboxResolutionRejected
	default:
		return 0
	}
}

func truncateOutboxError(value string) string {
	if len(value) <= 2000 {
		return value
	}
	return value[:2000]
}

type finalizedAttemptKey struct {
	itemID int64
	fence  uint64
}

type outboxLaneRow struct {
	streamID          int64
	headItemID        int64
	headSequence      int64
	state             string
	leaseFence        uint64
	leaseOwner        string
	leaseUntil        *time.Time
	windowEndItemID   *int64
	windowEndSequence *int64
}

type outboxWindowAttemptRow struct {
	itemID           int64
	sequence         int64
	attempt          *int
	issuedAt         *time.Time
	commandNotAfter  *time.Time
	evidenceDeadline *time.Time
	resolution       *string
	retryAt          *time.Time
	resolutionReady  *bool
}

func (s *durableOutboxState) FinalizeAttempts(ctx context.Context, requests []store.OutboxFinalizeRequest) (store.OutboxFinalizeBatch, error) {
	if len(requests) == 0 {
		return store.OutboxFinalizeBatch{Results: []store.OutboxFinalizeResult{}}, nil
	}
	if len(requests) > store.MaxDeliveryBatchItems {
		return store.OutboxFinalizeBatch{}, fmt.Errorf("finalize %s attempts: batch exceeds %d items", s.queueName(), store.MaxDeliveryBatchItems)
	}
	streamSet := make(map[int64]struct{}, len(requests))
	streamIDs := make([]int64, 0, len(requests))
	byStream := make(map[int64][]store.OutboxFinalizeRequest, len(requests))
	for i, request := range requests {
		if !validAttemptRef(request.Ref, s.cfg.kind) {
			return store.OutboxFinalizeBatch{}, fmt.Errorf("finalize %s attempt: invalid ref at index %d", s.queueName(), i)
		}
		if request.RetainLease {
			if strings.TrimSpace(request.Owner) == "" {
				return store.OutboxFinalizeBatch{}, fmt.Errorf("finalize %s attempt: retained owner is required at index %d", s.queueName(), i)
			}
			if request.WindowSize <= 0 {
				request.WindowSize = defaultOutboxWindowSize
			}
			if request.WindowSize > maxOutboxWindowSize {
				request.WindowSize = maxOutboxWindowSize
			}
			if request.WindowByteLimit <= 0 {
				request.WindowByteLimit = defaultOutboxWindowByteLimit
			}
			if request.WindowByteLimit > maxOutboxWindowByteLimit {
				request.WindowByteLimit = maxOutboxWindowByteLimit
			}
			if s.cfg.kind == store.OutboxQueueDispatchPTS {
				budgetCount := request.WindowByteLimit / ptsEventByteEstimate
				if budgetCount < 1 {
					budgetCount = 1
				}
				if request.WindowSize > budgetCount {
					request.WindowSize = budgetCount
				}
			}
			if request.LeaseDuration <= 0 {
				request.LeaseDuration = s.defaultLease
			}
		}
		requests[i] = request
		byStream[request.Ref.StreamID] = append(byStream[request.Ref.StreamID], request)
		if _, ok := streamSet[request.Ref.StreamID]; !ok {
			streamSet[request.Ref.StreamID] = struct{}{}
			streamIDs = append(streamIDs, request.Ref.StreamID)
		}
	}
	sort.Slice(streamIDs, func(i, j int) bool { return streamIDs[i] < streamIDs[j] })

	newlyFinalized := make(map[finalizedAttemptKey]string)
	var next []store.OutboxClaimWindow
	for groupStart := 0; groupStart < len(streamIDs); groupStart += outboxFinalizeTransactionLanes {
		groupEnd := min(groupStart+outboxFinalizeTransactionLanes, len(streamIDs))
		group := streamIDs[groupStart:groupEnd]
		// Finalization is durable lane work, not merely a transport batch. Use one
		// bounded transaction for a small stable-ordered group so 128 one-item
		// user lanes do not become 128 WAL flushes. The hard group limit prevents
		// one blocked lane from retaining an unbounded set of unrelated locks.
		err := withOutboxTx(ctx, s.db, func(tx pgx.Tx) error {
			if groupUsesExactFinalization(group, byStream) {
				finalized, err := s.finalizeLockedLaneGroup(ctx, tx, group, byStream)
				if err != nil {
					return err
				}
				for key, outcome := range finalized {
					newlyFinalized[key] = outcome
				}
				return nil
			}
			for _, streamID := range group {
				// finalizeLockedLane takes the blocking lane lock in ascending stream
				// order. Its later READ COMMITTED statements observe producers that
				// committed while it waited, closing the empty-lane marker race.
				window, finalized, err := s.finalizeLockedLane(ctx, tx, streamID, byStream[streamID])
				if err != nil {
					return err
				}
				for key, outcome := range finalized {
					newlyFinalized[key] = outcome
				}
				if window != nil {
					next = append(next, *window)
				}
			}
			return nil
		})
		if err != nil {
			return store.OutboxFinalizeBatch{}, err
		}
	}

	results := make([]store.OutboxFinalizeResult, len(requests))
	for i, request := range requests {
		result := store.OutboxFinalizeResult{Ref: request.Ref}
		if outcome, ok := newlyFinalized[finalizedAttemptKey{request.Ref.ItemID, request.Ref.LeaseFence}]; ok {
			result.Outcome = finalizeOutcome(outcome)
			results[i] = result
			continue
		}
		var attemptExists bool
		err := s.db.QueryRow(ctx, fmt.Sprintf(`
SELECT EXISTS(SELECT 1 FROM %s
WHERE stream_id = $1 AND item_id = $2 AND sequence = $3
  AND lease_fence = $4 AND attempt = $5)`, s.cfg.attemptsTable),
			request.Ref.StreamID, request.Ref.ItemID, request.Ref.Sequence,
			int64(request.Ref.LeaseFence), request.Ref.Attempt).Scan(&attemptExists)
		if err != nil {
			return store.OutboxFinalizeBatch{}, fmt.Errorf("classify %s finalize result: %w", s.queueName(), err)
		} else if attemptExists {
			var currentFence int64
			laneErr := s.db.QueryRow(ctx, fmt.Sprintf(`SELECT lease_fence FROM %s WHERE stream_id = $1`, s.cfg.lanesTable), request.Ref.StreamID).Scan(&currentFence)
			if laneErr == nil && currentFence == int64(request.Ref.LeaseFence) {
				result.Outcome = store.OutboxFinalizeWaitingForPredecessor
			} else if laneErr == nil || errors.Is(laneErr, pgx.ErrNoRows) {
				result.Outcome = store.OutboxFinalizeFenced
			} else {
				return store.OutboxFinalizeBatch{}, fmt.Errorf("classify %s lane fence: %w", s.queueName(), laneErr)
			}
		} else {
			result.Outcome = store.OutboxFinalizeFenced
		}
		results[i] = result
	}
	return store.OutboxFinalizeBatch{Results: results, Next: next}, nil
}

func groupUsesExactFinalization(streamIDs []int64, byStream map[int64][]store.OutboxFinalizeRequest) bool {
	for _, streamID := range streamIDs {
		for _, request := range byStream[streamID] {
			if request.RetainLease {
				return false
			}
		}
	}
	return true
}

func (s *durableOutboxState) finalizeLockedLane(ctx context.Context, tx pgx.Tx, streamID int64,
	requests []store.OutboxFinalizeRequest) (*store.OutboxClaimWindow, map[finalizedAttemptKey]string, error) {
	finalized := make(map[finalizedAttemptKey]string)
	lane, err := s.loadLockedLane(ctx, tx, streamID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, finalized, nil
	}
	if err != nil {
		return nil, nil, err
	}
	if lane.state != "leased" {
		return nil, finalized, nil
	}
	requestedCurrentFence := false
	for _, request := range requests {
		if request.Ref.LeaseFence == lane.leaseFence {
			requestedCurrentFence = true
			break
		}
	}
	if !requestedCurrentFence {
		return nil, finalized, nil
	}

	windowRows, err := s.loadCurrentWindow(ctx, tx, lane)
	if err != nil {
		return nil, nil, err
	}
	if len(windowRows) == 0 || windowRows[0].itemID != lane.headItemID {
		return nil, finalized, nil
	}

	deleteIDs := make([]int64, 0, len(windowRows))
	deleteOutcomes := make([]string, 0, len(windowRows))
	var stopResolution *outboxWindowAttemptRow
	stopIndex := -1
	for i := range windowRows {
		row := &windowRows[i]
		if row.attempt == nil || row.resolution == nil ||
			row.resolutionReady == nil || !*row.resolutionReady {
			break
		}
		switch *row.resolution {
		case "confirmed":
			deleteIDs = append(deleteIDs, row.itemID)
			deleteOutcomes = append(deleteOutcomes, "applied")
		case "abandoned", "terminal_resync":
			deleteIDs = append(deleteIDs, row.itemID)
			deleteOutcomes = append(deleteOutcomes, *row.resolution)
		case "retry":
			stopResolution = row
			stopIndex = i
		default:
			return nil, nil, fmt.Errorf("finalize %s lane %d: unknown resolution %q", s.queueName(), streamID, *row.resolution)
		}
		if stopResolution != nil {
			break
		}
	}

	if len(deleteIDs) > 0 {
		if err := s.deleteAttempts(ctx, tx, lane, deleteIDs); err != nil {
			return nil, nil, err
		}
		for i, itemID := range deleteIDs {
			finalized[finalizedAttemptKey{itemID, lane.leaseFence}] = deleteOutcomes[i]
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
DELETE FROM %s
WHERE target_user_id = $1 AND id = ANY($2::bigint[])`, s.cfg.itemsTable), streamID, deleteIDs); err != nil {
			return nil, nil, fmt.Errorf("delete finalized %s items: %w", s.queueName(), err)
		}
	}

	if stopResolution != nil {
		outcome := "scheduled_retry"
		readyAt := stopResolution.retryAt
		if err := s.deleteAttempts(ctx, tx, lane, []int64{stopResolution.itemID}); err != nil {
			return nil, nil, err
		}
		finalized[finalizedAttemptKey{stopResolution.itemID, lane.leaseFence}] = outcome
		// Once this head is retried/quarantined, every later command issued under
		// the old fence is invalid. Delete the suffix live attempts without
		// consuming immutable items; late receipts classify stale/fenced.
		superseded := make([]int64, 0, len(windowRows)-stopIndex-1)
		for i := stopIndex + 1; i < len(windowRows); i++ {
			if windowRows[i].attempt != nil {
				superseded = append(superseded, windowRows[i].itemID)
			}
		}
		if err := s.deleteAttempts(ctx, tx, lane, superseded); err != nil {
			return nil, nil, err
		}
		if readyAt == nil {
			return nil, nil, fmt.Errorf("finalize %s retry has no deadline", s.queueName())
		}
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
UPDATE %s
SET head_item_id = $2, head_sequence = $3, state = 'ready',
	ready_at = $4, lease_owner = '', lease_until = NULL,
	window_end_item_id = NULL, window_end_sequence = NULL,
	updated_at = now()
WHERE stream_id = $1 AND lease_fence = $5`, s.cfg.lanesTable),
			streamID, stopResolution.itemID, s.laneSequence(stopResolution),
			*readyAt, int64(lane.leaseFence)); err != nil {
			return nil, nil, fmt.Errorf("stop finalized %s lane: %w", s.queueName(), err)
		}
		return nil, finalized, nil
	}

	// If an issued successor in the current window is unresolved, advance the
	// durable head but keep the same lease; the executor already owns that item.
	if len(deleteIDs) < len(windowRows) {
		nextRow := windowRows[len(deleteIDs)]
		if len(deleteIDs) > 0 {
			if _, err := tx.Exec(ctx, fmt.Sprintf(`
UPDATE %s
SET head_item_id = $2, head_sequence = $3, updated_at = now()
WHERE stream_id = $1 AND lease_fence = $4`, s.cfg.lanesTable),
				streamID, nextRow.itemID, s.laneSequence(&nextRow), int64(lane.leaseFence)); err != nil {
				return nil, nil, fmt.Errorf("advance partial %s lane: %w", s.queueName(), err)
			}
		}
		return nil, finalized, nil
	}

	successorItemID, successorSequence, found, err := s.loadSuccessor(ctx, tx, streamID)
	if err != nil {
		return nil, nil, err
	}
	if !found {
		if err := s.deleteEmptyLaneAndRestoreAppendRace(ctx, tx, lane); err != nil {
			return nil, nil, err
		}
		return nil, finalized, nil
	}

	retain := selectRetainRequest(requests, lane)
	if retain == nil {
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
UPDATE %s
SET head_item_id = $3, head_sequence = $4,
    head_attempt = 0,
    state = 'ready', ready_at = now(), lease_owner = '', lease_until = NULL,
    window_end_item_id = NULL, window_end_sequence = NULL, updated_at = now()
WHERE stream_id = $1 AND lease_fence = $2`, s.cfg.lanesTable),
			streamID, int64(lane.leaseFence), successorItemID, successorSequence); err != nil {
			return nil, nil, fmt.Errorf("release %s successor: %w", s.queueName(), err)
		}
		return nil, finalized, nil
	}
	first := &windowRows[0]
	if first.issuedAt == nil || first.commandNotAfter == nil || first.evidenceDeadline == nil {
		return nil, nil, fmt.Errorf("issue %s handoff: current window has no persisted deadlines", s.queueName())
	}
	physicalDuration := first.commandNotAfter.Sub(*first.issuedAt)
	clockSkewAllowance := first.evidenceDeadline.Sub(*first.commandNotAfter)
	handoff, err := s.issueHandoffWindow(ctx, tx, lane, *retain, physicalDuration, clockSkewAllowance)
	if err != nil {
		return nil, nil, err
	}
	return handoff, finalized, nil
}

func (s *durableOutboxState) loadLockedLane(ctx context.Context, tx pgx.Tx, streamID int64) (outboxLaneRow, error) {
	var lane outboxLaneRow
	var fence int64
	err := tx.QueryRow(ctx, fmt.Sprintf(`
SELECT stream_id, head_item_id, head_sequence, state, lease_fence,
       lease_owner, lease_until, window_end_item_id, window_end_sequence
FROM %s
WHERE stream_id = $1
FOR UPDATE`, s.cfg.lanesTable), streamID).Scan(
		&lane.streamID, &lane.headItemID, &lane.headSequence, &lane.state, &fence,
		&lane.leaseOwner, &lane.leaseUntil, &lane.windowEndItemID, &lane.windowEndSequence)
	if err != nil {
		return lane, err
	}
	if fence < 0 {
		return lane, fmt.Errorf("load %s lane: negative fence", s.queueName())
	}
	lane.leaseFence = uint64(fence)
	return lane, nil
}

func (s *durableOutboxState) loadCurrentWindow(ctx context.Context, tx pgx.Tx, lane outboxLaneRow) ([]outboxWindowAttemptRow, error) {
	if lane.windowEndItemID == nil || lane.windowEndSequence == nil {
		return nil, nil
	}
	where := "i.id >= $3 AND i.id <= $5 AND $2::bigint IS NOT NULL AND $4::bigint IS NOT NULL"
	if s.cfg.kind == store.OutboxQueueDispatchPTS {
		where = "(i.pts, i.id) >= ($2::int, $3::bigint) AND (i.pts, i.id) <= ($4::int, $5::bigint)"
	}
	rows, err := tx.Query(ctx, fmt.Sprintf(`
SELECT i.id, %s AS sequence, a.attempt, a.issued_at, a.command_not_after, a.evidence_deadline,
       a.resolution, a.retry_at,
       CASE WHEN a.item_id IS NULL THEN NULL
            ELSE (a.retry_at IS NULL OR a.retry_at <= clock_timestamp())
       END AS resolution_ready
FROM %s i
LEFT JOIN %s a
  ON a.item_id = i.id AND a.lease_fence = $6
 AND a.stream_id = i.target_user_id AND a.sequence = %s
WHERE i.target_user_id = $1 AND %s
ORDER BY %s`, s.cfg.sequenceSQL, s.cfg.itemsTable, s.cfg.attemptsTable,
		s.cfg.sequenceSQL, where, s.cfg.orderSQL), lane.streamID, lane.headSequence,
		lane.headItemID, *lane.windowEndSequence, *lane.windowEndItemID, int64(lane.leaseFence))
	if err != nil {
		return nil, fmt.Errorf("load current %s window: %w", s.queueName(), err)
	}
	defer rows.Close()
	var out []outboxWindowAttemptRow
	for rows.Next() {
		var row outboxWindowAttemptRow
		if err := rows.Scan(&row.itemID, &row.sequence, &row.attempt, &row.issuedAt,
			&row.commandNotAfter, &row.evidenceDeadline, &row.resolution,
			&row.retryAt, &row.resolutionReady); err != nil {
			return nil, fmt.Errorf("scan current %s window: %w", s.queueName(), err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate current %s window: %w", s.queueName(), err)
	}
	return out, nil
}

func (s *durableOutboxState) deleteAttempts(ctx context.Context, tx pgx.Tx, lane outboxLaneRow,
	itemIDs []int64) error {
	if len(itemIDs) == 0 {
		return nil
	}
	tag, err := tx.Exec(ctx, fmt.Sprintf(`
DELETE FROM %s
WHERE stream_id = $1 AND lease_fence = $2
  AND item_id = ANY($3::bigint[])`, s.cfg.attemptsTable),
		lane.streamID, int64(lane.leaseFence), itemIDs)
	if err != nil {
		return fmt.Errorf("delete %s attempts: %w", s.queueName(), err)
	}
	if tag.RowsAffected() != int64(len(itemIDs)) {
		return fmt.Errorf("delete %s attempts: deleted %d of %d", s.queueName(), tag.RowsAffected(), len(itemIDs))
	}
	return nil
}

func (s *durableOutboxState) laneSequence(row *outboxWindowAttemptRow) int64 {
	if s.cfg.kind == store.OutboxQueueDispatchPTS {
		return row.sequence
	}
	return row.itemID
}

func (s *durableOutboxState) loadSuccessor(ctx context.Context, tx pgx.Tx, streamID int64) (int64, int64, bool, error) {
	var itemID, sequence int64
	err := tx.QueryRow(ctx, fmt.Sprintf(`
SELECT i.id, CASE WHEN $2::boolean THEN %s ELSE i.id END
FROM %s i
WHERE i.target_user_id = $1
ORDER BY %s
LIMIT 1`, s.cfg.sequenceSQL, s.cfg.itemsTable, s.cfg.orderSQL),
		streamID, s.cfg.kind == store.OutboxQueueDispatchPTS).Scan(&itemID, &sequence)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, 0, false, nil
	}
	if err != nil {
		return 0, 0, false, fmt.Errorf("load %s successor: %w", s.queueName(), err)
	}
	return itemID, sequence, true, nil
}

// deleteEmptyLaneAndRestoreAppendRace serializes only the empty transition with
// producer transaction commit. Insert-statement triggers hold the matching
// shared advisory fence until commit; active-lane producers remain mutually
// compatible and never update the marker. Once this exclusive fence is held, a
// producer is either visible to the fresh snapshot below or must observe the
// marker deletion and install its own marker before it can commit.
func (s *durableOutboxState) deleteEmptyLaneAndRestoreAppendRace(ctx context.Context, tx pgx.Tx, lane outboxLaneRow) error {
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(outbox_lane_advisory_key($1, $2))`,
		s.cfg.laneLockScope, lane.streamID); err != nil {
		return fmt.Errorf("lock empty %s lane transition: %w", s.queueName(), err)
	}
	itemID, sequence, found, err := s.loadSuccessor(ctx, tx, lane.streamID)
	if err != nil {
		return err
	}
	if found {
		tag, err := tx.Exec(ctx, fmt.Sprintf(`
UPDATE %s
SET head_item_id = $2, head_sequence = $3, state = 'ready', ready_at = now(),
    head_attempt = 0,
    lease_owner = '', lease_until = NULL,
    window_end_item_id = NULL, window_end_sequence = NULL,
    last_error = '', updated_at = now()
WHERE stream_id = $1 AND lease_fence = $4`, s.cfg.lanesTable),
			lane.streamID, itemID, sequence, int64(lane.leaseFence))
		if err != nil {
			return fmt.Errorf("restore visible %s lane successor: %w", s.queueName(), err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("restore visible %s lane successor: updated %d rows", s.queueName(), tag.RowsAffected())
		}
		return nil
	}
	tag, err := tx.Exec(ctx, fmt.Sprintf(`
DELETE FROM %s WHERE stream_id = $1 AND lease_fence = $2`, s.cfg.lanesTable),
		lane.streamID, int64(lane.leaseFence))
	if err != nil {
		return fmt.Errorf("delete empty %s lane marker: %w", s.queueName(), err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("delete empty %s lane marker: deleted %d rows", s.queueName(), tag.RowsAffected())
	}
	return nil
}

func selectRetainRequest(requests []store.OutboxFinalizeRequest, lane outboxLaneRow) *store.OutboxFinalizeRequest {
	var selected *store.OutboxFinalizeRequest
	for i := range requests {
		request := &requests[i]
		if !request.RetainLease || request.Ref.LeaseFence != lane.leaseFence || request.Owner != lane.leaseOwner {
			continue
		}
		if selected == nil || request.WindowSize < selected.WindowSize || request.WindowByteLimit < selected.WindowByteLimit {
			copy := *request
			selected = &copy
		}
	}
	return selected
}

func (s *durableOutboxState) issueHandoffWindow(ctx context.Context, tx pgx.Tx, lane outboxLaneRow,
	request store.OutboxFinalizeRequest, physicalDuration, clockSkewAllowance time.Duration) (*store.OutboxClaimWindow, error) {
	if physicalDuration <= 0 || clockSkewAllowance <= 0 ||
		request.LeaseDuration <= outboxLeaseSafetyMargin ||
		physicalDuration >= request.LeaseDuration-outboxLeaseSafetyMargin ||
		clockSkewAllowance >= request.LeaseDuration-outboxLeaseSafetyMargin-physicalDuration {
		return nil, fmt.Errorf("issue %s handoff: persisted deadline intervals exceed retained lease", s.queueName())
	}
	windowSize := request.WindowSize
	if s.cfg.kind == store.OutboxQueueDispatchPTS {
		budgetCount := request.WindowByteLimit / ptsEventByteEstimate
		if budgetCount < 1 {
			budgetCount = 1
		}
		if windowSize > budgetCount {
			windowSize = budgetCount
		}
	}
	windowSource := fmt.Sprintf(`
WITH headed AS (
  SELECT i.id, %s AS sequence, i.exclude_auth_key_id, i.exclude_session_id,
         %s AS payload_value, %s AS recovery_policy,
         row_number() OVER (ORDER BY %s) - 1 AS window_ordinal,
         first_value(i.exclude_auth_key_id) OVER (ORDER BY %s) AS head_auth_key_id,
         first_value(i.exclude_session_id) OVER (ORDER BY %s) AS head_session_id
  FROM %s i
  WHERE i.target_user_id = $1
), prefix AS (
  SELECT headed.*,
         bool_or(exclude_auth_key_id IS DISTINCT FROM head_auth_key_id
              OR exclude_session_id IS DISTINCT FROM head_session_id)
           OVER (ORDER BY window_ordinal ROWS UNBOUNDED PRECEDING) AS crossed_exclusion
  FROM headed
)
SELECT id, sequence, exclude_auth_key_id, exclude_session_id,
       payload_value, recovery_policy, window_ordinal
FROM prefix
WHERE NOT crossed_exclusion
ORDER BY window_ordinal
LIMIT LEAST($4, GREATEST(1, ($5::bigint / 65536)::integer))`, s.cfg.sequenceSQL, s.cfg.payloadSQL, s.cfg.recoverySQL,
		s.cfg.orderSQL, s.cfg.orderSQL, s.cfg.orderSQL, s.cfg.itemsTable)
	if s.cfg.kind == store.OutboxQueueAbsoluteDelivery {
		windowSource = fmt.Sprintf(`
WITH headed AS (
  SELECT i.id, 0::bigint AS sequence, i.exclude_auth_key_id, i.exclude_session_id,
         i.payload AS payload_value, i.recovery_policy::text AS recovery_policy,
         row_number() OVER (ORDER BY i.id) - 1 AS window_ordinal,
         sum(octet_length(i.payload)) OVER (ORDER BY i.id ROWS UNBOUNDED PRECEDING) AS cumulative_bytes,
         first_value(i.exclude_auth_key_id) OVER (ORDER BY i.id) AS head_auth_key_id,
         first_value(i.exclude_session_id) OVER (ORDER BY i.id) AS head_session_id
  FROM %s i WHERE i.target_user_id = $1
), prefix AS (
  SELECT headed.*,
         bool_or(exclude_auth_key_id IS DISTINCT FROM head_auth_key_id
              OR exclude_session_id IS DISTINCT FROM head_session_id)
           OVER (ORDER BY window_ordinal ROWS UNBOUNDED PRECEDING) AS crossed_exclusion
  FROM headed
)
SELECT id, sequence, exclude_auth_key_id, exclude_session_id, payload_value,
       recovery_policy, window_ordinal
FROM prefix
WHERE NOT crossed_exclusion
  AND (window_ordinal = 0 OR cumulative_bytes <= $5)
ORDER BY window_ordinal
LIMIT $4`, s.cfg.itemsTable)
	}
	rows, err := tx.Query(ctx, fmt.Sprintf(`
WITH windowed AS MATERIALIZED (%s), numbered AS MATERIALIZED (
  SELECT w.*, 1::integer AS attempt
  FROM windowed w
), issued AS MATERIALIZED (
  INSERT INTO %s (
    stream_id, item_id, sequence, lease_fence, attempt, window_ordinal,
    lease_owner, issued_at, lease_until, command_not_after, evidence_deadline
  )
  SELECT $1, id, sequence, $2, attempt, window_ordinal,
         $3, now(), now() + ($6::bigint * interval '1 millisecond'),
         now() + ($7::bigint * interval '1 microsecond'),
         now() + (($7::bigint + $8::bigint) * interval '1 microsecond')
  FROM numbered
  RETURNING item_id, lease_fence, attempt, lease_until,
            command_not_after, evidence_deadline
), advanced AS MATERIALIZED (
  UPDATE %s l
  SET head_item_id = bounds.head_id,
      head_sequence = CASE WHEN $9::boolean THEN bounds.head_sequence ELSE bounds.head_id END,
      head_attempt = 1,
      state = 'leased', lease_owner = $3,
      lease_until = now() + ($6::bigint * interval '1 millisecond'),
      window_end_item_id = bounds.tail_id,
      window_end_sequence = CASE WHEN $9::boolean THEN bounds.tail_sequence ELSE bounds.tail_id END,
      updated_at = now()
  FROM (
    SELECT
      (array_agg(id ORDER BY window_ordinal))[1] AS head_id,
      (array_agg(sequence ORDER BY window_ordinal))[1] AS head_sequence,
      (array_agg(id ORDER BY window_ordinal DESC))[1] AS tail_id,
      (array_agg(sequence ORDER BY window_ordinal DESC))[1] AS tail_sequence
    FROM numbered
  ) bounds
  WHERE l.stream_id = $1 AND l.lease_fence = $2
  RETURNING l.stream_id
)
SELECT n.id, n.sequence, n.exclude_auth_key_id, n.exclude_session_id,
       n.payload_value, n.recovery_policy, n.window_ordinal,
       i.attempt, i.lease_until, i.command_not_after, i.evidence_deadline
FROM numbered n
JOIN issued i ON i.item_id = n.id AND i.lease_fence = $2
JOIN advanced a ON a.stream_id = $1
ORDER BY n.window_ordinal`, windowSource, s.cfg.attemptsTable, s.cfg.lanesTable),
		lane.streamID, int64(lane.leaseFence), lane.leaseOwner, int32(windowSize),
		int64(request.WindowByteLimit), durationMillis(request.LeaseDuration),
		physicalDuration.Microseconds(), clockSkewAllowance.Microseconds(),
		s.cfg.kind == store.OutboxQueueDispatchPTS)
	if err != nil {
		return nil, fmt.Errorf("issue %s handoff: %w", s.queueName(), err)
	}
	defer rows.Close()
	window := store.OutboxClaimWindow{
		QueueKind: s.cfg.kind, StreamID: lane.streamID, Owner: lane.leaseOwner,
		LeaseFence: lane.leaseFence,
	}
	for rows.Next() {
		var itemID, sequence, excludeSession int64
		var excludeAuth []byte
		var payloadRaw any
		var recovery string
		var ordinal, attempt int
		var leaseUntil, nextCommandNotAfter, nextEvidenceDeadline time.Time
		if err := rows.Scan(&itemID, &sequence, &excludeAuth, &excludeSession, &payloadRaw,
			&recovery, &ordinal, &attempt, &leaseUntil, &nextCommandNotAfter, &nextEvidenceDeadline); err != nil {
			return nil, fmt.Errorf("scan %s handoff: %w", s.queueName(), err)
		}
		item, err := s.claimedItem(lane.streamID, itemID, sequence, lane.leaseFence,
			attempt, excludeAuth, excludeSession, payloadRaw, recovery)
		if err != nil {
			return nil, err
		}
		if ordinal != len(window.Items) {
			return nil, fmt.Errorf("scan %s handoff ordinal %d, want %d", s.queueName(), ordinal, len(window.Items))
		}
		window.LeaseUntil = leaseUntil
		item.CommandNotAfter = nextCommandNotAfter
		item.EvidenceDeadline = nextEvidenceDeadline
		window.Items = append(window.Items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s handoff: %w", s.queueName(), err)
	}
	if len(window.Items) == 0 {
		// A valid head is always admitted independently of the window budget;
		// reaching this branch therefore indicates a raced empty lane marker.
		if _, err := tx.Exec(ctx, fmt.Sprintf(`
UPDATE %s
SET state = 'ready', ready_at = now(), lease_owner = '', lease_until = NULL,
    window_end_item_id = NULL, window_end_sequence = NULL, updated_at = now()
WHERE stream_id = $1 AND lease_fence = $2`, s.cfg.lanesTable), lane.streamID, int64(lane.leaseFence)); err != nil {
			return nil, fmt.Errorf("release empty %s handoff: %w", s.queueName(), err)
		}
		return nil, nil
	}
	return &window, nil
}

func finalizeOutcome(value string) store.OutboxFinalizeOutcome {
	switch value {
	case "applied":
		return store.OutboxFinalizeApplied
	case "scheduled_retry":
		return store.OutboxFinalizeScheduledRetry
	case "abandoned":
		return store.OutboxFinalizeAbandoned
	case "terminal_resync":
		return store.OutboxFinalizeTerminalResync
	default:
		return store.OutboxFinalizeRejected
	}
}

func normalizeOutboxScheduleShards(shardCount int, shardIDs []int) ([]int16, bool, error) {
	if shardCount == 0 && len(shardIDs) == 0 {
		return nil, false, nil
	}
	if shardCount != store.DispatchOutboxLogicalShards {
		return nil, true, fmt.Errorf("shard count %d, want stable %d", shardCount, store.DispatchOutboxLogicalShards)
	}
	last := -1
	strictlyIncreasing := true
	for _, id := range shardIDs {
		if id < 0 || id >= shardCount || id <= last {
			strictlyIncreasing = false
			break
		}
		last = id
	}
	if strictlyIncreasing {
		ids := make([]int16, len(shardIDs))
		for i, id := range shardIDs {
			ids[i] = int16(id)
		}
		return ids, true, nil
	}

	var seen [store.DispatchOutboxLogicalShards / 64]uint64
	unique := 0
	for _, id := range shardIDs {
		if id < 0 || id >= shardCount {
			continue
		}
		word := id / 64
		mask := uint64(1) << uint(id%64)
		if seen[word]&mask != 0 {
			continue
		}
		seen[word] |= mask
		unique++
	}
	ids := make([]int16, 0, unique)
	for id := 0; id < shardCount; id++ {
		if seen[id/64]&(uint64(1)<<uint(id%64)) != 0 {
			ids = append(ids, int16(id))
		}
	}
	return ids, true, nil
}

func durationMillis(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	ms := d.Milliseconds()
	if ms < 1 {
		return 1
	}
	return ms
}

func (s *durableOutboxState) queueName() string {
	if s.cfg.kind == store.OutboxQueueDispatchPTS {
		return "dispatch outbox"
	}
	return "absolute delivery outbox"
}

func bytes16Hex(v [16]byte) string { return hex.EncodeToString(v[:]) }

func parseBytes16Hex(value string) ([16]byte, error) {
	var out [16]byte
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return out, err
	}
	if len(decoded) != len(out) {
		return out, fmt.Errorf("decoded length %d, want %d", len(decoded), len(out))
	}
	copy(out[:], decoded)
	return out, nil
}

func validBytes16(v [16]byte) bool { return v != ([16]byte{}) }

func validOutboxTargetUserID(kind store.OutboxQueueKind, streamID, targetUserID int64) bool {
	if kind == store.OutboxQueueChannelPTS {
		return targetUserID == 0
	}
	return targetUserID > 0 && targetUserID == streamID
}

func validAttemptRef(ref store.OutboxAttemptRef, kind store.OutboxQueueKind) bool {
	if ref.QueueKind != kind || ref.StreamID <= 0 || ref.ItemID <= 0 || ref.LeaseFence == 0 ||
		ref.LeaseFence > math.MaxInt64 || ref.Attempt <= 0 || ref.Attempt > math.MaxInt32 {
		return false
	}
	if kind == store.OutboxQueueDispatchPTS {
		return ref.Sequence > 0 && ref.Sequence <= math.MaxInt32
	}
	return ref.Sequence == 0
}

type outboxTxBeginner interface {
	Begin(context.Context) (pgx.Tx, error)
}

func withOutboxTx(ctx context.Context, db sqlcgen.DBTX, fn func(pgx.Tx) error) error {
	if tx, ok := db.(pgx.Tx); ok {
		return fn(tx)
	}
	beginner, ok := db.(outboxTxBeginner)
	if !ok {
		return errors.New("outbox store requires pgx transaction support")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// JSON input records used by the set-based evidence APIs.
type attemptRefJSON struct {
	Ordinal    int   `json:"ordinal"`
	StreamID   int64 `json:"stream_id"`
	ItemID     int64 `json:"item_id"`
	Sequence   int64 `json:"sequence"`
	LeaseFence int64 `json:"lease_fence"`
	Attempt    int   `json:"attempt"`
}

func refJSON(ordinal int, ref store.OutboxAttemptRef) attemptRefJSON {
	return attemptRefJSON{ordinal, ref.StreamID, ref.ItemID, ref.Sequence, int64(ref.LeaseFence), ref.Attempt}
}

func marshalJSONInput(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal outbox batch: %w", err)
	}
	return b, nil
}
