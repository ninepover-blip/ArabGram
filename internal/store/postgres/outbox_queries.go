package postgres

import (
	"fmt"
	"math"

	"telesrv/internal/store"
)

// withOutboxQueries formats immutable SQL once for each complete queue
// configuration. Every execution still supplies fresh request arguments and
// reads fresh PostgreSQL clocks, locks, fences, and durable state.
func withOutboxQueries(cfg outboxStateConfig) outboxStateConfig {
	cfg = withOutboxNextReadyQueries(cfg)
	cfg = withOutboxFinalizeQueries(cfg)
	cfg.claimAllSQL = outboxClaimQuery(cfg, "TRUE")
	cfg.claimShardSQL = outboxClaimQuery(cfg, "l.logical_shard = ANY($8::smallint[])")
	cfg.bindTargetsSQL = outboxBindTargetsQuery(cfg)
	cfg.recordEvidenceSQL = outboxRecordEvidenceQuery(cfg)
	return cfg
}

func outboxClaimQuery(cfg outboxStateConfig, shardPredicate string) string {
	windowSource := fmt.Sprintf(`
WITH headed AS (
  SELECT
    i.id,
    %s AS sequence,
    i.exclude_auth_key_id,
    i.exclude_session_id,
    %s AS payload_value,
    %s AS recovery_policy,
    row_number() OVER (ORDER BY %s) - 1 AS window_ordinal,
    first_value(i.exclude_auth_key_id) OVER (ORDER BY %s) AS head_auth_key_id,
    first_value(i.exclude_session_id) OVER (ORDER BY %s) AS head_session_id
  FROM %s i
  WHERE i.target_user_id = leased.stream_id
    AND (i.pts::bigint, i.id) >= (leased.head_sequence, leased.head_item_id)
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
WHERE NOT crossed_exclusion AND $5::bigint > 0
ORDER BY window_ordinal
LIMIT LEAST($4, GREATEST(1, ($5::bigint / 65536)::integer))`, cfg.sequenceSQL, cfg.payloadSQL, cfg.recoverySQL, cfg.orderSQL, cfg.orderSQL, cfg.orderSQL, cfg.itemsTable)
	if cfg.kind == store.OutboxQueueAbsoluteDelivery {
		windowSource = fmt.Sprintf(`
WITH headed AS (
  SELECT
    i.id,
    0::bigint AS sequence,
    i.exclude_auth_key_id,
    i.exclude_session_id,
    i.payload AS payload_value,
    i.recovery_policy::text AS recovery_policy,
    row_number() OVER (ORDER BY i.id) - 1 AS window_ordinal,
    sum(octet_length(i.payload)) OVER (ORDER BY i.id ROWS UNBOUNDED PRECEDING) AS cumulative_bytes,
    first_value(i.exclude_auth_key_id) OVER (ORDER BY i.id) AS head_auth_key_id,
    first_value(i.exclude_session_id) OVER (ORDER BY i.id) AS head_session_id
  FROM %s i
  WHERE i.target_user_id = leased.stream_id
    AND i.id >= leased.head_item_id
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
LIMIT $4`, cfg.itemsTable)
	}
	budgetPredicate := "TRUE"

	query := fmt.Sprintf(`
WITH ready_candidates AS MATERIALIZED (
  SELECT l.stream_id, l.head_item_id, l.head_sequence, l.head_attempt,
         l.lease_fence AS old_fence,
         l.ready_at AS available_at
  FROM %s l
  WHERE %s
    AND %s
    AND l.lease_fence < %d
    AND l.state = 'ready'
    AND l.ready_at <= now()
  ORDER BY l.ready_at, l.stream_id, l.head_sequence, l.head_item_id
  LIMIT $3
  FOR UPDATE OF l SKIP LOCKED
), expired_candidates AS MATERIALIZED (
  SELECT l.stream_id, l.head_item_id, l.head_sequence, l.head_attempt,
         l.lease_fence AS old_fence,
         l.lease_until AS available_at
  FROM %s l
  WHERE %s
    AND %s
    AND l.lease_fence < %d
    AND l.state = 'leased'
    AND l.lease_until <= now()
    AND NOT EXISTS (
      SELECT 1
      FROM %s pending_finalization
      WHERE pending_finalization.stream_id = l.stream_id
        AND pending_finalization.lease_fence = l.lease_fence
        AND pending_finalization.resolution IS NOT NULL
    )
    AND NOT EXISTS (
      SELECT 1
      FROM %s recoverable_bound
      WHERE recoverable_bound.stream_id = l.stream_id
        AND recoverable_bound.lease_fence = l.lease_fence
        AND recoverable_bound.item_id = l.head_item_id
        AND recoverable_bound.targets_bound
        AND recoverable_bound.resolution IS NULL
    )
  ORDER BY l.lease_until, l.stream_id, l.head_sequence, l.head_item_id
  LIMIT $3
  FOR UPDATE OF l SKIP LOCKED
), picked AS MATERIALIZED (
  SELECT stream_id, head_item_id, head_sequence, head_attempt, old_fence
  FROM (
    SELECT * FROM ready_candidates
    UNION ALL
    SELECT * FROM expired_candidates
  ) candidates
  ORDER BY available_at, stream_id, head_sequence, head_item_id
  LIMIT $3
), fence AS MATERIALIZED (
  SELECT nextval('%s') AS lease_fence
  FROM picked
  LIMIT 1
), leased AS MATERIALIZED (
  SELECT p.stream_id, p.head_item_id, p.head_sequence, p.old_fence,
         f.lease_fence,
         p.head_attempt + 1 AS attempt,
         $1::text AS lease_owner,
         now() + ($2::bigint * interval '1 millisecond') AS lease_until,
         now() + ($6::bigint * interval '1 millisecond') AS command_not_after,
         now() + (($6::bigint + $7::bigint) * interval '1 millisecond') AS evidence_deadline
  FROM picked p
  CROSS JOIN fence f
), superseded AS MATERIALIZED (
  DELETE FROM %s old
  USING leased fresh
  WHERE old.stream_id = fresh.stream_id
    AND old.lease_fence = fresh.old_fence
  RETURNING old.item_id, old.lease_fence
), windowed AS MATERIALIZED (
  SELECT leased.*, item.*
  FROM leased
  CROSS JOIN LATERAL (%s) item
), numbered AS MATERIALIZED (
  SELECT w.*
  FROM windowed w
), issued AS MATERIALIZED (
  INSERT INTO %s (
    stream_id, item_id, sequence, lease_fence, attempt, window_ordinal,
    lease_owner, issued_at, lease_until, command_not_after, evidence_deadline
  )
  SELECT stream_id, id, sequence, lease_fence, attempt, window_ordinal,
         lease_owner, now(), lease_until, command_not_after, evidence_deadline
  FROM numbered
  RETURNING item_id, lease_fence, attempt
), lane_windows AS MATERIALIZED (
  UPDATE %s l
  SET state = 'leased',
      lease_fence = ends.lease_fence,
      head_attempt = ends.attempt,
      lease_owner = ends.lease_owner,
      lease_until = ends.lease_until,
      window_end_item_id = ends.item_id,
      window_end_sequence = ends.sequence,
      last_error = '',
      updated_at = now()
  FROM (
    SELECT DISTINCT ON (n.stream_id)
           n.stream_id, n.old_fence, n.lease_fence, n.attempt,
           n.lease_owner, n.lease_until,
           n.id AS item_id, n.sequence
    FROM numbered n
    ORDER BY n.stream_id, n.window_ordinal DESC
  ) ends
  WHERE l.stream_id = ends.stream_id AND l.lease_fence = ends.old_fence
  RETURNING l.stream_id, l.lease_fence
)
SELECT
  n.stream_id, n.lease_fence, n.lease_owner, n.lease_until,
  n.id, n.sequence, n.exclude_auth_key_id, n.exclude_session_id,
  n.payload_value, n.recovery_policy, n.command_not_after, n.evidence_deadline,
  n.window_ordinal, i.attempt
FROM numbered n
JOIN issued i ON i.item_id = n.id AND i.lease_fence = n.lease_fence
JOIN lane_windows lw ON lw.stream_id = n.stream_id AND lw.lease_fence = n.lease_fence
ORDER BY n.stream_id, n.window_ordinal`,
		cfg.lanesTable, shardPredicate, budgetPredicate, int64(math.MaxInt64),
		cfg.lanesTable, shardPredicate, budgetPredicate, int64(math.MaxInt64), cfg.attemptsTable, cfg.attemptsTable,
		cfg.fenceSequence,
		cfg.attemptsTable, windowSource, cfg.attemptsTable, cfg.lanesTable)

	return query
}

func outboxBindTargetsQuery(cfg outboxStateConfig) string {
	return fmt.Sprintf(`
WITH input AS MATERIALIZED (

  SELECT ordinal, stream_id, item_id, sequence, lease_fence, attempt,
         source_instance_id,
         COALESCE(targets, '[]'::jsonb) AS targets
  FROM jsonb_to_recordset($1::jsonb) AS i(
    ordinal integer, stream_id bigint, item_id bigint, sequence bigint,

    lease_fence bigint, attempt integer, source_instance_id text, targets jsonb
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

  SELECT i.*, a.targets_bound, a.target_count, a.delivery_source_instance_id,
         a.empty_evidence_kind, a.resolution,
         a.command_not_after, a.evidence_deadline, a.lease_until,
         l.lease_fence AS current_fence, l.state AS lane_state
  FROM input i
  LEFT JOIN locked_attempts a
    ON a.stream_id = i.stream_id AND a.item_id = i.item_id
   AND a.sequence = i.sequence AND a.lease_fence = i.lease_fence
   AND a.attempt = i.attempt
  LEFT JOIN %s l ON l.stream_id = i.stream_id
), bindable AS MATERIALIZED (
  SELECT e.*
  FROM exact e
  WHERE NOT e.targets_bound
    AND e.command_not_after < e.evidence_deadline
    AND e.evidence_deadline < e.lease_until
    AND e.current_fence = e.lease_fence AND e.lane_state = 'leased'
), expanded AS MATERIALIZED (
  SELECT b.item_id, b.lease_fence,

         t.target_instance_id, t.target_user_id,
         decode(t.batch_id, 'hex') AS batch_id,
         decode(t.command_id, 'hex') AS command_id
  FROM bindable b
  CROSS JOIN LATERAL jsonb_to_recordset(b.targets) AS t(

    target_instance_id text, target_user_id bigint, batch_id text, command_id text
  )
), inserted_targets AS MATERIALIZED (
  INSERT INTO %s (
    item_id, lease_fence,

    target_instance_id, target_user_id, batch_id, command_id
  )
  SELECT item_id, lease_fence,

         target_instance_id, target_user_id, batch_id, command_id
  FROM expanded
  RETURNING item_id, lease_fence
), inserted_count AS MATERIALIZED (
  SELECT count(*)::integer AS target_count
  FROM inserted_targets
), newly_bound AS MATERIALIZED (
  UPDATE %s a
  SET targets_bound = true,

      target_count = jsonb_array_length(b.targets),
      delivery_source_instance_id = b.source_instance_id,
      empty_evidence_kind = CASE
        WHEN jsonb_array_length(b.targets) = 0 THEN 'authoritative_no_targets'
        ELSE NULL
      END,
      evidence_at = CASE
        WHEN jsonb_array_length(b.targets) = 0 THEN clock_timestamp()
        ELSE NULL
      END,
      resolution = CASE
        WHEN jsonb_array_length(b.targets) = 0 THEN 'confirmed'
        ELSE NULL
      END
  FROM bindable b
  CROSS JOIN inserted_count inserted
  WHERE a.item_id = b.item_id AND a.lease_fence = b.lease_fence
    AND NOT a.targets_bound
    AND inserted.target_count = (
      SELECT count(*)::integer
      FROM expanded x
    )
  RETURNING a.item_id, a.lease_fence
)
SELECT e.ordinal,
  CASE
    WHEN e.targets_bound IS NULL THEN 'fenced'
    WHEN e.current_fence IS DISTINCT FROM e.lease_fence OR e.lane_state <> 'leased' THEN 'fenced'
    WHEN n.item_id IS NOT NULL THEN 'bound'

    WHEN e.targets_bound
      AND e.delivery_source_instance_id = e.source_instance_id
      AND e.target_count = jsonb_array_length(e.targets)
      AND (
        e.target_count > 0 OR
        (e.empty_evidence_kind = 'authoritative_no_targets' AND e.resolution = 'confirmed')
      )
      AND NOT EXISTS (
        SELECT 1
        FROM jsonb_to_recordset(e.targets) AS wanted(

          target_instance_id text, target_user_id bigint, batch_id text, command_id text
        )
        WHERE NOT EXISTS (
          SELECT 1 FROM %s existing
          WHERE existing.item_id = e.item_id
            AND existing.lease_fence = e.lease_fence
            AND existing.target_instance_id = wanted.target_instance_id

            AND existing.target_user_id = wanted.target_user_id
            AND existing.batch_id = decode(wanted.batch_id, 'hex')
            AND existing.command_id = decode(wanted.command_id, 'hex')
        )
      ) THEN 'duplicate'
    ELSE 'rejected'
  END AS outcome,
  COALESCE((SELECT count(*) FROM inserted_targets), 0) AS inserted_guard
FROM exact e
LEFT JOIN newly_bound n USING (item_id, lease_fence)
ORDER BY e.ordinal`, cfg.attemptsTable, cfg.lanesTable,
		cfg.targetsTable, cfg.attemptsTable, cfg.targetsTable)
}

func outboxRecordEvidenceQuery(cfg outboxStateConfig) string {
	return fmt.Sprintf(`
WITH input AS MATERIALIZED (
  SELECT *
  FROM jsonb_to_recordset($1::jsonb) AS i(
    ordinal integer, stream_id bigint, item_id bigint, sequence bigint,
    lease_fence bigint, attempt integer, kind text, source_instance_id text,
    target_instance_id text, target_user_id bigint, batch_id text, command_id text,
    eligible_sessions integer, written_sessions integer,
    server_msg_id bigint, observed_at timestamptz
  )
), db_clock AS MATERIALIZED (
  SELECT clock_timestamp() AS observed_now
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
  SELECT i.*, a.targets_bound, a.target_count, a.resolution,
         a.delivery_source_instance_id,
         a.empty_evidence_kind, a.evidence_deadline,
         a.evidence_deadline > c.observed_now AS evidence_live,
         l.lease_fence AS current_fence, l.state AS lane_state,
         t.evidence_kind AS target_evidence_kind,
         t.eligible_sessions AS target_eligible_sessions,
         t.written_sessions AS target_written_sessions,
         t.physical_first_server_msg_id AS target_server_msg_id,
         (t.item_id IS NOT NULL) AS target_exists
  FROM input i
  LEFT JOIN locked_attempts a
    ON a.stream_id = i.stream_id AND a.item_id = i.item_id
   AND a.sequence = i.sequence AND a.lease_fence = i.lease_fence
   AND a.attempt = i.attempt
  LEFT JOIN %s l ON l.stream_id = i.stream_id
  LEFT JOIN %s t
    ON i.kind <> 'authoritative_no_targets'
   AND t.item_id = i.item_id AND t.lease_fence = i.lease_fence
   AND t.target_instance_id = i.target_instance_id
   AND t.target_user_id = i.target_user_id
   AND t.batch_id = decode(i.batch_id, 'hex')
   AND t.command_id = decode(i.command_id, 'hex')
  CROSS JOIN db_clock c
), target_updated AS MATERIALIZED (
  UPDATE %s t
  SET evidence_kind = e.kind,
      evidence_at = e.observed_at,
      eligible_sessions = e.eligible_sessions,
      written_sessions = e.written_sessions,
      physical_first_server_msg_id = e.server_msg_id
  FROM exact e
  WHERE e.kind IN ('edge_written', 'edge_no_eligible')
    AND t.item_id = e.item_id AND t.lease_fence = e.lease_fence
    AND t.target_instance_id = e.target_instance_id
    AND t.target_user_id = e.target_user_id
    AND t.batch_id = decode(e.batch_id, 'hex')
    AND t.command_id = decode(e.command_id, 'hex')
    AND t.evidence_kind IS NULL
    AND e.resolution IS NULL
    AND e.delivery_source_instance_id = e.source_instance_id
    AND e.targets_bound AND e.current_fence = e.lease_fence
    AND e.lane_state = 'leased'
    AND e.evidence_live
  RETURNING t.item_id, t.lease_fence, t.target_instance_id, t.target_user_id, t.batch_id, t.command_id
), empty_updated AS MATERIALIZED (
  UPDATE %s a
  SET empty_evidence_kind = 'authoritative_no_targets',
      evidence_at = e.observed_at,
      resolution = 'confirmed'
  FROM exact e
  WHERE e.kind = 'authoritative_no_targets'
    AND a.item_id = e.item_id AND a.lease_fence = e.lease_fence
    AND a.resolution IS NULL
    AND e.delivery_source_instance_id = e.source_instance_id
    AND a.targets_bound AND a.target_count = 0
    AND e.current_fence = e.lease_fence AND e.lane_state = 'leased'
    AND e.evidence_live
  RETURNING a.item_id, a.lease_fence
), confirmed_nonempty AS MATERIALIZED (
  UPDATE %s a
  SET resolution = 'confirmed',
      evidence_at = COALESCE(a.evidence_at, now())
  WHERE a.resolution IS NULL
    AND a.targets_bound AND a.target_count > 0
    AND EXISTS (
      SELECT 1 FROM target_updated u
      WHERE u.item_id = a.item_id AND u.lease_fence = a.lease_fence
    )
    AND NOT EXISTS (
      SELECT 1
      FROM %s pending
      WHERE pending.item_id = a.item_id
        AND pending.lease_fence = a.lease_fence
        AND pending.evidence_kind IS NULL
        AND NOT EXISTS (
          SELECT 1 FROM target_updated u
          WHERE u.item_id = pending.item_id
            AND u.lease_fence = pending.lease_fence
            AND u.target_instance_id = pending.target_instance_id
            AND u.target_user_id = pending.target_user_id
            AND u.batch_id = pending.batch_id
            AND u.command_id = pending.command_id
        )
    )
  RETURNING a.item_id, a.lease_fence
)
SELECT e.ordinal,
  CASE
    WHEN e.targets_bound IS NULL THEN 'fenced'
    WHEN e.current_fence IS DISTINCT FROM e.lease_fence OR e.lane_state <> 'leased' THEN 'fenced'
    WHEN e.delivery_source_instance_id IS DISTINCT FROM e.source_instance_id THEN 'fenced'
    WHEN NOT COALESCE(e.evidence_live, false) THEN 'fenced'
    WHEN NOT e.targets_bound THEN 'rejected'
    WHEN e.kind = 'authoritative_no_targets' AND e.target_count <> 0 THEN 'rejected'
    WHEN e.kind = 'authoritative_no_targets' AND e.empty_evidence_kind IS NOT NULL THEN 'duplicate'
    WHEN e.kind = 'authoritative_no_targets' AND eu.item_id IS NOT NULL THEN 'recorded'
    WHEN e.kind <> 'authoritative_no_targets' AND NOT e.target_exists THEN 'rejected'
    WHEN e.kind IN ('edge_written', 'edge_no_eligible')
      AND e.target_evidence_kind = e.kind
      AND e.target_eligible_sessions = e.eligible_sessions
      AND e.target_written_sessions = e.written_sessions
      AND e.target_server_msg_id = e.server_msg_id THEN 'duplicate'
    WHEN e.kind IN ('edge_written', 'edge_no_eligible') AND e.target_evidence_kind IS NOT NULL THEN 'rejected'
    WHEN e.kind IN ('edge_written', 'edge_no_eligible') AND tu.item_id IS NOT NULL THEN 'recorded'
    WHEN e.resolution = 'confirmed' THEN 'duplicate'
    ELSE 'rejected'
  END AS outcome,
  COALESCE((SELECT count(*) FROM confirmed_nonempty), 0) AS confirmed_guard
FROM exact e
LEFT JOIN empty_updated eu
  ON eu.item_id = e.item_id AND eu.lease_fence = e.lease_fence
LEFT JOIN target_updated tu
  ON tu.item_id = e.item_id AND tu.lease_fence = e.lease_fence
 AND tu.target_instance_id = e.target_instance_id
 AND tu.target_user_id = e.target_user_id
 AND tu.batch_id = decode(e.batch_id, 'hex')
 AND tu.command_id = decode(e.command_id, 'hex')
ORDER BY e.ordinal`, cfg.attemptsTable, cfg.lanesTable, cfg.targetsTable,
		cfg.targetsTable, cfg.attemptsTable, cfg.attemptsTable, cfg.targetsTable)
}
