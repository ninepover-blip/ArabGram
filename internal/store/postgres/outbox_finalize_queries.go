package postgres

import (
	"fmt"

	"telesrv/internal/store"
)

// withOutboxFinalizeQueries formats immutable exact-finalizer SQL once for a
// complete queue configuration. Runtime calls still bind fresh arguments and
// execute the same statements in the same transaction and lock order.
func withOutboxFinalizeQueries(cfg outboxStateConfig) outboxStateConfig {
	cfg.finalizeLockLaneGroupSQL = outboxFinalizeLockLaneGroupQuery(cfg)
	cfg.finalizeLoadCurrentWindowSQL = outboxFinalizeLoadCurrentWindowQuery(cfg)
	cfg.finalizeApplyTerminalGroupSQL = outboxFinalizeApplyTerminalGroupQuery(cfg)
	cfg.finalizeLoadSuccessorGroupSQL = outboxFinalizeLoadSuccessorGroupQuery(cfg)
	cfg.finalizeReleaseSuccessorGroupSQL = outboxFinalizeReleaseSuccessorGroupQuery(cfg)
	cfg.finalizeDeleteEmptyLaneGroupSQL = outboxFinalizeDeleteEmptyLaneGroupQuery(cfg)
	return cfg
}

func outboxFinalizeLockLaneGroupQuery(cfg outboxStateConfig) string {
	return fmt.Sprintf(`
SELECT stream_id, head_item_id, head_sequence, state, lease_fence,
       lease_owner, lease_until, window_end_item_id, window_end_sequence
FROM %s
WHERE stream_id = ANY($1::bigint[])
ORDER BY stream_id
FOR UPDATE`, cfg.lanesTable)
}

func outboxFinalizeLoadCurrentWindowQuery(cfg outboxStateConfig) string {
	windowPredicate := "i.id >= input.head_item_id AND i.id <= input.end_item_id"
	if cfg.kind == store.OutboxQueueDispatchPTS {
		windowPredicate = "(i.pts, i.id) >= (input.head_sequence::int, input.head_item_id) AND (i.pts, i.id) <= (input.end_sequence::int, input.end_item_id)"
	}
	return fmt.Sprintf(`
WITH input AS MATERIALIZED (
  SELECT *
  FROM unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[], $5::bigint[], $6::bigint[])
    AS x(stream_id, head_sequence, head_item_id, end_sequence, end_item_id, lease_fence)
)
SELECT input.stream_id, i.id, %s AS sequence, a.attempt,
       a.issued_at, a.command_not_after, a.evidence_deadline,
       a.resolution, a.retry_at,
       CASE WHEN a.item_id IS NULL THEN NULL
            ELSE (a.retry_at IS NULL OR a.retry_at <= clock_timestamp())
       END AS resolution_ready
FROM input
JOIN %s i ON i.target_user_id = input.stream_id AND %s
LEFT JOIN %s a
  ON a.item_id = i.id AND a.lease_fence = input.lease_fence
 AND a.stream_id = i.target_user_id AND a.sequence = %s
ORDER BY input.stream_id, %s`, cfg.sequenceSQL, cfg.itemsTable, windowPredicate,
		cfg.attemptsTable, cfg.sequenceSQL, cfg.orderSQL)
}

func outboxFinalizeApplyTerminalGroupQuery(cfg outboxStateConfig) string {
	return fmt.Sprintf(`
WITH item_input AS MATERIALIZED (
  SELECT *
  FROM unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::text[])
    AS x(item_id, stream_id, lease_fence, outcome)
), lane_input AS MATERIALIZED (
  SELECT *
  FROM unnest($5::bigint[], $6::bigint[], $7::boolean[], $8::bigint[], $9::bigint[])
    AS x(stream_id, lease_fence, partial, next_item_id, next_sequence)
), finalized AS MATERIALIZED (
DELETE FROM %s a
USING item_input input
WHERE a.item_id = input.item_id AND a.stream_id = input.stream_id
  AND a.lease_fence = input.lease_fence
RETURNING a.item_id, a.stream_id, a.lease_fence
), deleted AS MATERIALIZED (
DELETE FROM %s i
USING item_input input
WHERE i.id = input.item_id AND i.target_user_id = input.stream_id
  AND EXISTS (
    SELECT 1 FROM finalized f
    WHERE f.item_id = input.item_id AND f.stream_id = input.stream_id
      AND f.lease_fence = input.lease_fence
  )
RETURNING i.id, i.target_user_id
), delete_guard AS MATERIALIZED (
  SELECT count(*)::bigint AS deleted_count FROM deleted
), successors AS MATERIALIZED (
  SELECT input.stream_id, successor.item_id, successor.sequence
  FROM lane_input input
  CROSS JOIN delete_guard
  LEFT JOIN LATERAL (
    SELECT i.id AS item_id,
           CASE WHEN $10::boolean THEN %s ELSE i.id END AS sequence
    FROM %s i
    WHERE i.target_user_id = input.stream_id
      AND NOT EXISTS (
        SELECT 1 FROM item_input consumed
        WHERE consumed.item_id = i.id AND consumed.stream_id = i.target_user_id
      )
    ORDER BY %s
    LIMIT 1
  ) successor ON true
  WHERE NOT input.partial
), updated_lanes AS MATERIALIZED (
UPDATE %s l
SET head_item_id = CASE WHEN input.partial THEN input.next_item_id ELSE successor.item_id END,
    head_sequence = CASE WHEN input.partial THEN input.next_sequence ELSE successor.sequence END,
    head_attempt = CASE WHEN input.partial THEN l.head_attempt ELSE 0 END,
    state = CASE WHEN input.partial THEN l.state ELSE 'ready' END,
    ready_at = CASE WHEN input.partial THEN l.ready_at ELSE now() END,
    lease_owner = CASE WHEN input.partial THEN l.lease_owner ELSE '' END,
    lease_until = CASE WHEN input.partial THEN l.lease_until ELSE NULL END,
    window_end_item_id = CASE WHEN input.partial THEN l.window_end_item_id ELSE NULL END,
    window_end_sequence = CASE WHEN input.partial THEN l.window_end_sequence ELSE NULL END,
    last_error = CASE WHEN input.partial THEN l.last_error ELSE '' END,
    updated_at = now()
FROM lane_input input
LEFT JOIN successors successor ON successor.stream_id = input.stream_id
CROSS JOIN delete_guard
WHERE l.stream_id = input.stream_id AND l.lease_fence = input.lease_fence
  AND (input.partial OR successor.item_id IS NOT NULL)
RETURNING l.stream_id
)
SELECT
  (SELECT count(*)::bigint FROM finalized),
  delete_guard.deleted_count,
  (SELECT count(*)::bigint FROM updated_lanes),
  COALESCE(array_agg(successors.stream_id ORDER BY successors.stream_id)
    FILTER (WHERE successors.stream_id IS NOT NULL AND successors.item_id IS NULL), '{}'::bigint[])
FROM delete_guard
LEFT JOIN successors ON true
GROUP BY delete_guard.deleted_count`, cfg.attemptsTable, cfg.itemsTable,
		cfg.sequenceSQL, cfg.itemsTable, cfg.orderSQL, cfg.lanesTable)
}

func outboxFinalizeLoadSuccessorGroupQuery(cfg outboxStateConfig) string {
	return fmt.Sprintf(`
SELECT requested.stream_id, successor.item_id, successor.sequence
FROM unnest($1::bigint[]) AS requested(stream_id)
LEFT JOIN LATERAL (
  SELECT i.id AS item_id, CASE WHEN $2::boolean THEN %s ELSE i.id END AS sequence
  FROM %s i
  WHERE i.target_user_id = requested.stream_id
  ORDER BY %s
  LIMIT 1
) successor ON true
ORDER BY requested.stream_id`, cfg.sequenceSQL, cfg.itemsTable, cfg.orderSQL)
}

func outboxFinalizeReleaseSuccessorGroupQuery(cfg outboxStateConfig) string {
	return fmt.Sprintf(`
WITH input AS (
  SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::bigint[], $4::bigint[])
    AS x(stream_id, lease_fence, head_item_id, head_sequence)
)
UPDATE %s l
SET head_item_id = input.head_item_id, head_sequence = input.head_sequence,
    head_attempt = 0,
    state = 'ready', ready_at = now(), lease_owner = '', lease_until = NULL,
    window_end_item_id = NULL, window_end_sequence = NULL,
    last_error = '', updated_at = now()
FROM input
WHERE l.stream_id = input.stream_id AND l.lease_fence = input.lease_fence`, cfg.lanesTable)
}

func outboxFinalizeDeleteEmptyLaneGroupQuery(cfg outboxStateConfig) string {
	return fmt.Sprintf(`
WITH input AS (
  SELECT * FROM unnest($1::bigint[], $2::bigint[]) AS x(stream_id, lease_fence)
)
DELETE FROM %s l
USING input
WHERE l.stream_id = input.stream_id AND l.lease_fence = input.lease_fence`, cfg.lanesTable)
}
