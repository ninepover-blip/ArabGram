-- Repair markers created after an older uncommitted item was orphaned by the
-- pre-0199 empty-transition race.  Missing markers are recreated and existing
-- markers are moved only toward the earliest immutable item.
ALTER TABLE dispatch_outbox_lanes
    ADD CONSTRAINT dispatch_outbox_lanes_window_order_check
    CHECK (
        (state = 'ready' AND window_end_item_id IS NULL)
        OR
        (state = 'leased' AND window_end_item_id IS NOT NULL
         AND (head_sequence, head_item_id) <= (window_end_sequence, window_end_item_id))
    ) NOT VALID;

ALTER TABLE edge_delivery_outbox_lanes
    ADD CONSTRAINT edge_delivery_outbox_lanes_window_order_check
    CHECK (
        (state = 'ready' AND window_end_item_id IS NULL)
        OR
        (state = 'leased' AND window_end_item_id IS NOT NULL
         AND head_item_id <= window_end_item_id)
    ) NOT VALID;

INSERT INTO dispatch_outbox_lanes (stream_id, head_item_id, head_sequence)
SELECT DISTINCT ON (i.target_user_id) i.target_user_id, i.id, i.pts
FROM dispatch_outbox i
LEFT JOIN dispatch_outbox_lanes l ON l.stream_id = i.target_user_id
WHERE l.stream_id IS NULL
ORDER BY i.target_user_id, i.pts, i.id
ON CONFLICT (stream_id) DO NOTHING;

WITH earliest AS MATERIALIZED (
    SELECT DISTINCT ON (i.target_user_id)
           i.target_user_id AS stream_id, i.id AS item_id, i.pts::bigint AS sequence
    FROM dispatch_outbox i
    ORDER BY i.target_user_id, i.pts, i.id
)
UPDATE dispatch_outbox_lanes l
SET head_item_id = e.item_id,
    head_sequence = e.sequence,
    updated_at = now()
FROM earliest e
WHERE l.stream_id = e.stream_id
  AND (e.sequence, e.item_id) < (l.head_sequence, l.head_item_id);

INSERT INTO edge_delivery_outbox_lanes (stream_id, head_item_id, head_sequence)
SELECT DISTINCT ON (i.target_user_id) i.target_user_id, i.id, i.id
FROM edge_delivery_outbox i
LEFT JOIN edge_delivery_outbox_lanes l ON l.stream_id = i.target_user_id
WHERE l.stream_id IS NULL
ORDER BY i.target_user_id, i.id
ON CONFLICT (stream_id) DO NOTHING;

WITH earliest AS MATERIALIZED (
    SELECT DISTINCT ON (i.target_user_id)
           i.target_user_id AS stream_id, i.id AS item_id
    FROM edge_delivery_outbox i
    ORDER BY i.target_user_id, i.id
)
UPDATE edge_delivery_outbox_lanes l
SET head_item_id = e.item_id,
    head_sequence = e.item_id,
    updated_at = now()
FROM earliest e
WHERE l.stream_id = e.stream_id
  AND e.item_id < l.head_item_id;
