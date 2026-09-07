-- Reconcile channel markers that may have skipped an uncommitted successor
-- before the channel producer/finalizer commit fence was introduced.
INSERT INTO channel_delivery_lanes (
    channel_id, head_item_id, head_sequence, state, ready_at
)
SELECT DISTINCT ON (e.channel_id)
       e.channel_id, e.id, e.min_pts, 'ready', now()
FROM channel_delivery_events e
LEFT JOIN channel_delivery_lanes l ON l.channel_id = e.channel_id
WHERE l.channel_id IS NULL
ORDER BY e.channel_id, e.min_pts, e.id
ON CONFLICT (channel_id) DO NOTHING;

WITH earliest AS MATERIALIZED (
    SELECT DISTINCT ON (e.channel_id)
           e.channel_id, e.id AS item_id, e.min_pts AS sequence
    FROM channel_delivery_events e
    ORDER BY e.channel_id, e.min_pts, e.id
)
UPDATE channel_delivery_lanes l
SET head_item_id = e.item_id,
    head_sequence = e.sequence,
    updated_at = now()
FROM earliest e
WHERE l.channel_id = e.channel_id
  AND (e.sequence, e.item_id) < (l.head_sequence, l.head_item_id);
