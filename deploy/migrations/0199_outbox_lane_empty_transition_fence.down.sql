CREATE OR REPLACE FUNCTION dispatch_outbox_insert_lane_marker()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO dispatch_outbox_lanes (stream_id, head_item_id, head_sequence)
    SELECT DISTINCT ON (target_user_id) target_user_id, id, pts
    FROM new_rows
    ORDER BY target_user_id, pts, id
    ON CONFLICT (stream_id) DO NOTHING;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION edge_delivery_outbox_insert_lane_marker()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO edge_delivery_outbox_lanes (stream_id, head_item_id, head_sequence)
    SELECT DISTINCT ON (target_user_id) target_user_id, id, id
    FROM new_rows
    ORDER BY target_user_id, id
    ON CONFLICT (stream_id) DO NOTHING;
    RETURN NULL;
END;
$$;

DROP FUNCTION outbox_lane_advisory_key(text, bigint);
