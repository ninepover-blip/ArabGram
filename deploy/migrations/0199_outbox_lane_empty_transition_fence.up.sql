-- A producer may have inserted an outbox item but not committed yet after its
-- lane-marker INSERT observed the still-live old marker and did nothing.  The
-- finalizer cannot see that item in READ COMMITTED and must not delete the old
-- marker until that producer either commits or observes the deletion.
CREATE FUNCTION outbox_lane_advisory_key(queue_scope text, stream_id bigint)
RETURNS bigint
LANGUAGE sql
IMMUTABLE
STRICT
PARALLEL SAFE
AS $$
    SELECT hashtextextended('telesrv:outbox-lane:' || queue_scope || ':' || stream_id::text, 0)
$$;

CREATE OR REPLACE FUNCTION dispatch_outbox_insert_lane_marker()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_advisory_xact_lock_shared(
        outbox_lane_advisory_key('account_pts', streams.target_user_id)
    )
    FROM (
        SELECT DISTINCT target_user_id
        FROM new_rows
        ORDER BY target_user_id
    ) AS streams;

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
    PERFORM pg_advisory_xact_lock_shared(
        outbox_lane_advisory_key('account_absolute', streams.target_user_id)
    )
    FROM (
        SELECT DISTINCT target_user_id
        FROM new_rows
        ORDER BY target_user_id
    ) AS streams;

    INSERT INTO edge_delivery_outbox_lanes (stream_id, head_item_id, head_sequence)
    SELECT DISTINCT ON (target_user_id) target_user_id, id, id
    FROM new_rows
    ORDER BY target_user_id, id
    ON CONFLICT (stream_id) DO NOTHING;
    RETURN NULL;
END;
$$;
