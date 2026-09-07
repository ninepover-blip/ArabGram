CREATE OR REPLACE FUNCTION public.dispatch_outbox_insert_lane_marker()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock_shared(
        public.outbox_lane_advisory_key('account_pts', streams.target_user_id)
    )
    FROM (
        SELECT DISTINCT target_user_id
        FROM new_rows
        ORDER BY target_user_id
    ) AS streams;

    INSERT INTO public.dispatch_outbox_lanes (stream_id, head_item_id, head_sequence)
    SELECT DISTINCT ON (target_user_id) target_user_id, id, pts
    FROM new_rows
    ORDER BY target_user_id, pts, id
    ON CONFLICT (stream_id) DO NOTHING;
    RETURN NULL;
END;
$$;

CREATE OR REPLACE FUNCTION public.edge_delivery_outbox_insert_lane_marker()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM pg_advisory_xact_lock_shared(
        public.outbox_lane_advisory_key('account_absolute', streams.target_user_id)
    )
    FROM (
        SELECT DISTINCT target_user_id
        FROM new_rows
        ORDER BY target_user_id
    ) AS streams;

    INSERT INTO public.edge_delivery_outbox_lanes (stream_id, head_item_id, head_sequence)
    SELECT DISTINCT ON (target_user_id) target_user_id, id, id
    FROM new_rows
    ORDER BY target_user_id, id
    ON CONFLICT (stream_id) DO NOTHING;
    RETURN NULL;
END;
$$;
