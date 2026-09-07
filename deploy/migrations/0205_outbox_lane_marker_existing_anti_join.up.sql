-- Existing lanes are mutable consumer heads. INSERT ... ON CONFLICT against
-- one waits for an uncommitted HOT update even though the producer intends to
-- do nothing. Filter committed existing markers first; ON CONFLICT remains
-- only for the rare concurrent missing-marker race.
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

    WITH candidates AS MATERIALIZED (
        SELECT DISTINCT ON (target_user_id)
               target_user_id AS stream_id, id AS head_item_id, pts AS head_sequence
        FROM new_rows
        ORDER BY target_user_id, pts, id
    )
    INSERT INTO public.dispatch_outbox_lanes (stream_id, head_item_id, head_sequence)
    SELECT c.stream_id, c.head_item_id, c.head_sequence
    FROM candidates c
    WHERE NOT EXISTS (
        SELECT 1
        FROM public.dispatch_outbox_lanes existing
        WHERE existing.stream_id = c.stream_id
    )
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

    WITH candidates AS MATERIALIZED (
        SELECT DISTINCT ON (target_user_id)
               target_user_id AS stream_id, id AS head_item_id, id AS head_sequence
        FROM new_rows
        ORDER BY target_user_id, id
    )
    INSERT INTO public.edge_delivery_outbox_lanes (stream_id, head_item_id, head_sequence)
    SELECT c.stream_id, c.head_item_id, c.head_sequence
    FROM candidates c
    WHERE NOT EXISTS (
        SELECT 1
        FROM public.edge_delivery_outbox_lanes existing
        WHERE existing.stream_id = c.stream_id
    )
    ON CONFLICT (stream_id) DO NOTHING;
    RETURN NULL;
END;
$$;
