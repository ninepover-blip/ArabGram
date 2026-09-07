CREATE FUNCTION public.dispatch_outbox_attempts_notify_finalize()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE ready_shard smallint;
BEGIN
    FOR ready_shard IN
        SELECT DISTINCT l.logical_shard
        FROM new_rows a
        JOIN public.dispatch_outbox_lanes l ON l.stream_id = a.stream_id
        WHERE a.resolution IS NOT NULL AND a.finalized_at IS NULL
    LOOP
        PERFORM pg_notify('telesrv_dispatch_outbox_finalize', ready_shard::text);
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER dispatch_outbox_attempts_finalize_notify
AFTER UPDATE ON public.dispatch_outbox_attempts
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.dispatch_outbox_attempts_notify_finalize();

CREATE FUNCTION public.dispatch_outbox_attempts_notify_deadline()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE ready_shard smallint;
BEGIN
    FOR ready_shard IN
        SELECT DISTINCT l.logical_shard
        FROM new_rows a
        JOIN public.dispatch_outbox_lanes l ON l.stream_id = a.stream_id
        WHERE a.resolution IS NULL AND a.finalized_at IS NULL
    LOOP
        PERFORM pg_notify('telesrv_dispatch_outbox_ready', ready_shard::text);
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER dispatch_outbox_attempts_deadline_notify
AFTER INSERT ON public.dispatch_outbox_attempts
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.dispatch_outbox_attempts_notify_deadline();

CREATE FUNCTION public.edge_delivery_outbox_attempts_notify_finalize()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE ready_shard smallint;
BEGIN
    FOR ready_shard IN
        SELECT DISTINCT l.logical_shard
        FROM new_rows a
        JOIN public.edge_delivery_outbox_lanes l ON l.stream_id = a.stream_id
        WHERE a.resolution IS NOT NULL AND a.finalized_at IS NULL
    LOOP
        PERFORM pg_notify('telesrv_edge_delivery_outbox_finalize', ready_shard::text);
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER edge_delivery_outbox_attempts_finalize_notify
AFTER UPDATE ON public.edge_delivery_outbox_attempts
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.edge_delivery_outbox_attempts_notify_finalize();

CREATE FUNCTION public.edge_delivery_outbox_attempts_notify_deadline()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE ready_shard smallint;
BEGIN
    FOR ready_shard IN
        SELECT DISTINCT l.logical_shard
        FROM new_rows a
        JOIN public.edge_delivery_outbox_lanes l ON l.stream_id = a.stream_id
        WHERE a.resolution IS NULL AND a.finalized_at IS NULL
    LOOP
        PERFORM pg_notify('telesrv_edge_delivery_outbox_ready', ready_shard::text);
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER edge_delivery_outbox_attempts_deadline_notify
AFTER INSERT ON public.edge_delivery_outbox_attempts
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.edge_delivery_outbox_attempts_notify_deadline();

CREATE FUNCTION public.channel_delivery_attempts_notify_finalize()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE ready_shard smallint;
BEGIN
    FOR ready_shard IN
        SELECT DISTINCT l.logical_shard
        FROM new_rows a
        JOIN public.channel_delivery_lanes l ON l.channel_id = a.channel_id
        WHERE a.finalized_at IS NULL AND a.resolution <> 'pending'
    LOOP
        PERFORM pg_notify('telesrv_channel_delivery_finalize', ready_shard::text);
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER channel_delivery_attempts_finalize_notify
AFTER UPDATE ON public.channel_delivery_attempts
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.channel_delivery_attempts_notify_finalize();

CREATE FUNCTION public.channel_delivery_attempts_notify_deadline()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE ready_shard smallint;
BEGIN
    FOR ready_shard IN
        SELECT DISTINCT l.logical_shard
        FROM new_rows a
        JOIN public.channel_delivery_lanes l ON l.channel_id = a.channel_id
        WHERE a.finalized_at IS NULL AND a.resolution = 'pending'
    LOOP
        PERFORM pg_notify('telesrv_channel_delivery_ready', ready_shard::text);
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER channel_delivery_attempts_deadline_notify
AFTER INSERT ON public.channel_delivery_attempts
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.channel_delivery_attempts_notify_deadline();
