CREATE FUNCTION public.dispatch_outbox_lanes_notify_ready()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE ready_shard smallint;
BEGIN
    FOR ready_shard IN
        SELECT DISTINCT logical_shard
        FROM new_rows
        WHERE state = 'ready' AND ready_at <= now()
    LOOP
        PERFORM pg_notify('telesrv_dispatch_outbox_ready', ready_shard::text);
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER dispatch_outbox_lanes_insert_ready_notify
AFTER INSERT ON public.dispatch_outbox_lanes
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.dispatch_outbox_lanes_notify_ready();

CREATE TRIGGER dispatch_outbox_lanes_update_ready_notify
AFTER UPDATE ON public.dispatch_outbox_lanes
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.dispatch_outbox_lanes_notify_ready();

CREATE FUNCTION public.edge_delivery_outbox_lanes_notify_ready()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE ready_shard smallint;
BEGIN
    FOR ready_shard IN
        SELECT DISTINCT logical_shard
        FROM new_rows
        WHERE state = 'ready' AND ready_at <= now()
    LOOP
        PERFORM pg_notify('telesrv_edge_delivery_outbox_ready', ready_shard::text);
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER edge_delivery_outbox_lanes_insert_ready_notify
AFTER INSERT ON public.edge_delivery_outbox_lanes
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.edge_delivery_outbox_lanes_notify_ready();

CREATE TRIGGER edge_delivery_outbox_lanes_update_ready_notify
AFTER UPDATE ON public.edge_delivery_outbox_lanes
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.edge_delivery_outbox_lanes_notify_ready();

CREATE FUNCTION public.channel_delivery_lanes_notify_ready()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE ready_shard smallint;
BEGIN
    FOR ready_shard IN
        SELECT DISTINCT logical_shard
        FROM new_rows
        WHERE state = 'ready' AND ready_at <= now()
    LOOP
        PERFORM pg_notify('telesrv_channel_delivery_ready', ready_shard::text);
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER channel_delivery_lanes_insert_ready_notify
AFTER INSERT ON public.channel_delivery_lanes
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.channel_delivery_lanes_notify_ready();

CREATE TRIGGER channel_delivery_lanes_update_ready_notify
AFTER UPDATE ON public.channel_delivery_lanes
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION public.channel_delivery_lanes_notify_ready();
