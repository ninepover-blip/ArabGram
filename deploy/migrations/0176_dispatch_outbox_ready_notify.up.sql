-- Wake dedicated Egress workers when durable outbox user heads become ready.
-- The notification is statement-level and shard-scoped to avoid per-recipient
-- NOTIFY storms during large fanout transactions. The outbox table remains the
-- durable queue of record; this channel is only a low-latency wake path.

CREATE FUNCTION dispatch_outbox_notify_ready_heads()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    ready_shard smallint;
BEGIN
    IF TG_OP = 'INSERT' OR TG_OP = 'UPDATE' THEN
        FOR ready_shard IN
            SELECT DISTINCT h.logical_shard
            FROM new_rows r
            JOIN dispatch_outbox_user_heads h
              ON h.target_user_id = r.target_user_id
             AND h.head_id = r.id
            WHERE h.status = 'pending'
              AND h.next_attempt_at <= now()
        LOOP
            PERFORM pg_notify('telesrv_dispatch_outbox_ready', ready_shard::text);
        END LOOP;
        RETURN NULL;
    END IF;

    FOR ready_shard IN
        SELECT DISTINCT h.logical_shard
        FROM old_rows r
        JOIN dispatch_outbox_user_heads h
          ON h.target_user_id = r.target_user_id
         AND h.head_id <> r.id
        WHERE h.status = 'pending'
          AND h.next_attempt_at <= now()
    LOOP
        PERFORM pg_notify('telesrv_dispatch_outbox_ready', ready_shard::text);
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER dispatch_outbox_insert_ready_notify
AFTER INSERT ON dispatch_outbox
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION dispatch_outbox_notify_ready_heads();

CREATE TRIGGER dispatch_outbox_update_ready_notify
AFTER UPDATE ON dispatch_outbox
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION dispatch_outbox_notify_ready_heads();

CREATE TRIGGER dispatch_outbox_delete_ready_notify
AFTER DELETE ON dispatch_outbox
REFERENCING OLD TABLE AS old_rows
FOR EACH STATEMENT
EXECUTE FUNCTION dispatch_outbox_notify_ready_heads();
