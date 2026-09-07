-- Plain private sends already write one durable dispatch row per owner PTS.
-- Carry the exact rebuildable dialog-cache key on that row and stop performing
-- two contended read_model_versions upserts in the business transaction.

ALTER TABLE public.dispatch_outbox
    ADD COLUMN read_model_peer_type varchar(16) NOT NULL DEFAULT '',
    ADD COLUMN read_model_peer_id bigint NOT NULL DEFAULT 0,
    ADD CONSTRAINT dispatch_outbox_read_model_peer_check CHECK (
        (read_model_peer_type = '' AND read_model_peer_id = 0)
        OR (read_model_peer_type IN ('user', 'channel') AND read_model_peer_id > 0)
    );

CREATE OR REPLACE FUNCTION public.telesrv_notify_dialog_light_read_model()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    owner_id bigint;
    peer_type text;
    peer_id bigint;
BEGIN
    IF current_setting('telesrv.defer_dialog_light', true) = 'on' THEN
        RETURN NULL;
    END IF;

    IF TG_OP = 'DELETE' THEN
        owner_id := OLD.user_id;
        peer_type := OLD.peer_type;
        peer_id := OLD.peer_id;
    ELSE
        owner_id := NEW.user_id;
        peer_type := NEW.peer_type;
        peer_id := NEW.peer_id;
    END IF;

    PERFORM public.telesrv_bump_dialog_light(owner_id, peer_type, peer_id);
    RETURN NULL;
END;
$$;
