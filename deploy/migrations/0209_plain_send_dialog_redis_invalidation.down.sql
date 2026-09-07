CREATE OR REPLACE FUNCTION public.telesrv_notify_dialog_light_read_model()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    owner_id bigint;
    peer_type text;
    peer_id bigint;
BEGIN
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

ALTER TABLE public.dispatch_outbox
    DROP CONSTRAINT IF EXISTS dispatch_outbox_read_model_peer_check,
    DROP COLUMN IF EXISTS read_model_peer_id,
    DROP COLUMN IF EXISTS read_model_peer_type;
