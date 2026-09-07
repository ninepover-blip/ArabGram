-- The owner -> collectible phone projection is stable but participates in every
-- user hydration. Emit exact old/new owner invalidations so Core and Egress may
-- keep bounded positive and negative in-memory caches without stale ownership.

CREATE OR REPLACE FUNCTION public.telesrv_notify_collectible_phone_read_model()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    old_owner_id bigint := 0;
    new_owner_id bigint := 0;
BEGIN
    IF TG_OP <> 'INSERT' AND OLD.status = 'owned' THEN
        old_owner_id := OLD.owner_user_id;
    END IF;
    IF TG_OP <> 'DELETE' AND NEW.status = 'owned' THEN
        new_owner_id := NEW.owner_user_id;
    END IF;

    IF old_owner_id > 0 THEN
        PERFORM public.telesrv_bump_read_model_version(
            'collectible_phone', 0, 'user', old_owner_id
        );
    END IF;
    IF new_owner_id > 0 AND new_owner_id IS DISTINCT FROM old_owner_id THEN
        PERFORM public.telesrv_bump_read_model_version(
            'collectible_phone', 0, 'user', new_owner_id
        );
    END IF;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS collectible_phones_read_model_changed ON public.collectible_phones;
CREATE TRIGGER collectible_phones_read_model_changed
AFTER INSERT OR UPDATE OR DELETE ON public.collectible_phones
FOR EACH ROW
EXECUTE FUNCTION public.telesrv_notify_collectible_phone_read_model();
