-- A user's owned collectible number is viewer-independent but projected on
-- every User-bearing RPC. Version both positive and negative facts so response
-- hydration can reuse them without a query per page.

CREATE OR REPLACE FUNCTION public.telesrv_bump_user_collectible_phone(
    p_user_id bigint
) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF COALESCE(p_user_id, 0) = 0 THEN
        RETURN;
    END IF;
    PERFORM public.telesrv_bump_read_model_version(
        'user_collectible_phone', 0, 'user', p_user_id
    );
END;
$$;

CREATE OR REPLACE FUNCTION public.telesrv_notify_user_collectible_phone_row()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM public.telesrv_bump_user_collectible_phone(OLD.owner_user_id);
        RETURN OLD;
    END IF;

    IF TG_OP = 'UPDATE'
       AND OLD.owner_user_id IS DISTINCT FROM NEW.owner_user_id THEN
        PERFORM public.telesrv_bump_user_collectible_phone(OLD.owner_user_id);
    END IF;
    PERFORM public.telesrv_bump_user_collectible_phone(NEW.owner_user_id);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS collectible_phones_user_read_model_changed ON public.collectible_phones;
CREATE TRIGGER collectible_phones_user_read_model_changed
AFTER INSERT OR UPDATE OR DELETE ON public.collectible_phones
FOR EACH ROW EXECUTE FUNCTION public.telesrv_notify_user_collectible_phone_row();

CREATE OR REPLACE FUNCTION public.telesrv_maintain_user_projection_fact_versions()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        DELETE FROM public.read_model_versions
        WHERE owner_user_id = 0
          AND peer_type = 'user'
          AND peer_id = OLD.id
          AND model IN ('user_visibility', 'user_collectible_phone');
        RETURN OLD;
    END IF;
    PERFORM public.telesrv_bump_read_model_version(
        'user_visibility', 0, 'user', NEW.id
    );
    PERFORM public.telesrv_bump_read_model_version(
        'user_collectible_phone', 0, 'user', NEW.id
    );
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS users_projection_fact_versions_changed ON public.users;
CREATE TRIGGER users_projection_fact_versions_changed
AFTER INSERT OR DELETE ON public.users
FOR EACH ROW EXECUTE FUNCTION public.telesrv_maintain_user_projection_fact_versions();

-- Seed every durable user, including users with no collectible number. A zero
-- result is then a versioned fact rather than a TTL-based guess.
INSERT INTO public.read_model_versions (
    model, owner_user_id, peer_type, peer_id, version, hash, updated_at
)
SELECT 'user_collectible_phone', 0, 'user', u.id, 1,
       public.telesrv_random_read_model_hash(), now()
FROM public.users u
ON CONFLICT (model, owner_user_id, peer_type, peer_id) DO NOTHING;

-- user_visibility previously existed only for users that had already been
-- frozen. Seed absence too, otherwise the overwhelmingly common unfrozen fact
-- cannot participate in a versioned negative cache.
INSERT INTO public.read_model_versions (
    model, owner_user_id, peer_type, peer_id, version, hash, updated_at
)
SELECT 'user_visibility', 0, 'user', u.id, 1,
       public.telesrv_random_read_model_hash(), now()
FROM public.users u
ON CONFLICT (model, owner_user_id, peer_type, peer_id) DO NOTHING;
