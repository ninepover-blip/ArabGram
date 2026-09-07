CREATE OR REPLACE FUNCTION public.telesrv_bump_dialog_light(
    p_owner_user_id bigint,
    p_peer_type text,
    p_peer_id bigint
) RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    IF COALESCE(p_owner_user_id, 0) = 0
       OR COALESCE(p_peer_id, 0) = 0
       OR COALESCE(p_peer_type, '') = '' THEN
        RETURN;
    END IF;

    PERFORM public.telesrv_bump_read_model_version(
        'dialog_light', p_owner_user_id, p_peer_type, p_peer_id
    );
END;
$$;

DROP INDEX IF EXISTS public.read_model_versions_dialog_publish_idx;

ALTER TABLE public.read_model_versions
    DROP CONSTRAINT IF EXISTS read_model_versions_publish_check,
    DROP COLUMN IF EXISTS publish_lease_until,
    DROP COLUMN IF EXISTS publish_owner,
    DROP COLUMN IF EXISTS published_version;
