-- dialog_light changes are on every private-message transaction. PostgreSQL
-- NOTIFY serializes all committing writers on one database-object lock, so the
-- exact durable version remains here while cross-process cache transport moves
-- to the bounded Core -> Redis relay.
ALTER TABLE public.read_model_versions
    ADD COLUMN published_version bigint,
    ADD COLUMN publish_owner varchar(255) NOT NULL DEFAULT '',
    ADD COLUMN publish_lease_until timestamptz;

UPDATE public.read_model_versions
SET published_version = version
WHERE published_version IS NULL;

ALTER TABLE public.read_model_versions
    ALTER COLUMN published_version SET NOT NULL,
    ALTER COLUMN published_version SET DEFAULT 0,
    ADD CONSTRAINT read_model_versions_publish_check CHECK (
        published_version >= 0 AND published_version <= version
        AND ((publish_owner = '') = (publish_lease_until IS NULL))
    );

CREATE INDEX read_model_versions_dialog_publish_idx
    ON public.read_model_versions (publish_lease_until, updated_at, owner_user_id, peer_type, peer_id)
    WHERE model = 'dialog_light' AND published_version < version;

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

    INSERT INTO public.read_model_versions (
        model, owner_user_id, peer_type, peer_id, version, hash, updated_at
    ) VALUES (
        'dialog_light', p_owner_user_id, p_peer_type, p_peer_id,
        1, public.telesrv_random_read_model_hash(), now()
    )
    ON CONFLICT (model, owner_user_id, peer_type, peer_id) DO UPDATE SET
        version = read_model_versions.version + 1,
        hash = public.telesrv_random_read_model_hash(),
        updated_at = EXCLUDED.updated_at;
END;
$$;
