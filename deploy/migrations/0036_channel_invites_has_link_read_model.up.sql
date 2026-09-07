CREATE INDEX IF NOT EXISTS channel_invites_channel_active_idx
    ON public.channel_invites (channel_id)
    WHERE NOT revoked;

CREATE OR REPLACE FUNCTION public.telesrv_notify_channel_invites_channel_base_read_model() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
    changed_id BIGINT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        changed_id := OLD.channel_id;
    ELSE
        changed_id := NEW.channel_id;
    END IF;
    PERFORM telesrv_bump_read_model_version('channel_base', 0, 'channel', changed_id);
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS channel_invites_channel_base_read_model_changed ON public.channel_invites;
CREATE TRIGGER channel_invites_channel_base_read_model_changed
AFTER INSERT OR DELETE OR UPDATE OF channel_id, revoked ON public.channel_invites
FOR EACH ROW EXECUTE FUNCTION public.telesrv_notify_channel_invites_channel_base_read_model();

WITH missing AS (
    SELECT
        c.id AS channel_id,
        COALESCE((
            SELECT MAX(ci.invite_id) + 1
            FROM public.channel_invites ci
            WHERE ci.channel_id = c.id
        ), floor(random() * 9007199254740991)::bigint + 1) AS invite_id,
        'automain_' || c.id::text || '_' || substr(md5(c.id::text || ':' || clock_timestamp()::text || ':' || random()::text), 1, 16) AS hash,
        c.creator_user_id AS admin_user_id
    FROM public.channels c
    JOIN public.users u ON u.id = c.creator_user_id
    WHERE NOT c.deleted
      AND c.creator_user_id <> 0
      AND NOT EXISTS (
          SELECT 1
          FROM public.channel_invites ci
          WHERE ci.channel_id = c.id
            AND NOT ci.revoked
      )
), inserted AS (
    INSERT INTO public.channel_invites (
        channel_id, invite_id, hash, admin_user_id, permanent, revoked, request_needed, created_at, updated_at
    )
    SELECT channel_id, invite_id, hash, admin_user_id, true, false, false, now(), now()
    FROM missing
    ON CONFLICT (channel_id, invite_id) DO NOTHING
    RETURNING channel_id, invite_id, hash
)
INSERT INTO public.channel_invite_hashes (hash, channel_id, invite_id)
SELECT hash, channel_id, invite_id
FROM inserted
ON CONFLICT (hash) DO UPDATE SET
    channel_id = EXCLUDED.channel_id,
    invite_id = EXCLUDED.invite_id,
    updated_at = now();
