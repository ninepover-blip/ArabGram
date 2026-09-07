DROP INDEX IF EXISTS public.peer_star_gifts_owner_idx;
DROP INDEX IF EXISTS public.peer_star_gifts_user_msg_uniq;
DROP INDEX IF EXISTS public.peer_star_gifts_channel_saved_uniq;

ALTER TABLE public.peer_star_gifts
    DROP CONSTRAINT IF EXISTS peer_star_gifts_ref_check,
    DROP CONSTRAINT IF EXISTS peer_star_gifts_owner_peer_type_check;

DELETE FROM public.peer_star_gifts WHERE owner_peer_type <> 'user';

ALTER TABLE public.peer_star_gifts
    DROP COLUMN IF EXISTS saved_id,
    DROP COLUMN IF EXISTS owner_peer_type;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'peer_star_gifts'
          AND column_name = 'owner_peer_id'
    ) THEN
        ALTER TABLE public.peer_star_gifts RENAME COLUMN owner_peer_id TO owner_user_id;
    END IF;
END $$;

ALTER TABLE public.peer_star_gifts
    ADD CONSTRAINT user_star_gifts_owner_msg_uniq UNIQUE (owner_user_id, msg_id);

ALTER TABLE public.peer_star_gifts RENAME TO user_star_gifts;

CREATE INDEX user_star_gifts_owner_idx
    ON public.user_star_gifts (owner_user_id, gift_date DESC, id DESC);
