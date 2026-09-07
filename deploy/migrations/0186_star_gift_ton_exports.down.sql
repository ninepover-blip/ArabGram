-- Never erase evidence after a transaction may have reached TON. Restore a
-- pre-upgrade backup instead of forcing this downgrade.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.star_gift_ton_assets) OR EXISTS (
        SELECT 1 FROM public.star_gift_ton_exports
        WHERE status IN ('submitted','confirmed','finalized','quarantined')
    ) THEN
        RAISE EXCEPTION 'cannot downgrade TON Star Gift exports after chain submission; restore a pre-upgrade backup';
    END IF;
END $$;

DROP TRIGGER IF EXISTS star_gift_offer_externalization_guard ON public.star_gift_offers;
DROP FUNCTION IF EXISTS public.telesrv_guard_star_gift_offer_externalization();

CREATE OR REPLACE FUNCTION public.telesrv_guard_star_gift_listing() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    gift_owner_type text;
    gift_owner_id bigint;
    gift_burned boolean;
BEGIN
    SELECT owner_peer_type, owner_peer_id, burned
      INTO gift_owner_type, gift_owner_id, gift_burned
      FROM public.unique_star_gifts WHERE id=NEW.unique_gift_id FOR SHARE;
    IF gift_burned OR gift_owner_type IS DISTINCT FROM NEW.seller_peer_type OR gift_owner_id IS DISTINCT FROM NEW.seller_peer_id THEN
        RAISE EXCEPTION 'star gift listing owner/state mismatch';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS unique_star_gifts_externalization_guard ON public.unique_star_gifts;
DROP FUNCTION IF EXISTS public.telesrv_guard_star_gift_externalization();

DROP TABLE IF EXISTS public.star_gift_ton_profile_links;
DROP TABLE IF EXISTS public.star_gift_ton_claims;
DROP TABLE IF EXISTS public.star_gift_ton_jobs;
DROP TABLE IF EXISTS public.star_gift_ton_assets;
DROP TABLE IF EXISTS public.star_gift_ton_exports;
DROP TABLE IF EXISTS public.star_gift_ton_collection_cursors;

ALTER TABLE public.unique_star_gifts
    DROP CONSTRAINT IF EXISTS unique_star_gifts_externalization_check,
    DROP COLUMN IF EXISTS externalization_pending;
