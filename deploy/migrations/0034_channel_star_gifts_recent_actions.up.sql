-- New channel Star gifts are saved-gift/admin-log records, not channel history
-- messages, so channel gifts may have msg_id = 0. The >= 0 branch keeps older
-- rows migratable without rewriting already persisted data.

ALTER TABLE public.peer_star_gifts
    DROP CONSTRAINT IF EXISTS peer_star_gifts_ref_check;

ALTER TABLE public.peer_star_gifts
    ADD CONSTRAINT peer_star_gifts_ref_check CHECK (
        (owner_peer_type = 'user' AND msg_id > 0 AND saved_id = 0)
        OR
        (owner_peer_type = 'channel' AND msg_id >= 0 AND saved_id > 0)
    );
