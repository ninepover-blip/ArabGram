ALTER TABLE public.peer_star_gifts
    DROP CONSTRAINT IF EXISTS peer_star_gifts_ref_check;

UPDATE public.peer_star_gifts
SET msg_id = saved_id::integer
WHERE owner_peer_type = 'channel'
  AND msg_id = 0
  AND saved_id > 0
  AND saved_id <= 2147483647;

ALTER TABLE public.peer_star_gifts
    ADD CONSTRAINT peer_star_gifts_ref_check CHECK (
        (owner_peer_type = 'user' AND msg_id > 0 AND saved_id = 0)
        OR
        (owner_peer_type = 'channel' AND msg_id > 0 AND saved_id > 0)
    );
