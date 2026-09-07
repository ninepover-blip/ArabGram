-- Hard-cut index consolidation. Services must be stopped while the unique
-- constraint is rebuilt; the transaction restores the prior schema on any
-- failure and there is no dual-index compatibility mode.
--
-- PostgreSQL btrees scan in both directions, so these two partial indexes are
-- physically equivalent despite opposite display directions.
DROP INDEX public.message_boxes_reply_lookup_idx;

-- Pair uniqueness is order-independent. Make private_message_id the leading
-- key so this one unique index also serves private-message -> owner lookups,
-- replacing the separate non-unique reversed copy.
ALTER TABLE public.message_boxes
    DROP CONSTRAINT message_boxes_owner_user_id_private_message_id_key;
DROP INDEX public.message_boxes_private_lookup_idx;
ALTER TABLE public.message_boxes
    ADD CONSTRAINT message_boxes_owner_user_id_private_message_id_key
    UNIQUE (private_message_id, owner_user_id);
