ALTER TABLE public.message_boxes
    DROP CONSTRAINT message_boxes_owner_user_id_private_message_id_key;
ALTER TABLE public.message_boxes
    ADD CONSTRAINT message_boxes_owner_user_id_private_message_id_key
    UNIQUE (owner_user_id, private_message_id);

CREATE INDEX message_boxes_private_lookup_idx
    ON public.message_boxes USING btree (private_message_id, owner_user_id);

CREATE INDEX message_boxes_reply_lookup_idx
    ON public.message_boxes USING btree (
        owner_user_id, peer_type, peer_id, box_id
    ) WHERE NOT deleted;
