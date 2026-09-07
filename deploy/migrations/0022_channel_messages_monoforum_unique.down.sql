DROP INDEX IF EXISTS channel_messages_random_idx;
CREATE UNIQUE INDEX channel_messages_random_idx ON public.channel_messages
    USING btree (channel_id, sender_user_id, random_id) WHERE (random_id <> 0);
