DROP INDEX IF EXISTS channel_messages_monoforum_sublist_idx;
ALTER TABLE public.channel_messages DROP COLUMN IF EXISTS saved_peer_id;
ALTER TABLE public.channel_messages DROP COLUMN IF EXISTS saved_peer_type;
