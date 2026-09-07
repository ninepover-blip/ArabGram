ALTER TABLE public.channel_messages DROP COLUMN IF EXISTS grouped_id;
ALTER TABLE public.message_boxes DROP COLUMN IF EXISTS grouped_id;
ALTER TABLE public.private_messages DROP COLUMN IF EXISTS grouped_id;
