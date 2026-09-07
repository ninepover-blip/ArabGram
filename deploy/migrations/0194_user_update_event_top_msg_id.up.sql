-- Persist the forum topic identity carried by readChannelDiscussion updates.
-- max_id remains the topic-local read watermark; the two values are not
-- interchangeable and both must survive online dispatch/offline difference.
ALTER TABLE public.user_update_events
  ADD COLUMN top_msg_id integer DEFAULT 0 NOT NULL,
  ADD CONSTRAINT user_update_events_top_msg_id_nonnegative CHECK (top_msg_id >= 0);
