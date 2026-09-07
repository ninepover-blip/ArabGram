CREATE TABLE IF NOT EXISTS public.channel_usernames (
    username_lower text NOT NULL,
    channel_id bigint NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT channel_usernames_nonempty_check CHECK (username_lower <> '')
);

ALTER TABLE ONLY public.channel_usernames
    ADD CONSTRAINT channel_usernames_pkey PRIMARY KEY (username_lower);

INSERT INTO public.channel_usernames (username_lower, channel_id)
SELECT username_lower, peer_id
FROM public.peer_usernames
WHERE peer_type = 'channel';

DROP TRIGGER IF EXISTS users_delete_peer_username ON public.users;
DROP TRIGGER IF EXISTS channels_delete_peer_username ON public.channels;
DROP FUNCTION IF EXISTS public.delete_user_peer_username();
DROP FUNCTION IF EXISTS public.delete_channel_peer_username();
DROP TABLE IF EXISTS public.peer_usernames;
