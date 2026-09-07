CREATE TABLE IF NOT EXISTS public.peer_usernames (
    username_lower text NOT NULL,
    peer_type text NOT NULL,
    peer_id bigint NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT peer_usernames_pkey PRIMARY KEY (username_lower),
    CONSTRAINT peer_usernames_nonempty_check CHECK (username_lower <> ''),
    CONSTRAINT peer_usernames_peer_id_check CHECK (peer_id <> 0),
    CONSTRAINT peer_usernames_peer_type_check CHECK (peer_type IN ('user', 'channel'))
);

CREATE UNIQUE INDEX IF NOT EXISTS peer_usernames_peer_unique_idx
    ON public.peer_usernames (peer_type, peer_id);

INSERT INTO public.peer_usernames (username_lower, peer_type, peer_id)
SELECT lower(username), 'user', id
FROM public.users
WHERE username <> '';

INSERT INTO public.peer_usernames (username_lower, peer_type, peer_id)
SELECT lower(username), 'channel', id
FROM public.channels
WHERE NOT deleted AND COALESCE(username, '') <> '';

CREATE OR REPLACE FUNCTION public.delete_user_peer_username() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    DELETE FROM public.peer_usernames WHERE peer_type = 'user' AND peer_id = OLD.id;
    RETURN OLD;
END;
$$;

CREATE OR REPLACE FUNCTION public.delete_channel_peer_username() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    DELETE FROM public.peer_usernames WHERE peer_type = 'channel' AND peer_id = OLD.id;
    RETURN OLD;
END;
$$;

CREATE TRIGGER users_delete_peer_username
    AFTER DELETE ON public.users
    FOR EACH ROW EXECUTE FUNCTION public.delete_user_peer_username();

CREATE TRIGGER channels_delete_peer_username
    AFTER DELETE ON public.channels
    FOR EACH ROW EXECUTE FUNCTION public.delete_channel_peer_username();

DROP TABLE IF EXISTS public.channel_usernames;
