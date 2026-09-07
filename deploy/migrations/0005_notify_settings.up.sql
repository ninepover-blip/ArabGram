-- per-scope 通知设置（每用户多行）：具体 peer / forum 话题 / 三类全局默认
-- （users/chats/broadcasts）。此前 account.get/update/resetNotifySettings 为回显
-- stub（不持久化），mute 重启即丢、dialog 列表静音状态不正确。本表落地真实持久化。
-- 可空 bool/int 列表示“该项未设置=按所属类别继承默认”（与 TL flag-optional 一致）。
CREATE TABLE public.notify_settings (
    owner_user_id bigint NOT NULL,
    scope_kind text NOT NULL,
    peer_type text DEFAULT ''::text NOT NULL,
    peer_id bigint DEFAULT 0 NOT NULL,
    topic_id integer DEFAULT 0 NOT NULL,
    show_previews boolean,
    silent boolean,
    mute_until integer,
    stories_muted boolean,
    stories_hide_sender boolean,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT notify_settings_scope_kind_check CHECK ((scope_kind = ANY (ARRAY['peer'::text, 'users'::text, 'chats'::text, 'broadcasts'::text])))
);

ALTER TABLE ONLY public.notify_settings
    ADD CONSTRAINT notify_settings_pkey PRIMARY KEY (owner_user_id, scope_kind, peer_type, peer_id, topic_id);

-- dialog 列表批量读：按 owner + 一组 peer 取整-peer（topic 0）设置。
CREATE INDEX notify_settings_owner_peer_idx ON public.notify_settings USING btree (owner_user_id, peer_type, peer_id) WHERE ((scope_kind = 'peer'::text) AND (topic_id = 0));
