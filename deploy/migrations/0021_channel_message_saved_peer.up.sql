-- 频道私信(monoforum):私信消息存进 channel_messages(复用 channel pts/事件/difference),
-- 用 saved_peer 维度按订阅者分子会话。普通频道消息 saved_peer 为空,部分索引仅覆盖 monoforum 行。
ALTER TABLE public.channel_messages
    ADD COLUMN saved_peer_type character varying(16) DEFAULT ''::character varying NOT NULL;
ALTER TABLE public.channel_messages
    ADD COLUMN saved_peer_id bigint DEFAULT 0 NOT NULL;
CREATE INDEX channel_messages_monoforum_sublist_idx ON public.channel_messages
    USING btree (channel_id, saved_peer_id, id DESC)
    WHERE ((saved_peer_id <> 0) AND (NOT deleted));
