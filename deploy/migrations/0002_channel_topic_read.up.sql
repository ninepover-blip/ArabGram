-- per-(user, channel, topic) 独立已读水位。
-- 背景：forum 话题已读此前被频道级单一水位 channel_members.read_inbox_max_id 承载，
-- 读一个 topic 会污染其它 topic 的未读现算（reply_to_top_id 过滤 + id > 频道级水位）。
-- channel_forum_topics 那几个 read_*_max_id 列无 user 维度（PK=channel_id,topic_id），
-- 无法承载 per-viewer 已读；故新建本表，topic_id=1 表示 General。
CREATE TABLE public.channel_topic_read (
    channel_id bigint NOT NULL,
    user_id bigint NOT NULL,
    topic_id integer NOT NULL,
    read_inbox_max_id integer DEFAULT 0 NOT NULL,
    read_outbox_max_id integer DEFAULT 0 NOT NULL,
    read_inbox_date integer DEFAULT 0 NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT channel_topic_read_pkey PRIMARY KEY (channel_id, user_id, topic_id),
    CONSTRAINT channel_topic_read_topic_check CHECK (topic_id > 0),
    CONSTRAINT channel_topic_read_channel_fkey FOREIGN KEY (channel_id)
        REFERENCES public.channels (id) ON DELETE CASCADE
);

-- outbox 回执反查：某 topic 内某 viewer 自己消息被读到哪 = 该 topic 其他成员 read_inbox_max_id 的最大值。
-- (channel_id, topic_id, read_inbox_max_id DESC) 让 MAX 走 index-only，避免每帧全表聚合。
CREATE INDEX channel_topic_read_outbox_idx
    ON public.channel_topic_read (channel_id, topic_id, read_inbox_max_id DESC);
