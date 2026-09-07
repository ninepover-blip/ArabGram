-- 频道链接预览异步回填（updateChannelWebPage）新增 channel_update_events 事件类型
-- 'channel_web_page'：频道消息的 pending 链接预览被带外解析后，经 EditChannelMessage 的
-- WebPageResolve 模式就地替换并 append 一条 channel_web_page durable 事件。0001 的白名单未含它，
-- 真 Postgres 上 append 会被 CHECK 约束拒绝（SQLSTATE 23514）；memory store 无约束故单测未暴露
-- （同 0015/0003 教训）。
ALTER TABLE public.channel_update_events DROP CONSTRAINT IF EXISTS channel_update_events_type_check;
ALTER TABLE public.channel_update_events ADD CONSTRAINT channel_update_events_type_check CHECK (
    (event_type)::text = ANY (ARRAY[
        'new_channel_message', 'edit_channel_message', 'channel_web_page', 'delete_channel_messages',
        'channel_participant', 'pinned_channel_messages', 'noop'
    ]::text[])
);
