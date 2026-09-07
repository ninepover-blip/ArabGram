-- 相册分组 id：同一次 messages.sendMultiMedia 的各条消息共享一个非零 grouped_id，
-- 客户端据此把它们渲染成一个相册组。此前 sendMultiMedia 不绑定 grouped_id（各条独立气泡）。
-- 镜像 via_bot_id 的三张表：共享私聊主体 + 每 owner 收件箱 + 频道消息。
ALTER TABLE public.private_messages ADD COLUMN grouped_id bigint DEFAULT 0 NOT NULL;
ALTER TABLE public.message_boxes ADD COLUMN grouped_id bigint DEFAULT 0 NOT NULL;
ALTER TABLE public.channel_messages ADD COLUMN grouped_id bigint DEFAULT 0 NOT NULL;
