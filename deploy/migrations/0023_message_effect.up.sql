-- 消息特效（message effect）：messages.sendMessage/sendMedia 的 effect:flags2.2?long，
-- 私聊 1-1 专属动画特效（🎉/👍 等，发送方与接收方双向播放）。官方所有客户端仅在私聊
-- 显示特效选择器，群/频道从不渲染，故只镜像私聊两张表（共享主体 + 每 owner 收件箱），
-- channel_messages 不加列。镜像 via_bot_id / grouped_id 的标量列写法（bigint，0 表无特效）。
ALTER TABLE public.private_messages ADD COLUMN effect bigint DEFAULT 0 NOT NULL;
ALTER TABLE public.message_boxes ADD COLUMN effect bigint DEFAULT 0 NOT NULL;
