-- Story 回复（评论）：用户对一条 story 回复时，客户端发 messages.sendMessage(reply_to=
-- inputReplyToStory{peer, story_id})，消息落入与 story 作者的私聊，并投影为
-- messageReplyStoryHeader。此前 telesrv 直接拒（STORY_ID_INVALID）导致评论发送失败。
-- 复用已有的 reply_to_* 列族，新增 reply_to_story_id 持久化被回复的 story id（普通消息回复恒 0）。
-- 双盒（私聊主体 + per-owner 投影）都需要该列以便重读历史时还原 story 回复头。
ALTER TABLE public.private_messages ADD COLUMN reply_to_story_id integer DEFAULT 0 NOT NULL;
ALTER TABLE public.message_boxes ADD COLUMN reply_to_story_id integer DEFAULT 0 NOT NULL;
