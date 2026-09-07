-- 频道私信(monoforum):同一发件人(尤其频道管理员)可向不同订阅者子会话用相同 random_id 发消息,
-- 幂等唯一性必须含 saved_peer,否则跨子会话复用 random_id 会被旧三元组唯一索引误拒/误判为重复。
-- 普通频道消息 saved_peer_id=0 恒定,新四元组等价于原 (channel_id,sender_user_id,random_id),无行为变化。
DROP INDEX IF EXISTS channel_messages_random_idx;
CREATE UNIQUE INDEX channel_messages_random_idx ON public.channel_messages
    USING btree (channel_id, sender_user_id, saved_peer_id, random_id) WHERE (random_id <> 0);
