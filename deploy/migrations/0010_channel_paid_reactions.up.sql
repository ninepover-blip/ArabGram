-- 频道帖子付费 reaction（messages.sendPaidReaction）：每个 reactor 对一条频道消息累计
-- 投入的 Stars 星数 + 是否匿名。此前 sendPaidReaction 恒返 BALANCE_TOO_LOW（无账本无累计）。
-- 配合 Stars 账本（迁移 0009）：rpc 先从账本 Debit，再在此累计；消息上展示 ReactionPaid 总星数
-- 与 top reactors 排行。星数为正、按 (channel,message,user) 累加。
CREATE TABLE public.channel_message_paid_reactions (
    channel_id bigint NOT NULL,
    message_id integer NOT NULL,
    reactor_user_id bigint NOT NULL,
    stars bigint DEFAULT 0 NOT NULL,
    anonymous boolean DEFAULT false NOT NULL,
    reaction_date integer DEFAULT 0 NOT NULL,
    CONSTRAINT channel_message_paid_reactions_pkey PRIMARY KEY (channel_id, message_id, reactor_user_id),
    CONSTRAINT channel_message_paid_reactions_stars_pos CHECK ((stars > 0))
);

-- top reactors 排行按 (channel, message, stars DESC)。
CREATE INDEX channel_message_paid_reactions_top_idx ON public.channel_message_paid_reactions USING btree (channel_id, message_id, stars DESC);
