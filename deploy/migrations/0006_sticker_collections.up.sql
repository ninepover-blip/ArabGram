-- per-user 个人贴纸/GIF 集合：收藏贴纸 / 最近贴纸 / attach 最近贴纸 / 保存的 GIF。
-- 此前 messages.faveSticker/saveRecentSticker/saveGif 未注册(NOT_IMPLEMENTED)、
-- getFaved/getRecent/getSavedGifs 返回空 stub。本表落地真实持久化。
-- used_at 为入列/最近使用时刻，读取按 used_at DESC（最新在前）；document_id 引用
-- documents 表，读路径经 Files.GetDocuments 解析为完整文档。
CREATE TABLE public.user_sticker_collections (
    owner_user_id bigint NOT NULL,
    kind text NOT NULL,
    document_id bigint NOT NULL,
    used_at integer DEFAULT 0 NOT NULL,
    CONSTRAINT user_sticker_collections_kind_check CHECK ((kind = ANY (ARRAY['faved'::text, 'recent'::text, 'recent_attached'::text, 'gif'::text])))
);

ALTER TABLE ONLY public.user_sticker_collections
    ADD CONSTRAINT user_sticker_collections_pkey PRIMARY KEY (owner_user_id, kind, document_id);

-- 读取/截断按 (owner, kind, used_at DESC)。
CREATE INDEX user_sticker_collections_order_idx ON public.user_sticker_collections USING btree (owner_user_id, kind, used_at DESC);
