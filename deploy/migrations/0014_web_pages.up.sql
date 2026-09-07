-- 链接预览（webpage preview）解析缓存。键为规范化 URL 的 63-bit 哈希；同一 URL
-- 跨发送/跨实例去重到一行，避免对同一链接重复抓取外网。snapshot 是完整的
-- domain.MessageWebPage（含已解析卡片字段与内嵌预览图引用），与消息 media 列同构，
-- 读时无需重新抓取或 rehydrate 即可投影为 messageMediaWebPage。
--
-- web_page_id 由 url_hash 派生（二者同值），使 pending 占位与 done 解析结果携带
-- 同一 webPage id，客户端按 id 关联占位与解析（见异步回填阶段）。
-- state 冗余出来便于后续按 pending/empty 做清扫与刷新。snapshot 不可变（按内容寻址
-- 的预览图 blob 已独立持久化于 photos/blob 存储），跨实例靠 url_hash 唯一约束去重 +
-- 每实例 TTL，故本表无需 read-model NOTIFY 触发器。
CREATE TABLE public.web_pages (
    url_hash     bigint PRIMARY KEY,
    web_page_id  bigint NOT NULL,
    state        text   NOT NULL,
    snapshot     jsonb  NOT NULL DEFAULT '{}'::jsonb,
    created_at   bigint NOT NULL DEFAULT 0,
    refreshed_at bigint NOT NULL DEFAULT 0
);
