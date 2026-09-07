-- 0001_init 回滚（no-op）：本文件是全新项目由 0001-0140 迁移链压缩而来的初始 schema。
--
-- 全新项目明确不保留回退路径（沿用 0011 旧迁移的同一约定）。重置数据库请重建：
-- `docker compose -f deploy/docker-compose.yml down -v` 销毁卷后重新迁移，而非 golang-migrate down。
-- golang-migrate 执行本文件即把版本回退到 0，不删除任何对象或数据。
SELECT 1;
