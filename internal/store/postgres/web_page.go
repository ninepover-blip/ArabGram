package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
)

// web_pages 是链接预览解析缓存：按规范化 URL 的 63-bit 哈希去重，snapshot 持久化完整的
// domain.MessageWebPage 快照。用裸 pgx（非 sqlc），该表无需进入查询代码生成。

// PutWebPage upsert 一行解析结果；冲突按 url_hash 覆盖快照与 refreshed_at（created_at 保留首次）。
func (s *MediaStore) PutWebPage(ctx context.Context, urlHash int64, page domain.MessageWebPage, now int) error {
	snapshot, err := json.Marshal(page)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(ctx, `
		INSERT INTO web_pages (url_hash, web_page_id, state, snapshot, created_at, refreshed_at)
		VALUES ($1, $2, $3, $4, $5, $5)
		ON CONFLICT (url_hash) DO UPDATE SET
			web_page_id  = EXCLUDED.web_page_id,
			state        = EXCLUDED.state,
			snapshot     = EXCLUDED.snapshot,
			refreshed_at = EXCLUDED.refreshed_at`,
		urlHash, page.ID, string(page.State), snapshot, int64(now))
	return err
}

// GetWebPageByURLHash 读回已解析快照；未命中 found=false。
func (s *MediaStore) GetWebPageByURLHash(ctx context.Context, urlHash int64) (domain.MessageWebPage, int, bool, error) {
	var (
		snapshot    []byte
		refreshedAt int64
	)
	err := s.db.QueryRow(ctx, `SELECT snapshot, refreshed_at FROM web_pages WHERE url_hash = $1`, urlHash).
		Scan(&snapshot, &refreshedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.MessageWebPage{}, 0, false, nil
		}
		return domain.MessageWebPage{}, 0, false, err
	}
	var page domain.MessageWebPage
	if len(snapshot) > 0 {
		if err := json.Unmarshal(snapshot, &page); err != nil {
			return domain.MessageWebPage{}, 0, false, err
		}
	}
	return page, int(refreshedAt), true, nil
}
