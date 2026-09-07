package store

import (
	"context"

	"telesrv/internal/domain"
)

// ThemeStore 持久化自定义云主题(account.createTheme 等)与每用户已安装/已存主题列表。
// 读未命中返回 (zero,false,nil);更新/安装不存在的主题返回 domain.ErrThemeNotFound;
// slug 冲突返回 domain.ErrThemeSlugTaken。memory 与 postgres 实现必须行为一致。
type ThemeStore interface {
	CreateTheme(ctx context.Context, t domain.Theme) error
	GetThemeByID(ctx context.Context, id int64) (domain.Theme, bool, error)
	GetThemeBySlug(ctx context.Context, slug string) (domain.Theme, bool, error)
	SlugExists(ctx context.Context, slug string) (bool, error)
	UpdateTheme(ctx context.Context, t domain.Theme) error
	IncrementInstalls(ctx context.Context, id int64) error
	// SetInstalled 把 themeID 加入 userID 的已安装/已存列表(upsert,更新 dark)。
	SetInstalled(ctx context.Context, userID, themeID int64, dark bool) error
	// RemoveInstalled 从列表移除(saveTheme unsave)。不存在不报错。
	RemoveInstalled(ctx context.Context, userID, themeID int64) error
	ListInstalledByUser(ctx context.Context, userID int64) ([]domain.Theme, error)
	// ListThemesForUser 返回用户「创建 ∪ 安装」的全部主题(去重),供 getThemes 跨设备同步。
	// 按 created_at,id 升序(memory 与 postgres 必须一致)。
	ListThemesForUser(ctx context.Context, userID int64) ([]domain.Theme, error)
}
