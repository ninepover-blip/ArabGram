package rpc

import (
	"context"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/readmodelcache"
)

const (
	notifySettingsCacheMaxEntries = 4096
	// notifySettingsCacheTTL 兜底跨实例失效窗口；同实例 Save/Reset 即时失效。
	// 通知设置改动后其它实例的 dialog 列表静音态最多滞后该时长（可接受）。
	notifySettingsCacheTTL = 60 * time.Second
)

// notifySettingsCache 缓存 userID→该用户全部整-peer 通知设置 map，消除 getDialogs/
// getPeerDialogs/getFullUser/getFullChannel 每次刷新都查 notify_settings 的热路径开销
// （命中即 0 PG）。内建 TTL + singleflight + LRU。
type notifySettingsCache struct {
	cache *readmodelcache.Cache[int64, map[domain.Peer]domain.PeerNotifySettings]
}

func newNotifySettingsCache(now func() time.Time) *notifySettingsCache {
	return &notifySettingsCache{
		cache: readmodelcache.New[int64, map[domain.Peer]domain.PeerNotifySettings](readmodelcache.Config[int64, map[domain.Peer]domain.PeerNotifySettings]{
			MaxEntries: notifySettingsCacheMaxEntries,
			TTL:        notifySettingsCacheTTL,
			Now:        now,
		}),
	}
}

// getOrLoad 返回缓存的 map（只读，调用方查表后须 Clone 单条再持有）。
func (c *notifySettingsCache) getOrLoad(ctx context.Context, userID int64, load func() (map[domain.Peer]domain.PeerNotifySettings, error)) (map[domain.Peer]domain.PeerNotifySettings, error) {
	if c == nil || userID == 0 {
		return load()
	}
	return c.cache.GetOrLoad(ctx, userID, load)
}

func (c *notifySettingsCache) Delete(userID int64) {
	if c == nil || userID == 0 {
		return
	}
	c.cache.Invalidate(userID)
}

func (c *notifySettingsCache) Flush() {
	if c == nil {
		return
	}
	c.cache.Flush()
}

// userNotifySettings 取（缓存的）某用户全部整-peer 通知设置；服务未接通返回 nil。
func (r *Router) userNotifySettings(ctx context.Context, userID int64) map[domain.Peer]domain.PeerNotifySettings {
	svc, ok := r.accountNotifySvc()
	if !ok {
		return nil
	}
	settings, err := r.notifySettings.getOrLoad(ctx, userID, func() (map[domain.Peer]domain.PeerNotifySettings, error) {
		return svc.AllPeerNotifySettings(ctx, userID)
	})
	if err != nil {
		return nil
	}
	return settings
}
