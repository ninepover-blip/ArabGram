package rpc

import (
	"context"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/readmodelcache"
)

// storyPinnedAvailableCache 缓存 (viewer,peer)→是否有置顶故事,由统一缓存原语承载
// (LRU 单条驱逐 / epoch 守卫 / 内建 singleflight;纯 TTL,无版本闸门)。
type storyPinnedAvailableCache struct {
	cache *readmodelcache.Cache[storyProjectionCacheKey, bool]
}

func newStoryPinnedAvailableCache(now func() time.Time) *storyPinnedAvailableCache {
	return &storyPinnedAvailableCache{
		cache: readmodelcache.New[storyProjectionCacheKey, bool](readmodelcache.Config[storyProjectionCacheKey, bool]{
			MaxEntries: storyProjectionCacheMaxEntries,
			TTL:        storyPinnedStoriesCacheTTL,
			Now:        now,
		}),
	}
}

func (c *storyPinnedAvailableCache) getOrLoad(ctx context.Context, viewerUserID int64, peer domain.Peer, load func() (bool, error)) (bool, error) {
	if c == nil || viewerUserID == 0 || peer.ID == 0 {
		return load()
	}
	return c.cache.GetOrLoad(ctx, storyProjectionCacheKey{viewerUserID: viewerUserID, peer: peer}, load)
}

func (c *storyPinnedAvailableCache) Delete(viewerUserID int64, peer domain.Peer) {
	if c == nil || viewerUserID == 0 || peer.ID == 0 {
		return
	}
	c.cache.Invalidate(storyProjectionCacheKey{viewerUserID: viewerUserID, peer: peer})
}

func (c *storyPinnedAvailableCache) DeleteViewer(viewerUserID int64) {
	if c == nil || viewerUserID == 0 {
		return
	}
	c.cache.InvalidateWhere(func(k storyProjectionCacheKey) bool { return k.viewerUserID == viewerUserID })
}

func (c *storyPinnedAvailableCache) DeletePeer(peer domain.Peer) {
	if c == nil || peer.ID == 0 {
		return
	}
	c.cache.InvalidateWhere(func(k storyProjectionCacheKey) bool { return k.peer == peer })
}

func (c *storyPinnedAvailableCache) Flush() {
	if c == nil {
		return
	}
	c.cache.Flush()
}
