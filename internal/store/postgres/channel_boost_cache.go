package postgres

import (
	"context"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/readmodelcache"
)

type channelBoostCacheKey struct {
	userID   int64
	peerType domain.PeerType
	peerID   int64
}

type channelBoostPeerTotalCacheKey struct {
	peerType domain.PeerType
	peerID   int64
}

// ChannelBoostCache 缓存 boost 读投影,由统一缓存原语 readmodelcache.Cache 承载
// (LRU 单条驱逐 / epoch 守卫 / singleflight 内建,TTL 由原语时钟驱动)。
//
// 它刻意只在非事务读路径使用;发送权限校验与 boost apply 事务内仍实时查
// channel_boost_slots。selfBoosts 缓存 viewer 作用域的 SelfBoostsApplied,
// peerTotals 缓存 premium.getBoostsStatus 的频道总 active boost 数。
// boost 自身的到期由 load 时的 SQL(expires_at > now)过滤,本缓存的 TTL 只是新鲜度兜底,
// 与调用方传入的墙钟 now 等价(运行时 now=nowUnix()),故迁移到原语内部时钟行为不变。
type ChannelBoostCache struct {
	selfBoosts *readmodelcache.Cache[channelBoostCacheKey, int]
	peerTotals *readmodelcache.Cache[channelBoostPeerTotalCacheKey, int]
}

// NewChannelBoostCache 创建容量 max、新鲜度 ttl 的 boost 缓存;max<=0 或 ttl<=0 返回 nil(禁用)。
func NewChannelBoostCache(max int, ttl time.Duration) *ChannelBoostCache {
	return newChannelBoostCache(max, ttl, nil)
}

// newChannelBoostCache 允许注入时钟,仅供测试确定地推进 TTL;now=nil 用真实时钟。
func newChannelBoostCache(max int, ttl time.Duration, now func() time.Time) *ChannelBoostCache {
	if ttl <= 0 {
		return nil
	}
	selfBoosts := readmodelcache.New[channelBoostCacheKey, int](readmodelcache.Config[channelBoostCacheKey, int]{
		MaxEntries: max,
		TTL:        ttl,
		Now:        now,
	})
	peerTotals := readmodelcache.New[channelBoostPeerTotalCacheKey, int](readmodelcache.Config[channelBoostPeerTotalCacheKey, int]{
		MaxEntries: max,
		TTL:        ttl,
		Now:        now,
	})
	if selfBoosts == nil || peerTotals == nil {
		return nil
	}
	return &ChannelBoostCache{selfBoosts: selfBoosts, peerTotals: peerTotals}
}

func (c *ChannelBoostCache) get(userID int64, peer domain.Peer) (int, bool) {
	key, ok := newChannelBoostCacheKey(userID, peer)
	if c == nil || !ok {
		return 0, false
	}
	return c.selfBoosts.Peek(key)
}

func (c *ChannelBoostCache) getOrLoad(ctx context.Context, userID int64, peer domain.Peer, load func() (int, error)) (int, error) {
	if c == nil {
		return load()
	}
	key, ok := newChannelBoostCacheKey(userID, peer)
	if !ok {
		return load()
	}
	return c.selfBoosts.GetOrLoad(ctx, key, load)
}

func (c *ChannelBoostCache) put(userID int64, peer domain.Peer, boosts int) {
	key, ok := newChannelBoostCacheKey(userID, peer)
	if c == nil || !ok {
		return
	}
	c.selfBoosts.Store(key, boosts)
}

func (c *ChannelBoostCache) delete(userID int64, peer domain.Peer) {
	key, ok := newChannelBoostCacheKey(userID, peer)
	if c == nil || !ok {
		return
	}
	c.selfBoosts.Invalidate(key)
}

func (c *ChannelBoostCache) deleteUser(userID int64) {
	if c == nil || userID == 0 {
		return
	}
	c.selfBoosts.InvalidateWhere(func(k channelBoostCacheKey) bool { return k.userID == userID })
}

func (c *ChannelBoostCache) getPeerTotal(peer domain.Peer) (int, bool) {
	key, ok := newChannelBoostPeerTotalCacheKey(peer)
	if c == nil || !ok {
		return 0, false
	}
	return c.peerTotals.Peek(key)
}

func (c *ChannelBoostCache) getPeerTotalOrLoad(ctx context.Context, peer domain.Peer, load func() (int, error)) (int, error) {
	if c == nil {
		return load()
	}
	key, ok := newChannelBoostPeerTotalCacheKey(peer)
	if !ok {
		return load()
	}
	return c.peerTotals.GetOrLoad(ctx, key, load)
}

func (c *ChannelBoostCache) putPeerTotal(peer domain.Peer, boosts int) {
	key, ok := newChannelBoostPeerTotalCacheKey(peer)
	if c == nil || !ok {
		return
	}
	c.peerTotals.Store(key, boosts)
}

func (c *ChannelBoostCache) deletePeerTotal(peer domain.Peer) {
	key, ok := newChannelBoostPeerTotalCacheKey(peer)
	if c == nil || !ok {
		return
	}
	c.peerTotals.Invalidate(key)
}

func (c *ChannelBoostCache) flush() {
	if c == nil {
		return
	}
	c.selfBoosts.Flush()
	c.peerTotals.Flush()
}

func newChannelBoostCacheKey(userID int64, peer domain.Peer) (channelBoostCacheKey, bool) {
	if userID == 0 || peer.Type != domain.PeerTypeChannel || peer.ID == 0 {
		return channelBoostCacheKey{}, false
	}
	return channelBoostCacheKey{userID: userID, peerType: peer.Type, peerID: peer.ID}, true
}

func newChannelBoostPeerTotalCacheKey(peer domain.Peer) (channelBoostPeerTotalCacheKey, bool) {
	if peer.Type != domain.PeerTypeChannel || peer.ID == 0 {
		return channelBoostPeerTotalCacheKey{}, false
	}
	return channelBoostPeerTotalCacheKey{peerType: peer.Type, peerID: peer.ID}, true
}
