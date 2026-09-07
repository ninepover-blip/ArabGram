package rpc

import (
	"time"

	"telesrv/internal/readmodelcache"
)

// projectionCache 是 RPC 层 per-viewer TTL 投影缓存的统一核心,基于 readmodelcache.Cache。
// 4 个结构同形的投影缓存(userFull/peerSettings/channelFull/channelFullBot)收敛到它,
// 仅在键类型、值类型、clone、失效维度上不同;TTL 与时钟、LRU 单条驱逐、epoch 守卫均由原语承载。
//
// 这些投影走「外部构建再写回」模式(LoadEpoch → 在 handler 内构建 → StoreIfEpoch),而非
// GetOrLoad:投影需要 ctx、多次往返与富化,无法塞进单个 load 回调。epoch 守卫确保构建期间
// 到达的失效不会被 pre-invalidation 快照覆盖(避免陈旧 per-viewer 投影留满一个 TTL)。
type projectionCache[K comparable, V any] struct {
	cache *readmodelcache.Cache[K, V]
}

func newProjectionCache[K comparable, V any](max int, ttl time.Duration, clock func() time.Time, clone func(V) V) *projectionCache[K, V] {
	return &projectionCache[K, V]{
		cache: readmodelcache.New[K, V](readmodelcache.Config[K, V]{
			MaxEntries: max,
			TTL:        ttl,
			Now:        clock,
			Clone:      clone,
		}),
	}
}

// Lookup 返回缓存项(TTL 过期视为 miss);clone 由原语在返回边界完成。
func (c *projectionCache[K, V]) lookup(key K) (V, bool) {
	if c == nil {
		var zero V
		return zero, false
	}
	return c.cache.Peek(key)
}

// storeIfEpoch 仅在 epoch 自 loadEpoch 以来未变时写回外部构建的值。
func (c *projectionCache[K, V]) storeIfEpoch(key K, v V, loadEpoch uint64) {
	if c == nil {
		return
	}
	c.cache.StoreIfEpoch(key, v, loadEpoch)
}

// LoadEpoch 在构建前快照 epoch,与 storeIfEpoch 配对实现外部构建路径的 epoch 守卫。
func (c *projectionCache[K, V]) LoadEpoch() uint64 {
	if c == nil {
		return 0
	}
	return c.cache.LoadEpoch()
}

func (c *projectionCache[K, V]) deleteKey(key K) {
	if c == nil {
		return
	}
	c.cache.Invalidate(key)
}

func (c *projectionCache[K, V]) deleteWhere(pred func(K) bool) {
	if c == nil {
		return
	}
	c.cache.InvalidateWhere(pred)
}

// Clear 清空缓存并自增 epoch(listener 重连兜底)。
func (c *projectionCache[K, V]) Clear() {
	if c == nil {
		return
	}
	c.cache.Flush()
}
