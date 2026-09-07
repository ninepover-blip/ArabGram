package messages

import (
	"context"
	"sync"
	"time"

	"telesrv/internal/app/readmodel"
	"telesrv/internal/domain"
)

const (
	defaultPrivateMediaCountReadModelTTL = 24 * time.Hour
	privateMediaCountReadModelMaxEntries = 8192
)

type privateMediaCountCacheKey struct {
	userID int64
	peerID int64
}

type privateMediaCountSnapshot struct {
	counts   domain.MediaCategoryCounts
	hash     int64
	expireAt time.Time
}

type privateMediaCountReadModelCache struct {
	ttl time.Duration
	now func() time.Time

	mu        sync.RWMutex
	snapshots map[privateMediaCountCacheKey]privateMediaCountSnapshot
	epoch     uint64
}

func newPrivateMediaCountReadModelCache(ttl time.Duration) *privateMediaCountReadModelCache {
	if ttl <= 0 {
		ttl = defaultPrivateMediaCountReadModelTTL
	}
	return &privateMediaCountReadModelCache{
		ttl:       ttl,
		now:       time.Now,
		snapshots: make(map[privateMediaCountCacheKey]privateMediaCountSnapshot, 1024),
	}
}

func (s *Service) cachedPrivateMediaCounts(ctx context.Context, userID, peerID int64) (domain.MediaCategoryCounts, error) {
	if s.privateMediaCountCache == nil || s.versions == nil {
		return s.messages.CountPrivateMediaCategories(ctx, userID, peerID)
	}
	hash, found, err := s.versions.ReadModelHash(ctx, readmodel.ModelPrivateMediaCounts, userID, domain.PeerTypeUser, peerID)
	if err != nil {
		return nil, err
	}
	if !found || hash == 0 {
		return s.messages.CountPrivateMediaCategories(ctx, userID, peerID)
	}
	key := privateMediaCountCacheKey{userID: userID, peerID: peerID}
	loadEpoch := s.privateMediaCountCache.cacheEpoch()
	if snap, ok := s.privateMediaCountCache.lookup(key, s.privateMediaCountCache.now(), hash); ok {
		return clonePrivateMediaCategoryCounts(snap.counts), nil
	}
	counts, err := s.messages.CountPrivateMediaCategories(ctx, userID, peerID)
	if err != nil {
		return nil, err
	}
	s.privateMediaCountCache.putIfEpoch(key, counts, hash, loadEpoch)
	return counts, nil
}

func (c *privateMediaCountReadModelCache) lookup(key privateMediaCountCacheKey, now time.Time, currentHash int64) (privateMediaCountSnapshot, bool) {
	c.mu.RLock()
	snap, ok := c.snapshots[key]
	c.mu.RUnlock()
	if !ok || !snap.expireAt.After(now) {
		if ok {
			c.invalidate(key)
		}
		return privateMediaCountSnapshot{}, false
	}
	if currentHash != 0 && snap.hash != currentHash {
		return privateMediaCountSnapshot{}, false
	}
	return snap, true
}

func (c *privateMediaCountReadModelCache) putIfEpoch(key privateMediaCountCacheKey, counts domain.MediaCategoryCounts, hash int64, expectedEpoch uint64) {
	if c == nil || key.userID == 0 || key.peerID == 0 || hash == 0 {
		return
	}
	if c.cacheEpoch() != expectedEpoch {
		return
	}
	snap := privateMediaCountSnapshot{
		counts:   clonePrivateMediaCategoryCounts(counts),
		hash:     hash,
		expireAt: c.now().Add(c.ttl),
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.epoch != expectedEpoch {
		return
	}
	if len(c.snapshots) >= privateMediaCountReadModelMaxEntries {
		c.snapshots = make(map[privateMediaCountCacheKey]privateMediaCountSnapshot, 1024)
	}
	c.snapshots[key] = snap
}

func (c *privateMediaCountReadModelCache) invalidate(keys ...privateMediaCountCacheKey) {
	if c == nil || len(keys) == 0 {
		return
	}
	c.mu.Lock()
	c.epoch++
	for _, key := range keys {
		delete(c.snapshots, key)
	}
	c.mu.Unlock()
}

func (c *privateMediaCountReadModelCache) flush() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.epoch++
	c.snapshots = make(map[privateMediaCountCacheKey]privateMediaCountSnapshot, 1024)
	c.mu.Unlock()
}

func (s *Service) InvalidatePrivateMediaCountReadModel(userID, peerID int64) {
	if s == nil || s.privateMediaCountCache == nil || userID == 0 || peerID == 0 {
		return
	}
	s.privateMediaCountCache.invalidate(privateMediaCountCacheKey{userID: userID, peerID: peerID})
}

func (s *Service) FlushPrivateMediaCountReadModel() {
	if s == nil || s.privateMediaCountCache == nil {
		return
	}
	s.privateMediaCountCache.flush()
}

func (c *privateMediaCountReadModelCache) cacheEpoch() uint64 {
	c.mu.RLock()
	epoch := c.epoch
	c.mu.RUnlock()
	return epoch
}

func clonePrivateMediaCategoryCounts(in domain.MediaCategoryCounts) domain.MediaCategoryCounts {
	if len(in) == 0 {
		return domain.MediaCategoryCounts{}
	}
	out := make(domain.MediaCategoryCounts, len(in))
	for category, count := range in {
		out[category] = count
	}
	return out
}
