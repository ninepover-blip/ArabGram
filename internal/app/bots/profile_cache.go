package bots

import (
	"context"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/readmodelcache"
)

const (
	botProfileCacheMaxEntries = 100000
	botProfileCacheTTL        = 24 * time.Hour
)

// botProfileValue 是缓存值,found=false 表示「查过但该 bot 不存在」(负缓存),
// 避免未注册 bot 的 id 反复打后端。
type botProfileValue struct {
	profile domain.BotProfile
	found   bool
}

// botProfileCache 由统一缓存原语承载(LRU 单条驱逐 / epoch 守卫 / clone 内建)。
// 单个走 GetOrLoad,批量走 GetOrLoadBatch(一次 LoadEpoch + 合批 load + per-key epoch 写回)。
type botProfileCache struct {
	cache *readmodelcache.Cache[int64, botProfileValue]
}

func newBotProfileCache(max int, ttl time.Duration) *botProfileCache {
	cache := readmodelcache.New[int64, botProfileValue](readmodelcache.Config[int64, botProfileValue]{
		MaxEntries: max,
		TTL:        ttl,
		Clone:      cloneBotProfileValue,
	})
	if cache == nil {
		return nil
	}
	return &botProfileCache{cache: cache}
}

// getOrLoad 解析单个 bot;load 返回 (profile, found, err)。
func (c *botProfileCache) getOrLoad(ctx context.Context, botUserID int64, load func() (domain.BotProfile, bool, error)) (domain.BotProfile, bool, error) {
	if c == nil || botUserID == 0 {
		return load()
	}
	v, err := c.cache.GetOrLoad(ctx, botUserID, func() (botProfileValue, error) {
		profile, found, err := load()
		if err != nil {
			return botProfileValue{}, err
		}
		return botProfileValue{profile: normalizeBotProfile(botUserID, profile, found), found: found}, nil
	})
	if err != nil {
		return domain.BotProfile{}, false, err
	}
	return v.profile, v.found, nil
}

// getMany 批量解析;loadMissing 返回 misses 的 profiles(仅存在的;缺失即视为负结果)。
// 返回的 map 只含存在(found)的 bot,与旧 getMany 语义一致。
func (c *botProfileCache) getMany(ctx context.Context, ids []int64, loadMissing func(context.Context, []int64) (map[int64]domain.BotProfile, error)) (map[int64]domain.BotProfile, error) {
	unique := uniqueBotUserIDs(ids)
	if c == nil {
		return loadMissing(ctx, unique)
	}
	values, err := c.cache.GetOrLoadBatch(ctx, unique,
		func(int64) (int64, bool) { return 0, true }, // 纯 TTL,无版本闸门
		func(ctx context.Context, missing []int64) (map[int64]botProfileValue, error) {
			loaded, err := loadMissing(ctx, missing)
			if err != nil {
				return nil, err
			}
			out := make(map[int64]botProfileValue, len(missing))
			for _, id := range missing {
				profile, found := loaded[id]
				out[id] = botProfileValue{profile: normalizeBotProfile(id, profile, found), found: found}
			}
			return out, nil
		})
	if err != nil {
		return nil, err
	}
	out := make(map[int64]domain.BotProfile, len(values))
	for id, v := range values {
		if v.found {
			out[id] = v.profile
		}
	}
	return out, nil
}

func (c *botProfileCache) put(botUserID int64, profile domain.BotProfile, found bool) {
	if c == nil || botUserID == 0 {
		return
	}
	c.cache.Store(botUserID, botProfileValue{profile: normalizeBotProfile(botUserID, profile, found), found: found})
}

func (c *botProfileCache) delete(botUserID int64) {
	if c == nil || botUserID == 0 {
		return
	}
	c.cache.Invalidate(botUserID)
}

func (c *botProfileCache) flush() {
	if c == nil {
		return
	}
	c.cache.Flush()
}

func normalizeBotProfile(botUserID int64, profile domain.BotProfile, found bool) domain.BotProfile {
	if found && profile.BotUserID == 0 {
		profile.BotUserID = botUserID
	}
	return profile
}

func cloneBotProfileValue(v botProfileValue) botProfileValue {
	v.profile = cloneBotProfile(v.profile)
	return v
}

func cloneBotProfile(profile domain.BotProfile) domain.BotProfile {
	if len(profile.Commands) > 0 {
		profile.Commands = append([]domain.BotCommand(nil), profile.Commands...)
	}
	return profile
}
