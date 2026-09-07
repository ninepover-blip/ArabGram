package redisstore

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const DefaultBusinessAutomationGateTTL = 24 * time.Hour

// BusinessAutomationGateCache stores both positive and negative scalar gate
// results. Exact invalidation is driven by the durable read-model relay; TTL is
// only a bounded safety net for out-of-band database writes.
type BusinessAutomationGateCache struct {
	c   *redis.Client
	ttl time.Duration
}

func NewBusinessAutomationGateCache(c *redis.Client, ttl time.Duration) *BusinessAutomationGateCache {
	if ttl <= 0 {
		ttl = DefaultBusinessAutomationGateTTL
	}
	return &BusinessAutomationGateCache{c: c, ttl: ttl}
}

func businessAutomationGateKey(userID int64) string {
	return "business:automation:gate:v1:" + strconv.FormatInt(userID, 10)
}

func (c *BusinessAutomationGateCache) GetBusinessAutomationGate(ctx context.Context, userID int64) (bool, bool, error) {
	if c == nil || c.c == nil || userID <= 0 {
		return false, false, fmt.Errorf("Redis business automation gate cache is required")
	}
	value, err := c.c.Get(ctx, businessAutomationGateKey(userID)).Result()
	if err == redis.Nil {
		return false, false, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("redis get business automation gate: %w", err)
	}
	switch value {
	case "0":
		return false, true, nil
	case "1":
		return true, true, nil
	default:
		if err := c.c.Del(ctx, businessAutomationGateKey(userID)).Err(); err != nil {
			return false, false, fmt.Errorf("redis delete corrupt business automation gate: %w", err)
		}
		return false, false, nil
	}
}

func (c *BusinessAutomationGateCache) PutBusinessAutomationGate(ctx context.Context, userID int64, value bool) error {
	if c == nil || c.c == nil || userID <= 0 {
		return fmt.Errorf("Redis business automation gate cache is required")
	}
	wire := "0"
	if value {
		wire = "1"
	}
	if err := c.c.Set(ctx, businessAutomationGateKey(userID), wire, c.ttl).Err(); err != nil {
		return fmt.Errorf("redis put business automation gate: %w", err)
	}
	return nil
}

func (c *BusinessAutomationGateCache) DeleteBusinessAutomationGate(ctx context.Context, userIDs ...int64) error {
	if c == nil || c.c == nil {
		return fmt.Errorf("Redis business automation gate cache is required")
	}
	keys := make([]string, 0, len(userIDs))
	seen := make(map[int64]struct{}, len(userIDs))
	for _, userID := range userIDs {
		if userID <= 0 {
			continue
		}
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		keys = append(keys, businessAutomationGateKey(userID))
	}
	if len(keys) == 0 {
		return nil
	}
	if err := c.c.Del(ctx, keys...).Err(); err != nil {
		return fmt.Errorf("redis delete business automation gates: %w", err)
	}
	return nil
}
