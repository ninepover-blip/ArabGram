package store

import (
	"context"
	"time"
)

// RateLimiter 提供轻量窗口限流。返回 retryAfterSeconds 供 RPC 映射 FLOOD_WAIT_X。
type RateLimiter interface {
	Allow(ctx context.Context, key string, limit int, window time.Duration) (allowed bool, retryAfterSeconds int, err error)
	AllowN(ctx context.Context, key string, cost, limit int, window time.Duration) (allowed bool, retryAfterSeconds int, err error)
}
