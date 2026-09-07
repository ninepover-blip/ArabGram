package store

import (
	"context"

	"telesrv/internal/domain"
)

// UpdateEventStore 持久化 user 维度的增量事件。
type UpdateEventStore interface {
	Append(ctx context.Context, userID int64, event domain.UpdateEvent) error
	AppendAllocated(ctx context.Context, userID int64, event domain.UpdateEvent) (domain.UpdateEvent, error)
	ListAfter(ctx context.Context, userID int64, pts, limit int) ([]domain.UpdateEvent, error)
	// MaxContiguousPts 返回账号已提交 pts 水位。PG 实现把 pts 分配与事件写入放在
	// 同一事务边界内，因此该水位就是 durable log 的连续水位。
	MaxContiguousPts(ctx context.Context, userID int64) (int, error)
}

// EventCursor 精确定位单条账号事件，用于 outbox worker 批量加载已 claim 事件。
type EventCursor struct {
	UserID int64
	Pts    int
}

// DispatchUpdateEventStore is the sole Egress event-loading boundary. Durable
// dispatch never degrades to per-item ListAfter calls.
type DispatchUpdateEventStore interface {
	BatchByCursor(ctx context.Context, cursors []EventCursor) ([]domain.UpdateEvent, error)
}
