package rpc

import (
	"context"
	"time"

	"go.uber.org/zap"
)

const defaultPhoneExpiryTick = time.Second

// PhoneExpiryDispatcher 是私聊通话的服务端兜底超时 worker：双方都掉线/卡死时
// 由它把超时通话迁入终态、推送双方并落「未接来电」历史。正常路径下客户端自身
// 的 20s/90s 定时器会先行 discard，两者幂等汇合于 tombstone。
type PhoneExpiryDispatcher struct {
	router   *Router
	log      *zap.Logger
	interval time.Duration
}

func NewPhoneExpiryDispatcher(router *Router, log *zap.Logger, interval time.Duration) *PhoneExpiryDispatcher {
	if log == nil {
		log = zap.NewNop()
	}
	if interval <= 0 {
		interval = defaultPhoneExpiryTick
	}
	return &PhoneExpiryDispatcher{router: router, log: log, interval: interval}
}

func (d *PhoneExpiryDispatcher) Run(ctx context.Context) {
	if d == nil || d.router == nil {
		return
	}
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.DispatchOnce(ctx)
		}
	}
}

func (d *PhoneExpiryDispatcher) DispatchOnce(ctx context.Context) {
	if d == nil || d.router == nil || d.router.deps.Phone == nil {
		return
	}
	expired := d.router.deps.Phone.ExpireDue(ctx, d.router.clock.Now())
	for _, call := range expired {
		// ctx 无 session 锚点 ⇒ 推送不排除任何设备，双方全部设备收到终态。
		d.router.pushPhoneCallDiscardedBoth(ctx, call)
		d.router.sendPhoneCallServiceMessage(ctx, call)
		d.log.Info("phone call expired",
			zap.Int64("call_id", call.ID),
			zap.String("reason", string(call.DiscardReason)))
	}
}
