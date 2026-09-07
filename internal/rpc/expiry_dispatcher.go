package rpc

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// ExpiryDispatcher deletes messages whose per-message TTL snapshot has expired.
type ExpiryDispatcher struct {
	router   *Router
	log      *zap.Logger
	batch    int
	interval time.Duration
	maxIdle  time.Duration
}

func NewExpiryDispatcher(router *Router, log *zap.Logger) *ExpiryDispatcher {
	if log == nil {
		log = zap.NewNop()
	}
	return &ExpiryDispatcher{
		router:   router,
		log:      log,
		batch:    defaultExpiryDispatchBatch,
		interval: defaultExpiryDispatchTick,
		maxIdle:  defaultIdleDispatchMaxInterval,
	}
}

func (d *ExpiryDispatcher) Run(ctx context.Context) {
	if d == nil || d.router == nil {
		return
	}
	runIdleBackoffLoop(ctx, d.interval, d.maxIdle, d.DispatchOnce)
}

func (d *ExpiryDispatcher) DispatchOnce(ctx context.Context) bool {
	if d == nil || d.router == nil {
		return false
	}
	now := int(d.router.clock.Now().Unix())
	private := d.dispatchPrivate(ctx, now)
	channels := d.dispatchChannels(ctx, now)
	return private || channels
}

func (d *ExpiryDispatcher) dispatchPrivate(ctx context.Context, now int) bool {
	if d.router.deps.Messages == nil {
		return false
	}
	requests, err := d.router.deps.Messages.ClaimExpiredPrivateMessages(ctx, now, d.batch)
	if err != nil {
		d.log.Warn("claim expired private messages", zap.Error(err))
		return false
	}
	for _, req := range requests {
		if _, err := d.router.deps.Messages.DeleteMessages(ctx, req.OwnerUserID, req); err != nil {
			d.log.Warn("delete expired private messages", zap.Int64("owner_user_id", req.OwnerUserID), zap.Ints("ids", req.IDs), zap.Error(err))
			continue
		}
	}
	return len(requests) > 0
}

func (d *ExpiryDispatcher) dispatchChannels(ctx context.Context, now int) bool {
	if d.router.deps.Channels == nil {
		return false
	}
	ttlSvc, ok := d.router.deps.Channels.(channelHistoryTTLService)
	if !ok {
		return false
	}
	requests, err := ttlSvc.ClaimExpiredMessages(ctx, now, d.batch)
	if err != nil {
		d.log.Warn("claim expired channel messages", zap.Error(err))
		return false
	}
	for _, req := range requests {
		_, err := d.router.deps.Channels.DeleteMessages(ctx, req.UserID, req)
		if err != nil {
			d.log.Warn("delete expired channel messages", zap.Int64("channel_id", req.ChannelID), zap.Ints("ids", req.IDs), zap.Error(err))
			continue
		}
	}
	return len(requests) > 0
}
