package rpc

import (
	"context"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/domain"
)

const (
	defaultScheduledDispatchBatch = 100
	defaultScheduledDispatchLease = 60
	defaultScheduledDispatchTick  = time.Second
	defaultExpiryDispatchBatch    = 100
	defaultExpiryDispatchTick     = time.Second
)

// ScheduledDispatcher promotes due scheduled messages into normal history.
type ScheduledDispatcher struct {
	router       *Router
	log          *zap.Logger
	batch        int
	leaseSeconds int
	interval     time.Duration
	maxIdle      time.Duration
}

func NewScheduledDispatcher(router *Router, log *zap.Logger) *ScheduledDispatcher {
	if log == nil {
		log = zap.NewNop()
	}
	return &ScheduledDispatcher{
		router:       router,
		log:          log,
		batch:        defaultScheduledDispatchBatch,
		leaseSeconds: defaultScheduledDispatchLease,
		interval:     defaultScheduledDispatchTick,
		maxIdle:      defaultIdleDispatchMaxInterval,
	}
}

func (d *ScheduledDispatcher) Run(ctx context.Context) {
	if d == nil || d.router == nil {
		return
	}
	runIdleBackoffLoop(ctx, d.interval, d.maxIdle, d.DispatchOnce)
}

func (d *ScheduledDispatcher) DispatchOnce(ctx context.Context) bool {
	if d == nil || d.router == nil || d.router.deps.Messages == nil {
		return false
	}
	now := int(d.router.clock.Now().Unix())
	items, err := d.router.deps.Messages.ClaimDueScheduledMessages(ctx, now, d.batch, d.leaseSeconds)
	if err != nil {
		d.log.Warn("claim due scheduled messages", zap.Error(err))
		return false
	}
	for _, item := range items {
		if _, err := d.router.sendClaimedScheduledMessages(ctx, item.OwnerUserID, item.Peer, []domain.ScheduledMessage{item}, now); err != nil {
			d.log.Warn("send scheduled message", zap.Int64("owner_user_id", item.OwnerUserID), zap.Int("scheduled_id", item.ID), zap.Error(err))
		}
	}
	return len(items) > 0
}
