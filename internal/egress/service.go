// Package egress owns the durable delivery state machine and the fixed
// ordering-domain actor runtime.
package egress

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/edgecontrol"
	"telesrv/internal/store"
)

var ErrMissingDependency = errors.New("egress: missing dependency")

const (
	defaultDeliveryAttemptTimeout     = 2 * time.Second
	defaultDeliveryClockSkewAllowance = time.Second
	minimumDeliveryLeaseTailSafety    = 100 * time.Millisecond
	maxDomainActorPartitions          = 1024
	maxDomainActorMailbox             = 1024
	maxDomainActorTasks               = int64(262144)
	maxDomainActorBytes               = int64(16 << 30)
)

type Config struct {
	InstanceID                 string
	Workers                    int
	Batch                      int
	WindowSize                 int
	WindowByteLimit            int
	LeaseDuration              time.Duration
	DeliveryAttemptTimeout     time.Duration
	DeliveryClockSkewAllowance time.Duration
	ActorPartitions            int
	ActorMailbox               int
	ActorMailboxBytes          int64
}

type WakeSources struct {
	AccountPTS    <-chan struct{}
	AccountNonPTS <-chan struct{}
	ChannelPTS    <-chan struct{}
}

type Service struct {
	coordinator *deliveryCoordinator
	projection  *ptsProjectionPool
	config      Config
	log         *zap.Logger
}

func NewService(
	events store.DispatchUpdateEventStore,
	dispatch store.DispatchOutboxStore,
	absolute store.DeliveryOutboxStore,
	channel store.ChannelDeliveryStore,
	mutations AttemptMutationWriter,
	planner edgecontrol.DeliveryPlanner,
	builder OutboxUpdateBuilder,
	readModelInvalidations store.ReadModelInvalidationPublisher,
	channelBuilder ChannelUpdateBuilder,
	metrics Metrics,
	log *zap.Logger,
	cfg Config,
) (*Service, error) {
	if events == nil || dispatch == nil || absolute == nil || channel == nil || mutations == nil || planner == nil ||
		builder == nil || readModelInvalidations == nil || channelBuilder == nil || strings.TrimSpace(cfg.InstanceID) == "" {
		return nil, ErrMissingDependency
	}
	if metrics == nil {
		metrics = nopMetrics{}
	}
	if log == nil {
		log = zap.NewNop()
	}
	canonicalInstanceID := strings.TrimSpace(cfg.InstanceID)
	if canonicalInstanceID != cfg.InstanceID {
		return nil, errors.New("egress: instance identity must be canonical")
	}
	cfg.InstanceID = canonicalInstanceID
	if len(cfg.InstanceID) > edgecontrol.MaxDeliveryInstanceIDBytes {
		return nil, errors.New("egress: instance identity exceeds v3 delivery byte limit")
	}
	if cfg.Workers < 0 || cfg.Workers > outboxLogicalShards {
		return nil, errors.New("egress: workers must fit the fixed logical shard space")
	}
	if cfg.Workers == 0 {
		cfg.Workers = defaultOutboxWorkers
	}
	lastWorker := normalizedOutboxWorkers(cfg.Workers) - 1
	if len(deliveryWorkerOwner(cfg.InstanceID, store.OutboxQueueChannelPTS, lastWorker)) > store.MaxDeliveryLeaseOwnerBytes {
		return nil, errors.New("egress: instance identity leaves no room for the durable worker owner suffix")
	}
	if cfg.Batch < 0 || cfg.Batch > edgecontrol.MaxDeliveryBatchItems {
		return nil, errors.New("egress: claim batch exceeds the v3 item limit")
	}
	if cfg.Batch == 0 {
		cfg.Batch = defaultOutboxBatch
	}
	if cfg.WindowSize < 0 || cfg.WindowSize > edgecontrol.MaxDeliveryBatchItems {
		return nil, errors.New("egress: claim window exceeds the v3 item limit")
	}
	if cfg.WindowSize == 0 {
		cfg.WindowSize = defaultOutboxWindowSize
	}
	if cfg.WindowByteLimit < 0 || cfg.WindowByteLimit > edgecontrol.MaxDeliveryBatchBytes {
		return nil, errors.New("egress: claim window exceeds the v3 byte limit")
	}
	if cfg.WindowByteLimit == 0 {
		cfg.WindowByteLimit = defaultOutboxWindowBytes
	}
	if cfg.ActorPartitions < 0 || cfg.ActorPartitions > maxDomainActorPartitions ||
		cfg.ActorMailbox < 0 || cfg.ActorMailbox > maxDomainActorMailbox ||
		cfg.ActorMailboxBytes < 0 || cfg.ActorMailboxBytes > maxDomainActorBytes {
		return nil, errors.New("egress: actor configuration exceeds fixed runtime limits")
	}
	partitions := cfg.ActorPartitions
	if partitions == 0 {
		partitions = defaultDomainActorPartitions
	}
	mailbox := cfg.ActorMailbox
	if mailbox == 0 {
		mailbox = defaultDomainActorMailbox
	}
	if int64(partitions)*int64(mailbox) > maxDomainActorTasks {
		return nil, errors.New("egress: actor task capacity exceeds the fixed runtime limit")
	}
	if cfg.DeliveryAttemptTimeout <= 0 {
		cfg.DeliveryAttemptTimeout = defaultDeliveryAttemptTimeout
	}
	if cfg.DeliveryClockSkewAllowance <= 0 {
		cfg.DeliveryClockSkewAllowance = defaultDeliveryClockSkewAllowance
	}
	if cfg.DeliveryAttemptTimeout+cfg.DeliveryClockSkewAllowance > edgecontrol.MaxDeliveryNotAfterHorizon {
		return nil, errors.New("egress: delivery command and clock-skew horizon exceeds the v3 wire limit")
	}
	if cfg.LeaseDuration <= cfg.DeliveryAttemptTimeout+cfg.DeliveryClockSkewAllowance+minimumDeliveryLeaseTailSafety {
		return nil, errors.New("egress: lease duration must exceed delivery attempt timeout plus clock skew allowance and safety tail")
	}
	if cfg.ActorMailboxBytes > 0 && cfg.ActorMailboxBytes < minimumProductionActorBytes {
		return nil, errors.New("egress: actor byte budget cannot make progress on one maximum v3 delivery window")
	}
	projection := newPTSProjectionPool(events, builder)
	coordinator := &deliveryCoordinator{
		dispatch: dispatch, absolute: absolute, channel: channel,
		mutations:    mutations,
		planner:      planner,
		ptsProjector: projection, readModelInvalidations: readModelInvalidations,
		channelBuilder: channelBuilder, metrics: metrics, log: log, instanceID: cfg.InstanceID,
	}
	return &Service{coordinator: coordinator, projection: projection, config: cfg, log: log}, nil
}

// RunWithWake starts only fixed-size workers: stable shard claimers and a
// fixed ordering-domain actor table. It never allocates a goroutine or lock per
// user, channel, outbox item, or online session.
func (s *Service) RunWithWake(ctx context.Context, wakes WakeSources) {
	if s == nil || s.coordinator == nil || wakes.AccountPTS == nil || wakes.AccountNonPTS == nil || wakes.ChannelPTS == nil {
		return
	}
	actors := newDeliveryActorExecutor(
		s.coordinator, s.config.ActorPartitions, s.config.ActorMailbox, s.config.ActorMailboxBytes,
	)
	var wait sync.WaitGroup
	s.projection.Run(ctx, &wait)
	actors.run(ctx, &wait)
	base := queueRunnerConfig{
		workers: s.config.Workers, laneLimit: s.config.Batch,
		windowSize: s.config.WindowSize, windowBytes: s.config.WindowByteLimit,
		lease:              s.config.LeaseDuration,
		physicalDuration:   s.config.DeliveryAttemptTimeout,
		clockSkewAllowance: s.config.DeliveryClockSkewAllowance,
	}
	pts := base
	pts.kind, pts.store, pts.wake, pts.logicalCount = store.OutboxQueueDispatchPTS, s.coordinator.dispatch, wakes.AccountPTS, store.DispatchOutboxLogicalShards
	absolute := base
	absolute.kind, absolute.store, absolute.wake, absolute.logicalCount = store.OutboxQueueAbsoluteDelivery, s.coordinator.absolute, wakes.AccountNonPTS, store.DispatchOutboxLogicalShards
	channel := base
	channel.kind, channel.store, channel.wake, channel.logicalCount = store.OutboxQueueChannelPTS, s.coordinator.channel, wakes.ChannelPTS, store.ChannelDeliveryLogicalShards
	s.runQueue(ctx, pts, actors, &wait)
	s.runQueue(ctx, absolute, actors, &wait)
	s.runQueue(ctx, channel, actors, &wait)
	s.runStartupFinalizers(ctx, pts, actors, &wait)
	s.runStartupFinalizers(ctx, absolute, actors, &wait)
	s.runStartupFinalizers(ctx, channel, actors, &wait)
	wait.Wait()
}
