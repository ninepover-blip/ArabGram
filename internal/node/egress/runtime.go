// Package egress wires the dedicated durable Egress role.
package egress

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/iamxvbaba/td/tg"
	"go.uber.org/zap"

	"telesrv/internal/config"
	"telesrv/internal/edgecontrol"
	"telesrv/internal/edgecontrol/redisbus"
	"telesrv/internal/edgecontrol/redisregistry"
	egresssvc "telesrv/internal/egress"
	"telesrv/internal/node/common"
	obsmetrics "telesrv/internal/observability/metrics"
	"telesrv/internal/store/postgres"
	"telesrv/internal/store/redisstore"
)

// Run starts the dedicated Durable Egress outbox service.
func Run(logger *zap.Logger, buildMeta common.BuildMetadata) error {
	cfg, err := config.LoadEgress()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	return runWithConfig(logger, cfg, buildMeta)
}

func runWithConfig(logger *zap.Logger, cfg config.EgressConfig, buildMeta common.BuildMetadata) error {
	if err := validateEgressConfig(cfg); err != nil {
		return err
	}
	instanceID, err := config.RequireInstanceID(cfg.InstanceID)
	if err != nil {
		return fmt.Errorf("invalid instance id: %w", err)
	}
	if err := common.ConfigureProcessGlobals(cfg); err != nil {
		return err
	}
	logger.Info("telesrv egress starting",
		zap.Int("dc", cfg.DC),
		zap.Int("tl_layer", tg.Layer),
		zap.String("git_commit", buildMeta.Commit),
		zap.String("git_branch", buildMeta.Branch),
		zap.String("git_tree_state", buildMeta.TreeState),
		zap.String("build_time", buildMeta.BuildTime),
		zap.String("go_version", buildMeta.GoVersion),
		zap.String("instance_id", instanceID),
		zap.Int("outbox_workers", cfg.OutboxWorkers),
		zap.Int("outbox_batch", cfg.OutboxBatch),
		zap.Duration("outbox_lease_timeout", cfg.OutboxLeaseTimeout),
		zap.Duration("outbound_push_timeout", cfg.OutboundPushTimeout),
		zap.Duration("delivery_attempt_timeout", cfg.DeliveryAttemptTimeout),
		zap.Duration("delivery_clock_skew_allowance", cfg.DeliveryClockSkewAllowance),
		zap.String("egress_delivery_grpc_addr", cfg.EgressDeliveryGRPCAddr),
	)

	ctx, stop, metricRegistry := common.StartRuntimeSupport(cfg, logger)
	defer stop()
	migrationStatus, err := postgres.MigrateAndStatus(cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("postgres migrate: %w", err)
	}
	pool, err := postgres.Open(ctx, cfg.PostgresDSN,
		postgres.WithMaxConns(cfg.PostgresMaxConns),
		postgres.WithMinConns(cfg.PostgresMinConns),
	)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	metricRegistry.AddGaugeProvider(func() []obsmetrics.GaugeSample {
		stat := pool.Stat()
		return []obsmetrics.GaugeSample{
			{Name: "telesrv_postgres_pool_connections", Labels: []obsmetrics.Label{{Name: "state", Value: "total"}}, Value: float64(stat.TotalConns())},
			{Name: "telesrv_postgres_pool_connections", Labels: []obsmetrics.Label{{Name: "state", Value: "acquired"}}, Value: float64(stat.AcquiredConns())},
			{Name: "telesrv_postgres_pool_connections", Labels: []obsmetrics.Label{{Name: "state", Value: "idle"}}, Value: float64(stat.IdleConns())},
			{Name: "telesrv_postgres_pool_connections", Labels: []obsmetrics.Label{{Name: "state", Value: "constructing"}}, Value: float64(stat.ConstructingConns())},
			{Name: "telesrv_postgres_pool_max_connections", Value: float64(stat.MaxConns())},
			{Name: "telesrv_postgres_pool_acquire_count", Value: float64(stat.AcquireCount())},
			{Name: "telesrv_postgres_pool_acquire_wait_seconds", Value: stat.AcquireDuration().Seconds()},
			{Name: "telesrv_postgres_pool_empty_acquire_count", Value: float64(stat.EmptyAcquireCount())},
			{Name: "telesrv_postgres_pool_canceled_acquire_count", Value: float64(stat.CanceledAcquireCount())},
		}
	})
	rdb, err := redisstore.Open(ctx, cfg.RedisAddr, cfg.RedisPassword, cfg.RedisDB)
	if err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}
	defer func() { _ = rdb.Close() }()
	metricRegistry.AddGaugeProvider(func() []obsmetrics.GaugeSample {
		stat := rdb.PoolStats()
		return []obsmetrics.GaugeSample{
			{Name: "telesrv_redis_pool_connections", Labels: []obsmetrics.Label{{Name: "state", Value: "total"}}, Value: float64(stat.TotalConns)},
			{Name: "telesrv_redis_pool_connections", Labels: []obsmetrics.Label{{Name: "state", Value: "idle"}}, Value: float64(stat.IdleConns)},
			{Name: "telesrv_redis_pool_pending_requests", Value: float64(stat.PendingRequests)},
			{Name: "telesrv_redis_pool_hits", Value: float64(stat.Hits)},
			{Name: "telesrv_redis_pool_misses", Value: float64(stat.Misses)},
			{Name: "telesrv_redis_pool_timeouts", Value: float64(stat.Timeouts)},
			{Name: "telesrv_redis_pool_wait_count", Value: float64(stat.WaitCount)},
			{Name: "telesrv_redis_pool_wait_seconds", Value: time.Duration(stat.WaitDurationNs).Seconds()},
		}
	})

	updateEventStore := postgres.NewUpdateEventStore(pool, postgres.WithUpdateEventLogger(logger.Named("store").Named("updates")))
	dispatchOutboxStore := postgres.NewDispatchOutboxStore(pool, postgres.WithLeaseTimeout(cfg.OutboxLeaseTimeout))
	deliveryOutboxStore := postgres.NewDeliveryOutboxStore(pool, postgres.WithDeliveryLeaseTimeout(cfg.OutboxLeaseTimeout))
	channelDeliveryStore := postgres.NewChannelDeliveryStore(pool)
	welcomeMessageStore := postgres.NewWelcomeMessageStore(pool)
	wakes := newEgressWakeLanes()
	mutationBatcher, err := egresssvc.NewAttemptMutationBatcher(
		dispatchOutboxStore, deliveryOutboxStore, channelDeliveryStore, metricRegistry,
	)
	if err != nil {
		return fmt.Errorf("init egress mutation actors: %w", err)
	}
	go mutationBatcher.Run(ctx)
	if err := mutationBatcher.WaitReady(ctx); err != nil {
		return fmt.Errorf("start egress mutation actors: %w", err)
	}
	clientAckRing := egresssvc.NewClientAckObservationRing(65536)
	metricRegistry.AddGaugeProvider(func() []obsmetrics.GaugeSample {
		stats := clientAckRing.Stats()
		return []obsmetrics.GaugeSample{
			{Name: "telesrv_egress_client_ack_ring_capacity", Value: float64(stats.Capacity)},
			{Name: "telesrv_egress_client_ack_ring_written_total", Value: float64(stats.Written)},
			{Name: "telesrv_egress_client_ack_ring_overwritten_total", Value: float64(stats.Overwritten)},
		}
	})
	if _, err := egresssvc.StartGRPCDelivery(ctx, egresssvc.GRPCDeliveryServerConfig{
		Addr:            cfg.EgressDeliveryGRPCAddr,
		InstanceID:      instanceID,
		Token:           cfg.EgressDeliveryToken,
		TLSCertFile:     cfg.EgressDeliveryGRPCTLSCertFile,
		TLSKeyFile:      cfg.EgressDeliveryGRPCTLSKeyFile,
		TLSClientCAFile: cfg.EgressDeliveryGRPCTLSClientCAFile,
		Store:           dispatchOutboxStore,
		DeliveryStore:   deliveryOutboxStore,
		ChannelStore:    channelDeliveryStore,
		Mutations:       mutationBatcher,
		ClientAcks:      clientAckRing,
		Logger:          logger.Named("egress").Named("delivery").Named("grpc"),
	}); err != nil {
		return fmt.Errorf("start egress delivery grpc: %w", err)
	}
	registry := redisregistry.New(rdb)
	bus := redisbus.New(rdb)
	deliverer := edgecontrol.NewDeliveryFabric(edgecontrol.DeliveryFabricConfig{
		InstanceID:     instanceID,
		Registry:       registry,
		Bus:            bus,
		CommandTimeout: cfg.OutboundPushTimeout,
	})
	defer deliverer.Close()
	welcomeEdgeControl, err := edgecontrol.NewControlFabricController(
		edgecontrol.NewNoLocalController(),
		edgecontrol.NewSessionControlFabric(edgecontrol.SessionControlFabricConfig{
			InstanceID: instanceID, Registry: registry, Bus: bus,
			CommandTimeout: cfg.OutboundPushTimeout,
		}),
	)
	if err != nil {
		return fmt.Errorf("init welcome Edge control fabric: %w", err)
	}
	presence, ok := welcomeEdgeControl.(edgecontrol.UserLocationBatchProvider)
	if !ok {
		return fmt.Errorf("egress control fabric must provide batch user locations")
	}
	projection, err := newEgressProjectionRuntime(pool, rdb, cfg, instanceID, presence, logger.Named("egress").Named("projection"))
	if err != nil {
		return fmt.Errorf("init egress projection runtime: %w", err)
	}
	welcomeDispatcher := projection.projector.NewWelcomeDeliveryDispatcher(
		welcomeEdgeControl, welcomeMessageStore, logger.Named("egress").Named("welcome-delivery"),
	)
	readModelListener := postgres.NewReadModelChangeListener(cfg.PostgresDSN, projection.caches, logger.Named("egress").Named("read-model-listener"))
	go readModelListener.Run(ctx)
	readModelRedisBus := redisstore.NewReadModelInvalidationBus(rdb, logger.Named("egress").Named("read-model-redis"))
	go readModelRedisBus.Run(ctx,
		func() { readModelListener.FlushRelayedReadModelCaches("redis_subscribe") },
		readModelListener.HandleReadModelInvalidations,
	)
	service, err := egresssvc.NewService(
		updateEventStore,
		dispatchOutboxStore,
		deliveryOutboxStore,
		channelDeliveryStore,
		mutationBatcher,
		deliverer,
		projection.projector.BuildOutboxUpdateBytes,
		readModelRedisBus,
		projection.projector.BuildChannelUpdateBytes,
		metricRegistry,
		logger.Named("egress"),
		egresssvc.Config{
			InstanceID: instanceID, Workers: cfg.OutboxWorkers, Batch: cfg.OutboxBatch,
			LeaseDuration:              cfg.OutboxLeaseTimeout,
			DeliveryAttemptTimeout:     cfg.DeliveryAttemptTimeout,
			DeliveryClockSkewAllowance: cfg.DeliveryClockSkewAllowance,
		},
	)
	if err != nil {
		return fmt.Errorf("init egress service: %w", err)
	}
	logger.Info("telesrv egress ready",
		zap.String("instance_id", instanceID),
		zap.Int("pid", os.Getpid()),
		zap.String("git_commit", buildMeta.Commit),
		zap.Uint("schema_version", migrationStatus.Version),
		zap.String("egress_delivery_grpc_addr", cfg.EgressDeliveryGRPCAddr),
	)
	go welcomeDispatcher.Run(ctx)
	service.RunWithWake(ctx, egresssvc.WakeSources{
		AccountPTS: wakes.dispatchOutbox, AccountNonPTS: wakes.edgeDeliveryOutbox, ChannelPTS: wakes.channelDelivery,
	})
	mutationBatcher.Wait()
	return nil
}

func validateEgressConfig(cfg config.EgressConfig) error {
	if strings.TrimSpace(cfg.EgressDeliveryGRPCAddr) == "" {
		return fmt.Errorf("TELESRV_EGRESS_DELIVERY_GRPC_ADDR is required by cmd/telesrv-egress")
	}
	if strings.TrimSpace(cfg.EgressDeliveryToken) == "" {
		return fmt.Errorf("TELESRV_EGRESS_DELIVERY_TOKEN is required by cmd/telesrv-edge and cmd/telesrv-egress")
	}
	return nil
}

type egressWakeLanes struct {
	dispatchOutbox         <-chan struct{}
	edgeDeliveryOutbox     <-chan struct{}
	channelDelivery        <-chan struct{}
	wakeDispatchOutbox     func()
	wakeEdgeDeliveryOutbox func()
	wakeChannelDelivery    func()
}

func newEgressWakeLanes() egressWakeLanes {
	dispatchOutbox, wakeDispatchOutbox := newWakeLane()
	edgeDeliveryOutbox, wakeEdgeDeliveryOutbox := newWakeLane()
	channelDelivery, wakeChannelDelivery := newWakeLane()
	return egressWakeLanes{
		dispatchOutbox:         dispatchOutbox,
		edgeDeliveryOutbox:     edgeDeliveryOutbox,
		channelDelivery:        channelDelivery,
		wakeDispatchOutbox:     wakeDispatchOutbox,
		wakeEdgeDeliveryOutbox: wakeEdgeDeliveryOutbox,
		wakeChannelDelivery:    wakeChannelDelivery,
	}
}

func newWakeLane() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	wake := func() {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	return ch, wake
}
