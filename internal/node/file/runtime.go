// Package file wires the standalone FileData role.
package file

import (
	"fmt"
	"os"
	"strings"

	"github.com/iamxvbaba/td/tg"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	filesapp "telesrv/internal/app/files"
	"telesrv/internal/blobstorage"
	"telesrv/internal/config"
	"telesrv/internal/domain"
	"telesrv/internal/filedata"
	"telesrv/internal/node/common"
	obsmetrics "telesrv/internal/observability/metrics"
	"telesrv/internal/store/postgres"
)

func Run(logger *zap.Logger, buildMeta common.BuildMetadata) error {
	cfg, err := config.LoadFile()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	return runWithConfig(logger, cfg, buildMeta)
}

func runWithConfig(logger *zap.Logger, cfg config.FileConfig, buildMeta common.BuildMetadata) error {
	if err := validateConfig(cfg); err != nil {
		return err
	}
	instanceID, err := config.RequireInstanceID(cfg.InstanceID)
	if err != nil {
		return fmt.Errorf("invalid instance id: %w", err)
	}
	if err := common.ConfigureProcessGlobals(cfg); err != nil {
		return err
	}
	logger.Info("telesrv file starting",
		zap.Int("dc", cfg.DC),
		zap.Int("tl_layer", tg.Layer),
		zap.String("git_commit", buildMeta.Commit),
		zap.String("git_branch", buildMeta.Branch),
		zap.String("git_tree_state", buildMeta.TreeState),
		zap.String("build_time", buildMeta.BuildTime),
		zap.String("go_version", buildMeta.GoVersion),
		zap.String("instance_id", instanceID),
		zap.String("file_grpc_addr", cfg.FileGRPCAddr),
		zap.String("blob_backend", cfg.BlobBackendKind),
	)

	ctx, stop, metricRegistry := common.StartRuntimeSupport(cfg, logger)
	defer stop()
	migrationStatus, err := postgres.MigrateAndStatus(cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("postgres migrate: %w", err)
	}
	blobRuntimeLock, err := postgres.AcquireBlobRuntimeLock(ctx, cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("acquire blob runtime lock: %w", err)
	}
	defer func() {
		if err := blobRuntimeLock.Close(); err != nil {
			logger.Error("release blob runtime lock", zap.Error(err))
		}
	}()
	pool, err := postgres.Open(ctx, cfg.PostgresDSN,
		postgres.WithMaxConns(cfg.PostgresMaxConns),
		postgres.WithMinConns(cfg.PostgresMinConns),
	)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	registerPostgresMetrics(metricRegistry, pool)

	mediaStore := postgres.NewMediaStore(pool)
	if err := blobstorage.RequireConfiguredBackend(ctx, mediaStore, cfg.BlobBackendKind); err != nil {
		return fmt.Errorf("validate configured blob backend: %w", err)
	}
	blobCfg := blobStorageConfig(cfg)
	blobRuntime, err := blobstorage.NewRuntime(ctx, blobCfg)
	if err != nil {
		return fmt.Errorf("init blob backend: %w", err)
	}
	capacity, err := blobstorage.NewCapacityRuntime(ctx, blobCfg, mediaStore)
	if err != nil {
		return fmt.Errorf("init blob capacity guard: %w", err)
	}
	for _, guard := range capacity.Workers {
		go filesapp.NewDiskUsageWorker(guard, cfg.StorageUsageRefreshInterval, logger.Named("file").Named("capacity")).Run(ctx)
	}
	blobBackend := filesapp.BlobBackend(filesapp.NewGuardedBlobBackend(blobRuntime.Permanent, capacity.PermanentWrite))
	uploadPartBackend := filesapp.UploadPartBackend(filesapp.NewGuardedUploadPartBackend(blobRuntime.UploadPart, capacity.StagingWrite))
	filesService := filesapp.NewService(mediaStore, blobBackend, cfg.DC,
		filesapp.WithLogger(logger.Named("app").Named("files")),
		filesapp.WithGifCatalog(postgres.NewGifCatalogStore(pool)),
		filesapp.WithUploadPartBackend(uploadPartBackend),
		filesapp.WithUploadPartQuota(domain.UploadPartQuota{
			MaxBytes: cfg.UploadInFlightMaxBytes,
			MaxParts: cfg.UploadInFlightMaxParts,
			MaxFiles: cfg.UploadInFlightMaxFiles,
		}),
		filesapp.WithMapboxMapTiles(cfg.MapboxToken, cfg.MapTileCacheDir),
		externalMediaOption(cfg),
		webPagePreviewOption(cfg),
	)
	if _, err := filedata.StartGRPC(ctx, filedata.GRPCServerConfig{
		Addr:            cfg.FileGRPCAddr,
		InstanceID:      instanceID,
		Token:           cfg.FileToken,
		TLSCertFile:     cfg.FileGRPCTLSCertFile,
		TLSKeyFile:      cfg.FileGRPCTLSKeyFile,
		TLSClientCAFile: cfg.FileGRPCTLSClientCAFile,
		Service:         filesService,
		BlobBackend:     blobBackend,
		UploadParts:     uploadPartBackend,
		Logger:          logger.Named("filedata").Named("grpc"),
	}); err != nil {
		return fmt.Errorf("start filedata grpc: %w", err)
	}
	go filesapp.NewUploadPartGCWorker(filesService, logger.Named("file").Named("upload_gc"),
		cfg.UploadPartTTL,
		cfg.UploadPartGCInterval,
		cfg.UploadPartGCBatch,
	).Run(ctx)
	logger.Info("telesrv file ready",
		zap.String("instance_id", instanceID),
		zap.Int("pid", os.Getpid()),
		zap.String("git_commit", buildMeta.Commit),
		zap.Uint("schema_version", migrationStatus.Version),
		zap.String("file_grpc_addr", cfg.FileGRPCAddr),
		zap.String("blob_backend", blobBackend.Name()),
	)
	<-ctx.Done()
	return nil
}

func validateConfig(cfg config.FileConfig) error {
	if strings.TrimSpace(cfg.FileGRPCAddr) == "" {
		return fmt.Errorf("TELESRV_FILE_GRPC_ADDR is required by cmd/telesrv-file")
	}
	if strings.TrimSpace(cfg.FileToken) == "" {
		return fmt.Errorf("TELESRV_FILE_TOKEN is required by cmd/telesrv-file and FileData clients")
	}
	return nil
}

func externalMediaOption(cfg config.FileConfig) filesapp.Option {
	if !cfg.ExternalMediaEnable {
		return nil
	}
	return filesapp.WithExternalMedia(cfg.ExternalMediaMaxBytes, cfg.ExternalMediaRatePerMin)
}

func webPagePreviewOption(cfg config.FileConfig) filesapp.Option {
	if !cfg.WebPagePreviewEnable {
		return nil
	}
	return filesapp.WithWebPagePreview(cfg.WebPagePreviewMaxBytes, cfg.WebPagePreviewRatePerMin)
}

func blobStorageConfig(cfg config.FileConfig) blobstorage.Config {
	return blobstorage.Config{
		BlobBackendKind:            cfg.BlobBackendKind,
		BlobDir:                    cfg.BlobDir,
		BlobStagingDir:             cfg.BlobStagingDir,
		S3Endpoint:                 cfg.S3Endpoint,
		S3Region:                   cfg.S3Region,
		S3Bucket:                   cfg.S3Bucket,
		S3AccessKeyID:              cfg.S3AccessKeyID,
		S3SecretAccessKey:          cfg.S3SecretAccessKey,
		S3UseSSL:                   cfg.S3UseSSL,
		S3PathStyle:                cfg.S3PathStyle,
		S3CreateBucket:             cfg.S3CreateBucket,
		StorageLowSpaceGuardEnable: cfg.StorageLowSpaceGuardEnable,
		StorageMinFreeBytes:        cfg.StorageMinFreeBytes,
		StorageMaxTotalBytes:       cfg.StorageMaxTotalBytes,
	}
}

func registerPostgresMetrics(registry *obsmetrics.Registry, pool *pgxpool.Pool) {
	if registry == nil || pool == nil {
		return
	}
	registry.AddGaugeProvider(func() []obsmetrics.GaugeSample {
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
}
