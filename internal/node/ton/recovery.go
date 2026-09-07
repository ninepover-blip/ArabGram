package ton

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/config"
	"telesrv/internal/domain"
	"telesrv/internal/node/common"
	"telesrv/internal/store/postgres"
	"telesrv/internal/tonaddr"
)

type ResumeExportOptions struct {
	ExportID int64
	Actor    string
	Reason   string
}

func ResumeExport(logger *zap.Logger, buildMeta common.BuildMetadata, opts ResumeExportOptions) error {
	if logger == nil {
		logger = zap.NewNop()
	}
	opts.Actor = strings.TrimSpace(opts.Actor)
	opts.Reason = strings.TrimSpace(opts.Reason)
	if opts.ExportID <= 0 || opts.Actor == "" || opts.Reason == "" {
		return fmt.Errorf("resume-export requires positive --export-id, --actor and --reason")
	}
	cfg, err := config.LoadTON()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	if !cfg.MintEnabled {
		return fmt.Errorf("resume-export requires ton.mint.enabled=true")
	}
	network := domain.TONNetwork(cfg.Network)
	collection, err := tonaddr.Parse(cfg.CollectionAddress, network)
	if err != nil {
		return fmt.Errorf("canonicalize configured collection: %w", err)
	}
	codeHash, err := domain.DecodeTONHash(cfg.CollectionCodeHash)
	if err != nil {
		return fmt.Errorf("decode configured collection code hash: %w", err)
	}
	initialItemIndex, err := strconv.ParseUint(cfg.InitialItemIndex, 10, 64)
	if err != nil {
		return fmt.Errorf("parse configured initial item index: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.RequestTimeout)
	defer cancel()
	migrationStatus, err := postgres.MigrateAndStatus(cfg.PostgresDSN)
	if err != nil {
		return fmt.Errorf("postgres migrate: %w", err)
	}
	pool, err := postgres.Open(ctx, cfg.PostgresDSN,
		postgres.WithMaxConns(cfg.PostgresMaxConns), postgres.WithMinConns(cfg.PostgresMinConns))
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer pool.Close()
	result, err := postgres.NewStarGiftTONStore(pool).ResumeTONExport(ctx, domain.StarGiftTONRecoveryRequest{
		ExportID: opts.ExportID, Network: network, CollectionAddress: collection.StringRaw(),
		CollectionCodeHash: codeHash, MintABI: cfg.MintABI,
		InitialItemIndex: strconv.FormatUint(initialItemIndex, 10), Actor: opts.Actor, Reason: opts.Reason,
		RecoveredAt: int(time.Now().Unix()),
	})
	if err != nil {
		return fmt.Errorf("resume TON export %d: %w", opts.ExportID, err)
	}
	logger.Info("quarantined TON export resumed",
		zap.Int64("export_id", result.Export.ID), zap.Int64("job_id", result.Job.ID),
		zap.Int64("recovery_id", result.RecoveryID), zap.String("job_kind", string(result.Job.Kind)),
		zap.String("item_index", result.Export.ItemIndex), zap.String("actor", opts.Actor),
		zap.Uint("schema_version", migrationStatus.Version), zap.String("git_commit", buildMeta.Commit),
	)
	return nil
}
