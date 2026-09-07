// Package ton wires the isolated TON Star Gift worker role.
package ton

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"telesrv/internal/config"
	"telesrv/internal/domain"
	"telesrv/internal/node/common"
	"telesrv/internal/store/postgres"
	"telesrv/internal/tonchain"
	"telesrv/internal/tonworker"
)

func Run(logger *zap.Logger, buildMeta common.BuildMetadata) error {
	cfg, err := config.LoadTON()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	return runWithConfig(logger, cfg, buildMeta)
}

func runWithConfig(logger *zap.Logger, cfg config.TONConfig, buildMeta common.BuildMetadata) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	logger.Info("telesrv TON worker starting",
		zap.String("network", cfg.Network), zap.Bool("mint_enabled", cfg.MintEnabled),
		zap.String("instance_id", cfg.InstanceID), zap.Int("worker_batch", cfg.WorkerBatch),
		zap.Duration("lease_timeout", cfg.LeaseTimeout), zap.String("git_commit", buildMeta.Commit),
		zap.String("git_branch", buildMeta.Branch), zap.String("git_tree_state", buildMeta.TreeState),
		zap.String("build_time", buildMeta.BuildTime), zap.String("go_version", buildMeta.GoVersion),
	)
	ctx, stop, _ := common.StartRuntimeSupport(cfg, logger)
	defer stop()
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
	chain, err := tonchain.New(ctx, tonChainConfig(cfg))
	if err != nil {
		return fmt.Errorf("initialize TON chain: %w", err)
	}
	defer chain.Close()
	if cfg.MintEnabled {
		lock, err := acquireRelayerLock(ctx, pool, cfg.Network, chain.RelayerAddress())
		if err != nil {
			return err
		}
		defer lock.Release(context.Background())
	}
	worker, err := tonworker.New(postgres.NewStarGiftTONStore(pool), chain, tonworker.Config{
		WorkerID: cfg.InstanceID, InitialItemIndex: cfg.InitialItemIndex, MintABI: cfg.MintABI,
		MintEnabled: cfg.MintEnabled, Batch: cfg.WorkerBatch,
		PollInterval: cfg.PollInterval, LeaseTimeout: cfg.LeaseTimeout, RequestTimeout: cfg.RequestTimeout,
		OnReady: func(state domain.StarGiftTONChainState) {
			logger.Info("telesrv TON worker ready",
				zap.String("instance_id", cfg.InstanceID), zap.Int("pid", os.Getpid()),
				zap.Uint("schema_version", migrationStatus.Version), zap.String("relayer", chain.RelayerAddress()),
				zap.String("collection", state.CollectionAddress), zap.String("next_item_index", state.ObservedNextItemIndex),
			)
		},
	}, logger.Named("ton").Named("worker"))
	if err != nil {
		return fmt.Errorf("initialize TON worker: %w", err)
	}
	return worker.Run(ctx)
}

func tonChainConfig(cfg config.TONConfig) tonchain.Config {
	liteServers := make([]tonchain.LiteServer, 0, len(cfg.LiteServers))
	for _, server := range cfg.LiteServers {
		liteServers = append(liteServers, tonchain.LiteServer{Address: server.Address, PublicKey: server.PublicKey})
	}
	return tonchain.Config{
		Network: domain.TONNetwork(cfg.Network), LiteServers: liteServers,
		TrustedBlock: tonchain.TrustedBlock{
			Workchain: cfg.TrustedBlock.Workchain, Shard: cfg.TrustedBlock.Shard, Seqno: cfg.TrustedBlock.Seqno,
			RootHash: cfg.TrustedBlock.RootHash, FileHash: cfg.TrustedBlock.FileHash,
		},
		CollectionAddress: cfg.CollectionAddress, CollectionCodeHash: cfg.CollectionCodeHash, MintABI: cfg.MintABI,
		RelayerMnemonicFile: cfg.RelayerMnemonicFile, RelayerMinBalance: cfg.RelayerMinBalance,
		RelayerMessageTTL: cfg.RelayerMessageTTL, MintSendAmount: cfg.MintSendAmount,
		MintForwardAmount: cfg.MintForwardAmount, FinalityDepth: cfg.FinalityDepth,
	}
}

type relayerLock struct {
	conn *pgxpool.Conn
	key  string
}

func acquireRelayerLock(ctx context.Context, pool *pgxpool.Pool, network, relayer string) (*relayerLock, error) {
	if pool == nil || relayer == "" {
		return nil, fmt.Errorf("TON relayer lock requires a database and wallet address")
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire TON relayer lock connection: %w", err)
	}
	key := "telesrv-ton-relayer:" + network + ":" + relayer
	var acquired bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1,0))`, key).Scan(&acquired); err != nil {
		conn.Release()
		return nil, fmt.Errorf("acquire TON relayer advisory lock: %w", err)
	}
	if !acquired {
		conn.Release()
		return nil, fmt.Errorf("another telesrv-ton instance already owns relayer %s", relayer)
	}
	return &relayerLock{conn: conn, key: key}, nil
}

func (l *relayerLock) Release(ctx context.Context) {
	if l == nil || l.conn == nil {
		return
	}
	releaseCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var released bool
	_ = l.conn.QueryRow(releaseCtx, `SELECT pg_advisory_unlock(hashtextextended($1,0))`, l.key).Scan(&released)
	l.conn.Release()
	l.conn = nil
}
