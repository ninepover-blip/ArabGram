package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"

	"telesrv/internal/store"
)

var ErrCounterRecoveryClosed = errors.New("postgres counter recovery: closed")

// CounterRecovery owns the PostgreSQL pool used only to recover Redis-backed
// durable counters after a Redis key loss. It must never share the business
// transaction pool: allocator calls happen inside message/channel transactions,
// so sharing can deadlock when every business connection is already acquired.
type CounterRecovery struct {
	pool      *pgxpool.Pool
	closed    atomic.Bool
	closeOnce sync.Once

	boxIDs            store.CounterSource
	channelIDs        store.CounterSource
	channelMessageIDs store.CounterSource
}

// OpenCounterRecovery opens and verifies a physically independent recovery
// pool. maxConns must be explicit because silently using the pgx default makes
// the production connection budget unknowable.
func OpenCounterRecovery(ctx context.Context, dsn string, maxConns int) (*CounterRecovery, error) {
	if maxConns <= 0 {
		return nil, fmt.Errorf("open postgres counter recovery: max connections must be positive, got %d", maxConns)
	}
	pool, err := Open(ctx, dsn, WithMaxConns(maxConns))
	if err != nil {
		return nil, fmt.Errorf("open postgres counter recovery pool: %w", err)
	}
	r := &CounterRecovery{pool: pool}
	r.boxIDs = guardedCounterSource{owner: r, name: "message box", source: NewMessageBoxCounterSource(pool)}
	r.channelIDs = guardedCounterSource{owner: r, name: "channel id", source: NewChannelIDCounterSource(pool)}
	r.channelMessageIDs = guardedCounterSource{owner: r, name: "channel message id", source: NewChannelMessageIDCounterSource(pool)}
	return r, nil
}

// Close rejects new recovery work before waiting for in-flight pool users. It
// is idempotent and intentionally has no fallback source after shutdown.
func (r *CounterRecovery) Close() {
	if r == nil {
		return
	}
	r.closeOnce.Do(func() {
		r.closed.Store(true)
		r.pool.Close()
	})
}

func (r *CounterRecovery) MessageBoxSource() store.CounterSource {
	if r == nil {
		return guardedCounterSource{name: "message box"}
	}
	return r.boxIDs
}

func (r *CounterRecovery) ChannelIDSource() store.CounterSource {
	if r == nil {
		return guardedCounterSource{name: "channel id"}
	}
	return r.channelIDs
}

func (r *CounterRecovery) ChannelMessageIDSource() store.CounterSource {
	if r == nil {
		return guardedCounterSource{name: "channel message id"}
	}
	return r.channelMessageIDs
}

// Stat exposes only pool telemetry; callers cannot acquire a recovery
// connection or accidentally run business SQL through this owner.
func (r *CounterRecovery) Stat() *pgxpool.Stat {
	if r == nil || r.pool == nil {
		return nil
	}
	return r.pool.Stat()
}

type guardedCounterSource struct {
	owner  *CounterRecovery
	name   string
	source store.CounterSource
}

func (s guardedCounterSource) Current(ctx context.Context, ownerID int64) (int, error) {
	if err := s.ready(); err != nil {
		return 0, err
	}
	value, err := s.source.Current(ctx, ownerID)
	if err != nil {
		return 0, fmt.Errorf("postgres counter recovery %s current: %w", s.name, err)
	}
	return value, nil
}

func (s guardedCounterSource) CurrentBatch(ctx context.Context, ownerIDs []int64) (map[int64]int, error) {
	if err := s.ready(); err != nil {
		return nil, err
	}
	values, err := s.source.CurrentBatch(ctx, ownerIDs)
	if err != nil {
		return nil, fmt.Errorf("postgres counter recovery %s batch: %w", s.name, err)
	}
	return values, nil
}

func (s guardedCounterSource) ready() error {
	if s.owner == nil || s.source == nil || s.owner.pool == nil {
		return fmt.Errorf("postgres counter recovery %s: unavailable", s.name)
	}
	if s.owner.closed.Load() {
		return fmt.Errorf("postgres counter recovery %s: %w", s.name, ErrCounterRecoveryClosed)
	}
	return nil
}
