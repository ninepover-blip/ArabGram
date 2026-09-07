package dbtrace

import (
	"context"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

type contextKey struct{}

// Stats tracks database work done under one request context.
type Stats struct {
	queries atomic.Int64
	errors  atomic.Int64
	nanos   atomic.Int64
}

// Snapshot is an immutable view of request-scoped database work.
type Snapshot struct {
	Queries  int64
	Errors   int64
	Duration time.Duration
}

// WithStats installs a fresh Stats collector into ctx.
func WithStats(ctx context.Context) (context.Context, *Stats) {
	stats := &Stats{}
	return context.WithValue(ctx, contextKey{}, stats), stats
}

// FromContext returns the request-scoped Stats collector when one is present.
func FromContext(ctx context.Context) (*Stats, bool) {
	stats, ok := ctx.Value(contextKey{}).(*Stats)
	return stats, ok && stats != nil
}

// SnapshotFromContext returns a zero snapshot when ctx has no Stats collector.
func SnapshotFromContext(ctx context.Context) Snapshot {
	stats, ok := FromContext(ctx)
	if !ok {
		return Snapshot{}
	}
	return stats.Snapshot()
}

// Add records one database operation.
func (s *Stats) Add(d time.Duration, err error) {
	if s == nil {
		return
	}
	s.queries.Add(1)
	if err != nil {
		s.errors.Add(1)
	}
	if d > 0 {
		s.nanos.Add(d.Nanoseconds())
	}
}

// Snapshot returns the current counters.
func (s *Stats) Snapshot() Snapshot {
	if s == nil {
		return Snapshot{}
	}
	return Snapshot{
		Queries:  s.queries.Load(),
		Errors:   s.errors.Load(),
		Duration: time.Duration(s.nanos.Load()),
	}
}

// Sub returns the work added after before. Negative values are clamped so that
// logging stays sane if a future collector resets counters.
func (s Snapshot) Sub(before Snapshot) Snapshot {
	after := Snapshot{
		Queries:  s.Queries - before.Queries,
		Errors:   s.Errors - before.Errors,
		Duration: s.Duration - before.Duration,
	}
	if after.Queries < 0 {
		after.Queries = 0
	}
	if after.Errors < 0 {
		after.Errors = 0
	}
	if after.Duration < 0 {
		after.Duration = 0
	}
	return after
}

// Empty reports whether there is database work worth logging.
func (s Snapshot) Empty() bool {
	return s.Queries == 0 && s.Errors == 0
}

// AppendZapFields adds db_* fields to an existing zap field slice.
func AppendZapFields(fields []zap.Field, prefix string, snap Snapshot) []zap.Field {
	if snap.Empty() {
		return fields
	}
	return append(fields,
		zap.Int64(prefix+"db_queries", snap.Queries),
		zap.Duration(prefix+"db_time", snap.Duration),
		zap.Int64(prefix+"db_errors", snap.Errors),
	)
}
