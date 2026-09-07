package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/observability/dbtrace"
)

type queryStatsTracer struct{}

type queryStatsStartKey struct{}

func (queryStatsTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	if _, ok := dbtrace.FromContext(ctx); !ok {
		return ctx
	}
	return context.WithValue(ctx, queryStatsStartKey{}, time.Now())
}

func (queryStatsTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryEndData) {
	stats, ok := dbtrace.FromContext(ctx)
	if !ok {
		return
	}
	start, _ := ctx.Value(queryStatsStartKey{}).(time.Time)
	if start.IsZero() {
		stats.Add(0, data.Err)
		return
	}
	stats.Add(time.Since(start), data.Err)
}
