package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/observability/dbtrace"
)

func TestQueryStatsTracerRecordsContextStats(t *testing.T) {
	ctx, stats := dbtrace.WithStats(context.Background())
	tracer := queryStatsTracer{}

	ctx = tracer.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{})

	snap := stats.Snapshot()
	if snap.Queries != 1 || snap.Errors != 0 {
		t.Fatalf("Snapshot() = %+v, want 1 query without errors", snap)
	}

	ctx = tracer.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: errors.New("boom")})

	snap = stats.Snapshot()
	if snap.Queries != 2 || snap.Errors != 1 {
		t.Fatalf("Snapshot() = %+v, want 2 queries with 1 error", snap)
	}
}

func TestQueryStatsTracerIgnoresContextWithoutStats(t *testing.T) {
	tracer := queryStatsTracer{}
	ctx := tracer.TraceQueryStart(context.Background(), nil, pgx.TraceQueryStartData{})
	tracer.TraceQueryEnd(ctx, nil, pgx.TraceQueryEndData{Err: errors.New("boom")})
}
