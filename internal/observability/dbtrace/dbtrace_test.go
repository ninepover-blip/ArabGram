package dbtrace

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestStatsSnapshotAndDelta(t *testing.T) {
	ctx, stats := WithStats(context.Background())
	if got, ok := FromContext(ctx); !ok || got != stats {
		t.Fatalf("FromContext() = (%v, %v), want installed stats", got, ok)
	}

	stats.Add(10*time.Millisecond, nil)
	before := stats.Snapshot()
	stats.Add(5*time.Millisecond, errors.New("boom"))

	snap := stats.Snapshot()
	if snap.Queries != 2 || snap.Errors != 1 || snap.Duration != 15*time.Millisecond {
		t.Fatalf("Snapshot() = %+v, want 2 queries, 1 error, 15ms", snap)
	}

	delta := snap.Sub(before)
	if delta.Queries != 1 || delta.Errors != 1 || delta.Duration != 5*time.Millisecond {
		t.Fatalf("delta = %+v, want 1 query, 1 error, 5ms", delta)
	}
}

func TestSnapshotFromContextWithoutStats(t *testing.T) {
	if snap := SnapshotFromContext(context.Background()); !snap.Empty() {
		t.Fatalf("SnapshotFromContext() = %+v, want empty", snap)
	}
}
