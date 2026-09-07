package egress

import (
	"context"
	"errors"
	"testing"
	"time"

	"telesrv/internal/store"
)

// Measures the actual scheduler with a same-interface readiness function and
// a wake per observation, excluding network/PG and waiting time. The workload
// exercises each timer rearm branch; it is not a throughput/capacity benchmark.
func BenchmarkWakeScheduledLoopRearm(b *testing.B) {
	for _, mode := range []string{"idle", "schedule-error", "future", "empty-dispatch"} {
		b.Run(mode, func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			wake := make(chan struct{}, 1)
			count := 0
			scheduleErr := errors.New("benchmark unavailable")
			next := func(context.Context) (store.OutboxNextReady, bool, error) {
				count++
				if count == b.N {
					cancel()
				} else {
					select {
					case wake <- struct{}{}:
					default:
					}
				}
				switch mode {
				case "idle":
					return store.OutboxNextReady{}, false, nil
				case "schedule-error":
					return store.OutboxNextReady{}, false, scheduleErr
				case "future":
					now := time.Now()
					return store.OutboxNextReady{ObservedAt: now, ReadyAt: now.Add(time.Hour), Kind: store.OutboxReadyClaim}, true, nil
				default:
					now := time.Now()
					return store.OutboxNextReady{ObservedAt: now, ReadyAt: now, Kind: store.OutboxReadyClaim}, true, nil
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			runWakeScheduledDispatchLoop(ctx, wake, func(context.Context, bool) bool { return false }, next, func(error) {})
			b.StopTimer()
			if count != b.N {
				b.Fatalf("observations=%d, want %d", count, b.N)
			}
		})
	}
}
