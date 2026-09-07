package egress

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"telesrv/internal/store"
)

func TestWakeScheduledLoopExactProbeAndErrorSchedule(t *testing.T) {
	for _, tc := range []struct {
		name   string
		at     []time.Duration
		errors map[int]bool
	}{
		{"idle", []time.Duration{0, 4, 12, 28, 60, 110, 160}, nil},
		{"errors-cap", []time.Duration{0, 100, 300, 700, 1500, 3100, 6300, 11300, 16300}, map[int]bool{0: true, 1: true, 2: true, 3: true, 4: true, 5: true, 6: true, 7: true, 8: true}},
		{"success-resets-error", []time.Duration{0, 100, 104, 204}, map[int]bool{0: true, 2: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				epoch := time.Now()
				var at []time.Duration
				errorCount := 0
				next := func(context.Context) (store.OutboxNextReady, bool, error) {
					index := len(at)
					at = append(at, time.Since(epoch))
					if len(at) == len(tc.at) {
						cancel()
					}
					if tc.errors[index] {
						return store.OutboxNextReady{}, false, errors.New("schedule unavailable")
					}
					return store.OutboxNextReady{}, false, nil
				}
				runWakeScheduledDispatchLoop(ctx, make(chan struct{}), func(context.Context, bool) bool { t.Error("blind dispatch"); return false }, next, func(error) { errorCount++ })
				want := make([]time.Duration, len(tc.at))
				for i, d := range tc.at {
					want[i] = d * time.Millisecond
				}
				if !reflect.DeepEqual(at, want) || errorCount != len(tc.errors) {
					t.Fatalf("query schedule=%v errors=%d, want %v/%d", at, errorCount, want, len(tc.errors))
				}
			})
		})
	}
}

func TestWakeScheduledLoopFutureDeadlineAndQueryCost(t *testing.T) {
	for _, tc := range []struct {
		name           string
		cost, deadline time.Duration
		at             []time.Duration
		dispatchAt     time.Duration
		kind           store.OutboxReadyKind
	}{
		{"bounded-recovery", 0, 200 * time.Millisecond, []time.Duration{0, 50, 100, 150, 200}, 200 * time.Millisecond, store.OutboxReadyRecoverLease},
		{"subtract-query-cost", 8 * time.Millisecond, 20 * time.Millisecond, []time.Duration{0, 20}, 28 * time.Millisecond, store.OutboxReadyClaim},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				epoch := time.Now()
				var at []time.Duration
				next := func(context.Context) (store.OutboxNextReady, bool, error) {
					observed := time.Now()
					at = append(at, observed.Sub(epoch))
					time.Sleep(tc.cost)
					return store.OutboxNextReady{ObservedAt: observed, ReadyAt: epoch.Add(tc.deadline), Kind: tc.kind}, true, nil
				}
				calls := 0
				runWakeScheduledDispatchLoop(ctx, make(chan struct{}), func(_ context.Context, recoverLease bool) bool {
					calls++
					if recoverLease != (tc.kind == store.OutboxReadyRecoverLease) || time.Since(epoch) != tc.dispatchAt {
						t.Errorf("wrong reason/time: recover=%v at=%s", recoverLease, time.Since(epoch))
					}
					cancel()
					return false
				}, next, func(err error) { t.Error(err) })
				want := make([]time.Duration, len(tc.at))
				for i, d := range tc.at {
					want[i] = d * time.Millisecond
				}
				if calls != 1 || !reflect.DeepEqual(at, want) {
					t.Fatalf("dispatches=%d queries=%v, want 1/%v", calls, at, want)
				}
			})
		})
	}
}

func TestWakeScheduledLoopWakeStopsOldDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		epoch := time.Now()
		wake := make(chan struct{}, 1)
		time.AfterFunc(time.Millisecond, func() { wake <- struct{}{} })
		var at []time.Duration
		next := func(context.Context) (store.OutboxNextReady, bool, error) {
			at = append(at, time.Since(epoch))
			if len(at) == 3 {
				cancel()
			}
			if len(at) == 1 {
				return store.OutboxNextReady{}, false, nil
			}
			return store.OutboxNextReady{ObservedAt: time.Now(), ReadyAt: epoch.Add(time.Hour), Kind: store.OutboxReadyClaim}, true, nil
		}
		runWakeScheduledDispatchLoop(ctx, wake, func(context.Context, bool) bool { t.Error("early claim"); return false }, next, func(err error) { t.Error(err) })
		if !reflect.DeepEqual(at, []time.Duration{0, time.Millisecond, 51 * time.Millisecond}) {
			t.Fatalf("stale timer wake: %v", at)
		}
	})
}

func TestWakeScheduledLoopDispatchRetryAndDrain(t *testing.T) {
	for _, worked := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty-dispatch", true: "drain"}[worked], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				epoch := time.Now()
				var at []time.Duration
				next := func(context.Context) (store.OutboxNextReady, bool, error) {
					now := time.Now()
					return store.OutboxNextReady{ObservedAt: now, ReadyAt: now, Kind: store.OutboxReadyClaim}, true, nil
				}
				runWakeScheduledDispatchLoop(ctx, make(chan struct{}), func(_ context.Context, recoverLease bool) bool {
					if recoverLease {
						t.Error("ordinary claim requested recovery")
					}
					at = append(at, time.Since(epoch))
					if len(at) == 4 {
						cancel()
					}
					return worked
				}, next, func(err error) { t.Error(err) })
				want := []time.Duration{0, 0, 0, 0}
				if !worked {
					want = []time.Duration{0, time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond}
				}
				if !reflect.DeepEqual(at, want) {
					t.Fatalf("dispatch cadence=%v, want %v", at, want)
				}
			})
		})
	}
}

func TestWakeScheduledLoopTerminalBoundaries(t *testing.T) {
	for _, mode := range []string{"cancel", "closed-wake", "invalid-kind", "cancel-query"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				wake := make(chan struct{})
				queries, errs := 0, 0
				epoch := time.Now()
				if mode == "cancel" || mode == "cancel-query" {
					time.AfterFunc(7*time.Millisecond, cancel)
				}
				if mode == "closed-wake" {
					time.AfterFunc(7*time.Millisecond, func() { close(wake) })
				}
				next := func(ctx context.Context) (store.OutboxNextReady, bool, error) {
					queries++
					if mode == "cancel-query" {
						<-ctx.Done()
						return store.OutboxNextReady{}, false, ctx.Err()
					}
					if mode == "invalid-kind" {
						return store.OutboxNextReady{ObservedAt: epoch, ReadyAt: epoch, Kind: store.OutboxReadyKind(255)}, true, nil
					}
					return store.OutboxNextReady{ObservedAt: time.Now(), ReadyAt: epoch.Add(time.Hour), Kind: store.OutboxReadyClaim}, true, nil
				}
				runWakeScheduledDispatchLoop(ctx, wake, func(context.Context, bool) bool { t.Error("unexpected dispatch"); return false }, next, func(error) { errs++ })
				wantTime := 7 * time.Millisecond
				wantErrors := 0
				if mode == "invalid-kind" {
					wantTime = 0
					wantErrors = 1
				}
				if mode == "cancel-query" {
					wantErrors = 1
				}
				if queries != 1 || errs != wantErrors || time.Since(epoch) != wantTime {
					t.Fatalf("terminal query/error/time=%d/%d/%s", queries, errs, time.Since(epoch))
				}
				time.Sleep(time.Second)
				synctest.Wait()
				if queries != 1 {
					t.Fatal("scheduler survived exit")
				}
			})
		})
	}
}
