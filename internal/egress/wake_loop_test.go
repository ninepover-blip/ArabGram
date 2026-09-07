package egress

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"telesrv/internal/store"
)

func TestWakeScheduledLoopMarksOnlyDurableDeadlineWakeForRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wake := make(chan struct{}, 1)
	var calls atomic.Int32
	recovered := make(chan struct{}, 1)
	dispatch := func(_ context.Context, durableDeadlineWake bool) bool {
		calls.Add(1)
		if durableDeadlineWake {
			select {
			case recovered <- struct{}{}:
			default:
			}
			cancel()
		}
		return false
	}
	readyAt := time.Now().Add(12 * time.Millisecond)
	nextReady := func(context.Context) (store.OutboxNextReady, bool, error) {
		now := time.Now()
		return store.OutboxNextReady{ObservedAt: now, ReadyAt: readyAt, Kind: store.OutboxReadyRecoverLease}, true, nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		runWakeScheduledDispatchLoop(ctx, wake, dispatch, nextReady, nil)
	}()
	select {
	case <-recovered:
	case <-time.After(time.Second):
		t.Fatal("durable deadline did not request finalizable recovery")
	}
	<-done
	if got := calls.Load(); got != 1 {
		t.Fatalf("dispatch calls = %d, want only the due durable deadline", got)
	}
}

func TestWakeScheduledLoopDiscoversNewLaneWithoutExternalWake(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wake := make(chan struct{}, 1)
	var ready atomic.Bool
	var calls atomic.Int32
	discovered := make(chan struct{}, 1)
	dispatch := func(_ context.Context, _ bool) bool {
		calls.Add(1)
		if ready.Load() {
			select {
			case discovered <- struct{}{}:
			default:
			}
			cancel()
		}
		return false
	}
	nextReady := func(context.Context) (store.OutboxNextReady, bool, error) {
		if !ready.Load() {
			return store.OutboxNextReady{}, false, nil
		}
		now := time.Now()
		return store.OutboxNextReady{ObservedAt: now, ReadyAt: now, Kind: store.OutboxReadyClaim}, true, nil
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		runWakeScheduledDispatchLoop(ctx, wake, dispatch, nextReady, nil)
	}()
	time.Sleep(15 * time.Millisecond)
	if got := calls.Load(); got != 0 {
		t.Fatalf("idle scheduler issued %d blind claims", got)
	}
	ready.Store(true)
	select {
	case <-discovered:
	case <-time.After(2 * maximumOutboxIdleProbe):
		t.Fatal("durable lane was not discovered by the bounded idle probe")
	}
	<-done
}
