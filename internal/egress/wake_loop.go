package egress

import (
	"context"
	"errors"
	"time"

	"telesrv/internal/store"
)

const (
	minimumOutboxScheduleDelay = time.Millisecond
	outboxActiveProbeInterval  = 4 * time.Millisecond
	maximumOutboxIdleProbe     = 50 * time.Millisecond
	outboxScheduleErrorBackoff = 100 * time.Millisecond
	maxOutboxScheduleBackoff   = 5 * time.Second
)

type outboxNextReady func(context.Context) (store.OutboxNextReady, bool, error)

// runWakeScheduledDispatchLoop combines volatile local/Redis wakeups with one
// database-derived ready/lease/retry deadline. The bounded probe is the
// crash-safe discovery path for new durable lane heads; PostgreSQL NOTIFY is
// deliberately not part of the contract.
func runWakeScheduledDispatchLoop(
	ctx context.Context,
	wake <-chan struct{},
	dispatch func(context.Context, bool) bool,
	nextReady outboxNextReady,
	onScheduleError func(error),
) {
	if wake == nil || dispatch == nil || nextReady == nil {
		return
	}
	// The scheduler owns its bootstrap. External wake channels are optional
	// latency hints after startup; they must never be required to discover the
	// first durable head or crash-recovery deadline.
	timer := time.NewTimer(0)
	// This goroutine exclusively owns the timer. Reuse it across every rearm
	// path instead of allocating a timer and channel on each idle probe.
	stopTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
	defer stopTimer()

	errorBackoff := outboxScheduleErrorBackoff
	idleProbe := outboxActiveProbeInterval
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-wake:
			if !ok {
				return
			}
			idleProbe = outboxActiveProbeInterval
		case <-timer.C:
		}
		stopTimer()

		for {
			if ctx.Err() != nil {
				return
			}
			queryStarted := time.Now()
			next, ok, err := nextReady(ctx)
			if err != nil {
				if onScheduleError != nil {
					onScheduleError(err)
				}
				timer.Reset(errorBackoff)
				if errorBackoff < maxOutboxScheduleBackoff {
					errorBackoff *= 2
					if errorBackoff > maxOutboxScheduleBackoff {
						errorBackoff = maxOutboxScheduleBackoff
					}
				}
				break
			}
			errorBackoff = outboxScheduleErrorBackoff
			if !ok {
				timer.Reset(idleProbe)
				idleProbe *= 2
				if idleProbe > maximumOutboxIdleProbe {
					idleProbe = maximumOutboxIdleProbe
				}
				break
			}
			idleProbe = outboxActiveProbeInterval
			delay := next.Delay() - time.Since(queryStarted)
			if delay > 0 {
				// A new Core commit can introduce an earlier ready lane without a
				// signal, so even a known future lease deadline is rechecked within
				// the bounded discovery interval.
				if delay > maximumOutboxIdleProbe {
					delay = maximumOutboxIdleProbe
				}
				timer.Reset(delay)
				break
			}
			if next.Kind != store.OutboxReadyClaim && next.Kind != store.OutboxReadyRecoverLease {
				if onScheduleError != nil {
					onScheduleError(errors.New("egress: durable scheduler returned invalid ready kind"))
				}
				return
			}
			if !dispatch(ctx, next.Kind == store.OutboxReadyRecoverLease) {
				timer.Reset(minimumOutboxScheduleDelay)
				break
			}
		}
	}
}
