package loadharness

import (
	"context"
	"sync"
	"time"
)

const runFinalizationTimeout = 60 * time.Second

type RunLifecycleReport struct {
	Interrupted       bool      `json:"interrupted"`
	InterruptedPhase  string    `json:"interrupted_phase,omitempty"`
	InterruptionClass string    `json:"interruption_class,omitempty"`
	InterruptedAt     time.Time `json:"interrupted_at,omitempty"`
	FinalizationAt    time.Time `json:"finalization_at,omitempty"`
	FinalizationMS    int64     `json:"finalization_budget_ms"`
	RecoveryStartedAt time.Time `json:"recovery_started_at,omitempty"`
	RecoveryEndedAt   time.Time `json:"recovery_ended_at,omitempty"`
	RecoveryComplete  bool      `json:"recovery_complete"`
}

// Parent cancellation stops workload immediately, while a separately bounded
// read-only finalization context can still audit what actually happened.
type runLifecycle struct {
	mu        sync.Mutex
	parent    context.Context
	safety    *loadSafety
	events    *eventWriter
	phase     string
	sealed    bool
	closeOnce sync.Once
	rampEnd   time.Time
	result    RunLifecycleReport
	stopWatch func() bool
	watchDone chan struct{}
}

func newRunLifecycle(parent context.Context, safety *loadSafety, events *eventWriter) *runLifecycle {
	l := &runLifecycle{parent: parent, safety: safety, events: events, phase: "preflight", watchDone: make(chan struct{}), result: RunLifecycleReport{FinalizationMS: runFinalizationTimeout.Milliseconds()}}
	l.stopWatch = context.AfterFunc(parent, func() { defer close(l.watchDone); l.interrupt() })
	return l
}

func (l *runLifecycle) interrupt() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.parent.Err() == nil || l.sealed || l.result.Interrupted {
		return
	}
	l.result.Interrupted = true
	l.result.InterruptedPhase, l.result.InterruptionClass = l.phase, classifyError(l.parent.Err())
	if l.phase == "ramp" && !l.rampEnd.IsZero() && !time.Now().Before(l.rampEnd) {
		l.result.InterruptedPhase = "load"
	}
	l.result.InterruptedAt = time.Now().UTC()
	l.safety.trip("external", l.result.InterruptionClass, l.result.InterruptedAt)
	l.events.write(map[string]any{"type": "run_interrupted", "at": l.result.InterruptedAt, "phase": l.result.InterruptedPhase, "class": l.result.InterruptionClass})
}

func (l *runLifecycle) enter(phase string) {
	l.interrupt() // Preserve the phase that was canceled, even if watcher lags.
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.sealed {
		return
	}
	l.phase = phase
	l.events.write(map[string]any{"type": "run_phase", "at": time.Now().UTC(), "phase": phase})
}

func (l *runLifecycle) startWorkload(at time.Time, ramp time.Duration) {
	l.enter("ramp")
	l.mu.Lock()
	l.rampEnd = at.Add(ramp)
	l.mu.Unlock()
}

func (l *runLifecycle) recovery(start bool, complete bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if start {
		l.result.RecoveryStartedAt = time.Now().UTC()
	} else {
		l.result.RecoveryEndedAt = time.Now().UTC()
		// A timer and parent.Done may both be ready in the caller's select.
		// Cancellation observed before this completion boundary wins.
		l.result.RecoveryComplete = complete && l.parent.Err() == nil && !l.result.Interrupted
	}
}

func (l *runLifecycle) finalizeContext(parent context.Context) (context.Context, context.CancelFunc) {
	l.enter("finalize")
	l.mu.Lock()
	l.result.FinalizationAt = time.Now().UTC()
	l.mu.Unlock()
	return context.WithTimeout(parent, runFinalizationTimeout)
}

func (l *runLifecycle) close() {
	l.closeOnce.Do(func() {
		if !l.stopWatch() {
			<-l.watchDone
		}
		l.interrupt()
		l.mu.Lock()
		l.sealed = true
		l.mu.Unlock()
	})
}

func (l *runLifecycle) report() RunLifecycleReport {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.result
}
