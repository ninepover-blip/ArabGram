package loadharness

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunCancellationPreservesPhaseAndIndependentAudit(t *testing.T) {
	e, err := newEventWriter(filepath.Join(t.TempDir(), "events.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	defer e.close()
	parent, cancel := context.WithCancel(context.Background())
	work, s := newLoadSafety(parent)
	defer s.cancel()
	l := newRunLifecycle(parent, s, e)
	defer l.close()
	l.enter("settle")
	cancel()
	select {
	case <-s.stopped:
	case <-time.After(time.Second):
		t.Fatal("parent cancellation not recorded")
	}
	if work.Err() == nil {
		t.Fatal("workload not canceled")
	}
	final, stop := l.finalizeContext(context.WithoutCancel(parent))
	defer stop()
	if final.Err() != nil {
		t.Fatal("audit inherited workload cancellation")
	}
	deadline, ok := final.Deadline()
	if !ok || time.Until(deadline) > runFinalizationTimeout {
		t.Fatal("audit unbounded")
	}
	l.close()
	l.close()
	r := l.report()
	if !r.Interrupted || r.InterruptedPhase != "settle" || r.InterruptionClass != "canceled" || s.report().Source != "external" {
		t.Fatal(r, s.report())
	}
	if err := e.finalize(final); err != nil {
		t.Fatal(err)
	}
}

func TestCanceledRecoveryCannotWinCompletionTimerRace(t *testing.T) {
	e, err := newEventWriter(filepath.Join(t.TempDir(), "events.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	defer e.close()
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, s := newLoadSafety(parent)
	defer s.cancel()
	l := newRunLifecycle(parent, s, e)
	defer l.close()
	l.enter("recovery")
	l.recovery(true, false)
	cancel()
	// A select may choose its timer even when parent.Done is also ready.
	l.recovery(false, true)
	if l.report().RecoveryComplete {
		t.Fatal("canceled observation window marked complete")
	}
}

func TestSupervisorJoinsCanceledGenerationBeforeRestart(t *testing.T) {
	e, err := newEventWriter(filepath.Join(t.TempDir(), "events.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	defer e.close()
	w := &loadWorker{record: SessionRecord{Index: 0}, events: e, counters: &harnessCounters{}, signal: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan int, 4)
	closing := make(chan int, 4)
	release := make(chan struct{})
	var active, maximum, number atomic.Int32
	w.runClientOverride = func(ctx context.Context) error {
		n := int(number.Add(1))
		a := active.Add(1)
		maximum.Store(max(a, maximum.Load()))
		started <- n
		<-ctx.Done()
		closing <- n
		if n == 1 {
			<-release
		}
		active.Add(-1)
		return ctx.Err()
	}
	var wg sync.WaitGroup
	wg.Add(1)
	w.setOnline(true)
	go w.supervise(ctx, &wg)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first client did not start")
	}
	w.setOnline(false)
	select {
	case <-closing:
	case <-time.After(time.Second):
		t.Fatal("first client did not cancel")
	}
	// Restore desired=true while the old client is still closing. It must
	// neither overlap the old client nor wait forever on its consumed done.
	w.setOnline(true)
	select {
	case <-started:
		t.Fatal("overlapped old generation cleanup")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case n := <-started:
		if n != 2 {
			t.Fatal(n)
		}
	case <-time.After(time.Second):
		t.Fatal("restart lost after old cleanup")
	}
	cancel()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("supervisor did not join")
	}
	if active.Load() != 0 || maximum.Load() != 1 || w.clientGeneration.Load() != 2 || w.state.Load() != workerStopped || w.counters.fatalErrors.Load() != 0 {
		t.Fatal("invalid client lifetime accounting")
	}
}
