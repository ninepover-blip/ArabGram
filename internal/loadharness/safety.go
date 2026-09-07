package loadharness

import (
	"context"
	"sync"
	"time"
)

// All sources share one terminal stop. Triggering cancellation does not erase
// in-flight outcomes; the owner still joins workers and audits their evidence.
type loadSafety struct {
	mu      sync.Mutex
	cancel  context.CancelFunc
	stopped chan struct{}
	result  SafetyStopReport
}

type SafetyStopReport struct {
	Source           string    `json:"source,omitempty"`
	Class            string    `json:"class,omitempty"`
	At               time.Time `json:"at,omitempty"`
	WorkersStoppedAt time.Time `json:"workers_stopped_at,omitempty"`
}

func newLoadSafety(parent context.Context) (context.Context, *loadSafety) {
	ctx, cancel := context.WithCancel(parent)
	return ctx, &loadSafety{cancel: cancel, stopped: make(chan struct{})}
}

func (s *loadSafety) trip(source, class string, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.result.Source != "" {
		return
	}
	s.result.Source, s.result.Class, s.result.At = source, class, at
	s.cancel()
	close(s.stopped)
}

func (s *loadSafety) eventsFailure(class string, at time.Time) { s.trip("events", class, at) }
func (s *loadSafety) metricsFailure(target, class string) {
	s.trip("metrics", target+":"+class, time.Now().UTC())
}
func (s *loadSafety) invariantFailure(class string) {
	s.trip("delivery_invariant", class, time.Now().UTC())
}
func (s *loadSafety) deliveryFailure() {
	s.trip("delivery_ledger", "evidence_failure", time.Now().UTC())
}
func (s *loadSafety) workersStopped() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.result.WorkersStoppedAt.IsZero() {
		s.result.WorkersStoppedAt = time.Now().UTC()
	}
}
func (s *loadSafety) report() SafetyStopReport {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.result
}

func eventEvidenceFailures(e EventEvidenceReport, written, dropped uint64) []string {
	if e.Version != eventEvidenceVersion || !e.Finalized || e.Error != "" || e.Written != written || e.Dropped != dropped {
		return []string{"event evidence is incomplete or failed its final audit"}
	}
	return nil
}
