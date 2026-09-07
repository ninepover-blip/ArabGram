package edge

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"telesrv/internal/coreexec"
)

func TestEdgeSessionLifecycleReporterCoalescesAndRetries(t *testing.T) {
	sender := &captureSessionOfflineSender{failures: 1, success: make(chan []coreexec.SessionOfflineEvent, 1)}
	reporter, err := newEdgeSessionLifecycleReporter(sender, zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go reporter.Run(ctx)

	raw := [8]byte{1}
	if err := reporter.Submit(coreexec.SessionOfflineEvent{RawAuthKeyID: raw, SessionID: 10, UserID: 20, DisconnectedAt: 100}); err != nil {
		t.Fatal(err)
	}
	if err := reporter.Submit(coreexec.SessionOfflineEvent{RawAuthKeyID: raw, SessionID: 10, UserID: 20, LastForUser: true, DisconnectedAt: 200}); err != nil {
		t.Fatal(err)
	}
	if err := reporter.Submit(coreexec.SessionOfflineEvent{RawAuthKeyID: [8]byte{2}, SessionID: 11, UserID: 21, DisconnectedAt: 150}); err != nil {
		t.Fatal(err)
	}

	var got []coreexec.SessionOfflineEvent
	select {
	case got = <-sender.success:
	case <-time.After(2 * time.Second):
		t.Fatal("lifecycle batch was not retried")
	}
	if len(got) != 2 {
		t.Fatalf("batch size = %d, want 2: %+v", len(got), got)
	}
	var coalesced coreexec.SessionOfflineEvent
	for _, event := range got {
		if event.RawAuthKeyID == raw && event.SessionID == 10 {
			coalesced = event
		}
	}
	if coalesced.DisconnectedAt != 200 || !coalesced.LastForUser {
		t.Fatalf("coalesced event = %+v", coalesced)
	}
	waitForEdgeLifecyclePending(t, reporter, 0)
	cancel()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := reporter.Wait(waitCtx); err != nil {
		t.Fatal(err)
	}
	if sender.calls() < 2 {
		t.Fatalf("sender calls = %d, want retry", sender.calls())
	}
}

func TestEdgeSessionLifecycleReporterDrainsAfterCancellation(t *testing.T) {
	sender := &captureSessionOfflineSender{success: make(chan []coreexec.SessionOfflineEvent, 1)}
	reporter, err := newEdgeSessionLifecycleReporter(sender, zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := reporter.Submit(coreexec.SessionOfflineEvent{
		RawAuthKeyID: [8]byte{3}, SessionID: 30, UserID: 40, DisconnectedAt: 300,
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	go reporter.Run(ctx)
	waitCtx, waitCancel := context.WithTimeout(context.Background(), time.Second)
	defer waitCancel()
	if err := reporter.Wait(waitCtx); err != nil {
		t.Fatal(err)
	}
	if reporter.Pending() != 0 {
		t.Fatalf("pending after drain = %d", reporter.Pending())
	}
	select {
	case batch := <-sender.success:
		if len(batch) != 1 || batch[0].DisconnectedAt != 300 {
			t.Fatalf("drained batch = %+v", batch)
		}
	default:
		t.Fatal("shutdown drain did not send the event")
	}
	if err := reporter.Submit(coreexec.SessionOfflineEvent{
		RawAuthKeyID: [8]byte{4}, SessionID: 31, UserID: 41, DisconnectedAt: 301,
	}); !errors.Is(err, errEdgeSessionLifecycleStopped) {
		t.Fatalf("submit after stop = %v", err)
	}
}

func TestEdgeSessionLifecycleObserverSeparatesLocalCodecAndCoreBusinessFacts(t *testing.T) {
	sender := &captureSessionOfflineSender{}
	reporter, err := newEdgeSessionLifecycleReporter(sender, zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	local := &captureLocalSessionLifecycle{}
	observer, err := newEdgeSessionLifecycleObserver(local, reporter, zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	observer.now = func() time.Time { return time.Unix(400, 0) }
	raw := [8]byte{5}
	observer.SessionOffline(raw, 50, 60, true)
	if local.userID != 0 || local.rawAuthKeyID != raw || local.sessionID != 50 {
		t.Fatalf("local codec event = raw:%v session:%d user:%d", local.rawAuthKeyID, local.sessionID, local.userID)
	}
	batch := reporter.takeBatch(1)
	if len(batch) != 1 || batch[0] != (coreexec.SessionOfflineEvent{
		RawAuthKeyID: raw, SessionID: 50, UserID: 60, LastForUser: true, DisconnectedAt: 400,
	}) {
		t.Fatalf("Core lifecycle event = %+v", batch)
	}
	reporter.completeBatch(len(batch))
	observer.SessionDestroyed(raw, 50)
	if !local.destroyed {
		t.Fatal("local codec did not receive session destruction")
	}
}

func waitForEdgeLifecyclePending(t *testing.T, reporter *edgeSessionLifecycleReporter, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for reporter.Pending() != want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := reporter.Pending(); got != want {
		t.Fatalf("pending = %d, want %d", got, want)
	}
}

type captureSessionOfflineSender struct {
	mu       sync.Mutex
	failures int
	count    int
	success  chan []coreexec.SessionOfflineEvent
}

func (s *captureSessionOfflineSender) ReportSessionOfflineBatch(_ context.Context, events []coreexec.SessionOfflineEvent) error {
	s.mu.Lock()
	s.count++
	if s.failures > 0 {
		s.failures--
		s.mu.Unlock()
		return errors.New("unavailable")
	}
	success := s.success
	s.mu.Unlock()
	if success != nil {
		success <- append([]coreexec.SessionOfflineEvent(nil), events...)
	}
	return nil
}

func (s *captureSessionOfflineSender) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

type captureLocalSessionLifecycle struct {
	rawAuthKeyID [8]byte
	sessionID    int64
	userID       int64
	destroyed    bool
}

func (c *captureLocalSessionLifecycle) SessionOffline(rawAuthKeyID [8]byte, sessionID, userID int64, _ bool) {
	c.rawAuthKeyID = rawAuthKeyID
	c.sessionID = sessionID
	c.userID = userID
}

func (c *captureLocalSessionLifecycle) SessionDestroyed(_ [8]byte, _ int64) {
	c.destroyed = true
}
