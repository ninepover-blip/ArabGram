package core

import (
	"net/http/httptest"
	"strings"
	"testing"

	obsmetrics "telesrv/internal/observability/metrics"
	"telesrv/internal/rpc"
)

type capturedPresenceWork struct {
	snapshot rpc.PresenceWorkSnapshot
	reads    int
}

func (s *capturedPresenceWork) PresenceWorkSnapshot() rpc.PresenceWorkSnapshot {
	s.reads++
	return s.snapshot
}

func TestPresenceWorkMetricsUseOneSnapshotIncludingZero(t *testing.T) {
	r := obsmetrics.New()
	s := &capturedPresenceWork{}
	registerPresenceWorkMetrics(r, s)
	for _, state := range []rpc.PresenceWorkSnapshot{{}, {WaitingTimers: 2, RunningCallbacks: 3, DirectWrites: 5, PendingLastSeen: 7}} {
		s.snapshot = state
		before := s.reads
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
		if s.reads != before+1 {
			t.Fatal("provider did not use exactly one snapshot")
		}
		body := w.Body.String()
		if !strings.Contains(body, "# TYPE telesrv_presence_work gauge") {
			t.Fatal("missing metric schema")
		}
		want := []string{`telesrv_presence_work{stage="grace"} 2`, `telesrv_presence_work{stage="callback"} 3`, `telesrv_presence_work{stage="direct_write"} 5`, `telesrv_presence_work{stage="last_seen_batch"} 7`, `telesrv_presence_work_pending 17`}
		if state == (rpc.PresenceWorkSnapshot{}) {
			want = []string{`telesrv_presence_work{stage="grace"} 0`, `telesrv_presence_work{stage="callback"} 0`, `telesrv_presence_work{stage="direct_write"} 0`, `telesrv_presence_work{stage="last_seen_batch"} 0`, `telesrv_presence_work_pending 0`}
		}
		for _, line := range want {
			if !strings.Contains(body, line+"\n") {
				t.Fatalf("missing exact sample %q", line)
			}
		}
	}
}
