package loadharness

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
)

func TestMetricsSafetyStopsWithoutWaitingForRunReport(t *testing.T) {
	for _, mode := range []string{"unavailable", "epoch", "reset", "missing", "dropped"} {
		t.Run(mode, func(t *testing.T) {
			var broken atomic.Bool
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if broken.Load() && mode == "unavailable" {
					http.Error(w, "down", 503)
					return
				}
				body := "# TYPE telesrv_process_start_time_seconds gauge\ntelesrv_process_start_time_seconds 10\n# TYPE telesrv_go_goroutines gauge\ntelesrv_go_goroutines 2\n# TYPE telesrv_coreexec_grpc_calls_total counter\ntelesrv_coreexec_grpc_calls_total 10\n# TYPE telesrv_metrics_dropped_observations_total counter\ntelesrv_metrics_dropped_observations_total 0\n"
				if broken.Load() {
					switch mode {
					case "epoch":
						body = strings.ReplaceAll(body, "seconds 10", "seconds 11")
					case "reset":
						body = strings.ReplaceAll(body, "calls_total 10", "calls_total 9")
					case "missing":
						body = strings.ReplaceAll(body, "telesrv_coreexec_grpc_calls_total 10\n", "")
					case "dropped":
						body = strings.ReplaceAll(body, "observations_total 0", "observations_total 1")
					}
				}
				_, _ = w.Write([]byte(body))
			}))
			defer srv.Close()
			c := newServerMetricsCollector(MetricsTargets{{Role: "core", Instance: "one", URL: srv.URL}})
			ctx, safety := newLoadSafety(context.Background())
			defer safety.cancel()
			c.onIssue = safety.metricsFailure
			if _, err := c.scrape(context.Background()); err != nil {
				t.Fatal(err)
			}
			if ctx.Err() != nil {
				t.Fatal("baseline stopped load")
			}
			broken.Store(true)
			_, _ = c.scrape(context.Background())
			if ctx.Err() == nil || safety.report().Source != "metrics" {
				t.Fatal("metrics failure did not synchronously cancel clients")
			}
		})
	}
}

func TestSchedulerSafetyPreservesPlannedOfflineAccounting(t *testing.T) {
	for _, mode := range []string{"planned_offline", "unexpected_offline", "queue_full"} {
		t.Run(mode, func(t *testing.T) {
			ctx, safety := newLoadSafety(context.Background())
			defer safety.cancel()
			w := &loadWorker{sendQueue: make(chan messageArrival, 1), allowPlannedOffline: mode == "planned_offline"}
			w.state.Store(workerReady)
			w.businessReady.Store(true)
			w.desired.Store(true)
			c := &harnessCounters{safety: safety}
			var wg sync.WaitGroup
			wg.Add(1)
			go runFixedMessageSchedule(ctx, &wg, 0, time.Second, 100, []*loadWorker{w}, []*loadWorker{w}, c, nil)
			defer wg.Wait()
			defer safety.cancel()
			if mode != "queue_full" {
				select {
				case <-w.sendQueue:
				case <-time.After(time.Second):
					t.Fatal("scheduler never started")
				}
				w.desired.Store(false)
				w.state.Store(workerOffline)
				w.businessReady.Store(false)
			}
			if mode == "planned_offline" {
				deadline := time.NewTimer(time.Second)
				defer deadline.Stop()
				tick := time.NewTicker(time.Millisecond)
				defer tick.Stop()
				for c.messageNotReady.Load() < 3 {
					select {
					case <-ctx.Done():
						t.Fatal("planned offline canceled the workload")
					case <-deadline.C:
						t.Fatal("planned offline lost rejected arrivals")
					case <-tick.C:
					}
				}
				if ctx.Err() != nil {
					t.Fatal("planned offline canceled the workload")
				}
				return
			}
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
				t.Fatal("scheduler did not stop an invalid load")
			}
			want := "sender_not_ready"
			if mode == "queue_full" {
				want = "sender_queue_full"
			}
			if got := safety.report(); got.Source != "workload" || got.Class != want {
				t.Fatalf("wrong cause: %+v", got)
			}
		})
	}
}

func TestBusinessRateCountsRealCallsAndRequiresSustainedWindow(t *testing.T) {
	g := &businessGuard{}
	start := time.Unix(1700000000, 0)
	fault := errors.New("rpc")
	for second := 0; second <= 30; second++ {
		now := start.Add(time.Duration(second) * time.Second)
		g.observe("messages.sendMessage", now, fault)
		for i := 0; i < 1000; i++ {
			g.observe("ping", now, nil)
			g.observe("messages.getDialogs.first", now, nil)
			g.observe("lifecycle.business_ready", now, nil)
			g.observe("messages.getDialogs", now, context.Canceled)
		}
		r := g.evaluate(now)
		if r.Calls != min(uint64(second+1), 30) || r.Errors != r.Calls || r.Tripped != (second == 30) {
			t.Fatalf("bad window at %d: %+v", second, r)
		}
	}
	if !g.evaluate(start.Add(time.Hour)).Tripped {
		t.Fatal("trip evidence was erased")
	}
	t.Run("exact_one_percent", func(t *testing.T) {
		g := &businessGuard{}
		for s := 0; s < 61; s++ {
			now := start.Add(time.Duration(s) * time.Second)
			for n := 0; n < 100; n++ {
				var err error
				if n == 0 {
					err = fault
				}
				g.observe("messages.getDialogs", now, err)
			}
			if g.evaluate(now).Tripped {
				t.Fatal("exact 1% tripped >1% gate")
			}
		}
	})
	t.Run("empty_gap_resets", func(t *testing.T) {
		g := &businessGuard{}
		g.observe("messages.sendMessage", start, fault)
		g.evaluate(start)
		if r := g.evaluate(start.Add(30 * time.Second)); r.Tripped || !r.AboveSince.IsZero() {
			t.Fatal("empty window did not reset")
		}
	})
}

func goodResourceSample() ClientResourceSample {
	return ClientResourceSample{At: time.Now(), Heap: 1 << 20, RSS: 2 << 20, OpenFiles: 20, SoftFileLimit: 1000, CPUSeconds: 1,
		Disks: map[string]DiskResourceSample{"report": {Free: 50 << 30, Total: 100 << 30}, "events": {Free: 50 << 30, Total: 100 << 30}, "sessions": {Free: 50 << 30, Total: 100 << 30}}}
}
func TestResourceThresholdsAndMissingEvidence(t *testing.T) {
	l := ResourceLimits{HeapBytes: 100 << 20, RSSBytes: 200 << 20, OpenFiles: 1000, MinDiskFreeBytes: 20 << 30}
	if s := resourceViolation(goodResourceSample(), l); s != "" {
		t.Fatal(s)
	}
	for _, mode := range []string{"heap", "rss", "fd", "os_fd", "disk_bytes", "disk_ratio", "missing", "invalid", "cpu"} {
		t.Run(mode, func(t *testing.T) {
			v := goodResourceSample()
			switch mode {
			case "heap":
				v.Heap = l.HeapBytes * 85 / 100
			case "rss":
				v.RSS = l.RSSBytes * 85 / 100
			case "fd":
				v.OpenFiles = 850
			case "os_fd":
				v.SoftFileLimit = 20
			case "disk_bytes":
				v.Disks["report"] = DiskResourceSample{Free: 20<<30 - 1, Total: 100 << 30}
			case "disk_ratio":
				v.Disks["events"] = DiskResourceSample{Free: 21 << 30, Total: 1000 << 30}
			case "missing":
				delete(v.Disks, "sessions")
			case "invalid":
				v.RSS = 0
			case "cpu":
				v.CPUSeconds = -1
			}
			if resourceViolation(v, l) == "" {
				t.Fatal("unsafe or unknown sample accepted")
			}
		})
	}
	if err := (ResourceLimits{MinDiskFreeBytes: 1}).validate(); err == nil {
		t.Fatal("disk floor disabled")
	}
}

func TestNativeResourcesObserveOpenFilesAndRealVolumes(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("platform has no resource provider; Run fails closed")
	}
	dir := t.TempDir()
	paths := map[string]string{"report": dir, "events": dir, "sessions": dir}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	before, err := readClientResources(ctx, paths)
	if err != nil {
		t.Fatal(err)
	}
	var files []*os.File
	for i := 0; i < 8; i++ {
		f, err := os.CreateTemp(dir, "fd")
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, f)
		defer f.Close()
	}
	after, err := readClientResources(ctx, paths)
	if err != nil {
		t.Fatal(err)
	}
	if after.OpenFiles < before.OpenFiles+8 || after.RSS == 0 || after.Heap == 0 || after.CPUSeconds < before.CPUSeconds || after.Disks["report"].Total == 0 {
		t.Fatalf("native resource evidence incomplete: before=%+v after=%+v", before, after)
	}
	paths["events"] = filepath.Join(dir, "missing")
	if _, err := readClientResources(ctx, paths); err == nil {
		t.Fatal("missing filesystem silently became zero")
	}
}

func TestIntentBudgetAdmissionIsAtomicAndStops(t *testing.T) {
	ctx, safety := newLoadSafety(context.Background())
	defer safety.cancel()
	c := &harnessCounters{messageIntentLimit: 7, safety: safety}
	var accepted atomic.Uint64
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if c.admitMessageIntent() {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 7 || c.messageIntentAdmissions.Load() != 7 || ctx.Err() == nil || safety.report().Class != "message_intent_budget" {
		t.Fatal("atomic intent budget exceeded or did not stop")
	}
	tracker := testDeliveryTracker(t, "budget")
	if err := tracker.preflight(RunConfig{MessageRate: 10, Duration: time.Minute, MaxMessageIntents: 2}); err == nil {
		t.Fatal("plan exceeding budget admitted")
	}
}

func TestDeliveryInvariantStopsButPreservesAuditableFacts(t *testing.T) {
	for _, mode := range []string{"wrong_account", "bad_peer", "conflict", "origin"} {
		t.Run(mode, func(t *testing.T) {
			tracker := testDeliveryTracker(t, "stop")
			ctx, safety := newLoadSafety(context.Background())
			defer safety.cancel()
			tracker.onInvariant = safety.invariantFailure
			marker := beginTestDelivery(t, tracker, 0, 200, 1)
			tracker.finish(marker, sendAccepted, false, 1)
			observeTestDelivery(tracker, marker, 1, deliveryLive)
			switch mode {
			case "wrong_account":
				tracker.observe(deliveryMessage{marker: marker, id: 1, peerUserID: 100}, 2, deliveryLive, 1)
			case "bad_peer":
				tracker.observe(deliveryMessage{marker: marker, id: 1, peerUserID: 999}, 1, deliveryLive, 1)
			case "conflict":
				tracker.observe(deliveryMessage{marker: marker, id: 9, peerUserID: 100}, 1, deliveryLive, 1)
			case "origin":
				tracker.observe(deliveryMessage{marker: marker, id: 1, peerUserID: 200, out: true}, 0, deliveryLive, 1)
			}
			if ctx.Err() == nil || safety.report().Source != "delivery_invariant" {
				t.Fatal("data anomaly did not stop")
			}
			if err := tracker.finalize(context.Background()); err != nil {
				t.Fatal("data anomaly disguised as corrupt evidence", err)
			}
			if r := tracker.report(); r.CommittedMessages != 1 || r.LiveDelivered != 1 || !r.Ledger.AuditComplete {
				t.Fatal("previous fact lost")
			}
		})
	}
}

func TestMalformedSuccessfulSendStopsAndRetainsUncertainty(t *testing.T) {
	tracker := testDeliveryTracker(t, "malformed")
	ctx, safety := newLoadSafety(context.Background())
	defer safety.cancel()
	tracker.onInvariant = safety.invariantFailure
	worker := &loadWorker{record: SessionRecord{Index: 0, UserID: 100}, target: SessionRecord{UserID: 200},
		counters: &harnessCounters{safety: safety}, metrics: newMetricSet("messages.sendMessage"), delivery: tracker, operationTimeout: time.Second}
	calls := 0
	raw := tg.NewClient(safetyWireInvoker(func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
		calls++
		output.(*tg.UpdatesBox).Updates = &tg.Updates{} // Transport success lacks a correlated confirmation.
		return nil
	}))
	worker.sendMessage(ctx, raw, messageArrival{})
	if err := tracker.finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	r := tracker.report()
	if calls != 1 || ctx.Err() == nil || safety.report().Class != "send_confirmation" || r.InitialUncertain != 1 || r.UnresolvedMessages != 1 || r.RetryAttempts != 0 || !r.Ledger.AuditComplete {
		t.Fatalf("malformed success was retried or erased: calls=%d report=%+v", calls, r)
	}
}
