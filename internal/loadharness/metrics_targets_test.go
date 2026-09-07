package loadharness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestMetricsTargetsRejectAmbiguousAndSensitiveConfiguration(t *testing.T) {
	for _, value := range []string{"http://localhost/metrics", "edge/one=http://user:secret@localhost/metrics", "edge/one=http://localhost/metrics?token=secret", "unknown/one=http://localhost/metrics", "edge/bad instance=http://localhost/metrics", "edge/one=file:///secret"} {
		var targets MetricsTargets
		if err := targets.Set(value); err == nil || len(targets) != 0 {
			t.Fatalf("invalid target accepted: %q", value)
		}
	}
	var targets MetricsTargets
	if err := targets.Set("edge/one=http://localhost:16060/metrics"); err != nil {
		t.Fatal(err)
	}
	if err := targets.Set("core/one=http://localhost:16060/metrics"); err == nil {
		t.Fatal("same endpoint counted twice")
	}
	if err := targets.Set("edge/one=http://localhost:16061/metrics"); err == nil {
		t.Fatal("same target identity reused")
	}
}

func TestMetricsCollectorSeparatesRolesAndRestartsAboveOldCounter(t *testing.T) {
	var phase atomic.Int64
	runtimeCounters := []string{"telesrv_go_total_alloc_bytes", "telesrv_go_total_mallocs", "telesrv_go_total_frees"}
	newServer := func(role string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			epoch, calls := 100, 10
			if role == "core" {
				calls = 100
			}
			if phase.Load() >= 1 {
				calls += 5
			}
			if phase.Load() == 2 && role == "core" {
				epoch, calls = 200, 150
			}
			fmt.Fprintf(w, "telesrv_process_start_time_seconds %d\ntelesrv_go_goroutines 4\n", epoch)
			fmt.Fprintf(w, "telesrv_rpc_db_queries_total{method=\"messages.sendMessage\",user_id=\"secret\"} %d\n", calls)
			for _, name := range runtimeCounters {
				// Runtime cumulative values arrive with the exporter's gauge TYPE.
				fmt.Fprintf(w, "# TYPE %s gauge\n%s %d\n", name, name, calls)
			}
			fmt.Fprintln(w, "# TYPE telesrv_egress_outbox_stage_seconds histogram")
			fmt.Fprintln(w, `telesrv_egress_outbox_stage_seconds_sum{queue="account_pts",stage="projection"} 2`)
			fmt.Fprintln(w, `telesrv_egress_outbox_stage_seconds_count{queue="account_pts",stage="projection"} 4`)
		}))
	}
	edge, core := newServer("edge"), newServer("core")
	defer edge.Close()
	defer core.Close()
	targets := MetricsTargets{{Role: "edge", Instance: "one", URL: edge.URL}, {Role: "core", Instance: "one", URL: core.URL}}
	collector := newServerMetricsCollector(targets)
	baseline, err := collector.scrape(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	phase.Store(1)
	final, err := collector.scrape(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	work := startupDatabaseWork(baseline, final)["messages.sendMessage"]
	if work.Queries != 10 {
		t.Fatalf("cross-role DB work = %d", work.Queries)
	}
	collector.markWorkloadEnd()
	if errs := metricsEvidenceFailures(targets, collector.reports()); len(errs) != 0 {
		t.Fatal(errs)
	}
	if collector.reports()["core/one"].Baseline["telesrv_rpc_db_queries_total"] != 100 || collector.reports()["edge/one"].Baseline["telesrv_rpc_db_queries_total"] != 10 {
		t.Fatal("role baselines merged")
	}
	for _, name := range runtimeCounters {
		coreReport, edgeReport := collector.reports()["core/one"], collector.reports()["edge/one"]
		if coreReport.Baseline[name] != 100 || coreReport.Final[name] != 105 || coreReport.CounterDelta[name] != 5 ||
			edgeReport.Baseline[name] != 10 || edgeReport.Final[name] != 15 || edgeReport.CounterDelta[name] != 5 {
			t.Fatalf("runtime counter %s lost role-local values or deltas: core=%+v edge=%+v", name, coreReport, edgeReport)
		}
	}
	phase.Store(2)
	if _, err := collector.scrape(context.Background()); err != nil {
		t.Fatal(err)
	}
	r := collector.reports()["core/one"]
	if r.Restarts != 1 || r.ContinuityComplete || r.CounterDelta["telesrv_rpc_db_queries_total"] != 155 {
		t.Fatalf("restart was hidden: %+v", r)
	}
	for _, name := range runtimeCounters {
		if r.Final[name] != 150 || r.CounterDelta[name] != 155 {
			t.Fatalf("runtime counter %s crossed process epochs: %+v", name, r)
		}
	}
	if collector.samples()["core/one"].Segment != 2 || len(metricsEvidenceFailures(targets, collector.reports())) == 0 {
		t.Fatal("restart gap accepted as continuous evidence")
	}
	encoded, err := json.Marshal(collector.reports())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "secret") || strings.Contains(string(encoded), core.URL) {
		t.Fatal("report leaked raw labels or endpoint")
	}
	if r.Final[`telesrv_egress_outbox_stage_seconds_count{queue="account_pts",stage="projection"}`] != 4 {
		t.Fatal("stage breakdown missing")
	}
}

func TestMetricsCollectorMissingAndFailedTargetsDoNotBecomeZero(t *testing.T) {
	var phase atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if phase.Load() == 1 {
			http.Error(w, "secret", http.StatusServiceUnavailable)
			return
		}
		if phase.Load() == 2 {
			fmt.Fprintln(w, "unrelated_metric 1")
			return
		}
		fmt.Fprintln(w, "telesrv_process_start_time_seconds 100\ntelesrv_go_goroutines 4")
	}))
	defer server.Close()
	targets := MetricsTargets{{Role: "egress", Instance: "one", URL: server.URL}}
	collector := newServerMetricsCollector(targets)
	if _, err := collector.scrape(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, stage := range []int64{1, 2} {
		phase.Store(stage)
		if values, err := collector.scrape(context.Background()); err == nil || values != nil {
			t.Fatal("partial metrics appeared complete")
		}
		if sample := collector.samples()["egress/one"]; sample.Values != nil || sample.Error == "" {
			t.Fatalf("stale values reused: %+v", sample)
		}
	}
	phase.Store(0)
	if _, err := collector.scrape(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(metricsEvidenceFailures(targets, collector.reports())) != 1 {
		t.Fatal("later success erased evidence gap")
	}
}

func TestMetricsCollectorCounterDisappearanceAndResetAreVisible(t *testing.T) {
	var phase atomic.Int64
	runtimeCounters := []string{"telesrv_go_total_alloc_bytes", "telesrv_go_total_mallocs", "telesrv_go_total_frees"}
	values := []int{10, 15, 22, -1, 25, 2}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintln(w, "telesrv_process_start_time_seconds 100\ntelesrv_go_goroutines 4")
		if value := values[phase.Load()]; value >= 0 {
			fmt.Fprintf(w, "telesrv_rpc_db_queries_total %d\n", value)
			for _, name := range runtimeCounters {
				fmt.Fprintf(w, "# TYPE %s gauge\n%s %d\n", name, name, value)
			}
		}
	}))
	defer server.Close()
	collector := newServerMetricsCollector(MetricsTargets{{Role: "core", Instance: "one", URL: server.URL}})
	for stage := int64(0); stage < int64(len(values)); stage++ {
		phase.Store(stage)
		if _, err := collector.scrape(context.Background()); err != nil {
			t.Fatal(err)
		}
		if stage == 2 {
			r := collector.reports()["core/one"]
			for _, name := range runtimeCounters {
				if r.CounterDelta[name] != 12 || !r.ContinuityComplete {
					t.Fatalf("runtime counter %s did not accumulate both intervals: %+v", name, r)
				}
			}
		}
		if stage == 3 {
			r := collector.reports()["core/one"]
			for _, name := range runtimeCounters {
				if _, exists := r.Final[name]; exists || r.CounterDelta[name] != 12 {
					t.Fatalf("missing runtime counter %s was replaced or lost its observed delta: %+v", name, r)
				}
			}
		}
	}
	r := collector.reports()["core/one"]
	if r.MissingCounters != 4 || r.CounterResets != 4 || r.ContinuityComplete || r.CounterDelta["telesrv_rpc_db_queries_total"] != 17 {
		t.Fatalf("counter gap/delta: %+v", r)
	}
	for _, name := range runtimeCounters {
		if r.Baseline[name] != 10 || r.Final[name] != 2 || r.CounterDelta[name] != 17 {
			t.Fatalf("runtime counter %s lost its reset-aware delta: %+v", name, r)
		}
	}
}

func TestMetricsScraperRejectsTruncationInvalidValuesAndRedirect(t *testing.T) {
	for _, body := range []string{strings.Repeat("#", maxServerMetricsBytes+1), "telesrv_go_goroutines NaN\n"} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) }))
		_, err := newServerMetricsClient(server.URL).scrape(context.Background())
		server.Close()
		if err == nil {
			t.Fatal("invalid/oversized metrics accepted")
		}
	}
	var followed atomic.Bool
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { followed.Store(true) }))
	defer destination.Close()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, destination.URL, http.StatusFound) }))
	defer origin.Close()
	if _, err := newServerMetricsClient(origin.URL).scrape(context.Background()); err == nil || followed.Load() {
		t.Fatal("metrics redirect followed")
	}
}

func TestLifecycleDeadlineDoesNotManufactureMetricsOutage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(40 * time.Millisecond)
		fmt.Fprint(w, "telesrv_process_start_time_seconds 100\ntelesrv_go_goroutines 4\ntelesrv_presence_last_seen_submitted_total 1\ntelesrv_presence_last_seen_pending 0\ntelesrv_bootstrap_ready_pending 0\ntelesrv_active_channel_ids_pending 0\n")
	}))
	defer server.Close()
	var targets MetricsTargets
	if err := targets.Set("core/one=" + server.URL); err != nil {
		t.Fatal(err)
	}
	c := newServerMetricsCollector(targets)
	_, safety := newLoadSafety(context.Background())
	defer safety.cancel()
	c.onIssue = safety.metricsFailure
	sample, err := c.waitForPresenceLastSeenSettlement(context.Background(), 0, 2, 20*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) || sample == nil || c.failures() != 0 || safety.report().Source != "" || !c.reports()["core/one"].ContinuityComplete {
		t.Fatal("condition timeout became a metrics outage", err, c.reports())
	}
}

func TestLifecycleMissingPredicateRetainsSuccessfulSample(t *testing.T) {
	predicates := []string{"telesrv_presence_last_seen_submitted_total", "telesrv_presence_last_seen_pending", "telesrv_bootstrap_ready_pending", "telesrv_active_channel_ids_pending"}
	for _, missing := range predicates {
		t.Run(missing, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprint(w, "telesrv_process_start_time_seconds 100\ntelesrv_go_goroutines 4\n")
				for _, name := range predicates {
					if name == missing {
						continue
					}
					value := 0
					if name == "telesrv_presence_last_seen_submitted_total" {
						value = 2
					}
					fmt.Fprintf(w, "%s %d\n", name, value)
				}
			}))
			defer server.Close()
			var targets MetricsTargets
			if err := targets.Set("core/one=" + server.URL); err != nil {
				t.Fatal(err)
			}
			collector := newServerMetricsCollector(targets)
			_, safety := newLoadSafety(context.Background())
			defer safety.cancel()
			collector.onIssue = safety.metricsFailure
			sample, err := collector.waitForPresenceLastSeenSettlement(context.Background(), 0, 2, time.Second)
			if err == nil || !strings.Contains(err.Error(), "Core lifecycle metrics missing") || sample == nil || sample["telesrv_go_goroutines"] != 4 {
				t.Fatalf("missing predicate must fail and retain successful sample: sample=%v err=%v", sample, err)
			}
			if _, present := sample[missing]; present {
				t.Fatal("missing lifecycle predicate was silently replaced with zero")
			}
			report := collector.reports()["core/one"]
			if collector.successes() != 1 || collector.failures() != 0 || !report.ContinuityComplete || report.Final == nil || report.Final["telesrv_go_goroutines"] != 4 || safety.report().Source != "" {
				t.Fatalf("predicate failure became an HTTP/continuity failure: %#v", report)
			}
		})
	}
}
