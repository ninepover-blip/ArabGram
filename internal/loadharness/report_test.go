package loadharness

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/iamxvbaba/td/pool"
	tdrpc "github.com/iamxvbaba/td/rpc"
)

func TestOperationMetricsUsesBoundedHistogramAndFixedErrorClasses(t *testing.T) {
	metrics := &operationMetrics{}
	metrics.observe(time.Now().Add(-20*time.Millisecond), nil)
	metrics.observe(time.Now().Add(-200*time.Millisecond), errors.New("FLOOD_WAIT_1 phone=secret"))
	report := metrics.report()
	if report.Count != 2 || report.Errors != 1 || report.FloodWaits != 1 {
		t.Fatalf("report = %#v", report)
	}
	if report.P50UpperMS <= 0 || report.P99UpperMS < report.P50UpperMS || report.MaxMS <= 0 {
		t.Fatalf("latency report = %#v", report)
	}
}

func TestClassifyErrorReasonUsesFiniteRedactedVocabulary(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{errors.New("dial tcp 10.0.0.1:2398: socket: too many open files"), "file_descriptor_limit"},
		{errors.New("read: temporary auth key not found: pfs reconnect required"), "pfs_reconnect"},
		{errors.New("read tcp: EOF auth_key_id=secret"), "eof"},
	}
	for _, test := range tests {
		if got := classifyErrorReason(test.err); got != test.want {
			t.Fatalf("classifyErrorReason(%v) = %q, want %q", test.err, got, test.want)
		}
	}
}

func TestClassifyErrorRecognizesTypedReconnectFailures(t *testing.T) {
	tests := []error{
		fmt.Errorf("invoke: %w", tdrpc.ErrEngineClosed),
		fmt.Errorf("acquire: %w", pool.ErrConnDead),
		fmt.Errorf("read: %w", net.ErrClosed),
		errors.New("write: broken pipe"),
	}
	for _, err := range tests {
		if got := classifyError(err); got != "connection" {
			t.Fatalf("classifyError(%v) = %q, want connection", err, got)
		}
	}
}

func TestOperationMetricsSeparatesHarnessCancellation(t *testing.T) {
	metrics := &operationMetrics{}
	metrics.observe(time.Now(), context.Canceled)
	report := metrics.report()
	if report.Count != 1 || report.Canceled != 1 || report.Errors != 0 {
		t.Fatalf("report = %#v", report)
	}
}

func TestEvaluateReportAllowsOnlyConnectionErrorsForExpectedRestart(t *testing.T) {
	report := &RunReport{
		ExpectedSessions: 2, PeakReadySessions: 2, Reconnects: 2,
		SteadySamples: 1, SteadyReadyRatio: 1, MinSteadyReadySessions: 2,
		Operations: map[string]OperationReport{
			"connection.dead": {Count: 2, Errors: 2, ConnectionErrors: 2},
		},
	}
	evaluateReport(report, RunConfig{MinimumReadyRatio: 1, ExpectServerRestart: true})
	if !report.Pass {
		t.Fatalf("report = %#v", report)
	}
	report.Operations["ping"] = OperationReport{Count: 1, Errors: 1}
	report.Failures = nil
	evaluateReport(report, RunConfig{MinimumReadyRatio: 1, ExpectServerRestart: true})
	if report.Pass {
		t.Fatalf("unexpected application error passed: %#v", report)
	}
}

func TestEvaluateReportRequiresReclamationAndNoFloodWait(t *testing.T) {
	report := &RunReport{
		ExpectedSessions: 10, PeakReadySessions: 10, ServerMetricsScrapes: 1,
		SteadySamples: 1, SteadyReadyRatio: 1, MinSteadyReadySessions: 10,
		Operations: map[string]OperationReport{"ping": {Count: 10}},
		BaselineServerMetrics: map[string]float64{
			"telesrv_mtproto_raw_connections":      2,
			"telesrv_mtproto_logical_outbox_bytes": 3,
		},
		WorkloadEndServerMetrics: map[string]float64{
			"telesrv_mtproto_raw_connections":      12,
			"telesrv_mtproto_logical_outbox_bytes": 4,
		},
		FinalServerMetrics: map[string]float64{
			"telesrv_mtproto_raw_connections":      2,
			"telesrv_mtproto_logical_outbox_bytes": 4,
		},
	}
	targets := MetricsTargets{{Role: "edge", Instance: "one", URL: "http://metrics"}}
	report.ServerMetricsTargets = map[string]MetricsTargetReport{"edge/one": {Baseline: report.BaselineServerMetrics, Final: report.FinalServerMetrics, Scrapes: report.ServerMetricsScrapes, ContinuityComplete: true}}
	evaluateReport(report, RunConfig{MinimumReadyRatio: 1, RecoveryDuration: time.Minute, ServerMetricsTargets: targets})
	if report.Pass || len(report.Failures) != 1 {
		t.Fatalf("report = %#v", report)
	}
}

func TestEvaluateReportAcceptsReturnToNonZeroSharedServerBaseline(t *testing.T) {
	report := &RunReport{
		ExpectedSessions: 10, PeakReadySessions: 10, ServerMetricsScrapes: 2,
		SteadySamples: 1, SteadyReadyRatio: 1, MinSteadyReadySessions: 10,
		Operations: map[string]OperationReport{"ping": {Count: 10}},
		BaselineServerMetrics: map[string]float64{
			"telesrv_mtproto_raw_connections":      2,
			"telesrv_mtproto_logical_sessions":     2,
			"telesrv_mtproto_logical_outbox_bytes": 1024,
		},
		WorkloadEndServerMetrics: map[string]float64{
			"telesrv_mtproto_raw_connections":      12,
			"telesrv_mtproto_logical_sessions":     12,
			"telesrv_mtproto_logical_outbox_bytes": 2048,
		},
		FinalServerMetrics: map[string]float64{
			"telesrv_mtproto_raw_connections":      2,
			"telesrv_mtproto_logical_sessions":     2,
			"telesrv_mtproto_logical_outbox_bytes": 1024,
		},
	}
	targets := MetricsTargets{{Role: "edge", Instance: "one", URL: "http://metrics"}}
	report.ServerMetricsTargets = map[string]MetricsTargetReport{"edge/one": {Baseline: report.BaselineServerMetrics, Final: report.FinalServerMetrics, Scrapes: report.ServerMetricsScrapes, ContinuityComplete: true}}
	evaluateReport(report, RunConfig{MinimumReadyRatio: 1, RecoveryDuration: time.Minute, ServerMetricsTargets: targets})
	if !report.Pass || len(report.Failures) != 0 {
		t.Fatalf("report = %#v", report)
	}
}

func TestEvaluateReportRejectsFixedRateDeliveryLoss(t *testing.T) {
	report := &RunReport{
		ExpectedSessions: 1, PeakReadySessions: 1,
		SteadySamples: 1, SteadyReadyRatio: 1, MinSteadyReadySessions: 1,
		MessageScheduled: 2, MessageEnqueued: 2, MessageCompleted: 2,
		Delivery:   DeliveryReport{Expected: 2, Delivered: 1, Missing: 1},
		Operations: map[string]OperationReport{},
	}
	evaluateReport(report, RunConfig{MinimumReadyRatio: 1, MessageRate: 1})
	if report.Pass {
		t.Fatal("fixed-rate report with a missing recipient delivery passed")
	}
}

func TestEventWriteFailureCannotProducePassingReport(t *testing.T) {
	writer, err := newEventWriter(filepath.Join(t.TempDir(), "events.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.f.Close(); err != nil {
		t.Fatal(err)
	}
	writer.write(map[string]any{"type": "sample"})
	written, dropped := writer.counts()
	if written != 0 || dropped != 1 {
		t.Fatalf("failed write counted as evidence: %d/%d", written, dropped)
	}
	report := &RunReport{ExpectedSessions: 1, PeakReadySessions: 1, SteadySamples: 1, SteadyReadyRatio: 1, EventsDropped: dropped}
	evaluateReport(report, RunConfig{MinimumReadyRatio: 1})
	if report.Pass {
		t.Fatal("lost events accepted")
	}
}
