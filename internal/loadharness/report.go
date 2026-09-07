package loadharness

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var latencyBounds = [...]time.Duration{
	5 * time.Millisecond, 10 * time.Millisecond, 25 * time.Millisecond,
	50 * time.Millisecond, 100 * time.Millisecond, 250 * time.Millisecond,
	500 * time.Millisecond, time.Second, 2 * time.Second, 5 * time.Second,
	10 * time.Second, 30 * time.Second,
}

type operationMetrics struct {
	count       atomic.Uint64
	errors      atomic.Uint64
	canceled    atomic.Uint64
	floodWaits  atomic.Uint64
	timeouts    atomic.Uint64
	connections atomic.Uint64
	sumNS       atomic.Int64
	maxNS       atomic.Int64
	buckets     [len(latencyBounds)]atomic.Uint64
}

func (m *operationMetrics) observe(start time.Time, err error) {
	m.observeDuration(time.Since(start), err)
}

func (m *operationMetrics) observeDuration(d time.Duration, err error) {
	if d < 0 {
		d = 0
	}
	m.count.Add(1)
	m.sumNS.Add(int64(d))
	for {
		previous := m.maxNS.Load()
		if int64(d) <= previous || m.maxNS.CompareAndSwap(previous, int64(d)) {
			break
		}
	}
	for i, bound := range latencyBounds {
		if d <= bound {
			m.buckets[i].Add(1)
		}
	}
	if err != nil {
		outcome := classifyError(err)
		if outcome == "canceled" {
			m.canceled.Add(1)
			return
		}
		m.errors.Add(1)
		switch outcome {
		case "flood_wait":
			m.floodWaits.Add(1)
		case "timeout":
			m.timeouts.Add(1)
		case "connection":
			m.connections.Add(1)
		}
	}
}

type OperationReport struct {
	Count            uint64  `json:"count"`
	Errors           uint64  `json:"errors"`
	Canceled         uint64  `json:"canceled"`
	FloodWaits       uint64  `json:"flood_waits"`
	Timeouts         uint64  `json:"timeouts"`
	ConnectionErrors uint64  `json:"connection_errors"`
	MeanMS           float64 `json:"mean_ms"`
	P50UpperMS       float64 `json:"p50_upper_ms"`
	P95UpperMS       float64 `json:"p95_upper_ms"`
	P99UpperMS       float64 `json:"p99_upper_ms"`
	MaxMS            float64 `json:"max_ms"`
}

func (m *operationMetrics) report() OperationReport {
	count := m.count.Load()
	report := OperationReport{
		Count: count, Errors: m.errors.Load(), Canceled: m.canceled.Load(), FloodWaits: m.floodWaits.Load(), Timeouts: m.timeouts.Load(), ConnectionErrors: m.connections.Load(),
		MaxMS: durationMS(time.Duration(m.maxNS.Load())),
	}
	if count > 0 {
		report.MeanMS = durationMS(time.Duration(m.sumNS.Load() / int64(count)))
		report.P50UpperMS = durationMS(m.quantile(count, 0.50))
		report.P95UpperMS = durationMS(m.quantile(count, 0.95))
		report.P99UpperMS = durationMS(m.quantile(count, 0.99))
	}
	return report
}

func (m *operationMetrics) quantile(count uint64, q float64) time.Duration {
	target := uint64(math.Ceil(float64(count) * q))
	for i, bound := range latencyBounds {
		if m.buckets[i].Load() >= target {
			return bound
		}
	}
	// The overflow bucket has no finite bound. Do not understate a 90-second
	// p99 as <=30 seconds: the observed maximum is a conservative upper bound.
	return time.Duration(m.maxNS.Load())
}

func durationMS(d time.Duration) float64 {
	return math.Round(float64(d)/float64(time.Millisecond)*1000) / 1000
}

type metricSet struct {
	mu         sync.RWMutex
	ops        map[string]*operationMetrics
	frozen     bool
	onBusiness func(string, time.Time, error)
}

func newMetricSet(names ...string) *metricSet {
	m := &metricSet{ops: make(map[string]*operationMetrics, len(names))}
	for _, name := range names {
		m.ops[name] = &operationMetrics{}
	}
	return m
}

func (m *metricSet) observe(name string, start time.Time, err error) {
	m.mu.RLock()
	if m.frozen {
		m.mu.RUnlock()
		return
	}
	op := m.ops[name]
	if op != nil {
		debugOperationError(name, err)
		op.observe(start, err)
		if m.onBusiness != nil {
			m.onBusiness(name, time.Now(), err)
		}
		m.mu.RUnlock()
		return
	}
	m.mu.RUnlock()
	{
		// Operation names are code-owned and finite, but retain a lock-protected
		// fallback for optional scenarios added by the harness.
		m.mu.Lock()
		defer m.mu.Unlock()
		if m.frozen {
			return
		}
		op = m.ops[name]
		if op == nil && len(m.ops) < 32 {
			op = &operationMetrics{}
			m.ops[name] = op
		}
		if op != nil {
			debugOperationError(name, err)
			op.observe(start, err)
			if m.onBusiness != nil {
				m.onBusiness(name, time.Now(), err)
			}
		}
	}
}

func (m *metricSet) report() map[string]OperationReport {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.reportLocked()
}

// freeze returns the immutable pre-teardown operation cut. Holding the write
// lock waits for any observer already publishing its complete metric tuple and
// prevents later coordinated-cancel outcomes from entering the business report.
func (m *metricSet) freeze() map[string]OperationReport {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.frozen = true
	return m.reportLocked()
}

func (m *metricSet) reportLocked() map[string]OperationReport {
	out := make(map[string]OperationReport, len(m.ops))
	for name, op := range m.ops {
		out[name] = op.report()
	}
	return out
}

type MessageTimingReport struct {
	SchedulerLag    OperationReport `json:"scheduler_lag"`
	SenderQueueWait OperationReport `json:"sender_queue_wait"`
	PlannedToSend   OperationReport `json:"planned_to_send"`
}

type RunReport struct {
	Lifecycle                RunLifecycleReport              `json:"lifecycle"`
	WorkloadStarted          bool                            `json:"workload_started"`
	Preparation              FilePreparationReport           `json:"preparation"`
	Version                  int                             `json:"version"`
	StartOrder               string                          `json:"start_order,omitempty"`
	StartOrderSeed           int64                           `json:"start_order_seed,omitempty"`
	StartedAt                time.Time                       `json:"started_at"`
	LoadEndedAt              time.Time                       `json:"load_ended_at"`
	FinishedAt               time.Time                       `json:"finished_at"`
	RequestedDuration        string                          `json:"requested_duration"`
	RecoveryDuration         string                          `json:"recovery_duration"`
	ExpectedSessions         int                             `json:"expected_sessions"`
	PeakReadySessions        int                             `json:"peak_ready_sessions"`
	FinalReadySessions       int                             `json:"final_ready_sessions"`
	SteadySamples            int                             `json:"steady_samples"`
	SteadyReadyRatio         float64                         `json:"steady_ready_ratio"`
	MinSteadyReadySessions   int                             `json:"min_steady_ready_sessions"`
	ConnectionAttempts       uint64                          `json:"connection_attempts"`
	Reconnects               uint64                          `json:"reconnects"`
	Disconnects              uint64                          `json:"disconnects"`
	UpdatesReceived          uint64                          `json:"updates_received"`
	DownloadedBytes          uint64                          `json:"downloaded_bytes"`
	WorkerFatalErrors        uint64                          `json:"worker_fatal_errors"`
	MessageRatePerSecond     float64                         `json:"message_rate_per_second"`
	MessageScheduled         uint64                          `json:"message_scheduled"`
	MessageEnqueued          uint64                          `json:"message_enqueued"`
	MessageCompleted         uint64                          `json:"message_completed"`
	MessageQueueFull         uint64                          `json:"message_queue_full"`
	MessageNotReady          uint64                          `json:"message_not_ready"`
	MessageReadiness         OperationReport                 `json:"message_readiness"`
	MessageReadyAt           time.Time                       `json:"message_ready_at"`
	MessageTiming            MessageTimingReport             `json:"message_timing"`
	Delivery                 DeliveryReport                  `json:"delivery"`
	Operations               map[string]OperationReport      `json:"operations"`
	ResponseBytes            map[string]StartupResponseBytes `json:"response_bytes,omitempty"`
	RPCDeliveryOutcomes      map[string]map[string]uint64    `json:"rpc_delivery_outcomes,omitempty"`
	DatabaseWork             map[string]StartupDatabaseWork  `json:"database_work,omitempty"`
	BaselineServerMetrics    map[string]float64              `json:"baseline_server_metrics,omitempty"`
	WorkloadEndServerMetrics map[string]float64              `json:"workload_end_server_metrics,omitempty"`
	FinalServerMetrics       map[string]float64              `json:"final_server_metrics,omitempty"`
	ServerMetricsTargets     map[string]MetricsTargetReport  `json:"server_metrics_targets,omitempty"`
	ServerMetricsScrapes     uint64                          `json:"server_metrics_scrapes"`
	ServerMetricsErrors      uint64                          `json:"server_metrics_errors"`
	EventsWritten            uint64                          `json:"events_written"`
	EventsDropped            uint64                          `json:"events_dropped"`
	EventEvidence            EventEvidenceReport             `json:"event_evidence"`
	SafetyStop               SafetyStopReport                `json:"safety_stop"`
	Resources                ResourceGuardReport             `json:"resources"`
	Environment              EnvironmentReport               `json:"environment"`
	BusinessGuard            BusinessGuardReport             `json:"business_guard"`
	MessageIntentLimit       uint64                          `json:"message_intent_limit"`
	MessageIntentAdmissions  uint64                          `json:"message_intent_admissions"`
	Pass                     bool                            `json:"pass"`
	Failures                 []string                        `json:"failures,omitempty"`
}

func WriteReport(path string, report *RunReport) error {
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	return writeNewEvidenceReport(path, append(data, '\n'))
}

func sortedOperationNames(ops map[string]OperationReport) []string {
	names := make([]string, 0, len(ops))
	for name := range ops {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
