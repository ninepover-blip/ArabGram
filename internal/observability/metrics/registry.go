// Package metrics provides a dependency-free Prometheus text exporter for the
// bounded runtime signals emitted by telesrv. It deliberately accepts only a
// small fixed label shape and caps dynamic series so observability cannot become
// an attacker-controlled memory cache.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// One additional fixed series reports observations rejected by this cap, so
	// the complete exporter remains bounded to 8192 resident series.
	defaultMaxSeries  = int64(8191)
	maxLabelBytes     = 96
	maxProviderSeries = 1024
)

var durationBuckets = [...]time.Duration{
	time.Millisecond,
	5 * time.Millisecond,
	10 * time.Millisecond,
	25 * time.Millisecond,
	50 * time.Millisecond,
	100 * time.Millisecond,
	250 * time.Millisecond,
	500 * time.Millisecond,
	time.Second,
	2 * time.Second,
	5 * time.Second,
	10 * time.Second,
	30 * time.Second,
}

// Label is a bounded Prometheus label attached to a provider sample.
type Label struct {
	Name  string
	Value string
}

// GaugeSample is an identity-free point-in-time value supplied at scrape time.
type GaugeSample struct {
	Name   string
	Labels []Label
	Value  float64
}

// GaugeProvider is evaluated only during a scrape. Providers must be bounded
// and must not perform unbounded database scans.
type GaugeProvider func() []GaugeSample

type seriesKey struct {
	name string
	k1   string
	v1   string
	k2   string
	v2   string
	k3   string
	v3   string
}

func newSeriesKey(name string, labels ...Label) seriesKey {
	key := seriesKey{name: sanitizeMetricName(name)}
	if len(labels) > 0 {
		key.k1, key.v1 = sanitizeLabelName(labels[0].Name), sanitizeLabelValue(labels[0].Value)
	}
	if len(labels) > 1 {
		key.k2, key.v2 = sanitizeLabelName(labels[1].Name), sanitizeLabelValue(labels[1].Value)
	}
	if len(labels) > 2 {
		key.k3, key.v3 = sanitizeLabelName(labels[2].Name), sanitizeLabelValue(labels[2].Value)
	}
	return key
}

func (k seriesKey) overflow() seriesKey {
	if k.k1 != "" {
		k.v1 = "overflow"
	}
	if k.k2 != "" {
		k.v2 = "overflow"
	}
	if k.k3 != "" {
		k.v3 = "overflow"
	}
	return k
}

type counterSeries struct {
	key   seriesKey
	value atomic.Uint64
}

type gaugeSeries struct {
	key   seriesKey
	value atomic.Int64
}

type histogramSeries struct {
	key     seriesKey
	buckets [len(durationBuckets)]atomic.Uint64
	count   atomic.Uint64
	sumNS   atomic.Int64
}

func (h *histogramSeries) observe(d time.Duration) {
	if d < 0 {
		d = 0
	}
	h.count.Add(1)
	h.sumNS.Add(int64(d))
	for i, bound := range durationBuckets {
		if d <= bound {
			h.buckets[i].Add(1)
		}
	}
}

// Registry implements the mtprotoedge and rpc metric hooks and serves the
// Prometheus text exposition format.
type Registry struct {
	maxSeries int64
	series    atomic.Int64
	seriesMu  sync.Mutex
	dropped   atomic.Uint64
	counters  sync.Map // seriesKey -> *counterSeries
	gauges    sync.Map // seriesKey -> *gaugeSeries
	hist      sync.Map // seriesKey -> *histogramSeries

	providersMu sync.RWMutex
	providers   []GaugeProvider
}

// New returns an empty bounded registry.
func New() *Registry {
	return &Registry{maxSeries: defaultMaxSeries}
}

// AddGaugeProvider registers a bounded point-in-time provider.
func (r *Registry) AddGaugeProvider(provider GaugeProvider) {
	if r == nil || provider == nil {
		return
	}
	r.providersMu.Lock()
	r.providers = append(r.providers, provider)
	r.providersMu.Unlock()
}

func (r *Registry) counter(key seriesKey) *counterSeries {
	if existing, ok := r.counters.Load(key); ok {
		return existing.(*counterSeries)
	}
	r.seriesMu.Lock()
	defer r.seriesMu.Unlock()
	if existing, ok := r.counters.Load(key); ok {
		return existing.(*counterSeries)
	}
	if r.series.Load() >= r.maxSeries {
		r.dropped.Add(1)
		return &counterSeries{}
	}
	created := &counterSeries{key: key}
	r.counters.Store(key, created)
	r.series.Add(1)
	return created
}

func (r *Registry) gauge(key seriesKey) *gaugeSeries {
	if existing, ok := r.gauges.Load(key); ok {
		return existing.(*gaugeSeries)
	}
	r.seriesMu.Lock()
	defer r.seriesMu.Unlock()
	if existing, ok := r.gauges.Load(key); ok {
		return existing.(*gaugeSeries)
	}
	if r.series.Load() >= r.maxSeries {
		r.dropped.Add(1)
		return &gaugeSeries{}
	}
	created := &gaugeSeries{key: key}
	r.gauges.Store(key, created)
	r.series.Add(1)
	return created
}

func (r *Registry) histogram(key seriesKey) *histogramSeries {
	if existing, ok := r.hist.Load(key); ok {
		return existing.(*histogramSeries)
	}
	r.seriesMu.Lock()
	defer r.seriesMu.Unlock()
	if existing, ok := r.hist.Load(key); ok {
		return existing.(*histogramSeries)
	}
	if r.series.Load() >= r.maxSeries {
		r.dropped.Add(1)
		return &histogramSeries{}
	}
	created := &histogramSeries{key: key}
	r.hist.Store(key, created)
	r.series.Add(1)
	return created
}

func (r *Registry) inc(name string, labels ...Label) {
	if r == nil {
		return
	}
	r.counter(newSeriesKey(name, labels...)).value.Add(1)
}

func (r *Registry) add(name string, value uint64, labels ...Label) {
	if r == nil || value == 0 {
		return
	}
	r.counter(newSeriesKey(name, labels...)).value.Add(value)
}

func (r *Registry) addGauge(name string, delta int64, labels ...Label) {
	if r == nil || delta == 0 {
		return
	}
	r.gauge(newSeriesKey(name, labels...)).value.Add(delta)
}

func (r *Registry) observe(name string, d time.Duration, labels ...Label) {
	if r == nil {
		return
	}
	r.histogram(newSeriesKey(name, labels...)).observe(d)
}

// ConnOpened implements mtprotoedge.Metrics.
func (r *Registry) ConnOpened() {
	r.inc("telesrv_mtproto_connections_opened_total")
	r.addGauge("telesrv_mtproto_connections_active", 1)
}

// ConnClosed implements mtprotoedge.Metrics.
func (r *Registry) ConnClosed() {
	r.inc("telesrv_mtproto_connections_closed_total")
	r.addGauge("telesrv_mtproto_connections_active", -1)
}

// HandshakeDone implements mtprotoedge.Metrics.
func (r *Registry) HandshakeDone(d time.Duration) {
	r.inc("telesrv_mtproto_handshakes_total")
	r.observe("telesrv_mtproto_handshake_duration_seconds", d)
}

// RPCHandled implements mtprotoedge.Metrics.
func (r *Registry) RPCHandled(method string, d time.Duration, err error) {
	labels := []Label{{Name: "method", Value: method}, {Name: "outcome", Value: errorOutcome(err)}}
	r.inc("telesrv_mtproto_rpc_handled_total", labels...)
	r.observe("telesrv_mtproto_rpc_duration_seconds", d, labels...)
}

func (r *Registry) RPCDatabase(method string, queries int64, d time.Duration, errors int64) {
	labels := []Label{{Name: "method", Value: method}}
	if queries > 0 {
		r.add("telesrv_rpc_db_queries_total", uint64(queries), labels...)
	}
	if errors > 0 {
		r.add("telesrv_rpc_db_errors_total", uint64(errors), labels...)
	}
	if d > 0 {
		r.observe("telesrv_rpc_db_time_seconds", d, labels...)
	}
}

// InboundRPCQueued implements mtprotoedge.Metrics.
func (r *Registry) InboundRPCQueued(method string, length, capacity int) {
	r.inc("telesrv_mtproto_inbound_rpc_queued_total", Label{Name: "method", Value: method})
	r.add("telesrv_mtproto_inbound_rpc_queue_depth_observed_total", uint64(max(length, 0)), Label{Name: "method", Value: method})
	if capacity > 0 && length >= capacity {
		r.inc("telesrv_mtproto_inbound_rpc_queue_full_total", Label{Name: "method", Value: method})
	}
}

// InboundRPCStarted implements mtprotoedge.Metrics.
func (r *Registry) InboundRPCStarted(method string, queueWait time.Duration) {
	r.observe("telesrv_mtproto_inbound_rpc_queue_wait_seconds", queueWait, Label{Name: "method", Value: method})
}

// InboundRPCDropped implements mtprotoedge.Metrics.
func (r *Registry) InboundRPCDropped(method, reason string) {
	r.inc("telesrv_mtproto_inbound_rpc_dropped_total", Label{Name: "method", Value: method}, Label{Name: "reason", Value: reason})
}

// OutboundSend implements mtprotoedge.Metrics.
func (r *Registry) OutboundSend(typeID uint32, queueWait time.Duration, bytes int, err error) {
	labels := []Label{{Name: "type_id", Value: fmt.Sprintf("%08x", typeID)}, {Name: "outcome", Value: errorOutcome(err)}}
	r.inc("telesrv_mtproto_outbound_send_total", labels...)
	r.add("telesrv_mtproto_outbound_send_bytes_total", uint64(max(bytes, 0)), labels...)
	r.observe("telesrv_mtproto_outbound_queue_wait_seconds", queueWait, labels...)
}

// OutboundResend implements mtprotoedge.Metrics.
func (r *Registry) OutboundResend(count int, err error) {
	labels := []Label{{Name: "outcome", Value: errorOutcome(err)}}
	r.inc("telesrv_mtproto_outbound_resend_requests_total", labels...)
	r.add("telesrv_mtproto_outbound_resent_frames_total", uint64(max(count, 0)), labels...)
}

// OutboundDropped implements mtprotoedge.Metrics.
func (r *Registry) OutboundDropped(reason string) {
	r.inc("telesrv_mtproto_outbound_dropped_total", Label{Name: "reason", Value: reason})
}

// OutboundQueueWait implements mtprotoedge.Metrics.
func (r *Registry) OutboundQueueWait(length, capacity int) {
	r.inc("telesrv_mtproto_outbound_queue_wait_total")
	if capacity > 0 && length >= capacity {
		r.inc("telesrv_mtproto_outbound_queue_full_total")
	}
}

// RPCResultPrepared implements mtprotoedge.RPCResultMetrics.
func (r *Registry) RPCResultPrepared(method, priority string, innerBytes, wireBytes int, compressed bool) {
	encoding := "plain"
	if compressed {
		encoding = "gzip"
	}
	labels := []Label{{Name: "method", Value: method}, {Name: "priority", Value: priority}, {Name: "encoding", Value: encoding}}
	r.inc("telesrv_mtproto_rpc_result_prepared_total", labels...)
	r.add("telesrv_mtproto_rpc_result_inner_bytes_total", uint64(max(innerBytes, 0)), labels...)
	r.add("telesrv_mtproto_rpc_result_wire_bytes_total", uint64(max(wireBytes, 0)), labels...)
}

// RPCResultDelivered implements mtprotoedge.RPCResultMetrics.
func (r *Registry) RPCResultDelivered(method string, egressLatency time.Duration, wireBytes int, err error) {
	labels := []Label{{Name: "method", Value: method}, {Name: "outcome", Value: errorOutcome(err)}}
	r.inc("telesrv_mtproto_rpc_result_delivered_total", labels...)
	r.add("telesrv_mtproto_rpc_result_delivered_bytes_total", uint64(max(wireBytes, 0)), labels...)
	r.observe("telesrv_mtproto_rpc_result_egress_seconds", egressLatency, labels...)
}

// LogicalOutboxAcknowledged implements mtprotoedge.LogicalOutboxMetrics.
func (r *Registry) LogicalOutboxAcknowledged(bytes int, retainedFor time.Duration, rpcResult bool) {
	kind := "service_or_update"
	if rpcResult {
		kind = "rpc_result"
	}
	labels := []Label{{Name: "kind", Value: kind}}
	r.inc("telesrv_mtproto_logical_outbox_acked_frames_total", labels...)
	r.add("telesrv_mtproto_logical_outbox_acked_bytes_total", uint64(max(bytes, 0)), labels...)
	r.observe("telesrv_mtproto_logical_outbox_retained_seconds", retainedFor, labels...)
}

// ConnectionIntake implements mtprotoedge.ConnectionIntakeMetrics.
func (r *Registry) ConnectionIntake(stage, outcome string, d time.Duration) {
	labels := []Label{{Name: "stage", Value: stage}, {Name: "outcome", Value: outcome}}
	r.inc("telesrv_mtproto_connection_intake_total", labels...)
	r.observe("telesrv_mtproto_connection_intake_seconds", d, labels...)
}

// CoreExecGRPCCall implements coreexec.Metrics.
func (r *Registry) CoreExecGRPCCall(side, operation, outcome string, d time.Duration) {
	labels := []Label{
		{Name: "side", Value: side},
		{Name: "operation", Value: operation},
		{Name: "outcome", Value: outcome},
	}
	r.inc("telesrv_coreexec_grpc_calls_total", labels...)
	r.observe("telesrv_coreexec_grpc_call_seconds", d, labels...)
}

// CoreExecPendingAdmissionRejected implements coreexec.Metrics.
func (r *Registry) CoreExecPendingAdmissionRejected(transport, reason string) {
	labels := []Label{
		{Name: "transport", Value: transport},
		{Name: "reason", Value: reason},
	}
	r.inc("telesrv_coreexec_pending_admission_rejected_total", labels...)
}

// MessageSend implements rpc.Metrics.
func (r *Registry) MessageSend(d time.Duration, duplicate bool, err error) {
	dup := "false"
	if duplicate {
		dup = "true"
	}
	labels := []Label{{Name: "outcome", Value: errorOutcome(err)}, {Name: "duplicate", Value: dup}}
	r.inc("telesrv_rpc_message_send_total", labels...)
	r.observe("telesrv_rpc_message_send_duration_seconds", d, labels...)
}

// PlainPrivateSendBatch implements postgres.PlainPrivateSendBatchObserver.
// Batch outcomes use the fixed error vocabulary; task count is an aggregate,
// never a request label.
func (r *Registry) PlainPrivateSendBatch(d time.Duration, tasks int, err error) {
	labels := []Label{{Name: "outcome", Value: errorOutcome(err)}}
	r.inc("telesrv_core_plain_private_send_batches_total", labels...)
	r.add("telesrv_core_plain_private_send_tasks_total", uint64(max(tasks, 0)), labels...)
	r.observe("telesrv_core_plain_private_send_batch_seconds", d, labels...)
}

// MessageRateLimited implements rpc.Metrics.
func (r *Registry) MessageRateLimited(retryAfterSeconds int) {
	r.inc("telesrv_rpc_message_rate_limited_total")
	r.add("telesrv_rpc_message_rate_limit_wait_seconds_total", uint64(max(retryAfterSeconds, 0)))
}

// OutboxClaimed implements rpc.Metrics.
func (r *Registry) OutboxClaimed(count int) {
	r.add("telesrv_rpc_outbox_claimed_total", uint64(max(count, 0)))
}

// OutboxDelivered implements rpc.Metrics.
func (r *Registry) OutboxDelivered(d time.Duration) {
	r.inc("telesrv_rpc_outbox_delivered_total")
	r.observe("telesrv_rpc_outbox_delivery_seconds", d)
}

// OutboxFailed implements rpc.Metrics.
func (r *Registry) OutboxFailed(err error) {
	r.inc("telesrv_rpc_outbox_failed_total", Label{Name: "outcome", Value: errorOutcome(err)})
}

func (r *Registry) BootstrapReadyBatch(inputs int, matched int, d time.Duration, err error) {
	outcome := errorOutcome(err)
	r.inc("telesrv_bootstrap_ready_batches_total", Label{Name: "outcome", Value: outcome})
	r.observe("telesrv_bootstrap_ready_batch_duration_seconds", d, Label{Name: "outcome", Value: outcome})
	inputs = max(inputs, 0)
	matched = max(min(matched, inputs), 0)
	if err != nil {
		r.add("telesrv_bootstrap_ready_selectors_total", uint64(inputs), Label{Name: "outcome", Value: "error"})
		return
	}
	r.add("telesrv_bootstrap_ready_selectors_total", uint64(matched), Label{Name: "outcome", Value: "matched"})
	r.add("telesrv_bootstrap_ready_selectors_total", uint64(inputs-matched), Label{Name: "outcome", Value: "miss"})
}

func (r *Registry) BootstrapReadyPending(delta int) {
	if r != nil {
		r.gauge(newSeriesKey("telesrv_bootstrap_ready_pending")).value.Add(int64(delta))
	}
}

func (r *Registry) ActiveChannelIDsCache(outcome string) {
	r.inc("telesrv_active_channel_ids_cache_total", Label{Name: "outcome", Value: outcome})
}

func (r *Registry) ActiveChannelIDsBatch(selectors int, rows int, d time.Duration, err error) {
	outcome := errorOutcome(err)
	r.inc("telesrv_active_channel_ids_batches_total", Label{Name: "outcome", Value: outcome})
	r.add("telesrv_active_channel_ids_selectors_total", uint64(max(selectors, 0)), Label{Name: "outcome", Value: outcome})
	if err == nil {
		r.add("telesrv_active_channel_ids_rows_total", uint64(max(rows, 0)))
	}
	r.observe("telesrv_active_channel_ids_batch_duration_seconds", d, Label{Name: "outcome", Value: outcome})
}

func (r *Registry) ActiveChannelIDsPending(delta int) {
	if r != nil {
		r.gauge(newSeriesKey("telesrv_active_channel_ids_pending")).value.Add(int64(delta))
	}
}

func (r *Registry) PresenceLastSeenBatch(count int, d time.Duration, err error) {
	labels := []Label{{Name: "outcome", Value: errorOutcome(err)}}
	r.inc("telesrv_presence_last_seen_batches_total", labels...)
	r.add("telesrv_presence_last_seen_updates_total", uint64(max(count, 0)), labels...)
	r.observe("telesrv_presence_last_seen_batch_duration_seconds", d, labels...)
}

func (r *Registry) PresenceLastSeenSubmitted() {
	r.inc("telesrv_presence_last_seen_submitted_total")
}

func (r *Registry) PresenceLastSeenPending(delta int) {
	if r != nil {
		r.gauge(newSeriesKey("telesrv_presence_last_seen_pending")).value.Add(int64(delta))
	}
}

func (r *Registry) PresenceLastSeenOverflow() {
	r.inc("telesrv_presence_last_seen_overflow_total")
}

func (r *Registry) PresenceLastSeenDrainDropped(count int) {
	r.add("telesrv_presence_last_seen_drain_dropped_total", uint64(max(count, 0)))
}

// OutboxStage records fixed Egress pipeline stages. Callers supply only the
// closed queue/stage vocabulary owned by the server, never request data.
func (r *Registry) OutboxStage(queue, stage string, d time.Duration, items int) {
	labels := []Label{{Name: "queue", Value: queue}, {Name: "stage", Value: stage}}
	r.observe("telesrv_egress_outbox_stage_seconds", d, labels...)
	r.add("telesrv_egress_outbox_stage_items_total", uint64(max(items, 0)), labels...)
}

// ServeHTTP streams through a request-owned buffer. Nothing is pooled or retained
// across requests, and provider-owned samples are only read, never sanitized in place.
func (r *Registry) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	var counters []*counterSeries
	r.counters.Range(func(_, value any) bool {
		counters = append(counters, value.(*counterSeries))
		return true
	})
	gauges := r.providerSamples()
	r.gauges.Range(func(_, value any) bool {
		item := value.(*gaugeSeries)
		gauges = append(gauges, GaugeSample{Name: item.key.name, Labels: keyLabels(item.key), Value: float64(item.value.Load())})
		return true
	})
	var histograms []*histogramSeries
	r.hist.Range(func(_, value any) bool {
		histograms = append(histograms, value.(*histogramSeries))
		return true
	})
	slices.SortFunc(counters, func(a, b *counterSeries) int { return compareSeries(a.key, b.key) })
	slices.SortFunc(histograms, func(a, b *histogramSeries) int { return compareSeries(a.key, b.key) })
	slices.SortFunc(gauges, func(a, b GaugeSample) int {
		if order := strings.Compare(a.Name, b.Name); order != 0 {
			return order
		}
		return compareLabels(a.Labels, b.Labels)
	})
	out := expositionWriter{response: w, buf: make([]byte, 0, min(expositionChunkSize, expositionCapacity(counters, gauges, histograms)))}
	out.buf = append(out.buf, "# TYPE telesrv_metrics_dropped_observations_total counter\ntelesrv_metrics_dropped_observations_total "...)
	out.buf = strconv.AppendUint(out.buf, r.dropped.Load(), 10)
	out.buf = append(out.buf, '\n')
	if !out.writeCounterSeries(counters) || !out.writeGaugeSeries(gauges) || !out.writeHistogramSeries(histograms) {
		return
	}
	out.flush()
}

const expositionChunkSize = 16 << 10

type expositionWriter struct {
	response http.ResponseWriter
	buf      []byte
}

func (w *expositionWriter) flush() bool {
	if len(w.buf) == 0 {
		return true
	}
	n, err := w.response.Write(w.buf)
	ok := err == nil && n == len(w.buf)
	w.buf = w.buf[:0]
	return ok
}

// Reserve before appending so ordinary lines never grow the chunk buffer.
// An individual oversized metric remains intact; only this request owns its
// larger buffer. A failed or short write terminates the entire scrape.
func (w *expositionWriter) reserve(size int) bool {
	if size <= cap(w.buf)-len(w.buf) {
		return true
	}
	if !w.flush() {
		return false
	}
	if size > cap(w.buf) {
		w.buf = make([]byte, 0, size)
	}
	return true
}

func (w *expositionWriter) metricType(name, kind string) bool {
	if !w.reserve(len(name) + len(kind) + 9) {
		return false
	}
	w.buf = appendMetricType(w.buf, name, kind)
	return true
}

func (w *expositionWriter) sample(name, suffix string, labels []Label, value float64, size int) bool {
	if !w.reserve(size) {
		return false
	}
	w.buf = writeSample(w.buf, name, suffix, labels, value)
	return true
}

func (r *Registry) providerSamples() (samples []GaugeSample) {
	r.providersMu.RLock()
	providers := append([]GaugeProvider(nil), r.providers...)
	r.providersMu.RUnlock()
	for _, provider := range providers {
		func() {
			defer func() { _ = recover() }()
			for _, sample := range provider() {
				if len(samples) >= maxProviderSeries {
					r.dropped.Add(1)
					return
				}
				sample.Name = sanitizeMetricName(sample.Name)
				if n := min(len(sample.Labels), 3); n > 0 {
					labels := make([]Label, n)
					for i := range labels {
						labels[i] = Label{Name: sanitizeLabelName(sample.Labels[i].Name), Value: sanitizeLabelValue(sample.Labels[i].Value)}
					}
					sample.Labels = labels
				}
				samples = append(samples, sample)
			}
		}()
	}
	return samples
}

// These bounds are fixed by durationBuckets. Formatting once avoids a numeric
// string and label-slice allocation for every bucket on every scrape.
var durationBucketText = func() [len(durationBuckets)]string {
	var result [len(durationBuckets)]string
	for i, bound := range durationBuckets {
		result[i] = strconv.FormatFloat(bound.Seconds(), 'g', -1, 64)
	}
	return result
}()

// Capacity includes the longest float64 text and escaping expansion. It is an
// upper bound for this snapshot, not a retained high-water buffer or a new cap.
func expositionCapacity(counters []*counterSeries, gauges []GaugeSample, histograms []*histogramSeries) int {
	size := 128
	var scratch [3]Label
	for _, item := range counters {
		size += len(item.key.name)*2 + encodedLabelsSize(item.key.appendLabels(scratch[:0])) + 46
	}
	for _, item := range gauges {
		size += len(item.Name)*2 + encodedLabelsSize(item.Labels) + 46
	}
	for _, item := range histograms {
		base := len(item.key.name) + encodedLabelsSize(item.key.appendLabels(scratch[:0])) + 26
		size += len(item.key.name) + 20 + (len(durationBuckets)+1)*(base+19) + (base + 4) + (base + 6)
	}
	return size
}

func encodedLabelsSize(labels []Label) int {
	if len(labels) == 0 {
		return 0
	}
	size := 1
	for _, label := range labels {
		size += len(label.Name) + len(label.Value) + 4
		for i := 0; i < len(label.Value); i++ {
			switch label.Value[i] {
			case '\\', '\n', '"':
				size++
			}
		}
	}
	return size
}

func appendMetricType(out []byte, name, kind string) []byte {
	out = append(out, "# TYPE "...)
	out = append(out, name...)
	out = append(out, ' ')
	out = append(out, kind...)
	return append(out, '\n')
}

func (w *expositionWriter) writeCounterSeries(series []*counterSeries) bool {
	last := ""
	var scratch [3]Label
	for _, item := range series {
		if item.key.name != last {
			if !w.metricType(item.key.name, "counter") {
				return false
			}
			last = item.key.name
		}
		labels := item.key.appendLabels(scratch[:0])
		if !w.sample(item.key.name, "", labels, float64(item.value.Load()), sampleCapacity(item.key.name, labels)) {
			return false
		}
	}
	return true
}

func (w *expositionWriter) writeGaugeSeries(series []GaugeSample) bool {
	last := ""
	for _, item := range series {
		if item.Name != last {
			if !w.metricType(item.Name, "gauge") {
				return false
			}
			last = item.Name
		}
		if !w.sample(item.Name, "", item.Labels, item.Value, sampleCapacity(item.Name, item.Labels)) {
			return false
		}
	}
	return true
}

func (w *expositionWriter) writeHistogramSeries(series []*histogramSeries) bool {
	// An unlabelled bucket adds {le="value"}; existing labels need one byte less.
	maxBucketLabels := len(`{le="+Inf"}`)
	for _, bound := range durationBucketText {
		maxBucketLabels = max(maxBucketLabels, len(bound)+7)
	}
	last := ""
	var scratch [4]Label
	for _, item := range series {
		if item.key.name != last {
			if !w.metricType(item.key.name, "histogram") {
				return false
			}
			last = item.key.name
		}
		labels := item.key.appendLabels(scratch[:0])
		lineSize := sampleCapacity(item.key.name, labels)
		bucketSize := lineSize + len("_bucket") + maxBucketLabels
		buckets := append(labels, Label{Name: "le"})
		for i, boundary := range durationBucketText {
			buckets[len(labels)].Value = boundary
			if !w.sample(item.key.name, "_bucket", buckets, float64(item.buckets[i].Load()), bucketSize) {
				return false
			}
		}
		buckets[len(labels)].Value = "+Inf"
		if !w.sample(item.key.name, "_bucket", buckets, float64(item.count.Load()), bucketSize) ||
			!w.sample(item.key.name, "_sum", labels, time.Duration(item.sumNS.Load()).Seconds(), lineSize+len("_sum")) ||
			!w.sample(item.key.name, "_count", labels, float64(item.count.Load()), lineSize+len("_count")) {
			return false
		}
	}
	return true
}

// 24 bytes cover any float64 representation; two more cover space/newline.
func sampleCapacity(name string, labels []Label) int {
	return len(name) + encodedLabelsSize(labels) + 26
}

func writeSample(out []byte, name, suffix string, labels []Label, value float64) []byte {
	out = append(out, name...)
	out = append(out, suffix...)
	if len(labels) > 0 {
		out = append(out, '{')
		for i, label := range labels {
			if i > 0 {
				out = append(out, ',')
			}
			out = append(out, label.Name...)
			out = append(out, '=', '"')
			for j := 0; j < len(label.Value); j++ {
				switch c := label.Value[j]; c {
				case '\\', '"':
					out = append(out, '\\', c)
				case '\n':
					out = append(out, '\\', 'n')
				default:
					out = append(out, c)
				}
			}
			out = append(out, '"')
		}
		out = append(out, '}')
	}
	out = append(out, ' ')
	out = strconv.AppendFloat(out, value, 'g', -1, 64)
	return append(out, '\n')
}

func (key seriesKey) appendLabels(labels []Label) []Label {
	if key.k1 != "" {
		labels = append(labels, Label{Name: key.k1, Value: key.v1})
	}
	if key.k2 != "" {
		labels = append(labels, Label{Name: key.k2, Value: key.v2})
	}
	if key.k3 != "" {
		labels = append(labels, Label{Name: key.k3, Value: key.v3})
	}
	return labels
}

func keyLabels(key seriesKey) []Label {
	if key.k1 == "" && key.k2 == "" && key.k3 == "" {
		return nil
	}
	return key.appendLabels(make([]Label, 0, 3))
}

func compareSeries(a, b seriesKey) int {
	if order := strings.Compare(a.name, b.name); order != 0 {
		return order
	}
	left := [...]string{a.k1, a.v1, a.k2, a.v2, a.k3, a.v3}
	right := [...]string{b.k1, b.v1, b.k2, b.v2, b.k3, b.v3}
	return compareStringParts(left[:], right[:])
}

func compareLabels(a, b []Label) int {
	// All call sites use seriesKey labels or already bounded provider labels.
	var left, right [12]string
	for i, label := range a {
		left[i*4], left[i*4+1], left[i*4+2], left[i*4+3] = label.Name, "=", label.Value, ","
	}
	for i, label := range b {
		right[i*4], right[i*4+1], right[i*4+2], right[i*4+3] = label.Name, "=", label.Value, ","
	}
	return compareStringParts(left[:len(a)*4], right[:len(b)*4])
}

// Compare exactly as concatenation would, without allocating temporary keys.
func compareStringParts(a, b []string) int {
	var left, right string
	for {
		for left == "" && len(a) > 0 {
			left, a = a[0], a[1:]
		}
		for right == "" && len(b) > 0 {
			right, b = b[0], b[1:]
		}
		if left == "" || right == "" {
			return strings.Compare(left, right)
		}
		n := min(len(left), len(right))
		if order := strings.Compare(left[:n], right[:n]); order != 0 {
			return order
		}
		left, right = left[n:], right[n:]
	}
}

func errorOutcome(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	message := strings.ToUpper(err.Error())
	switch {
	case strings.Contains(message, "FLOOD_WAIT"):
		return "flood_wait"
	case strings.Contains(message, "WORKER_BUSY") || strings.Contains(message, "BUDGET") || strings.Contains(message, "CAPACITY"):
		return "edge_overload"
	default:
		return "error"
	}
}

func sanitizeMetricName(value string) string {
	if validMetricName(value) {
		return value
	}
	if value == "" {
		return "telesrv_invalid_metric"
	}
	var b strings.Builder
	for i, r := range value {
		valid := r == '_' || r == ':' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || i > 0 && r >= '0' && r <= '9'
		if valid {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return b.String()
}

func validMetricName(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if !(c == '_' || c == ':' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || i > 0 && c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

func sanitizeLabelName(value string) string {
	return strings.ReplaceAll(sanitizeMetricName(value), ":", "_")
}

func sanitizeLabelValue(value string) string {
	if value == "" {
		return "unknown"
	}
	if len(value) > maxLabelBytes {
		return value[:maxLabelBytes]
	}
	return value
}
