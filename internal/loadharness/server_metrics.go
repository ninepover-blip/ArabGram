package loadharness

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const maxServerMetricsBytes = 4 << 20
const maxServerMetricSeries = 8192

var selectedServerMetrics = map[string]struct{}{
	"telesrv_process_start_time_seconds":                       {},
	"telesrv_mtproto_raw_connections":                          {},
	"telesrv_mtproto_connections_active":                       {},
	"telesrv_mtproto_sessions":                                 {},
	"telesrv_mtproto_logical_sessions":                         {},
	"telesrv_mtproto_logical_outbox_frames":                    {},
	"telesrv_mtproto_logical_outbox_bytes":                     {},
	"telesrv_mtproto_logical_outbox_acked_frames_total":        {},
	"telesrv_mtproto_logical_outbox_acked_bytes_total":         {},
	"telesrv_mtproto_logical_outbox_retained_seconds_count":    {},
	"telesrv_mtproto_logical_outbox_retained_seconds_sum":      {},
	"telesrv_mtproto_pending_push_bytes":                       {},
	"telesrv_mtproto_inbound_rpc_tasks":                        {},
	"telesrv_mtproto_inbound_rpc_bytes":                        {},
	"telesrv_mtproto_rpc_delivery_hook_workers":                {},
	"telesrv_mtproto_rpc_delivery_hook_capacity":               {},
	"telesrv_mtproto_rpc_delivery_hook_reserved":               {},
	"telesrv_mtproto_rpc_delivery_hook_queued":                 {},
	"telesrv_mtproto_rpc_delivery_hook_running":                {},
	"telesrv_mtproto_rpc_delivery_hook_completed_total":        {},
	"telesrv_mtproto_rpc_delivery_hook_rejected_total":         {},
	"telesrv_mtproto_rpc_delivery_hook_panics_total":           {},
	"telesrv_mtproto_rpc_delivery_hook_duration_seconds_total": {},
	"telesrv_mtproto_inbound_frame_bytes":                      {},
	"telesrv_mtproto_outbound_tracked_bytes":                   {},
	"telesrv_mtproto_outbound_write_bytes":                     {},
	"telesrv_mtproto_rpc_execution_owners":                     {},
	"telesrv_mtproto_rpc_execution_reserved_entries":           {},
	"telesrv_mtproto_rpc_execution_receipts":                   {},
	"telesrv_mtproto_rpc_execution_receipt_budget_bytes":       {},
	"telesrv_mtproto_rpc_execution_subscribers":                {},
	"telesrv_mtproto_rpc_result_inner_bytes_total":             {},
	"telesrv_mtproto_rpc_result_wire_bytes_total":              {},
	"telesrv_mtproto_rpc_result_delivered_total":               {},
	"telesrv_mtproto_rpc_result_delivered_bytes_total":         {},
	"telesrv_go_goroutines":                                    {},
	"telesrv_process_cpu_seconds":                              {},
	"telesrv_go_scheduler_busy_seconds":                        {},
	"telesrv_go_gc_cycles":                                     {},
	"telesrv_go_gc_pause_seconds":                              {},
	"telesrv_go_total_alloc_bytes":                             {},
	"telesrv_go_total_mallocs":                                 {},
	"telesrv_go_total_frees":                                   {},
	"telesrv_go_heap_alloc_bytes":                              {},
	"telesrv_go_heap_inuse_bytes":                              {},
	"telesrv_go_heap_objects":                                  {},
	"telesrv_go_stack_inuse_bytes":                             {},
	"telesrv_go_sys_bytes":                                     {},
	"telesrv_postgres_pool_connections":                        {},
	"telesrv_postgres_pool_acquire_count":                      {},
	"telesrv_postgres_pool_acquire_wait_seconds":               {},
	"telesrv_postgres_pool_empty_acquire_count":                {},
	"telesrv_postgres_pool_canceled_acquire_count":             {},
	"telesrv_postgres_pool_max_connections":                    {},
	"telesrv_redis_pool_connections":                           {},
	"telesrv_redis_pool_hits":                                  {},
	"telesrv_redis_pool_misses":                                {},
	"telesrv_redis_pool_pending_requests":                      {},
	"telesrv_redis_pool_timeouts":                              {},
	"telesrv_redis_pool_wait_count":                            {},
	"telesrv_redis_pool_wait_seconds":                          {},
	"telesrv_rpc_db_queries_total":                             {},
	"telesrv_rpc_db_errors_total":                              {},
	"telesrv_rpc_db_time_seconds_sum":                          {},
	"telesrv_rpc_db_time_seconds_count":                        {},
	"telesrv_channel_difference_cache_entries":                 {},
	"telesrv_channel_difference_cache_weight_bytes":            {},
	"telesrv_channel_difference_cache_hits":                    {},
	"telesrv_channel_difference_cache_misses":                  {},
	"telesrv_channel_difference_cache_loads":                   {},
	"telesrv_channel_difference_cache_load_errors":             {},
	"telesrv_bootstrap_ready_batches_total":                    {},
	"telesrv_bootstrap_ready_selectors_total":                  {},
	"telesrv_bootstrap_ready_pending":                          {},
	"telesrv_active_channel_ids_cache_total":                   {},
	"telesrv_active_channel_ids_batches_total":                 {},
	"telesrv_active_channel_ids_selectors_total":               {},
	"telesrv_active_channel_ids_rows_total":                    {},
	"telesrv_active_channel_ids_pending":                       {},
	"telesrv_presence_last_seen_batches_total":                 {},
	"telesrv_presence_last_seen_updates_total":                 {},
	"telesrv_presence_last_seen_submitted_total":               {},
	"telesrv_presence_last_seen_pending":                       {},
	"telesrv_presence_last_seen_overflow_total":                {},
	"telesrv_presence_last_seen_drain_dropped_total":           {},
	"telesrv_coreexec_grpc_calls_total":                        {},
	"telesrv_metrics_dropped_observations_total":               {},
}

type serverMetricsClient struct {
	url      string
	client   *http.Client
	success  atomic.Uint64
	errors   atomic.Uint64
	counters map[string]bool
}

func newServerMetricsClient(url string) *serverMetricsClient {
	if strings.TrimSpace(url) == "" {
		return nil
	}
	return &serverMetricsClient{url: url, client: &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

func (c *serverMetricsClient) scrape(ctx context.Context) (map[string]float64, error) {
	if c == nil {
		return nil, nil
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		c.errors.Add(1)
		return nil, err
	}
	response, err := c.client.Do(request)
	if err != nil {
		c.errors.Add(1)
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		c.errors.Add(1)
		return nil, fmt.Errorf("metrics HTTP status %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxServerMetricsBytes+1))
	if err != nil || len(body) > maxServerMetricsBytes {
		c.errors.Add(1)
		return nil, fmt.Errorf("metrics body unavailable or exceeds byte budget")
	}
	reader := bufio.NewScanner(bytes.NewReader(body))
	reader.Buffer(make([]byte, 64<<10), 1<<20)
	values := make(map[string]float64, len(selectedServerMetrics))
	types := make(map[string]string)
	c.counters = make(map[string]bool)
	for reader.Scan() {
		line := strings.TrimSpace(reader.Text())
		if strings.HasPrefix(line, "# TYPE ") {
			parts := strings.Fields(line)
			if len(parts) == 4 && (selectedMetric(parts[2]) || selectedMetric(parts[2]+"_count")) {
				types[parts[2]] = parts[3]
				if len(types) > maxServerMetricSeries {
					c.errors.Add(1)
					return nil, fmt.Errorf("metrics type budget exceeded")
				}
			}
			continue
		}
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		name := fields[0]
		if idx := strings.IndexByte(name, '{'); idx >= 0 {
			name = name[:idx]
		}
		if !selectedMetric(name) {
			continue
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
			c.errors.Add(1)
			return nil, fmt.Errorf("selected metric has invalid value")
		}
		// Reports need bounded, comparable capacity signals, not an unbounded copy
		// of Prometheus label series. Aggregate every selected family into one
		// key. Response-byte and DB-work families additionally retain only their
		// code-owned method label; bounded pool/session families retain their state
		// label. This supports attribution without copying auth/session/user
		// cardinality.
		counter := metricIsCounter(name, types)
		add := func(key string) {
			values[key] += value
			if counter {
				c.counters[key] = true
			}
		}
		if !strings.HasSuffix(name, "_bucket") {
			add(name)
		}
		if isCoreExecOperationOutcomeServerMetric(name) {
			side, sideOK := prometheusLabelValue(fields[0], "side")
			operation, operationOK := prometheusLabelValue(fields[0], "operation")
			outcome, outcomeOK := prometheusLabelValue(fields[0], "outcome")
			if sideOK && operationOK && outcomeOK {
				add(name + `{side="` + side + `",operation="` + operation + `",outcome="` + outcome + `"}`)
			}
		} else if isPerMethodOutcomeServerMetric(name) {
			method, methodOK := prometheusLabelValue(fields[0], "method")
			outcome, outcomeOK := prometheusLabelValue(fields[0], "outcome")
			if methodOK && outcomeOK {
				add(name + `{method="` + method + `",outcome="` + outcome + `"}`)
			}
		} else if isPerMethodServerMetric(name) {
			if method, ok := prometheusLabelValue(fields[0], "method"); ok {
				add(name + `{method="` + method + `"}`)
			}
		} else if isOutcomeServerMetric(name) {
			if outcome, ok := prometheusLabelValue(fields[0], "outcome"); ok {
				add(name + `{outcome="` + outcome + `"}`)
			}
		} else if extendedMetric(name) && !isStateServerMetric(name) {
			// Only exporter-owned dimensions survive. Never retain arbitrary raw labels.
			labels := make([]string, 0, 4)
			for _, label := range []string{"side", "operation", "queue", "stage", "method", "outcome", "state", "transport", "reason", "kind", "phase", "le"} {
				if value, ok := prometheusLabelValue(fields[0], label); ok && safeMetricLabel(value) {
					labels = append(labels, label+"="+strconv.Quote(value))
				}
			}
			if len(labels) > 0 {
				add(name + "{" + strings.Join(labels, ",") + "}")
			}
		}
		if isStateServerMetric(name) {
			if state, ok := prometheusLabelValue(fields[0], "state"); ok {
				add(name + `{state="` + state + `"}`)
			}
		}
		if len(values) > maxServerMetricSeries {
			c.errors.Add(1)
			return nil, fmt.Errorf("metrics series budget exceeded")
		}
	}
	if err := reader.Err(); err != nil {
		c.errors.Add(1)
		return nil, err
	}
	c.success.Add(1)
	return values, nil
}

func selectedMetric(name string) bool {
	_, ok := selectedServerMetrics[name]
	return ok || extendedMetric(name)
}

func extendedMetric(name string) bool {
	for _, prefix := range []string{"telesrv_egress_", "telesrv_coreexec_", "telesrv_filedata_", "telesrv_sfu_",
		"telesrv_mtproto_", "telesrv_rpc_", "telesrv_core_", "telesrv_presence_", "telesrv_bootstrap_", "telesrv_active_channel_"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func safeMetricLabel(value string) bool {
	if len(value) == 0 || len(value) > 96 {
		return false
	}
	for _, ch := range value {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || strings.ContainsRune("._-+", ch)) {
			return false
		}
	}
	return true
}

func metricIsCounter(name string, types map[string]string) bool {
	if types[name] == "counter" || strings.HasSuffix(name, "_total") {
		return true
	}
	for _, suffix := range []string{"_bucket", "_sum", "_count"} {
		if strings.HasSuffix(name, suffix) && types[strings.TrimSuffix(name, suffix)] == "histogram" {
			return true
		}
	}
	// These cumulative runtime/pool values are exported by gauge providers.
	switch name {
	case "telesrv_process_cpu_seconds", "telesrv_go_scheduler_busy_seconds", "telesrv_go_gc_cycles", "telesrv_go_gc_pause_seconds",
		"telesrv_go_total_alloc_bytes", "telesrv_go_total_mallocs", "telesrv_go_total_frees",
		"telesrv_postgres_pool_acquire_count", "telesrv_postgres_pool_acquire_wait_seconds", "telesrv_postgres_pool_empty_acquire_count", "telesrv_postgres_pool_canceled_acquire_count",
		"telesrv_redis_pool_hits", "telesrv_redis_pool_misses", "telesrv_redis_pool_timeouts", "telesrv_redis_pool_wait_count", "telesrv_redis_pool_wait_seconds",
		"telesrv_channel_difference_cache_hits", "telesrv_channel_difference_cache_misses", "telesrv_channel_difference_cache_loads", "telesrv_channel_difference_cache_load_errors":
		return true
	}
	return false
}

func isCoreExecOperationOutcomeServerMetric(name string) bool {
	return name == "telesrv_coreexec_grpc_calls_total"
}

func isOutcomeServerMetric(name string) bool {
	switch name {
	case "telesrv_presence_last_seen_batches_total", "telesrv_presence_last_seen_updates_total",
		"telesrv_bootstrap_ready_batches_total", "telesrv_bootstrap_ready_selectors_total",
		"telesrv_active_channel_ids_cache_total", "telesrv_active_channel_ids_batches_total",
		"telesrv_active_channel_ids_selectors_total":
		return true
	default:
		return false
	}
}

func isPerMethodServerMetric(name string) bool {
	switch name {
	case "telesrv_mtproto_rpc_result_inner_bytes_total",
		"telesrv_mtproto_rpc_result_wire_bytes_total",
		"telesrv_rpc_db_queries_total",
		"telesrv_rpc_db_errors_total",
		"telesrv_rpc_db_time_seconds_sum",
		"telesrv_rpc_db_time_seconds_count":
		return true
	default:
		return false
	}
}

func isPerMethodOutcomeServerMetric(name string) bool {
	switch name {
	case "telesrv_mtproto_rpc_result_delivered_total", "telesrv_mtproto_rpc_result_delivered_bytes_total":
		return true
	default:
		return false
	}
}

func isStateServerMetric(name string) bool {
	switch name {
	case "telesrv_mtproto_sessions", "telesrv_mtproto_logical_sessions",
		"telesrv_postgres_pool_connections", "telesrv_redis_pool_connections":
		return true
	default:
		return false
	}
}

func prometheusLabelValue(series, label string) (string, bool) {
	needle := label + `="`
	start := strings.Index(series, needle)
	if start < 1 || (series[start-1] != '{' && series[start-1] != ',') {
		return "", false
	}
	start += len(needle)
	end := start
	for end < len(series) {
		if series[end] == '"' && (end == start || series[end-1] != '\\') {
			value := series[start:end]
			return value, safeMetricLabel(value)
		}
		end++
	}
	return "", false
}

func (c *serverMetricsClient) successes() uint64 {
	if c == nil {
		return 0
	}
	return c.success.Load()
}

func (c *serverMetricsClient) failures() uint64 {
	if c == nil {
		return 0
	}
	return c.errors.Load()
}
