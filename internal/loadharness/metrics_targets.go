package loadharness

import (
	"context"
	"fmt"
	"maps"
	"net/url"
	"strings"
	"sync"
	"time"
)

const maxMetricsTargets = 32

// MetricsTargets is also the repeatable command-line flag. URLs are private
// configuration and are deliberately absent from samples and reports.
type MetricsTarget struct {
	Role     string
	Instance string
	URL      string
}

type MetricsTargets []MetricsTarget

func (targets *MetricsTargets) String() string { return fmt.Sprintf("%d targets", len(*targets)) }

func (targets *MetricsTargets) Set(value string) error {
	identity, endpoint, ok := strings.Cut(value, "=")
	role, instance, identityOK := strings.Cut(identity, "/")
	if !ok || !identityOK {
		return fmt.Errorf("server-metrics requires role/instance=http(s)://host/metrics")
	}
	candidate := append(append(MetricsTargets(nil), (*targets)...), MetricsTarget{Role: role, Instance: instance, URL: endpoint})
	if err := candidate.validate(); err != nil {
		return err
	}
	*targets = candidate
	return nil
}

func (targets MetricsTargets) validate() error {
	if len(targets) > maxMetricsTargets {
		return fmt.Errorf("metrics target limit is %d", maxMetricsTargets)
	}
	ids, endpoints := map[string]bool{}, map[string]bool{}
	for _, target := range targets {
		switch target.Role {
		case "edge", "core", "egress", "file", "sfu", "admin":
		default:
			return fmt.Errorf("unknown metrics role")
		}
		if !safeMetricLabel(target.Instance) {
			return fmt.Errorf("metrics instance must be a short non-sensitive identifier")
		}
		u, err := url.Parse(target.URL)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return fmt.Errorf("metrics URL requires http(s), a host and no credentials/query/fragment")
		}
		if ids[target.id()] || endpoints[u.String()] {
			return fmt.Errorf("duplicate metrics identity or endpoint")
		}
		ids[target.id()], endpoints[u.String()] = true, true
	}
	return nil
}

func (target MetricsTarget) id() string { return target.Role + "/" + target.Instance }

type MetricsTargetSample struct {
	Role     string             `json:"role"`
	Instance string             `json:"instance"`
	At       time.Time          `json:"at"`
	Epoch    float64            `json:"process_start_time_seconds"`
	Segment  uint64             `json:"segment"`
	Values   map[string]float64 `json:"values,omitempty"`
	Error    string             `json:"error,omitempty"`
}

type MetricsTargetReport struct {
	Role               string             `json:"role"`
	Instance           string             `json:"instance"`
	Baseline           map[string]float64 `json:"baseline,omitempty"`
	WorkloadEnd        map[string]float64 `json:"workload_end,omitempty"`
	Final              map[string]float64 `json:"final,omitempty"`
	Peak               map[string]float64 `json:"peak,omitempty"`
	CounterDelta       map[string]float64 `json:"observed_counter_delta,omitempty"`
	Scrapes            uint64             `json:"scrapes"`
	Errors             uint64             `json:"errors"`
	Restarts           uint64             `json:"restarts"`
	CounterResets      uint64             `json:"counter_resets"`
	MissingCounters    uint64             `json:"missing_counter_samples"`
	ContinuityComplete bool               `json:"continuity_complete"`
	LastError          string             `json:"last_error,omitempty"`
}

type metricsTargetState struct {
	target       MetricsTarget
	client       *serverMetricsClient
	report       MetricsTargetReport
	sample       MetricsTargetSample
	previous     map[string]float64
	cumulative   map[string]float64
	lastCounters map[string]float64
	epoch        float64
	segment      uint64
}

// All mutation happens after the bounded fan-out has joined. Sampling is
// sequential at the run level; only independent HTTP requests run concurrently.
type serverMetricsCollector struct {
	targets []*metricsTargetState
	onIssue func(string, string)
}

func (c *serverMetricsCollector) issue(target, class string) {
	if c.onIssue != nil {
		c.onIssue(target, class)
	}
}

func newServerMetricsCollector(targets MetricsTargets) *serverMetricsCollector {
	if len(targets) == 0 {
		return nil
	}
	c := &serverMetricsCollector{}
	for _, target := range targets {
		c.targets = append(c.targets, &metricsTargetState{
			target: target, client: newServerMetricsClient(target.URL), cumulative: map[string]float64{}, lastCounters: map[string]float64{},
			report: MetricsTargetReport{Role: target.Role, Instance: target.Instance, Peak: map[string]float64{}, CounterDelta: map[string]float64{}, ContinuityComplete: true},
		})
	}
	return c
}

func (c *serverMetricsCollector) scrape(ctx context.Context) (map[string]float64, error) {
	if c == nil {
		return nil, nil
	}
	// A slow target must not multiply the sampling pause by the number of
	// batches. Individual requests and the entire fan-out share this deadline.
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	type result struct {
		values map[string]float64
		err    error
		at     time.Time
	}
	results := make([]result, len(c.targets))
	var wg sync.WaitGroup
	// Limit network fan-out independently of the configured target count.
	work := make(chan int, len(c.targets))
	for i := range c.targets {
		work <- i
	}
	close(work)
	for range min(8, len(c.targets)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range work {
				results[i].values, results[i].err = c.targets[i].client.scrape(ctx)
				results[i].at = time.Now().UTC()
			}
		}()
	}
	wg.Wait()
	aggregate := map[string]float64{}
	failed := false
	for i, state := range c.targets {
		result := results[i]
		state.sample = MetricsTargetSample{Role: state.target.Role, Instance: state.target.Instance, At: result.at, Segment: state.segment}
		if result.err == nil && (result.values["telesrv_process_start_time_seconds"] <= 0 || result.values["telesrv_go_goroutines"] <= 0) {
			result.err = fmt.Errorf("required runtime metrics missing")
		}
		if result.err == nil {
			known := len(state.report.Peak)
			for key := range result.values {
				if _, exists := state.report.Peak[key]; !exists {
					known++
				}
			}
			if known > maxServerMetricSeries {
				result.err = fmt.Errorf("metrics lifetime series budget exceeded")
			}
		}
		if result.err != nil {
			state.report.Errors++
			state.report.ContinuityComplete = false
			state.report.Final = nil
			state.report.LastError = classifyError(result.err)
			state.sample.Error = state.report.LastError
			failed = true
			c.issue(state.target.id(), "scrape_failed")
			continue
		}
		values := result.values
		epoch := values["telesrv_process_start_time_seconds"]
		restarted := state.previous != nil && epoch != state.epoch
		if restarted {
			c.issue(state.target.id(), "process_epoch_changed")
			state.report.Restarts++
			state.segment++
			state.report.ContinuityComplete = false
			clear(state.lastCounters)
		}
		if state.previous == nil {
			state.segment = 1
			state.report.Baseline = maps.Clone(values)
		}
		reset := false
		for key, value := range values {
			if state.client.counters[key] {
				previous := state.lastCounters[key]
				if state.previous == nil {
					state.cumulative[key] = value
				} else {
					delta := value - previous
					if restarted || delta < 0 {
						delta = value
						if !restarted {
							c.issue(state.target.id(), "counter_reset")
							state.report.CounterResets++
							reset = true
							state.report.ContinuityComplete = false
						}
					}
					state.cumulative[key] += delta
					state.report.CounterDelta[key] += delta
					if key == "telesrv_metrics_dropped_observations_total" && delta > 0 {
						c.issue(state.target.id(), "exporter_dropped")
					}
				}
				state.lastCounters[key] = value
			} else if key != "telesrv_process_start_time_seconds" {
				aggregate[key] += value
			}
		}
		// Disappearing counters are lost evidence, never an implicit zero.
		if !restarted {
			for key := range state.lastCounters {
				if _, exists := values[key]; !exists {
					c.issue(state.target.id(), "counter_missing")
					state.report.ContinuityComplete = false
					state.report.MissingCounters++
				}
			}
		}
		for key, value := range state.cumulative {
			aggregate[key] += value
		}
		if reset {
			state.segment++
		}
		state.report.Scrapes++
		state.report.LastError = ""
		state.report.Final = maps.Clone(values)
		updateMetricPeaks(state.report.Peak, values)
		state.previous, state.epoch = values, epoch
		state.sample.Epoch, state.sample.Segment, state.sample.Values = epoch, state.segment, values
	}
	if failed {
		return nil, fmt.Errorf("one or more metrics targets failed; see target evidence")
	}
	return aggregate, nil
}

func (c *serverMetricsCollector) samples() map[string]MetricsTargetSample {
	if c == nil {
		return nil
	}
	out := make(map[string]MetricsTargetSample, len(c.targets))
	for _, state := range c.targets {
		out[state.target.id()] = state.sample
	}
	return out
}

func (c *serverMetricsCollector) reports() map[string]MetricsTargetReport {
	if c == nil {
		return nil
	}
	out := make(map[string]MetricsTargetReport, len(c.targets))
	for _, state := range c.targets {
		out[state.target.id()] = state.report
	}
	return out
}

func (c *serverMetricsCollector) markWorkloadEnd() {
	if c == nil {
		return
	}
	for _, state := range c.targets {
		state.report.WorkloadEnd = maps.Clone(state.report.Final)
	}
}

func (c *serverMetricsCollector) successes() (n uint64) {
	if c != nil {
		for _, state := range c.targets {
			n += state.report.Scrapes
		}
	}
	return
}

func (c *serverMetricsCollector) failures() (n uint64) {
	if c != nil {
		for _, state := range c.targets {
			n += state.report.Errors
		}
	}
	return
}

func metricsEvidenceFailures(targets MetricsTargets, reports map[string]MetricsTargetReport) []string {
	var failures []string
	for _, target := range targets {
		report, ok := reports[target.id()]
		if !ok || report.Scrapes == 0 || report.Baseline == nil || report.Final == nil {
			failures = append(failures, target.id()+": metrics baseline/final evidence missing")
		} else if !report.ContinuityComplete || report.Errors > 0 {
			failures = append(failures, target.id()+": metrics continuity incomplete; evaluate restart/fault segments separately")
		}
	}
	return failures
}

func (c *serverMetricsCollector) waitForPresenceLastSeenSettlement(ctx context.Context, baselineSubmitted float64, expectedSubmitted uint64, timeout time.Duration) (map[string]float64, error) {
	if c == nil {
		return nil, nil
	}
	deadline := time.Now().Add(timeout)
	expired := time.NewTimer(timeout)
	defer expired.Stop()
	poll := time.NewTicker(100 * time.Millisecond)
	defer poll.Stop()
	var last map[string]float64
	for {
		if !time.Now().Before(deadline) {
			return last, context.DeadlineExceeded
		}
		// The condition deadline must not manufacture HTTP scrape failures.
		// Each real observation retains its own existing five-second bound.
		sample, err := c.scrape(ctx)
		if err != nil {
			return nil, err
		}
		last = sample
		submitted, present := sample["telesrv_presence_last_seen_submitted_total"]
		pending, pendingPresent := sample["telesrv_presence_last_seen_pending"]
		bootstrap, bootstrapPresent := sample["telesrv_bootstrap_ready_pending"]
		active, activePresent := sample["telesrv_active_channel_ids_pending"]
		if !present || !pendingPresent || !bootstrapPresent || !activePresent {
			return sample, fmt.Errorf("Core lifecycle metrics missing")
		}
		if !time.Now().Before(deadline) {
			return sample, context.DeadlineExceeded
		}
		if submitted-baselineSubmitted >= float64(expectedSubmitted) && pending == 0 && bootstrap == 0 && active == 0 {
			return sample, nil
		}
		select {
		case <-expired.C:
			return sample, context.DeadlineExceeded
		case <-ctx.Done():
			return sample, ctx.Err()
		case <-poll.C:
		}
	}
}
