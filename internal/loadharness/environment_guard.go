package loadharness

import (
	"context"
	"errors"
	"maps"
	"math"
	"os"
	"sync"
	"time"
)

type EnvironmentTargetSample struct {
	Kind         string             `json:"kind"`
	At           time.Time          `json:"at"`
	Epoch        string             `json:"epoch,omitempty"`
	RSS          uint64             `json:"rss_bytes,omitempty"`
	RSSBudget    uint64             `json:"rss_budget_bytes,omitempty"`
	Files        uint64             `json:"open_files,omitempty"`
	FileBudget   uint64             `json:"open_file_budget,omitempty"`
	FileLimit    uint64             `json:"os_file_limit,omitempty"`
	CPU          float64            `json:"cumulative_cpu"`
	CPUUnit      string             `json:"cpu_unit,omitempty"`
	MemoryBytes  uint64             `json:"memory_bytes,omitempty"`
	MemoryBudget uint64             `json:"memory_budget_bytes,omitempty"`
	MemoryLimit  uint64             `json:"memory_limit_bytes,omitempty"`
	Disk         DiskResourceSample `json:"disk"`
	Error        string             `json:"error,omitempty"`
}
type EnvironmentSample struct {
	At         time.Time                          `json:"at"`
	Targets    map[string]EnvironmentTargetSample `json:"targets"`
	DatabaseAt time.Time                          `json:"database_at"`
	Queues     map[string]environmentQueueSample  `json:"queues"`
	Error      string                             `json:"error,omitempty"`
}
type EnvironmentReport struct {
	Version     int                                `json:"version"`
	Enabled     bool                               `json:"enabled"`
	Provider    string                             `json:"provider"`
	ConfigSHA   string                             `json:"config_sha256,omitempty"`
	IntervalMS  int                                `json:"interval_ms"`
	MinDiskFree uint64                             `json:"min_disk_free_bytes"`
	Samples     uint64                             `json:"samples"`
	Errors      uint64                             `json:"errors"`
	First       EnvironmentSample                  `json:"first"`
	Last        EnvironmentSample                  `json:"last"`
	Peak        map[string]EnvironmentTargetSample `json:"peak"`
	QueuePeak   map[string]environmentQueueSample  `json:"queue_peak"`
	Coverage    []string                           `json:"coverage"`
}
type environmentProcessState struct {
	identity       nativeProcessIdentity
	binary, config os.FileInfo
}
type environmentGuard struct {
	mu             sync.Mutex
	spec           environmentSpec
	owner          nativeProcessIdentity
	processes      map[string]environmentProcessState
	result         EnvironmentReport
	rising         map[string]int
	previousQueues map[string]environmentQueueSample
	stop           context.CancelFunc
	done           chan struct{}
	safety         *loadSafety
	events         *eventWriter
}

func newEnvironmentGuard(parent context.Context, path string, targets MetricsTargets, safety *loadSafety, events *eventWriter) (*environmentGuard, error) {
	if path == "" {
		return nil, nil
	}
	spec, digest, e := loadEnvironmentSpec(path, targets)
	if e != nil {
		return nil, e
	}
	st, e := os.Stat(spec.DockerSocket)
	if e != nil || st.Mode()&os.ModeSocket == 0 {
		return nil, errors.New("environment Docker socket missing")
	}
	owner, e := readProcessIdentity(spec.OwnerPID)
	if e != nil {
		return nil, errors.New("environment guardian missing")
	}
	stopping := make(chan struct{})
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(func() { close(stopping) }) }
	g := &environmentGuard{spec: spec, owner: owner, processes: map[string]environmentProcessState{}, rising: map[string]int{}, stop: stop, done: make(chan struct{}), safety: safety, events: events,
		result: EnvironmentReport{Version: environmentVersion, Enabled: true, Provider: spec.Provider, ConfigSHA: digest, IntervalMS: 2000, MinDiskFree: spec.MinDiskFree, Peak: map[string]EnvironmentTargetSample{}, QueuePeak: map[string]environmentQueueSample{}, Coverage: []string{"owned_role_rss_fd_cpu", "declared_local_volumes", "owned_container_cgroup_memory_cpu", "owned_container_data_filesystems", "three_durable_queue_ages"}}}
	for _, p := range spec.Processes {
		if !processOwnedBy(p.PID, spec.OwnerPID) {
			e = errors.New("environment process ownership mismatch")
			break
		}
		var v environmentProcessState
		v.identity, e = readProcessIdentity(p.PID)
		if e != nil {
			break
		}
		v.binary, e = verifiedEnvironmentFile(p.Executable, p.BinarySHA)
		if e != nil {
			break
		}
		if p.ConfigPath != "" {
			v.config, e = verifiedEnvironmentFile(p.ConfigPath, p.ConfigSHA)
			if e != nil {
				break
			}
		}
		g.processes[p.ID] = v
	}
	if e == nil {
		e = g.sample(parent)
	}
	if e != nil {
		stop()
		close(g.done)
		return g, errors.New("environment preflight failed")
	}
	// Stop future samples, but let the bounded in-flight sample finish so the
	// exact observation that canceled workers is not lost during finalization.
	go func() {
		defer close(g.done)
		t := time.NewTicker(2 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-parent.Done():
				return
			case <-stopping:
				return
			case <-t.C:
				_ = g.sample(parent)
			}
		}
	}()
	return g, nil
}
func (g *environmentGuard) readProcess(ctx context.Context, p environmentProcessSpec) (EnvironmentTargetSample, error) {
	v := EnvironmentTargetSample{Kind: "process", RSSBudget: p.RSSBudget, FileBudget: p.FileBudget}
	s := g.processes[p.ID]
	if !unchangedEnvironmentFile(p.Executable, s.binary) || (p.ConfigPath != "" && !unchangedEnvironmentFile(p.ConfigPath, s.config)) {
		return v, errors.New("process_source_changed")
	}
	r, e := readProcessResources(ctx, p.PID)
	if e != nil {
		return v, e
	}
	if r.Identity != s.identity || r.Executable != p.Executable || r.Command != p.Command || !processOwnedBy(p.PID, g.spec.OwnerPID) {
		return v, errors.New("process_identity_changed")
	}
	v.At = time.Now().UTC()
	v.Epoch = processEpoch(r.Identity)
	v.RSS = r.RSS
	v.Files = r.Files
	v.FileLimit = r.FileLimit
	v.CPU = r.CPU
	v.CPUUnit = r.CPUUnit
	return v, nil
}
func environmentTargetViolation(v EnvironmentTargetSample, minDisk uint64) string {
	if v.At.IsZero() || math.IsNaN(v.CPU) || math.IsInf(v.CPU, 0) || v.CPU < 0 {
		return "target_sample_invalid"
	}
	switch v.Kind {
	case "process":
		if v.RSS == 0 || v.Files == 0 || v.RSSBudget == 0 || v.FileBudget == 0 || v.Epoch == "" || v.CPUUnit == "" {
			return "process_sample_invalid"
		}
		if v.RSS >= v.RSSBudget*85/100 {
			return "process_rss"
		}
		limit := v.FileBudget
		if v.FileLimit > 0 {
			limit = min(limit, v.FileLimit)
		}
		if v.Files >= limit*85/100 {
			return "process_open_files"
		}
	case "container":
		if v.MemoryBytes == 0 || v.MemoryLimit == 0 || v.MemoryBudget == 0 || v.MemoryBudget > v.MemoryLimit {
			return "container_sample_invalid"
		}
		if v.MemoryBytes >= v.MemoryBudget*85/100 {
			return "container_memory"
		}
	case "volume":
	default:
		return "target_kind_invalid"
	}
	if v.Kind != "process" {
		if v.Disk.Total == 0 || v.Disk.Free > v.Disk.Total {
			return "disk_sample_invalid"
		}
		if v.Disk.Free < max(minDisk, v.Disk.Total/100*15) {
			return "disk_free"
		}
	}
	return ""
}
func (g *environmentGuard) sample(ctx context.Context) error {
	owner, e := readProcessIdentity(g.spec.OwnerPID)
	if e != nil || owner != g.owner {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		g.safety.trip("environment", "guardian_identity_changed", time.Now().UTC())
		g.mu.Lock()
		g.result.Errors++
		g.mu.Unlock()
		g.events.write(map[string]any{"type": "environment_error", "at": time.Now().UTC(), "class": "guardian_identity_changed"})
		return errors.New("guardian_identity_changed")
	}
	collectCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	type result struct {
		id       string
		value    EnvironmentTargetSample
		database *environmentDatabaseSample
		err      error
	}
	count := len(g.spec.Processes) + len(g.spec.Containers) + len(g.spec.Volumes) + 1
	out := make(chan result, count)
	sem := make(chan struct{}, 4)
	var wg sync.WaitGroup
	launch := func(id string, fn func() result) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-collectCtx.Done():
				out <- result{id: id, err: errors.New("sample_timeout")}
				return
			}
			r := fn()
			r.id = id
			out <- r
		}()
	}
	for _, p := range g.spec.Processes {
		launch(p.ID, func() result { v, e := g.readProcess(collectCtx, p); return result{value: v, err: e} })
	}
	for _, c := range g.spec.Containers {
		launch(c.ID, func() result {
			v, e := readEnvironmentContainer(collectCtx, g.spec, c)
			return result{value: v, err: e}
		})
	}
	for _, v := range g.spec.Volumes {
		launch(v.ID, func() result {
			d, e := nativeDiskResources(v.Path)
			return result{value: EnvironmentTargetSample{Kind: "volume", At: time.Now().UTC(), Disk: d}, err: e}
		})
	}
	launch("queues", func() result { v, e := readEnvironmentQueues(collectCtx, g.spec); return result{database: &v, err: e} })
	go func() { wg.Wait(); close(out) }()
	s := EnvironmentSample{At: time.Now().UTC(), Targets: make(map[string]EnvironmentTargetSample, count-1)}
	issues := uint64(0)
	issue := func(id, class string) {
		if ctx.Err() != nil {
			return
		}
		issues++
		if s.Error == "" {
			s.Error = id + ":" + class
		}
		g.safety.trip("environment", id+":"+class, time.Now().UTC())
	}
	for r := range out {
		if r.err != nil {
			class := environmentErrorClass(r.err)
			if collectCtx.Err() != nil {
				class = "sample_timeout"
			}
			r.value.Error = class
			issue(r.id, class)
		} else if r.database != nil {
			s.DatabaseAt = r.database.ObservedAt
			s.Queues = r.database.Queues
			for _, name := range g.observeQueueAges(s.Queues) {
				issue(name, "backlog_age")
			}
		} else if class := environmentTargetViolation(r.value, g.spec.MinDiskFree); class != "" {
			r.value.Error = class
			issue(r.id, class)
		}
		if r.id != "queues" {
			s.Targets[r.id] = r.value
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if collectCtx.Err() != nil && s.Error == "" {
		issue("observer", "sample_timeout")
	}
	if s.Queues == nil {
		g.previousQueues = nil
		clear(g.rising)
	}
	s.At = time.Now().UTC()
	g.mu.Lock()
	g.result.Samples++
	g.result.Errors += issues
	if g.result.Samples == 1 {
		g.result.First = s
	}
	g.result.Last = s
	for id, v := range s.Targets {
		p := g.result.Peak[id]
		p.Kind = v.Kind
		p.RSS = max(p.RSS, v.RSS)
		p.Files = max(p.Files, v.Files)
		p.MemoryBytes = max(p.MemoryBytes, v.MemoryBytes)
		p.CPU = max(p.CPU, v.CPU)
		p.CPUUnit = v.CPUUnit
		g.result.Peak[id] = p
	}
	for id, q := range s.Queues {
		p := g.result.QueuePeak[id]
		p.Rows = max(p.Rows, q.Rows)
		p.AgeSeconds = max(p.AgeSeconds, q.AgeSeconds)
		g.result.QueuePeak[id] = p
	}
	g.mu.Unlock()
	g.events.write(map[string]any{"type": "environment_sample", "environment": s})
	if s.Error != "" {
		return errors.New("environment_sample_failed")
	}
	return nil
}
func environmentErrorClass(err error) string {
	// Native/command errors can contain paths or arguments. Only this finite
	// vocabulary is allowed in shareable evidence.
	switch err.Error() {
	case "process_identity_changed", "process_identity_missing", "process_source_changed", "process_sample_invalid",
		"container_identity_or_limit", "container_not_loopback", "container_data_mount_missing", "container_disk_invalid",
		"database_identity_changed", "database_clock_or_coverage", "backlog_incomplete_or_invalid", "queue_age_clock_mismatch",
		"cgroup_sample_invalid", "cgroup_memory_invalid", "cgroup_limit_invalid", "cgroup_cpu_invalid", "cgroup_cpu_missing":
		return err.Error()
	default:
		return "sample_failed"
	}
}
func (g *environmentGuard) observeQueueAges(queues map[string]environmentQueueSample) []string {
	if queues == nil {
		g.previousQueues = nil
		clear(g.rising)
		return nil
	}
	var failed []string
	for name, q := range queues {
		prev, ok := g.previousQueues[name]
		if q.Rows > 0 && q.AgeSeconds > 60 && ok && prev.Rows > 0 && q.AgeSeconds > prev.AgeSeconds {
			g.rising[name]++
		} else if q.Rows > 0 && q.AgeSeconds > 60 {
			g.rising[name] = 1
		} else {
			g.rising[name] = 0
		}
		if g.rising[name] >= 3 {
			failed = append(failed, name)
		}
	}
	g.previousQueues = queues
	return failed
}
func (g *environmentGuard) close() {
	if g != nil {
		g.stop()
		<-g.done
	}
}
func (g *environmentGuard) finish(ctx context.Context) {
	if g != nil {
		g.close()
		_ = g.sample(ctx)
	}
}
func (g *environmentGuard) report() EnvironmentReport {
	if g == nil {
		return EnvironmentReport{Version: environmentVersion, Provider: "not_configured"}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	r := g.result
	r.Peak = maps.Clone(r.Peak)
	r.QueuePeak = maps.Clone(r.QueuePeak)
	return r
}
