package loadharness

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

type ResourceLimits struct {
	HeapBytes        uint64 `json:"heap_bytes"`
	RSSBytes         uint64 `json:"rss_bytes"`
	OpenFiles        uint64 `json:"open_files"`
	MinDiskFreeBytes uint64 `json:"min_disk_free_bytes"`
}

func (l ResourceLimits) defaults() ResourceLimits {
	if l.HeapBytes == 0 {
		l.HeapBytes = 2 << 30
	}
	if l.RSSBytes == 0 {
		l.RSSBytes = 4 << 30
	}
	if l.OpenFiles == 0 {
		l.OpenFiles = 65536
	}
	if l.MinDiskFreeBytes == 0 {
		l.MinDiskFreeBytes = 20 << 30
	}
	return l
}
func (l ResourceLimits) validate() error {
	l = l.defaults()
	if l.HeapBytes < 1<<20 || l.HeapBytes > 1<<40 || l.RSSBytes < 1<<20 || l.RSSBytes > 1<<40 || l.OpenFiles < 16 || l.OpenFiles > 1<<20 || l.MinDiskFreeBytes < 20<<30 || l.MinDiskFreeBytes > 1<<50 {
		return errors.New("invalid resource budgets; disk safety floor cannot be lowered below 20GiB")
	}
	return nil
}

type DiskResourceSample struct {
	Free  uint64 `json:"free_bytes"`
	Total uint64 `json:"total_bytes"`
}
type ClientResourceSample struct {
	At            time.Time                     `json:"at"`
	Heap          uint64                        `json:"heap_bytes"`
	RSS           uint64                        `json:"rss_bytes"`
	OpenFiles     uint64                        `json:"open_files"`
	SoftFileLimit uint64                        `json:"soft_file_limit"`
	CPUSeconds    float64                       `json:"cpu_seconds"`
	Disks         map[string]DiskResourceSample `json:"disks"`
}
type ResourceGuardReport struct {
	Version       int                  `json:"version"`
	Platform      string               `json:"platform"`
	IntervalMS    int                  `json:"interval_ms"`
	Limits        ResourceLimits       `json:"limits"`
	Samples       uint64               `json:"samples"`
	Errors        uint64               `json:"errors"`
	First         ClientResourceSample `json:"first"`
	Last          ClientResourceSample `json:"last"`
	PeakHeap      uint64               `json:"peak_heap_bytes"`
	PeakRSS       uint64               `json:"peak_rss_bytes"`
	PeakOpenFiles uint64               `json:"peak_open_files"`
	Coverage      []string             `json:"coverage"`
}

type resourceGuard struct {
	mu       sync.Mutex
	result   ResourceGuardReport
	stop     context.CancelFunc
	done     chan struct{}
	safety   *loadSafety
	events   *eventWriter
	business *businessGuard
	paths    map[string]string
}

func resourceViolation(v ClientResourceSample, l ResourceLimits) string {
	l = l.defaults()
	if v.At.IsZero() || v.Heap == 0 || v.RSS == 0 || v.OpenFiles == 0 || v.SoftFileLimit == 0 || math.IsNaN(v.CPUSeconds) || math.IsInf(v.CPUSeconds, 0) || v.CPUSeconds < 0 || len(v.Disks) != 3 {
		return "invalid_sample"
	}
	if v.Heap >= l.HeapBytes*85/100 {
		return "client_heap"
	}
	if v.RSS >= l.RSSBytes*85/100 {
		return "client_rss"
	}
	if v.OpenFiles >= min(l.OpenFiles, v.SoftFileLimit)*85/100 {
		return "client_open_files"
	}
	for _, id := range []string{"report", "events", "sessions"} {
		d, ok := v.Disks[id]
		if !ok || d.Total == 0 || d.Free > d.Total {
			return "invalid_disk_sample"
		}
		if d.Free < max(l.MinDiskFreeBytes, d.Total/100*15) {
			return "disk_" + id
		}
	}
	return ""
}

func readClientResources(ctx context.Context, paths map[string]string) (ClientResourceSample, error) {
	v := ClientResourceSample{Disks: make(map[string]DiskResourceSample)}
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	v.Heap = m.HeapAlloc
	var err error
	v.RSS, v.OpenFiles, v.SoftFileLimit, v.CPUSeconds, err = nativeClientResources(ctx)
	if err != nil {
		return v, errors.New("process_sample_failed")
	}
	for _, id := range []string{"report", "events", "sessions"} {
		d, err := nativeDiskResources(paths[id])
		if err != nil {
			return v, errors.New("disk_sample_failed")
		}
		v.Disks[id] = d
	}
	v.At = time.Now().UTC()
	return v, nil
}

func newResourceGuard(parent context.Context, limits ResourceLimits, report, events, manifest string, safety *loadSafety, writer *eventWriter, business *businessGuard) (*resourceGuard, error) {
	ctx, cancel := context.WithCancel(parent)
	g := &resourceGuard{stop: cancel, done: make(chan struct{}), safety: safety, events: writer, business: business,
		paths:  map[string]string{"report": filepath.Dir(report), "events": filepath.Dir(events), "sessions": filepath.Dir(manifest)},
		result: ResourceGuardReport{Version: 1, Platform: runtime.GOOS, IntervalMS: 1000, Limits: limits.defaults(), Coverage: []string{"load_heap", "load_rss", "load_open_files", "load_cpu", "local_artifact_volumes"}}}
	if err := g.sample(ctx); err != nil {
		cancel()
		close(g.done)
		return g, err
	}
	go func() {
		defer close(g.done)
		timer := time.NewTicker(time.Second)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
				_ = g.sample(ctx)
			}
		}
	}()
	return g, nil
}

func (g *resourceGuard) sample(ctx context.Context) error {
	sampleCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	v, err := readClientResources(sampleCtx, g.paths)
	// Normal observer shutdown is not a resource failure.
	if ctx.Err() != nil {
		return ctx.Err()
	}
	g.mu.Lock()
	class := ""
	if err != nil {
		class = err.Error()
		g.result.Errors++
	} else {
		if sampleCtx.Err() != nil {
			class = "sample_deadline"
			g.result.Errors++
		} else {
			class = resourceViolation(v, g.result.Limits)
		}
		g.result.Samples++
		if g.result.Samples == 1 {
			g.result.First = v
		}
		g.result.Last = v
		g.result.PeakHeap = max(g.result.PeakHeap, v.Heap)
		g.result.PeakRSS = max(g.result.PeakRSS, v.RSS)
		g.result.PeakOpenFiles = max(g.result.PeakOpenFiles, v.OpenFiles)
	}
	g.mu.Unlock()
	if class != "" {
		g.safety.trip("resources", class, time.Now().UTC())
	}
	b := g.business.evaluate(time.Now())
	if b.Tripped {
		g.safety.trip("business", "error_rate", time.Now().UTC())
	}
	g.events.write(map[string]any{"type": "safety_sample", "at": time.Now().UTC(), "resources": v, "resource_error": class, "business": b})
	if class != "" {
		return fmt.Errorf("resource guard: %s", class)
	}
	return nil
}
func (g *resourceGuard) close() { g.stop(); <-g.done }
func (g *resourceGuard) finish(ctx context.Context) {
	g.close()
	// Preserve one observation after workers and the configured recovery window
	// have ended, even when they end between ticks. This is not a retention wait.
	_ = g.sample(ctx)
}
func (g *resourceGuard) report() ResourceGuardReport {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.result
}
