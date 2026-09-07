package loadharness

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestEventOversizedSampleStopsBeforeWrite(t *testing.T) {
	w, err := newEventWriter(filepath.Join(t.TempDir(), "events.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	defer w.close()
	w.write(map[string]any{"type": "sample", "payload": strings.Repeat("x", 8<<20)})
	if written, dropped := w.counts(); written != 0 || dropped != 1 {
		t.Fatalf("oversized sample escaped bound: written=%d dropped=%d", written, dropped)
	}
}

func TestEventExistingEvidenceIsNeverTruncated(t *testing.T) {
	p := filepath.Join(t.TempDir(), "events.ndjson")
	if err := os.WriteFile(p, []byte("original evidence\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	w, err := newEventWriter(p)
	if err == nil {
		_ = w.close()
		t.Error("existing evidence accepted")
	}
	data, err := os.ReadFile(p)
	if err != nil || string(data) != "original evidence\n" {
		t.Fatal("prior evidence destroyed")
	}
}

func smallEventWriter(t *testing.T, limits EventLimits) *eventWriter {
	t.Helper()
	w, err := newBoundedEventWriter(filepath.Join(t.TempDir(), "events.ndjson"), limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = w.close() })
	return w
}

func TestEventRotationOrderAndAudit(t *testing.T) {
	w := smallEventWriter(t, EventLimits{LineBytes: 1024, SegmentBytes: 1024, TotalBytes: 1 << 20})
	for i := 0; i < 200; i++ {
		w.write(map[string]any{"sequence": i, "payload": strings.Repeat("x", 80)})
	}
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
	r := w.report()
	if !r.Finalized || r.Written != 200 || len(r.Segments) < 2 {
		t.Fatalf("bad report: %+v", r)
	}
	sequence, totalBytes := 0, int64(0)
	for _, seg := range w.segments {
		data, err := os.ReadFile(seg.path)
		if err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(data)
		if int64(len(data)) != seg.Bytes || seg.Bytes > 1024 || hex.EncodeToString(digest[:]) != seg.SHA256 {
			t.Fatal("segment evidence mismatch")
		}
		s := bufio.NewScanner(strings.NewReader(string(data)))
		for s.Scan() {
			var v struct{ Sequence int }
			if err := json.Unmarshal(s.Bytes(), &v); err != nil || v.Sequence != sequence {
				t.Fatal("rotation lost/reordered event")
			}
			sequence++
		}
		if err := s.Err(); err != nil {
			t.Fatal(err)
		}
		totalBytes += seg.Bytes
		info, _ := os.Stat(seg.path)
		if info.Mode().Perm() != 0o600 {
			t.Fatal("event not owner-only")
		}
	}
	if sequence != 200 || totalBytes != r.Bytes {
		t.Fatal("aggregate mismatch")
	}
	data, err := os.ReadFile(filepath.Join(w.dir, "index.json"))
	if err != nil {
		t.Fatal(err)
	}
	var index EventEvidenceReport
	if err := json.Unmarshal(data, &index); err != nil || !reflect.DeepEqual(index, r) {
		t.Fatal("index/report mismatch")
	}
	w.write(map[string]any{"late": true})
	if w.report().Error != "write_after_close" {
		t.Fatal("late write did not invalidate evidence")
	}
}

func TestEventExactTotalBoundStopsBothWorkloads(t *testing.T) {
	// The same cancellation boundary is used by Run and StartupRun, including
	// delayed ramp tasks. No polling/sampling timer is needed to propagate it.
	w := smallEventWriter(t, EventLimits{LineBytes: 1024, SegmentBytes: 1024, TotalBytes: 1024})
	ctx, safety := newLoadSafety(context.Background())
	defer safety.cancel()
	w.onFailure = safety.eventsFailure
	// JSON string "x" + LF is exactly four bytes.
	for i := 0; i < 256; i++ {
		w.write("x")
	}
	if ctx.Err() != nil || w.report().Bytes != 1024 {
		t.Fatal("exact quota rejected")
	}
	w.write("x")
	if ctx.Err() == nil || w.report().Error != "total_bytes" || w.report().Bytes != 1024 {
		t.Fatal("quota did not synchronously stop load")
	}
	first := safety.report()
	safety.trip("delivery_ledger", "other", time.Now())
	if safety.report() != first {
		t.Fatal("first cause was overwritten")
	}
	if err := w.close(); err == nil {
		t.Fatal("exhausted evidence passed audit")
	}
}

type forbiddenEventMarshaler struct{ calls *atomic.Int64 }

func (v forbiddenEventMarshaler) MarshalJSON() ([]byte, error) {
	v.calls.Add(1)
	return []byte("null"), nil
}

func TestEventAdmissionRejectsCyclesAndCustomEncodingWithoutCallingIt(t *testing.T) {
	var calls atomic.Int64
	cycle := map[string]any{}
	cycle["self"] = cycle
	for name, value := range map[string]any{"cycle": cycle, "marshaler": forbiddenEventMarshaler{&calls}, "function": func() {}} {
		t.Run(name, func(t *testing.T) {
			w := smallEventWriter(t, EventLimits{})
			w.write(value)
			if w.report().Error != "encoding_budget" {
				t.Fatal("unbounded encoding admitted")
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatal("custom marshaler ran")
	}
	// Admission of an already allocated huge string must not allocate a JSON copy.
	value := reflect.ValueOf(strings.Repeat("x", 8<<20))
	allocs := testing.AllocsPerRun(100, func() {
		budget := int64(1024)
		if eventEncodingFits(value, &budget, 0) {
			t.Fatal("oversized value admitted")
		}
	})
	if allocs != 0 {
		t.Fatalf("oversized admission allocated %f", allocs)
	}
}

func TestEventOptionalStandardTimeStaysBounded(t *testing.T) {
	now := time.Now().UTC()
	for _, value := range []*time.Time{nil, &now} {
		budget := int64(64)
		if !eventEncodingFits(reflect.ValueOf(value), &budget, 0) {
			t.Fatal("bounded standard timestamp rejected")
		}
		budget = 1
		if eventEncodingFits(reflect.ValueOf(value), &budget, 0) {
			t.Fatal("timestamp bypassed encoding budget")
		}
	}
	w := smallEventWriter(t, EventLimits{})
	w.write(map[string]any{"type": "environment_sample", "environment": EnvironmentSample{At: now, Queues: map[string]environmentQueueSample{
		"empty": {}, "pending": {Rows: 1, Oldest: &now},
	}}})
	if err := w.close(); err != nil || w.report().Written != 1 || w.report().Dropped != 0 {
		t.Fatal("environment timestamps did not survive bounded evidence audit", err)
	}
}

type eventFaultFile struct {
	eventFile
	writeAfter, writes int
	partial            bool
	syncFail           bool
}

func (f *eventFaultFile) Write(p []byte) (int, error) {
	f.writes++
	if f.writeAfter > 0 && f.writes >= f.writeAfter {
		if f.partial {
			n, _ := f.eventFile.Write(p[:len(p)/2])
			return n, io.ErrShortWrite
		}
		return 0, errors.New("private path/credential must not enter report")
	}
	return f.eventFile.Write(p)
}
func (f *eventFaultFile) Sync() error {
	if f.syncFail {
		return errors.New("private sync detail")
	}
	return f.eventFile.Sync()
}

func TestEventIOAndAuditFailuresRemainTerminal(t *testing.T) {
	for _, mode := range []string{"write", "partial", "sync", "corrupt_early", "truncate", "unlink", "replace", "index_exists", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			w := smallEventWriter(t, EventLimits{LineBytes: 1024, SegmentBytes: 1024, TotalBytes: 1 << 20})
			ctx, s := newLoadSafety(context.Background())
			defer s.cancel()
			w.onFailure = s.eventsFailure
			for i := 0; i < 100; i++ {
				w.write(strings.Repeat("x", 100))
			}
			if len(w.segments) < 2 {
				t.Fatal("no early segment")
			}
			first := w.segments[0].path
			auditCtx := context.Background()
			switch mode {
			case "write", "partial":
				w.f = &eventFaultFile{eventFile: w.f, writeAfter: 1, partial: mode == "partial"}
				w.write("fault")
			case "sync":
				w.f = &eventFaultFile{eventFile: w.f, syncFail: true}
			case "corrupt_early":
				f, _ := os.OpenFile(first, os.O_WRONLY, 0)
				_, err := f.WriteAt([]byte("X"), 2)
				_ = f.Close()
				if err != nil {
					t.Fatal(err)
				}
			case "truncate":
				if err := os.Truncate(first, 1); err != nil {
					t.Fatal(err)
				}
			case "unlink":
				if err := os.Remove(first); err != nil {
					t.Fatal(err)
				}
			case "replace":
				data, _ := os.ReadFile(first)
				if err := os.Rename(first, first+".original"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(first, data, 0o600); err != nil {
					t.Fatal(err)
				}
			case "index_exists":
				if err := os.WriteFile(filepath.Join(w.dir, "index.json"), []byte("prior"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				c, cancel := context.WithCancel(context.Background())
				cancel()
				auditCtx = c
			}
			if err := w.finalize(auditCtx); err == nil || w.report().Finalized || ctx.Err() == nil {
				t.Fatal("failed evidence did not stop/fail")
			}
			data, _ := json.Marshal(w.report())
			if strings.Contains(string(data), "private") || strings.Contains(string(data), w.path) {
				t.Fatal("private IO detail leaked")
			}
		})
	}
}

func TestEventConcurrentWritersCannotOverrunQuota(t *testing.T) {
	w := smallEventWriter(t, EventLimits{LineBytes: 1024, SegmentBytes: 1024, TotalBytes: 4096})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				w.write("x")
			}
		}()
	}
	wg.Wait()
	if err := w.close(); err == nil {
		t.Fatal("quota did not fail")
	}
	r := w.report()
	if r.Bytes != 4096 || r.Written != 1024 || r.Dropped != 3200-1024 || len(r.Segments) != 4 {
		t.Fatalf("bad bounded counts: %+v", r)
	}
}

func TestEventSegmentLimitNeverDeletesHistory(t *testing.T) {
	w := smallEventWriter(t, EventLimits{LineBytes: 1024, SegmentBytes: 1024, TotalBytes: 2 << 20})
	for i := 0; i < 11000; i++ {
		w.write(strings.Repeat("x", 100))
	}
	if err := w.close(); err == nil || w.report().Error != "segment_limit" || len(w.segments) != 1024 {
		t.Fatal("segment metadata unbounded")
	}
	if _, err := os.Stat(w.path); err != nil {
		t.Fatal("first evidence deleted")
	}
}

func TestEventReportCannotPassWithoutFinalizationOrAfterStop(t *testing.T) {
	base := RunReport{Version: 12, ExpectedSessions: 1, PeakReadySessions: 1, SteadySamples: 1, SteadyReadyRatio: 1,
		Delivery:      DeliveryReport{Ledger: DeliveryLedgerReport{Version: deliveryLedgerVersion, AuditComplete: true}},
		EventEvidence: EventEvidenceReport{Version: 1, Finalized: true}}
	for _, mode := range []string{"audit", "drop", "stop"} {
		t.Run(mode, func(t *testing.T) {
			r := base
			switch mode {
			case "audit":
				r.EventEvidence.Finalized = false
			case "drop":
				r.EventsDropped = 1
			case "stop":
				r.SafetyStop = SafetyStopReport{Source: "events", Class: "total_bytes"}
			}
			evaluateReport(&r, RunConfig{MinimumReadyRatio: 1, ExpectServerRestart: true})
			if r.Pass {
				t.Fatal("unsafe evidence passed")
			}
		})
	}
}

func TestEventPreviousMetricsSamplesFitDefaultBudget(t *testing.T) {
	// A full topology sample uses the same inert DTO shape as the real collector.
	values := make(map[string]float64)
	for i := 0; i < 8192; i++ {
		values[fmt.Sprintf("telesrv_counter_%04d{outcome=\"ok\"}", i)] = float64(i)
	}
	v := map[string]any{"at": time.Now(), "metrics_targets": map[string]MetricsTargetSample{"edge/one": {Role: "edge", Instance: "one", Values: values}}}
	w := smallEventWriter(t, EventLimits{})
	w.write(v)
	if err := w.close(); err != nil {
		t.Fatal(err)
	}
}

type eventAuditContext struct {
	context.Context
	onErr func()
}

func (c eventAuditContext) Err() error { c.onErr(); return nil }

func TestEventEarlierSegmentChangedDuringLaterAuditFails(t *testing.T) {
	w := smallEventWriter(t, EventLimits{LineBytes: 1024, SegmentBytes: 1024, TotalBytes: 1 << 20})
	for i := 0; i < 100; i++ {
		w.write(strings.Repeat("x", 100))
	}
	checks := 0
	ctx := eventAuditContext{Context: context.Background(), onErr: func() {
		checks++
		if checks == 5 {
			f, err := os.OpenFile(w.segments[0].path, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			_, err = f.WriteAt([]byte("Y"), 1)
			_ = f.Close()
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().Add(time.Second)
			if err := os.Chtimes(w.segments[0].path, now, now); err != nil {
				t.Fatal(err)
			}
		}
	}}
	if err := w.finalize(ctx); err == nil || w.report().Error != "segment_changed" {
		t.Fatal("earlier segment changed after its checksum audit was accepted")
	}
}

func TestEventReportPublicationCannotReplaceExistingEvidence(t *testing.T) {
	p := filepath.Join(t.TempDir(), "report.json")
	if err := writeNewEvidenceReport(p, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := writeNewEvidenceReport(p, []byte("second")); err == nil {
		t.Fatal("prior report replaced")
	}
	data, _ := os.ReadFile(p)
	if string(data) != "first" {
		t.Fatal("prior report changed")
	}
	if err := requireNewReport(p); err == nil {
		t.Fatal("preflight accepted existing report")
	}
	for _, pair := range [][2]string{{p, p}, {p, p + ".delivery/events"}, {p + ".segments/index.json", p}, {p, p + "/events"}, {p + "/report", p}} {
		if err := validateEvidencePaths(pair[0], pair[1]); err == nil {
			t.Fatal("overlapping outputs accepted")
		}
	}
}
