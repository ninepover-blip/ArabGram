package loadharness

import (
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"time"
)

const eventEvidenceVersion = 1

func validateEvidencePaths(report, events string) error {
	r, err := filepath.Abs(report)
	if err != nil {
		return err
	}
	e, err := filepath.Abs(events)
	if err != nil {
		return err
	}
	inside := func(path, dir string) bool {
		rel, err := filepath.Rel(dir, path)
		return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
	}
	if inside(r, e) || inside(e, r) || inside(e, r+".delivery") || inside(r, e+".segments") {
		return errors.New("report, event and delivery evidence paths overlap")
	}
	return nil
}

func requireNewReport(path string) error {
	_, err := os.Lstat(path)
	if err == nil {
		return errors.New("report already exists; use a new run path")
	}
	if !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Publish without replacing any concurrent or earlier result. The temporary
// file shares the destination filesystem; unsupported hard links fail closed.
func writeNewEvidenceReport(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".load-report-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if n, err := f.Write(data); err != nil || n != len(data) {
		_ = f.Close()
		return errors.New("report write failed")
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Link(f.Name(), path)
}

// EventLimits bounds retained evidence. Rotation never deletes earlier segments.
// Zero fields select defaults; no value disables a bound.
type EventLimits struct {
	LineBytes    int64 `json:"line_bytes"`
	SegmentBytes int64 `json:"segment_bytes"`
	TotalBytes   int64 `json:"total_bytes"`
}

func (l EventLimits) defaults() EventLimits {
	if l.LineBytes == 0 {
		l.LineBytes = 8 << 20
	}
	if l.SegmentBytes == 0 {
		l.SegmentBytes = 64 << 20
	}
	if l.TotalBytes == 0 {
		l.TotalBytes = 4 << 30
	}
	return l
}

func (l EventLimits) validate() error {
	l = l.defaults()
	if l.LineBytes < 1024 || l.LineBytes > 32<<20 || l.SegmentBytes < l.LineBytes || l.SegmentBytes > 1<<30 || l.TotalBytes < l.SegmentBytes || l.TotalBytes > 1<<40 {
		return errors.New("event limits require 1KiB <= line <= 32MiB, line <= segment <= 1GiB, segment <= total <= 1TiB")
	}
	return nil
}

type EventSegmentReport struct {
	Index  int    `json:"index"`
	Bytes  int64  `json:"bytes"`
	Lines  uint64 `json:"lines"`
	SHA256 string `json:"sha256"`
}

type EventEvidenceReport struct {
	Version   int                  `json:"version"`
	Limits    EventLimits          `json:"limits"`
	Written   uint64               `json:"written"`
	Dropped   uint64               `json:"dropped"`
	Bytes     int64                `json:"bytes"`
	Finalized bool                 `json:"finalized"`
	Error     string               `json:"error,omitempty"`
	FailedAt  time.Time            `json:"failed_at,omitempty"`
	Segments  []EventSegmentReport `json:"segments"`
}

type eventSegment struct {
	EventSegmentReport
	path     string
	identity os.FileInfo
}

type eventFile interface {
	Write([]byte) (int, error)
	Sync() error
	Close() error
}

type eventWriter struct {
	mu                sync.Mutex
	f                 eventFile
	path, dir         string
	limits            EventLimits
	segments          []eventSegment
	hash              hash.Hash
	written, dropped  uint64
	bytes             int64
	firstError        string
	failedAt          time.Time
	failed            chan struct{}
	onFailure         func(string, time.Time) // Set before any producer starts; never calls back into the writer.
	finalized, closed bool
}

func newEventWriter(path string) (*eventWriter, error) {
	return newBoundedEventWriter(path, EventLimits{})
}

func newBoundedEventWriter(path string, limits EventLimits) (*eventWriter, error) {
	if path == "" {
		return nil, errors.New("an event evidence path is required")
	}
	if err := limits.validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	w := &eventWriter{path: path, dir: path + ".segments", limits: limits.defaults(), failed: make(chan struct{})}
	// This directory also reserves future segment names before any load starts.
	if err := os.Mkdir(w.dir, 0o700); err != nil {
		return nil, err
	}
	if err := w.openSegment(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *eventWriter) openSegment() error {
	index := len(w.segments)
	if index >= 1024 {
		return errors.New("segment_limit")
	}
	path := w.path
	if index > 0 {
		path = filepath.Join(w.dir, fmt.Sprintf("%06d.ndjson", index))
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return errors.New("segment_create")
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return errors.New("segment_stat")
	}
	w.f, w.hash = f, sha256.New()
	w.segments = append(w.segments, eventSegment{EventSegmentReport: EventSegmentReport{Index: index}, path: path, identity: info})
	return nil
}

func (w *eventWriter) failLocked(class string) {
	if w.firstError != "" {
		return
	}
	w.firstError, w.failedAt = class, time.Now().UTC()
	close(w.failed)
	if w.onFailure != nil {
		w.onFailure(class, w.failedAt)
	}
}

func (w *eventWriter) write(value any) {
	if w == nil {
		return
	} // Explicit test workers may omit evidence; Run never does.
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.firstError != "" {
		w.dropped++
		return
	}
	if w.closed {
		w.dropped++
		w.failLocked("write_after_close")
		return
	}
	// Check before Marshal, including traversal/depth and custom marshaler guards.
	// Only one bounded encoding is in flight, independent of worker concurrency.
	budget := w.limits.LineBytes - 1
	if !eventEncodingFits(reflect.ValueOf(value), &budget, 0) {
		w.dropped++
		w.failLocked("encoding_budget")
		return
	}
	data, err := json.Marshal(value)
	if err != nil {
		w.dropped++
		w.failLocked("encode")
		return
	}
	data = append(data, '\n')
	if int64(len(data)) > w.limits.LineBytes {
		w.dropped++
		w.failLocked("line_bytes")
		return
	}
	if int64(len(data)) > w.limits.TotalBytes-w.bytes {
		w.dropped++
		w.failLocked("total_bytes")
		return
	}
	seg := &w.segments[len(w.segments)-1]
	if seg.Bytes+int64(len(data)) > w.limits.SegmentBytes {
		w.sealSegment()
		if w.firstError != "" {
			w.dropped++
			return
		}
		if err := w.openSegment(); err != nil {
			w.dropped++
			w.failLocked(err.Error())
			return
		}
		seg = &w.segments[len(w.segments)-1]
	}
	n, err := w.f.Write(data)
	if n > 0 {
		_, _ = w.hash.Write(data[:n])
		w.bytes += int64(n)
		seg.Bytes += int64(n)
	}
	if err != nil || n != len(data) {
		w.dropped++
		w.failLocked("write")
		return
	}
	w.written++
	seg.Lines++
}

func (w *eventWriter) sealSegment() {
	if w.f == nil {
		return
	}
	w.segments[len(w.segments)-1].SHA256 = hex.EncodeToString(w.hash.Sum(nil))
	if err := w.f.Sync(); err != nil {
		w.failLocked("sync")
	}
	if err := w.f.Close(); err != nil {
		w.failLocked("close")
	}
	w.f = nil
}

func (w *eventWriter) finalize(ctx context.Context) error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.closed {
		w.closed = true
		w.sealSegment()
		// Verify even a failed run's surviving prefix where possible. An earlier
		// failure remains authoritative and is never cleared by a successful audit.
		buf := make([]byte, 64<<10)
		before := make([]os.FileInfo, len(w.segments))
		for i, seg := range w.segments {
			before[i], _ = os.Lstat(seg.path)
		}
		for _, seg := range w.segments {
			if err := auditEventSegment(ctx, seg, buf); err != nil {
				w.failLocked(err.Error())
				break
			}
		}
		for i, seg := range w.segments {
			after, err := os.Lstat(seg.path)
			if err != nil || before[i] == nil || !os.SameFile(before[i], after) || before[i].Size() != after.Size() || !before[i].ModTime().Equal(after.ModTime()) {
				w.failLocked("segment_changed")
			}
		}
		w.finalized = w.firstError == ""
		data, err := json.MarshalIndent(w.reportLocked(), "", "  ")
		if err != nil {
			w.failLocked("index_encode")
		} else {
			f, err := os.OpenFile(filepath.Join(w.dir, "index.json"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
			if err != nil {
				w.failLocked("index_create")
			} else {
				data = append(data, '\n')
				if n, err := f.Write(data); err != nil || n != len(data) {
					w.failLocked("index_write")
				}
				if err := f.Sync(); err != nil {
					w.failLocked("index_sync")
				}
				if err := f.Close(); err != nil {
					w.failLocked("index_close")
				}
			}
		}
		w.finalized = w.firstError == ""
	}
	if w.firstError != "" {
		return errors.New(w.firstError)
	}
	return nil
}

func auditEventSegment(ctx context.Context, seg eventSegment, buf []byte) error {
	if ctx.Err() != nil {
		return errors.New("audit_canceled")
	}
	before, err := os.Lstat(seg.path)
	if err != nil || !before.Mode().IsRegular() || !os.SameFile(seg.identity, before) || before.Size() != seg.Bytes {
		return errors.New("segment_identity")
	}
	f, err := os.Open(seg.path)
	if err != nil {
		return errors.New("audit_open")
	}
	defer f.Close()
	opened, err := f.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return errors.New("segment_identity")
	}
	h := sha256.New()
	remaining := seg.Bytes
	for remaining > 0 {
		if ctx.Err() != nil {
			return errors.New("audit_canceled")
		}
		n, err := io.ReadFull(f, buf[:min(int64(len(buf)), remaining)])
		if err != nil {
			return errors.New("audit_read")
		}
		_, _ = h.Write(buf[:n])
		remaining -= int64(n)
	}
	after, err := os.Lstat(seg.path)
	if err != nil || !os.SameFile(before, after) || before.Size() != after.Size() || !before.ModTime().Equal(after.ModTime()) {
		return errors.New("segment_changed")
	}
	if hex.EncodeToString(h.Sum(nil)) != seg.SHA256 {
		return errors.New("segment_checksum")
	}
	return nil
}

func (w *eventWriter) reportLocked() EventEvidenceReport {
	r := EventEvidenceReport{Version: eventEvidenceVersion, Limits: w.limits, Written: w.written, Dropped: w.dropped, Bytes: w.bytes, Finalized: w.finalized, Error: w.firstError, FailedAt: w.failedAt}
	for _, seg := range w.segments {
		r.Segments = append(r.Segments, seg.EventSegmentReport)
	}
	return r
}

func (w *eventWriter) report() EventEvidenceReport {
	if w == nil {
		return EventEvidenceReport{}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.reportLocked()
}

func (w *eventWriter) counts() (uint64, uint64) {
	if w == nil {
		return 0, 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.written, w.dropped
}

func (w *eventWriter) close() error { return w.finalize(context.Background()) }

// This accepts only inert data. time.Time is the sole known bounded marshaler.
// Conservative escaping/number charges may reject a value below the exact JSON
// size limit; they cannot admit arbitrary allocations or user marshal code.
func eventEncodingFits(v reflect.Value, budget *int64, depth int) bool {
	charge := func(n int64) bool { *budget -= n; return *budget >= 0 }
	if depth > 32 || !charge(1) {
		return false
	}
	if !v.IsValid() {
		return charge(4)
	}
	if v.Kind() == reflect.Interface {
		if v.IsNil() {
			return charge(4)
		}
		return eventEncodingFits(v.Elem(), budget, depth+1)
	}
	if v.Type() == reflect.TypeFor[time.Time]() {
		return charge(40)
	}
	if v.Type() == reflect.TypeFor[*time.Time]() {
		// Optional queue timestamps retain the same bounded standard encoder;
		// named or arbitrary pointer marshalers are still rejected below.
		if v.IsNil() {
			return charge(4)
		}
		return charge(40)
	}
	if v.CanInterface() {
		if _, ok := v.Interface().(json.Marshaler); ok {
			return false
		}
		if _, ok := v.Interface().(encoding.TextMarshaler); ok {
			return false
		}
	}
	// Pointer-receiver marshalers must not bypass admission for addressable fields.
	if v.CanAddr() && v.Addr().CanInterface() {
		if _, ok := v.Addr().Interface().(json.Marshaler); ok {
			return false
		}
		if _, ok := v.Addr().Interface().(encoding.TextMarshaler); ok {
			return false
		}
	}
	switch v.Kind() {
	case reflect.Bool:
		return charge(5)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		return charge(32)
	case reflect.String:
		return int64(v.Len()) <= *budget/6 && charge(2+6*int64(v.Len()))
	case reflect.Pointer:
		return v.IsNil() && charge(4) || !v.IsNil() && eventEncodingFits(v.Elem(), budget, depth+1)
	case reflect.Map:
		if v.IsNil() {
			return charge(4)
		}
		if v.Type().Key().Kind() != reflect.String || !charge(2) {
			return false
		}
		iter := v.MapRange()
		for iter.Next() {
			if !eventEncodingFits(iter.Key(), budget, depth+1) || !charge(2) || !eventEncodingFits(iter.Value(), budget, depth+1) {
				return false
			}
		}
		return true
	case reflect.Slice, reflect.Array:
		if !charge(4) {
			return false
		}
		for i := 0; i < v.Len(); i++ {
			if !eventEncodingFits(v.Index(i), budget, depth+1) {
				return false
			}
		}
		return true
	case reflect.Struct:
		if !charge(2) {
			return false
		}
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			if !field.IsExported() {
				continue
			}
			name := strings.SplitN(field.Tag.Get("json"), ",", 2)[0]
			if name == "-" {
				continue
			}
			if name == "" {
				name = field.Name
			}
			if !charge(4+6*int64(len(name))) || !eventEncodingFits(v.Field(i), budget, depth+1) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
