//go:build darwin || linux

package loadharness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gotd/log"
	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/telegram"
	"github.com/iamxvbaba/td/tg"
)

// The gate forwards opaque bytes. Closing its actual listener and sockets
// makes reconnects fail in the kernel without replacing MTProto responses.
type physicalGate struct {
	mu       sync.Mutex
	listener net.Listener
	address  string
	target   string
	conns    map[net.Conn]bool
	wg       sync.WaitGroup
	events   *eventWriter
}

func (g *physicalGate) open() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.listener != nil {
		return fmt.Errorf("gate already open")
	}
	address := g.address
	if address == "" {
		address = "127.0.0.1:0"
	}
	ln, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	g.listener, g.address = ln, ln.Addr().String()
	g.events.write(map[string]any{"type": "gate_open", "at": time.Now().UTC(), "address": g.address})
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		for {
			incoming, err := ln.Accept()
			if err != nil {
				return
			}
			g.mu.Lock()
			if g.listener != ln {
				g.mu.Unlock()
				_ = incoming.Close()
				return
			}
			g.conns[incoming] = true
			g.wg.Add(1)
			g.mu.Unlock()
			go g.forward(incoming, ln)
		}
	}()
	return nil
}

func (g *physicalGate) forward(incoming net.Conn, owner net.Listener) {
	defer g.wg.Done()
	defer incoming.Close()
	defer func() { g.mu.Lock(); delete(g.conns, incoming); g.mu.Unlock() }()
	outgoing, err := net.DialTimeout("tcp", g.target, time.Second)
	if err != nil {
		g.events.write(map[string]any{"type": "gate_upstream_error", "at": time.Now().UTC(), "class": classifyError(err)})
		return
	}
	defer outgoing.Close()
	g.mu.Lock()
	if g.listener != owner {
		g.mu.Unlock()
		return
	}
	g.conns[outgoing] = true
	g.mu.Unlock()
	defer func() { g.mu.Lock(); delete(g.conns, outgoing); g.mu.Unlock() }()
	g.events.write(map[string]any{"type": "gate_forward", "at": time.Now().UTC(), "client": incoming.RemoteAddr().String(), "upstream_local": outgoing.LocalAddr().String(), "upstream_remote": outgoing.RemoteAddr().String()})
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(outgoing, incoming)
		_ = outgoing.Close()
		_ = incoming.Close()
		close(done)
	}()
	_, _ = io.Copy(incoming, outgoing)
	_ = incoming.Close()
	_ = outgoing.Close()
	<-done
}

func (g *physicalGate) close() {
	g.mu.Lock()
	active := g.listener != nil || len(g.conns) != 0
	if g.listener != nil {
		_ = g.listener.Close()
		g.listener = nil
	}
	for conn := range g.conns {
		_ = conn.Close()
	}
	g.mu.Unlock()
	g.wg.Wait()
	if active {
		g.events.write(map[string]any{"type": "gate_closed", "at": time.Now().UTC()})
	}
}

type physicalProbe struct {
	events *eventWriter
	mu     sync.Mutex
	next   map[int]int
	ready  map[int]int
}

type physicalProbeLog struct {
	probe *physicalProbe
	index int
}

func (physicalProbeLog) Enabled(context.Context, log.Level) bool { return true }
func (l physicalProbeLog) Log(_ context.Context, _ log.Level, message string, attrs ...log.Attr) {
	if message != "Binding temporary auth key" && message != "Temporary auth key bound" {
		return
	}
	row := map[string]any{"type": "pfs", "at": time.Now().UTC(), "session_index": l.index, "message": message}
	for _, attr := range attrs {
		if attr.Value.Kind() != log.KindInt64 {
			continue
		}
		switch attr.Key {
		case "temp_key_id", "perm_key_id", "temp_session_id", "expires_at", "conn_id", "dc_id":
			row[attr.Key] = attr.Value.Int64()
		}
	}
	l.probe.events.write(row)
}

type physicalObservedConn struct {
	net.Conn
	events *eventWriter
	index  int
	number int
	once   sync.Once
	err    error
}

func (c *physicalObservedConn) Close() error {
	c.once.Do(func() {
		c.err = c.Conn.Close()
		c.events.write(map[string]any{"type": "socket_closed", "at": time.Now().UTC(), "session_index": c.index, "socket_number": c.number, "class": classifyError(c.err)})
	})
	return c.err
}

func TestRealPhysicalReconnect(t *testing.T) {
	fixture, out, spec, targets := lifecycleFixture(t)
	manifest, err := LoadManifest(filepath.Join(fixture, "pool/manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(out, 0700); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"normal", "tcp_gap", "startup_race"} {
		if !t.Run(mode, func(t *testing.T) {
			parent, cancel := context.WithTimeout(context.Background(), 80*time.Second)
			defer cancel()
			probeEvents, err := newEventWriter(filepath.Join(out, mode+"-physical.ndjson"))
			if err != nil {
				t.Fatal(err)
			}
			defer probeEvents.close()
			probeEvents.onFailure = func(string, time.Time) { cancel() }
			p := &physicalProbe{events: probeEvents, next: map[int]int{}, ready: map[int]int{}}
			gate := &physicalGate{target: manifest.Endpoint.Address, conns: map[net.Conn]bool{}, events: probeEvents}
			if err := gate.open(); err != nil {
				t.Fatal(err)
			}
			defer gate.close()
			var victim *loadWorker
			var workers []*loadWorker
			var monitor sync.WaitGroup
			var injected atomic.Bool
			var held atomic.Bool
			holdReady := make(chan struct{})
			release := make(chan struct{})
			cfg := RunConfig{ManifestPath: filepath.Join(fixture, "pool/manifest.json"), SessionKeyPath: filepath.Join(fixture, "pool/session.key"),
				ReportPath: filepath.Join(out, mode+"-report.json"), EventsPath: filepath.Join(out, mode+"-events.ndjson"), EnvironmentPath: spec,
				ServerMetricsTargets: targets, SessionLimit: 4, Duration: 40 * time.Second, RampDuration: 2 * time.Second, SampleInterval: time.Second,
				RPCInterval: 5 * time.Second, MessageInterval: -time.Second, MessageRate: 1, MessageQueueDepth: 4, OperationTimeout: 8 * time.Second,
				DeliverySettle: 2 * time.Second, DeliveryCacheRecords: 1, MinimumReadyRatio: 1}
			cfg.configureClient = func(record SessionRecord, hooks *clientHooks) {
				hooks.Logger = physicalProbeLog{probe: p, index: record.Index}
				base := hooks.ConnectionState
				hooks.ConnectionState = func(state telegram.ConnectionState) {
					base(state)
					p.mu.Lock()
					if state == telegram.ConnectionStateReady {
						p.ready[record.Index]++
					}
					ready := p.ready[record.Index]
					p.mu.Unlock()
					probeEvents.write(map[string]any{"type": "primary_state", "at": time.Now().UTC(), "session_index": record.Index, "state": state.String(), "ready_number": ready})
				}
				hooks.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
					if address != manifest.Endpoint.Address {
						return nil, fmt.Errorf("probe refuses undeclared endpoint")
					}
					if record.DeviceIndex == 1 && record.AccountIndex == 0 {
						gate.mu.Lock()
						address = gate.address
						gate.mu.Unlock()
					}
					var dialer net.Dialer
					conn, err := dialer.DialContext(ctx, network, address)
					if err != nil {
						probeEvents.write(map[string]any{"type": "socket_dial_error", "at": time.Now().UTC(), "session_index": record.Index, "class": classifyError(err), "connection_refused": errors.Is(err, syscall.ECONNREFUSED)})
						return nil, err
					}
					p.mu.Lock()
					p.next[record.Index]++
					number := p.next[record.Index]
					p.mu.Unlock()
					probeEvents.write(map[string]any{"type": "socket_open", "at": time.Now().UTC(), "session_index": record.Index, "socket_number": number, "local": conn.LocalAddr().String(), "remote": conn.RemoteAddr().String()})
					return &physicalObservedConn{Conn: conn, events: probeEvents, index: record.Index, number: number}, nil
				}
			}
			cfg.wrapInvoker = func(record SessionRecord, next tg.Invoker) tg.Invoker {
				return arrivalInvoker(func(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
					start := time.Now().UTC()
					err := next.Invoke(ctx, in, out)
					row := map[string]any{"type": "rpc", "at": time.Now().UTC(), "started_at": start, "session_index": record.Index, "method": fmt.Sprintf("%T", in), "wire_error": classifyError(err)}
					if req, ok := in.(*tg.MessagesSendMessageRequest); ok {
						row["marker"], row["random_id"] = req.Message, req.RandomID
					}
					if mode == "startup_race" && record.AccountIndex == 0 && record.DeviceIndex == 1 && err == nil {
						if _, ok := in.(*tg.UpdatesGetStateRequest); ok && held.CompareAndSwap(false, true) {
							probeEvents.write(map[string]any{"type": "old_state_held", "at": time.Now().UTC(), "session_index": record.Index})
							close(holdReady)
							select {
							case <-release:
								row["released_after_reconnect"] = true
							case <-ctx.Done():
								err = ctx.Err()
							}
						}
					}
					row["returned_at"] = time.Now().UTC()
					probeEvents.write(row)
					return err
				})
			}
			cfg.wrapUpdates = func(record SessionRecord, next telegram.UpdateHandler) telegram.UpdateHandler {
				var worker *loadWorker
				for _, w := range workers {
					if w.record.Index == record.Index {
						worker = w
					}
				}
				generation := worker.clientGeneration.Load()
				return telegram.UpdateHandlerFunc(func(ctx context.Context, updates tg.UpdatesClass) error {
					at := time.Now()
					messages := deviceProbeMessages(updates)
					err := next.Handle(ctx, updates)
					handled := time.Now()
					for _, m := range messages {
						if strings.HasPrefix(m.marker, "telesrv-load-v3/"+worker.delivery.runID+"/") {
							probeEvents.write(map[string]any{"type": "live", "at": at.UTC(), "handled_at": handled.UTC(), "entered_relative_ns": at.Sub(worker.delivery.ledger.epoch).Nanoseconds(), "handled_relative_ns": handled.Sub(worker.delivery.ledger.epoch).Nanoseconds(), "session_index": record.Index, "client_generation": generation, "marker": m.marker, "message_id": m.id, "handler_error": classifyError(err)})
						}
					}
					return err
				})
			}
			cfg.observeWorkers = func(value []*loadWorker) {
				workers = value
				for _, w := range workers {
					if w.record.AccountIndex == 0 && w.record.DeviceIndex == 1 {
						victim = w
					}
				}
				if mode == "normal" {
					return
				}
				monitor.Add(1)
				go func() {
					defer monitor.Done()
					defer close(release)
					wait := func(predicate func() bool) bool {
						ticker := time.NewTicker(time.Millisecond)
						defer ticker.Stop()
						for !predicate() {
							select {
							case <-parent.Done():
								return false
							case <-ticker.C:
							}
						}
						return true
					}
					if mode == "startup_race" {
						select {
						case <-holdReady:
						case <-parent.Done():
							return
						}
					} else {
						if !wait(func() bool { return workers[0].delivery.report().CommittedMessages >= 5 }) {
							return
						}
					}
					probeEvents.write(map[string]any{"type": "fault_begin", "at": time.Now().UTC(), "session_index": victim.record.Index, "client_generation": victim.clientGeneration.Load()})
					gate.close()
					timer := time.NewTimer(3 * time.Second)
					select {
					case <-parent.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
					if err := gate.open(); err != nil {
						probeEvents.write(map[string]any{"type": "fault_restore_error", "at": time.Now().UTC()})
						cancel()
						return
					}
					if !wait(func() bool { p.mu.Lock(); defer p.mu.Unlock(); return p.ready[victim.record.Index] >= 2 }) {
						return
					}
					injected.Store(true)
					probeEvents.write(map[string]any{"type": "fault_transport_restored", "at": time.Now().UTC(), "session_index": victim.record.Index, "client_generation": victim.clientGeneration.Load()})
				}()
			}
			report, err := Run(parent, cfg)
			cancel()
			monitor.Wait()
			gate.close()
			if err := probeEvents.finalize(context.Background()); err != nil {
				t.Fatal(err)
			}
			if err != nil {
				t.Fatal(err)
			}
			guardWireProof(t, out, mode, map[string]any{"mode": mode, "injected": injected.Load(), "victim_index": victim.record.Index, "physical_evidence": probeEvents.report(), "scope": "real opaque TCP gate and kernel refusal; published PFS/session ID logs; startup_race additionally delays one already-received getState result; no callback physical-origin assertion"})
			if !report.Delivery.Ledger.AuditComplete || !report.EventEvidence.Finalized || report.Delivery.Missing != 0 || report.Delivery.FinalReconciledDevices != 4 {
				t.Fatalf("recovery incomplete: delivery=%+v failures=%v", report.Delivery, report.Failures)
			}
			// PFS warm-up is slower with -race. Count every fixed arrival from
			// the actual readiness boundary to the declared load deadline.
			window := report.StartedAt.Add(cfg.Duration).Sub(report.MessageReadyAt)
			expected := uint64((window + time.Second - 1) / time.Second)
			if report.MessageReadyAt.IsZero() || window <= 0 || expected < 30 || report.MessageScheduled != expected || report.MessageEnqueued != expected || report.MessageCompleted != expected || report.Delivery.CommittedMessages != expected {
				t.Fatalf("fixed arrivals incomplete: expected=%d scheduled=%d enqueued=%d completed=%d committed=%d", expected, report.MessageScheduled, report.MessageEnqueued, report.MessageCompleted, report.Delivery.CommittedMessages)
			}
			if mode == "normal" {
				if !report.Pass || report.Delivery.OnlineLiveDelivered != expected*3 {
					t.Fatal(report.Failures, report.Delivery)
				}
				return
			}
			if !injected.Load() || victim.clientGeneration.Load() != 1 || p.next[victim.record.Index] != 2 || p.ready[victim.record.Index] != 2 {
				t.Fatal("did not prove internal reconnect with two real sockets", injected.Load(), victim.clientGeneration.Load(), p.next, p.ready)
			}
			if mode == "tcp_gap" && (report.Pass || report.Delivery.UnavailableExpected == 0 || report.Delivery.OfflineExpected != 0) {
				t.Fatal("unplanned outage was excused", report.Delivery, report.Failures)
			}
			if mode == "startup_race" {
				assertNoPrematureReady(t, cfg.EventsPath, filepath.Join(out, mode+"-physical.ndjson"), victim.record.Index)
			}
		}) {
			return
		}
	}
}

func assertNoPrematureReady(t *testing.T, eventsPath, physicalPath string, victim int) {
	t.Helper()
	read := func(path string) []map[string]any {
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		var rows []map[string]any
		decoder := json.NewDecoder(f)
		for {
			var row map[string]any
			if err := decoder.Decode(&row); err == io.EOF {
				break
			} else if err != nil {
				t.Fatal(err)
			}
			rows = append(rows, row)
		}
		return rows
	}
	var restored, audited time.Time
	for _, row := range read(physicalPath) {
		if row["type"] == "fault_transport_restored" {
			restored, _ = time.Parse(time.RFC3339Nano, row["at"].(string))
		}
		if row["type"] == "rpc" && row["session_index"] == float64(victim) && row["method"] == "*tg.UpdatesGetDifferenceRequest" && row["wire_error"] == "ok" && audited.IsZero() {
			audited, _ = time.Parse(time.RFC3339Nano, row["returned_at"].(string))
		}
	}
	if restored.IsZero() || audited.IsZero() {
		t.Fatal("missing restore/audit boundaries")
	}
	for _, row := range read(eventsPath) {
		if row["type"] == "session_business_ready" && row["session_index"] == float64(victim) {
			at, _ := time.Parse(time.RFC3339Nano, row["at"].(string))
			if at.After(restored) && at.Before(audited) {
				t.Fatal("old physical connection response published business-ready before replacement audit")
			}
		}
	}
}
