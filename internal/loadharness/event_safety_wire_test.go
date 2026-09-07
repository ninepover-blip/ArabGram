package loadharness

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
)

type safetyWireInvoker func(context.Context, bin.Encoder, bin.Decoder) error

func (f safetyWireInvoker) Invoke(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
	return f(ctx, in, out)
}

// Opt-in only: guardian verifies dedicated role binaries/configs/container
// ownership. This test never provisions accounts or connects outside loopback.
func TestRealEventSafety(t *testing.T) {
	fixture, output := os.Getenv("TELESRV_LOAD_EVENT_FIXTURE"), os.Getenv("TELESRV_LOAD_EVENT_OUTPUT")
	if fixture == "" || output == "" {
		t.Skip("requires explicit isolated wire fixture")
	}
	manifestPath := filepath.Join(fixture, "pool/manifest.json")
	manifest, err := LoadManifest(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(manifest.Endpoint.Address, "127.0.0.1:") || len(manifest.Sessions) != 4 || len(primaryTargets(manifest.Sessions)) != 2 {
		t.Fatal("two-account/four-device isolated loopback fixture required")
	}
	if err := os.Mkdir(output, 0o700); err != nil {
		t.Fatal("refusing to overwrite wire evidence", err)
	}
	var targets MetricsTargets
	for _, role := range []string{"edge", "core", "egress", "file", "sfu"} {
		data, err := os.ReadFile(filepath.Join(fixture, "configs", role+".yaml"))
		if err != nil {
			t.Fatal(err)
		}
		var c struct {
			Debug struct {
				Addr string `json:"addr"`
			} `json:"debug"`
		}
		if err := json.Unmarshal(data, &c); err != nil || !strings.HasPrefix(c.Debug.Addr, "127.0.0.1:") {
			t.Fatal("loopback metrics required")
		}
		if err := targets.Set(role + "/one=http://" + c.Debug.Addr + "/metrics"); err != nil {
			t.Fatal(err)
		}
	}
	for _, mode := range []string{"rotate", "quota", "write_inflight"} {
		if !t.Run(mode, func(t *testing.T) {
			limits := EventLimits{LineBytes: 2 << 20, SegmentBytes: 2 << 20, TotalBytes: 64 << 20}
			if mode == "quota" {
				limits.TotalBytes = 2 << 20
			}
			cfg := RunConfig{ManifestPath: manifestPath, SessionKeyPath: filepath.Join(fixture, "pool/session.key"),
				ReportPath: filepath.Join(output, mode+"-report.json"), EventsPath: filepath.Join(output, mode+"-events.ndjson"), EventLimits: limits,
				ServerMetricsTargets: targets, SessionLimit: 4, Duration: 45 * time.Second, RampDuration: 2 * time.Second, RPCInterval: 5 * time.Second,
				MessageInterval: -time.Second, MessageRate: 1, MessageQueueDepth: 4, DeliverySettle: 2 * time.Second, DeliveryCacheRecords: 2,
				OperationTimeout: 5 * time.Second, SampleInterval: time.Second, MinimumReadyRatio: 1}
			var events *eventWriter
			cfg.observeEvents = func(w *eventWriter) { events = w }
			var injected atomic.Bool
			var mu sync.Mutex
			var calls []map[string]any
			var committedBeforeCancel map[string]any
			cfg.wrapInvoker = func(r SessionRecord, base tg.Invoker) tg.Invoker {
				return safetyWireInvoker(func(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
					start := time.Now().UTC()
					if ctx.Err() != nil {
						return ctx.Err()
					}
					err := base.Invoke(ctx, in, out)
					if request, ok := in.(*tg.MessagesSendMessageRequest); ok {
						mu.Lock()
						calls = append(calls, map[string]any{"started_at": start, "finished_at": time.Now().UTC(), "sender_session": r.Index, "marker": request.Message, "random_id": request.RandomID, "rpc_error": classifyError(err)})
						mu.Unlock()
						if mode == "write_inflight" && err == nil && injected.CompareAndSwap(false, true) {
							mu.Lock()
							committedBeforeCancel = map[string]any{"sender_session": r.Index, "marker": request.Message, "random_id": request.RandomID, "response_type": "real successful send result discarded after event write fault"}
							mu.Unlock()
							events.mu.Lock()
							_ = events.f.Close()
							events.mu.Unlock()
							events.write(map[string]any{"type": "injected_flush_failure", "at": time.Now().UTC()})
							return ctx.Err() // Server success is now an uncertain client outcome.
						}
					}
					return err
				})
			}
			ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
			defer cancel()
			r, err := Run(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			proof := map[string]any{"mode": mode, "run_id": r.Delivery.RunID, "calls": calls, "committed_before_cancel": committedBeforeCancel,
				"scope":       "real MTProto; quota is real; write_inflight closes the local event file after a server success and suppresses that response",
				"safety_stop": r.SafetyStop, "events": r.EventEvidence}
			data, _ := json.MarshalIndent(proof, "", "  ")
			if err := writeFileAtomic(filepath.Join(output, mode+"-probe.json"), append(data, '\n'), 0o600); err != nil {
				t.Fatal(err)
			}
			if !r.Delivery.Ledger.AuditComplete || r.EventsWritten == 0 {
				t.Fatal("missing final delivery/evidence prefix")
			}
			if mode == "rotate" {
				if !r.Pass || !r.EventEvidence.Finalized || len(r.EventEvidence.Segments) < 2 || r.Delivery.DifferenceRecovered != 0 {
					t.Fatalf("normal rotation failed: %v / %+v", r.Failures, r.EventEvidence)
				}
			} else {
				want := "total_bytes"
				if mode == "write_inflight" {
					want = "write"
				}
				if r.Pass || r.SafetyStop.Source != "events" || r.SafetyStop.Class != want || r.EventEvidence.Finalized {
					t.Fatalf("safety stop mismatch: %+v / %v", r.SafetyStop, r.Failures)
				}
				if r.SafetyStop.WorkersStoppedAt.Sub(r.SafetyStop.At) > 3*time.Second || r.LoadEndedAt.Sub(r.StartedAt) > 20*time.Second {
					t.Fatal("stop did not promptly cancel work")
				}
				for _, call := range calls {
					if call["started_at"].(time.Time).After(r.SafetyStop.At) {
						t.Fatal("new send began after stop")
					}
				}
				if r.Operations["updates.getDifference.delivery"].Count != 0 {
					t.Fatal("safety stop started reconciliation load")
				}
				if mode == "write_inflight" && (!injected.Load() || r.Delivery.InitialUncertain != 1 || r.Delivery.RetryAttempts != 0 || r.Operations["messages.sendMessage"].Canceled != 1) {
					t.Fatal("in-flight cancellation was lost or retried")
				}
			}
			for _, target := range r.ServerMetricsTargets {
				if target.Errors != 0 || !target.ContinuityComplete {
					t.Fatal("unexpected metrics evidence gap")
				}
			}
		}) {
			return
		}
	}
}

func TestSafetyCanceledQueueCannotCreateSendIntent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := &loadWorker{counters: &harnessCounters{}}
	w.sendMessage(ctx, nil, messageArrival{})
	if w.messageSeq.Load() != 0 || w.counters.messageCompleted.Load() != 1 {
		t.Fatal("canceled queue created a new intention")
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("test context not canceled")
	}
}
