package loadharness

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/telegram"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tgerr"
)

// Only a guardian-owned, loopback fixture is supported. Faults affect the load
// client's observation/request, never service configuration or unrelated data.
func TestRealSafetyGuards(t *testing.T) {
	fixture, output := os.Getenv("TELESRV_LOAD_GUARD_FIXTURE"), os.Getenv("TELESRV_LOAD_GUARD_OUTPUT")
	if fixture == "" || output == "" {
		t.Skip("requires explicit isolated wire fixture")
	}
	manifestPath := filepath.Join(fixture, "pool/manifest.json")
	manifest, err := LoadManifest(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(manifest.Endpoint.Address, "127.0.0.1:") || len(manifest.Sessions) != 4 || len(primaryTargets(manifest.Sessions)) != 2 {
		t.Fatal("two-account/four-device loopback fixture required")
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
	for _, mode := range []string{"baseline_metrics", "normal", "metrics", "business", "invariant", "resource_preflight", "intent_preflight"} {
		if !t.Run(mode, func(t *testing.T) {
			cfg := RunConfig{ManifestPath: manifestPath, SessionKeyPath: filepath.Join(fixture, "pool/session.key"),
				ReportPath: filepath.Join(output, mode+"-report.json"), EventsPath: filepath.Join(output, mode+"-events.ndjson"),
				ServerMetricsTargets: append(MetricsTargets(nil), targets...), SessionLimit: 4, Duration: 45 * time.Second, RampDuration: 2 * time.Second, RPCInterval: 5 * time.Second,
				MessageInterval: -time.Second, MessageRate: 1, MessageQueueDepth: 4, DeliverySettle: 2 * time.Second, DeliveryCacheRecords: 2,
				OperationTimeout: 5 * time.Second, SampleInterval: time.Second, MinimumReadyRatio: 1}
			var mu sync.Mutex
			var calls []map[string]any
			active := make(map[string]bool)
			var faultAt time.Time
			var injected atomic.Bool
			if mode == "metrics" || mode == "baseline_metrics" {
				upstream := cfg.ServerMetricsTargets[1].URL
				client := &http.Client{Timeout: 2 * time.Second}
				var scrapes atomic.Uint64
				proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if scrapes.Add(1) > 7 {
						if injected.CompareAndSwap(false, true) {
							mu.Lock()
							faultAt = time.Now().UTC()
							mu.Unlock()
						}
						http.Error(w, "isolated metrics observation fault", 503)
						return
					}
					req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, upstream, nil)
					if err != nil {
						http.Error(w, "request failed", 500)
						return
					}
					resp, err := client.Do(req)
					if err != nil {
						http.Error(w, "upstream failed", 502)
						return
					}
					defer resp.Body.Close()
					w.WriteHeader(resp.StatusCode)
					_, _ = io.Copy(w, io.LimitReader(resp.Body, 8<<20))
				}))
				defer proxy.Close()
				cfg.ServerMetricsTargets[1].URL = proxy.URL
			}
			cfg.wrapInvoker = func(record SessionRecord, base tg.Invoker) tg.Invoker {
				return safetyWireInvoker(func(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					start := time.Now().UTC()
					request, send := in.(*tg.MessagesSendMessageRequest)
					var wireRandomID int64
					if send {
						mu.Lock()
						active[request.Message] = true
						mu.Unlock()
						wireRandomID = request.RandomID
						if mode == "business" {
							// The real RPC rejects this before mutation. Keep the original
							// immutable intent in the ledger; do not fake a local RPC error.
							copyRequest := *request
							copyRequest.RandomID = 0
							in = &copyRequest
							wireRandomID = 0
							if injected.CompareAndSwap(false, true) {
								mu.Lock()
								faultAt = start
								mu.Unlock()
							}
						}
					}
					err := base.Invoke(ctx, in, out)
					if send {
						rejection := ""
						if rpcErr, ok := tgerr.As(err); ok && rpcErr.Code == 400 && tgerr.Is(err, "RANDOM_ID_EMPTY") {
							rejection = "RANDOM_ID_EMPTY"
						}
						mu.Lock()
						calls = append(calls, map[string]any{"started_at": start, "finished_at": time.Now().UTC(), "sender_session": record.Index, "marker": request.Message, "random_id": request.RandomID, "wire_random_id": wireRandomID, "rpc_error": classifyError(err), "rpc_rejection": rejection})
						mu.Unlock()
					}
					return err
				})
			}
			if mode == "invariant" {
				cfg.wrapUpdates = func(record SessionRecord, base telegram.UpdateHandler) telegram.UpdateHandler {
					return telegram.UpdateHandlerFunc(func(ctx context.Context, updates tg.UpdatesClass) error {
						err := base.Handle(ctx, updates)
						if value, ok := updates.(*tg.Updates); ok {
							for _, u := range value.Updates {
								if update, ok := u.(*tg.UpdateNewMessage); ok {
									if m, ok := update.Message.(*tg.Message); ok {
										mu.Lock()
										matches := active[m.Message]
										mu.Unlock()
										if matches && injected.CompareAndSwap(false, true) {
											mu.Lock()
											faultAt = time.Now().UTC()
											mu.Unlock()
											badMessage := *m
											badMessage.PeerID = &tg.PeerUser{UserID: -1}
											badUpdate := *update
											badUpdate.Message = &badMessage
											_ = base.Handle(ctx, &tg.Updates{Updates: []tg.UpdateClass{&badUpdate}})
										}
									}
								}
							}
						}
						return err
					})
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
			defer cancel()
			var report *RunReport
			if mode == "baseline_metrics" {
				baseline := os.Getenv("TELESRV_LOAD_GUARD_BASELINE")
				if baseline == "" {
					t.Fatal("explicit archived v12 binary required for A/B")
				}
				args := []string{"run", "-manifest", cfg.ManifestPath, "-session-key", cfg.SessionKeyPath, "-report", cfg.ReportPath, "-events", cfg.EventsPath,
					"-sessions", "4", "-duration", "45s", "-ramp", "2s", "-rpc-interval", "5s", "-recovery", "0", "-offline-fraction", "0", "-file-size", "0",
					"-message-interval=-1s", "-message-rate", "1", "-message-queue", "4", "-delivery-settle", "2s", "-delivery-cache-records", "2", "-operation-timeout", "5s", "-sample-interval", "1s", "-min-ready-ratio", "1"}
				for _, target := range cfg.ServerMetricsTargets {
					args = append(args, "-server-metrics", target.Role+"/"+target.Instance+"="+target.URL)
				}
				cmd := exec.CommandContext(ctx, baseline, args...)
				log, err := os.OpenFile(filepath.Join(output, "baseline-cli.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
				if err != nil {
					t.Fatal(err)
				}
				cmd.Stdout, cmd.Stderr = log, log
				runErr := cmd.Run()
				_ = log.Close()
				data, err := os.ReadFile(cfg.ReportPath)
				if err != nil {
					t.Fatalf("baseline report: %v; CLI: %v", err, runErr)
				}
				if err := json.Unmarshal(data, &report); err != nil {
					t.Fatal(err)
				}
			} else {
				if mode == "resource_preflight" {
					cfg.ResourceLimits.HeapBytes = 1 << 20
				}
				if mode == "intent_preflight" {
					cfg.MaxMessageIntents = 2
				}
				report, err = Run(ctx, cfg)
				if strings.HasSuffix(mode, "_preflight") {
					if err == nil || report != nil || len(calls) != 0 {
						t.Fatal("preflight did not reject before clients", err)
					}
					guardWireProof(t, output, mode, map[string]any{"mode": mode, "error": err.Error(), "wire_send_calls": len(calls), "scope": "real process heap or planned intent budget; no OOM or disk filling"})
					return
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			guardWireProof(t, output, mode, map[string]any{"mode": mode, "run_id": report.Delivery.RunID, "fault_at": faultAt, "calls": calls, "safety_stop": report.SafetyStop,
				"scope": "five real roles and MTProto/PFS; metrics fault is a local HTTP proxy; business fault sends random_id=0 to the real server; invariant fault replays a locally altered envelope after a genuine live message"})
			if report.Pass != (mode == "normal") || !report.Delivery.Ledger.AuditComplete || !report.EventEvidence.Finalized {
				t.Fatalf("unexpected result: %+v", report.Failures)
			}
			if mode == "baseline_metrics" {
				if report.Version != 12 || !injected.Load() || report.SafetyStop.Source != "" || report.Delivery.AttemptedMessages < 40 || report.LoadEndedAt.Sub(report.StartedAt) < 44*time.Second {
					t.Fatal("v12 did not reproduce continuing after evidence loss")
				}
				return
			}
			if report.Version != RunReportVersion || report.Resources.Samples < 2 || report.Resources.Errors != 0 || report.Resources.Last.At.Before(report.LoadEndedAt) {
				t.Fatal("resource evidence did not cover workload and recovery")
			}
			if mode == "normal" {
				if report.Delivery.CommittedMessages < 40 || report.Delivery.LiveDelivered != 3*report.Delivery.CommittedMessages || report.Delivery.DifferenceRecovered != 0 {
					t.Fatal("normal real delivery failed")
				}
				return
			}
			if !injected.Load() || report.SafetyStop.Source == "" || report.SafetyStop.WorkersStoppedAt.Sub(report.SafetyStop.At) > 3*time.Second {
				t.Fatal("fault did not promptly stop workers")
			}
			for _, call := range calls {
				if call["started_at"].(time.Time).After(report.SafetyStop.At) {
					t.Fatal("new send after safety stop")
				}
			}
			if report.Operations["updates.getDifference.delivery"].Count != 0 || report.Delivery.RetryAttempts != 0 {
				t.Fatal("fault started reconciliation/retry work")
			}
			switch mode {
			case "metrics":
				if report.SafetyStop.Source != "metrics" || report.LoadEndedAt.Sub(report.StartedAt) > 15*time.Second {
					t.Fatalf("metrics guard: %+v", report.SafetyStop)
				}
			case "business":
				if report.SafetyStop.Source != "business" || !report.BusinessGuard.Tripped || report.SafetyStop.At.Sub(faultAt) < 30*time.Second || report.LoadEndedAt.Sub(report.StartedAt) > 40*time.Second || report.Delivery.InitialRejected != uint64(len(calls)) || report.Delivery.CommittedMessages != 0 {
					t.Fatalf("business guard: %+v / %+v", report.SafetyStop, report.BusinessGuard)
				}
				for _, call := range calls {
					if call["rpc_rejection"] != "RANDOM_ID_EMPTY" {
						t.Fatalf("not a real audited rejection: %v", call["rpc_error"])
					}
				}
			case "invariant":
				if report.SafetyStop.Source != "delivery_invariant" || report.Delivery.CommittedMessages != 1 || report.Delivery.LiveDelivered < 1 || report.LoadEndedAt.Sub(report.StartedAt) > 10*time.Second {
					t.Fatalf("invariant did not preserve real fact: %+v", report.Delivery)
				}
			}
		}) {
			return
		}
	}
}

func guardWireProof(t *testing.T, output, mode string, v any) {
	t.Helper()
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFileAtomic(filepath.Join(output, mode+"-probe.json"), append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}
