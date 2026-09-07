//go:build darwin || linux

package loadharness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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

func lifecycleFixture(t *testing.T) (string, string, string, MetricsTargets) {
	t.Helper()
	fixture, out, spec := os.Getenv("TELESRV_LOAD_LIFECYCLE_FIXTURE"), os.Getenv("TELESRV_LOAD_LIFECYCLE_OUTPUT"), os.Getenv("TELESRV_LOAD_LIFECYCLE_SPEC")
	if fixture == "" || out == "" || spec == "" {
		t.Skip("explicit owned loopback fixture required")
	}
	manifest, e := LoadManifest(filepath.Join(fixture, "pool/manifest.json"))
	if e != nil {
		t.Fatal(e)
	}
	if !strings.HasPrefix(manifest.Endpoint.Address, "127.0.0.1:") || len(manifest.Sessions) != 4 || len(primaryTargets(manifest.Sessions)) != 2 {
		t.Fatal("two-account/four-device fixture required")
	}
	var targets MetricsTargets
	for _, role := range []string{"edge", "core", "egress", "file", "sfu"} {
		b, e := os.ReadFile(filepath.Join(fixture, "configs", role+".yaml"))
		if e != nil {
			t.Fatal(e)
		}
		var c struct {
			Debug struct {
				Addr string `json:"addr"`
			} `json:"debug"`
		}
		if json.Unmarshal(b, &c) != nil || !strings.HasPrefix(c.Debug.Addr, "127.0.0.1:") {
			t.Fatal("loopback metrics required")
		}
		if e := targets.Set(role + "/one=http://" + c.Debug.Addr + "/metrics"); e != nil {
			t.Fatal(e)
		}
	}
	if _, _, e := loadEnvironmentSpec(spec, targets); e != nil {
		t.Fatal(e)
	}
	return fixture, out, spec, targets
}

// Prepare once, only through public RPCs. Never insert or alter database rows.
func TestRealLifecycleDataset(t *testing.T) {
	fixture, out, _, _ := lifecycleFixture(t)
	if e := os.Mkdir(out, 0700); e != nil {
		t.Fatal("dataset already exists; do not overwrite", e)
	}
	dataset, e := PlanDataset(DatasetConfig{Accounts: 2, Seed: 2026083102, PrivateFanout: 1, HotGroups: 1, HotMembers: 2, HotHistory: 2, SmallGroups: 1, SmallMembers: 2, SmallHistory: 2})
	if e != nil {
		t.Fatal(e)
	}
	plan := filepath.Join(out, "dataset.json")
	seed := filepath.Join(out, "seed.json")
	snapshot := filepath.Join(out, "client-state.json")
	mutation := filepath.Join(out, "mutation.json")
	if e := WriteDataset(plan, dataset); e != nil {
		t.Fatal(e)
	}
	manifest, key := filepath.Join(fixture, "pool/manifest.json"), filepath.Join(fixture, "pool/session.key")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	seeded, e := Seed(ctx, SeedConfig{ManifestPath: manifest, SessionKeyPath: key, DatasetPath: plan, SeedStatePath: seed, Concurrency: 1, OperationTimeout: 5 * time.Second}, nil)
	if e != nil {
		t.Fatal(e)
	}
	guardWireProof(t, out, "seed", seeded)
	snap, e := SnapshotClientState(ctx, SnapshotConfig{ManifestPath: manifest, SessionKeyPath: key, DatasetPath: plan, SeedStatePath: seed, ClientStatePath: snapshot, Concurrency: 1, OperationTimeout: 5 * time.Second}, nil)
	if e != nil {
		t.Fatal(e)
	}
	guardWireProof(t, out, "snapshot", snap)
	mutated, e := MutateOffline(ctx, MutateOfflineConfig{ManifestPath: manifest, SessionKeyPath: key, DatasetPath: plan, SeedStatePath: seed, ClientStatePath: snapshot, MutationStatePath: mutation, Concurrency: 1, OperationTimeout: 5 * time.Second}, nil)
	if e != nil {
		t.Fatal(e)
	}
	guardWireProof(t, out, "mutation", mutated)
	hashes := map[string]string{}
	for _, p := range []string{plan, seed, snapshot, mutation} {
		b, e := os.ReadFile(p)
		if e != nil {
			t.Fatal(e)
		}
		v := sha256.Sum256(b)
		hashes[filepath.Base(p)] = hex.EncodeToString(v[:])
	}
	guardWireProof(t, out, "inputs", map[string]any{"sha256": hashes, "scope": "real authenticated seed/snapshot/offline mutations; two groups, 220 offline channel messages; no database fixtures"})
}

func TestRealStartupLifecycle(t *testing.T) {
	t.Setenv("TELESRV_LOAD_DEBUG_ERRORS", "1")
	fixture, out, spec, targets := lifecycleFixture(t)
	dataset := os.Getenv("TELESRV_LOAD_LIFECYCLE_DATASET")
	if dataset == "" {
		t.Fatal("prepared immutable dataset required")
	}
	if e := os.Mkdir(out, 0700); e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"normal_tdesktop", "normal_tdlib", "cursor_backwards", "dialogs_payload", "metrics_mid_ramp"} {
		if !t.Run(mode, func(t *testing.T) {
			cfg := StartupRunConfig{ManifestPath: filepath.Join(fixture, "pool/manifest.json"), SessionKeyPath: filepath.Join(fixture, "pool/session.key"), DatasetPath: filepath.Join(dataset, "dataset.json"), SeedStatePath: filepath.Join(dataset, "seed.json"), ClientStatePath: filepath.Join(dataset, "client-state.json"), MutationStatePath: filepath.Join(dataset, "mutation.json"), ReportPath: filepath.Join(out, mode+"-report.json"), EventsPath: filepath.Join(out, mode+"-events.ndjson"), EnvironmentPath: spec, ServerMetricsTargets: append(MetricsTargets(nil), targets...), Profile: StartupProfileTDesktopReturningV1, StartOrder: StartupOrderAccountIndex, RampDuration: 2 * time.Second, OperationTimeout: 5 * time.Second, SampleInterval: time.Second}
			normal := strings.HasPrefix(mode, "normal_")
			if normal {
				waitColdPresenceWindow(t, targets, out, mode)
			}
			if mode == "normal_tdlib" {
				cfg.Profile = StartupProfileTDLibReturningV1
			}
			if !normal {
				cfg.RampDuration = 10 * time.Second
			}
			var armed atomic.Bool
			var mu sync.Mutex
			var calls []map[string]any
			var faultAt time.Time
			if mode == "metrics_mid_ramp" {
				upstream := cfg.ServerMetricsTargets[1].URL
				client := http.Client{Timeout: 2 * time.Second}
				proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if armed.Load() {
						http.Error(w, "owned metrics probe", 503)
						return
					}
					req, e := http.NewRequestWithContext(r.Context(), http.MethodGet, upstream, nil)
					if e != nil {
						http.Error(w, "request", 500)
						return
					}
					res, e := client.Do(req)
					if e != nil {
						http.Error(w, "upstream", 502)
						return
					}
					defer res.Body.Close()
					w.WriteHeader(res.StatusCode)
					io.Copy(w, io.LimitReader(res.Body, 8<<20))
				}))
				defer proxy.Close()
				cfg.ServerMetricsTargets[1].URL = proxy.URL
			}
			cfg.wrapInvoker = func(record SessionRecord, base tg.Invoker) tg.Invoker {
				return safetyWireInvoker(func(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
					start := time.Now().UTC()
					e := base.Invoke(ctx, in, out)
					injected := false
					if e == nil && record.AccountIndex == 0 {
						switch mode {
						case "cursor_backwards":
							if v, ok := out.(*tg.UpdatesState); ok && armed.CompareAndSwap(false, true) {
								v.Pts = 0
								injected = true
							}
						case "dialogs_payload":
							if v, ok := out.(*tg.MessagesDialogsBox); ok && armed.CompareAndSwap(false, true) {
								v.Dialogs = &tg.MessagesDialogsNotModified{}
								injected = true
							}
						case "metrics_mid_ramp":
							if _, ok := out.(*tg.UpdatesState); ok && armed.CompareAndSwap(false, true) {
								injected = true
							}
						}
					}
					mu.Lock()
					if injected {
						faultAt = time.Now().UTC()
					}
					calls = append(calls, map[string]any{"account": record.AccountIndex, "method": fmt.Sprintf("%T", in), "started_at": start, "finished_at": time.Now().UTC(), "rpc_error": classifyError(e), "altered_reply": injected && mode != "metrics_mid_ramp"})
					mu.Unlock()
					return e
				})
			}
			ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
			defer cancel()
			r, e := StartupRun(ctx, cfg)
			if e != nil {
				t.Fatal(e)
			}
			guardWireProof(t, out, mode, map[string]any{"mode": mode, "calls": calls, "fault_at": faultAt, "safety_stop": r.SafetyStop, "scope": "real returning profile; only fault cases alter a successful local reply or owned metrics observation; no server response modification"})
			if !r.EventEvidence.Finalized || r.Resources.Samples < 2 || r.Environment.Samples < 2 || r.Environment.Last.At.Before(r.SafetyStop.WorkersStoppedAt) {
				t.Fatal("missing final evidence")
			}
			if normal {
				if !r.Pass || r.BusinessReady != 2 || r.ChannelDifference.TooLong != 2 || r.ChannelDifference.Empty != 4 {
					t.Fatalf("returning profile failed: %v %+v", r.Failures, r.ChannelDifference)
				}
				if mode == "normal_tdlib" && r.AccountDifference.Full != 2 {
					t.Fatal("missing account difference")
				}
			} else {
				expected := "startup_invariant"
				if mode == "metrics_mid_ramp" {
					expected = "metrics"
				}
				if r.Pass || r.SafetyStop.Source != expected || !armed.Load() || r.BusinessReady >= 2 || r.SafetyStop.At.Sub(r.StartedAt) > 5*time.Second {
					t.Fatalf("did not stop ramp: %+v %v", r.SafetyStop, r.Failures)
				}
				for _, v := range calls {
					if v["account"].(int) != 0 || v["started_at"].(time.Time).After(r.SafetyStop.At) {
						t.Fatal("RPC admitted after stop or on canceled ramp account")
					}
				}
			}
		}) {
			return
		}
	}
}

func TestRealMediaLifecycle(t *testing.T) {
	fixture, out, spec, targets := lifecycleFixture(t)
	if e := os.Mkdir(out, 0700); e != nil {
		t.Fatal(e)
	}
	shared := filepath.Join(out, "valid-file.json")
	for _, mode := range []string{"normal", "payload_corrupt", "part_rejected", "part_timeout", "persist_collision", "fixture_invalid"} {
		if !t.Run(mode, func(t *testing.T) {
			cfg := RunConfig{ManifestPath: filepath.Join(fixture, "pool/manifest.json"), SessionKeyPath: filepath.Join(fixture, "pool/session.key"), ReportPath: filepath.Join(out, mode+"-report.json"), EventsPath: filepath.Join(out, mode+"-events.ndjson"), EnvironmentPath: spec, ServerMetricsTargets: targets, SessionLimit: 4, Duration: 35 * time.Second, RampDuration: time.Second, RPCInterval: 5 * time.Second, MessageInterval: -time.Second, FileInterval: 2 * time.Second, FileSizeBytes: 1 << 20, FileChunkBytes: 64 << 10, FileFixturePath: shared, SetupTimeout: 15 * time.Second, OperationTimeout: 2 * time.Second, SampleInterval: time.Second, MinimumReadyRatio: 1, DeliveryCacheRecords: 2}
			if mode != "normal" && mode != "payload_corrupt" {
				cfg.FileFixturePath = filepath.Join(out, mode+"-file.json")
			}
			if mode == "fixture_invalid" {
				if e := os.WriteFile(cfg.FileFixturePath, []byte("{}\n{}\n"), 0600); e != nil {
					t.Fatal(e)
				}
			}
			var mu sync.Mutex
			var calls []map[string]any
			var altered atomic.Bool
			var faultAt time.Time
			wrap := func(preparation bool) func(SessionRecord, tg.Invoker) tg.Invoker {
				return func(record SessionRecord, base tg.Invoker) tg.Invoker {
					return safetyWireInvoker(func(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
						start := time.Now().UTC()
						row := map[string]any{"session_index": record.Index, "method": fmt.Sprintf("%T", in), "started_at": start, "preparation": preparation}
						var e error
						if req, ok := in.(*tg.UploadSaveFilePartRequest); ok {
							row["file_id"] = req.FileID
							row["part"] = req.FilePart
							row["part_bytes"] = len(req.Bytes)
							if req.FilePart == 1 && (mode == "part_rejected" || mode == "part_timeout") && altered.CompareAndSwap(false, true) {
								mu.Lock()
								faultAt = time.Now().UTC()
								mu.Unlock()
								if mode == "part_rejected" {
									copyRequest := *req
									copyRequest.FilePart = -1
									row["wire_part"] = -1
									e = base.Invoke(ctx, &copyRequest, out)
								} else {
									row["not_dispatched"] = true
									<-ctx.Done()
									e = ctx.Err()
								}
							} else {
								e = base.Invoke(ctx, in, out)
							}
						} else {
							e = base.Invoke(ctx, in, out)
						}
						if e == nil {
							if req, ok := in.(*tg.UploadGetFileRequest); ok {
								row["offset"] = req.Offset
								row["limit"] = req.Limit
								if box, ok := out.(*tg.UploadFileBox); ok {
									if v, ok := box.File.(*tg.UploadFile); ok {
										h := sha256.Sum256(v.Bytes)
										row["wire_payload_sha256"] = hex.EncodeToString(h[:])
										row["wire_bytes"] = len(v.Bytes)
										if mode == "payload_corrupt" && altered.CompareAndSwap(false, true) {
											v.Bytes = append([]byte(nil), v.Bytes...)
											v.Bytes[0] ^= 1
											mu.Lock()
											faultAt = time.Now().UTC()
											mu.Unlock()
											row["altered"] = true
										}
									}
								}
							}
							if _, ok := in.(*tg.MessagesUploadMediaRequest); ok && mode == "persist_collision" && altered.CompareAndSwap(false, true) {
								if writeErr := os.WriteFile(cfg.FileFixturePath, []byte("reserved by explicit race probe\n"), 0600); writeErr != nil {
									return writeErr
								}
								mu.Lock()
								faultAt = time.Now().UTC()
								mu.Unlock()
							}
						}
						row["finished_at"] = time.Now().UTC()
						row["rpc_error"] = classifyError(e)
						mu.Lock()
						calls = append(calls, row)
						mu.Unlock()
						return e
					})
				}
			}
			cfg.wrapPreparationInvoker = wrap(true)
			cfg.wrapInvoker = wrap(false)
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
			defer cancel()
			r, e := Run(ctx, cfg)
			if e != nil {
				t.Fatal(e)
			}
			guardWireProof(t, out, mode, map[string]any{"mode": mode, "calls": calls, "fault_at": faultAt, "preparation": r.Preparation, "safety_stop": r.SafetyStop, "scope": "real upload/getFile; payload is changed only after valid wire response; second part rejected by real server or held until deadline; persistence collision preserves both file and reserved local evidence"})
			if r.Version != RunReportVersion || !r.EventEvidence.Finalized || !r.Delivery.Ledger.AuditComplete || r.Environment.Last.At.Before(r.SafetyStop.WorkersStoppedAt) {
				t.Fatal("missing media final evidence")
			}
			if mode == "normal" {
				if !r.Pass || !r.WorkloadStarted || r.Preparation.Disposition != "created" || r.DownloadedBytes == 0 {
					t.Fatalf("normal media failed: %v %+v", r.Failures, r.Preparation)
				}
				return
			}
			if r.Pass || r.SafetyStop.Source == "" {
				t.Fatal("media fault did not stop", r.Failures)
			}
			if mode == "payload_corrupt" {
				if r.SafetyStop.Source != "media_invariant" || r.SafetyStop.Class != "file_response_payload" || !r.WorkloadStarted {
					t.Fatal("payload stop mismatch", r.SafetyStop)
				}
			} else {
				if r.WorkloadStarted || r.ConnectionAttempts != 0 || r.Preparation.ErrorClass == "" || r.SafetyStop.Source != "preparation" {
					t.Fatal("preparation admitted workers", r.Preparation, r.SafetyStop)
				}
				for _, call := range calls {
					if !call["preparation"].(bool) {
						t.Fatal("ordinary workload after preparation failure")
					}
				}
			}
			if mode == "part_timeout" && r.Preparation.FinishedAt.Sub(r.Preparation.StartedAt) > 5*time.Second {
				t.Fatal("part waited for setup timeout")
			}
			if mode == "persist_collision" {
				b, _ := os.ReadFile(cfg.FileFixturePath)
				if string(b) != "reserved by explicit race probe\n" || r.Preparation.Disposition != "uploaded" || r.Preparation.ErrorClass != "filesystem" {
					t.Fatal("collision overwrote evidence or lost uploaded fact")
				}
			}
			for _, call := range calls {
				if call["started_at"].(time.Time).After(r.SafetyStop.At) {
					t.Fatal("RPC began after stop")
				}
			}
		}) {
			return
		}
	}
}

// The existing cold-start contract expects online + final-offline submissions.
// The production online debounce is 25s; do not turn a warm immediate reconnect
// into a fake cold PASS or weaken the two-submission check.
func waitColdPresenceWindow(t *testing.T, targets MetricsTargets, out, mode string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	collector := newServerMetricsCollector(targets)
	stable := time.Now()
	var previous float64 = -1
	var samples []map[string]any
	for {
		v, e := collector.scrape(ctx)
		if e != nil {
			t.Fatal("cold-window observation failed", e)
		}
		submitted := v["telesrv_presence_last_seen_submitted_total"]
		pending := v["telesrv_presence_last_seen_pending"]
		now := time.Now().UTC()
		if submitted != previous || pending != 0 {
			stable = now
			previous = submitted
		}
		samples = append(samples, map[string]any{"at": now, "submitted": submitted, "pending": pending})
		if now.Sub(stable) >= 26*time.Second {
			guardWireProof(t, out, mode+"-cold-window", map[string]any{"samples": samples, "stable_seconds": now.Sub(stable).Seconds(), "scope": "explicit cold baseline outside production 25-second online last-seen debounce; no lifecycle threshold change"})
			return
		}
		timer := time.NewTimer(2 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			t.Fatal("cold-window budget exhausted")
		case <-timer.C:
		}
	}
}
