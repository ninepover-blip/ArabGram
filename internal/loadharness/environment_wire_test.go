//go:build darwin || linux

package loadharness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
)

// These probes only run under the explicitly owned fixture guardian. The
// backlog case pauses that Egress, and always resumes the same process epoch.
func TestRealEnvironmentSafety(t *testing.T) {
	fixture, output, specPath := os.Getenv("TELESRV_LOAD_ENV_FIXTURE"), os.Getenv("TELESRV_LOAD_ENV_OUTPUT"), os.Getenv("TELESRV_LOAD_ENV_SPEC")
	if fixture == "" || output == "" || specPath == "" {
		t.Skip("explicit owned loopback fixture required")
	}
	manifestPath := filepath.Join(fixture, "pool/manifest.json")
	manifest, e := LoadManifest(manifestPath)
	if e != nil {
		t.Fatal(e)
	}
	if !strings.HasPrefix(manifest.Endpoint.Address, "127.0.0.1:") || len(manifest.Sessions) != 4 || len(primaryTargets(manifest.Sessions)) != 2 {
		t.Fatal("two-account/four-device fixture required")
	}
	if e := os.Mkdir(output, 0700); e != nil {
		t.Fatal("refusing to overwrite evidence", e)
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
	base, _, e := loadEnvironmentSpec(specPath, targets)
	if e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"normal", "rss_growth", "process_exit", "backlog_age", "disk_preflight", "identity_preflight"} {
		if !t.Run(mode, func(t *testing.T) {
			spec := base
			spec.Processes = append([]environmentProcessSpec(nil), base.Processes...)
			cfg := RunConfig{ManifestPath: manifestPath, SessionKeyPath: filepath.Join(fixture, "pool/session.key"), ReportPath: filepath.Join(output, mode+"-report.json"), EventsPath: filepath.Join(output, mode+"-events.ndjson"), EnvironmentPath: filepath.Join(output, mode+"-environment.json"),
				ServerMetricsTargets: targets, SessionLimit: 4, Duration: 45 * time.Second, RampDuration: 2 * time.Second, RPCInterval: 5 * time.Second, MessageInterval: -time.Second, MessageRate: 1, MessageQueueDepth: 4, DeliverySettle: 2 * time.Second, DeliveryCacheRecords: 2, OperationTimeout: 5 * time.Second, SampleInterval: time.Second, MinimumReadyRatio: 1}
			var helper *environmentTestProcess
			if mode == "rss_growth" || mode == "process_exit" {
				helper = startEnvironmentTestProcess(t)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				v, e := readProcessResources(ctx, helper.cmd.Process.Pid)
				cancel()
				if e != nil {
					t.Fatal(e)
				}
				exe, e := os.Executable()
				if e != nil {
					t.Fatal(e)
				}
				b, e := os.ReadFile(exe)
				if e != nil {
					t.Fatal(e)
				}
				sha := sha256.Sum256(b)
				spec.Processes = append(spec.Processes, environmentProcessSpec{ID: "probe/pressure", PID: helper.cmd.Process.Pid, Executable: exe, Command: strings.Join(helper.cmd.Args, " "), BinarySHA: hex.EncodeToString(sha[:]), RSSBudget: (v.RSS + (32 << 20)) * 100 / 85, FileBudget: 8192})
			}
			if mode == "disk_preflight" {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				v, e := readEnvironmentContainer(ctx, spec, spec.Containers[0])
				cancel()
				if e != nil {
					t.Fatal(e)
				}
				spec.MinDiskFree = v.Disk.Free + 1<<30
			}
			if mode == "identity_preflight" {
				spec.Postgres.SystemID = "1"
			}
			var paused nativeProcessIdentity
			var resumeOnce sync.Once
			var injected atomic.Bool
			var mu sync.Mutex
			var faultAt time.Time
			var calls []map[string]any
			resume := func() {
				resumeOnce.Do(func() {
					if paused.PID == 0 || !injected.Load() {
						return
					}
					now, e := readProcessIdentity(paused.PID)
					if e != nil || now != paused {
						t.Error("cannot resume changed Egress process epoch")
						return
					}
					if e := syscall.Kill(paused.PID, syscall.SIGCONT); e != nil {
						t.Error("Egress resume failed")
					}
				})
			}
			if mode == "backlog_age" {
				// Egress is deliberately paused. This focused queue-age fault does
				// not claim metrics continuity or ordinary load acceptance.
				cfg.ServerMetricsTargets = nil
				cfg.Duration = 90 * time.Second
				cfg.MessageRate = .1
				for _, p := range spec.Processes {
					if p.ID == "egress/one" {
						paused, e = readProcessIdentity(p.PID)
						if e != nil {
							t.Fatal(e)
						}
					}
				}
				if paused.PID == 0 {
					t.Fatal("owned Egress required")
				}
				defer resume()
			}
			b, e := json.MarshalIndent(spec, "", "  ")
			if e != nil {
				t.Fatal(e)
			}
			if e := writeFileAtomic(cfg.EnvironmentPath, append(b, '\n'), 0600); e != nil {
				t.Fatal(e)
			}
			cfg.wrapInvoker = func(record SessionRecord, base tg.Invoker) tg.Invoker {
				return safetyWireInvoker(func(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					start := time.Now().UTC()
					request, send := in.(*tg.MessagesSendMessageRequest)
					if send && mode == "backlog_age" && injected.CompareAndSwap(false, true) {
						if e := syscall.Kill(paused.PID, syscall.SIGSTOP); e != nil {
							return errors.New("owned Egress pause failed")
						}
						mu.Lock()
						faultAt = time.Now().UTC()
						mu.Unlock()
					}
					err := base.Invoke(ctx, in, out)
					if send {
						mu.Lock()
						calls = append(calls, map[string]any{"started_at": start, "finished_at": time.Now().UTC(), "sender_session": record.Index, "marker": request.Message, "random_id": request.RandomID, "rpc_error": classifyError(err)})
						mu.Unlock()
						if helper != nil && err == nil && injected.CompareAndSwap(false, true) {
							mu.Lock()
							faultAt = time.Now().UTC()
							mu.Unlock()
							command := "grow\n"
							if mode == "process_exit" {
								command = "exit\n"
							}
							if _, e := io.WriteString(helper.input, command); e != nil {
								return errors.New("helper command failed")
							}
						}
					}
					return err
				})
			}
			ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration+30*time.Second)
			defer cancel()
			r, e := Run(ctx, cfg)
			resume()
			if strings.HasSuffix(mode, "_preflight") {
				if e == nil || r != nil || len(calls) != 0 {
					t.Fatal("preflight accepted unsafe environment", e)
				}
				guardWireProof(t, output, mode, map[string]any{"mode": mode, "error": e.Error(), "wire_send_calls": len(calls), "scope": "actual data-filesystem floor or database identity mismatch; no disk filling"})
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			mu.Lock()
			guardWireProof(t, output, mode, map[string]any{"mode": mode, "run_id": r.Delivery.RunID, "fault_at": faultAt, "calls": calls, "safety_stop": r.SafetyStop, "scope": "real roles and MTProto/PFS; RSS/exit uses a guardian-owned auxiliary process; backlog naturally ages while owned Egress is SIGSTOP, then same epoch is resumed"})
			mu.Unlock()
			if r.Version != RunReportVersion || !r.Environment.Enabled || r.Environment.Samples < 2 || !r.EventEvidence.Finalized || !r.Delivery.Ledger.AuditComplete || r.Environment.Last.At.Before(r.SafetyStop.WorkersStoppedAt) {
				t.Fatal("missing final environment/delivery evidence")
			}
			if mode == "normal" {
				if !r.Pass || r.Environment.Errors != 0 || r.Delivery.CommittedMessages < 40 || r.Delivery.LiveDelivered != 3*r.Delivery.CommittedMessages || r.Delivery.DifferenceRecovered != 0 {
					t.Fatalf("normal environment run failed: %v", r.Failures)
				}
				return
			}
			if r.Pass || !injected.Load() || r.SafetyStop.Source != "environment" || r.Environment.Errors == 0 || r.SafetyStop.WorkersStoppedAt.Sub(r.SafetyStop.At) > 3*time.Second {
				t.Fatalf("environment did not stop work: %+v %v", r.SafetyStop, r.Failures)
			}
			for _, c := range calls {
				if c["started_at"].(time.Time).After(r.SafetyStop.At) {
					t.Fatal("send after environment stop")
				}
			}
			if r.Delivery.RetryAttempts != 0 || r.Operations["updates.getDifference.delivery"].Count != 0 {
				t.Fatal("unsafe stop started extra retry/difference work")
			}
			switch mode {
			case "rss_growth":
				if r.SafetyStop.Class != "probe/pressure:process_rss" || r.LoadEndedAt.Sub(r.StartedAt) > 12*time.Second {
					t.Fatal("RSS stop mismatch", r.SafetyStop)
				}
			case "process_exit":
				if !strings.HasPrefix(r.SafetyStop.Class, "probe/pressure:") || r.LoadEndedAt.Sub(r.StartedAt) > 12*time.Second {
					t.Fatal("exit stop mismatch", r.SafetyStop)
				}
			case "backlog_age":
				if r.SafetyStop.Class != "account_pts:backlog_age" || r.SafetyStop.At.Sub(faultAt) < 60*time.Second || r.LoadEndedAt.Sub(r.StartedAt) > 80*time.Second || r.Environment.QueuePeak["account_pts"].AgeSeconds <= 60 || r.Delivery.LiveDelivered != 0 || r.Delivery.CommittedMessages == 0 {
					t.Fatal("natural queue-age stop mismatch", r.SafetyStop)
				}
				deadline := time.Now().Add(10 * time.Second)
				for {
					qctx, qcancel := context.WithTimeout(context.Background(), 3*time.Second)
					v, e := readEnvironmentQueues(qctx, base)
					qcancel()
					if e != nil {
						t.Fatal("resumed queue observation failed", e)
					}
					pending := uint64(0)
					for _, q := range v.Queues {
						pending += q.Rows
					}
					if pending == 0 {
						guardWireProof(t, output, "backlog-drained", map[string]any{"same_egress_resumed": true, "queues": v.Queues, "at": v.ObservedAt})
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("resumed Egress did not drain naturally")
					}
					time.Sleep(100 * time.Millisecond)
				}
			}
		}) {
			return
		}
	}
}
