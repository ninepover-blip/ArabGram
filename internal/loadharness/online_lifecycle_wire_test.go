//go:build darwin || linux

package loadharness

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/telegram"
	"github.com/iamxvbaba/td/tg"
)

func TestRealOnlineLifecycle(t *testing.T) {
	fixture, out, spec, targets := lifecycleFixture(t)
	if e := os.Mkdir(out, 0700); e != nil {
		t.Fatal(e)
	}
	for _, mode := range []string{"normal", "prepare_cancel", "ramp_cancel", "commit_cancel", "settle_cancel", "recovery_cancel", "deadline", "client_cycles"} {
		if !t.Run(mode, func(t *testing.T) {
			cfg := RunConfig{ManifestPath: filepath.Join(fixture, "pool/manifest.json"), SessionKeyPath: filepath.Join(fixture, "pool/session.key"),
				ReportPath: filepath.Join(out, mode+"-report.json"), EventsPath: filepath.Join(out, mode+"-events.ndjson"), EnvironmentPath: spec,
				ServerMetricsTargets: targets, SessionLimit: 4, Duration: 35 * time.Second, RampDuration: 2 * time.Second, SampleInterval: time.Second,
				RPCInterval: 5 * time.Second, MessageInterval: -time.Second, MessageRate: 1, MessageQueueDepth: 4, OperationTimeout: 5 * time.Second,
				DeliverySettle: 2 * time.Second, DeliveryCacheRecords: 2, MinimumReadyRatio: 1}
			if mode == "ramp_cancel" {
				cfg.RampDuration = 10 * time.Second
			}
			if mode == "settle_cancel" {
				cfg.Duration = 6 * time.Second
				cfg.DeliverySettle = 15 * time.Second
				cfg.RampDuration = time.Second
			}
			if mode == "recovery_cancel" {
				cfg.Duration = 6 * time.Second
				cfg.RecoveryDuration = 20 * time.Second
				cfg.RampDuration = time.Second
			}
			if mode == "prepare_cancel" {
				cfg.FileSizeBytes = 1 << 20
				cfg.FileChunkBytes = 64 << 10
				cfg.FileInterval = 2 * time.Second
				cfg.SetupTimeout = 15 * time.Second
				cfg.FileFixturePath = filepath.Join(out, mode+"-file.json")
			}
			if mode == "client_cycles" {
				cfg.MessageRate = 0
			}
			budget := 55 * time.Second
			if mode == "deadline" {
				budget = 3 * time.Second
			}
			parent, cancel := context.WithTimeout(context.Background(), budget)
			defer cancel()
			monitorCtx, stopMonitor := context.WithCancel(context.Background())
			defer stopMonitor()
			var monitor sync.WaitGroup
			var mu sync.Mutex
			var calls []map[string]any
			var faultAt time.Time
			var cycles []map[string]any
			var injected atomic.Bool
			interrupt := func() {
				if injected.CompareAndSwap(false, true) {
					mu.Lock()
					faultAt = time.Now().UTC()
					mu.Unlock()
					cancel()
				}
			}
			cfg.observeLifecycle = func(l *runLifecycle) {
				if mode != "settle_cancel" && mode != "recovery_cancel" {
					return
				}
				phase := "settle"
				if mode == "recovery_cancel" {
					phase = "recovery"
				}
				monitor.Add(1)
				go func() {
					defer monitor.Done()
					ticker := time.NewTicker(time.Millisecond)
					defer ticker.Stop()
					for {
						select {
						case <-monitorCtx.Done():
							return
						case <-ticker.C:
							l.mu.Lock()
							current := l.phase
							l.mu.Unlock()
							if current == phase {
								if phase == "recovery" {
									timer := time.NewTimer(2100 * time.Millisecond)
									select {
									case <-monitorCtx.Done():
										timer.Stop()
										return
									case <-timer.C:
									}
								}
								interrupt()
								return
							}
						}
					}
				}()
			}
			cfg.observeWorkers = func(workers []*loadWorker) {
				if mode != "client_cycles" {
					return
				}
				monitor.Add(1)
				go func() {
					defer monitor.Done()
					wait := func(predicate func() bool) bool {
						timer := time.NewTicker(time.Millisecond)
						defer timer.Stop()
						deadline := time.NewTimer(8 * time.Second)
						defer deadline.Stop()
						for {
							if predicate() {
								return true
							}
							select {
							case <-monitorCtx.Done():
								return false
							case <-deadline.C:
								return false
							case <-timer.C:
							}
						}
					}
					if !wait(func() bool {
						for _, w := range workers {
							if !w.businessReady.Load() {
								return false
							}
						}
						return true
					}) {
						return
					}
					for n := 0; n < 3; n++ {
						for _, w := range workers {
							if w.record.DeviceIndex != 1 {
								continue
							}
							previous := w.clientGeneration.Load()
							w.setOnline(false)
							if !wait(func() bool { return w.state.Load() == workerOffline }) {
								return
							}
							w.setOnline(true)
							if !wait(func() bool { return w.clientGeneration.Load() == previous+1 && w.businessReady.Load() }) {
								return
							}
							mu.Lock()
							cycles = append(cycles, map[string]any{"at": time.Now().UTC(), "session_index": w.record.Index, "old_generation": previous, "new_generation": w.clientGeneration.Load()})
							mu.Unlock()
						}
					}
				}()
			}
			if mode == "commit_cancel" || mode == "settle_cancel" {
				cfg.wrapUpdates = func(_ SessionRecord, _ telegram.UpdateHandler) telegram.UpdateHandler {
					return telegram.UpdateHandlerFunc(func(context.Context, tg.UpdatesClass) error { return nil })
				}
			}
			wrap := func(preparation bool) func(SessionRecord, tg.Invoker) tg.Invoker {
				return func(record SessionRecord, base tg.Invoker) tg.Invoker {
					return safetyWireInvoker(func(ctx context.Context, in bin.Encoder, output bin.Decoder) error {
						row := map[string]any{"session_index": record.Index, "started_at": time.Now().UTC(), "preparation": preparation, "method": fmt.Sprintf("%T", in)}
						if v, ok := in.(*tg.MessagesSendMessageRequest); ok {
							row["marker"] = v.Message
							row["random_id"] = v.RandomID
						}
						if v, ok := in.(*tg.UploadSaveFilePartRequest); ok {
							row["file_id"] = v.FileID
							row["part"] = v.FilePart
							row["part_bytes"] = len(v.Bytes)
						}
						err := base.Invoke(ctx, in, output)
						row["wire_error"] = classifyError(err)
						if err == nil {
							_, state := in.(*tg.UpdatesGetStateRequest)
							_, send := in.(*tg.MessagesSendMessageRequest)
							_, part := in.(*tg.UploadSaveFilePartRequest)
							if (mode == "ramp_cancel" && state) || (mode == "commit_cancel" && send) || (mode == "prepare_cancel" && part) {
								interrupt()
								row["response_withheld"] = true
								err = context.Canceled
							}
						}
						row["finished_at"] = time.Now().UTC()
						row["returned_error"] = classifyError(err)
						mu.Lock()
						calls = append(calls, row)
						mu.Unlock()
						return err
					})
				}
			}
			cfg.wrapInvoker = wrap(false)
			cfg.wrapPreparationInvoker = wrap(true)
			r, err := Run(parent, cfg)
			stopMonitor()
			monitor.Wait()
			if err != nil {
				t.Fatal("run lost final report", err)
			}
			guardWireProof(t, out, mode, map[string]any{"mode": mode, "calls": calls, "fault_at": faultAt, "cycles": cycles, "lifecycle": r.Lifecycle, "safety_stop": r.SafetyStop, "scope": "real clients; parent cancellation by phase; commit/preparation response withheld after real RPC; client_cycles changes only selected secondary clients; does not claim physical transport generation attribution"})
			if !r.EventEvidence.Finalized || !r.Delivery.Ledger.AuditComplete || r.Environment.Last.At.Before(r.SafetyStop.WorkersStoppedAt) || r.Resources.Last.At.Before(r.SafetyStop.WorkersStoppedAt) {
				t.Fatal("final evidence missing")
			}
			if mode == "normal" || mode == "client_cycles" {
				if !r.Pass || r.Lifecycle.Interrupted {
					t.Fatal(r.Failures)
				}
				if mode == "client_cycles" && len(cycles) != 6 {
					t.Fatal("missing real client cycles", len(cycles))
				}
				return
			}
			if r.Pass || !r.Lifecycle.Interrupted || r.SafetyStop.Source != "external" {
				t.Fatal("cancellation not represented", r.Failures, r.Lifecycle, r.SafetyStop)
			}
			phase := map[string]string{"prepare_cancel": "preparation", "ramp_cancel": "ramp", "commit_cancel": "load", "settle_cancel": "settle", "recovery_cancel": "recovery", "deadline": "load"}[mode]
			if r.Lifecycle.InterruptedPhase != phase {
				t.Fatal("wrong canceled phase", r.Lifecycle)
			}
			for _, c := range calls {
				if c["started_at"].(time.Time).After(r.SafetyStop.At) {
					t.Fatal("new workload RPC after stop")
				}
			}
			for _, v := range r.ServerMetricsTargets {
				if v.Errors > 0 || !v.ContinuityComplete {
					t.Fatal("parent cancel manufactured metrics outage")
				}
			}
			if mode == "commit_cancel" && (r.Delivery.AttemptedMessages != 1 || r.Delivery.UnresolvedMessages != 1 || r.Delivery.RetryAttempts != 0) {
				t.Fatal("uncertain commit erased", r.Delivery)
			}
			if mode == "prepare_cancel" && (r.WorkloadStarted || r.ConnectionAttempts != 0 || r.Preparation.PartCalls != 1 || r.Preparation.PartResponsesOK != 0) {
				t.Fatal("preparation cancellation lost boundary", r.Preparation)
			}
			if mode == "recovery_cancel" && (r.Lifecycle.RecoveryComplete || r.Lifecycle.RecoveryStartedAt.IsZero() || r.Lifecycle.RecoveryEndedAt.Before(r.Lifecycle.RecoveryStartedAt)) {
				t.Fatal("canceled recovery claimed complete")
			}
			if mode == "deadline" && r.Lifecycle.InterruptionClass != "timeout" {
				t.Fatal("deadline misclassified", r.Lifecycle)
			}
		}) {
			return
		}
	}
}
