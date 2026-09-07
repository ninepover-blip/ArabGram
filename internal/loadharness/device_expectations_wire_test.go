//go:build darwin || linux

package loadharness

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/telegram"
	"github.com/iamxvbaba/td/tg"
)

func TestRealDeviceExpectations(t *testing.T) {
	fixture, out, spec, targets := lifecycleFixture(t)
	if err := os.Mkdir(out, 0700); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"normal", "offline", "online_drop", "late_callback", "duplicate_live", "too_long"} {
		if !t.Run(mode, func(t *testing.T) {
			cfg := RunConfig{ManifestPath: filepath.Join(fixture, "pool/manifest.json"), SessionKeyPath: filepath.Join(fixture, "pool/session.key"),
				ReportPath: filepath.Join(out, mode+"-report.json"), EventsPath: filepath.Join(out, mode+"-events.ndjson"), EnvironmentPath: spec,
				ServerMetricsTargets: targets, SessionLimit: 4, Duration: 35 * time.Second, RampDuration: 2 * time.Second, SampleInterval: time.Second,
				RPCInterval: 5 * time.Second, MessageInterval: -time.Second, MessageRate: 1, MessageQueueDepth: 4, OperationTimeout: 5 * time.Second,
				DeliverySettle: 2 * time.Second, DeliveryCacheRecords: 1, MinimumReadyRatio: 1}
			if mode != "normal" && mode != "duplicate_live" {
				cfg.Duration = 45 * time.Second
				cfg.OfflineFraction = 1
				cfg.OfflineAt = 5500 * time.Millisecond
				cfg.OfflineFor = 3 * time.Second
			}
			var mu sync.Mutex
			var calls, observations []map[string]any
			var held func()
			var runID string
			generations := map[int]uint64{}
			var injected atomic.Bool
			monitorCtx, stopMonitor := context.WithCancel(context.Background())
			defer stopMonitor()
			var monitor sync.WaitGroup
			cfg.observeWorkers = func(workers []*loadWorker) {
				runID = workers[0].delivery.runID
				if mode != "late_callback" {
					return
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
							for _, w := range workers {
								if w.record.DeviceIndex > 0 && w.record.AccountIndex == 0 && w.clientGeneration.Load() >= 2 && w.businessReady.Load() {
									mu.Lock()
									f := held
									mu.Unlock()
									if f != nil {
										f()
										injected.Store(true)
										return
									}
								}
							}
						}
					}
				}()
			}
			cfg.wrapInvoker = func(record SessionRecord, next tg.Invoker) tg.Invoker {
				return arrivalInvoker(func(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
					start := time.Now().UTC()
					err := next.Invoke(ctx, in, out)
					item := map[string]any{"start": start, "end": time.Now().UTC(), "session_index": record.Index, "method": fmt.Sprintf("%T", in), "wire_error": classifyError(err)}
					if req, ok := in.(*tg.MessagesSendMessageRequest); ok {
						item["marker"], item["random_id"] = req.Message, req.RandomID
					}
					if req, ok := in.(*tg.UpdatesGetDifferenceRequest); ok {
						item["pts"], item["date"], item["qts"] = req.Pts, req.Date, req.Qts
						if err == nil {
							item["wire_result"] = fmt.Sprintf("%T", out.(*tg.UpdatesDifferenceBox).Difference)
						}
						if mode == "too_long" && record.DeviceIndex > 0 && err == nil && injected.CompareAndSwap(false, true) {
							out.(*tg.UpdatesDifferenceBox).Difference = &tg.UpdatesDifferenceTooLong{Pts: req.Pts + 100}
							item["injected"] = "too_long"
						}
					}
					mu.Lock()
					calls = append(calls, item)
					mu.Unlock()
					return err
				})
			}
			cfg.wrapUpdates = func(record SessionRecord, next telegram.UpdateHandler) telegram.UpdateHandler {
				mu.Lock()
				generations[record.Index]++
				generation := generations[record.Index]
				mu.Unlock()
				return telegram.UpdateHandlerFunc(func(ctx context.Context, u tg.UpdatesClass) error {
					var messages []deliveryMessage
					for _, m := range deviceProbeMessages(u) {
						if strings.HasPrefix(m.marker, "telesrv-load-v3/"+runID+"/") {
							messages = append(messages, m)
						}
					}
					if len(messages) == 0 {
						return next.Handle(ctx, u)
					}
					action := "pass"
					if mode == "online_drop" && record.DeviceIndex == 0 && record.AccountIndex == 1 {
						action = "drop"
					}
					mu.Lock()
					if mode == "late_callback" && record.DeviceIndex > 0 && record.AccountIndex == 0 && held == nil {
						action = "hold_old_handler"
						held = func() {
							mu.Lock()
							for _, m := range messages {
								observations = append(observations, map[string]any{"at": time.Now().UTC(), "session_index": record.Index, "generation": generation, "marker": m.marker, "message_id": m.id, "action": "replay_old_handler"})
							}
							mu.Unlock()
							_ = next.Handle(context.Background(), u)
						}
					}
					if mode == "duplicate_live" && injected.CompareAndSwap(false, true) {
						action = "duplicate"
					}
					for _, m := range messages {
						observations = append(observations, map[string]any{"at": time.Now().UTC(), "session_index": record.Index, "generation": generation, "marker": m.marker, "message_id": m.id, "action": action})
					}
					mu.Unlock()
					if action != "pass" && action != "duplicate" {
						return nil
					}
					if err := next.Handle(ctx, u); err != nil {
						return err
					}
					if action == "duplicate" {
						return next.Handle(ctx, u)
					}
					return nil
				})
			}
			ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
			defer cancel()
			report, err := Run(ctx, cfg)
			stopMonitor()
			monitor.Wait()
			mu.Lock()
			guardWireProof(t, out, mode, map[string]any{"mode": mode, "calls": calls, "observations": observations, "injected": injected.Load(), "scope": "actual MTProto messages; explicit application observation/cursor fault seams; no physical session identity assertion"})
			mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
			if report.Version != RunReportVersion || report.Delivery.Ledger.Version != 2 || !report.Delivery.Ledger.AuditComplete || !report.EventEvidence.Finalized {
				t.Fatalf("incomplete evidence: %+v", report)
			}
			d := report.Delivery
			switch mode {
			case "normal":
				if !report.Pass || d.CommittedMessages != 33 || d.OnlineExpected != 99 || d.OnlineLiveDelivered != 99 || d.PlannedArrivalSamples != 99 {
					t.Fatalf("normal: delivery=%+v failures=%v", d, report.Failures)
				}
			case "offline":
				if !report.Pass || d.CommittedMessages != 43 || d.OfflineExpected == 0 || d.OfflineDelivered != d.OfflineExpected || d.OnlineMissing != 0 || d.DuplicateObservations != 0 || d.Expected != 129 || d.PlannedArrivalSamples != d.OnlineExpected {
					t.Fatalf("offline: delivery=%+v failures=%v", d, report.Failures)
				}
			case "online_drop":
				if report.Pass || d.OfflineExpected == 0 || d.OfflineDelivered != d.OfflineExpected || d.OnlineMissing != 22 || d.Missing != 0 || d.UnavailableExpected != 0 {
					t.Fatalf("online drop: delivery=%+v failures=%v", d, report.Failures)
				}
			case "late_callback":
				if report.Pass || !injected.Load() || d.StaleObservations != 1 || report.SafetyStop.Class != "stale_client_observation" {
					t.Fatalf("late callback: delivery=%+v stop=%+v", d, report.SafetyStop)
				}
			case "duplicate_live":
				if report.Pass || !injected.Load() || d.DuplicateObservations != 1 {
					t.Fatalf("duplicate: delivery=%+v", d)
				}
			case "too_long":
				if report.Pass || !injected.Load() || report.SafetyStop.Class != "difference_evidence" {
					t.Fatalf("too long: stop=%+v failures=%v", report.SafetyStop, report.Failures)
				}
			}
		}) {
			return
		}
	}
}

func deviceProbeMessages(u tg.UpdatesClass) []deliveryMessage {
	var updates []tg.UpdateClass
	switch v := u.(type) {
	case *tg.UpdateShortMessage:
		return []deliveryMessage{{marker: v.Message, id: v.ID}}
	case *tg.UpdateShort:
		updates = []tg.UpdateClass{v.Update}
	case *tg.Updates:
		updates = v.Updates
	case *tg.UpdatesCombined:
		updates = v.Updates
	}
	var result []deliveryMessage
	for _, v := range updates {
		if n, ok := v.(*tg.UpdateNewMessage); ok {
			if m, ok := n.Message.(*tg.Message); ok {
				result = append(result, privateDeliveryMessage(m))
			}
		}
	}
	return result
}
