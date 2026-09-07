//go:build darwin || linux

package loadharness

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/telegram"
	"github.com/iamxvbaba/td/tg"
)

type crossRoleRecorder struct{ events *eventWriter }

func crossRoleFrozenHashes(cfg RunConfig) (map[string]string, error) {
	paths := []string{cfg.ReportPath, cfg.EventsPath, filepath.Join(cfg.ReportPath+".delivery", "layout.json"), filepath.Join(cfg.ReportPath+".delivery", "records.bin"), filepath.Join(cfg.ReportPath+".delivery", "audit.json")}
	segments, err := filepath.Glob(cfg.EventsPath + ".segments/*")
	if err != nil {
		return nil, err
	}
	paths = append(paths, segments...)
	result := make(map[string]string, len(paths))
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		h := sha256.New()
		_, err = io.Copy(h, f)
		closeErr := f.Close()
		if err != nil || closeErr != nil {
			return nil, errors.Join(err, closeErr)
		}
		result[strings.TrimPrefix(path, filepath.Dir(cfg.ReportPath)+string(os.PathSeparator))] = fmt.Sprintf("%x", h.Sum(nil))
	}
	return result, nil
}

func (p crossRoleRecorder) record(phase, kind string, fields map[string]any) {
	fields["type"], fields["phase"], fields["at"] = kind, phase, time.Now().UTC()
	p.events.write(fields)
}

func (p crossRoleRecorder) configure(cfg *RunConfig, phase string, workers *[]*loadWorker) {
	cfg.observeWorkers = func(v []*loadWorker) { *workers = v }
	cfg.configureClient = func(r SessionRecord, hooks *clientHooks) {
		hooks.Logger = physicalProbeLog{probe: &physicalProbe{events: p.events}, index: r.Index}
	}
	cfg.wrapInvoker = func(r SessionRecord, next tg.Invoker) tg.Invoker {
		return arrivalInvoker(func(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
			start := time.Now()
			err := next.Invoke(ctx, in, out)
			v := map[string]any{"session_index": r.Index, "started_at": start.UTC(), "finished_at": time.Now().UTC(), "method": fmt.Sprintf("%T", in), "wire_error": classifyError(err)}
			if req, ok := in.(*tg.MessagesSendMessageRequest); ok {
				v["marker"], v["random_id"] = req.Message, req.RandomID
			}
			if _, ok := in.(*tg.UpdatesGetStateRequest); ok && err == nil {
				v["wire_state"] = *out.(*tg.UpdatesState)
			}
			p.record(phase, "rpc", v)
			return err
		})
	}
	cfg.wrapUpdates = func(r SessionRecord, next telegram.UpdateHandler) telegram.UpdateHandler {
		var worker *loadWorker
		for _, w := range *workers {
			if w.record.Index == r.Index {
				worker = w
			}
		}
		generation := worker.clientGeneration.Load()
		return telegram.UpdateHandlerFunc(func(ctx context.Context, u tg.UpdatesClass) error {
			entered := time.Now()
			messages := deviceProbeMessages(u)
			err := next.Handle(ctx, u)
			handled := time.Now()
			for _, m := range messages {
				if strings.HasPrefix(m.marker, "telesrv-load-v3/"+worker.delivery.runID+"/") {
					p.record(phase, "live", map[string]any{"entered_at": entered.UTC(), "handled_at": handled.UTC(), "entered_relative_ns": entered.Sub(worker.delivery.ledger.epoch).Nanoseconds(), "handled_relative_ns": handled.Sub(worker.delivery.ledger.epoch).Nanoseconds(), "session_index": r.Index, "client_generation": generation, "marker": m.marker, "message_id": m.id, "handler_error": classifyError(err)})
				}
			}
			return err
		})
	}
}

type crossRolePauseResult struct {
	healthy time.Time
	err     error
}

func crossRoleWait(ctx context.Context, interval time.Duration, predicate func() (bool, error)) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ok, err := predicate()
		if err != nil || ok {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// This only signals the exact process declared by the fixture guardian. The
// timer belongs to the fault controller, not the canceled workload context.
func (p crossRoleRecorder) pause(ctx context.Context, role string, spec environmentSpec, target MetricsTarget, workers []*loadWorker, injected *atomic.Bool) (result crossRolePauseResult) {
	var identity nativeProcessIdentity
	for _, process := range spec.Processes {
		if process.ID == role+"/one" {
			identity, result.err = readProcessIdentity(process.PID)
		}
	}
	if result.err != nil || identity.PID == 0 || !processOwnedBy(identity.PID, spec.OwnerPID) {
		result.err = errors.New("declared owned role identity missing")
		return
	}
	if result.err = crossRoleWait(ctx, 10*time.Millisecond, func() (bool, error) {
		return workers[0].delivery.report().CommittedMessages >= 5, nil
	}); result.err != nil {
		return
	}
	checkIdentity := func() error {
		now, err := readProcessIdentity(identity.PID)
		if err != nil || now != identity || !processOwnedBy(identity.PID, spec.OwnerPID) {
			return errors.New("role identity changed")
		}
		return nil
	}
	if result.err = checkIdentity(); result.err != nil {
		return
	}
	p.record("fault", "injection_requested", map[string]any{"role": role, "pid": identity.PID, "process_epoch": processEpoch(identity), "pause_ms": 8000})
	if result.err = syscall.Kill(identity.PID, syscall.SIGSTOP); result.err != nil {
		return
	}
	injected.Store(true)
	resumed := false
	resume := func() error {
		if resumed {
			return nil
		}
		if err := checkIdentity(); err != nil {
			return err
		}
		if err := syscall.Kill(identity.PID, syscall.SIGCONT); err != nil {
			return err
		}
		resumed = true
		p.record("fault", "resume_requested", map[string]any{"role": role, "pid": identity.PID, "process_epoch": processEpoch(identity)})
		return nil
	}
	defer func() {
		if err := resume(); result.err == nil && err != nil {
			result.err = err
		}
	}()
	state, err := environmentCommand(ctx, "/bin/ps", nil, 1024, "-o", "stat=", "-p", strconv.Itoa(identity.PID))
	if err != nil || !strings.Contains(string(state), "T") {
		result.err = errors.New("OS did not confirm stopped role")
		return
	}
	effective := time.Now()
	p.record("fault", "pause_effective", map[string]any{"role": role, "os_state": strings.TrimSpace(string(state)), "process_epoch": processEpoch(identity)})
	probeCtx, cancelProbe := context.WithTimeout(ctx, 250*time.Millisecond)
	_, probeErr := newServerMetricsClient(target.URL).scrape(probeCtx)
	cancelProbe()
	p.record("fault", "paused_health_probe", map[string]any{"role": role, "class": classifyError(probeErr)})
	if !errors.Is(probeErr, context.DeadlineExceeded) {
		result.err = errors.New("stopped role health probe did not time out")
		return
	}
	timer := time.NewTimer(time.Until(effective.Add(8 * time.Second)))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		result.err = ctx.Err()
		return
	case <-timer.C:
	}
	if result.err = resume(); result.err != nil {
		return
	}
	healthCtx, stopHealth := context.WithTimeout(ctx, 10*time.Second)
	defer stopHealth()
	result.err = crossRoleWait(healthCtx, 100*time.Millisecond, func() (bool, error) {
		if err := checkIdentity(); err != nil {
			return false, err
		}
		probeCtx, stop := context.WithTimeout(healthCtx, time.Second)
		v, err := newServerMetricsClient(target.URL).scrape(probeCtx)
		stop()
		p.record("fault", "restored_health_probe", map[string]any{"role": role, "class": classifyError(err), "metrics": v, "process_epoch": processEpoch(identity)})
		return err == nil && v["telesrv_process_start_time_seconds"] > 0, nil
	})
	if result.err == nil {
		result.healthy = time.Now().UTC()
		p.record("fault", "dependency_healthy", map[string]any{"role": role, "process_epoch": processEpoch(identity)})
	}
	return
}

func TestRealCrossRoleRecovery(t *testing.T) {
	fixture, out, specPath, targets := lifecycleFixture(t)
	spec, _, err := loadEnvironmentSpec(specPath, targets)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(out, 0700); err != nil {
		t.Fatal("refusing to overwrite cross-role evidence", err)
	}
	for _, role := range []string{"normal", "edge", "core", "egress"} {
		if !t.Run(role, func(t *testing.T) {
			parent, cancel := context.WithTimeout(context.Background(), 140*time.Second)
			defer cancel()
			events, err := newEventWriter(filepath.Join(out, role+"-probe.ndjson"))
			if err != nil {
				t.Fatal(err)
			}
			defer events.close()
			events.onFailure = func(string, time.Time) { cancel() }
			p := crossRoleRecorder{events: events}
			defer func() {
				if err := events.finalize(context.Background()); err != nil {
					t.Error(err)
				}
				guardWireProof(t, out, role, map[string]any{"role": role, "events": events.report(), "scope": "real 8s owned process pause; unchanged safety stop; saved-cursor recovery and separate post-recovery live run; old report remains immutable"})
			}()
			makeConfig := func(phase string, duration time.Duration) RunConfig {
				return RunConfig{ManifestPath: filepath.Join(fixture, "pool/manifest.json"), SessionKeyPath: filepath.Join(fixture, "pool/session.key"), ReportPath: filepath.Join(out, role+"-"+phase+"-report.json"), EventsPath: filepath.Join(out, role+"-"+phase+"-events.ndjson"), EnvironmentPath: specPath, ServerMetricsTargets: targets,
					SessionLimit: 4, Duration: duration, RampDuration: 2 * time.Second, SampleInterval: time.Second, RPCInterval: 5 * time.Second, MessageInterval: -time.Second, MessageRate: 1, MessageQueueDepth: 4, OperationTimeout: 8 * time.Second, DeliverySettle: 2 * time.Second, DeliveryCacheRecords: 1, MinimumReadyRatio: 1}
			}
			cfg := makeConfig("fault", 45*time.Second)
			if role == "normal" {
				cfg.Duration = 35 * time.Second
			}
			var workers []*loadWorker
			p.configure(&cfg, "fault", &workers)
			monitorCtx, stopMonitor := context.WithCancel(parent)
			defer stopMonitor()
			done := make(chan crossRolePauseResult, 1)
			var injected atomic.Bool
			monitorStarted := false
			if role != "normal" {
				baseObserve := cfg.observeWorkers
				cfg.observeWorkers = func(v []*loadWorker) {
					baseObserve(v)
					monitorStarted = true
					var target MetricsTarget
					for _, x := range targets {
						if x.Role == role {
							target = x
						}
					}
					go func() { done <- p.pause(monitorCtx, role, spec, target, v, &injected) }()
				}
			}
			report, err := Run(parent, cfg)
			p.record("fault", "workload_joined", map[string]any{"error": classifyError(err)})
			if role == "normal" {
				if err != nil || report == nil {
					t.Fatal("normal control did not produce a report", err)
				}
				if !report.Pass || report.Delivery.Missing != 0 || report.Delivery.OnlineLiveDelivered != report.Delivery.CommittedMessages*3 || report.Delivery.FinalReconciledDevices != 4 {
					t.Fatal("normal control failed", err, report.Failures)
				}
				return
			}
			if !injected.Load() {
				stopMonitor()
			}
			if !monitorStarted {
				t.Fatal("preflight stopped before fault controller launch", err)
			}
			pause := <-done
			if pause.err != nil || err != nil || !injected.Load() {
				t.Fatal("real role fault failed", pause.err, err, injected.Load())
			}
			if report.Pass || report.SafetyStop.Source != "metrics" || report.SafetyStop.Class != role+"/one:scrape_failed" || !report.Delivery.Ledger.AuditComplete || !report.EventEvidence.Finalized {
				t.Fatal("fault did not retain scoped metrics safety stop", report.SafetyStop, report.Failures)
			}
			before, err := crossRoleFrozenHashes(cfg)
			if err != nil {
				t.Fatal("cannot freeze original fault artifacts", err)
			}
			p.record("recovery", "original_evidence_frozen", map[string]any{"sha256": before})
			if err := p.reconcile(parent, fixture, spec, workers, pause.healthy); err != nil {
				t.Fatal("saved cursor recovery failed", err)
			}
			post := makeConfig("post", 35*time.Second)
			var postWorkers []*loadWorker
			p.configure(&post, "post", &postWorkers)
			postReport, err := Run(parent, post)
			p.record("post", "workload_joined", map[string]any{"error": classifyError(err)})
			if err != nil || postReport == nil {
				t.Fatal("post-recovery workload did not produce a report", err)
			}
			if !postReport.Pass || postReport.Delivery.OnlineLiveDelivered != postReport.Delivery.CommittedMessages*3 || postReport.Delivery.FinalReconciledDevices != 4 || postReport.MessageReadyAt.Sub(pause.healthy) > 60*time.Second || postReport.FinishedAt.Sub(pause.healthy) > 5*time.Minute {
				t.Fatal("post-recovery workload failed", err, postReport.Failures)
			}
			after, err := crossRoleFrozenHashes(cfg)
			if err != nil || len(before) != len(after) {
				t.Fatal("cannot verify original artifacts after recovery", err)
			}
			for path, digest := range before {
				if after[path] != digest {
					t.Fatal("recovery modified frozen fault artifacts", path)
				}
			}
			p.record("post", "original_evidence_unchanged", map[string]any{"sha256": after})
		}) {
			return
		}
	}
}

func (p crossRoleRecorder) reconcile(parent context.Context, fixture string, spec environmentSpec, workers []*loadWorker, healthy time.Time) error {
	ctx, cancel := context.WithTimeout(parent, 25*time.Second)
	defer cancel()
	err := crossRoleWait(ctx, 200*time.Millisecond, func() (bool, error) {
		qctx, stop := context.WithTimeout(ctx, 3*time.Second)
		q, err := readEnvironmentQueues(qctx, spec)
		stop()
		p.record("recovery", "queue_observation", map[string]any{"class": classifyError(err), "queues": q})
		if err != nil {
			return false, err
		}
		var pending uint64
		for _, v := range q.Queues {
			pending += v.Rows
		}
		return pending == 0, nil
	})
	if err != nil {
		return err
	}
	p.record("recovery", "queues_drained", map[string]any{"since_healthy_ms": time.Since(healthy).Milliseconds()})
	var wg sync.WaitGroup
	results := make(chan error, len(workers))
	cursors := make([]tg.UpdatesState, len(workers))
	for index, w := range workers {
		cursor, valid := w.deliveryState.load()
		if !valid {
			return errors.New("missing original audit cursor")
		}
		cursors[index] = cursor
	}
	for index, w := range workers {
		wg.Add(1)
		go func(w *loadWorker, state tg.UpdatesState) {
			defer wg.Done()
			p.record("recovery", "client_start", map[string]any{"session_index": w.record.Index, "saved_cursor": state, "old_run_id": w.delivery.runID})
			client, err := newClient(w.endpoint, w.publicKey, w.storage, clientHooks{Logger: physicalProbeLog{probe: &physicalProbe{events: p.events}, index: w.record.Index}})
			if err == nil {
				err = client.Run(ctx, func(ctx context.Context) error {
					authCtx, stopAuth := context.WithTimeout(ctx, 5*time.Second)
					status, err := client.Auth().Status(authCtx)
					stopAuth()
					if err != nil || !status.Authorized || status.User == nil || status.User.ID != w.record.UserID {
						return errors.New("recovery authorization identity mismatch")
					}
					for page := 0; page < 32; page++ {
						start := time.Now().UTC()
						op, stop := context.WithTimeout(ctx, 5*time.Second)
						diff, err := client.API().UpdatesGetDifference(op, &tg.UpdatesGetDifferenceRequest{Pts: state.Pts, Date: state.Date, Qts: state.Qts})
						stop()
						next, empty, validationErr := deliveryDifferenceState(state, diff)
						if err != nil {
							validationErr = err
						}
						var messages []tg.MessageClass
						var updates []tg.UpdateClass
						switch v := diff.(type) {
						case *tg.UpdatesDifference:
							messages, updates = v.NewMessages, v.OtherUpdates
						case *tg.UpdatesDifferenceSlice:
							messages, updates = v.NewMessages, v.OtherUpdates
						}
						var observed []map[string]any
						capture := func(m tg.MessageClass, source string) {
							if value, ok := m.(*tg.Message); ok && strings.HasPrefix(value.Message, "telesrv-load-v3/"+w.delivery.runID+"/") {
								observed = append(observed, map[string]any{"marker": value.Message, "message_id": value.ID, "out": value.GetOut(), "peer": value.PeerID, "from_id": value.FromID, "source": source})
							}
						}
						for _, m := range messages {
							capture(m, "new_messages")
						}
						for _, update := range updates {
							if m, ok := update.(*tg.UpdateNewMessage); ok {
								capture(m.Message, "other_updates")
							}
						}
						p.record("recovery", "difference_page", map[string]any{"session_index": w.record.Index, "page": page, "started_at": start, "from": state, "next": next, "empty": empty, "wire_type": fmt.Sprintf("%T", diff), "class": classifyError(validationErr), "messages": observed, "other_updates": len(updates)})
						if validationErr != nil {
							return validationErr
						}
						state = next
						if empty {
							return nil
						}
					}
					return errors.New("recovery difference page limit")
				})
			}
			p.record("recovery", "client_joined", map[string]any{"session_index": w.record.Index, "class": classifyError(err)})
			results <- err
		}(w, cursors[index])
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			return err
		}
	}
	p.record("recovery", "all_cursors_empty", map[string]any{"devices": len(workers), "since_healthy_ms": time.Since(healthy).Milliseconds()})
	return nil
}
