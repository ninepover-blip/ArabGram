package loadharness

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/telegram"
	"github.com/iamxvbaba/td/tg"
)

// This opt-in probe uses the same Invoker and UpdateHandler as Run. It drops
// responses/observations at the client boundary, not packets on the network.
func TestRealSendUncertainty(t *testing.T) {
	fixture, output := os.Getenv("TELESRV_LOAD_WIRE_FIXTURE"), os.Getenv("TELESRV_LOAD_WIRE_OUTPUT")
	if fixture == "" {
		t.Skip("requires an explicitly isolated wire fixture")
	}
	if !filepath.IsAbs(fixture) || !filepath.IsAbs(output) {
		t.Fatal("absolute fixture/output paths required")
	}
	evidence, err := os.ReadFile(filepath.Join(fixture, "fixture-evidence.json"))
	if err != nil {
		t.Fatal(err)
	}
	var ownership struct {
		Isolated bool `json:"isolated"`
		Loopback bool `json:"loopback_only"`
		Sessions int  `json:"sessions_limit"`
	}
	if err := json.Unmarshal(evidence, &ownership); err != nil || !ownership.Isolated || !ownership.Loopback || ownership.Sessions != 4 {
		t.Fatal("fixture isolation contract not satisfied")
	}
	manifestPath := filepath.Join(fixture, "pool/manifest.json")
	manifest, err := LoadManifest(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(manifest.Endpoint.Address, "127.0.0.1:") || len(manifest.Sessions) != 4 || len(primaryTargets(manifest.Sessions)) != 2 {
		t.Fatal("probe requires the isolated two-account/four-device loopback fixture")
	}
	if err := os.Mkdir(output, 0o700); err != nil {
		t.Fatal("refusing to reuse probe output: ", err)
	}
	var targets MetricsTargets
	for _, role := range []string{"edge", "core", "egress", "file", "sfu"} {
		data, err := os.ReadFile(filepath.Join(fixture, "configs", role+".yaml"))
		if err != nil {
			t.Fatal(err)
		}
		var config struct {
			Debug struct {
				Addr string `json:"addr"`
			} `json:"debug"`
		}
		if err := json.Unmarshal(data, &config); err != nil || !strings.HasPrefix(config.Debug.Addr, "127.0.0.1:") {
			t.Fatal("loopback metrics config required")
		}
		if err := targets.Set(role + "/one=http://" + config.Debug.Addr + "/metrics"); err != nil {
			t.Fatal(err)
		}
	}
	for _, mode := range []string{"drop_first", "drop_all", "difference"} {
		if !t.Run(mode, func(t *testing.T) {
			oracle := &uncertaintyWireOracle{mode: mode, attempts: map[string]uncertaintyWireAttempt{}}
			cfg := RunConfig{ManifestPath: manifestPath, SessionKeyPath: filepath.Join(fixture, "pool/session.key"),
				ReportPath: filepath.Join(output, mode+"-report.json"), EventsPath: filepath.Join(output, mode+"-events.ndjson"),
				ServerMetricsTargets: targets, SessionLimit: 4, Duration: 45 * time.Second, RampDuration: 2 * time.Second, RPCInterval: 5 * time.Second,
				MessageInterval: -time.Second, MessageRate: 1, MessageQueueDepth: 4, DeliverySettle: 2 * time.Second,
				DeliveryCacheRecords: 2,
				OperationTimeout:     5 * time.Second, SampleInterval: time.Second, MinimumReadyRatio: 1,
				wrapInvoker: oracle.wrapInvoker, wrapUpdates: oracle.wrapUpdates}
			ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
			defer cancel()
			report, err := Run(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			oracle.mu.Lock()
			defer oracle.mu.Unlock()
			intents := make(map[string]any, len(oracle.attempts))
			for marker, attempt := range oracle.attempts {
				intents[marker] = map[string]any{"random_id": attempt.randomID, "peer": attempt.peer, "message_id": attempt.id, "pts": attempt.pts, "wire_calls": attempt.calls}
			}
			rawProof := map[string]any{"mode": mode, "scope": "client response/observation loss over real MTProto; not network packet loss", "run_id": report.Delivery.RunID,
				"targeted_intents": len(oracle.attempts), "dropped_responses": oracle.droppedResponses, "dropped_live_observations": oracle.droppedLive,
				"wire_errors": oracle.wireErrors, "same_intent_message_id_and_pts": oracle.conflicts == 0, "ordinary_report_pass": report.Pass, "intents": intents}
			data, _ := json.MarshalIndent(rawProof, "", "  ")
			if err := writeFileAtomic(filepath.Join(output, mode+"-injection.json"), append(data, '\n'), 0o600); err != nil {
				t.Fatal(err)
			}
			if len(oracle.attempts) != 2 || oracle.wireErrors != 0 || oracle.conflicts != 0 {
				t.Fatalf("wire injection did not match fixture: %+v", rawProof)
			}
			for _, attempt := range oracle.attempts {
				if attempt.calls != 2 || attempt.id <= 0 || attempt.pts <= 0 {
					t.Fatalf("exact replay evidence missing: %+v", attempt)
				}
			}
			expectedResponses, expectedLive := 4, 0
			if mode == "drop_first" {
				expectedResponses = 2
			}
			if mode == "difference" {
				expectedLive = 6
			}
			if oracle.droppedResponses != expectedResponses || oracle.droppedLive != expectedLive {
				t.Fatalf("unexpected injection counts: %+v", rawProof)
			}
			d := report.Delivery
			if !d.Ledger.AuditComplete || d.Ledger.AuditRecords != d.AttemptedMessages || d.Ledger.Evictions == 0 || d.Ledger.PeakCacheRecords > 2 || d.Ledger.Error != "" {
				t.Fatalf("bounded ledger did not cover wire probe: %+v", d.Ledger)
			}
			if d.InitialUncertain != 2 || d.RetryAttempts != 2 || d.CommittedAfterUncertainty != 2 || d.CommittedMessages != report.MessageCompleted || d.Expected != 3*d.CommittedMessages || d.Delivered != d.Expected || d.Missing != 0 || d.UnresolvedMessages != 0 || d.NotCommittedMessages != 0 || d.PendingMessages != 0 {
				t.Fatalf("uncertainty accounting: %+v", d)
			}
			if d.InvalidMessageObserved+d.MessageIDConflicts+d.CommitContradictions+d.DuplicateObservations+d.WrongAccountObserved+d.UnknownDeviceObserved+d.OriginLiveObserved+d.UnmatchedMarkers != 0 {
				t.Fatalf("invalid evidence: %+v", d)
			}
			observationOnly := uint64(2)
			if mode == "drop_first" {
				observationOnly = 0
			}
			if d.CommittedByObservationOnly != observationOnly || d.RetryConfirmed != 2-observationOnly || d.DifferenceRecovered != uint64(expectedLive) || d.LiveDelivered != d.Expected-uint64(expectedLive) {
				t.Fatalf("wrong evidence source: %+v", d)
			}
			expectedFailures := map[string]bool{"messages.sendMessage returned 2 unexpected non-cancel errors": true}
			if mode != "drop_first" {
				expectedFailures["messages.sendMessage.retry returned 2 unexpected non-cancel errors"] = true
			}
			if mode == "difference" {
				expectedFailures["device online expectation violated: online missing 6, unavailable 0, stale observations 0"] = true
			}
			if report.Pass || len(report.Failures) != len(expectedFailures) {
				t.Fatalf("ordinary report hid errors or has unexpected failures: %v", report.Failures)
			}
			for _, failure := range report.Failures {
				if !expectedFailures[failure] {
					t.Fatal("unexpected failure: ", failure)
				}
			}
			t.Logf("probe PASS; ordinary report intentionally FAIL: messages=%d devices=%d uncertain=2 recovered=%d", d.CommittedMessages, d.Delivered, d.CommittedAfterUncertainty)
		}) {
			break
		}
	}
}

type uncertaintyWireAttempt struct {
	randomID, peer int64
	calls, id, pts int
}
type uncertaintyWireOracle struct {
	mu                                                   sync.Mutex
	mode                                                 string
	attempts                                             map[string]uncertaintyWireAttempt
	droppedResponses, droppedLive, wireErrors, conflicts int
}

func uncertaintyProbeMarker(marker string) bool {
	parts := strings.Split(marker, "/")
	return len(parts) == 4 && parts[0] == deliveryMarkerPrefix && parts[3] == "1"
}

func (o *uncertaintyWireOracle) wrapInvoker(record SessionRecord, next tg.Invoker) tg.Invoker {
	return arrivalInvoker(func(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
		req, targeted := in.(*tg.MessagesSendMessageRequest)
		if !targeted || !uncertaintyProbeMarker(req.Message) {
			return next.Invoke(ctx, in, out)
		}
		// Register before the wire call: live delivery can precede rpc_result.
		// Only these current-run intents may be hidden, never old fixture traffic.
		o.mu.Lock()
		if _, exists := o.attempts[req.Message]; !exists {
			o.attempts[req.Message] = uncertaintyWireAttempt{}
		}
		o.mu.Unlock()
		err := next.Invoke(ctx, in, out)
		o.mu.Lock()
		defer o.mu.Unlock()
		if err != nil {
			o.wireErrors++
			return err
		}
		box, ok := out.(*tg.UpdatesBox)
		if !ok {
			o.wireErrors++
			return fmt.Errorf("unexpected response box %T", out)
		}
		peer := req.Peer.(*tg.InputPeerUser).UserID
		id, err := validateSendConfirmation(box.Updates, req.Message, req.RandomID, record.UserID, peer)
		if err != nil {
			o.wireErrors++
			return err
		}
		pts := 0
		if updates, ok := box.Updates.(*tg.Updates); ok {
			for _, u := range updates.Updates {
				if m, ok := u.(*tg.UpdateNewMessage); ok {
					pts = m.Pts
				}
			}
		}
		a := o.attempts[req.Message]
		if a.calls > 0 && (a.randomID != req.RandomID || a.peer != peer || a.id != id || a.pts != pts) {
			o.conflicts++
		}
		a.randomID, a.peer, a.id, a.pts = req.RandomID, peer, id, pts
		a.calls++
		o.attempts[req.Message] = a
		if o.mode != "drop_first" || a.calls == 1 {
			o.droppedResponses++
			box.Updates = nil
			return context.DeadlineExceeded
		}
		return nil
	})
}

func (o *uncertaintyWireOracle) wrapUpdates(_ SessionRecord, next telegram.UpdateHandler) telegram.UpdateHandler {
	return telegram.UpdateHandlerFunc(func(ctx context.Context, updates tg.UpdatesClass) error {
		if o.mode != "difference" {
			return next.Handle(ctx, updates)
		}
		drop := func(marker string) bool {
			o.mu.Lock()
			defer o.mu.Unlock()
			if _, registered := o.attempts[marker]; !registered {
				return false
			}
			o.droppedLive++
			return true
		}
		filter := func(source []tg.UpdateClass) []tg.UpdateClass {
			out := make([]tg.UpdateClass, 0, len(source))
			for _, u := range source {
				if n, ok := u.(*tg.UpdateNewMessage); ok {
					if m, ok := n.Message.(*tg.Message); ok && drop(m.Message) {
						continue
					}
				}
				out = append(out, u)
			}
			return out
		}
		switch value := updates.(type) {
		case *tg.Updates:
			copy := *value
			copy.Updates = filter(value.Updates)
			updates = &copy
		case *tg.UpdatesCombined:
			copy := *value
			copy.Updates = filter(value.Updates)
			updates = &copy
		case *tg.UpdateShort:
			if len(filter([]tg.UpdateClass{value.Update})) == 0 {
				return nil
			}
		case *tg.UpdateShortMessage:
			if drop(value.Message) {
				return nil
			}
		}
		return next.Handle(ctx, updates)
	})
}
