package loadharness

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestDeviceExpectationSurvivesEvictionAndClientReplacement(t *testing.T) {
	tr := testDeliveryTrackerWithOptions(t, "epochs", deliveryLedgerOptions{CacheRecords: 1})
	old := beginTestDelivery(t, tr, 0, 200, 1)
	tr.finish(old, sendAccepted, false, 1)
	tr.deviceTransition(1, 1, "offline")
	tr.deviceTransition(1, 1, "stop")
	offline := beginTestDelivery(t, tr, 0, 200, 2)
	tr.finish(offline, sendAccepted, false, 1)
	tr.deviceTransition(1, 2, "start")
	tr.observe(deliveryMessage{marker: offline, id: 1, peerUserID: 100}, 1, deliveryDifference, 2)
	tr.deviceTransition(1, 2, "ready")
	current := beginTestDelivery(t, tr, 0, 200, 3)
	tr.finish(current, sendAccepted, false, 1)
	tr.observe(deliveryMessage{marker: current, id: 1, peerUserID: 100}, 1, deliveryLive, 2)
	// The old callback and the new client's replay cannot satisfy the original
	// live deadline, even after the intent has left the cache.
	tr.observe(deliveryMessage{marker: old, id: 1, peerUserID: 100}, 1, deliveryLive, 1)
	tr.observe(deliveryMessage{marker: old, id: 1, peerUserID: 100}, 1, deliveryLive, 2)
	tr.observe(deliveryMessage{marker: old, id: 1, peerUserID: 100}, 1, deliveryDifference, 2)
	if err := tr.finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	d := tr.report()
	if d.OnlineExpected != 2 || d.OfflineExpected != 1 || d.UnavailableExpected != 0 || d.OnlineLiveDelivered != 1 || d.OnlineMissing != 1 || d.OfflineDelivered != 1 || d.StaleObservations != 1 || d.DuplicateObservations != 1 || d.PlannedArrivalSamples != 1 || d.Missing != 0 || d.Ledger.Evictions == 0 || d.Ledger.Version != 2 {
		t.Fatalf("generation evidence lost: %+v", d)
	}
}

func TestUnplannedLossAndNewReadyEpochCannotBecomeOfflineExemption(t *testing.T) {
	tr := testDeliveryTracker(t, "unexpected")
	old := beginTestDelivery(t, tr, 0, 200, 1)
	tr.finish(old, sendAccepted, false, 1)
	tr.deviceTransition(1, 1, "unavailable")
	missing := beginTestDelivery(t, tr, 0, 200, 2)
	tr.finish(missing, sendAccepted, false, 1)
	tr.deviceTransition(1, 1, "ready")
	for _, marker := range []string{old, missing} {
		tr.observe(deliveryMessage{marker: marker, id: 1, peerUserID: 100}, 1, deliveryLive, 1)
	}
	if err := tr.finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	d := tr.report()
	if d.OnlineExpected != 1 || d.OnlineMissing != 1 || d.UnavailableExpected != 1 || d.OfflineExpected != 0 || d.Delivered != 2 || d.PlannedArrivalSamples != 0 {
		t.Fatalf("unexpected outage hidden: %+v", d)
	}
}

func TestOfflineFractionDoesNotExcuseAnotherOnlineDevice(t *testing.T) {
	for _, offline := range []bool{false, true} {
		tr := testDeliveryTracker(t, "exemption")
		if offline {
			tr.deviceTransition(1, 1, "offline")
		}
		marker := beginTestDelivery(t, tr, 0, 200, 1)
		tr.finish(marker, sendAccepted, false, 1)
		tr.observe(deliveryMessage{marker: marker, id: 1, peerUserID: 100}, 1, deliveryDifference, 1)
		r := &RunReport{Version: 17, Delivery: tr.report(), Operations: map[string]OperationReport{"messages.sendMessage": {Count: 1}}}
		evaluateReport(r, RunConfig{MessageInterval: time.Second, OfflineFraction: .5})
		found := false
		for _, s := range r.Failures {
			if strings.Contains(s, "device online expectation violated") {
				found = true
			}
		}
		if found == offline {
			t.Fatalf("offline=%v failures=%v", offline, r.Failures)
		}
	}
}

func TestInactiveOrOverlappingClientCannotCreateOnlineEvidence(t *testing.T) {
	for _, transition := range []string{"start", "ready"} {
		tr := testDeliveryTracker(t, "invalid")
		if transition == "ready" {
			tr.deviceTransition(1, 1, "stop")
		}
		tr.deviceTransition(1, 2, transition)
		if tr.report().Ledger.Error == "" {
			t.Fatal("invalid generation transition accepted")
		}
	}
}

func TestOnlineDifferenceCannotPassBecauseSomeOtherDeviceIsOffline(t *testing.T) {
	r := &RunReport{ExpectedSessions: 2, PeakReadySessions: 2, SteadySamples: 1, SteadyReadyRatio: 1, MinSteadyReadySessions: 2,
		Delivery:   DeliveryReport{Ledger: DeliveryLedgerReport{Version: deliveryLedgerVersion, AuditComplete: true, AuditRecords: 1}, AttemptedMessages: 1, InitialConfirmed: 1, CommittedMessages: 1, Expected: 3, Delivered: 3, LiveDelivered: 2, DifferenceRecovered: 1},
		Operations: map[string]OperationReport{"messages.sendMessage": {Count: 1}}}
	evaluateReport(r, RunConfig{MinimumReadyRatio: 1, MessageInterval: time.Second, OfflineFraction: .5})
	if r.Pass {
		t.Fatal("online-only-through-difference falsely passed under a whole-run offline exemption")
	}
}
