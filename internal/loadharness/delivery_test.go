package loadharness

import (
	"github.com/iamxvbaba/td/tg"
	"path/filepath"
	"testing"
	"time"
)

func testDeliveryTracker(t *testing.T, runID string, extra ...SessionRecord) *deliveryTracker {
	return testDeliveryTrackerWithOptions(t, runID, deliveryLedgerOptions{Targets: map[int]int64{2: 200}}, extra...)
}

func testDeliveryTrackerWithOptions(t *testing.T, runID string, options deliveryLedgerOptions, extra ...SessionRecord) *deliveryTracker {
	t.Helper()
	records := []SessionRecord{{Index: 0, AccountIndex: 0, UserID: 100}, {Index: 1, AccountIndex: 1, UserID: 200}, {Index: 2, AccountIndex: 2, UserID: 300}}
	tracker, err := newDeliveryTracker(runID, append(records, extra...), filepath.Join(t.TempDir(), "ledger"), options)
	if err != nil {
		t.Fatal(err)
	}
	for index := range tracker.devices {
		tracker.deviceTransition(index, 1, "start")
		tracker.deviceTransition(index, 1, "ready")
	}
	t.Cleanup(func() {
		if err := tracker.close(); err != nil {
			t.Error(err)
		}
	})
	return tracker
}

func beginTestDelivery(t *testing.T, tracker *deliveryTracker, sender int, recipient int64, sequence uint64) string {
	t.Helper()
	marker := tracker.marker(sender, sequence)
	if err := tracker.begin(marker, sender, recipient, int64(sequence), time.Now(), time.Now()); err != nil {
		t.Fatal(err)
	}
	return marker
}

func TestDeliveryTrackerReconcilesObservationBeforeSendReturn(t *testing.T) {
	tracker := testDeliveryTracker(t, "run")
	marker := beginTestDelivery(t, tracker, 0, 200, 9)
	observeTestDelivery(tracker, marker, 1, deliveryLive)
	if report := tracker.report(); report.CommittedMessages != 1 || report.Expected != 1 || report.PendingMessages != 1 {
		t.Fatalf("valid live evidence did not preserve the in-flight commit: %+v", report)
	}
	tracker.finish(marker, sendAccepted, false, 1)
	report := tracker.report()
	if report.CommittedMessages != 1 || report.Expected != 1 || report.Delivered != 1 || report.LiveDelivered != 1 || report.Missing != 0 {
		t.Fatalf("unexpected report: %+v", report)
	}
}

func TestDeliveryTrackerMeasuresFromSendStart(t *testing.T) {
	tracker := testDeliveryTracker(t, "run")
	marker := tracker.marker(0, 1)
	if err := tracker.begin(marker, 0, 200, 1, time.Now().Add(-25*time.Millisecond), time.Time{}); err != nil {
		t.Fatal(err)
	}
	observeTestDelivery(tracker, marker, 1, deliveryLive)
	tracker.finish(marker, sendAccepted, false, 1)
	report := tracker.report()
	if report.E2EP50MS < 20 || report.E2EP99MS < report.E2EP50MS || report.E2EMaxMS < report.E2EP99MS {
		t.Fatalf("unexpected e2e latency report: %+v", report)
	}
}

func TestDeliveryTrackerSeparatesDifferenceRecoveryAndDuplicates(t *testing.T) {
	tracker := testDeliveryTracker(t, "run")
	live := beginTestDelivery(t, tracker, 0, 200, 1)
	difference := beginTestDelivery(t, tracker, 1, 300, 1)
	tracker.finish(live, sendAccepted, false, 1)
	tracker.finish(difference, sendAccepted, false, 1)
	observeTestDelivery(tracker, live, 1, deliveryLive)
	observeTestDelivery(tracker, live, 1, deliveryLive)
	observeTestDelivery(tracker, live, 1, deliveryDifference)
	observeTestDelivery(tracker, difference, 2, deliveryDifference)
	report := tracker.report()
	if report.Delivered != 2 || report.LiveDelivered != 1 || report.DifferenceRecovered != 1 || report.DuplicateObservations != 1 {
		t.Fatalf("unexpected delivery sources: %+v", report)
	}
}

func TestObserveUpdatesClassExtractsPrivateMessages(t *testing.T) {
	tracker := testDeliveryTracker(t, "run")
	short := beginTestDelivery(t, tracker, 0, 200, 1)
	full := beginTestDelivery(t, tracker, 2, 200, 1)
	tracker.finish(short, sendAccepted, false, 1)
	tracker.finish(full, sendAccepted, false, 1)
	observeUpdatesClass(tracker, 1, &tg.UpdateShortMessage{Message: short, ID: 1, UserID: 100}, deliveryLive, 1)
	observeUpdatesClass(tracker, 1, &tg.Updates{Updates: []tg.UpdateClass{&tg.UpdateNewMessage{Message: &tg.Message{Message: full, ID: 1, PeerID: &tg.PeerUser{UserID: 300}}}}}, deliveryLive, 1)
	if report := tracker.report(); report.Delivered != 2 || report.Missing != 0 {
		t.Fatalf("unexpected report: %+v", report)
	}
}

func TestDeliveryTrackerRejectsForeignAndMalformedMarkers(t *testing.T) {
	tracker := testDeliveryTracker(t, "run")
	for _, marker := range []string{"telesrv-load-v3/other/1/1", "telesrv-load-v3/run/not-an-index/1", "telesrv-load-v3/run/-1/1"} {
		observeTestDelivery(tracker, marker, 1, deliveryLive)
	}
	if report := tracker.report(); report.UnmatchedMarkers != 0 {
		t.Fatalf("foreign marker entered report: %+v", report)
	}
}

func TestDeliveryTrackerRequiresEveryRecipientAndOtherSenderDevice(t *testing.T) {
	for _, missing := range []int{-1, 3, 4} {
		tracker := testDeliveryTracker(t, "run", SessionRecord{Index: 3, AccountIndex: 0, DeviceIndex: 1, UserID: 100}, SessionRecord{Index: 4, AccountIndex: 1, DeviceIndex: 1, UserID: 200})
		marker := beginTestDelivery(t, tracker, 0, 200, 1)
		for _, device := range []int{1, 3, 4} {
			if device != missing {
				observeTestDelivery(tracker, marker, device, deliveryLive)
			}
		}
		tracker.finish(marker, sendAccepted, false, 1)
		report := tracker.report()
		wantMissing := uint64(0)
		if missing >= 0 {
			wantMissing = 1
		}
		if report.CommittedMessages != 1 || report.Expected != 3 || report.LiveDelivered != 3-wantMissing || report.Missing != wantMissing || report.DuplicateObservations != 0 {
			t.Fatalf("missing=%d report=%+v", missing, report)
		}
		for _, device := range report.Devices {
			want := uint64(0)
			if device.SessionIndex == missing {
				want = 1
			}
			if device.Missing != want {
				t.Fatalf("missing device attribution: %+v", report.Devices)
			}
		}
	}
}

func TestDeliveryTrackerSelfMessageExcludesOriginOnce(t *testing.T) {
	tracker := testDeliveryTrackerWithOptions(t, "run", deliveryLedgerOptions{Targets: map[int]int64{0: 100}}, SessionRecord{Index: 3, AccountIndex: 0, DeviceIndex: 1, UserID: 100})
	marker := beginTestDelivery(t, tracker, 0, 100, 1)
	observeTestDelivery(tracker, marker, 3, deliveryLive)
	observeTestDelivery(tracker, marker, 0, deliveryDifference)
	tracker.finish(marker, sendAccepted, false, 1)
	if report := tracker.report(); report.Expected != 1 || report.Delivered != 1 || report.OriginLiveObserved != 0 {
		t.Fatalf("self delivery: %+v", report)
	}
	observeTestDelivery(tracker, marker, 0, deliveryLive)
	if report := tracker.report(); report.OriginLiveObserved != 1 {
		t.Fatalf("origin exclusion violation hidden: %+v", report)
	}
}

func TestDeliveryTrackerFlagsWrongAccountAndUnknownDevice(t *testing.T) {
	tracker := testDeliveryTracker(t, "run")
	marker := beginTestDelivery(t, tracker, 0, 200, 1)
	tracker.finish(marker, sendAccepted, false, 1)
	observeTestDelivery(tracker, marker, 2, deliveryLive)
	observeTestDelivery(tracker, marker, 999, deliveryLive)
	if report := tracker.report(); report.WrongAccountObserved != 1 || report.UnknownDeviceObserved != 1 || report.Delivered != 0 {
		t.Fatalf("wrong device accepted: %+v", report)
	}
}

func TestDeliveryTrackerUsesLiveTimestampAfterDifference(t *testing.T) {
	tracker := testDeliveryTracker(t, "run")
	marker := beginTestDelivery(t, tracker, 0, 200, 1)
	tracker.finish(marker, sendAccepted, false, 1)
	key, _ := tracker.key(marker)
	before, _, err := tracker.ledger.get(key)
	if err != nil {
		t.Fatal(err)
	}
	after := before.clone()
	start := before.expectation.startedAt
	o := &after.observations[1]
	o.sources, o.firstAt, o.firstLiveAt, o.onlineLiveAt = deliveryLive|deliveryDifference, start.Add(time.Millisecond), start.Add(50*time.Millisecond), start.Add(50*time.Millisecond)
	o.firstGeneration, o.liveGeneration, o.differenceGeneration = 1, 1, 1
	if err := tracker.replace(before, after); err != nil {
		t.Fatal(err)
	}
	if report := tracker.report(); report.E2EP99MS < 50 || report.E2EP99MS > 51.563 || report.LiveDelivered != 1 {
		t.Fatalf("difference timestamp used for live latency: %+v", report)
	}
}

func TestDeliveryTrackerRejectsAmbiguousTopologyAndExpectation(t *testing.T) {
	base := SessionRecord{Index: 0, AccountIndex: 0, UserID: 100}
	for _, bad := range []SessionRecord{
		{Index: 0, AccountIndex: 1, UserID: 200},
		{Index: 1, AccountIndex: 0, UserID: 100},
		{Index: 1, AccountIndex: 0, DeviceIndex: 1, UserID: 200},
		{Index: 1, AccountIndex: 1, UserID: 100},
		{Index: -1, AccountIndex: 1, UserID: 200},
	} {
		if _, err := newDeliveryTracker("run", []SessionRecord{base, bad}, filepath.Join(t.TempDir(), "ledger"), deliveryLedgerOptions{}); err == nil {
			t.Fatalf("invalid topology accepted: %+v", bad)
		}
	}
	tracker := testDeliveryTracker(t, "run")
	marker := beginTestDelivery(t, tracker, 0, 200, 1)
	if err := tracker.begin(marker, 0, 200, 1, time.Now(), time.Now()); err == nil {
		t.Fatal("duplicate expectation replaced")
	}
	if err := tracker.begin(tracker.marker(99, 1), 99, 200, 1, time.Now(), time.Now()); err == nil {
		t.Fatal("unknown sender accepted")
	}
	if err := tracker.begin(tracker.marker(0, 2), 0, 999, 1, time.Now(), time.Now()); err == nil {
		t.Fatal("unknown recipient accepted")
	}
}

func TestEvaluateReportSeparatesMessageCountFromDeviceDeliveries(t *testing.T) {
	for _, fixedRate := range []bool{true, false} {
		cfg := RunConfig{MinimumReadyRatio: 1, MessageInterval: time.Second}
		if fixedRate {
			cfg.MessageInterval = -time.Second
			cfg.MessageRate = 1
		}
		for _, fault := range []string{"none", "missing", "origin", "unknown", "difference"} {
			report := &RunReport{ExpectedSessions: 4, PeakReadySessions: 4, SteadySamples: 1, SteadyReadyRatio: 1,
				MessageReadiness: OperationReport{Count: 1}, MessageReadyAt: time.Now(),
				MessageScheduled: 1, MessageEnqueued: 1, MessageCompleted: 1,
				MessageTiming: MessageTimingReport{SchedulerLag: OperationReport{Count: 1}, SenderQueueWait: OperationReport{Count: 1}, PlannedToSend: OperationReport{Count: 1}},
				Operations:    map[string]OperationReport{"messages.sendMessage": {Count: 1}},
				Delivery:      DeliveryReport{Ledger: DeliveryLedgerReport{Version: deliveryLedgerVersion, AuditComplete: true, AuditRecords: 1}, AttemptedMessages: 1, InitialConfirmed: 1, CommittedMessages: 1, Expected: 3, Delivered: 3, LiveDelivered: 3, PlannedArrivalSamples: 3}}
			switch fault {
			case "missing":
				report.Delivery.Missing = 1
				report.Delivery.Delivered = 2
				report.Delivery.LiveDelivered = 2
				report.Delivery.PlannedArrivalSamples = 2
			case "origin":
				report.Delivery.OriginLiveObserved = 1
			case "unknown":
				report.Delivery.UnknownDeviceObserved = 1
			case "difference":
				report.Delivery.DifferenceRecovered = 1
				report.Delivery.LiveDelivered = 2
			}
			evaluateReport(report, cfg)
			if report.Pass != (fault == "none") {
				t.Fatalf("fixed=%v fault=%s pass=%v failures=%v", fixedRate, fault, report.Pass, report.Failures)
			}
		}
	}
}

func TestDeliveryTrackerRetainsCommittedMessageAfterRPCError(t *testing.T) {
	tracker := testDeliveryTracker(t, "run")
	marker := beginTestDelivery(t, tracker, 0, 200, 1)
	observeTestDelivery(tracker, marker, 1, deliveryLive)
	tracker.finish(marker, sendUncertain, false, 0)
	report := tracker.report()
	if report.CommittedMessages != 1 || report.Expected != 1 || report.LiveDelivered != 1 || report.UnmatchedMarkers != 0 {
		t.Fatalf("RPC error erased observed commit: %+v", report)
	}
}
