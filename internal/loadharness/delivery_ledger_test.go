package loadharness

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDeliveryLedgerEvictionRetainsLateEvidenceAndConflicts(t *testing.T) {
	tracker := testDeliveryTrackerWithOptions(t, "late", deliveryLedgerOptions{CacheRecords: 1})
	first := beginTestDelivery(t, tracker, 0, 200, 1)
	tracker.finish(first, sendUncertain, false, 0)
	second := beginTestDelivery(t, tracker, 0, 200, 2)
	tracker.finish(second, sendAccepted, false, 1)
	observeTestDelivery(tracker, second, 1, deliveryLive)
	tracker.observe(deliveryMessage{marker: first, id: 9, peerUserID: 100}, 1, deliveryDifference, 1)
	tracker.finish(first, sendAccepted, true, 7)
	// Force another eviction, then promote difference to live without resetting time.
	third := beginTestDelivery(t, tracker, 0, 200, 3)
	tracker.finish(third, sendAccepted, false, 1)
	observeTestDelivery(tracker, third, 1, deliveryLive)
	tracker.observe(deliveryMessage{marker: first, id: 9, peerUserID: 100}, 1, deliveryLive, 1)
	tracker.observe(deliveryMessage{marker: first, id: 9, peerUserID: 100}, 1, deliveryLive, 1)
	tracker.observe(deliveryMessage{marker: first, id: 10, peerUserID: 100}, 1, deliveryLive, 1)
	r := tracker.report()
	if r.CommittedMessages != 3 || r.LiveDelivered != 3 || r.DifferenceRecovered != 0 || r.CommittedAfterUncertainty != 1 || r.RetryConfirmed != 1 || r.DuplicateObservations != 1 || r.MessageIDConflicts != 1 || r.Ledger.Evictions < 3 || r.Ledger.PeakCacheRecords != 1 {
		t.Fatalf("evicted evidence lost: %+v", r)
	}
	if err := tracker.finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r := tracker.report(); !r.Ledger.AuditComplete || r.Ledger.AuditRecords != 3 || r.Ledger.SHA256 == "" {
		t.Fatalf("missing audit: %+v", r.Ledger)
	}
}

func TestDeliveryLedgerFaultsCannotPassAudit(t *testing.T) {
	for _, fault := range []string{"truncate", "checksum", "erase", "closed", "cancel", "layout", "layout_grow", "unlink", "replace"} {
		t.Run(fault, func(t *testing.T) {
			tracker := testDeliveryTrackerWithOptions(t, "fault", deliveryLedgerOptions{CacheRecords: 1})
			marker := beginTestDelivery(t, tracker, 0, 200, 1)
			tracker.finish(marker, sendAccepted, false, 1)
			observeTestDelivery(tracker, marker, 1, deliveryLive)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch fault {
			case "truncate":
				if err := tracker.ledger.f.Truncate(tracker.ledger.stats.FileBytes - 1); err != nil {
					t.Fatal(err)
				}
			case "checksum":
				if _, err := tracker.ledger.f.WriteAt([]byte{99}, 16); err != nil {
					t.Fatal(err)
				}
			case "erase":
				if _, err := tracker.ledger.f.WriteAt(make([]byte, tracker.ledger.routes[0].size), 0); err != nil {
					t.Fatal(err)
				}
			case "closed":
				if err := tracker.ledger.f.Close(); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				cancel()
			case "layout":
				if err := os.WriteFile(filepath.Join(tracker.ledger.dir, "layout.json"), []byte("{}"), 0o600); err != nil {
					t.Fatal(err)
				}
			case "layout_grow":
				if err := os.WriteFile(filepath.Join(tracker.ledger.dir, "layout.json"), make([]byte, 1<<20), 0o600); err != nil {
					t.Fatal(err)
				}
			case "unlink", "replace":
				path := filepath.Join(tracker.ledger.dir, "records.bin")
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if fault == "replace" {
					if err := os.WriteFile(path, make([]byte, tracker.ledger.stats.FileBytes), 0o600); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := tracker.finalize(ctx); err == nil {
				t.Fatal("damaged/unfinished ledger audit passed")
			}
			select {
			case <-tracker.failed:
			default:
				t.Fatal("evidence failure did not signal load stop")
			}
			r := &RunReport{Version: RunReportVersion, ExpectedSessions: 1, PeakReadySessions: 1, SteadySamples: 1, SteadyReadyRatio: 1, Delivery: tracker.report()}
			evaluateReport(r, RunConfig{MinimumReadyRatio: 1})
			if r.Pass || r.Delivery.Ledger.AuditComplete || r.Delivery.Ledger.Error == "" {
				t.Fatalf("false evidence PASS: %+v", r)
			}
		})
	}
}

func TestDeliveryLedgerQuotaAndUnknownMarkersDoNotAllocate(t *testing.T) {
	tracker := testDeliveryTrackerWithOptions(t, "quota", deliveryLedgerOptions{CacheRecords: 1, MaxBytes: 912})
	marker := beginTestDelivery(t, tracker, 0, 200, 1)
	tracker.finish(marker, sendAccepted, false, 1)
	before := tracker.report().Ledger
	tracker.observe(deliveryMessage{marker: tracker.marker(0, math.MaxUint64), id: 1, peerUserID: 100}, 1, deliveryLive, 1)
	if after := tracker.report(); after.Ledger.FileBytes != before.FileBytes || after.Ledger.CacheRecords != before.CacheRecords || after.UnmatchedMarkers != 1 {
		t.Fatalf("unknown marker allocated: %+v", after)
	}
	if err := tracker.begin(tracker.marker(0, 2), 0, 200, 2, time.Now(), time.Now()); err == nil {
		t.Fatal("quota overflow accepted")
	}
	if after := tracker.report(); after.AttemptedMessages != 1 || after.Ledger.FileBytes != before.FileBytes || after.Ledger.Error == "" {
		t.Fatalf("quota overflow lost evidence: %+v", after)
	}
	select {
	case <-tracker.failed:
	default:
		t.Fatal("quota did not stop load")
	}
}

func TestDeliveryLedgerRejectsOverwriteAndWriteFailures(t *testing.T) {
	tracker := testDeliveryTracker(t, "io")
	if _, err := newDeliveryTracker("io", []SessionRecord{{Index: 0, AccountIndex: 0, UserID: 100}}, tracker.ledger.dir, deliveryLedgerOptions{}); err == nil {
		t.Fatal("overwrote existing ledger")
	}
	if err := tracker.ledger.f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tracker.begin(tracker.marker(0, 1), 0, 200, 1, time.Now(), time.Now()); err == nil {
		t.Fatal("write error hidden")
	}
	if r := tracker.report(); r.AttemptedMessages != 0 || r.Ledger.Error == "" {
		t.Fatalf("fell back after I/O failure: %+v", r)
	}
	_ = tracker.finalize(context.Background())
}

func TestDeliveryLedgerReadCorruptionAfterEviction(t *testing.T) {
	tracker := testDeliveryTrackerWithOptions(t, "read", deliveryLedgerOptions{CacheRecords: 1})
	first := beginTestDelivery(t, tracker, 0, 200, 1)
	_ = beginTestDelivery(t, tracker, 0, 200, 2)
	if _, err := tracker.ledger.f.WriteAt([]byte{99}, 16); err != nil {
		t.Fatal(err)
	}
	tracker.observe(deliveryMessage{marker: first, id: 1, peerUserID: 100}, 1, deliveryLive, 1)
	if r := tracker.report(); r.CommittedMessages != 0 || r.Ledger.Error == "" {
		t.Fatalf("corrupt record became evidence: %+v", r)
	}
}

func TestDeliveryLedgerByteBudgetAlsoBoundsWideDeviceRecords(t *testing.T) {
	records := make([]SessionRecord, 3000)
	for index := range records {
		records[index] = SessionRecord{Index: index, DeviceIndex: index, UserID: 100}
	}
	tracker, err := newDeliveryTracker("wide", records, filepath.Join(t.TempDir(), "ledger"), deliveryLedgerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer tracker.close()
	for sequence := uint64(1); sequence <= 100; sequence++ {
		beginTestDelivery(t, tracker, 0, 100, sequence)
	}
	r := tracker.report().Ledger
	if r.Evictions == 0 || r.PeakCacheBytes > deliveryCacheBytes || r.PeakCacheRecords >= 100 {
		t.Fatalf("wide records escaped byte limit: %+v", r)
	}
}

func TestDeliveryQuantileBoundsAndRemoval(t *testing.T) {
	values := []time.Duration{0, 1, time.Microsecond, 63999, 64 * time.Microsecond, 50 * time.Millisecond, 30 * time.Second, 24 * time.Hour, time.Duration(math.MaxInt64)}
	for _, d := range values {
		upper := deliveryBucketUpper(deliveryBucket(d))
		if upper < d || float64(upper) > float64(d)*1.03125+float64(time.Microsecond) {
			t.Fatalf("invalid upper bound: d=%s upper=%s", d, upper)
		}
		var h deliveryHistogram
		h.add(d, 1)
		h.add(d, -1)
		if h.count != 0 || h.quantile(.99) != 0 {
			t.Fatal("histogram removal changed samples")
		}
	}
}

func TestDeliveryLedgerPreflightRejectsExcessWorkBeforeNetwork(t *testing.T) {
	tracker := testDeliveryTrackerWithOptions(t, "budget", deliveryLedgerOptions{MaxBytes: 3840})
	if err := tracker.preflight(RunConfig{MessageRate: 1000, Duration: time.Hour}); err == nil {
		t.Fatal("unaffordable fixed-rate plan accepted")
	}
	if err := tracker.preflight(RunConfig{MessageInterval: time.Second, Duration: time.Hour}); err == nil {
		t.Fatal("unaffordable periodic plan accepted")
	}
	stat, err := os.Stat(filepath.Join(tracker.ledger.dir, "records.bin"))
	if err != nil || stat.Size() != 0 {
		t.Fatal("preflight wrote message evidence")
	}
}

func TestDeliveryLedgerMarkerAliasesCannotProveCommit(t *testing.T) {
	tracker := testDeliveryTracker(t, "alias")
	marker := beginTestDelivery(t, tracker, 0, 200, 1)
	alias := strings.Replace(marker, "/0/1", "/00/01", 1)
	tracker.observe(deliveryMessage{marker: alias, id: 1, peerUserID: 100}, 1, deliveryLive, 1)
	if r := tracker.report(); r.CommittedMessages != 0 || r.UnmatchedMarkers != 1 || len(r.Anomalies) != 1 {
		t.Fatalf("alias became commit evidence: %+v", r)
	}
}

func TestDeliveryLedgerDetectsChangesDuringAudit(t *testing.T) {
	for _, part := range []string{"records.bin", "layout.json"} {
		t.Run(part, func(t *testing.T) {
			tracker := testDeliveryTracker(t, "concurrent-change")
			beginTestDelivery(t, tracker, 0, 200, 1)
			err := tracker.ledger.audit(context.Background(), func(*deliveryRecord) {
				path := filepath.Join(tracker.ledger.dir, part)
				if part == "records.bin" {
					if _, err := tracker.ledger.f.WriteAt([]byte{99}, 16); err != nil {
						t.Fatal(err)
					}
					now := time.Now().Add(time.Second)
					if err := os.Chtimes(path, now, now); err != nil {
						t.Fatal(err)
					}
				} else if err := os.WriteFile(path, []byte("{}"), 0o600); err != nil {
					t.Fatal(err)
				}
			})
			if err == nil {
				t.Fatal("concurrent ledger change passed audit")
			}
		})
	}
}
