package loadharness

import (
	"context"
	"os"
	"runtime"
	"testing"
)

// Explicit local fixture only; this is harness memory accounting, not a server
// capacity test. Keep the tracker reachable across GC to measure retained state.
func TestDeliveryAccountingMemoryGrowth(t *testing.T) {
	if os.Getenv("TELESRV_LOAD_MEMORY_PROBE") != "1" {
		t.Skip("opt-in bounded synthetic accounting probe")
	}
	tracker := testDeliveryTracker(t, "memory")
	var first uint64
	for sequence := uint64(1); sequence <= 300000; sequence++ {
		marker := beginTestDelivery(t, tracker, 0, 200, sequence)
		tracker.finish(marker, sendAccepted, false, 1)
		observeTestDelivery(tracker, marker, 1, deliveryLive)
		if sequence != 10000 && sequence != 100000 && sequence != 300000 {
			continue
		}
		report := tracker.report()
		if report.CommittedMessages != sequence || report.LiveDelivered != sequence {
			t.Fatalf("accounting mismatch at %d: %+v", sequence, report)
		}
		runtime.GC()
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		t.Logf("records=%d retained_heap_bytes=%d", sequence, memory.HeapAlloc)
		if first == 0 {
			first = memory.HeapAlloc
		}
		if sequence == 300000 && memory.HeapAlloc > first+16<<20 {
			t.Fatalf("retained heap grew with the whole run: first=%d final=%d", first, memory.HeapAlloc)
		}
	}
	if err := tracker.finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if ledger := tracker.report().Ledger; !ledger.AuditComplete || ledger.AuditRecords != 300000 || ledger.PeakCacheRecords > ledger.CacheLimitRecords || ledger.PeakCacheBytes > ledger.CacheLimitBytes {
		t.Fatalf("invalid bounded ledger audit: %+v", ledger)
	}
	runtime.KeepAlive(tracker)
}
