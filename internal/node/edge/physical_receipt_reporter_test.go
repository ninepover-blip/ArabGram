package edge

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"telesrv/internal/edgecontrol"
)

type capturePhysicalRemote struct {
	mu       sync.Mutex
	receipts []edgecontrol.PhysicalReceipt
	notify   chan struct{}
}

type cancelThenDrainPhysicalRemote struct {
	parent        context.Context
	mu            sync.Mutex
	receipts      []edgecontrol.PhysicalReceipt
	parentAttempt chan struct{}
}

func (r *cancelThenDrainPhysicalRemote) ReportPhysicalReceipts(ctx context.Context, receipts []edgecontrol.PhysicalReceipt) ([]edgecontrol.PhysicalReceiptResult, error) {
	if r.parent.Err() == nil {
		select {
		case r.parentAttempt <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	r.mu.Lock()
	r.receipts = append(r.receipts, receipts...)
	r.mu.Unlock()
	results := make([]edgecontrol.PhysicalReceiptResult, len(receipts))
	for i := range results {
		results[i].Outcome = edgecontrol.PhysicalReceiptApplied
	}
	return results, nil
}

func (r *capturePhysicalRemote) ReportPhysicalReceipts(_ context.Context, receipts []edgecontrol.PhysicalReceipt) ([]edgecontrol.PhysicalReceiptResult, error) {
	r.mu.Lock()
	r.receipts = append(r.receipts, receipts...)
	r.mu.Unlock()
	select {
	case r.notify <- struct{}{}:
	default:
	}
	results := make([]edgecontrol.PhysicalReceiptResult, len(receipts))
	for i := range results {
		results[i].Outcome = edgecontrol.PhysicalReceiptApplied
	}
	return results, nil
}

func TestPhysicalReceiptReporterReservationAndCommit(t *testing.T) {
	remote := &capturePhysicalRemote{notify: make(chan struct{}, 1)}
	reporter := newPhysicalReceiptReporter(remote, time.Second, zaptest.NewLogger(t))
	go reporter.Run(context.Background())
	defer func() { reporter.Close(); reporter.Wait() }()
	ref := edgecontrol.DeliveryRef{Domain: edgecontrol.OrderingDomain{Kind: edgecontrol.QueueAccountPTS, StreamID: 42}, OutboxID: 9, TargetUserID: 42, PTS: 3, LeaseFence: 7, Attempt: 1}
	reservation, err := reporter.ReservePhysicalReceipts(edgecontrol.BatchID{1}, "egress-a", "edge-b", edgecontrol.CommandID{2}, time.Now().Add(time.Second), []edgecontrol.DeliveryRef{ref})
	if err != nil {
		t.Fatal(err)
	}
	observed := time.Now()
	receipt := edgecontrol.PhysicalReceipt{BatchID: edgecontrol.BatchID{1}, CommandID: edgecontrol.CommandID{2}, SourceInstanceID: "egress-a", TargetInstanceID: "edge-b", Ref: ref, Outcome: edgecontrol.PhysicalWritten, EligibleSessions: 1, WrittenSessions: 1, FirstServerMsgID: 99, ObservedAt: observed}
	reservation.Commit(0, receipt)
	reservation.Commit(0, receipt)
	select {
	case <-remote.notify:
	case <-time.After(time.Second):
		t.Fatal("receipt was not reported")
	}
	remote.mu.Lock()
	got := append([]edgecontrol.PhysicalReceipt(nil), remote.receipts...)
	remote.mu.Unlock()
	if len(got) != 1 || !got[0].ObservedAt.Equal(observed) {
		t.Fatalf("receipts=%+v", got)
	}
}

func TestPhysicalReceiptReporterReservationIsBounded(t *testing.T) {
	reporter := newPhysicalReceiptReporter(&capturePhysicalRemote{notify: make(chan struct{}, 1)}, time.Second, zaptest.NewLogger(t))
	refs := make([]edgecontrol.DeliveryRef, physicalReceiptShardCapacity+1)
	for i := range refs {
		refs[i] = edgecontrol.DeliveryRef{Domain: edgecontrol.OrderingDomain{Kind: edgecontrol.QueueAccountPTS, StreamID: 42}, OutboxID: int64(i + 1), TargetUserID: 42, PTS: i + 1, LeaseFence: 1, Attempt: 1}
	}
	if _, err := reporter.ReservePhysicalReceipts(edgecontrol.BatchID{1}, "egress-a", "edge-b", edgecontrol.CommandID{2}, time.Now().Add(time.Second), refs); err != edgecontrol.ErrDeliveryOverloaded {
		t.Fatalf("err=%v", err)
	}
}

func TestPhysicalReceiptReporterFullReservationCommitsEverySlot(t *testing.T) {
	remote := &capturePhysicalRemote{notify: make(chan struct{}, 1)}
	reporter := newPhysicalReceiptReporter(remote, time.Second, zaptest.NewLogger(t))
	go reporter.Run(context.Background())
	defer func() { reporter.Close(); reporter.Wait() }()
	refs := make([]edgecontrol.DeliveryRef, physicalReceiptShardCapacity)
	for i := range refs {
		refs[i] = edgecontrol.DeliveryRef{
			Domain:   edgecontrol.OrderingDomain{Kind: edgecontrol.QueueAccountPTS, StreamID: 42},
			OutboxID: int64(i + 1), TargetUserID: 42, PTS: i + 1, LeaseFence: 1, Attempt: 1,
		}
	}
	reservation, err := reporter.ReservePhysicalReceipts(
		edgecontrol.BatchID{1}, "egress-a", "edge-b", edgecontrol.CommandID{2},
		time.Now().Add(time.Second), refs,
	)
	if err != nil {
		t.Fatal(err)
	}
	observed := time.Now()
	for i, ref := range refs {
		reservation.Commit(i, edgecontrol.PhysicalReceipt{
			BatchID: edgecontrol.BatchID{1}, CommandID: edgecontrol.CommandID{2},
			SourceInstanceID: "egress-a", TargetInstanceID: "edge-b", Ref: ref,
			Outcome: edgecontrol.PhysicalNoEligibleSessions, ObservedAt: observed,
		})
	}
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		remote.mu.Lock()
		count := len(remote.receipts)
		remote.mu.Unlock()
		if count == len(refs) {
			return
		}
		select {
		case <-remote.notify:
		case <-deadline.C:
			t.Fatalf("reported receipts = %d, want %d", count, len(refs))
		}
	}
}

func TestPhysicalReceiptReporterParentCancelTransfersPendingToShutdownDrain(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	remote := &cancelThenDrainPhysicalRemote{parent: parent, parentAttempt: make(chan struct{}, 1)}
	reporter := newPhysicalReceiptReporter(remote, 250*time.Millisecond, zaptest.NewLogger(t))
	go reporter.Run(parent)
	ref := edgecontrol.DeliveryRef{Domain: edgecontrol.OrderingDomain{Kind: edgecontrol.QueueAccountPTS, StreamID: 42}, OutboxID: 9, TargetUserID: 42, PTS: 3, LeaseFence: 7, Attempt: 1}
	reservation, err := reporter.ReservePhysicalReceipts(edgecontrol.BatchID{1}, "egress-a", "edge-b", edgecontrol.CommandID{2}, time.Now().Add(time.Second), []edgecontrol.DeliveryRef{ref})
	if err != nil {
		t.Fatal(err)
	}
	receipt := edgecontrol.PhysicalReceipt{BatchID: edgecontrol.BatchID{1}, CommandID: edgecontrol.CommandID{2}, SourceInstanceID: "egress-a", TargetInstanceID: "edge-b", Ref: ref, Outcome: edgecontrol.PhysicalWritten, EligibleSessions: 1, WrittenSessions: 1, FirstServerMsgID: 99, ObservedAt: time.Now()}
	reservation.Commit(0, receipt)
	select {
	case <-remote.parentAttempt:
	case <-time.After(time.Second):
		t.Fatal("parent-context report did not start")
	}
	cancel()
	reporter.Wait()
	remote.mu.Lock()
	defer remote.mu.Unlock()
	if len(remote.receipts) != 1 || remote.receipts[0].Ref != ref {
		t.Fatalf("shutdown receipts=%+v", remote.receipts)
	}
}
