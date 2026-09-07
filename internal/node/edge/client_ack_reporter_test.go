package edge

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	"telesrv/internal/edgecontrol"
	"telesrv/internal/mtprotoedge"
)

type captureClientAckRemote struct {
	mu           sync.Mutex
	observations []edgecontrol.ClientAckObservation
	notify       chan struct{}
}

func (r *captureClientAckRemote) ReportClientAcks(_ context.Context, observations []edgecontrol.ClientAckObservation) ([]edgecontrol.ClientAckObservationResult, error) {
	r.mu.Lock()
	r.observations = append(r.observations, observations...)
	r.mu.Unlock()
	select {
	case r.notify <- struct{}{}:
	default:
	}
	results := make([]edgecontrol.ClientAckObservationResult, len(observations))
	for i := range results {
		results[i].Outcome = edgecontrol.ClientAckObservationApplied
	}
	return results, nil
}

func testDeliveryClientAck(item int64) mtprotoedge.DeliveryClientAck {
	return mtprotoedge.DeliveryClientAck{Tracking: edgecontrol.DeliveryTracking{BatchID: edgecontrol.BatchID{1}, CommandID: edgecontrol.CommandID{2}, SourceInstanceID: "egress-a", Ref: edgecontrol.DeliveryRef{Domain: edgecontrol.OrderingDomain{Kind: edgecontrol.QueueAccountPTS, StreamID: 42}, OutboxID: item, TargetUserID: 42, PTS: int(item), LeaseFence: 7, Attempt: 1}}, AuthKeyID: [8]byte{3}, SessionID: 4, ServerMsgID: 8, AckedAt: time.Now()}
}

func TestClientAckReporterIsNonBlockingAndExplicit(t *testing.T) {
	remote := &captureClientAckRemote{notify: make(chan struct{}, 1)}
	reporter := newDeliveryClientAckReporter(remote, "edge-b", time.Second, zaptest.NewLogger(t))
	go reporter.Run(context.Background())
	defer func() { reporter.Close(); reporter.Wait() }()
	if !reporter.TrySubmit(testDeliveryClientAck(1)) {
		t.Fatal("valid observation rejected")
	}
	select {
	case <-remote.notify:
	case <-time.After(time.Second):
		t.Fatal("observation not reported")
	}
	remote.mu.Lock()
	got := append([]edgecontrol.ClientAckObservation(nil), remote.observations...)
	remote.mu.Unlock()
	if len(got) != 1 || got[0].TargetInstanceID != "edge-b" || got[0].Tracking.Ref.Domain.Kind != edgecontrol.QueueAccountPTS {
		t.Fatalf("observations=%+v", got)
	}
}

func TestClientAckReporterBoundedMailboxNeverBlocks(t *testing.T) {
	reporter := newDeliveryClientAckReporter(&captureClientAckRemote{notify: make(chan struct{}, 1)}, "edge-b", time.Second, zaptest.NewLogger(t))
	for i := 0; i < clientAckShardCapacity; i++ {
		if !reporter.TrySubmit(testDeliveryClientAck(int64(i + 1))) {
			t.Fatalf("item %d rejected before capacity", i)
		}
	}
	started := time.Now()
	if reporter.TrySubmit(testDeliveryClientAck(clientAckShardCapacity + 1)) {
		t.Fatal("over-capacity observation accepted")
	}
	if elapsed := time.Since(started); elapsed > 10*time.Millisecond {
		t.Fatalf("TrySubmit blocked for %s", elapsed)
	}
}
