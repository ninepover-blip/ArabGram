package loadharness

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
)

func TestReconnectDifferenceUsesAuditCursorAndDoesNotRescanAtFinalize(t *testing.T) {
	tr := testDeliveryTracker(t, "cursor")
	marker := beginTestDelivery(t, tr, 0, 200, 1)
	tr.finish(marker, sendAccepted, false, 1)
	w := &loadWorker{record: SessionRecord{Index: 1, UserID: 200}, delivery: tr, metrics: newMetricSet(), counters: &harnessCounters{}, operationTimeout: time.Second}
	w.clientGeneration.Store(1)
	w.deliveryState.store(tg.UpdatesState{Pts: 10, Date: 1})
	w.lastUpdate.store(tg.UpdatesState{Pts: 999, Date: 99}) // unrelated getState probe
	calls := 0
	raw := tg.NewClient(arrivalInvoker(func(_ context.Context, in bin.Encoder, out bin.Decoder) error {
		req := in.(*tg.UpdatesGetDifferenceRequest)
		want := 10
		if calls > 0 {
			want = 11
		}
		if req.Pts != want {
			t.Fatalf("cursor=%d want %d", req.Pts, want)
		}
		if calls == 0 {
			out.(*tg.UpdatesDifferenceBox).Difference = &tg.UpdatesDifference{State: tg.UpdatesState{Pts: 11, Date: 2}, NewMessages: []tg.MessageClass{&tg.Message{ID: 1, Message: marker, PeerID: &tg.PeerUser{UserID: 100}}}}
		} else {
			out.(*tg.UpdatesDifferenceBox).Difference = &tg.UpdatesDifferenceEmpty{Date: 2}
		}
		calls++
		return nil
	}))
	if err := w.catchUp(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if err := w.catchUpDelivery(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if err := tr.finalize(context.Background()); err != nil {
		t.Fatal(err)
	}
	if d := tr.report(); calls != 3 || d.DifferenceRecovered != 1 || d.OnlineMissing != 1 || d.DuplicateObservations != 0 {
		t.Fatalf("calls=%d delivery=%+v", calls, d)
	}
}

func TestDifferenceInvalidPagesFailWithoutAdvancing(t *testing.T) {
	original := tg.UpdatesState{Pts: 10, Qts: 2, Date: 5, Seq: 3}
	cases := map[string]tg.UpdatesDifferenceClass{
		"too_long":              &tg.UpdatesDifferenceTooLong{Pts: 999},
		"backwards_pts":         &tg.UpdatesDifference{State: tg.UpdatesState{Pts: 9, Qts: 2, Date: 5, Seq: 3}},
		"backwards_qts":         &tg.UpdatesDifference{State: tg.UpdatesState{Pts: 10, Qts: 1, Date: 5, Seq: 3}},
		"empty_date_regression": &tg.UpdatesDifferenceEmpty{Date: 4, Seq: 3},
		"stationary_slice":      &tg.UpdatesDifferenceSlice{IntermediateState: original},
		"unknown":               nil,
	}
	for name, result := range cases {
		t.Run(name, func(t *testing.T) {
			tr := testDeliveryTracker(t, "invalid_cursor")
			failures := 0
			tr.onInvariant = func(string) { failures++ }
			w := &loadWorker{delivery: tr, metrics: newMetricSet(), counters: &harnessCounters{}, operationTimeout: time.Second}
			w.deliveryState.store(original)
			raw := tg.NewClient(arrivalInvoker(func(_ context.Context, _ bin.Encoder, out bin.Decoder) error {
				out.(*tg.UpdatesDifferenceBox).Difference = result
				return nil
			}))
			if err := w.catchUpDelivery(context.Background(), raw); err == nil {
				t.Fatal("bad page accepted")
			}
			state, _ := w.deliveryState.load()
			if state != original || failures != 1 || w.metrics.report()["updates.getDifference.delivery"].Errors != 1 {
				t.Fatalf("cursor changed or failure hidden: %+v", state)
			}
		})
	}
}

func TestDifferencePageLimitAndRPCErrorRemainVisible(t *testing.T) {
	for _, rpcError := range []bool{false, true} {
		tr := testDeliveryTracker(t, "limit")
		w := &loadWorker{delivery: tr, metrics: newMetricSet(), counters: &harnessCounters{}, operationTimeout: time.Second}
		w.deliveryState.store(tg.UpdatesState{Pts: 1, Date: 1})
		calls := 0
		raw := tg.NewClient(arrivalInvoker(func(_ context.Context, in bin.Encoder, out bin.Decoder) error {
			calls++
			if rpcError {
				return errors.New("explicit test RPC failure")
			}
			req := in.(*tg.UpdatesGetDifferenceRequest)
			out.(*tg.UpdatesDifferenceBox).Difference = &tg.UpdatesDifferenceSlice{IntermediateState: tg.UpdatesState{Pts: req.Pts + 1, Date: 1}}
			return nil
		}))
		if err := w.catchUpDelivery(context.Background(), raw); err == nil {
			t.Fatal("unbounded difference accepted")
		}
		want := 256
		if rpcError {
			want = 1
		}
		if calls != want || w.metrics.report()["updates.getDifference.delivery"].Errors != 1 {
			t.Fatalf("calls=%d", calls)
		}
	}
}

func TestFinalReconciliationCannotSkipUnavailableDeviceOrTimeout(t *testing.T) {
	for _, ready := range []bool{false, true} {
		w := &loadWorker{reconcile: make(chan chan struct{})}
		if ready {
			w.state.Store(workerReady)
			w.businessReady.Store(true)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		if err := reconcileDeliveries(ctx, []*loadWorker{w}); err == nil {
			t.Fatal("incomplete final reconciliation returned success")
		}
		cancel()
	}
	w := &loadWorker{reconcile: make(chan chan struct{})}
	w.state.Store(workerReady)
	w.businessReady.Store(true)
	joined := make(chan struct{})
	go func() { defer close(joined); done := <-w.reconcile; close(done) }()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := reconcileDeliveries(ctx, []*loadWorker{w}); err != nil {
		t.Fatal(err)
	}
	<-joined
}
