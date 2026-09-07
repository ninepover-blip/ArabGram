package loadharness

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/telegram"
	"github.com/iamxvbaba/td/tg"
)

func connectionTestWorker(t *testing.T) *loadWorker {
	t.Helper()
	events, err := newEventWriter(filepath.Join(t.TempDir(), "events.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { events.close() })
	w := &loadWorker{record: SessionRecord{Index: 1, UserID: 200}, events: events, metrics: newMetricSet(), counters: &harnessCounters{}, operationTimeout: time.Second}
	w.clientGeneration.Store(1)
	w.desired.Store(true)
	w.primaryState(1, telegram.ConnectionStateConnecting)
	return w
}

func TestBusinessAuditCannotPublishAcrossInternalReconnect(t *testing.T) {
	w := connectionTestWorker(t)
	w.primaryState(1, telegram.ConnectionStateReady)
	var reads int
	raw := tg.NewClient(arrivalInvoker(func(_ context.Context, in bin.Encoder, out bin.Decoder) error {
		reads++
		if reads == 1 {
			if _, ok := in.(*tg.UpdatesGetStateRequest); !ok {
				t.Fatal("first audit must establish a cursor")
			}
			*out.(*tg.UpdatesState) = tg.UpdatesState{Pts: 10, Date: 5}
			w.primaryState(1, telegram.ConnectionStateDisconnected)
			w.primaryState(1, telegram.ConnectionStateConnecting)
			w.primaryState(1, telegram.ConnectionStateReady)
			return nil
		}
		if w.businessReady.Load() {
			t.Fatal("stale response exposed readiness before replacement audit")
		}
		request, ok := in.(*tg.UpdatesGetDifferenceRequest)
		if !ok || request.Pts != 10 || request.Date != 5 {
			t.Fatal("replacement discarded initial audit cursor", in)
		}
		out.(*tg.UpdatesDifferenceBox).Difference = &tg.UpdatesDifferenceEmpty{Date: 6}
		return nil
	}))
	if err := w.activateBusiness(context.Background(), raw, make(chan struct{}, 1)); err != nil {
		t.Fatal(err)
	}
	if reads != 2 || !w.businessReady.Load() || w.primaryAttempt != 2 {
		t.Fatal("replacement was not audited exactly once", reads)
	}
	// A stale or duplicated ready notification cannot repeat the audit.
	if err := w.activateBusiness(context.Background(), raw, make(chan struct{}, 1)); err != nil || reads != 2 {
		t.Fatal("duplicate notification caused another read", err, reads)
	}
}

func TestBusinessAuditWaitsForFirstPrimaryReady(t *testing.T) {
	w := connectionTestWorker(t)
	var reads atomic.Int32
	raw := tg.NewClient(arrivalInvoker(func(_ context.Context, _ bin.Encoder, out bin.Decoder) error {
		reads.Add(1)
		*out.(*tg.UpdatesState) = tg.UpdatesState{Pts: 10, Date: 5}
		return nil
	}))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ready := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() { done <- w.activateBusiness(ctx, raw, ready) }()
	select {
	case <-done:
		t.Fatal("activation finished before transport ready")
	case <-time.After(10 * time.Millisecond):
	}
	if reads.Load() != 0 || w.businessReady.Load() {
		t.Fatal("work admitted before first Ready")
	}
	w.primaryState(1, telegram.ConnectionStateReady)
	ready <- struct{}{}
	if err := <-done; err != nil || reads.Load() != 1 || !w.businessReady.Load() {
		t.Fatal("late first Ready was lost", err, reads.Load())
	}
}

func TestBusinessAuditCancellationAndFailureCannotPublish(t *testing.T) {
	for _, mode := range []string{"canceled", "rpc_error", "offline", "disconnected"} {
		t.Run(mode, func(t *testing.T) {
			w := connectionTestWorker(t)
			w.primaryState(1, telegram.ConnectionStateReady)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "offline" {
				w.desired.Store(false)
			}
			if mode == "disconnected" {
				w.primaryState(1, telegram.ConnectionStateDisconnected)
			}
			if mode == "offline" || mode == "disconnected" {
				if w.markBusinessReady(ctx, 1) {
					t.Fatal("unavailable primary became ready")
				}
				return
			}
			raw := tg.NewClient(arrivalInvoker(func(_ context.Context, _ bin.Encoder, out bin.Decoder) error {
				if mode == "rpc_error" {
					return errors.New("explicit RPC failure")
				}
				*out.(*tg.UpdatesState) = tg.UpdatesState{Pts: 10, Date: 5}
				cancel()
				return nil
			}))
			if err := w.activateBusiness(ctx, raw, make(chan struct{})); err == nil || w.businessReady.Load() {
				t.Fatal("failed/canceled audit became ready", err)
			}
		})
	}
}

func TestBusinessAuditPreservesCursorThroughRepeatedReconnects(t *testing.T) {
	w := connectionTestWorker(t)
	w.primaryState(1, telegram.ConnectionStateReady)
	w.deliveryState.store(tg.UpdatesState{Pts: 10, Date: 1})
	reads := 0
	raw := tg.NewClient(arrivalInvoker(func(_ context.Context, in bin.Encoder, out bin.Decoder) error {
		if w.businessReady.Load() {
			t.Fatal("unreconciled primary became ready")
		}
		if in.(*tg.UpdatesGetDifferenceRequest).Date != reads+1 {
			t.Fatal("cursor rescan or skip")
		}
		reads++
		out.(*tg.UpdatesDifferenceBox).Difference = &tg.UpdatesDifferenceEmpty{Date: reads + 1}
		if reads < 3 {
			w.primaryState(1, telegram.ConnectionStateDisconnected)
			w.primaryState(1, telegram.ConnectionStateConnecting)
			w.primaryState(1, telegram.ConnectionStateReady)
		}
		return nil
	}))
	if err := w.activateBusiness(context.Background(), raw, make(chan struct{})); err != nil || reads != 3 || !w.businessReady.Load() {
		t.Fatal("repeated replacement audit failed", reads, err)
	}
}
