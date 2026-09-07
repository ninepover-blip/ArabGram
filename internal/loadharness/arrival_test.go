package loadharness

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/tg"
)

type arrivalInvoker func(context.Context, bin.Encoder, bin.Decoder) error

func (f arrivalInvoker) Invoke(ctx context.Context, in bin.Encoder, out bin.Decoder) error {
	return f(ctx, in, out)
}

func TestSendTimingIncludesPreRPCQueueAndObservationBeforeReturn(t *testing.T) {
	tracker := testDeliveryTracker(t, "arrival")
	counters := &harnessCounters{}
	worker := &loadWorker{record: SessionRecord{Index: 0, UserID: 100}, target: SessionRecord{UserID: 200},
		counters: counters, metrics: newMetricSet("messages.sendMessage"), delivery: tracker, operationTimeout: time.Second}
	now := time.Now()
	arrival := messageArrival{plannedAt: now.Add(-700 * time.Millisecond), enqueuedAt: now.Add(-400 * time.Millisecond)}
	raw := tg.NewClient(arrivalInvoker(func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
		request := input.(*tg.MessagesSendMessageRequest)
		observeTestDelivery(tracker, request.Message, 1, deliveryLive)
		output.(*tg.UpdatesBox).Updates = testSendConfirmation(request, 1)
		return nil
	}))
	worker.sendMessage(context.Background(), raw, arrival)
	report := tracker.report()
	if report.Expected != 1 || report.LiveDelivered != 1 || report.PlannedArrivalSamples != 1 {
		t.Fatalf("delivery report: %+v", report)
	}
	if report.PlannedArrivalP99MS-report.E2EP99MS < 690 {
		t.Fatalf("planned arrival lost pre-RPC latency: %+v", report)
	}
	queue, total := counters.senderQueueWait.report(), counters.plannedToSend.report()
	if queue.Count != 1 || total.Count != 1 || queue.MeanMS < 390 || total.MeanMS-queue.MeanMS < 290 {
		t.Fatalf("queue=%+v total=%+v", queue, total)
	}
}

func TestFixedScheduleCountsRejectedArrivalsAndKeepsDeadline(t *testing.T) {
	worker := &loadWorker{sendQueue: make(chan messageArrival, 1)}
	worker.state.Store(workerReady)
	worker.businessReady.Store(true)
	counters := &harnessCounters{}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go runFixedMessageSchedule(ctx, &wg, 0, time.Second, 1000, []*loadWorker{worker}, []*loadWorker{worker}, counters, nil)
	wg.Wait()
	arrival := <-worker.sendQueue
	if arrival.plannedAt.IsZero() || arrival.enqueuedAt.Before(arrival.plannedAt) {
		t.Fatalf("arrival lost original timing: %+v", arrival)
	}
	scheduled, rejected := counters.messageScheduled.Load(), counters.messageQueueFull.Load()
	if scheduled < 2 || counters.messageEnqueued.Load() != 1 || rejected != scheduled-1 || counters.schedulerLag.report().Count != scheduled {
		t.Fatalf("scheduled=%d rejected=%d enqueued=%d lag=%+v", scheduled, rejected, counters.messageEnqueued.Load(), counters.schedulerLag.report())
	}
}

func TestOperationQuantileOverflowRemainsAnUpperBound(t *testing.T) {
	m := &operationMetrics{}
	m.observeDuration(90*time.Second, nil)
	m.observeDuration(120*time.Second, nil)
	report := m.report()
	if report.P99UpperMS != 120000 || report.P50UpperMS < 90000 {
		t.Fatalf("overflow latency understated: %+v", report)
	}
}

func TestFixedRateReportCannotPassWithoutArrivalTiming(t *testing.T) {
	report := &RunReport{ExpectedSessions: 1, PeakReadySessions: 1, SteadySamples: 1, SteadyReadyRatio: 1,
		MessageReadiness: OperationReport{Count: 1}, MessageReadyAt: time.Now(),
		MessageScheduled: 1, MessageEnqueued: 1, MessageCompleted: 1,
		Operations: map[string]OperationReport{"messages.sendMessage": {Count: 1}},
		Delivery:   DeliveryReport{Ledger: DeliveryLedgerReport{Version: deliveryLedgerVersion, AuditComplete: true, AuditRecords: 1}, AttemptedMessages: 1, InitialConfirmed: 1, CommittedMessages: 1, Expected: 1, Delivered: 1, LiveDelivered: 1}}
	cfg := RunConfig{MinimumReadyRatio: 1, MessageRate: 1}
	evaluateReport(report, cfg)
	if report.Pass {
		t.Fatal("missing planned-arrival timing passed")
	}
	report.Failures = nil
	report.MessageTiming = MessageTimingReport{SchedulerLag: OperationReport{Count: 1}, SenderQueueWait: OperationReport{Count: 1}, PlannedToSend: OperationReport{Count: 1}}
	report.Delivery.PlannedArrivalSamples = 1
	evaluateReport(report, cfg)
	if !report.Pass {
		t.Fatalf("complete timing rejected: %v", report.Failures)
	}
}

func TestFixedScheduleRequiresBusinessReadyOnExtraDevices(t *testing.T) {
	primary := &loadWorker{sendQueue: make(chan messageArrival, 1)}
	primary.state.Store(workerReady)
	primary.businessReady.Store(true)
	extra := &loadWorker{}
	extra.state.Store(workerReady) // Transport ready, but auth/update bootstrap is not complete.
	counters := &harnessCounters{}
	var wg sync.WaitGroup
	wg.Add(1)
	runFixedMessageSchedule(context.Background(), &wg, 0, 20*time.Millisecond, 100, []*loadWorker{primary}, []*loadWorker{primary, extra}, counters, nil)
	wg.Wait()
	if counters.messageScheduled.Load() != 0 || counters.messageReadiness.report().Timeouts != 1 || !counters.messageReadyAt.IsZero() {
		t.Fatalf("unready extra device passed: arrivals=%d readiness=%+v", counters.messageScheduled.Load(), counters.messageReadiness.report())
	}
	extra.businessReady.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := waitMessageReadiness(ctx, []*loadWorker{primary, extra}, time.Second); err != nil {
		t.Fatal(err)
	}
	extra.state.Store(workerDisconnected)
	if err := waitMessageReadiness(ctx, []*loadWorker{primary, extra}, 20*time.Millisecond); err == nil {
		t.Fatal("disconnected device passed readiness")
	}
}

func TestFixedScheduleCannotSendAfterAnExpiredDeadline(t *testing.T) {
	worker := &loadWorker{sendQueue: make(chan messageArrival, 1)}
	worker.state.Store(workerReady)
	worker.businessReady.Store(true)
	counters := &harnessCounters{}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	runFixedMessageSchedule(ctx, &wg, 0, time.Second, 100, []*loadWorker{worker}, []*loadWorker{worker}, counters, nil)
	wg.Wait()
	if counters.messageScheduled.Load() != 0 {
		t.Fatal("arrival admitted after deadline")
	}
}
