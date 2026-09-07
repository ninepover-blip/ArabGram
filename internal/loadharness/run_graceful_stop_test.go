package loadharness

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/telegram"
	"github.com/iamxvbaba/td/tg"
)

func gracefulTestWorker(t *testing.T) *loadWorker {
	t.Helper()
	events, err := newEventWriter(filepath.Join(t.TempDir(), "events.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { events.close() })
	return &loadWorker{
		record:           SessionRecord{Index: 0},
		events:           events,
		metrics:          newMetricSet("test.rpc"),
		counters:         &harnessCounters{},
		signal:           make(chan struct{}, 1),
		producerStop:     make(chan struct{}),
		producerDrained:  make(chan struct{}),
		workStop:         make(chan struct{}),
		workDrained:      make(chan struct{}),
		gracefulStop:     make(chan struct{}),
		operationTimeout: 100 * time.Millisecond,
	}
}

func TestSupervisorTwoStageStopIncludesInFlightResultBeforeDisconnect(t *testing.T) {
	worker := gracefulTestWorker(t)
	started := make(chan struct{})
	release := make(chan struct{})
	var canceled atomic.Bool
	worker.runClientOverride = func(ctx context.Context) error {
		if !worker.beginWork() {
			return errors.New("initial work was not admitted")
		}
		close(started)
		<-worker.workStop
		select {
		case <-ctx.Done():
			canceled.Store(true)
			worker.endWork()
			return ctx.Err()
		case <-release:
			worker.metrics.observe("test.rpc", time.Now(), nil)
			worker.endWork()
		}
		select {
		case <-ctx.Done():
			canceled.Store(true)
			return ctx.Err()
		case <-worker.gracefulStop:
			return worker.plannedGracefulDisconnect(worker.clientGeneration.Load())
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	worker.setOnline(true)
	go worker.supervise(ctx, &wg)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("client did not start")
	}

	worker.requestWorkStop()
	if worker.beginWork() {
		worker.endWork()
		t.Fatal("work was admitted after the stop fence")
	}
	drained := make(chan error, 1)
	go func() { drained <- waitWorkerWorkDrain(context.Background(), []*loadWorker{worker}) }()
	select {
	case <-drained:
		t.Fatal("work drain completed before the admitted RPC returned")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case err := <-drained:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("work drain did not observe the completed RPC")
	}
	cut := worker.metrics.freeze()
	if cut["test.rpc"].Count != 1 || cut["test.rpc"].Errors != 0 || cut["test.rpc"].Canceled != 0 {
		t.Fatal("in-flight result missing from workload-end cut", cut)
	}

	worker.requestGracefulStop()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("supervisor did not finish graceful disconnect")
	}
	if canceled.Load() {
		t.Fatal("two-stage stop canceled the admitted RPC or parked client")
	}
	if worker.counters.fatalErrors.Load() != 0 || worker.state.Load() != workerStopped {
		t.Fatal("graceful stop changed fatal or terminal state accounting")
	}
	worker.primaryState(worker.clientGeneration.Load(), telegram.ConnectionStateDisconnected)
	if worker.counters.disconnects.Load() != 0 {
		t.Fatal("generation-fenced planned disconnect was counted as workload loss")
	}
}

func TestSupervisorGracefulErrorIsNotHidden(t *testing.T) {
	worker := gracefulTestWorker(t)
	started := make(chan struct{})
	worker.runClientOverride = func(context.Context) error {
		close(started)
		<-worker.gracefulStop
		return errors.New("real close-boundary failure")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	worker.setOnline(true)
	go worker.supervise(ctx, &wg)
	<-started
	worker.requestGracefulStop()
	wg.Wait()
	if worker.counters.fatalErrors.Load() != 1 {
		t.Fatal("close-boundary client error was hidden")
	}
}

func TestSupervisorClientJoinTimeoutCannotPublishTerminalState(t *testing.T) {
	worker := gracefulTestWorker(t)
	workCtx, safety := newLoadSafety(context.Background())
	defer safety.cancel()
	worker.counters.safety = safety
	worker.clientCloseTimeout = 20 * time.Millisecond
	worker.clientJoinTimeout = 20 * time.Millisecond
	started := make(chan struct{})
	release := make(chan struct{})
	worker.runClientOverride = func(context.Context) error {
		close(started)
		<-release // Deliberately ignores both graceful stop and cancellation.
		return nil
	}
	var wg sync.WaitGroup
	wg.Add(1)
	worker.setOnline(true)
	go worker.supervise(workCtx, &wg)
	<-started
	start := time.Now()
	worker.requestGracefulStop()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		t.Fatal("supervisor published terminal state before the client joined")
	case <-time.After(80 * time.Millisecond):
	}
	if elapsed := time.Since(start); elapsed < 2*worker.clientJoinTimeout {
		t.Fatal("test did not cross both close and cancel-join deadlines", elapsed)
	}
	report := safety.report()
	if report.Source != "lifecycle" || report.Class != "client_graceful_close_timeout" {
		t.Fatal("graceful timeout did not fail closed", report)
	}
	if !report.WorkersStoppedAt.IsZero() {
		t.Fatal("unjoined generation was reported as stopped", report)
	}
	if worker.counters.unjoinedClients.Load() != 1 {
		t.Fatal("unjoined generation was not counted explicitly")
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("supervisor did not finish after the real client joined")
	}
}

func TestClientBoundaryPrefersBufferedDoneOverReadyTimeout(t *testing.T) {
	for i := 0; i < 1000; i++ {
		done := make(chan error, 1)
		timeout := make(chan time.Time, 1)
		want := errors.New("joined")
		done <- want
		timeout <- time.Now()
		got, result := waitClientBoundary(nil, done, timeout)
		if result != clientBoundaryJoined || !errors.Is(got, want) {
			t.Fatalf("ready timeout beat buffered completion at iteration %d: %v %v", i, result, got)
		}
	}
}

func TestClientBoundaryPrefersContextOverReadyTimeout(t *testing.T) {
	for i := 0; i < 1000; i++ {
		ctxDone := make(chan struct{})
		timeout := make(chan time.Time, 1)
		close(ctxDone)
		timeout <- time.Now()
		got, result := waitClientBoundary(ctxDone, make(chan error), timeout)
		if result != clientBoundaryContext || got != nil {
			t.Fatalf("ready timeout beat parent cancellation at iteration %d: %v %v", i, result, got)
		}
	}
}

func TestClientBoundaryPrefersDoneOverContextAndTimeout(t *testing.T) {
	for i := 0; i < 1000; i++ {
		ctxDone := make(chan struct{})
		done := make(chan error, 1)
		timeout := make(chan time.Time, 1)
		want := errors.New("joined")
		close(ctxDone)
		done <- want
		timeout <- time.Now()
		got, result := waitClientBoundary(ctxDone, done, timeout)
		if result != clientBoundaryJoined || !errors.Is(got, want) {
			t.Fatalf("completion lost priority at iteration %d: %v %v", i, result, got)
		}
	}
}

func TestPlannedDisconnectFenceIsGenerationExact(t *testing.T) {
	worker := gracefulTestWorker(t)
	worker.clientGeneration.Store(1)
	worker.plannedDisconnectGeneration.Store(1)
	worker.primaryState(1, telegram.ConnectionStateDisconnected)
	if worker.counters.disconnects.Load() != 0 {
		t.Fatal("planned generation was counted as disconnected")
	}
	worker.clientGeneration.Store(2)
	worker.primaryState(2, telegram.ConnectionStateDisconnected)
	if worker.counters.disconnects.Load() != 1 {
		t.Fatal("a different generation was hidden by the planned disconnect fence")
	}
}

func TestWorkStopDrainsWholeAdmittedSendRetry(t *testing.T) {
	tracker := testDeliveryTracker(t, "graceful_send_retry")
	worker := gracefulTestWorker(t)
	worker.record = SessionRecord{Index: 0, UserID: 100}
	worker.target = SessionRecord{UserID: 200, AccessHash: 22}
	worker.delivery = tracker
	worker.metrics = newMetricSet("messages.sendMessage", "messages.sendMessage.retry")
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	calls := 0
	raw := tg.NewClient(arrivalInvoker(func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
		request := input.(*tg.MessagesSendMessageRequest)
		calls++
		if calls == 1 {
			close(firstStarted)
			<-releaseFirst
			return context.DeadlineExceeded
		}
		output.(*tg.UpdatesBox).Updates = testSendConfirmation(request, 7)
		return nil
	}))
	done := make(chan struct{})
	go func() {
		if !worker.beginWork() {
			close(done)
			return
		}
		worker.sendMessage(context.Background(), raw, messageArrival{})
		worker.endWork()
		close(done)
	}()
	<-firstStarted
	worker.requestWorkStop()
	drain := make(chan error, 1)
	go func() { drain <- waitWorkerWorkDrain(context.Background(), []*loadWorker{worker}) }()
	select {
	case <-drain:
		t.Fatal("send drained before its admitted retry chain completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseFirst)
	<-done
	if err := <-drain; err != nil {
		t.Fatal(err)
	}
	metrics := worker.metrics.report()
	if calls != 2 || metrics["messages.sendMessage"].Timeouts != 1 || metrics["messages.sendMessage.retry"].Count != 1 {
		t.Fatal("work stop truncated or hid the admitted send retry", calls, metrics)
	}
}

func TestWorkStopDrainsWholeAdmittedDifferencePagination(t *testing.T) {
	tracker := testDeliveryTracker(t, "graceful_difference")
	worker := gracefulTestWorker(t)
	worker.delivery = tracker
	worker.deliveryState.store(tg.UpdatesState{Pts: 1, Date: 1})
	worker.metrics = newMetricSet("updates.getDifference.delivery")
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	calls := 0
	raw := tg.NewClient(arrivalInvoker(func(_ context.Context, input bin.Encoder, output bin.Decoder) error {
		request := input.(*tg.UpdatesGetDifferenceRequest)
		calls++
		if calls == 1 {
			close(firstStarted)
			<-releaseFirst
			output.(*tg.UpdatesDifferenceBox).Difference = &tg.UpdatesDifferenceSlice{IntermediateState: tg.UpdatesState{Pts: request.Pts + 1, Date: 1}}
			return nil
		}
		output.(*tg.UpdatesDifferenceBox).Difference = &tg.UpdatesDifferenceEmpty{Date: 1}
		return nil
	}))
	done := make(chan error, 1)
	go func() {
		if !worker.beginWork() {
			done <- errors.New("difference was not admitted")
			return
		}
		err := worker.catchUpDelivery(context.Background(), raw)
		worker.endWork()
		done <- err
	}()
	<-firstStarted
	worker.requestWorkStop()
	drain := make(chan error, 1)
	go func() { drain <- waitWorkerWorkDrain(context.Background(), []*loadWorker{worker}) }()
	select {
	case <-drain:
		t.Fatal("difference drained before its admitted page chain completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseFirst)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-drain; err != nil {
		t.Fatal(err)
	}
	if calls != 2 || worker.metrics.report()["updates.getDifference.delivery"].Count != 2 {
		t.Fatal("work stop truncated difference pagination", calls, worker.metrics.report())
	}
}

func TestWorkStopBeforeSupervisorPreventsGenerationCreation(t *testing.T) {
	worker := gracefulTestWorker(t)
	worker.setOnline(true)
	worker.requestWorkStop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go worker.supervise(ctx, &wg)
	worker.requestGracefulStop()
	wg.Wait()
	if worker.clientGeneration.Load() != 0 || worker.counters.connectionAttempts.Load() != 0 {
		t.Fatal("generation crossed the closed work-admission fence")
	}
}

func TestProducerStopDrainsTrafficAndKeepsObservationAdmission(t *testing.T) {
	worker := gracefulTestWorker(t)
	if !worker.beginProducerWork() {
		t.Fatal("initial producer work was not admitted")
	}
	worker.requestProducerStop()
	select {
	case <-worker.producerDrained:
		t.Fatal("producer drain completed before admitted traffic returned")
	default:
	}
	if worker.beginProducerWork() {
		worker.endProducerWork()
		t.Fatal("traffic was admitted after the producer fence")
	}
	// Update callbacks, queued fixed-rate sends, and explicit reconciliation
	// remain admitted until the later workStop fence.
	if !worker.beginWork() {
		t.Fatal("producer fence also stopped observation/reconciliation")
	}
	worker.endProducerWork()
	select {
	case <-worker.producerDrained:
	case <-time.After(time.Second):
		t.Fatal("producer drain did not close after admitted traffic returned")
	}
	worker.endWork()
	worker.requestWorkStop()
	select {
	case <-worker.workDrained:
	case <-time.After(time.Second):
		t.Fatal("observation work did not drain at the final fence")
	}
}

func TestProducerStopBeforeSupervisorPreventsGenerationCreation(t *testing.T) {
	worker := gracefulTestWorker(t)
	worker.setOnline(true)
	worker.requestProducerStop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go worker.supervise(ctx, &wg)
	worker.requestGracefulStop()
	wg.Wait()
	if worker.clientGeneration.Load() != 0 || worker.counters.connectionAttempts.Load() != 0 {
		t.Fatal("generation crossed the closed producer-admission fence")
	}
}

func TestProducerDeadlinePreventsLateAdmissionBeforeStopSignal(t *testing.T) {
	worker := gracefulTestWorker(t)
	worker.producerDeadline = time.Now().Add(-time.Millisecond)
	if worker.beginProducerWork() {
		worker.endProducerWork()
		t.Fatal("producer work crossed the immutable scheduled deadline")
	}
	if !worker.beginWork() {
		t.Fatal("producer deadline also stopped update/reconciliation admission")
	}
	worker.endWork()
	worker.setOnline(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go worker.supervise(ctx, &wg)
	worker.requestGracefulStop()
	wg.Wait()
	if worker.clientGeneration.Load() != 0 {
		t.Fatal("generation crossed the immutable scheduled deadline")
	}
}
