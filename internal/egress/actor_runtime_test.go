package egress

import (
	"context"
	"sync"
	"testing"
	"time"

	"telesrv/internal/edgecontrol"
	"telesrv/internal/store"
)

func TestDomainActorRunsCollidingDomainsWithoutIOHeadOfLineBlocking(t *testing.T) {
	const partitions = 2
	first, second := collidingActorStreams(t, partitions)
	executor := newDeliveryActorExecutor(nil, partitions, 8, 1<<20)
	started := make(chan int64, 2)
	release := make(chan struct{})
	executor.process = func(_ context.Context, window store.OutboxClaimWindow, _ *deliveryActorReservation) {
		started <- window.StreamID
		<-release
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wait sync.WaitGroup
	executor.run(ctx, &wait)
	defer func() {
		cancel()
		close(release)
		wait.Wait()
	}()

	if !executor.submit(ctx, actorTestWindow(first)) || !executor.submit(ctx, actorTestWindow(second)) {
		t.Fatal("submit colliding ordering domains")
	}
	seen := make(map[int64]bool, 2)
	for len(seen) < 2 {
		select {
		case streamID := <-started:
			seen[streamID] = true
		case <-time.After(time.Second):
			t.Fatalf("only %d colliding domains started; shard I/O is still synchronous", len(seen))
		}
	}
}

func TestDefaultDomainIOPoolFitsConcurrentWorstCaseProjectionReservations(t *testing.T) {
	executor := newDeliveryActorExecutor(nil, defaultDomainActorPartitions, defaultDomainActorMailbox, defaultDomainActorBytes)
	if executor.ioWorkers != defaultDomainIOWorkers {
		t.Fatalf("default I/O workers=%d want=%d", executor.ioWorkers, defaultDomainIOWorkers)
	}
	reserved := int64(executor.ioWorkers) * maxProjectedWindowRetainedBytes
	if reserved >= executor.maxBytes {
		t.Fatalf("projection reservations=%d exhaust actor budget=%d", reserved, executor.maxBytes)
	}
}

func TestDomainActorSerializesOneOrderingDomain(t *testing.T) {
	executor := newDeliveryActorExecutor(nil, 2, 8, 1<<20)
	started := make(chan struct{}, 2)
	releaseFirst := make(chan struct{})
	var call int
	var mu sync.Mutex
	executor.process = func(_ context.Context, _ store.OutboxClaimWindow, _ *deliveryActorReservation) {
		mu.Lock()
		call++
		current := call
		mu.Unlock()
		started <- struct{}{}
		if current == 1 {
			<-releaseFirst
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wait sync.WaitGroup
	executor.run(ctx, &wait)
	defer func() {
		cancel()
		wait.Wait()
	}()

	window := actorTestWindow(77)
	if !executor.submit(ctx, window) || !executor.submit(ctx, window) {
		t.Fatal("submit same ordering domain")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first ordering-domain work did not start")
	}
	select {
	case <-started:
		t.Fatal("same ordering domain executed concurrently")
	case <-time.After(30 * time.Millisecond):
	}
	close(releaseFirst)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("second ordering-domain work did not start after completion")
	}
}

func TestDomainActorCancellationReleasesInFlightAndPendingBudgets(t *testing.T) {
	executor := newDeliveryActorExecutor(nil, 2, 8, 1<<20)
	started := make(chan struct{}, 1)
	executor.process = func(ctx context.Context, _ store.OutboxClaimWindow, _ *deliveryActorReservation) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wait sync.WaitGroup
	executor.run(ctx, &wait)
	window := actorTestWindow(88)
	for i := 0; i < 3; i++ {
		if !executor.submit(ctx, window) {
			t.Fatalf("submit ordering-domain work %d", i)
		}
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("in-flight ordering-domain work did not start")
	}
	cancel()
	wait.Wait()
	if got := executor.usedTasks.Load(); got != 0 {
		t.Fatalf("reserved actor tasks after shutdown = %d, want 0", got)
	}
	if got := executor.usedBytes.Load(); got != 0 {
		t.Fatalf("reserved actor bytes after shutdown = %d, want 0", got)
	}
}

func TestDomainActorCancellationReleasesBufferedCompletionBudget(t *testing.T) {
	executor := newDeliveryActorExecutor(nil, 1, 1, 1<<20)
	executor.process = func(context.Context, store.OutboxClaimWindow, *deliveryActorReservation) {}
	window := actorTestWindow(99)
	bytes := claimWindowBytes(window)
	if !executor.reserve(bytes) {
		t.Fatal("reserve actor budget")
	}
	work := deliveryActorWork{
		window: window, bytes: bytes, reservation: newDeliveryActorReservation(executor, bytes),
	}
	domain := orderingDomainOf(window)
	executor.shards[0].domains[domain] = &deliveryDomainState{inFlight: &work}

	ctx, cancel := context.WithCancel(context.Background())
	var wait sync.WaitGroup
	wait.Add(1)
	go func() {
		defer wait.Done()
		executor.runIOWorker(ctx)
	}()
	executor.ioQueue <- deliveryActorIOTask{
		shard:  &executor.shards[0],
		domain: domain,
		work:   work,
	}

	deadline := time.Now().Add(time.Second)
	for len(executor.shards[0].completed) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(executor.shards[0].completed) == 0 {
		t.Fatal("I/O completion was not buffered")
	}
	cancel()
	executor.shards[0].releasePending()
	wait.Wait()
	if got := executor.usedTasks.Load(); got != 0 {
		t.Fatalf("reserved actor tasks after buffered completion cancellation = %d, want 0", got)
	}
	if got := executor.usedBytes.Load(); got != 0 {
		t.Fatalf("reserved actor bytes after buffered completion cancellation = %d, want 0", got)
	}
}

func TestDomainActorDynamicReservationTransfersIntoTerminalRelease(t *testing.T) {
	executor := newDeliveryActorExecutor(nil, 1, 4, 1<<20)
	retained := make(chan bool, 1)
	executor.process = func(_ context.Context, _ store.OutboxClaimWindow, reservation *deliveryActorReservation) {
		retained <- reservation.retain(4096)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wait sync.WaitGroup
	executor.run(ctx, &wait)
	if !executor.submit(ctx, actorTestWindow(109)) {
		cancel()
		wait.Wait()
		t.Fatal("submit actor work")
	}
	select {
	case ok := <-retained:
		if !ok {
			cancel()
			wait.Wait()
			t.Fatal("retain projected bytes")
		}
	case <-time.After(time.Second):
		cancel()
		wait.Wait()
		t.Fatal("actor work did not run")
	}
	deadline := time.Now().Add(time.Second)
	for (executor.usedTasks.Load() != 0 || executor.usedBytes.Load() != 0) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := executor.usedTasks.Load(); got != 0 {
		t.Fatalf("dynamic reservation left %d actor tasks", got)
	}
	if got := executor.usedBytes.Load(); got != 0 {
		t.Fatalf("dynamic reservation left %d actor bytes", got)
	}
	cancel()
	wait.Wait()
}

func TestDomainActorDynamicReservationFailsWithoutBlockingOrLeaking(t *testing.T) {
	window := actorTestWindow(110)
	base := claimWindowBytes(window)
	executor := newDeliveryActorExecutor(nil, 1, 1, base+31)
	if !executor.reserve(base) {
		t.Fatal("reserve base actor work")
	}
	work := deliveryActorWork{
		window: window, bytes: base, reservation: newDeliveryActorReservation(executor, base),
	}
	if work.reservation.retain(32) {
		t.Fatal("dynamic reservation crossed actor byte limit")
	}
	if got := executor.usedBytes.Load(); got != base {
		t.Fatalf("bytes after rejected dynamic reservation = %d, want base %d", got, base)
	}
	executor.releaseWork(work)
	if got := executor.usedTasks.Load(); got != 0 {
		t.Fatalf("tasks after release = %d", got)
	}
	if got := executor.usedBytes.Load(); got != 0 {
		t.Fatalf("bytes after release = %d", got)
	}
}

func TestClaimWindowBudgetIncludesPersistedTargetLedger(t *testing.T) {
	window := actorTestWindow(111)
	window.Owner = "egress-a/q1/w0"
	base := claimWindowBytes(window)
	window.Items[0].SourceInstanceID = "egress-a"
	window.Items[0].Targets = []store.OutboxAttemptTarget{{TargetInstanceID: "edge-a"}}
	withTarget := claimWindowBytes(window)
	minimumGrowth := int64(len("egress-a")+len("edge-a")) + claimAttemptTargetBytes
	if withTarget-base < minimumGrowth {
		t.Fatalf("target ledger budget growth=%d want at least %d", withTarget-base, minimumGrowth)
	}
}

func TestClaimWindowRejectsTargetLedgerBeyondV3Bound(t *testing.T) {
	window := store.OutboxClaimWindow{
		QueueKind: store.OutboxQueueDispatchPTS, StreamID: 112, Owner: "egress-a/q1/w0", LeaseFence: 7,
		Items: []store.OutboxClaimedItem{{
			Ref: store.OutboxAttemptRef{
				QueueKind: store.OutboxQueueDispatchPTS, StreamID: 112, ItemID: 1,
				Sequence: 1, LeaseFence: 7, Attempt: 1,
			},
			Payload: store.DispatchOutboxPayload{}, Targets: make([]store.OutboxAttemptTarget, edgecontrol.MaxDeliveryTargets+1),
		}},
	}
	if err := validateClaimWindow(window); err == nil {
		t.Fatal("persisted target ledger beyond v3 bound was accepted")
	}
}

func TestActorActiveWindowAdmissionIsFixedAndReleased(t *testing.T) {
	executor := newDeliveryActorExecutor(nil, defaultDomainIOWorkers, 8, defaultDomainActorBytes)
	if executor.maxWindows != defaultDomainIOWorkers*activeWindowsPerIOWorker {
		t.Fatalf("active window capacity=%d", executor.maxWindows)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if got := executor.reserveWindowSlots(ctx, executor.maxWindows); got != executor.maxWindows {
		t.Fatalf("reserved active windows=%d want %d", got, executor.maxWindows)
	}
	acquired := make(chan bool, 1)
	go func() { acquired <- executor.reserveWindowSlot(ctx) }()
	select {
	case <-acquired:
		t.Fatal("active window admission exceeded its fixed capacity")
	case <-time.After(20 * time.Millisecond):
	}
	executor.releaseWindowSlots(1)
	select {
	case ok := <-acquired:
		if !ok {
			t.Fatal("active window admission did not resume after release")
		}
	case <-time.After(time.Second):
		t.Fatal("active window waiter remained blocked after release")
	}
	executor.releaseWindowSlots(executor.maxWindows)
}

func TestDeliveryActorLaneLimitSharesActiveWindowsAcrossWorkers(t *testing.T) {
	if got := deliveryActorLaneLimit(128, 4, 16); got != 4 {
		t.Fatalf("lane limit=%d want 4", got)
	}
	if got := deliveryActorLaneLimit(2, 4, 16); got != 2 {
		t.Fatalf("configured smaller lane limit=%d want 2", got)
	}
	if got := deliveryActorLaneLimit(128, 32, 16); got != 1 {
		t.Fatalf("oversubscribed worker lane limit=%d want 1", got)
	}
}

func collidingActorStreams(t *testing.T, partitions uint64) (int64, int64) {
	t.Helper()
	byShard := make(map[uint64]int64)
	for streamID := int64(1); streamID < 1000; streamID++ {
		shard := orderingDomainHash(store.OutboxQueueDispatchPTS, streamID) % partitions
		if first, ok := byShard[shard]; ok {
			return first, streamID
		}
		byShard[shard] = streamID
	}
	t.Fatal("failed to find colliding ordering domains")
	return 0, 0
}

func actorTestWindow(streamID int64) store.OutboxClaimWindow {
	return store.OutboxClaimWindow{
		QueueKind: store.OutboxQueueDispatchPTS,
		StreamID:  streamID,
		Items: []store.OutboxClaimedItem{{
			Ref:     store.OutboxAttemptRef{QueueKind: store.OutboxQueueDispatchPTS, StreamID: streamID, ItemID: streamID},
			Payload: store.DispatchOutboxPayload{},
		}},
	}
}
