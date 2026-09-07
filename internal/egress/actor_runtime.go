package egress

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/edgecontrol"
	"telesrv/internal/store"
)

const (
	defaultDomainActorPartitions = 256
	defaultDomainActorMailbox    = 128
	defaultDomainActorBytes      = int64(3 << 30)
	// Actor partitions distribute ordering domains; they are not a database
	// concurrency target. One hundred twenty-eight callers feed the fixed
	// projection actors and the single set-based mutation actor; they do not become
	// independent event or mutation writers. The 3 GiB reservation ceiling covers every
	// caller holding one legal 16 MiB projection window plus bounded claim/plan
	// metadata without preallocating that memory.
	defaultDomainIOWorkers      = 128
	activeWindowsPerIOWorker    = 4
	ptsClaimByteEstimate        = 64 << 10
	claimItemRetainedBytes      = int64(256)
	claimAttemptTargetBytes     = int64(64)
	deliveryReservationReleased = uint64(1) << 63
	deliveryReservationByteMask = deliveryReservationReleased - 1
	maxClaimWindowRetainedBytes = int64(edgecontrol.MaxDeliveryBatchBytes) +
		int64(edgecontrol.MaxDeliveryBatchItems)*claimItemRetainedBytes +
		int64(edgecontrol.MaxDeliveryBatchItems)*int64(edgecontrol.MaxDeliveryTargets)*
			(claimAttemptTargetBytes+int64(edgecontrol.MaxDeliveryInstanceIDBytes))
	minimumProductionActorBytes = maxClaimWindowRetainedBytes + maxProjectedWindowRetainedBytes +
		maxDeliveryPlanRetainedBytes + maxAttemptTargetLedgerRetainedBytes + maxReplayIndexRetainedBytes
)

type deliveryActorWork struct {
	window      store.OutboxClaimWindow
	bytes       int64
	reservation *deliveryActorReservation
}

type deliveryActorReservation struct {
	owner      *deliveryActorExecutor
	state      atomic.Uint64
	windowSlot bool
}

type deliveryOrderingDomain struct {
	queueKind store.OutboxQueueKind
	streamID  int64
}

type deliveryDomainState struct {
	pending  []deliveryActorWork
	inFlight *deliveryActorWork
}

type deliveryActorCompletion struct {
	domain deliveryOrderingDomain
}

type deliveryActorShard struct {
	queue     chan deliveryActorWork
	completed chan deliveryActorCompletion
	domains   map[deliveryOrderingDomain]*deliveryDomainState
	executor  *deliveryActorExecutor
}

type deliveryActorIOTask struct {
	shard  *deliveryActorShard
	domain deliveryOrderingDomain
	work   deliveryActorWork
}

type deliveryActorExecutor struct {
	coordinator *deliveryCoordinator
	process     func(context.Context, store.OutboxClaimWindow, *deliveryActorReservation)
	shards      []deliveryActorShard
	ioQueue     chan deliveryActorIOTask
	ioWorkers   int
	maxWindows  int
	windowSlots chan struct{}
	maxBytes    int64
	usedBytes   atomic.Int64
	maxTasks    int64
	usedTasks   atomic.Int64
	capacity    chan struct{}
	lifecycle   sync.RWMutex
	stopped     bool
}

func newDeliveryActorExecutor(coordinator *deliveryCoordinator, partitions, mailbox int, maxBytes int64) *deliveryActorExecutor {
	if partitions <= 0 {
		partitions = defaultDomainActorPartitions
	}
	if mailbox <= 0 {
		mailbox = defaultDomainActorMailbox
	}
	if maxBytes <= 0 {
		maxBytes = defaultDomainActorBytes
	}
	maxTasks := int64(partitions) * int64(mailbox)
	if maxTasks <= 0 {
		maxTasks = 1
	}
	ioWorkers := partitions
	if ioWorkers > defaultDomainIOWorkers {
		ioWorkers = defaultDomainIOWorkers
	}
	if ioWorkers <= 0 {
		ioWorkers = 1
	}
	executor := &deliveryActorExecutor{
		coordinator: coordinator, shards: make([]deliveryActorShard, partitions), maxBytes: maxBytes,
		maxTasks: maxTasks, ioWorkers: ioWorkers,
		ioQueue:  make(chan deliveryActorIOTask, int(maxTasks)),
		capacity: make(chan struct{}, 1),
	}
	executor.maxWindows = ioWorkers * activeWindowsPerIOWorker
	executor.windowSlots = make(chan struct{}, executor.maxWindows)
	if coordinator != nil {
		executor.process = coordinator.processWindowBudgeted
	}
	for i := range executor.shards {
		executor.shards[i] = deliveryActorShard{
			queue: make(chan deliveryActorWork, mailbox),
			// Completion advances per-domain serialization and releases the shared
			// reservation. A single slot decouples the worker from a shard briefly
			// submitting another domain to ioQueue; the idempotent reservation token
			// makes cancellation safe without making the runtime unbounded.
			completed: make(chan deliveryActorCompletion, 1),
			domains:   make(map[deliveryOrderingDomain]*deliveryDomainState),
			executor:  executor,
		}
	}
	return executor
}

func (e *deliveryActorExecutor) run(ctx context.Context, wait *sync.WaitGroup) {
	for i := 0; i < e.ioWorkers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			e.runIOWorker(ctx)
		}()
	}
	for i := range e.shards {
		wait.Add(1)
		go func(shard *deliveryActorShard) {
			defer wait.Done()
			shard.run(ctx)
		}(&e.shards[i])
	}
}

func (s *deliveryActorShard) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			s.executor.stopAdmissions()
			s.releasePending()
			return
		case work := <-s.queue:
			domain := orderingDomainOf(work.window)
			state := s.domains[domain]
			if state == nil {
				state = &deliveryDomainState{}
				s.domains[domain] = state
			}
			state.pending = append(state.pending, work)
			s.dispatchNext(ctx, domain, state)
		case completion := <-s.completed:
			state := s.domains[completion.domain]
			if state == nil || state.inFlight == nil {
				continue
			}
			s.executor.releaseWork(*state.inFlight)
			state.inFlight = nil
			s.dispatchNext(ctx, completion.domain, state)
			if state.inFlight == nil && len(state.pending) == 0 {
				delete(s.domains, completion.domain)
			}
		}
	}
}

func (s *deliveryActorShard) dispatchNext(ctx context.Context, domain deliveryOrderingDomain, state *deliveryDomainState) {
	if state == nil || state.inFlight != nil || len(state.pending) == 0 {
		return
	}
	work := state.pending[0]
	select {
	case s.executor.ioQueue <- deliveryActorIOTask{shard: s, domain: domain, work: work}:
		state.pending[0] = deliveryActorWork{}
		state.pending = state.pending[1:]
		state.inFlight = &work
	case <-ctx.Done():
	}
}

func (s *deliveryActorShard) releasePending() {
	// Cancellation may race a worker publishing a buffered completion. Every
	// copy shares an idempotent reservation token, so the shard can release all
	// work it still references while the worker safely attempts the same thing.
	for _, state := range s.domains {
		if state.inFlight != nil {
			s.executor.releaseWork(*state.inFlight)
			state.inFlight = nil
		}
		for _, work := range state.pending {
			s.executor.releaseWork(work)
		}
		state.pending = nil
	}
	for {
		select {
		case work := <-s.queue:
			s.executor.releaseWork(work)
		default:
			return
		}
	}
}

func (e *deliveryActorExecutor) runIOWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			e.drainCanceledIOTasks()
			return
		case task := <-e.ioQueue:
			if e.process != nil {
				e.process(ctx, task.work.window, task.work.reservation)
			}
			if ctx.Err() != nil {
				e.releaseWork(task.work)
				continue
			}
			select {
			case task.shard.completed <- deliveryActorCompletion{domain: task.domain}:
			case <-ctx.Done():
				e.releaseWork(task.work)
			}
		}
	}
}

func (e *deliveryActorExecutor) drainCanceledIOTasks() {
	for {
		select {
		case task := <-e.ioQueue:
			e.releaseWork(task.work)
		default:
			return
		}
	}
}

func orderingDomainOf(window store.OutboxClaimWindow) deliveryOrderingDomain {
	return deliveryOrderingDomain{queueKind: window.QueueKind, streamID: window.StreamID}
}

func (e *deliveryActorExecutor) submit(ctx context.Context, window store.OutboxClaimWindow) bool {
	if !e.reserveWindowSlot(ctx) {
		return false
	}
	return e.submitReserved(ctx, window)
}

func (e *deliveryActorExecutor) submitReserved(ctx context.Context, window store.OutboxClaimWindow) bool {
	if e == nil || len(e.shards) == 0 || len(window.Items) == 0 {
		if e != nil {
			e.releaseWindowSlots(1)
		}
		return false
	}
	e.lifecycle.RLock()
	defer e.lifecycle.RUnlock()
	if e.stopped || ctx.Err() != nil {
		e.releaseWindowSlots(1)
		return false
	}
	bytes := claimWindowBytes(window)
	if !e.reserveContext(ctx, bytes) {
		e.releaseWindowSlots(1)
		return false
	}
	reservation := newDeliveryActorReservation(e, bytes)
	reservation.windowSlot = true
	work := deliveryActorWork{window: window, bytes: bytes, reservation: reservation}
	index := orderingDomainHash(window.QueueKind, window.StreamID) % uint64(len(e.shards))
	select {
	case e.shards[index].queue <- work:
		return true
	case <-ctx.Done():
		e.releaseWork(work)
		return false
	}
}

func (e *deliveryActorExecutor) reserveWindowSlot(ctx context.Context) bool {
	if e == nil || e.maxWindows <= 0 || e.windowSlots == nil {
		return false
	}
	select {
	case e.windowSlots <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (e *deliveryActorExecutor) reserveWindowSlots(ctx context.Context, limit int) int {
	if limit <= 0 || !e.reserveWindowSlot(ctx) {
		return 0
	}
	reserved := 1
	for reserved < limit {
		select {
		case e.windowSlots <- struct{}{}:
			reserved++
		default:
			return reserved
		}
	}
	return reserved
}

func (e *deliveryActorExecutor) releaseWindowSlots(count int) {
	if e == nil || e.windowSlots == nil {
		return
	}
	for i := 0; i < count; i++ {
		select {
		case <-e.windowSlots:
		default:
			panic("egress: active window reservation underflow")
		}
	}
}

func (e *deliveryActorExecutor) stopAdmissions() {
	if e == nil {
		return
	}
	e.lifecycle.Lock()
	e.stopped = true
	e.lifecycle.Unlock()
}

func (e *deliveryActorExecutor) reserveContext(ctx context.Context, bytes int64) bool {
	if bytes <= 0 || bytes > e.maxBytes {
		return false
	}
	for {
		if e.reserve(bytes) {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-e.capacity:
		}
	}
}

func (e *deliveryActorExecutor) release(bytes int64) {
	if e == nil || bytes <= 0 {
		return
	}
	e.releaseBytes(bytes)
	e.usedTasks.Add(-1)
	select {
	case e.capacity <- struct{}{}:
	default:
	}
}

func (e *deliveryActorExecutor) releaseBytes(bytes int64) {
	if e == nil || bytes <= 0 {
		return
	}
	e.usedBytes.Add(-bytes)
	select {
	case e.capacity <- struct{}{}:
	default:
	}
}

func (e *deliveryActorExecutor) releaseWork(work deliveryActorWork) {
	if work.reservation == nil {
		e.release(work.bytes)
		return
	}
	if bytes, ok := work.reservation.take(); ok {
		e.release(bytes)
		if work.reservation.windowSlot {
			e.releaseWindowSlots(1)
		}
	}
}

func (e *deliveryActorExecutor) reserve(bytes int64) bool {
	if bytes <= 0 || bytes > e.maxBytes {
		return false
	}
	for {
		used := e.usedTasks.Load()
		if used >= e.maxTasks {
			return false
		}
		if e.usedTasks.CompareAndSwap(used, used+1) {
			break
		}
	}
	if !e.reserveBytes(bytes) {
		e.usedTasks.Add(-1)
		return false
	}
	return true
}

func (e *deliveryActorExecutor) reserveBytes(bytes int64) bool {
	if e == nil || bytes <= 0 || bytes > e.maxBytes {
		return false
	}
	for {
		used := e.usedBytes.Load()
		if used > e.maxBytes-bytes {
			return false
		}
		if e.usedBytes.CompareAndSwap(used, used+bytes) {
			return true
		}
	}
}

func newDeliveryActorReservation(owner *deliveryActorExecutor, bytes int64) *deliveryActorReservation {
	r := &deliveryActorReservation{owner: owner}
	if bytes > 0 {
		r.state.Store(uint64(bytes))
	}
	return r
}

// retain charges memory allocated after claim (projection bytes and immutable
// delivery-plan clones) to the same global actor budget. It is deliberately
// non-blocking: waiting while every worker retains a large claim could create a
// capacity deadlock in which no work can finish and release bytes.
func (r *deliveryActorReservation) retain(bytes int64) bool {
	if r == nil || r.owner == nil || bytes <= 0 || !r.owner.reserveBytes(bytes) {
		return false
	}
	addition := uint64(bytes)
	for {
		state := r.state.Load()
		if state&deliveryReservationReleased != 0 || state&deliveryReservationByteMask > deliveryReservationByteMask-addition {
			r.owner.releaseBytes(bytes)
			return false
		}
		if r.state.CompareAndSwap(state, state+addition) {
			return true
		}
	}
}

func (r *deliveryActorReservation) releaseRetained(bytes int64) bool {
	if r == nil || r.owner == nil || bytes <= 0 {
		return false
	}
	subtraction := uint64(bytes)
	for {
		state := r.state.Load()
		retained := state & deliveryReservationByteMask
		if state&deliveryReservationReleased != 0 || retained < subtraction {
			return false
		}
		if r.state.CompareAndSwap(state, state-subtraction) {
			r.owner.releaseBytes(bytes)
			return true
		}
	}
}

func (r *deliveryActorReservation) take() (int64, bool) {
	if r == nil {
		return 0, false
	}
	for {
		state := r.state.Load()
		if state&deliveryReservationReleased != 0 {
			return 0, false
		}
		if r.state.CompareAndSwap(state, deliveryReservationReleased) {
			return int64(state & deliveryReservationByteMask), true
		}
	}
}

func claimWindowBytes(window store.OutboxClaimWindow) int64 {
	bytes := int64(256+len(window.Owner)) + claimItemRetainedBytes*int64(len(window.Items))
	for _, item := range window.Items {
		bytes += int64(len(item.SourceInstanceID))
		for _, target := range item.Targets {
			bytes += claimAttemptTargetBytes + int64(len(target.TargetInstanceID))
		}
		switch payload := item.Payload.(type) {
		case store.AbsoluteDeliveryPayload:
			bytes += int64(len(payload.TL))
		case store.DispatchOutboxPayload:
			bytes += ptsClaimByteEstimate
		case store.ChannelDeliveryPayload:
			bytes += int64(512 + 8*(len(payload.AudienceUserIDs)+len(payload.AffectedUserIDs)))
		}
	}
	return bytes
}

func orderingDomainHash(kind store.OutboxQueueKind, streamID int64) uint64 {
	x := uint64(streamID) ^ uint64(kind)*0x9e3779b97f4a7c15
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

type queueRunnerConfig struct {
	kind               store.OutboxQueueKind
	store              store.DurableOutboxStateStore
	wake               <-chan struct{}
	logicalCount       int
	workers            int
	laneLimit          int
	windowSize         int
	windowBytes        int
	lease              time.Duration
	physicalDuration   time.Duration
	clockSkewAllowance time.Duration
}

type deliveryQueueWorkerSignal struct {
	durableDeadlineWake bool
}

func (s *Service) runQueue(ctx context.Context, cfg queueRunnerConfig, actors *deliveryActorExecutor, wait *sync.WaitGroup) {
	if cfg.store == nil || cfg.wake == nil {
		s.log.Error("durable queue dependency missing", zap.Uint8("queue_kind", uint8(cfg.kind)))
		return
	}
	workers := normalizedOutboxWorkers(cfg.workers)
	workerWakes := make([]chan deliveryQueueWorkerSignal, workers)
	workerDone := make(chan bool, workers)
	for i := range workerWakes {
		workerWakes[i] = make(chan deliveryQueueWorkerSignal, 1)
	}
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wait.Add(1)
		go func() {
			defer wait.Done()
			s.runQueueWorker(ctx, cfg, worker, workerWakes[worker], workerDone, actors)
		}()
	}
	wait.Add(1)
	go func() {
		defer wait.Done()
		dispatch := func(ctx context.Context, durableDeadlineWake bool) bool {
			for _, workerWake := range workerWakes {
				select {
				case workerWake <- deliveryQueueWorkerSignal{durableDeadlineWake: durableDeadlineWake}:
				case <-ctx.Done():
					return false
				}
			}
			worked := false
			for range workerWakes {
				select {
				case workerWorked := <-workerDone:
					worked = worked || workerWorked
				case <-ctx.Done():
					return false
				}
			}
			return worked
		}
		nextReady := func(ctx context.Context) (store.OutboxNextReady, bool, error) {
			return cfg.store.NextReadyAt(ctx, cfg.kind, 0, nil)
		}
		runWakeScheduledDispatchLoop(ctx, cfg.wake, dispatch, nextReady, func(err error) {
			s.log.Warn("schedule durable delivery queue", zap.Uint8("queue_kind", uint8(cfg.kind)), zap.Error(err))
		})
	}()
}

func (s *Service) runQueueWorker(
	ctx context.Context,
	cfg queueRunnerConfig,
	worker int,
	wake <-chan deliveryQueueWorkerSignal,
	done chan<- bool,
	actors *deliveryActorExecutor,
) {
	shards := logicalShardsForWorker(worker, cfg.workers)
	owner := deliveryWorkerOwner(s.coordinator.instanceID, cfg.kind, worker)
	laneLimit := deliveryActorLaneLimit(cfg.laneLimit, cfg.workers, actors.maxWindows)
	recovery := store.OutboxRecoverBoundRequest{
		QueueKind: cfg.kind, Mode: store.OutboxBoundRecoveryExactOwnerLive, Owner: owner,
		LogicalShardCount: cfg.logicalCount, LogicalShardIDs: shards,
		LaneLimit: laneLimit, LeaseDuration: cfg.lease,
	}
	recoverySlots := actors.reserveWindowSlots(ctx, laneLimit)
	recovery.LaneLimit = recoverySlots
	if recoverySlots == 0 {
		return
	}
	if windows, err := cfg.store.RecoverBoundWindows(ctx, recovery); err != nil {
		actors.releaseWindowSlots(recoverySlots)
		s.log.Error("recover bound delivery windows", zap.Uint8("queue_kind", uint8(cfg.kind)), zap.Error(err))
	} else {
		if !submitReservedWindows(ctx, actors, windows, recoverySlots) {
			return
		}
	}
	claim := store.OutboxClaimRequest{
		QueueKind: cfg.kind, LogicalShardCount: cfg.logicalCount, LogicalShardIDs: shards,
		LaneLimit: laneLimit, WindowSize: cfg.windowSize, WindowByteLimit: cfg.windowBytes,
		LeaseDuration: cfg.lease, PhysicalDuration: cfg.physicalDuration,
		ClockSkewAllowance: cfg.clockSkewAllowance, Owner: owner,
	}
	expired := store.OutboxEvidenceExpiryRequest{
		QueueKind:         cfg.kind,
		LogicalShardCount: cfg.logicalCount, LogicalShardIDs: shards,
		LaneLimit: laneLimit, MaxAttempts: maxOnlineDeliveryAttempts,
	}
	finalizable := store.OutboxRecoverFinalizableRequest{
		QueueKind: cfg.kind, LogicalShardCount: cfg.logicalCount,
		LogicalShardIDs: shards, LaneLimit: laneLimit,
		AttemptLimit: store.MaxDeliveryBatchItems,
	}
	dispatch := func(ctx context.Context, durableDeadlineWake bool) bool {
		// The DB clock waits until recover_after (wire NotAfter + skew allowance)
		// before resolving a missing receipt. Finalize before claiming a new fence;
		// the domain actor queues any successor behind work still completing.
		// A successful attempt mutation normally wakes the fixed finalizer through
		// the local/Redis signal lane. If that acceleration signal is lost, the
		// claim-time durable evidence deadline is still armed here and closes the
		// commit-before-publish crash window without a periodic queue scan.
		slots := actors.reserveWindowSlots(ctx, laneLimit)
		if slots == 0 {
			return false
		}
		if durableDeadlineWake {
			request := finalizable
			request.LaneLimit = slots
			ready, err := cfg.store.RecoverFinalizableAttempts(ctx, request)
			if err != nil {
				actors.releaseWindowSlots(slots)
				s.log.Warn("recover durable finalization at evidence deadline", zap.Uint8("queue_kind", uint8(cfg.kind)), zap.Error(err))
				return false
			}
			if len(ready) > 0 {
				finalized, err := cfg.store.FinalizeAttempts(ctx, ready)
				if err != nil {
					actors.releaseWindowSlots(slots)
					s.log.Warn("finalize durable evidence at deadline", zap.Uint8("queue_kind", uint8(cfg.kind)), zap.Error(err))
					return false
				}
				if !submitReservedWindows(ctx, actors, finalized.Next, slots) {
					return false
				}
				return true
			}
			expiry := expired
			expiry.LaneLimit = slots
			expiredFinalizers, err := cfg.store.ExpireEvidenceDeadlines(ctx, expiry)
			if err != nil {
				actors.releaseWindowSlots(slots)
				s.log.Warn("expire durable delivery evidence", zap.Uint8("queue_kind", uint8(cfg.kind)), zap.Error(err))
				return false
			}
			if len(expiredFinalizers) > 0 {
				finalized, err := cfg.store.FinalizeAttempts(ctx, expiredFinalizers)
				if err != nil {
					actors.releaseWindowSlots(slots)
					s.log.Warn("finalize expired durable delivery evidence", zap.Uint8("queue_kind", uint8(cfg.kind)), zap.Error(err))
					return false
				}
				if !submitReservedWindows(ctx, actors, finalized.Next, slots) {
					return false
				}
				return true
			}
		}
		claimRequest := claim
		claimRequest.LaneLimit = slots
		claimStarted := time.Now()
		windows, err := cfg.store.ClaimWindows(ctx, claimRequest)
		claimedItems := 0
		for _, window := range windows {
			claimedItems += len(window.Items)
		}
		s.coordinator.observeStage(cfg.kind, "claim", claimStarted, claimedItems)
		if err != nil {
			actors.releaseWindowSlots(slots)
			s.log.Warn("claim durable delivery windows", zap.Uint8("queue_kind", uint8(cfg.kind)), zap.Error(err))
			return false
		}
		if len(windows) == 0 {
			actors.releaseWindowSlots(slots)
			return false
		}
		for _, window := range windows {
			s.coordinator.metrics.OutboxClaimed(len(window.Items))
		}
		if !submitReservedWindows(ctx, actors, windows, slots) {
			return false
		}
		return true
	}
	for {
		select {
		case <-ctx.Done():
			return
		case signal := <-wake:
			worked := dispatch(ctx, signal.durableDeadlineWake)
			select {
			case done <- worked:
			case <-ctx.Done():
				return
			}
		}
	}
}

func deliveryWorkerOwner(instanceID string, kind store.OutboxQueueKind, worker int) string {
	return fmt.Sprintf("%s/q%d/w%d", instanceID, kind, worker)
}

func errorsNewActorCapacity() error {
	return errActorCapacity
}

func deliveryActorLaneLimit(configured, workers, maxWindows int) int {
	if configured <= 0 {
		configured = 1
	}
	workers = normalizedOutboxWorkers(workers)
	limit := maxWindows / workers
	if limit < 1 {
		limit = 1
	}
	if configured < limit {
		return configured
	}
	return limit
}

func submitReservedWindows(
	ctx context.Context,
	actors *deliveryActorExecutor,
	windows []store.OutboxClaimWindow,
	reserved int,
) bool {
	if actors == nil || reserved <= 0 || len(windows) > reserved {
		if actors != nil && reserved > 0 {
			actors.releaseWindowSlots(reserved)
		}
		return false
	}
	for i, window := range windows {
		if actors.submitReserved(ctx, window) {
			continue
		}
		actors.releaseWindowSlots(reserved - i - 1)
		return false
	}
	actors.releaseWindowSlots(reserved - len(windows))
	return true
}

func (s *Service) runStartupFinalizers(
	ctx context.Context,
	cfg queueRunnerConfig,
	actors *deliveryActorExecutor,
	wait *sync.WaitGroup,
) {
	workers := normalizedOutboxWorkers(cfg.workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wake := make(chan struct{}, 1)
		wake <- struct{}{}
		close(wake)
		wait.Add(1)
		go func() {
			defer wait.Done()
			s.runFinalizerWorker(ctx, cfg, worker, wake, actors)
		}()
	}
}

func (s *Service) runFinalizerWorker(
	ctx context.Context,
	cfg queueRunnerConfig,
	worker int,
	wake <-chan struct{},
	actors *deliveryActorExecutor,
) {
	shards := logicalShardsForWorker(worker, cfg.workers)
	laneLimit := deliveryActorLaneLimit(cfg.laneLimit, cfg.workers, actors.maxWindows)
	request := store.OutboxRecoverFinalizableRequest{
		QueueKind: cfg.kind, LogicalShardCount: cfg.logicalCount,
		LogicalShardIDs: shards, LaneLimit: laneLimit,
		AttemptLimit: store.MaxDeliveryBatchItems,
	}
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-wake:
			if !ok {
				return
			}
		}
		backoff := outboxScheduleErrorBackoff
		for {
			slots := actors.reserveWindowSlots(ctx, laneLimit)
			if slots == 0 {
				return
			}
			bounded := request
			bounded.LaneLimit = slots
			recoverStarted := time.Now()
			requests, err := cfg.store.RecoverFinalizableAttempts(ctx, bounded)
			s.coordinator.observeStage(cfg.kind, "recover_finalizable", recoverStarted, len(requests))
			if err == nil && len(requests) == 0 {
				actors.releaseWindowSlots(slots)
				break
			}
			if err == nil {
				var finalized store.OutboxFinalizeBatch
				finalizeStarted := time.Now()
				finalized, err = cfg.store.FinalizeAttempts(ctx, requests)
				s.coordinator.observeStage(cfg.kind, "finalize", finalizeStarted, len(requests))
				if err == nil {
					if !submitReservedWindows(ctx, actors, finalized.Next, slots) {
						return
					}
					continue
				}
			}
			actors.releaseWindowSlots(slots)
			s.log.Warn("recover durable finalization", zap.Uint8("queue_kind", uint8(cfg.kind)), zap.Error(err))
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				return
			case <-timer.C:
			}
			if backoff < maxOutboxScheduleBackoff {
				backoff *= 2
				if backoff > maxOutboxScheduleBackoff {
					backoff = maxOutboxScheduleBackoff
				}
			}
		}
	}
}
