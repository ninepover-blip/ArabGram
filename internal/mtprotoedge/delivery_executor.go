package mtprotoedge

import (
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"telesrv/internal/edgecontrol"
)

const (
	deliveryExecutorShards        = 32
	deliveryExecutorMailbox       = 256
	deliveryExecutorMaxBytes      = int64(128 << 20)
	deliveryAttemptsPerShard      = 4096
	deliveryFanoutSessionQuantum  = 32
	deliveryFanoutTimeQuantum     = 500 * time.Microsecond
	deliveryRetainedBytesPerShard = int64(4 << 20)
)

type deliveryAttemptFingerprint struct {
	batchID          edgecontrol.BatchID
	commandID        edgecontrol.CommandID
	hash             [16]byte
	message          byte
	targetUserID     int64
	excludeAuthKeyID [8]byte
	excludeSessionID int64
	targetInstanceID string
	sourceInstanceID string
	notAfterUnixNano int64
	evidenceUnixNano int64
	channelRouteHash [16]byte
}

type deliveryAttemptEntry struct {
	fingerprint   deliveryAttemptFingerprint
	reservation   edgecontrol.PhysicalReceiptReservation
	index         int
	base          edgecontrol.PhysicalReceipt
	deadline      time.Time
	evidenceUntil time.Time
	terminalAt    time.Time
	terminal      edgecontrol.PhysicalReceipt
	retainedBytes int64
}

type deliveryFenceState struct {
	max          atomic.Uint64
	expiresAt    time.Time
	deadlineSlot int
}

type deliveryDeadlineKind uint8

const (
	deliveryDeadlineAttempt deliveryDeadlineKind = iota + 1
	deliveryDeadlineTerminal
	deliveryDeadlineFence
)

type deliveryDeadlineEntry struct {
	at         time.Time
	kind       deliveryDeadlineKind
	ref        edgecontrol.DeliveryRef
	domain     edgecontrol.OrderingDomain
	fence      uint64
	fenceState *deliveryFenceState
}

type deliveryDeadlineHeap []deliveryDeadlineEntry

func (h deliveryDeadlineHeap) Len() int           { return len(h) }
func (h deliveryDeadlineHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h deliveryDeadlineHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h.setFenceDeadlineSlot(i)
	h.setFenceDeadlineSlot(j)
}
func (h deliveryDeadlineHeap) setFenceDeadlineSlot(index int) {
	if state := h[index].fenceState; h[index].kind == deliveryDeadlineFence && state != nil {
		state.deadlineSlot = index + 1
	}
}
func (h *deliveryDeadlineHeap) Push(value any) {
	*h = append(*h, value.(deliveryDeadlineEntry))
	h.setFenceDeadlineSlot(len(*h) - 1)
}
func (h *deliveryDeadlineHeap) Pop() any {
	old := *h
	last := old[len(old)-1]
	if last.kind == deliveryDeadlineFence && last.fenceState != nil {
		last.fenceState.deadlineSlot = 0
	}
	*h = old[:len(old)-1]
	return last
}

type deliveryCommandTask struct {
	ctx        context.Context
	batch      edgecontrol.DeliveryBatch
	bytes      int
	replayOnly bool
	done       chan edgecontrol.DeliveryAdmission
}

type deliveryFanoutTask struct {
	batch      edgecontrol.DeliveryBatch
	indexes    []int
	fenceState *deliveryFenceState
	bytes      int
	itemPos    int
	conns      []frozenDeliveryConn
	connPos    int
	admitted   int
	fanout     *layerUpdatesFanout
	hook       *deliveryWriteAggregate
	ctx        context.Context
	cancel     context.CancelFunc
}

type deliveryTerminalEvent struct {
	ref     edgecontrol.DeliveryRef
	receipt edgecontrol.PhysicalReceipt
}

type deliveryExecutorShard struct {
	owner         *deliveryExecutor
	commands      chan deliveryCommandTask
	fanout        chan *deliveryFanoutTask
	terminal      chan deliveryTerminalEvent
	attempts      map[edgecontrol.DeliveryRef]*deliveryAttemptEntry
	fences        map[edgecontrol.OrderingDomain]*deliveryFenceState
	deadlines     deliveryDeadlineHeap
	active        int
	retainedBytes int64
}

type deliveryExecutor struct {
	manager  *SessionManager
	sink     edgecontrol.PhysicalReceiptSink
	shards   []deliveryExecutorShard
	maxBytes int64
	used     atomic.Int64
	stop     chan struct{}
	done     sync.WaitGroup
	once     sync.Once
}

// SetPhysicalReceiptSink installs the mandatory Edge -> Egress receipt plane.
// There is no in-memory or Redis fallback. It may be installed exactly once.
func (m *SessionManager) SetPhysicalReceiptSink(sink edgecontrol.PhysicalReceiptSink) error {
	if m == nil || sink == nil {
		return errors.New("mtprotoedge: physical receipt sink is required")
	}
	executor := newDeliveryExecutor(m, sink)
	if !m.deliveryExecutor.CompareAndSwap(nil, executor) {
		executor.close()
		return errors.New("mtprotoedge: physical receipt sink already installed")
	}
	return nil
}

func (m *SessionManager) CloseDeliveryExecutor() {
	if m == nil {
		return
	}
	if executor := m.deliveryExecutor.Swap(nil); executor != nil {
		executor.close()
	}
}

// AdmitDeliveryBatch is command admission only. The returned Accepted outcome
// means the ordering actor, same-attempt state and reporter slots are reserved;
// it never means a socket write has happened.
func (m *SessionManager) AdmitDeliveryBatch(ctx context.Context, batch edgecontrol.DeliveryBatch) edgecontrol.DeliveryAdmission {
	admission := edgecontrol.DeliveryAdmission{
		BatchID: batch.BatchID, CommandID: batch.CommandID,
		SourceInstanceID: batch.SourceInstanceID, TargetInstanceID: batch.TargetInstanceID,
	}
	if err := edgecontrol.ValidateDeliveryBatch(batch); err != nil {
		admission.Outcome, admission.Detail = edgecontrol.AdmissionRejected, edgecontrol.DetailInvalidPayload
		return admission
	}
	now := time.Now()
	if !now.Before(batch.EvidenceDeadline) {
		admission.Outcome, admission.Detail = edgecontrol.AdmissionRejected, edgecontrol.DetailDeadline
		return admission
	}
	executor := m.deliveryExecutor.Load()
	if executor == nil {
		admission.Outcome, admission.Detail = edgecontrol.AdmissionRejected, edgecontrol.DetailCapacity
		return admission
	}
	return executor.admit(ctx, batch, !now.Before(batch.NotAfter))
}

func newDeliveryExecutor(manager *SessionManager, sink edgecontrol.PhysicalReceiptSink) *deliveryExecutor {
	e := &deliveryExecutor{manager: manager, sink: sink, maxBytes: deliveryExecutorMaxBytes, stop: make(chan struct{})}
	e.shards = make([]deliveryExecutorShard, deliveryExecutorShards)
	for i := range e.shards {
		e.shards[i] = deliveryExecutorShard{
			owner: e, commands: make(chan deliveryCommandTask, deliveryExecutorMailbox),
			fanout:   make(chan *deliveryFanoutTask, deliveryAttemptsPerShard),
			terminal: make(chan deliveryTerminalEvent, deliveryAttemptsPerShard),
			attempts: make(map[edgecontrol.DeliveryRef]*deliveryAttemptEntry),
			fences:   make(map[edgecontrol.OrderingDomain]*deliveryFenceState),
		}
		heap.Init(&e.shards[i].deadlines)
		e.done.Add(1)
		go e.shards[i].run()
	}
	return e
}

func (e *deliveryExecutor) admit(ctx context.Context, batch edgecontrol.DeliveryBatch, replayOnly bool) edgecontrol.DeliveryAdmission {
	admission := edgecontrol.DeliveryAdmission{BatchID: batch.BatchID, CommandID: batch.CommandID, SourceInstanceID: batch.SourceInstanceID, TargetInstanceID: batch.TargetInstanceID}
	if !time.Now().Before(batch.EvidenceDeadline) {
		admission.Outcome, admission.Detail = edgecontrol.AdmissionRejected, edgecontrol.DetailDeadline
		return admission
	}
	bytes := deliveryBatchBytes(batch)
	if !reserveDeliveryBytes(&e.used, e.maxBytes, bytes) {
		admission.Outcome, admission.Detail = edgecontrol.AdmissionOverloaded, edgecontrol.DetailCapacity
		return admission
	}
	if ctx == nil {
		ctx = context.Background()
	}
	task := deliveryCommandTask{ctx: ctx, batch: batch, bytes: bytes, replayOnly: replayOnly, done: make(chan edgecontrol.DeliveryAdmission, 1)}
	shard := &e.shards[edgecontrol.OrderingDomainHash(batch.Items[0].Ref.Domain)&uint64(len(e.shards)-1)]
	select {
	case shard.commands <- task:
	case <-e.stop:
		e.used.Add(-int64(bytes))
		admission.Outcome, admission.Detail = edgecontrol.AdmissionRejected, edgecontrol.DetailCapacity
		return admission
	default:
		e.used.Add(-int64(bytes))
		admission.Outcome, admission.Detail = edgecontrol.AdmissionOverloaded, edgecontrol.DetailCapacity
		return admission
	}
	select {
	case got := <-task.done:
		return got
	case <-ctx.Done():
		admission.Outcome, admission.Detail = edgecontrol.AdmissionOverloaded, edgecontrol.DetailDeadline
		return admission
	case <-e.stop:
		admission.Outcome, admission.Detail = edgecontrol.AdmissionRejected, edgecontrol.DetailCapacity
		return admission
	}
}

func (s *deliveryExecutorShard) run() {
	defer s.owner.done.Done()
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	for {
		var deadline <-chan time.Time
		if len(s.deadlines) > 0 {
			wait := time.Until(s.deadlines[0].at)
			if wait < 0 {
				wait = 0
			}
			timer.Reset(wait)
			deadline = timer.C
		}
		select {
		case <-s.owner.stop:
			s.stopAll()
			return
		case task := <-s.commands:
			s.handle(task)
		case task := <-s.fanout:
			s.continueFanout(task)
		case event := <-s.terminal:
			s.complete(event.ref, event.receipt)
		case now := <-deadline:
			s.expire(now)
		}
		if deadline != nil && !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
}

func (s *deliveryExecutorShard) handle(task deliveryCommandTask) {
	releaseCommandBytes := true
	defer func() {
		if releaseCommandBytes && task.bytes > 0 {
			s.owner.used.Add(-int64(task.bytes))
		}
	}()
	batch := task.batch
	admission := edgecontrol.DeliveryAdmission{BatchID: batch.BatchID, CommandID: batch.CommandID, SourceInstanceID: batch.SourceInstanceID, TargetInstanceID: batch.TargetInstanceID}
	now := time.Now()
	if (task.ctx != nil && task.ctx.Err() != nil) || !now.Before(batch.EvidenceDeadline) {
		admission.Outcome, admission.Detail = edgecontrol.AdmissionRejected, edgecontrol.DetailDeadline
		task.done <- admission
		return
	}
	if task.replayOnly || !now.Before(batch.NotAfter) {
		s.replayTerminalOnly(task, admission)
		return
	}
	domain := batch.Items[0].Ref.Domain
	fence := batch.Items[0].Ref.LeaseFence
	for _, item := range batch.Items[1:] {
		if item.Ref.LeaseFence != fence {
			admission.Outcome, admission.Detail = edgecontrol.AdmissionRejected, edgecontrol.DetailAttemptConflict
			task.done <- admission
			return
		}
	}
	fenceState := s.fences[domain]
	if fenceState != nil && fence < fenceState.max.Load() {
		admission.Outcome, admission.Detail = edgecontrol.AdmissionRejected, edgecontrol.DetailAttemptConflict
		task.done <- admission
		return
	}
	if fenceState == nil {
		fenceState = &deliveryFenceState{}
		s.fences[domain] = fenceState
	}
	previousFence := fenceState.max.Load()
	fenceAdvanced := fence > previousFence
	if fenceAdvanced {
		// A validated higher database fence invalidates the previous owner even
		// when this command cannot reserve local capacity. Advance before every
		// overload branch so an old Conn queue cannot write after reclamation.
		fenceState.max.Store(fence)
	}
	fenceExpiresAt := batch.EvidenceDeadline
	deadlineExtended := fenceExpiresAt.After(fenceState.expiresAt)
	if deadlineExtended {
		fenceState.expiresAt = fenceExpiresAt
	}
	if fenceAdvanced || deadlineExtended {
		// Keep exactly one mutable deadline per ordering domain. A validated
		// higher fence can arrive with a shorter wire deadline than the previous
		// retention tail: updating the existing heap node both prevents permanent
		// residency and bounds actor memory under repeated higher-fence overloads.
		deadline := deliveryDeadlineEntry{
			at: fenceState.expiresAt, kind: deliveryDeadlineFence,
			domain: domain, fence: fence, fenceState: fenceState,
		}
		if fenceState.deadlineSlot == 0 {
			heap.Push(&s.deadlines, deadline)
		} else {
			index := fenceState.deadlineSlot - 1
			s.deadlines[index] = deadline
			heap.Fix(&s.deadlines, index)
		}
	}
	newIndexes := make([]int, 0, len(batch.Items))
	terminalIndexes := make([]int, 0, len(batch.Items))
	allTerminal := true
	for i, item := range batch.Items {
		fingerprint := deliveryFingerprint(batch, item)
		if current := s.attempts[item.Ref]; current != nil {
			if current.fingerprint != fingerprint {
				admission.Outcome, admission.Detail = edgecontrol.AdmissionRejected, edgecontrol.DetailAttemptConflict
				task.done <- admission
				return
			}
			if current.terminalAt.IsZero() {
				allTerminal = false
			} else {
				terminalIndexes = append(terminalIndexes, i)
			}
			continue
		}
		allTerminal = false
		newIndexes = append(newIndexes, i)
	}
	if len(newIndexes) == 0 {
		if allTerminal {
			if err := s.republishTerminal(batch, terminalIndexes, batch.EvidenceDeadline); err != nil {
				admission.Outcome, admission.Detail = edgecontrol.AdmissionOverloaded, edgecontrol.DetailCapacity
				task.done <- admission
				return
			}
			admission.Outcome = edgecontrol.AdmissionDuplicateTerminal
		} else {
			admission.Outcome = edgecontrol.AdmissionDuplicateInFlight
		}
		task.done <- admission
		return
	}
	if s.active+len(newIndexes) > deliveryAttemptsPerShard {
		admission.Outcome, admission.Detail = edgecontrol.AdmissionOverloaded, edgecontrol.DetailCapacity
		task.done <- admission
		return
	}
	var retainedNeeded int64
	for _, index := range newIndexes {
		retainedNeeded += deliveryAttemptRetainedBytes(batch, batch.Items[index])
	}
	if retainedNeeded <= 0 || s.retainedBytes > deliveryRetainedBytesPerShard-retainedNeeded {
		admission.Outcome, admission.Detail = edgecontrol.AdmissionOverloaded, edgecontrol.DetailCapacity
		task.done <- admission
		return
	}
	refs := make([]edgecontrol.DeliveryRef, len(newIndexes))
	for i, index := range newIndexes {
		refs[i] = batch.Items[index].Ref
	}
	deadline := batch.NotAfter
	reservation, err := s.owner.sink.ReservePhysicalReceipts(batch.BatchID, batch.SourceInstanceID, batch.TargetInstanceID, batch.CommandID, deadline, refs)
	if err != nil {
		admission.Outcome, admission.Detail = edgecontrol.AdmissionOverloaded, edgecontrol.DetailCapacity
		task.done <- admission
		return
	}
	if err := s.republishTerminal(batch, terminalIndexes, batch.EvidenceDeadline); err != nil {
		for i := range refs {
			reservation.Release(i)
		}
		admission.Outcome, admission.Detail = edgecontrol.AdmissionOverloaded, edgecontrol.DetailCapacity
		task.done <- admission
		return
	}
	for reservationIndex, itemIndex := range newIndexes {
		item := batch.Items[itemIndex]
		retained := deliveryAttemptRetainedBytes(batch, item)
		s.attempts[item.Ref] = &deliveryAttemptEntry{
			fingerprint: deliveryFingerprint(batch, item), reservation: reservation, index: reservationIndex,
			deadline:      deadline,
			evidenceUntil: batch.EvidenceDeadline,
			base:          edgecontrol.PhysicalReceipt{BatchID: batch.BatchID, CommandID: batch.CommandID, SourceInstanceID: batch.SourceInstanceID, TargetInstanceID: batch.TargetInstanceID, Ref: item.Ref},
			retainedBytes: retained,
		}
		heap.Push(&s.deadlines, deliveryDeadlineEntry{at: deadline, kind: deliveryDeadlineAttempt, ref: item.Ref})
	}
	s.active += len(newIndexes)
	s.retainedBytes += retainedNeeded
	admission.Outcome, admission.Detail = edgecontrol.AdmissionAccepted, edgecontrol.DetailNone
	task.done <- admission

	// The command payload remains referenced by the fanout task. Transfer its
	// exact byte reservation instead of releasing it at admission; otherwise a
	// full fanout mailbox could retain many large TL batches outside maxBytes.
	fanoutTask := &deliveryFanoutTask{
		batch: batch, indexes: append([]int(nil), newIndexes...),
		fenceState: fenceState, bytes: task.bytes,
	}
	releaseCommandBytes = false
	select {
	case s.fanout <- fanoutTask:
	default:
		s.failFanout(fanoutTask, edgecontrol.DetailCapacity)
	}
}

// replayTerminalOnly is the post-command, pre-evidence-deadline state. It may
// republish a receipt for an exact command already owned by this actor, but it
// can never create an attempt, advance a fence, enumerate sessions, or enqueue
// bytes to a Conn. A missing entry is therefore a terminal deadline rejection,
// not permission to execute the command again.
func (s *deliveryExecutorShard) replayTerminalOnly(task deliveryCommandTask, admission edgecontrol.DeliveryAdmission) {
	batch := task.batch
	terminalIndexes := make([]int, 0, len(batch.Items))
	inFlight := false
	for i, item := range batch.Items {
		entry := s.attempts[item.Ref]
		if entry == nil {
			admission.Outcome, admission.Detail = edgecontrol.AdmissionRejected, edgecontrol.DetailDeadline
			task.done <- admission
			return
		}
		if entry.fingerprint != deliveryFingerprint(batch, item) {
			admission.Outcome, admission.Detail = edgecontrol.AdmissionRejected, edgecontrol.DetailAttemptConflict
			task.done <- admission
			return
		}
		if entry.terminalAt.IsZero() {
			inFlight = true
			continue
		}
		terminalIndexes = append(terminalIndexes, i)
	}
	if err := s.republishTerminal(batch, terminalIndexes, batch.EvidenceDeadline); err != nil {
		admission.Outcome, admission.Detail = edgecontrol.AdmissionOverloaded, edgecontrol.DetailCapacity
		task.done <- admission
		return
	}
	if inFlight {
		admission.Outcome = edgecontrol.AdmissionDuplicateInFlight
	} else {
		admission.Outcome = edgecontrol.AdmissionDuplicateTerminal
	}
	admission.Detail = edgecontrol.DetailNone
	task.done <- admission
}

func (s *deliveryExecutorShard) republishTerminal(batch edgecontrol.DeliveryBatch, indexes []int, reserveUntil time.Time) error {
	if len(indexes) == 0 {
		return nil
	}
	refs := make([]edgecontrol.DeliveryRef, len(indexes))
	for i, index := range indexes {
		refs[i] = batch.Items[index].Ref
	}
	reservation, err := s.owner.sink.ReservePhysicalReceipts(
		batch.BatchID, batch.SourceInstanceID, batch.TargetInstanceID, batch.CommandID, reserveUntil, refs,
	)
	if err != nil {
		return err
	}
	for i, index := range indexes {
		entry := s.attempts[batch.Items[index].Ref]
		if entry == nil || entry.terminalAt.IsZero() || entry.terminal.Ref != batch.Items[index].Ref {
			for j := i; j < len(indexes); j++ {
				reservation.Release(j)
			}
			return errors.New("mtprotoedge: terminal delivery cache incomplete")
		}
		reservation.Commit(i, entry.terminal)
	}
	return nil
}

func (s *deliveryExecutorShard) continueFanout(task *deliveryFanoutTask) {
	if task == nil {
		return
	}
	quantumStart := time.Now()
	for task.itemPos < len(task.indexes) {
		if task.hook == nil && !s.startFanoutItem(task) {
			task.itemPos++
			if time.Since(quantumStart) >= deliveryFanoutTimeQuantum {
				s.requeueFanout(task)
				return
			}
			continue
		}
		item := task.batch.Items[task.indexes[task.itemPos]]
		limit := task.connPos + deliveryFanoutSessionQuantum
		if limit > len(task.conns) {
			limit = len(task.conns)
		}
		for task.connPos < limit && time.Since(quantumStart) < deliveryFanoutTimeQuantum {
			frozen := task.conns[task.connPos]
			task.connPos++
			if !task.hook.allowPhysicalWrite(time.Now()) ||
				!frozen.stillEligible(task.batch.ExcludeAuthKeyID, task.batch.ExcludeSessionID) {
				continue
			}
			conn := frozen.conn
			encoded, err := task.fanout.prepareForConn(task.ctx, conn)
			if err != nil || encoded == nil {
				continue
			}
			tracking := edgecontrol.DeliveryTracking{
				BatchID: task.batch.BatchID, CommandID: task.batch.CommandID,
				SourceInstanceID: task.batch.SourceInstanceID, Ref: item.Ref,
			}
			hook := &deliverySessionWriteHook{
				aggregate: task.hook, frozen: frozen,
				excludeAuthKeyID: task.batch.ExcludeAuthKeyID,
				excludeSessionID: task.batch.ExcludeSessionID,
			}
			if err := conn.enqueueDurableEncoded(task.ctx, item.MessageType, encoded, tracking, hook); err == nil {
				task.admitted++
			}
		}
		if task.connPos < len(task.conns) {
			s.requeueFanout(task)
			return
		}
		if task.admitted != len(task.conns) {
			task.hook.failed(time.Now())
		}
		if task.cancel != nil {
			task.cancel()
		}
		if task.fanout != nil {
			task.fanout.close()
		}
		task.conns, task.fanout, task.hook, task.ctx, task.cancel = nil, nil, nil, nil, nil
		task.connPos, task.admitted = 0, 0
		task.itemPos++
		if time.Since(quantumStart) >= deliveryFanoutTimeQuantum {
			s.requeueFanout(task)
			return
		}
	}
	s.releaseFanoutBytes(task)
}

func (s *deliveryExecutorShard) startFanoutItem(task *deliveryFanoutTask) bool {
	item := task.batch.Items[task.indexes[task.itemPos]]
	entry := s.attempts[item.Ref]
	if entry == nil || !entry.terminalAt.IsZero() {
		return false
	}
	now := time.Now()
	if !now.Before(task.batch.NotAfter) || task.fenceState == nil || task.fenceState.max.Load() != item.Ref.LeaseFence {
		receipt := entry.base
		receipt.Outcome, receipt.Detail, receipt.ObservedAt = edgecontrol.PhysicalIndeterminate, edgecontrol.DetailDeadline, now
		s.complete(item.Ref, receipt)
		return false
	}
	if item.Ref.Domain.Kind == edgecontrol.QueueChannelPTS {
		task.conns = s.owner.manager.snapshotReadyChannelDeliveryConns(item.Channel)
	} else {
		task.conns = s.owner.manager.snapshotReadyDeliveryConns(task.batch.TargetUserID, task.batch.ExcludeAuthKeyID, task.batch.ExcludeSessionID)
	}
	entry.base.EligibleSessions = len(task.conns)
	if len(task.conns) == 0 {
		receipt := entry.base
		receipt.Outcome, receipt.ObservedAt = edgecontrol.PhysicalNoEligibleSessions, now
		s.complete(item.Ref, receipt)
		task.conns = nil
		return false
	}
	update, err := decodeDurableDeliveryUpdate(item.UpdateBytes)
	if err != nil {
		receipt := entry.base
		receipt.Outcome, receipt.Detail, receipt.ObservedAt = edgecontrol.PhysicalRejected, edgecontrol.DetailInvalidPayload, now
		s.complete(item.Ref, receipt)
		task.conns = nil
		return false
	}
	task.ctx, task.cancel = context.WithDeadline(context.Background(), entry.deadline)
	task.fanout, err = newRetainedLayerUpdatesFanoutContext(
		task.ctx,
		update,
		func(bytes int) bool {
			return reserveDeliveryBytes(&s.owner.used, s.owner.maxBytes, bytes)
		},
		func(bytes int) {
			if bytes > 0 {
				s.owner.used.Add(-int64(bytes))
			}
		},
		1,
	)
	if err != nil {
		task.cancel()
		task.ctx, task.cancel = nil, nil
		receipt := entry.base
		if errors.Is(err, ErrOutboundTrackedBudget) {
			receipt.Outcome, receipt.Detail = edgecontrol.PhysicalIndeterminate, edgecontrol.DetailCapacity
		} else {
			receipt.Outcome, receipt.Detail = edgecontrol.PhysicalRejected, edgecontrol.DetailInvalidPayload
		}
		receipt.ObservedAt = now
		s.complete(item.Ref, receipt)
		task.conns = nil
		return false
	}
	task.hook = &deliveryWriteAggregate{
		terminal: s.terminal, ref: item.Ref, receipt: entry.base, reservation: entry.reservation,
		index: entry.index, expected: int32(len(task.conns)), notAfter: task.batch.NotAfter,
		fence: item.Ref.LeaseFence, fenceState: task.fenceState,
	}
	return true
}

func (s *deliveryExecutorShard) requeueFanout(task *deliveryFanoutTask) {
	if task == nil {
		return
	}
	if task.itemPos >= len(task.indexes) {
		s.releaseFanoutBytes(task)
		return
	}
	select {
	case s.fanout <- task:
	default:
		s.failFanout(task, edgecontrol.DetailCapacity)
	}
}

func (s *deliveryExecutorShard) failFanout(task *deliveryFanoutTask, detail edgecontrol.DetailCode) {
	if task == nil {
		return
	}
	defer s.releaseFanoutBytes(task)
	if task.cancel != nil {
		task.cancel()
	}
	if task.fanout != nil {
		task.fanout.close()
		task.fanout = nil
	}
	if task.hook != nil {
		task.hook.failed(time.Now())
	}
	now := time.Now()
	for pos := task.itemPos; pos < len(task.indexes); pos++ {
		item := task.batch.Items[task.indexes[pos]]
		entry := s.attempts[item.Ref]
		if entry == nil || !entry.terminalAt.IsZero() || (pos == task.itemPos && task.hook != nil) {
			continue
		}
		receipt := entry.base
		receipt.Outcome, receipt.Detail, receipt.ObservedAt = edgecontrol.PhysicalIndeterminate, detail, now
		s.complete(item.Ref, receipt)
	}
}

func (s *deliveryExecutorShard) releaseFanoutBytes(task *deliveryFanoutTask) {
	if task == nil || task.bytes <= 0 {
		return
	}
	bytes := task.bytes
	task.bytes = 0
	s.owner.used.Add(-int64(bytes))
}

func (s *deliveryExecutorShard) complete(ref edgecontrol.DeliveryRef, receipt edgecontrol.PhysicalReceipt) {
	entry := s.attempts[ref]
	if entry == nil || !entry.terminalAt.IsZero() {
		return
	}
	if receipt.ObservedAt.IsZero() {
		receipt.ObservedAt = time.Now()
	}
	entry.terminalAt = receipt.ObservedAt
	entry.terminal = receipt
	if s.active > 0 {
		s.active--
	}
	entry.reservation.Commit(entry.index, receipt)
	heap.Push(&s.deadlines, deliveryDeadlineEntry{at: entry.evidenceUntil, kind: deliveryDeadlineTerminal, ref: ref})
}

func (s *deliveryExecutorShard) expire(now time.Time) {
	for len(s.deadlines) > 0 && !now.Before(s.deadlines[0].at) {
		deadline := heap.Pop(&s.deadlines).(deliveryDeadlineEntry)
		switch deadline.kind {
		case deliveryDeadlineAttempt:
			entry := s.attempts[deadline.ref]
			if entry != nil && entry.terminalAt.IsZero() && !now.Before(entry.deadline) {
				receipt := entry.base
				receipt.Outcome = edgecontrol.PhysicalIndeterminate
				receipt.Detail = edgecontrol.DetailDeadline
				receipt.ObservedAt = now
				s.complete(deadline.ref, receipt)
			}
		case deliveryDeadlineTerminal:
			entry := s.attempts[deadline.ref]
			if entry != nil && !entry.terminalAt.IsZero() && !now.Before(entry.evidenceUntil) {
				s.retainedBytes -= entry.retainedBytes
				delete(s.attempts, deadline.ref)
			}
		case deliveryDeadlineFence:
			state := s.fences[deadline.domain]
			if state == deadline.fenceState && state != nil && state.max.Load() == deadline.fence && !now.Before(state.expiresAt) {
				delete(s.fences, deadline.domain)
			}
		}
	}
}

func (s *deliveryExecutorShard) stopAll() {
	now := time.Now()
	for ref, entry := range s.attempts {
		if entry.terminalAt.IsZero() {
			receipt := entry.base
			receipt.Outcome = edgecontrol.PhysicalIndeterminate
			receipt.Detail = edgecontrol.DetailDeadline
			receipt.ObservedAt = now
			s.complete(ref, receipt)
		}
	}
	for {
		select {
		case task := <-s.fanout:
			s.failFanout(task, edgecontrol.DetailDeadline)
		case task := <-s.commands:
			s.owner.used.Add(-int64(task.bytes))
			admission := edgecontrol.DeliveryAdmission{BatchID: task.batch.BatchID, CommandID: task.batch.CommandID, SourceInstanceID: task.batch.SourceInstanceID, TargetInstanceID: task.batch.TargetInstanceID, Outcome: edgecontrol.AdmissionRejected, Detail: edgecontrol.DetailCapacity}
			task.done <- admission
		default:
			return
		}
	}
}

type deliveryWriteAggregate struct {
	done         atomic.Bool
	writtenCount atomic.Int32
	firstMsgID   atomic.Int64
	terminal     chan<- deliveryTerminalEvent
	ref          edgecontrol.DeliveryRef
	receipt      edgecontrol.PhysicalReceipt
	reservation  edgecontrol.PhysicalReceiptReservation
	index        int
	expected     int32
	notAfter     time.Time
	fence        uint64
	fenceState   *deliveryFenceState
}

// deliverySessionWriteHook carries the immutable snapshot identity all the way
// into deadlineOutboundGuardedScratchWriter's socket-lock callback. Aggregate
// fence/deadline validity is necessary but cannot prove that a queued Conn still
// belongs to the account/session generation selected during local expansion.
type deliverySessionWriteHook struct {
	aggregate        *deliveryWriteAggregate
	frozen           frozenDeliveryConn
	excludeAuthKeyID [8]byte
	excludeSessionID int64
}

func (h *deliverySessionWriteHook) allowPhysicalWrite(now time.Time) bool {
	return h != nil && h.aggregate != nil && h.aggregate.allowPhysicalWrite(now) &&
		h.frozen.stillEligible(h.excludeAuthKeyID, h.excludeSessionID)
}

func (h *deliverySessionWriteHook) written(serverMsgID int64, observedAt time.Time) {
	if h != nil && h.aggregate != nil {
		h.aggregate.written(serverMsgID, observedAt)
	}
}

func (h *deliverySessionWriteHook) failed(observedAt time.Time) {
	if h != nil && h.aggregate != nil {
		h.aggregate.failed(observedAt)
	}
}

func (h *deliveryWriteAggregate) allowPhysicalWrite(now time.Time) bool {
	return h != nil && !h.done.Load() && h.fenceState != nil && h.fence != 0 && h.fenceState.max.Load() == h.fence && now.Before(h.notAfter)
}

func (h *deliveryWriteAggregate) written(serverMsgID int64, observedAt time.Time) {
	if h == nil || h.done.Load() {
		return
	}
	if !h.allowPhysicalWrite(observedAt) {
		h.failed(observedAt)
		return
	}
	h.firstMsgID.CompareAndSwap(0, serverMsgID)
	if h.writtenCount.Add(1) != h.expected || !h.done.CompareAndSwap(false, true) {
		return
	}
	receipt := h.receipt
	receipt.Outcome = edgecontrol.PhysicalWritten
	receipt.WrittenSessions = int(h.expected)
	receipt.FirstServerMsgID = h.firstMsgID.Load()
	receipt.ObservedAt = observedAt
	// Commit directly from the Conn actor into the pre-reserved O(1) reporter
	// slot. The actor event below only advances same-attempt dedupe state.
	h.reservation.Commit(h.index, receipt)
	select {
	case h.terminal <- deliveryTerminalEvent{ref: h.ref, receipt: receipt}:
	default:
		// The terminal mailbox has one reserved slot for every active attempt
		// and each hook is one-shot. A full mailbox is an internal capacity
		// invariant violation; never lose exact replay state after committing the
		// authoritative reporter slot.
		panic("mtprotoedge: delivery terminal mailbox invariant violated")
	}
}

func (h *deliveryWriteAggregate) failed(observedAt time.Time) {
	if h == nil || !h.done.CompareAndSwap(false, true) {
		return
	}
	receipt := h.receipt
	receipt.Outcome = edgecontrol.PhysicalIndeterminate
	receipt.Detail = edgecontrol.DetailWriteFailed
	receipt.ObservedAt = observedAt
	h.reservation.Commit(h.index, receipt)
	select {
	case h.terminal <- deliveryTerminalEvent{ref: h.ref, receipt: receipt}:
	default:
		panic("mtprotoedge: delivery terminal mailbox invariant violated")
	}
}

type frozenDeliveryConn struct {
	conn           *Conn
	key            sessionKey
	expectedUserID int64
	generation     uint64
	profile        int
}

func freezeDeliveryConn(conn *Conn, key sessionKey, expectedUserID int64) frozenDeliveryConn {
	frozen := frozenDeliveryConn{conn: conn, key: key, expectedUserID: expectedUserID}
	if conn != nil {
		frozen.generation = conn.deliveryIdentityGeneration.Load()
		frozen.profile = int(conn.LayerProfileState().Profile)
	}
	return frozen
}

func (f frozenDeliveryConn) stillEligible(excludeAuthKeyID [8]byte, excludeSessionID int64) bool {
	conn := f.conn
	return conn != nil && conn.authKeyID == f.key.authKeyID && conn.sessionID == f.key.sessionID &&
		conn.deliveryIdentityGeneration.Load() == f.generation &&
		deliveryConnStillEligible(conn, f.expectedUserID, excludeAuthKeyID, excludeSessionID)
}

func (m *SessionManager) snapshotReadyDeliveryConns(userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64) []frozenDeliveryConn {
	m.mu.RLock()
	defer m.mu.RUnlock()
	conns := make([]frozenDeliveryConn, 0, len(m.byUser[userID]))
	for key, conn := range m.byUser[userID] {
		if deliveryConnStillEligible(conn, userID, excludeAuthKeyID, excludeSessionID) {
			conns = append(conns, freezeDeliveryConn(conn, key, userID))
		}
	}
	sortFrozenDeliveryConnsByProfile(conns)
	return conns
}

func (m *SessionManager) snapshotReadyChannelDeliveryConns(route edgecontrol.ChannelDeliveryRoute) []frozenDeliveryConn {
	if m == nil || !route.ValidFor(edgecontrol.OrderingDomain{Kind: edgecontrol.QueueChannelPTS, StreamID: route.ChannelID}) {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	seen := make(map[sessionKey]struct{})
	conns := make([]frozenDeliveryConn, 0)
	add := func(key sessionKey, userID int64) {
		if userID <= 0 {
			return
		}
		if _, exists := seen[key]; exists {
			return
		}
		conn := m.bySession[key]
		if !deliveryConnStillEligible(conn, userID, [8]byte{}, 0) {
			return
		}
		seen[key] = struct{}{}
		conns = append(conns, freezeDeliveryConn(conn, key, userID))
	}
	addUser := func(userID int64) {
		for key := range m.byUser[userID] {
			add(key, userID)
		}
	}
	switch route.Audience {
	case edgecontrol.ChannelAudienceMembers:
		for key, userID := range m.byMemberChannel[route.ChannelID] {
			add(key, userID)
		}
		for key, userID := range m.byChannel[route.ChannelID] {
			add(key, userID)
		}
		now := time.Now().UnixNano()
		for key, subscription := range m.bySubscribedChannel[route.ChannelID] {
			if subscription.expiresAt <= now {
				continue
			}
			add(key, subscription.userID)
		}
	case edgecontrol.ChannelAudienceMessageBox:
		allowed := make(map[int64]struct{}, len(route.AudienceUsers))
		for _, userID := range route.AudienceUsers {
			allowed[userID] = struct{}{}
		}
		for key, userID := range m.byChannel[route.ChannelID] {
			if _, ok := allowed[userID]; ok {
				add(key, userID)
			}
		}
		now := time.Now().UnixNano()
		for key, subscription := range m.bySubscribedChannel[route.ChannelID] {
			if subscription.expiresAt <= now {
				continue
			}
			if _, ok := allowed[subscription.userID]; ok {
				add(key, subscription.userID)
			}
		}
		for key, userID := range m.byMemberChannel[route.ChannelID] {
			if _, ok := allowed[userID]; ok {
				add(key, userID)
			}
		}
	}
	for _, userID := range route.AudienceUsers {
		addUser(userID)
	}
	for _, userID := range route.AffectedUsers {
		addUser(userID)
	}
	sortFrozenDeliveryConnsByProfile(conns)
	return conns
}

func sortFrozenDeliveryConnsByProfile(conns []frozenDeliveryConn) {
	// The durable fanout cache deliberately retains one exact Layer body. Group
	// equal profiles so a mixed client fleet performs one encode per profile
	// without retaining every profile body at once.
	sort.Slice(conns, func(i, j int) bool {
		return conns[i].profile < conns[j].profile
	})
}

func deliveryConnStillEligible(conn *Conn, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64) bool {
	if conn == nil || !conn.isActive() || conn.userID.Load() != userID || !conn.receivesUpdates.Load() || conn.outbound == nil || conn.outboundControl == nil {
		return false
	}
	if excludeSessionID != 0 && conn.authKeyID == excludeAuthKeyID && conn.sessionID == excludeSessionID {
		return false
	}
	_, ok := conn.LayerProfile()
	return ok
}
func deliveryFingerprint(batch edgecontrol.DeliveryBatch, item edgecontrol.DeliveryItem) deliveryAttemptFingerprint {
	return deliveryAttemptFingerprint{
		batchID: batch.BatchID, commandID: batch.CommandID, hash: item.PayloadHash,
		message: byte(item.MessageType), targetUserID: batch.TargetUserID,
		excludeAuthKeyID: batch.ExcludeAuthKeyID, excludeSessionID: batch.ExcludeSessionID,
		targetInstanceID: batch.TargetInstanceID, sourceInstanceID: batch.SourceInstanceID,
		notAfterUnixNano: batch.NotAfter.UnixNano(),
		evidenceUnixNano: batch.EvidenceDeadline.UnixNano(),
		channelRouteHash: deliveryChannelRouteHash(item.Channel),
	}
}

func deliveryChannelRouteHash(route edgecontrol.ChannelDeliveryRoute) [16]byte {
	h := sha256.New()
	var raw [8]byte
	binary.LittleEndian.PutUint64(raw[:], uint64(route.ChannelID))
	_, _ = h.Write(raw[:])
	_, _ = h.Write([]byte{byte(route.Audience)})
	for _, ids := range [][]int64{route.AudienceUsers, route.AffectedUsers} {
		binary.LittleEndian.PutUint64(raw[:], uint64(len(ids)))
		_, _ = h.Write(raw[:])
		for _, id := range ids {
			binary.LittleEndian.PutUint64(raw[:], uint64(id))
			_, _ = h.Write(raw[:])
		}
	}
	full := h.Sum(nil)
	var out [16]byte
	copy(out[:], full)
	return out
}
func deliveryBatchBytes(batch edgecontrol.DeliveryBatch) int {
	n := 256 + len(batch.Items)*128
	for _, item := range batch.Items {
		n += len(item.UpdateBytes) + 8*(len(item.Channel.AudienceUsers)+len(item.Channel.AffectedUsers))
	}
	return n
}

func deliveryAttemptRetainedBytes(batch edgecontrol.DeliveryBatch, item edgecontrol.DeliveryItem) int64 {
	// attempts retain identities, hashes, receipt state and two instance strings;
	// payload bytes remain owned by the bounded command/fanout task only.
	return int64(320 + len(batch.SourceInstanceID) + len(batch.TargetInstanceID) +
		8*(len(item.Channel.AudienceUsers)+len(item.Channel.AffectedUsers)))
}
func reserveDeliveryBytes(used *atomic.Int64, max int64, bytes int) bool {
	if bytes <= 0 || int64(bytes) > max {
		return false
	}
	for {
		cur := used.Load()
		if cur > max-int64(bytes) {
			return false
		}
		if used.CompareAndSwap(cur, cur+int64(bytes)) {
			return true
		}
	}
}
func (e *deliveryExecutor) close() { e.once.Do(func() { close(e.stop) }); e.done.Wait() }
