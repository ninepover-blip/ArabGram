package egress

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/edgecontrol"
	"telesrv/internal/store"
)

const (
	defaultPTSProjectionActors    = 4
	defaultPTSProjectionMailbox   = 64
	defaultPTSProjectionWindows   = 64
	defaultPTSProjectionItems     = edgecontrol.MaxDeliveryBatchItems
	defaultPTSProjectionBatchWait = 5 * time.Millisecond
)

var errPTSProjectionStopped = errors.New("egress: PTS projection actor stopped")

type ptsWindowProjector interface {
	Project(context.Context, store.OutboxClaimWindow) ([]projectedOutboxItem, error)
}

type ptsProjectionPool struct {
	actors []*ptsProjectionActor
}

func newPTSProjectionPool(events store.DispatchUpdateEventStore, builder OutboxUpdateBuilder) *ptsProjectionPool {
	pool := &ptsProjectionPool{actors: make([]*ptsProjectionActor, defaultPTSProjectionActors)}
	for i := range pool.actors {
		pool.actors[i] = newPTSProjectionActor(events, builder)
	}
	return pool
}

func (p *ptsProjectionPool) Run(ctx context.Context, wait *sync.WaitGroup) {
	if p == nil {
		return
	}
	for _, actor := range p.actors {
		actor.Run(ctx, wait)
	}
}

func (p *ptsProjectionPool) Project(ctx context.Context, window store.OutboxClaimWindow) ([]projectedOutboxItem, error) {
	if p == nil || len(p.actors) == 0 || window.StreamID <= 0 {
		return nil, errOutboxUpdateBuilderMissing
	}
	return p.actors[p.actorIndex(window.StreamID)].Project(ctx, window)
}

func (p *ptsProjectionPool) actorIndex(userID int64) int {
	return int(orderingDomainHash(store.OutboxQueueDispatchPTS, userID) % uint64(len(p.actors)))
}

type ptsProjectionRequest struct {
	ctx    context.Context
	window store.OutboxClaimWindow
	result chan ptsProjectionResult
}

type ptsProjectionResult struct {
	items []projectedOutboxItem
	err   error
}

// ptsProjectionActor coalesces read-only event loading and receiver-specific
// TL construction across independent account lanes. Ordering remains owned by
// deliveryActorExecutor; this actor never mutates a lane, attempt, or PTS fact.
type ptsProjectionActor struct {
	events       store.DispatchUpdateEventStore
	builder      OutboxUpdateBuilder
	mailbox      chan ptsProjectionRequest
	maxWindows   int
	maxItems     int
	batchWait    time.Duration
	done         chan struct{}
	runOnce      sync.Once
	completeOnce sync.Once
}

func newPTSProjectionActor(events store.DispatchUpdateEventStore, builder OutboxUpdateBuilder) *ptsProjectionActor {
	return newPTSProjectionActorWithLimits(
		events, builder,
		defaultPTSProjectionMailbox,
		defaultPTSProjectionWindows,
		defaultPTSProjectionItems,
		defaultPTSProjectionBatchWait,
	)
}

func newPTSProjectionActorWithLimits(
	events store.DispatchUpdateEventStore,
	builder OutboxUpdateBuilder,
	mailbox, maxWindows, maxItems int,
	batchWait time.Duration,
) *ptsProjectionActor {
	if mailbox <= 0 {
		mailbox = 1
	}
	if maxWindows <= 0 {
		maxWindows = 1
	}
	if maxItems <= 0 || maxItems > edgecontrol.MaxDeliveryBatchItems {
		maxItems = edgecontrol.MaxDeliveryBatchItems
	}
	if batchWait < 0 {
		batchWait = 0
	}
	return &ptsProjectionActor{
		events: events, builder: builder,
		mailbox:    make(chan ptsProjectionRequest, mailbox),
		maxWindows: maxWindows, maxItems: maxItems, batchWait: batchWait,
		done: make(chan struct{}),
	}
}

func (a *ptsProjectionActor) Run(ctx context.Context, wait *sync.WaitGroup) {
	if a == nil || wait == nil {
		return
	}
	a.runOnce.Do(func() {
		wait.Add(1)
		go func() {
			defer wait.Done()
			defer a.complete()
			a.loop(ctx)
		}()
	})
}

func (a *ptsProjectionActor) Project(ctx context.Context, window store.OutboxClaimWindow) ([]projectedOutboxItem, error) {
	if a == nil || a.events == nil || a.builder == nil {
		return nil, errOutboxUpdateBuilderMissing
	}
	if ctx == nil {
		ctx = context.Background()
	}
	result := make(chan ptsProjectionResult, 1)
	request := ptsProjectionRequest{ctx: ctx, window: window, result: result}
	select {
	case a.mailbox <- request:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-a.done:
		return nil, errPTSProjectionStopped
	}
	select {
	case projected := <-result:
		return projected.items, projected.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-a.done:
		select {
		case projected := <-result:
			return projected.items, projected.err
		default:
			return nil, errPTSProjectionStopped
		}
	}
}

func (a *ptsProjectionActor) loop(ctx context.Context) {
	var carry *ptsProjectionRequest
	for {
		first, ok := a.next(ctx, &carry)
		if !ok {
			a.failQueued(errPTSProjectionStopped)
			return
		}
		batch := []ptsProjectionRequest{first}
		items := len(first.window.Items)
		timer := time.NewTimer(a.batchWait)
	gather:
		for len(batch) < a.maxWindows && items < a.maxItems {
			select {
			case request := <-a.mailbox:
				requestItems := len(request.window.Items)
				if requestItems > a.maxItems-items {
					carry = &request
					break gather
				}
				batch = append(batch, request)
				items += requestItems
			case <-timer.C:
				break gather
			case <-ctx.Done():
				break gather
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		a.projectBatch(ctx, batch)
		if ctx.Err() != nil {
			if carry != nil {
				completePTSProjection(*carry, nil, errPTSProjectionStopped)
				carry = nil
			}
			a.failQueued(errPTSProjectionStopped)
			return
		}
	}
}

func (a *ptsProjectionActor) next(ctx context.Context, carry **ptsProjectionRequest) (ptsProjectionRequest, bool) {
	if *carry != nil {
		request := **carry
		*carry = nil
		return request, true
	}
	select {
	case request := <-a.mailbox:
		return request, true
	case <-ctx.Done():
		return ptsProjectionRequest{}, false
	}
}

type ptsProjectionWindow struct {
	batchIndex int
	start      int
	length     int
}

type ptsProjectionCursor struct {
	userID int64
	pts    int
}

func (a *ptsProjectionActor) projectBatch(runCtx context.Context, batch []ptsProjectionRequest) {
	cursorCapacity := 0
	for _, request := range batch {
		if len(request.window.Items) > a.maxItems-cursorCapacity {
			cursorCapacity = a.maxItems
			break
		}
		cursorCapacity += len(request.window.Items)
	}
	results := make([]ptsProjectionResult, len(batch))
	valid := make([]ptsProjectionWindow, 0, len(batch))
	cursors := make([]store.EventCursor, 0, cursorCapacity)
	earliest, hasDeadline := time.Time{}, false
	for i, request := range batch {
		if err := request.ctx.Err(); err != nil {
			results[i].err = err
			continue
		}
		var err error
		cursors, err = appendPTSWindowCursors(cursors, request.window)
		if err != nil {
			results[i].err = err
			continue
		}
		valid = append(valid, ptsProjectionWindow{batchIndex: i})
		if deadline, ok := request.ctx.Deadline(); ok && (!hasDeadline || deadline.Before(earliest)) {
			earliest, hasDeadline = deadline, true
		}
	}
	if len(valid) == 0 {
		completePTSProjectionBatch(batch, results)
		return
	}
	batchCtx := runCtx
	cancel := func() {}
	if hasDeadline {
		batchCtx, cancel = context.WithDeadline(runCtx, earliest)
	}
	defer cancel()
	events, err := a.events.BatchByCursor(batchCtx, cursors)
	if err != nil {
		wrapped := fmt.Errorf("egress: load PTS projection batch: %w", err)
		for _, window := range valid {
			results[window.batchIndex].err = wrapped
		}
		completePTSProjectionBatch(batch, results)
		return
	}
	byCursor := make(map[ptsProjectionCursor]domain.UpdateEvent, len(events))
	for _, event := range events {
		byCursor[ptsProjectionCursor{userID: event.UserID, pts: event.Pts}] = event
	}
	flattenedEvents := make([]domain.UpdateEvent, 0, len(cursors))
	flattened := make([]OutboxUpdateRequest, 0, len(cursors))
	projectable := valid[:0]
	for _, window := range valid {
		request := batch[window.batchIndex]
		window.start = len(flattened)
		for _, item := range request.window.Items {
			event, ok := byCursor[ptsProjectionCursor{userID: request.window.StreamID, pts: int(item.Ref.Sequence)}]
			if !ok {
				results[window.batchIndex].err = fmt.Errorf("%w user=%d pts=%d", errMissingOutboxEvent, request.window.StreamID, item.Ref.Sequence)
				break
			}
			payload, ok := item.Payload.(store.DispatchOutboxPayload)
			if !ok || payload.EventType != event.Type {
				results[window.batchIndex].err = fmt.Errorf("egress: durable event identity mismatch item=%d", item.Ref.ItemID)
				break
			}
			flattenedEvents = append(flattenedEvents, event)
			flattened = append(flattened, OutboxUpdateRequest{TargetUserID: request.window.StreamID, Event: event})
		}
		if results[window.batchIndex].err != nil {
			flattenedEvents = flattenedEvents[:window.start]
			flattened = flattened[:window.start]
			continue
		}
		window.length = len(flattened) - window.start
		projectable = append(projectable, window)
	}
	if len(projectable) == 0 {
		completePTSProjectionBatch(batch, results)
		return
	}
	built, err := a.builder(batchCtx, flattened)
	if err != nil {
		wrapped := fmt.Errorf("egress: build PTS projection batch: %w", err)
		for _, window := range projectable {
			results[window.batchIndex].err = wrapped
		}
		completePTSProjectionBatch(batch, results)
		return
	}
	if len(built) != len(flattened) {
		wrapped := fmt.Errorf("%w: got %d want %d", errOutboxUpdateBuilderCount, len(built), len(flattened))
		for _, window := range projectable {
			results[window.batchIndex].err = wrapped
		}
		completePTSProjectionBatch(batch, results)
		return
	}
	for _, window := range projectable {
		request := batch[window.batchIndex]
		projected := make([]projectedOutboxItem, window.length)
		for i := 0; i < window.length; i++ {
			item := request.window.Items[i]
			payload := built[window.start+i]
			if len(payload) == 0 && flattenedEvents[window.start+i].Type != domain.UpdateEventNoop {
				results[window.batchIndex].err = fmt.Errorf("%w item=%d", errOutboxUpdateBuilderEmpty, item.Ref.ItemID)
				break
			}
			projected[i] = projectedOutboxItem{claimed: item, payload: payload, targetUsers: []int64{request.window.StreamID}}
		}
		if results[window.batchIndex].err == nil {
			results[window.batchIndex].items = projected
		}
	}
	completePTSProjectionBatch(batch, results)
}

func appendPTSWindowCursors(cursors []store.EventCursor, window store.OutboxClaimWindow) ([]store.EventCursor, error) {
	if window.QueueKind != store.OutboxQueueDispatchPTS || window.StreamID <= 0 || len(window.Items) == 0 || len(window.Items) > edgecontrol.MaxDeliveryBatchItems {
		return cursors, errors.New("egress: invalid account PTS projection window")
	}
	start := len(cursors)
	for _, item := range window.Items {
		if item.Ref.QueueKind != store.OutboxQueueDispatchPTS || item.Ref.StreamID != window.StreamID ||
			item.Ref.Sequence <= 0 || item.Ref.Sequence > int64(^uint32(0)>>1) {
			return cursors[:start], fmt.Errorf("egress: invalid account PTS %d", item.Ref.Sequence)
		}
		cursors = append(cursors, store.EventCursor{UserID: window.StreamID, Pts: int(item.Ref.Sequence)})
	}
	return cursors, nil
}

func completePTSProjectionBatch(batch []ptsProjectionRequest, results []ptsProjectionResult) {
	for i := range batch {
		completePTSProjection(batch[i], results[i].items, results[i].err)
	}
}

func completePTSProjection(request ptsProjectionRequest, items []projectedOutboxItem, err error) {
	select {
	case request.result <- ptsProjectionResult{items: items, err: err}:
	default:
	}
}

func (a *ptsProjectionActor) failQueued(err error) {
	for {
		select {
		case request := <-a.mailbox:
			completePTSProjection(request, nil, err)
		default:
			return
		}
	}
}

func (a *ptsProjectionActor) complete() {
	a.completeOnce.Do(func() { close(a.done) })
}
