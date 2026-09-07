package egress

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"telesrv/internal/store"
)

const (
	mutationActorQueueRequests = 1024
	mutationActorQueuedItems   = int64(4096)
	mutationActorFlushInterval = 4 * time.Millisecond
	mutationFinalizeQueuedRefs = 4096
	mutationFinalizePartitions = 4
)

var ErrMutationActorCapacity = errors.New("egress: mutation actor capacity exhausted")

// AttemptMutationWriter is the only Egress entry point for high-rate attempt
// bind/evidence mutations. PostgreSQL remains authoritative; the actor merely
// combines independent ordering domains into fewer set-based transactions.
type AttemptMutationWriter interface {
	BindAttemptTargets(context.Context, store.OutboxQueueKind, []store.OutboxAttemptTargetSet) ([]store.OutboxBindTargetResult, error)
	RecordAttemptEvidenceBatch(context.Context, store.OutboxQueueKind, []store.OutboxAttemptEvidence) ([]store.OutboxEvidenceResult, error)
	ResolveTargetAttemptBatch(context.Context, store.OutboxQueueKind, []store.OutboxTargetAttemptResolution) ([]store.OutboxResolutionResult, error)
	ResolveOwnedAttemptBatch(context.Context, store.OutboxQueueKind, []store.OutboxOwnedAttemptResolution) ([]store.OutboxResolutionResult, error)
}

type bindMutationResponse struct {
	results []store.OutboxBindTargetResult
	err     error
}

type bindMutationRequest struct {
	ctx      context.Context
	items    []store.OutboxAttemptTargetSet
	response chan bindMutationResponse
}

type evidenceMutationResponse struct {
	results []store.OutboxEvidenceResult
	err     error
}

type evidenceMutationRequest struct {
	ctx      context.Context
	items    []store.OutboxAttemptEvidence
	response chan evidenceMutationResponse
}

type targetResolutionResponse struct {
	results []store.OutboxResolutionResult
	err     error
}

type targetResolutionRequest struct {
	ctx      context.Context
	items    []store.OutboxTargetAttemptResolution
	response chan targetResolutionResponse
}

type ownedResolutionResponse struct {
	results []store.OutboxResolutionResult
	err     error
}

type ownedResolutionRequest struct {
	ctx      context.Context
	items    []store.OutboxOwnedAttemptResolution
	response chan ownedResolutionResponse
}

type attemptMutationLane struct {
	store       store.DurableOutboxStateStore
	kind        store.OutboxQueueKind
	bind        chan bindMutationRequest
	evidence    chan evidenceMutationRequest
	target      chan targetResolutionRequest
	owned       chan ownedResolutionRequest
	finalize    [mutationFinalizePartitions]chan []store.OutboxAttemptRef
	finalSlots  chan struct{}
	queuedItems *atomic.Int64
	metrics     Metrics
}

// AttemptMutationBatcher owns twenty-four fixed actors: four mutation
// operations plus four stable-hash exact-ref finalizers for each of the three
// queue kinds. It never
// creates a goroutine per request and never turns memory admission into a
// durable completion fact.
type AttemptMutationBatcher struct {
	lanes       [4]attemptMutationLane
	queuedItems atomic.Int64
	startOnce   sync.Once
	started     chan struct{}
	done        chan struct{}
}

func NewAttemptMutationBatcher(
	dispatch store.DispatchOutboxStore,
	absolute store.DeliveryOutboxStore,
	channel store.ChannelDeliveryStore,
	metrics Metrics,
) (*AttemptMutationBatcher, error) {
	if dispatch == nil || absolute == nil || channel == nil {
		return nil, ErrMissingDependency
	}
	b := &AttemptMutationBatcher{started: make(chan struct{}), done: make(chan struct{})}
	if metrics == nil {
		metrics = nopMetrics{}
	}
	stores := [4]store.DurableOutboxStateStore{
		store.OutboxQueueDispatchPTS:      dispatch,
		store.OutboxQueueAbsoluteDelivery: absolute,
		store.OutboxQueueChannelPTS:       channel,
	}
	for kind := store.OutboxQueueDispatchPTS; kind <= store.OutboxQueueChannelPTS; kind++ {
		lane := attemptMutationLane{
			store:       stores[kind],
			kind:        kind,
			bind:        make(chan bindMutationRequest, mutationActorQueueRequests),
			evidence:    make(chan evidenceMutationRequest, mutationActorQueueRequests),
			target:      make(chan targetResolutionRequest, mutationActorQueueRequests),
			owned:       make(chan ownedResolutionRequest, mutationActorQueueRequests),
			finalSlots:  make(chan struct{}, mutationFinalizeQueuedRefs),
			queuedItems: &b.queuedItems,
			metrics:     metrics,
		}
		for partition := range lane.finalize {
			lane.finalize[partition] = make(chan []store.OutboxAttemptRef, mutationActorQueueRequests)
		}
		b.lanes[kind] = lane
	}
	return b, nil
}

// Run starts the complete fixed actor set and blocks until shutdown. Calling it
// more than once does not create a second mutation owner.
func (b *AttemptMutationBatcher) Run(ctx context.Context) {
	if b == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	b.startOnce.Do(func() {
		var wait sync.WaitGroup
		for kind := store.OutboxQueueDispatchPTS; kind <= store.OutboxQueueChannelPTS; kind++ {
			lane := &b.lanes[kind]
			wait.Add(4 + mutationFinalizePartitions)
			go func() {
				defer wait.Done()
				lane.runBind(ctx)
			}()
			go func() {
				defer wait.Done()
				lane.runEvidence(ctx)
			}()
			go func() {
				defer wait.Done()
				lane.runTargetResolution(ctx)
			}()
			go func() {
				defer wait.Done()
				lane.runOwnedResolution(ctx)
			}()
			for partition := range lane.finalize {
				input := lane.finalize[partition]
				go func() {
					defer wait.Done()
					lane.runFinalizer(ctx, input)
				}()
			}
		}
		close(b.started)
		wait.Wait()
		close(b.done)
	})
}

func (b *AttemptMutationBatcher) WaitReady(ctx context.Context) error {
	if b == nil {
		return ErrMissingDependency
	}
	select {
	case <-b.started:
		return nil
	case <-b.done:
		return context.Canceled
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *AttemptMutationBatcher) Wait() {
	if b != nil {
		<-b.done
	}
}

func (b *AttemptMutationBatcher) BindAttemptTargets(
	ctx context.Context,
	kind store.OutboxQueueKind,
	sets []store.OutboxAttemptTargetSet,
) ([]store.OutboxBindTargetResult, error) {
	if len(sets) == 0 {
		return []store.OutboxBindTargetResult{}, nil
	}
	lane, err := b.lane(kind, len(sets))
	if err != nil {
		return nil, err
	}
	if !lane.reserve(len(sets)) {
		return nil, ErrMutationActorCapacity
	}
	request := bindMutationRequest{ctx: ctx, items: sets, response: make(chan bindMutationResponse, 1)}
	select {
	case lane.bind <- request:
	case <-ctx.Done():
		lane.release(len(sets))
		return nil, ctx.Err()
	case <-b.done:
		lane.release(len(sets))
		return nil, context.Canceled
	}
	select {
	case response := <-request.response:
		return response.results, response.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.done:
		return nil, context.Canceled
	}
}

func (b *AttemptMutationBatcher) RecordAttemptEvidenceBatch(
	ctx context.Context,
	kind store.OutboxQueueKind,
	evidence []store.OutboxAttemptEvidence,
) ([]store.OutboxEvidenceResult, error) {
	if len(evidence) == 0 {
		return []store.OutboxEvidenceResult{}, nil
	}
	lane, err := b.lane(kind, len(evidence))
	if err != nil {
		return nil, err
	}
	if !lane.reserve(len(evidence)) {
		return nil, ErrMutationActorCapacity
	}
	request := evidenceMutationRequest{ctx: ctx, items: evidence, response: make(chan evidenceMutationResponse, 1)}
	select {
	case lane.evidence <- request:
	case <-ctx.Done():
		lane.release(len(evidence))
		return nil, ctx.Err()
	case <-b.done:
		lane.release(len(evidence))
		return nil, context.Canceled
	}
	select {
	case response := <-request.response:
		return response.results, response.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.done:
		return nil, context.Canceled
	}
}

func (b *AttemptMutationBatcher) ResolveTargetAttemptBatch(
	ctx context.Context,
	kind store.OutboxQueueKind,
	resolutions []store.OutboxTargetAttemptResolution,
) ([]store.OutboxResolutionResult, error) {
	if len(resolutions) == 0 {
		return []store.OutboxResolutionResult{}, nil
	}
	lane, err := b.lane(kind, len(resolutions))
	if err != nil {
		return nil, err
	}
	if !lane.reserve(len(resolutions)) {
		return nil, ErrMutationActorCapacity
	}
	request := targetResolutionRequest{ctx: ctx, items: resolutions, response: make(chan targetResolutionResponse, 1)}
	select {
	case lane.target <- request:
	case <-ctx.Done():
		lane.release(len(resolutions))
		return nil, ctx.Err()
	case <-b.done:
		lane.release(len(resolutions))
		return nil, context.Canceled
	}
	select {
	case response := <-request.response:
		return response.results, response.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.done:
		return nil, context.Canceled
	}
}

func (b *AttemptMutationBatcher) ResolveOwnedAttemptBatch(
	ctx context.Context,
	kind store.OutboxQueueKind,
	resolutions []store.OutboxOwnedAttemptResolution,
) ([]store.OutboxResolutionResult, error) {
	if len(resolutions) == 0 {
		return []store.OutboxResolutionResult{}, nil
	}
	lane, err := b.lane(kind, len(resolutions))
	if err != nil {
		return nil, err
	}
	if !lane.reserve(len(resolutions)) {
		return nil, ErrMutationActorCapacity
	}
	request := ownedResolutionRequest{ctx: ctx, items: resolutions, response: make(chan ownedResolutionResponse, 1)}
	select {
	case lane.owned <- request:
	case <-ctx.Done():
		lane.release(len(resolutions))
		return nil, ctx.Err()
	case <-b.done:
		lane.release(len(resolutions))
		return nil, context.Canceled
	}
	select {
	case response := <-request.response:
		return response.results, response.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.done:
		return nil, context.Canceled
	}
}

func (b *AttemptMutationBatcher) lane(kind store.OutboxQueueKind, count int) (*attemptMutationLane, error) {
	if b == nil || kind < store.OutboxQueueDispatchPTS || kind > store.OutboxQueueChannelPTS || b.lanes[kind].store == nil {
		return nil, ErrMissingDependency
	}
	if count <= 0 || count > store.MaxDeliveryBatchItems {
		return nil, fmt.Errorf("egress: mutation batch size must be in [1,%d]", store.MaxDeliveryBatchItems)
	}
	return &b.lanes[kind], nil
}

func (l *attemptMutationLane) reserve(count int) bool {
	if l.queuedItems == nil {
		return false
	}
	want := int64(count)
	for {
		used := l.queuedItems.Load()
		if used > mutationActorQueuedItems-want {
			return false
		}
		if l.queuedItems.CompareAndSwap(used, used+want) {
			return true
		}
	}
}

func (l *attemptMutationLane) release(count int) {
	if count > 0 && (l.queuedItems == nil || l.queuedItems.Add(-int64(count)) < 0) {
		panic("egress: mutation actor reservation underflow")
	}
}

func (l *attemptMutationLane) runBind(ctx context.Context) {
	var carry *bindMutationRequest
	for {
		first, ok := l.nextBind(ctx, carry)
		carry = nil
		if !ok {
			l.drainBind(context.Canceled)
			return
		}
		requests := []bindMutationRequest{first}
		items := append([]store.OutboxAttemptTargetSet(nil), first.items...)
		refs := bindRequestRefs(first)
		timer := time.NewTimer(mutationActorFlushInterval)
	collect:
		for len(items) < store.MaxDeliveryBatchItems {
			select {
			case candidate := <-l.bind:
				if err := candidate.ctx.Err(); err != nil {
					l.completeBind(candidate, nil, err)
					continue
				}
				if len(items)+len(candidate.items) > store.MaxDeliveryBatchItems || bindRefsOverlap(refs, candidate) {
					carry = &candidate
					break collect
				}
				requests = append(requests, candidate)
				items = append(items, candidate.items...)
				addBindRefs(refs, candidate)
			case <-timer.C:
				break collect
			case <-ctx.Done():
				stopMutationTimer(timer)
				for _, request := range requests {
					l.completeBind(request, nil, context.Canceled)
				}
				if carry != nil {
					l.completeBind(*carry, nil, context.Canceled)
					carry = nil
				}
				l.drainBind(context.Canceled)
				return
			}
		}
		stopMutationTimer(timer)
		execCtx, cancel := mutationBatchContext(ctx, bindRequestContexts(requests))
		started := time.Now()
		results, err := l.store.BindAttemptTargets(execCtx, items)
		l.observe("mutation_bind", started, len(items))
		cancel()
		if err == nil && len(results) != len(items) {
			err = fmt.Errorf("egress: bind mutation result count %d want %d", len(results), len(items))
		}
		if err == nil {
			err = l.enqueueFinalizable(ctx, finalizableBindRefs(items, results))
		}
		offset := 0
		for _, request := range requests {
			var slice []store.OutboxBindTargetResult
			if err == nil {
				slice = append([]store.OutboxBindTargetResult(nil), results[offset:offset+len(request.items)]...)
			}
			offset += len(request.items)
			l.completeBind(request, slice, err)
		}
	}
}

func (l *attemptMutationLane) runEvidence(ctx context.Context) {
	var carry *evidenceMutationRequest
	for {
		first, ok := l.nextEvidence(ctx, carry)
		carry = nil
		if !ok {
			l.drainEvidence(context.Canceled)
			return
		}
		requests := []evidenceMutationRequest{first}
		items := append([]store.OutboxAttemptEvidence(nil), first.items...)
		refs := evidenceRequestRefs(first)
		timer := time.NewTimer(mutationActorFlushInterval)
	collect:
		for len(items) < store.MaxDeliveryBatchItems {
			select {
			case candidate := <-l.evidence:
				if err := candidate.ctx.Err(); err != nil {
					l.completeEvidence(candidate, nil, err)
					continue
				}
				if len(items)+len(candidate.items) > store.MaxDeliveryBatchItems || evidenceRefsOverlap(refs, candidate) {
					carry = &candidate
					break collect
				}
				requests = append(requests, candidate)
				items = append(items, candidate.items...)
				addEvidenceRefs(refs, candidate)
			case <-timer.C:
				break collect
			case <-ctx.Done():
				stopMutationTimer(timer)
				for _, request := range requests {
					l.completeEvidence(request, nil, context.Canceled)
				}
				if carry != nil {
					l.completeEvidence(*carry, nil, context.Canceled)
					carry = nil
				}
				l.drainEvidence(context.Canceled)
				return
			}
		}
		stopMutationTimer(timer)
		execCtx, cancel := mutationBatchContext(ctx, evidenceRequestContexts(requests))
		started := time.Now()
		results, err := l.store.RecordAttemptEvidenceBatch(execCtx, items)
		l.observe("mutation_evidence", started, len(items))
		cancel()
		if err == nil && len(results) != len(items) {
			err = fmt.Errorf("egress: evidence mutation result count %d want %d", len(results), len(items))
		}
		if err == nil {
			err = l.enqueueFinalizable(ctx, finalizableEvidenceRefs(results))
		}
		offset := 0
		for _, request := range requests {
			var slice []store.OutboxEvidenceResult
			if err == nil {
				slice = append([]store.OutboxEvidenceResult(nil), results[offset:offset+len(request.items)]...)
			}
			offset += len(request.items)
			l.completeEvidence(request, slice, err)
		}
	}
}

func (l *attemptMutationLane) runTargetResolution(ctx context.Context) {
	var carry *targetResolutionRequest
	for {
		first, ok := l.nextTargetResolution(ctx, carry)
		carry = nil
		if !ok {
			l.drainTargetResolution(context.Canceled)
			return
		}
		requests := []targetResolutionRequest{first}
		items := append([]store.OutboxTargetAttemptResolution(nil), first.items...)
		refs := targetResolutionRefs(first)
		timer := time.NewTimer(mutationActorFlushInterval)
	collect:
		for len(items) < store.MaxDeliveryBatchItems {
			select {
			case candidate := <-l.target:
				if err := candidate.ctx.Err(); err != nil {
					l.completeTargetResolution(candidate, nil, err)
					continue
				}
				if len(items)+len(candidate.items) > store.MaxDeliveryBatchItems || targetResolutionRefsOverlap(refs, candidate) {
					carry = &candidate
					break collect
				}
				requests = append(requests, candidate)
				items = append(items, candidate.items...)
				addTargetResolutionRefs(refs, candidate)
			case <-timer.C:
				break collect
			case <-ctx.Done():
				stopMutationTimer(timer)
				for _, request := range requests {
					l.completeTargetResolution(request, nil, context.Canceled)
				}
				if carry != nil {
					l.completeTargetResolution(*carry, nil, context.Canceled)
				}
				l.drainTargetResolution(context.Canceled)
				return
			}
		}
		stopMutationTimer(timer)
		execCtx, cancel := mutationBatchContext(ctx, targetResolutionContexts(requests))
		started := time.Now()
		results, err := l.store.ResolveTargetAttemptBatch(execCtx, items)
		l.observe("mutation_resolve_target", started, len(items))
		cancel()
		if err == nil && len(results) != len(items) {
			err = fmt.Errorf("egress: target resolution result count %d want %d", len(results), len(items))
		}
		if err == nil {
			err = l.enqueueFinalizable(ctx, finalizableResolutionRefs(results))
		}
		offset := 0
		for _, request := range requests {
			var slice []store.OutboxResolutionResult
			if err == nil {
				slice = append([]store.OutboxResolutionResult(nil), results[offset:offset+len(request.items)]...)
			}
			offset += len(request.items)
			l.completeTargetResolution(request, slice, err)
		}
	}
}

func (l *attemptMutationLane) runOwnedResolution(ctx context.Context) {
	var carry *ownedResolutionRequest
	for {
		first, ok := l.nextOwnedResolution(ctx, carry)
		carry = nil
		if !ok {
			l.drainOwnedResolution(context.Canceled)
			return
		}
		requests := []ownedResolutionRequest{first}
		items := append([]store.OutboxOwnedAttemptResolution(nil), first.items...)
		refs := ownedResolutionRefs(first)
		timer := time.NewTimer(mutationActorFlushInterval)
	collect:
		for len(items) < store.MaxDeliveryBatchItems {
			select {
			case candidate := <-l.owned:
				if err := candidate.ctx.Err(); err != nil {
					l.completeOwnedResolution(candidate, nil, err)
					continue
				}
				if len(items)+len(candidate.items) > store.MaxDeliveryBatchItems || ownedResolutionRefsOverlap(refs, candidate) {
					carry = &candidate
					break collect
				}
				requests = append(requests, candidate)
				items = append(items, candidate.items...)
				addOwnedResolutionRefs(refs, candidate)
			case <-timer.C:
				break collect
			case <-ctx.Done():
				stopMutationTimer(timer)
				for _, request := range requests {
					l.completeOwnedResolution(request, nil, context.Canceled)
				}
				if carry != nil {
					l.completeOwnedResolution(*carry, nil, context.Canceled)
				}
				l.drainOwnedResolution(context.Canceled)
				return
			}
		}
		stopMutationTimer(timer)
		execCtx, cancel := mutationBatchContext(ctx, ownedResolutionContexts(requests))
		started := time.Now()
		results, err := l.store.ResolveOwnedAttemptBatch(execCtx, items)
		l.observe("mutation_resolve_owned", started, len(items))
		cancel()
		if err == nil && len(results) != len(items) {
			err = fmt.Errorf("egress: owned resolution result count %d want %d", len(results), len(items))
		}
		if err == nil {
			err = l.enqueueFinalizable(ctx, finalizableResolutionRefs(results))
		}
		offset := 0
		for _, request := range requests {
			var slice []store.OutboxResolutionResult
			if err == nil {
				slice = append([]store.OutboxResolutionResult(nil), results[offset:offset+len(request.items)]...)
			}
			offset += len(request.items)
			l.completeOwnedResolution(request, slice, err)
		}
	}
}

func (l *attemptMutationLane) runFinalizer(ctx context.Context, input <-chan []store.OutboxAttemptRef) {
	var carry []store.OutboxAttemptRef
	for {
		var first []store.OutboxAttemptRef
		if len(carry) > 0 {
			first, carry = carry, nil
		} else {
			select {
			case first = <-input:
			case <-ctx.Done():
				l.drainFinalize(input)
				return
			}
		}
		reserved := len(first)
		refs := append([]store.OutboxAttemptRef(nil), first...)
		timer := time.NewTimer(mutationActorFlushInterval)
	collect:
		for len(refs) < store.MaxDeliveryBatchItems {
			select {
			case candidate := <-input:
				if len(refs)+len(candidate) > store.MaxDeliveryBatchItems {
					carry = candidate
					break collect
				}
				refs = append(refs, candidate...)
				reserved += len(candidate)
			case <-timer.C:
				break collect
			case <-ctx.Done():
				stopMutationTimer(timer)
				l.releaseFinalizeSlots(reserved)
				if len(carry) > 0 {
					l.releaseFinalizeSlots(len(carry))
				}
				l.drainFinalize(input)
				return
			}
		}
		stopMutationTimer(timer)
		unique := uniqueFinalizeRefs(refs)
		requests := make([]store.OutboxFinalizeRequest, len(unique))
		for i := range unique {
			requests[i] = store.OutboxFinalizeRequest{Ref: unique[i]}
		}
		backoff := outboxScheduleErrorBackoff
		for {
			started := time.Now()
			batch, err := l.store.FinalizeAttempts(ctx, requests)
			l.observe("finalize_exact", started, len(requests))
			if err == nil && len(batch.Results) != len(requests) {
				err = fmt.Errorf("egress: exact finalize result count %d want %d", len(batch.Results), len(requests))
			}
			if err == nil && len(batch.Next) != 0 {
				err = errors.New("egress: exact finalizer unexpectedly retained a lane lease")
			}
			if err == nil {
				l.releaseFinalizeSlots(reserved)
				break
			}
			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				stopMutationTimer(timer)
				l.releaseFinalizeSlots(reserved)
				if len(carry) > 0 {
					l.releaseFinalizeSlots(len(carry))
				}
				l.drainFinalize(input)
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

func (l *attemptMutationLane) nextBind(ctx context.Context, carry *bindMutationRequest) (bindMutationRequest, bool) {
	if carry != nil {
		return *carry, true
	}
	for {
		select {
		case request := <-l.bind:
			if err := request.ctx.Err(); err != nil {
				l.completeBind(request, nil, err)
				continue
			}
			return request, true
		case <-ctx.Done():
			return bindMutationRequest{}, false
		}
	}
}

func (l *attemptMutationLane) nextEvidence(ctx context.Context, carry *evidenceMutationRequest) (evidenceMutationRequest, bool) {
	if carry != nil {
		return *carry, true
	}
	for {
		select {
		case request := <-l.evidence:
			if err := request.ctx.Err(); err != nil {
				l.completeEvidence(request, nil, err)
				continue
			}
			return request, true
		case <-ctx.Done():
			return evidenceMutationRequest{}, false
		}
	}
}

func (l *attemptMutationLane) nextTargetResolution(ctx context.Context, carry *targetResolutionRequest) (targetResolutionRequest, bool) {
	if carry != nil {
		return *carry, true
	}
	for {
		select {
		case request := <-l.target:
			if err := request.ctx.Err(); err != nil {
				l.completeTargetResolution(request, nil, err)
				continue
			}
			return request, true
		case <-ctx.Done():
			return targetResolutionRequest{}, false
		}
	}
}

func (l *attemptMutationLane) nextOwnedResolution(ctx context.Context, carry *ownedResolutionRequest) (ownedResolutionRequest, bool) {
	if carry != nil {
		return *carry, true
	}
	for {
		select {
		case request := <-l.owned:
			if err := request.ctx.Err(); err != nil {
				l.completeOwnedResolution(request, nil, err)
				continue
			}
			return request, true
		case <-ctx.Done():
			return ownedResolutionRequest{}, false
		}
	}
}

func (l *attemptMutationLane) completeBind(request bindMutationRequest, results []store.OutboxBindTargetResult, err error) {
	l.release(len(request.items))
	request.response <- bindMutationResponse{results: results, err: err}
}

func (l *attemptMutationLane) completeEvidence(request evidenceMutationRequest, results []store.OutboxEvidenceResult, err error) {
	l.release(len(request.items))
	request.response <- evidenceMutationResponse{results: results, err: err}
}

func (l *attemptMutationLane) completeTargetResolution(request targetResolutionRequest, results []store.OutboxResolutionResult, err error) {
	l.release(len(request.items))
	request.response <- targetResolutionResponse{results: results, err: err}
}

func (l *attemptMutationLane) completeOwnedResolution(request ownedResolutionRequest, results []store.OutboxResolutionResult, err error) {
	l.release(len(request.items))
	request.response <- ownedResolutionResponse{results: results, err: err}
}

func (l *attemptMutationLane) drainBind(err error) {
	for {
		select {
		case request := <-l.bind:
			l.completeBind(request, nil, err)
		default:
			return
		}
	}
}

func (l *attemptMutationLane) drainEvidence(err error) {
	for {
		select {
		case request := <-l.evidence:
			l.completeEvidence(request, nil, err)
		default:
			return
		}
	}
}

func (l *attemptMutationLane) drainTargetResolution(err error) {
	for {
		select {
		case request := <-l.target:
			l.completeTargetResolution(request, nil, err)
		default:
			return
		}
	}
}

func (l *attemptMutationLane) drainOwnedResolution(err error) {
	for {
		select {
		case request := <-l.owned:
			l.completeOwnedResolution(request, nil, err)
		default:
			return
		}
	}
}

func (l *attemptMutationLane) enqueueFinalizable(ctx context.Context, refs []store.OutboxAttemptRef) error {
	refs = uniqueFinalizeRefs(refs)
	if len(refs) == 0 {
		return nil
	}
	reserved := 0
	for range refs {
		select {
		case l.finalSlots <- struct{}{}:
			reserved++
		case <-ctx.Done():
			l.releaseFinalizeSlots(reserved)
			return ctx.Err()
		}
	}
	var groups [mutationFinalizePartitions][]store.OutboxAttemptRef
	for _, ref := range refs {
		partition := uint64(ref.StreamID) % mutationFinalizePartitions
		groups[partition] = append(groups[partition], ref)
	}
	sent := 0
	for partition, group := range groups {
		if len(group) == 0 {
			continue
		}
		select {
		case l.finalize[partition] <- group:
			sent += len(group)
		case <-ctx.Done():
			l.releaseFinalizeSlots(reserved - sent)
			return ctx.Err()
		}
	}
	return nil
}

func (l *attemptMutationLane) releaseFinalizeSlots(count int) {
	for i := 0; i < count; i++ {
		select {
		case <-l.finalSlots:
		default:
			panic("egress: exact finalizer reservation underflow")
		}
	}
}

func (l *attemptMutationLane) drainFinalize(input <-chan []store.OutboxAttemptRef) {
	for {
		select {
		case refs := <-input:
			l.releaseFinalizeSlots(len(refs))
		default:
			return
		}
	}
}

func (l *attemptMutationLane) observe(stage string, started time.Time, items int) {
	if l.metrics != nil {
		l.metrics.OutboxStage(outboxQueueMetric(l.kind), stage, time.Since(started), items)
	}
}

func bindRequestRefs(request bindMutationRequest) map[store.OutboxAttemptRef]struct{} {
	refs := make(map[store.OutboxAttemptRef]struct{}, len(request.items))
	addBindRefs(refs, request)
	return refs
}

func addBindRefs(refs map[store.OutboxAttemptRef]struct{}, request bindMutationRequest) {
	for _, item := range request.items {
		refs[item.Ref] = struct{}{}
	}
}

func bindRefsOverlap(refs map[store.OutboxAttemptRef]struct{}, request bindMutationRequest) bool {
	for _, item := range request.items {
		if _, ok := refs[item.Ref]; ok {
			return true
		}
	}
	return false
}

func evidenceRequestRefs(request evidenceMutationRequest) map[store.OutboxAttemptRef]struct{} {
	refs := make(map[store.OutboxAttemptRef]struct{}, len(request.items))
	addEvidenceRefs(refs, request)
	return refs
}

func addEvidenceRefs(refs map[store.OutboxAttemptRef]struct{}, request evidenceMutationRequest) {
	for _, item := range request.items {
		refs[item.Ref] = struct{}{}
	}
}

func evidenceRefsOverlap(refs map[store.OutboxAttemptRef]struct{}, request evidenceMutationRequest) bool {
	for _, item := range request.items {
		if _, ok := refs[item.Ref]; ok {
			return true
		}
	}
	return false
}

func targetResolutionRefs(request targetResolutionRequest) map[store.OutboxAttemptRef]struct{} {
	refs := make(map[store.OutboxAttemptRef]struct{}, len(request.items))
	addTargetResolutionRefs(refs, request)
	return refs
}

func addTargetResolutionRefs(refs map[store.OutboxAttemptRef]struct{}, request targetResolutionRequest) {
	for _, item := range request.items {
		refs[item.Ref] = struct{}{}
	}
}

func targetResolutionRefsOverlap(refs map[store.OutboxAttemptRef]struct{}, request targetResolutionRequest) bool {
	for _, item := range request.items {
		if _, ok := refs[item.Ref]; ok {
			return true
		}
	}
	return false
}

func ownedResolutionRefs(request ownedResolutionRequest) map[store.OutboxAttemptRef]struct{} {
	refs := make(map[store.OutboxAttemptRef]struct{}, len(request.items))
	addOwnedResolutionRefs(refs, request)
	return refs
}

func addOwnedResolutionRefs(refs map[store.OutboxAttemptRef]struct{}, request ownedResolutionRequest) {
	for _, item := range request.items {
		refs[item.Ref] = struct{}{}
	}
}

func ownedResolutionRefsOverlap(refs map[store.OutboxAttemptRef]struct{}, request ownedResolutionRequest) bool {
	for _, item := range request.items {
		if _, ok := refs[item.Ref]; ok {
			return true
		}
	}
	return false
}

func finalizableBindRefs(items []store.OutboxAttemptTargetSet, results []store.OutboxBindTargetResult) []store.OutboxAttemptRef {
	refs := make([]store.OutboxAttemptRef, 0, len(results))
	for i, result := range results {
		if i >= len(items) || len(items[i].Targets) != 0 {
			continue
		}
		if result.Outcome == store.OutboxBindTargetBound || result.Outcome == store.OutboxBindTargetDuplicate {
			refs = append(refs, result.Ref)
		}
	}
	return refs
}

func finalizableEvidenceRefs(results []store.OutboxEvidenceResult) []store.OutboxAttemptRef {
	refs := make([]store.OutboxAttemptRef, 0, len(results))
	for _, result := range results {
		if result.Outcome == store.OutboxEvidenceRecorded || result.Outcome == store.OutboxEvidenceDuplicate {
			refs = append(refs, result.Ref)
		}
	}
	return refs
}

func finalizableResolutionRefs(results []store.OutboxResolutionResult) []store.OutboxAttemptRef {
	refs := make([]store.OutboxAttemptRef, 0, len(results))
	for _, result := range results {
		if result.Outcome == store.OutboxResolutionRecorded || result.Outcome == store.OutboxResolutionDuplicate {
			refs = append(refs, result.Ref)
		}
	}
	return refs
}

func uniqueFinalizeRefs(refs []store.OutboxAttemptRef) []store.OutboxAttemptRef {
	if len(refs) < 2 {
		return refs
	}
	seen := make(map[store.OutboxAttemptRef]struct{}, len(refs))
	unique := make([]store.OutboxAttemptRef, 0, len(refs))
	for _, ref := range refs {
		if _, ok := seen[ref]; ok {
			continue
		}
		seen[ref] = struct{}{}
		unique = append(unique, ref)
	}
	return unique
}

func bindRequestContexts(requests []bindMutationRequest) []context.Context {
	out := make([]context.Context, len(requests))
	for i := range requests {
		out[i] = requests[i].ctx
	}
	return out
}

func evidenceRequestContexts(requests []evidenceMutationRequest) []context.Context {
	out := make([]context.Context, len(requests))
	for i := range requests {
		out[i] = requests[i].ctx
	}
	return out
}

func targetResolutionContexts(requests []targetResolutionRequest) []context.Context {
	out := make([]context.Context, len(requests))
	for i := range requests {
		out[i] = requests[i].ctx
	}
	return out
}

func ownedResolutionContexts(requests []ownedResolutionRequest) []context.Context {
	out := make([]context.Context, len(requests))
	for i := range requests {
		out[i] = requests[i].ctx
	}
	return out
}

func mutationBatchContext(parent context.Context, callers []context.Context) (context.Context, context.CancelFunc) {
	deadline := time.Time{}
	for _, caller := range callers {
		if current, ok := caller.Deadline(); ok && (deadline.IsZero() || current.Before(deadline)) {
			deadline = current
		}
	}
	if !deadline.IsZero() {
		return context.WithDeadline(parent, deadline)
	}
	return context.WithCancel(parent)
}

func stopMutationTimer(timer *time.Timer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}

var _ AttemptMutationWriter = (*AttemptMutationBatcher)(nil)
