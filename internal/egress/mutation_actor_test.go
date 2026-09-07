package egress

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"telesrv/internal/store"
)

type directAttemptMutationWriter struct {
	stores [4]store.DurableOutboxStateStore
}

func directMutations(
	dispatch store.DurableOutboxStateStore,
	absolute store.DurableOutboxStateStore,
	channel store.DurableOutboxStateStore,
) *directAttemptMutationWriter {
	return &directAttemptMutationWriter{stores: [4]store.DurableOutboxStateStore{
		store.OutboxQueueDispatchPTS: dispatch, store.OutboxQueueAbsoluteDelivery: absolute, store.OutboxQueueChannelPTS: channel,
	}}
}

func (w *directAttemptMutationWriter) BindAttemptTargets(ctx context.Context, kind store.OutboxQueueKind, sets []store.OutboxAttemptTargetSet) ([]store.OutboxBindTargetResult, error) {
	return w.stores[kind].BindAttemptTargets(ctx, sets)
}

func (w *directAttemptMutationWriter) RecordAttemptEvidenceBatch(ctx context.Context, kind store.OutboxQueueKind, evidence []store.OutboxAttemptEvidence) ([]store.OutboxEvidenceResult, error) {
	return w.stores[kind].RecordAttemptEvidenceBatch(ctx, evidence)
}

func (w *directAttemptMutationWriter) ResolveTargetAttemptBatch(ctx context.Context, kind store.OutboxQueueKind, resolutions []store.OutboxTargetAttemptResolution) ([]store.OutboxResolutionResult, error) {
	return w.stores[kind].ResolveTargetAttemptBatch(ctx, resolutions)
}

func (w *directAttemptMutationWriter) ResolveOwnedAttemptBatch(ctx context.Context, kind store.OutboxQueueKind, resolutions []store.OutboxOwnedAttemptResolution) ([]store.OutboxResolutionResult, error) {
	return w.stores[kind].ResolveOwnedAttemptBatch(ctx, resolutions)
}

type mutationStoreCapture struct {
	store.DurableOutboxStateStore
	mu               sync.Mutex
	bindCalls        int
	evidenceCalls    int
	maxBindBatch     int
	maxEvidenceBatch int
	bindSets         []store.OutboxAttemptTargetSet
	finalizeCalls    int
	finalizedRefs    []store.OutboxAttemptRef
	maxFinalizeBatch int
	finalizeFailures int
	targetCalls      int
	ownedCalls       int
}

func (s *mutationStoreCapture) FinalizeAttempts(_ context.Context, requests []store.OutboxFinalizeRequest) (store.OutboxFinalizeBatch, error) {
	s.mu.Lock()
	s.finalizeCalls++
	if len(requests) > s.maxFinalizeBatch {
		s.maxFinalizeBatch = len(requests)
	}
	if s.finalizeFailures > 0 {
		s.finalizeFailures--
		s.mu.Unlock()
		return store.OutboxFinalizeBatch{}, errors.New("transient finalize failure")
	}
	results := make([]store.OutboxFinalizeResult, len(requests))
	for i := range requests {
		s.finalizedRefs = append(s.finalizedRefs, requests[i].Ref)
		results[i] = store.OutboxFinalizeResult{Ref: requests[i].Ref, Outcome: store.OutboxFinalizeApplied}
	}
	s.mu.Unlock()
	return store.OutboxFinalizeBatch{Results: results}, nil
}

func (s *mutationStoreCapture) ResolveTargetAttemptBatch(_ context.Context, resolutions []store.OutboxTargetAttemptResolution) ([]store.OutboxResolutionResult, error) {
	s.mu.Lock()
	s.targetCalls++
	s.mu.Unlock()
	results := make([]store.OutboxResolutionResult, len(resolutions))
	for i := range resolutions {
		results[i] = store.OutboxResolutionResult{Ref: resolutions[i].Ref, Outcome: store.OutboxResolutionRecorded}
	}
	return results, nil
}

func (s *mutationStoreCapture) ResolveOwnedAttemptBatch(_ context.Context, resolutions []store.OutboxOwnedAttemptResolution) ([]store.OutboxResolutionResult, error) {
	s.mu.Lock()
	s.ownedCalls++
	s.mu.Unlock()
	results := make([]store.OutboxResolutionResult, len(resolutions))
	for i := range resolutions {
		results[i] = store.OutboxResolutionResult{Ref: resolutions[i].Ref, Outcome: store.OutboxResolutionRecorded}
	}
	return results, nil
}

func (s *mutationStoreCapture) BindAttemptTargets(_ context.Context, sets []store.OutboxAttemptTargetSet) ([]store.OutboxBindTargetResult, error) {
	s.mu.Lock()
	s.bindCalls++
	s.bindSets = append(s.bindSets, sets...)
	if len(sets) > s.maxBindBatch {
		s.maxBindBatch = len(sets)
	}
	s.mu.Unlock()
	results := make([]store.OutboxBindTargetResult, len(sets))
	for i := range sets {
		results[i] = store.OutboxBindTargetResult{Ref: sets[i].Ref, Outcome: store.OutboxBindTargetBound}
	}
	return results, nil
}

func (s *mutationStoreCapture) RecordAttemptEvidenceBatch(_ context.Context, evidence []store.OutboxAttemptEvidence) ([]store.OutboxEvidenceResult, error) {
	s.mu.Lock()
	s.evidenceCalls++
	if len(evidence) > s.maxEvidenceBatch {
		s.maxEvidenceBatch = len(evidence)
	}
	s.mu.Unlock()
	results := make([]store.OutboxEvidenceResult, len(evidence))
	for i := range evidence {
		results[i] = store.OutboxEvidenceResult{Ref: evidence[i].Ref, Outcome: store.OutboxEvidenceRecorded}
	}
	return results, nil
}

type mutationDeliveryCapture struct {
	*mutationStoreCapture
}

func (*mutationDeliveryCapture) Enqueue(context.Context, store.DeliveryOutboxEnqueue) (store.DeliveryOutboxItem, error) {
	return store.DeliveryOutboxItem{}, nil
}

func TestAttemptMutationBatcherCombinesIndependentLanesAndSeparatesSameAttempt(t *testing.T) {
	dispatch := &mutationStoreCapture{}
	absoluteState := &mutationStoreCapture{}
	absolute := &mutationDeliveryCapture{mutationStoreCapture: absoluteState}
	channel := &mutationStoreCapture{}
	batcher, err := NewAttemptMutationBatcher(dispatch, absolute, channel, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go batcher.Run(ctx)
	if err := batcher.WaitReady(context.Background()); err != nil {
		t.Fatal(err)
	}

	const requests = 64
	start := make(chan struct{})
	var wait sync.WaitGroup
	errs := make(chan error, requests)
	for i := 0; i < requests; i++ {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			<-start
			ref := store.OutboxAttemptRef{QueueKind: store.OutboxQueueDispatchPTS, StreamID: int64(i + 1), ItemID: int64(i + 1), Sequence: 1, LeaseFence: 1, Attempt: 1}
			results, err := batcher.BindAttemptTargets(context.Background(), store.OutboxQueueDispatchPTS, []store.OutboxAttemptTargetSet{{Ref: ref, SourceInstanceID: "egress-test"}})
			if err != nil || len(results) != 1 || results[0].Ref != ref {
				errs <- err
			}
		}(i)
	}
	close(start)
	wait.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("batched bind: %v", err)
	}
	dispatch.mu.Lock()
	bindCalls, maxBind := dispatch.bindCalls, dispatch.maxBindBatch
	dispatch.mu.Unlock()
	if bindCalls >= requests || maxBind <= 1 {
		t.Fatalf("bind actor calls=%d max_batch=%d, want cross-lane batching", bindCalls, maxBind)
	}

	same := store.OutboxAttemptRef{QueueKind: store.OutboxQueueDispatchPTS, StreamID: 9001, ItemID: 9001, Sequence: 1, LeaseFence: 1, Attempt: 1}
	before := bindCalls
	start = make(chan struct{})
	wait = sync.WaitGroup{}
	for i := 0; i < 2; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, _ = batcher.BindAttemptTargets(context.Background(), store.OutboxQueueDispatchPTS, []store.OutboxAttemptTargetSet{{Ref: same, SourceInstanceID: "egress-test"}})
		}()
	}
	close(start)
	wait.Wait()
	dispatch.mu.Lock()
	after := dispatch.bindCalls
	dispatch.mu.Unlock()
	if after-before != 2 {
		t.Fatalf("same-attempt requests used %d store calls, want 2", after-before)
	}

	cancel()
	batcher.Wait()
}

func TestAttemptMutationBatcherBatchesPhysicalEvidence(t *testing.T) {
	dispatch := &mutationStoreCapture{}
	absoluteState := &mutationStoreCapture{}
	batcher, err := NewAttemptMutationBatcher(dispatch, &mutationDeliveryCapture{mutationStoreCapture: absoluteState}, &mutationStoreCapture{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go batcher.Run(ctx)
	if err := batcher.WaitReady(context.Background()); err != nil {
		t.Fatal(err)
	}
	const requests = 32
	start := make(chan struct{})
	var wait sync.WaitGroup
	for i := 0; i < requests; i++ {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			<-start
			ref := store.OutboxAttemptRef{QueueKind: store.OutboxQueueDispatchPTS, StreamID: int64(i + 1), ItemID: int64(i + 1), Sequence: 1, LeaseFence: 1, Attempt: 1}
			_, _ = batcher.RecordAttemptEvidenceBatch(context.Background(), store.OutboxQueueDispatchPTS, []store.OutboxAttemptEvidence{{Ref: ref, Kind: store.OutboxEvidenceEdgeNoEligible, ObservedAt: time.Now()}})
		}(i)
	}
	close(start)
	wait.Wait()
	dispatch.mu.Lock()
	calls, maxBatch := dispatch.evidenceCalls, dispatch.maxEvidenceBatch
	dispatch.mu.Unlock()
	if calls >= requests || maxBatch <= 1 {
		t.Fatalf("evidence actor calls=%d max_batch=%d, want cross-request batching", calls, maxBatch)
	}
	cancel()
	batcher.Wait()
}

func TestAttemptMutationBatcherFinalizesExactRefsWithoutRecoveryScan(t *testing.T) {
	dispatch := &mutationStoreCapture{finalizeFailures: 1}
	batcher, err := NewAttemptMutationBatcher(
		dispatch,
		&mutationDeliveryCapture{mutationStoreCapture: &mutationStoreCapture{}},
		&mutationStoreCapture{},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go batcher.Run(ctx)
	if err := batcher.WaitReady(context.Background()); err != nil {
		t.Fatal(err)
	}

	const count = 64
	start := make(chan struct{})
	var wait sync.WaitGroup
	for i := 0; i < count; i++ {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			<-start
			ref := store.OutboxAttemptRef{QueueKind: store.OutboxQueueDispatchPTS, StreamID: int64(i + 1), ItemID: int64(i + 1), Sequence: 1, LeaseFence: 1, Attempt: 1}
			if i%2 == 0 {
				_, _ = batcher.RecordAttemptEvidenceBatch(context.Background(), store.OutboxQueueDispatchPTS, []store.OutboxAttemptEvidence{{Ref: ref, Kind: store.OutboxEvidenceEdgeWritten}})
				return
			}
			_, _ = batcher.BindAttemptTargets(context.Background(), store.OutboxQueueDispatchPTS, []store.OutboxAttemptTargetSet{{Ref: ref, SourceInstanceID: "egress-test"}})
		}(i)
	}
	close(start)
	wait.Wait()
	waitMutationCondition(t, func() bool {
		dispatch.mu.Lock()
		defer dispatch.mu.Unlock()
		return len(dispatch.finalizedRefs) == count
	})
	dispatch.mu.Lock()
	finalizeCalls, maxFinalizeBatch := dispatch.finalizeCalls, dispatch.maxFinalizeBatch
	dispatch.mu.Unlock()
	if finalizeCalls >= count || maxFinalizeBatch <= 1 {
		t.Fatalf("exact finalizer calls=%d max_batch=%d, want bounded cross-lane batching", finalizeCalls, maxFinalizeBatch)
	}

	ref := store.OutboxAttemptRef{QueueKind: store.OutboxQueueDispatchPTS, StreamID: 9001, ItemID: 9001, Sequence: 1, LeaseFence: 2, Attempt: 2}
	if _, err := batcher.ResolveOwnedAttemptBatch(context.Background(), store.OutboxQueueDispatchPTS, []store.OutboxOwnedAttemptResolution{{Ref: ref, Owner: "egress-test", Kind: store.OutboxResolutionRetry}}); err != nil {
		t.Fatal(err)
	}
	ref.ItemID++
	ref.StreamID++
	if _, err := batcher.ResolveTargetAttemptBatch(context.Background(), store.OutboxQueueDispatchPTS, []store.OutboxTargetAttemptResolution{{Ref: ref, SourceInstanceID: "egress-test", TargetInstanceID: "edge-test", Kind: store.OutboxResolutionRetry}}); err != nil {
		t.Fatal(err)
	}
	waitMutationCondition(t, func() bool {
		dispatch.mu.Lock()
		defer dispatch.mu.Unlock()
		return dispatch.ownedCalls == 1 && dispatch.targetCalls == 1 && len(dispatch.finalizedRefs) == count+2
	})

	dispatch.mu.Lock()
	before := len(dispatch.finalizedRefs)
	dispatch.mu.Unlock()
	ref.ItemID++
	ref.StreamID++
	if _, err := batcher.BindAttemptTargets(context.Background(), store.OutboxQueueDispatchPTS, []store.OutboxAttemptTargetSet{{
		Ref: ref, SourceInstanceID: "egress-test", Targets: []store.OutboxAttemptTarget{{TargetInstanceID: "edge-test", TargetUserID: ref.StreamID}},
	}}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * mutationActorFlushInterval)
	dispatch.mu.Lock()
	after := len(dispatch.finalizedRefs)
	dispatch.mu.Unlock()
	if after != before {
		t.Fatalf("non-empty bind submitted exact finalize ref: before=%d after=%d", before, after)
	}

	cancel()
	batcher.Wait()
}

func waitMutationCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for mutation actor condition")
		}
		time.Sleep(time.Millisecond)
	}
}
