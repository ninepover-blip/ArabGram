package egress

import (
	"context"
	"errors"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/iamxvbaba/td/proto"
	"go.uber.org/zap/zaptest"

	"telesrv/internal/edgecontrol"
	"telesrv/internal/store"
	"telesrv/internal/store/memory"
)

type v3TestRegistry struct{ records []edgecontrol.LocationRecord }

func (r v3TestRegistry) ListUser(context.Context, int64) ([]edgecontrol.LocationRecord, error) {
	return append([]edgecontrol.LocationRecord(nil), r.records...), nil
}
func (v3TestRegistry) ListBusinessAuthKey(context.Context, [8]byte) ([]edgecontrol.LocationRecord, error) {
	return nil, nil
}
func (v3TestRegistry) ListInstance(context.Context, string) ([]edgecontrol.LocationRecord, error) {
	return nil, nil
}

type v3ChannelRegistry struct {
	v3TestRegistry
	targets []string
	routes  []edgecontrol.ChannelDeliveryRoute
}

func (r *v3ChannelRegistry) ListChannelDeliveryTargets(_ context.Context, routes []edgecontrol.ChannelDeliveryRoute) ([]string, error) {
	r.routes = append(r.routes, routes...)
	return append([]string(nil), r.targets...), nil
}

type v3TestDeliveryBus struct {
	mu      sync.Mutex
	batches []edgecontrol.DeliveryBatch
}

type transientIndeterminatePlanner struct {
	edgecontrol.DeliveryPlanner
	remaining int
	calls     int
}

type deadlineThenReplayPlanner struct {
	edgecontrol.DeliveryPlanner
	notAfter time.Time
	mu       sync.Mutex
	calls    []time.Time
}

func (p *deadlineThenReplayPlanner) AdmitPreparedDelivery(
	ctx context.Context,
	plan edgecontrol.FrozenDeliveryPlan,
) (edgecontrol.FrozenDelivery, error) {
	p.mu.Lock()
	p.calls = append(p.calls, time.Now())
	p.mu.Unlock()
	if time.Now().Before(p.notAfter) {
		<-ctx.Done()
		return edgecontrol.FrozenDelivery{BatchID: plan.BatchID()}, edgecontrol.ErrDeliveryIndeterminate
	}
	return p.DeliveryPlanner.AdmitPreparedDelivery(ctx, plan)
}

func (p *transientIndeterminatePlanner) AdmitPreparedDelivery(
	ctx context.Context,
	plan edgecontrol.FrozenDeliveryPlan,
) (edgecontrol.FrozenDelivery, error) {
	p.calls++
	if p.remaining > 0 {
		p.remaining--
		return edgecontrol.FrozenDelivery{BatchID: plan.BatchID()}, edgecontrol.ErrDeliveryIndeterminate
	}
	return p.DeliveryPlanner.AdmitPreparedDelivery(ctx, plan)
}

type partialAdmissionBus struct {
	mu         sync.Mutex
	failTarget string
	batches    []edgecontrol.DeliveryBatch
}

func (b *partialAdmissionBus) SendDeliveryBatch(_ context.Context, _ string, batch edgecontrol.DeliveryBatch) (edgecontrol.DeliveryAdmission, error) {
	b.mu.Lock()
	b.batches = append(b.batches, batch)
	fail := batch.TargetInstanceID == b.failTarget
	b.mu.Unlock()
	if fail {
		return edgecontrol.DeliveryAdmission{}, errors.New("injected partial admission")
	}
	return edgecontrol.DeliveryAdmission{
		BatchID: batch.BatchID, CommandID: batch.CommandID,
		SourceInstanceID: batch.SourceInstanceID, TargetInstanceID: batch.TargetInstanceID,
		Outcome: edgecontrol.AdmissionAccepted,
	}, nil
}

func (*partialAdmissionBus) SubscribeDeliveryBatches(context.Context, string, edgecontrol.DeliveryBatchHandler) error {
	return nil
}

func (b *partialAdmissionBus) reset(failTarget string) {
	b.mu.Lock()
	b.failTarget = failTarget
	b.batches = nil
	b.mu.Unlock()
}

func (b *partialAdmissionBus) targets() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	targets := make([]string, len(b.batches))
	for i := range b.batches {
		targets[i] = b.batches[i].TargetInstanceID
	}
	sort.Strings(targets)
	return targets
}

type pendingChannelStore struct {
	store.DurableOutboxStateStore
	bound        []store.OutboxAttemptTargetSet
	owned        []store.OutboxOwnedAttemptResolution
	finalizeCall int
}

type routeFirstDispatchStore struct{ store.DispatchOutboxStore }

type ptsWindowProjectorFunc func(context.Context, store.OutboxClaimWindow) ([]projectedOutboxItem, error)

func (f ptsWindowProjectorFunc) Project(ctx context.Context, window store.OutboxClaimWindow) ([]projectedOutboxItem, error) {
	return f(ctx, window)
}

func (s *pendingChannelStore) BindAttemptTargets(_ context.Context, sets []store.OutboxAttemptTargetSet) ([]store.OutboxBindTargetResult, error) {
	s.bound = append(s.bound, sets...)
	results := make([]store.OutboxBindTargetResult, len(sets))
	for i := range sets {
		results[i] = store.OutboxBindTargetResult{Ref: sets[i].Ref, Outcome: store.OutboxBindTargetBound}
	}
	return results, nil
}

func (s *pendingChannelStore) ResolveOwnedAttemptBatch(_ context.Context, resolutions []store.OutboxOwnedAttemptResolution) ([]store.OutboxResolutionResult, error) {
	s.owned = append(s.owned, resolutions...)
	results := make([]store.OutboxResolutionResult, len(resolutions))
	for i := range resolutions {
		results[i] = store.OutboxResolutionResult{Ref: resolutions[i].Ref, Outcome: store.OutboxResolutionRecorded}
	}
	return results, nil
}

func (s *pendingChannelStore) FinalizeAttempts(context.Context, []store.OutboxFinalizeRequest) (store.OutboxFinalizeBatch, error) {
	s.finalizeCall++
	return store.OutboxFinalizeBatch{}, nil
}

func TestOwnedResolutionUsesMutationWriterWithoutSynchronousFinalize(t *testing.T) {
	state := &pendingChannelStore{}
	coordinator := &deliveryCoordinator{mutations: directMutations(nil, nil, state), metrics: nopMetrics{}, log: zaptest.NewLogger(t)}
	coordinator.resolveItems(context.Background(), state, "egress-a/q3/w0", []store.OutboxClaimedItem{{
		Ref: store.OutboxAttemptRef{
			QueueKind: store.OutboxQueueChannelPTS, StreamID: 7001,
			ItemID: 91, Sequence: 11, LeaseFence: 3, Attempt: 1,
		},
	}}, errActorCapacity, false)
	if len(state.owned) != 1 || state.owned[0].Kind != store.OutboxResolutionRetry {
		t.Fatalf("owned resolutions=%+v", state.owned)
	}
	if state.finalizeCall != 0 {
		t.Fatalf("actor-owned resolution synchronously finalized %d times", state.finalizeCall)
	}
}

func TestAccountRouteEmptySkipsPTSProjectionAndBindsExactAttempt(t *testing.T) {
	ctx := context.Background()
	excludeAuth := [8]byte{1, 2, 3}
	const (
		userID    = int64(77)
		sessionID = int64(88)
	)
	bus := &v3TestDeliveryBus{}
	fabric := edgecontrol.NewDeliveryFabric(edgecontrol.DeliveryFabricConfig{
		InstanceID: "egress-a",
		Registry: v3TestRegistry{records: []edgecontrol.LocationRecord{{
			InstanceID: "edge-a", UserID: userID, RawAuthKeyID: excludeAuth,
			SessionID: sessionID, ReceivesUpdates: true,
		}}},
		Bus: bus,
	})
	defer fabric.Close()
	mutations := &mutationStoreCapture{}
	projectionCalls := 0
	notAfter := time.Now().Add(2 * time.Second).UTC()
	window := store.OutboxClaimWindow{
		QueueKind: store.OutboxQueueDispatchPTS, StreamID: userID, LeaseFence: 9,
		Owner: "egress-a/q1/w0", LeaseUntil: notAfter.Add(2 * time.Second),
		Items: []store.OutboxClaimedItem{{
			Ref: store.OutboxAttemptRef{
				QueueKind: store.OutboxQueueDispatchPTS, StreamID: userID,
				ItemID: 101, Sequence: 1, LeaseFence: 9, Attempt: 1,
			},
			ExcludeAuthKeyID: excludeAuth, ExcludeSessionID: sessionID,
			CommandNotAfter: notAfter, EvidenceDeadline: notAfter.Add(time.Second),
		}},
	}
	coordinator := &deliveryCoordinator{
		dispatch: &routeFirstDispatchStore{}, mutations: directMutations(mutations, nil, nil),
		planner: fabric, instanceID: "egress-a", metrics: nopMetrics{}, log: zaptest.NewLogger(t),
		ptsProjector: ptsWindowProjectorFunc(func(context.Context, store.OutboxClaimWindow) ([]projectedOutboxItem, error) {
			projectionCalls++
			return nil, errors.New("projection must not run for an empty frozen route")
		}),
	}
	coordinator.processWindow(ctx, window)
	mutations.mu.Lock()
	bindCalls, maxBatch := mutations.bindCalls, mutations.maxBindBatch
	bindSets := append([]store.OutboxAttemptTargetSet(nil), mutations.bindSets...)
	mutations.mu.Unlock()
	if projectionCalls != 0 || bindCalls != 1 || maxBatch != 1 || len(bus.snapshot()) != 0 {
		t.Fatalf("projection=%d bind_calls=%d bind_batch=%d Redis_commands=%d", projectionCalls, bindCalls, maxBatch, len(bus.snapshot()))
	}
	if len(bindSets) != 1 || bindSets[0].Ref != window.Items[0].Ref || bindSets[0].SourceInstanceID != "egress-a" || len(bindSets[0].Targets) != 0 {
		t.Fatalf("empty route bind=%+v want exact attempt/source with no targets", bindSets)
	}
}

type physicalResolutionCapture struct {
	store.DurableOutboxStateStore
	got []store.OutboxTargetAttemptResolution
}

func (s *physicalResolutionCapture) ResolveTargetAttemptBatch(
	_ context.Context,
	resolutions []store.OutboxTargetAttemptResolution,
) ([]store.OutboxResolutionResult, error) {
	s.got = append(s.got, resolutions...)
	results := make([]store.OutboxResolutionResult, len(resolutions))
	for i := range resolutions {
		results[i] = store.OutboxResolutionResult{Ref: resolutions[i].Ref, Outcome: store.OutboxResolutionRecorded}
	}
	return results, nil
}

func (*physicalResolutionCapture) FinalizeAttempts(context.Context, []store.OutboxFinalizeRequest) (store.OutboxFinalizeBatch, error) {
	return store.OutboxFinalizeBatch{}, nil
}

type physicalAbsoluteCapture struct{ *physicalResolutionCapture }

func (*physicalAbsoluteCapture) Enqueue(context.Context, store.DeliveryOutboxEnqueue) (store.DeliveryOutboxItem, error) {
	return store.DeliveryOutboxItem{}, nil
}

func (b *v3TestDeliveryBus) SendDeliveryBatch(_ context.Context, _ string, batch edgecontrol.DeliveryBatch) (edgecontrol.DeliveryAdmission, error) {
	b.mu.Lock()
	b.batches = append(b.batches, batch)
	b.mu.Unlock()
	return edgecontrol.DeliveryAdmission{
		BatchID: batch.BatchID, CommandID: batch.CommandID,
		SourceInstanceID: batch.SourceInstanceID, TargetInstanceID: batch.TargetInstanceID,
		Outcome: edgecontrol.AdmissionAccepted,
	}, nil
}
func (*v3TestDeliveryBus) SubscribeDeliveryBatches(context.Context, string, edgecontrol.DeliveryBatchHandler) error {
	return nil
}
func (b *v3TestDeliveryBus) snapshot() []edgecontrol.DeliveryBatch {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]edgecontrol.DeliveryBatch(nil), b.batches...)
}

func TestFrozenPlanRetriesExactAdmissionWithinOriginalDeadline(t *testing.T) {
	bus := &v3TestDeliveryBus{}
	fabric := edgecontrol.NewDeliveryFabric(edgecontrol.DeliveryFabricConfig{
		InstanceID: "egress-a",
		Registry: v3TestRegistry{records: []edgecontrol.LocationRecord{{
			InstanceID: "edge-a", UserID: 42, ReceivesUpdates: true,
		}}},
		Bus: bus,
	})
	defer fabric.Close()
	notAfter := time.Now().Add(time.Second)
	evidenceDeadline := notAfter.Add(time.Second)
	payload := []byte{1, 2, 3}
	plan, err := fabric.PrepareDelivery(context.Background(), edgecontrol.DeliveryRequest{
		TargetUserID: 42, NotAfter: notAfter, EvidenceDeadline: evidenceDeadline,
		Items: []edgecontrol.DeliveryItem{{
			Ref: edgecontrol.DeliveryRef{
				Domain:   edgecontrol.OrderingDomain{Kind: edgecontrol.QueueAccountPTS, StreamID: 42},
				OutboxID: 1, TargetUserID: 42, PTS: 1, LeaseFence: 1, Attempt: 1,
			},
			MessageType: proto.MessageFromServer, PayloadHash: edgecontrol.DeliveryPayloadHash(payload), UpdateBytes: payload,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	planner := &transientIndeterminatePlanner{DeliveryPlanner: fabric, remaining: 2}
	coordinator := &deliveryCoordinator{planner: planner}
	if err := coordinator.admitFrozenPlan(context.Background(), context.Background(), plan, notAfter, evidenceDeadline); err != nil {
		t.Fatalf("exact admission retry: %v", err)
	}
	if planner.calls != 3 {
		t.Fatalf("admission calls=%d want 3", planner.calls)
	}
	if batches := bus.snapshot(); len(batches) != 1 {
		t.Fatalf("published batches=%d want exactly one final accepted command", len(batches))
	}
}

func TestFrozenPlanTransitionsToExactReceiptReplayAfterCommandDeadline(t *testing.T) {
	bus := &v3TestDeliveryBus{}
	fabric := edgecontrol.NewDeliveryFabric(edgecontrol.DeliveryFabricConfig{
		InstanceID: "egress-a",
		Registry: v3TestRegistry{records: []edgecontrol.LocationRecord{{
			InstanceID: "edge-a", UserID: 42, ReceivesUpdates: true,
		}}},
		Bus: bus,
	})
	defer fabric.Close()
	notAfter := time.Now().Add(40 * time.Millisecond)
	evidenceDeadline := notAfter.Add(time.Second)
	payload := []byte{4, 5, 6}
	plan, err := fabric.PrepareDelivery(context.Background(), edgecontrol.DeliveryRequest{
		TargetUserID: 42, NotAfter: notAfter, EvidenceDeadline: evidenceDeadline,
		Items: []edgecontrol.DeliveryItem{{
			Ref: edgecontrol.DeliveryRef{
				Domain:   edgecontrol.OrderingDomain{Kind: edgecontrol.QueueAccountPTS, StreamID: 42},
				OutboxID: 2, TargetUserID: 42, PTS: 2, LeaseFence: 2, Attempt: 1,
			},
			MessageType: proto.MessageFromServer,
			PayloadHash: edgecontrol.DeliveryPayloadHash(payload), UpdateBytes: payload,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	planner := &deadlineThenReplayPlanner{DeliveryPlanner: fabric, notAfter: notAfter}
	coordinator := &deliveryCoordinator{planner: planner}
	if err := coordinator.admitFrozenPlan(context.Background(), context.Background(), plan, notAfter, evidenceDeadline); err != nil {
		t.Fatalf("receipt replay transition: %v", err)
	}
	planner.mu.Lock()
	calls := append([]time.Time(nil), planner.calls...)
	planner.mu.Unlock()
	if len(calls) != 2 || calls[0].After(notAfter) || calls[1].Before(notAfter) {
		t.Fatalf("admission phases=%v not_after=%v", calls, notAfter)
	}
	batches := bus.snapshot()
	if len(batches) != 1 || !batches[0].NotAfter.Equal(notAfter) || !batches[0].EvidenceDeadline.Equal(evidenceDeadline) {
		t.Fatalf("receipt replay did not preserve exact command deadlines: %+v", batches)
	}
}

func TestAbsoluteDeliveryCompletesOnlyAfterPhysicalReceipt(t *testing.T) {
	ctx := context.Background()
	queue := memory.NewDeliveryOutboxStore()
	if _, err := queue.Enqueue(ctx, store.DeliveryOutboxEnqueue{
		TargetUserID: 42, Payload: []byte{1, 2, 3}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
	}); err != nil {
		t.Fatal(err)
	}
	windows, err := queue.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-a/q2/w0",
		LaneLimit: 1, WindowSize: 8, WindowByteLimit: 1024, LeaseDuration: 5 * time.Second,
		PhysicalDuration: time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != 1 {
		t.Fatalf("ClaimWindows=%+v err=%v", windows, err)
	}
	bus := &v3TestDeliveryBus{}
	fabric := edgecontrol.NewDeliveryFabric(edgecontrol.DeliveryFabricConfig{
		InstanceID: "egress-a", Registry: v3TestRegistry{records: []edgecontrol.LocationRecord{
			{InstanceID: "edge-a", UserID: 42, ReceivesUpdates: true},
		}}, Bus: bus,
	})
	defer fabric.Close()
	coordinator := &deliveryCoordinator{
		absolute: queue, mutations: directMutations(nil, queue, nil), planner: fabric, metrics: nopMetrics{}, log: zaptest.NewLogger(t),
		instanceID: "egress-a",
	}
	coordinator.processWindow(ctx, windows[0])
	batches := bus.snapshot()
	if len(batches) != 1 || len(batches[0].Items) != 1 {
		t.Fatalf("published batches=%+v", batches)
	}
	// Admission is not completion: the lane is still leased and cannot be
	// claimed by another worker before a physical receipt is durable.
	if claimed, err := queue.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-b/q2/w0", LaneLimit: 1,
		LeaseDuration: 5 * time.Second, PhysicalDuration: time.Second, ClockSkewAllowance: time.Second,
	}); err != nil || len(claimed) != 0 {
		t.Fatalf("claim after admission=%+v err=%v", claimed, err)
	}
	batch := batches[0]
	results := applyPhysicalReceiptBatch(ctx, deliveryEvidenceStores{absolute: queue, mutations: directMutations(nil, queue, nil)}, []edgecontrol.PhysicalReceipt{{
		BatchID: batch.BatchID, CommandID: batch.CommandID,
		SourceInstanceID: batch.SourceInstanceID, TargetInstanceID: batch.TargetInstanceID,
		Ref: batch.Items[0].Ref, Outcome: edgecontrol.PhysicalWritten,
		EligibleSessions: 1, WrittenSessions: 1, FirstServerMsgID: 9001, ObservedAt: time.Now(),
	}})
	if len(results) != 1 || results[0].Outcome != edgecontrol.PhysicalReceiptApplied {
		t.Fatalf("physical results=%+v", results)
	}
	// The receipt RPC commits evidence only. Production PostgreSQL emits a
	// transactional finalize wake; drive the same durable recovery contract
	// explicitly for this in-memory store instead of relying on a synchronous
	// per-receipt finalizer hidden inside the handler.
	finalizable, err := queue.RecoverFinalizableAttempts(ctx, store.OutboxRecoverFinalizableRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, LaneLimit: 1, AttemptLimit: store.MaxDeliveryBatchItems,
	})
	if err != nil || len(finalizable) != 1 {
		t.Fatalf("RecoverFinalizableAttempts=%+v err=%v", finalizable, err)
	}
	finalized, err := queue.FinalizeAttempts(ctx, finalizable)
	if err != nil || len(finalized.Results) != 1 || finalized.Results[0].Outcome != store.OutboxFinalizeApplied {
		t.Fatalf("FinalizeAttempts=%+v err=%v", finalized, err)
	}
	if next, ok, err := queue.NextReadyAt(ctx, store.OutboxQueueAbsoluteDelivery, 0, nil); err != nil || ok {
		t.Fatalf("NextReadyAt=%+v ok=%v err=%v", next, ok, err)
	}
}

func TestAuthoritativeNoTargetFinalizesWithoutRedisCommand(t *testing.T) {
	ctx := context.Background()
	queue := memory.NewDeliveryOutboxStore()
	_, _ = queue.Enqueue(ctx, store.DeliveryOutboxEnqueue{
		TargetUserID: 77, Payload: []byte{7}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
	})
	windows, err := queue.ClaimWindows(ctx, store.OutboxClaimRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, Owner: "egress-a/q2/w0", LaneLimit: 1,
		LeaseDuration: 5 * time.Second, PhysicalDuration: time.Second, ClockSkewAllowance: time.Second,
	})
	if err != nil || len(windows) != 1 {
		t.Fatalf("ClaimWindows=%+v err=%v", windows, err)
	}
	bus := &v3TestDeliveryBus{}
	fabric := edgecontrol.NewDeliveryFabric(edgecontrol.DeliveryFabricConfig{
		InstanceID: "egress-a", Registry: v3TestRegistry{}, Bus: bus,
	})
	defer fabric.Close()
	coordinator := &deliveryCoordinator{
		absolute: queue, mutations: directMutations(nil, queue, nil), planner: fabric, metrics: nopMetrics{}, log: zaptest.NewLogger(t),
		instanceID: "egress-a",
	}
	coordinator.processWindow(ctx, windows[0])
	if got := len(bus.snapshot()); got != 0 {
		t.Fatalf("Redis commands=%d want 0", got)
	}
	finalizable, err := queue.RecoverFinalizableAttempts(ctx, store.OutboxRecoverFinalizableRequest{
		QueueKind: store.OutboxQueueAbsoluteDelivery, LaneLimit: 1, AttemptLimit: store.MaxDeliveryBatchItems,
	})
	if err != nil || len(finalizable) != 1 {
		t.Fatalf("RecoverFinalizableAttempts=%+v err=%v", finalizable, err)
	}
	if finalized, err := queue.FinalizeAttempts(ctx, finalizable); err != nil || len(finalized.Results) != 1 || finalized.Results[0].Outcome != store.OutboxFinalizeApplied {
		t.Fatalf("FinalizeAttempts=%+v err=%v", finalized, err)
	}
	if _, ok, err := queue.NextReadyAt(ctx, store.OutboxQueueAbsoluteDelivery, 0, nil); err != nil || ok {
		t.Fatalf("queue remained after durable finalizer ok=%v err=%v", ok, err)
	}
}

func TestOrderingDomainHashSeparatesQueueKinds(t *testing.T) {
	account := orderingDomainHash(store.OutboxQueueDispatchPTS, 99)
	absolute := orderingDomainHash(store.OutboxQueueAbsoluteDelivery, 99)
	channel := orderingDomainHash(store.OutboxQueueChannelPTS, 99)
	if account == absolute || account == channel || absolute == channel {
		t.Fatalf("ordering-domain hashes collided: %d %d %d", account, absolute, channel)
	}
	if account != orderingDomainHash(store.OutboxQueueDispatchPTS, 99) {
		t.Fatal("ordering-domain hash is not stable")
	}
}

func TestMonoforumFrozenAudienceProjectsOneCommandPerEdge(t *testing.T) {
	const channelID = int64(901)
	coordinator := &deliveryCoordinator{channelBuilder: func(_ context.Context, requests []ChannelUpdateRequest) ([][]byte, error) {
		if len(requests) != 1 || requests[0] != (ChannelUpdateRequest{ChannelID: channelID, PTS: 51}) {
			t.Fatalf("channel update requests = %+v", requests)
		}
		return [][]byte{{0x71, 0x72}}, nil
	}}
	window := store.OutboxClaimWindow{
		QueueKind: store.OutboxQueueChannelPTS, StreamID: channelID,
		Items: []store.OutboxClaimedItem{{
			Ref: store.OutboxAttemptRef{
				QueueKind: store.OutboxQueueChannelPTS, StreamID: channelID,
				ItemID: 41, Sequence: 51, LeaseFence: 61, Attempt: 1,
			},
			Payload: store.ChannelDeliveryPayload{
				MinPTS: 51, MaxPTS: 51, ProjectionKind: "new_message",
				AudienceKind:    store.ChannelDeliveryAudienceMonoforumAdmins,
				AudienceUserIDs: []int64{101, 102},
			},
		}},
	}
	projected, err := coordinator.projectChannelWindow(context.Background(), window)
	if err != nil {
		t.Fatal(err)
	}
	notAfter := time.Now().Add(time.Second)
	request, err := coordinator.deliveryRequest(projected, 0, notAfter, notAfter.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	registry := &v3ChannelRegistry{targets: []string{"edge-b", "edge-a", "edge-a"}}
	bus := &v3TestDeliveryBus{}
	fabric := edgecontrol.NewDeliveryFabric(edgecontrol.DeliveryFabricConfig{
		InstanceID: "egress-a", Registry: registry, Bus: bus,
	})
	defer fabric.Close()
	plan, err := fabric.PrepareDelivery(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(registry.routes) != 1 || registry.routes[0].Audience != edgecontrol.ChannelAudienceMonoforumAdmins ||
		!slices.Equal(registry.routes[0].AudienceUsers, []int64{101, 102}) {
		t.Fatalf("planned monoforum route = %+v", registry.routes)
	}
	targets := plan.Targets()
	if len(targets) != 2 || targets[0].TargetInstanceID != "edge-a" || targets[1].TargetInstanceID != "edge-b" {
		t.Fatalf("frozen targets = %+v, want one command per owning Edge", targets)
	}
	if request.TargetUserID != 0 || request.Items[0].Ref.TargetUserID != 0 {
		t.Fatalf("channel request regressed to per-user target: %+v", request)
	}
}

func TestChannelProjectionRejectsNonCanonicalDurableAudience(t *testing.T) {
	const channelID = int64(902)
	coordinator := &deliveryCoordinator{channelBuilder: func(_ context.Context, _ []ChannelUpdateRequest) ([][]byte, error) {
		t.Fatal("builder ran for a non-canonical durable audience")
		return nil, nil
	}}
	window := store.OutboxClaimWindow{
		QueueKind: store.OutboxQueueChannelPTS, StreamID: channelID,
		Items: []store.OutboxClaimedItem{{
			Ref: store.OutboxAttemptRef{
				QueueKind: store.OutboxQueueChannelPTS, StreamID: channelID,
				ItemID: 42, Sequence: 52, LeaseFence: 62, Attempt: 1,
			},
			Payload: store.ChannelDeliveryPayload{
				MinPTS: 52, MaxPTS: 52, ProjectionKind: "new_message",
				AudienceKind:    store.ChannelDeliveryAudienceMonoforumAdmins,
				AudienceUserIDs: []int64{102, 101, 101},
			},
		}},
	}
	if _, err := coordinator.projectChannelWindow(context.Background(), window); err == nil {
		t.Fatal("non-canonical durable channel audience was silently normalized")
	}
}

func TestPartialChannelAdmissionStaysBoundAndReplaysOldTopology(t *testing.T) {
	const channelID = int64(911)
	registry := &v3ChannelRegistry{targets: []string{"edge-a", "edge-b"}}
	bus := &partialAdmissionBus{failTarget: "edge-b"}
	fabric := edgecontrol.NewDeliveryFabric(edgecontrol.DeliveryFabricConfig{
		InstanceID: "egress-a", Registry: registry, Bus: bus,
	})
	defer fabric.Close()
	state := &pendingChannelStore{}
	coordinator := &deliveryCoordinator{
		channel: state, mutations: directMutations(nil, nil, state), planner: fabric, instanceID: "egress-a",
		metrics: nopMetrics{}, log: zaptest.NewLogger(t),
		channelBuilder: func(_ context.Context, requests []ChannelUpdateRequest) ([][]byte, error) {
			if len(requests) != 1 || requests[0].ChannelID != channelID || requests[0].PTS != 71 {
				t.Fatalf("channel requests = %+v", requests)
			}
			return [][]byte{{0x51, 0x52}}, nil
		},
	}
	now := time.Now()
	window := store.OutboxClaimWindow{
		QueueKind: store.OutboxQueueChannelPTS, StreamID: channelID,
		Owner: "egress-a/q3/w0", LeaseFence: 81, LeaseUntil: now.Add(5 * time.Second),
		Items: []store.OutboxClaimedItem{{
			Ref: store.OutboxAttemptRef{
				QueueKind: store.OutboxQueueChannelPTS, StreamID: channelID,
				ItemID: 61, Sequence: 71, LeaseFence: 81, Attempt: 1,
			},
			CommandNotAfter: now.Add(2 * time.Second), EvidenceDeadline: now.Add(3 * time.Second),
			Payload: store.ChannelDeliveryPayload{
				MinPTS: 71, MaxPTS: 71, ProjectionKind: "new_message",
				AudienceKind:    store.ChannelDeliveryAudienceMonoforumAdmins,
				AudienceUserIDs: []int64{101},
			},
		}},
	}
	done := make(chan struct{})
	go func() {
		coordinator.processWindow(context.Background(), window)
		close(done)
	}()
	deadline := time.Now().Add(time.Second)
	for len(bus.targets()) < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := bus.targets(); len(got) < 2 {
		t.Fatalf("initial partial admission targets=%v", got)
	}
	// The registry changes while the same owner retries admission. The frozen
	// plan must keep edge-a/edge-b and must not re-resolve edge-c.
	registry.targets = []string{"edge-c"}
	bus.reset("")
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("same-owner exact admission retry did not converge")
	}
	if len(state.bound) != 1 || len(state.bound[0].Targets) != 2 {
		t.Fatalf("bound target ledger = %+v, want both original Edges", state.bound)
	}
	if len(state.owned) != 0 {
		t.Fatalf("partial post-bind admission resolved attempt early: %+v", state.owned)
	}

	recovered := window
	recovered.Items = append([]store.OutboxClaimedItem(nil), window.Items...)
	recovered.Items[0].SourceInstanceID = "egress-a"
	recovered.Items[0].Targets = append([]store.OutboxAttemptTarget(nil), state.bound[0].Targets...)
	projected, err := coordinator.projectChannelWindow(context.Background(), recovered)
	if err != nil {
		t.Fatal(err)
	}
	if got := bus.targets(); !slices.Equal(got, []string{"edge-a", "edge-b"}) {
		t.Fatalf("live exact retry targets = %v, want frozen old topology", got)
	}
	bus.reset("")
	if err := coordinator.replayBoundItems(context.Background(), context.Background(), projected, recovered.Items[0].CommandNotAfter, recovered.Items[0].EvidenceDeadline, nil); err != nil {
		t.Fatal(err)
	}
	if got := bus.targets(); !slices.Equal(got, []string{"edge-a", "edge-b"}) {
		t.Fatalf("exact replay targets = %v, want frozen old topology; new edge-c must wait for next fence", got)
	}
}

func TestReplayBoundItemsRejectsWidenedPerItemTopology(t *testing.T) {
	now := time.Now()
	ref := func(itemID, sequence int64) store.OutboxAttemptRef {
		return store.OutboxAttemptRef{
			QueueKind: store.OutboxQueueChannelPTS, StreamID: 9101,
			ItemID: itemID, Sequence: sequence, LeaseFence: 33, Attempt: 1,
		}
	}
	items := []projectedOutboxItem{
		{
			claimed: store.OutboxClaimedItem{
				Ref: ref(1, 71), SourceInstanceID: "egress-a",
				Targets: []store.OutboxAttemptTarget{{
					TargetInstanceID: "edge-a", TargetUserID: 0,
					BatchID: [16]byte{1}, CommandID: [16]byte{11},
				}},
			},
			payload: []byte{1},
		},
		{
			claimed: store.OutboxClaimedItem{
				Ref: ref(2, 72), SourceInstanceID: "egress-a",
				Targets: []store.OutboxAttemptTarget{{
					TargetInstanceID: "edge-b", TargetUserID: 0,
					BatchID: [16]byte{1}, CommandID: [16]byte{12},
				}},
			},
			payload: []byte{2},
		},
	}
	coordinator := &deliveryCoordinator{channel: &pendingChannelStore{}}
	err := coordinator.replayBoundItems(context.Background(), context.Background(), items, now.Add(time.Second), now.Add(2*time.Second), nil)
	if err == nil || !strings.Contains(err.Error(), "topology differs") {
		t.Fatalf("mismatched frozen topology error = %v", err)
	}
}

func TestClaimWindowRejectsMixedBoundSourcesAndUnboundTargets(t *testing.T) {
	now := time.Now()
	window := store.OutboxClaimWindow{
		QueueKind: store.OutboxQueueChannelPTS, StreamID: 9102,
		Owner: "egress-a/q3/w0", LeaseFence: 34, LeaseUntil: now.Add(3 * time.Second),
		Items: []store.OutboxClaimedItem{
			{
				Ref:              store.OutboxAttemptRef{QueueKind: store.OutboxQueueChannelPTS, StreamID: 9102, ItemID: 1, Sequence: 81, LeaseFence: 34, Attempt: 1},
				SourceInstanceID: "egress-a", CommandNotAfter: now.Add(time.Second), EvidenceDeadline: now.Add(2 * time.Second),
			},
			{
				Ref:              store.OutboxAttemptRef{QueueKind: store.OutboxQueueChannelPTS, StreamID: 9102, ItemID: 2, Sequence: 82, LeaseFence: 34, Attempt: 1},
				SourceInstanceID: "egress-b", CommandNotAfter: now.Add(time.Second), EvidenceDeadline: now.Add(2 * time.Second),
			},
		},
	}
	if err := validateClaimWindow(window); err == nil {
		t.Fatal("mixed durable delivery sources were accepted in one ordering window")
	}
	window.Items[0].SourceInstanceID = ""
	window.Items[1].SourceInstanceID = ""
	window.Items[0].Targets = []store.OutboxAttemptTarget{{
		TargetInstanceID: "edge-a", BatchID: [16]byte{1}, CommandID: [16]byte{2},
	}}
	if err := validateClaimWindow(window); err == nil {
		t.Fatal("unbound window carrying frozen target commands was accepted")
	}
}

func TestDomainActorByteCapacityBackpressuresFixedWorker(t *testing.T) {
	executor := newDeliveryActorExecutor(nil, 1, 1, 1024)
	if !executor.reserve(1024) {
		t.Fatal("initial actor byte reservation failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	started := make(chan struct{})
	acquired := make(chan bool, 1)
	go func() {
		close(started)
		acquired <- executor.reserveContext(ctx, 512)
	}()
	<-started
	select {
	case <-acquired:
		t.Fatal("reserveContext bypassed the fixed global byte budget")
	case <-time.After(20 * time.Millisecond):
	}
	executor.release(1024)
	select {
	case ok := <-acquired:
		if !ok {
			t.Fatal("reserveContext did not resume after capacity release")
		}
	case <-time.After(time.Second):
		t.Fatal("reserveContext remained blocked after capacity release")
	}
	executor.release(512)
}

func TestDefaultActorBudgetCanAdmitMaximumRecoveryWindow(t *testing.T) {
	if defaultDomainActorBytes < minimumProductionActorBytes {
		t.Fatalf("default actor bytes=%d cannot admit one maximum recovery window=%d",
			defaultDomainActorBytes, minimumProductionActorBytes)
	}
}

func TestProjectedWindowLimitIncludesExplicitChannelRouteBytes(t *testing.T) {
	payload := make([]byte, edgecontrol.MaxDeliveryBatchBytes-7)
	item := projectedOutboxItem{
		payload: payload[:edgecontrol.MaxDeliveryBatchBytes-8],
		channelRoute: edgecontrol.ChannelDeliveryRoute{
			ChannelID: 99, Audience: edgecontrol.ChannelAudienceMembers, AffectedUsers: []int64{42},
		},
	}
	if err := validateProjectedWindowHomogeneous([]projectedOutboxItem{item}); err != nil {
		t.Fatalf("exact projected envelope limit rejected: %v", err)
	}
	item.payload = payload
	if err := validateProjectedWindowHomogeneous([]projectedOutboxItem{item}); !errors.Is(err, errProjectedDeliveryTooLarge) {
		t.Fatalf("projected route metadata overflow error=%v", err)
	}
}

func TestExhaustedResolutionIsExplicitPerOrderingDomain(t *testing.T) {
	if got := exhaustedResolutionKind(store.OutboxQueueDispatchPTS); got != store.OutboxResolutionTerminalResync {
		t.Fatalf("account PTS exhaustion = %d, want terminal resync", got)
	}
	if got := exhaustedResolutionKind(store.OutboxQueueChannelPTS); got != store.OutboxResolutionTerminalResync {
		t.Fatalf("channel PTS exhaustion = %d, want terminal resync", got)
	}
	if got := exhaustedResolutionKind(store.OutboxQueueAbsoluteDelivery); got != store.OutboxResolutionAbandoned {
		t.Fatalf("absolute delivery exhaustion = %d, want audited abandon", got)
	}
}

func TestDeterministicPoisonUsesRecoveryPolicyInsteadOfHaltingLane(t *testing.T) {
	for _, queue := range []store.OutboxQueueKind{store.OutboxQueueDispatchPTS, store.OutboxQueueChannelPTS} {
		if got := exhaustedResolutionKind(queue); got != store.OutboxResolutionTerminalResync {
			t.Fatalf("queue %d poison resolution = %d, want terminal resync", queue, got)
		}
	}
	if got := exhaustedResolutionKind(store.OutboxQueueAbsoluteDelivery); got != store.OutboxResolutionAbandoned {
		t.Fatalf("absolute poison resolution = %d, want abandoned", got)
	}
}

func TestPhysicalRejectedUsesExactTargetIdentityAndRecoveryPolicy(t *testing.T) {
	dispatchCapture := &physicalResolutionCapture{}
	absoluteCapture := &physicalResolutionCapture{}
	channelCapture := &physicalResolutionCapture{}
	stores := deliveryEvidenceStores{
		dispatch:  dispatchCapture,
		absolute:  &physicalAbsoluteCapture{absoluteCapture},
		channel:   channelCapture,
		mutations: directMutations(dispatchCapture, &physicalAbsoluteCapture{absoluteCapture}, channelCapture),
	}
	var batch edgecontrol.BatchID
	var command edgecontrol.CommandID
	batch[0], command[0] = 0x41, 0x42
	receipts := []edgecontrol.PhysicalReceipt{
		{
			BatchID: batch, CommandID: command, SourceInstanceID: "egress-a", TargetInstanceID: "edge-a",
			Ref: edgecontrol.DeliveryRef{
				Domain:   edgecontrol.OrderingDomain{Kind: edgecontrol.QueueAccountPTS, StreamID: 11},
				OutboxID: 101, TargetUserID: 11, PTS: 21, LeaseFence: 31, Attempt: 1,
			},
			Outcome: edgecontrol.PhysicalRejected,
		},
		{
			BatchID: batch, CommandID: command, SourceInstanceID: "egress-a", TargetInstanceID: "edge-b",
			Ref: edgecontrol.DeliveryRef{
				Domain:   edgecontrol.OrderingDomain{Kind: edgecontrol.QueueAccountNonPTS, StreamID: 12},
				OutboxID: 102, TargetUserID: 12, LeaseFence: 32, Attempt: 1,
			},
			Outcome: edgecontrol.PhysicalRejected,
		},
		{
			BatchID: batch, CommandID: command, SourceInstanceID: "egress-a", TargetInstanceID: "edge-c",
			Ref: edgecontrol.DeliveryRef{
				Domain:   edgecontrol.OrderingDomain{Kind: edgecontrol.QueueChannelPTS, StreamID: 13},
				OutboxID: 103, TargetUserID: 0, PTS: 23, LeaseFence: 33, Attempt: 1,
			},
			Outcome: edgecontrol.PhysicalRejected,
		},
	}
	results := applyPhysicalReceiptBatch(context.Background(), stores, receipts)
	for i, result := range results {
		if result.Outcome != edgecontrol.PhysicalReceiptApplied {
			t.Fatalf("receipt %d outcome = %+v, want applied", i, result)
		}
	}
	assertResolution := func(label string, capture *physicalResolutionCapture, want store.OutboxResolutionKind, receipt edgecontrol.PhysicalReceipt) {
		t.Helper()
		if len(capture.got) != 1 {
			t.Fatalf("%s resolutions = %+v, want one", label, capture.got)
		}
		got := capture.got[0]
		if got.Kind != want || got.SourceInstanceID != receipt.SourceInstanceID || got.TargetInstanceID != receipt.TargetInstanceID ||
			got.TargetUserID != receipt.Ref.TargetUserID || got.BatchID != [16]byte(receipt.BatchID) || got.CommandID != [16]byte(receipt.CommandID) {
			t.Fatalf("%s resolution = %+v, want kind=%d and exact receipt identity", label, got, want)
		}
	}
	assertResolution("account PTS", dispatchCapture, store.OutboxResolutionTerminalResync, receipts[0])
	assertResolution("absolute", absoluteCapture, store.OutboxResolutionAbandoned, receipts[1])
	assertResolution("channel PTS", channelCapture, store.OutboxResolutionTerminalResync, receipts[2])
}
