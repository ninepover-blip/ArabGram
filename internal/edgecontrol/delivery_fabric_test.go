package edgecontrol

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iamxvbaba/td/proto"
	"github.com/iamxvbaba/td/tg"
)

type deliveryPlanRegistry struct {
	records        []LocationRecord
	channelTargets []string
	userCalls      *atomic.Int32
}

func (r deliveryPlanRegistry) ListUser(context.Context, int64) ([]LocationRecord, error) {
	if r.userCalls != nil {
		r.userCalls.Add(1)
	}
	return append([]LocationRecord(nil), r.records...), nil
}
func (deliveryPlanRegistry) ListBusinessAuthKey(context.Context, [8]byte) ([]LocationRecord, error) {
	return nil, nil
}
func (deliveryPlanRegistry) ListInstance(context.Context, string) ([]LocationRecord, error) {
	return nil, nil
}
func (r deliveryPlanRegistry) ListChannelDeliveryTargets(context.Context, []ChannelDeliveryRoute) ([]string, error) {
	return append([]string(nil), r.channelTargets...), nil
}

type deliveryPlanBus struct {
	mu      sync.Mutex
	batches []DeliveryBatch
}

type blockingDeliveryPlanBus struct {
	active  atomic.Int32
	peak    atomic.Int32
	calls   atomic.Int32
	entered chan struct{}
	release chan struct{}
}

func (b *blockingDeliveryPlanBus) SendDeliveryBatch(ctx context.Context, _ string, batch DeliveryBatch) (DeliveryAdmission, error) {
	b.calls.Add(1)
	active := b.active.Add(1)
	for {
		peak := b.peak.Load()
		if active <= peak || b.peak.CompareAndSwap(peak, active) {
			break
		}
	}
	select {
	case b.entered <- struct{}{}:
	default:
		{
		}
	}
	select {
	case <-b.release:
	case <-ctx.Done():
		b.active.Add(-1)
		return DeliveryAdmission{}, ctx.Err()
	}
	b.active.Add(-1)
	return bindDeliveryAdmission(batch, DeliveryAdmission{Outcome: AdmissionAccepted}), nil
}
func (*blockingDeliveryPlanBus) SubscribeDeliveryBatches(context.Context, string, DeliveryBatchHandler) error {
	return nil
}

func (b *deliveryPlanBus) SendDeliveryBatch(_ context.Context, target string, batch DeliveryBatch) (DeliveryAdmission, error) {
	b.mu.Lock()
	b.batches = append(b.batches, batch)
	b.mu.Unlock()
	return bindDeliveryAdmission(batch, DeliveryAdmission{Outcome: AdmissionAccepted}), nil
}
func (*deliveryPlanBus) SubscribeDeliveryBatches(context.Context, string, DeliveryBatchHandler) error {
	return nil
}
func (b *deliveryPlanBus) count() int { b.mu.Lock(); defer b.mu.Unlock(); return len(b.batches) }

func testDeliveryRequestEvidence(request DeliveryRequest) DeliveryRequest {
	request.EvidenceDeadline = request.NotAfter.Add(time.Second)
	return request
}

func testDeliveryRouteEvidence(request AccountDeliveryRouteRequest) AccountDeliveryRouteRequest {
	request.EvidenceDeadline = request.NotAfter.Add(time.Second)
	return request
}

func testDeliveryBatchEvidence(batch DeliveryBatch) DeliveryBatch {
	batch.EvidenceDeadline = batch.NotAfter.Add(time.Second)
	return batch
}

func TestDeliveryFabricPrepareThenAdmitPreservesFrozenTargets(t *testing.T) {
	bus := &deliveryPlanBus{}
	fabric := NewDeliveryFabric(DeliveryFabricConfig{InstanceID: "egress-a", Registry: deliveryPlanRegistry{records: []LocationRecord{
		{InstanceID: "edge-b", UserID: 42, ReceivesUpdates: true}, {InstanceID: "edge-c", UserID: 42, ReceivesUpdates: true}, {InstanceID: "edge-b", UserID: 42, ReceivesUpdates: true},
	}}, Bus: bus})
	defer fabric.Close()
	raw, err := EncodeDeliveryUpdate(&tg.Updates{Date: 7})
	if err != nil {
		t.Fatal(err)
	}
	ref := DeliveryRef{Domain: OrderingDomain{Kind: QueueAccountPTS, StreamID: 42}, OutboxID: 91, TargetUserID: 42, PTS: 8, LeaseFence: 11, Attempt: 2}
	plan, err := fabric.PrepareDelivery(context.Background(), testDeliveryRequestEvidence(DeliveryRequest{TargetUserID: 42, NotAfter: time.Now().Add(time.Minute), Items: []DeliveryItem{{Ref: ref, MessageType: proto.MessageFromServer, PayloadHash: DeliveryPayloadHash(raw), UpdateBytes: raw}}}))
	if err != nil {
		t.Fatalf("PrepareDelivery: %v", err)
	}
	if bus.count() != 0 {
		t.Fatal("PrepareDelivery published before durable target bind")
	}
	targets := plan.Targets()
	if len(targets) != 2 || targets[0].CommandID.Empty() || targets[1].CommandID.Empty() || targets[0].CommandID == targets[1].CommandID {
		t.Fatalf("targets=%+v", targets)
	}
	result, err := fabric.AdmitPreparedDelivery(context.Background(), plan)
	if err != nil {
		t.Fatalf("AdmitPreparedDelivery: %v", err)
	}
	if len(result.Targets) != 2 || bus.count() != 2 {
		t.Fatalf("result=%+v publishes=%d", result, bus.count())
	}
	for i, target := range result.Targets {
		if target.TargetInstanceID != targets[i].TargetInstanceID || target.CommandID != targets[i].CommandID {
			t.Fatalf("target[%d]=%+v want %+v", i, target, targets[i])
		}
	}
}

func TestFrozenAccountRouteBindsPayloadWithoutSecondRegistryRead(t *testing.T) {
	bus := &deliveryPlanBus{}
	var calls atomic.Int32
	fabric := NewDeliveryFabric(DeliveryFabricConfig{InstanceID: "egress-a", Registry: deliveryPlanRegistry{
		userCalls: &calls,
		records: []LocationRecord{
			{InstanceID: "edge-b", UserID: 42, ReceivesUpdates: true},
			{InstanceID: "edge-c", UserID: 42, ReceivesUpdates: true},
		},
	}, Bus: bus})
	defer fabric.Close()
	notAfter := time.Now().Add(time.Minute)
	route, err := fabric.PrepareAccountDeliveryRoute(context.Background(), testDeliveryRouteEvidence(AccountDeliveryRouteRequest{
		TargetUserID: 42, NotAfter: notAfter,
	}))
	if err != nil {
		t.Fatalf("PrepareAccountDeliveryRoute: %v", err)
	}
	if calls.Load() != 1 || len(route.Targets()) != 2 {
		t.Fatalf("registry calls=%d targets=%+v", calls.Load(), route.Targets())
	}
	raw, err := EncodeDeliveryUpdate(&tg.Updates{Date: 7})
	if err != nil {
		t.Fatal(err)
	}
	ref := DeliveryRef{Domain: OrderingDomain{Kind: QueueAccountPTS, StreamID: 42}, OutboxID: 91, TargetUserID: 42, PTS: 8, LeaseFence: 11, Attempt: 2}
	plan, err := route.BindDelivery(testDeliveryRequestEvidence(DeliveryRequest{TargetUserID: 42, NotAfter: notAfter, Items: []DeliveryItem{{
		Ref: ref, MessageType: proto.MessageFromServer, PayloadHash: DeliveryPayloadHash(raw), UpdateBytes: raw,
	}}}))
	if err != nil {
		t.Fatalf("BindDelivery: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("payload bind repeated registry lookup: calls=%d", calls.Load())
	}
	if got, want := plan.Targets(), route.Targets(); len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("plan targets=%+v route targets=%+v", got, want)
	}
}

func TestDeliveryFabricRejectsElapsedAbsoluteDeadlineBeforeRouting(t *testing.T) {
	bus := &deliveryPlanBus{}
	fabric := NewDeliveryFabric(DeliveryFabricConfig{InstanceID: "egress-a", Registry: deliveryPlanRegistry{records: []LocationRecord{{InstanceID: "edge-b", UserID: 42, ReceivesUpdates: true}}}, Bus: bus})
	defer fabric.Close()
	raw, err := EncodeDeliveryUpdate(&tg.Updates{Date: 7})
	if err != nil {
		t.Fatal(err)
	}
	ref := DeliveryRef{Domain: OrderingDomain{Kind: QueueAccountPTS, StreamID: 42}, OutboxID: 91, TargetUserID: 42, PTS: 8, LeaseFence: 11, Attempt: 2}
	_, err = fabric.PrepareDelivery(context.Background(), testDeliveryRequestEvidence(DeliveryRequest{TargetUserID: 42, NotAfter: time.Now().Add(-time.Millisecond), Items: []DeliveryItem{{Ref: ref, MessageType: proto.MessageFromServer, PayloadHash: DeliveryPayloadHash(raw), UpdateBytes: raw}}}))
	if err == nil {
		t.Fatal("elapsed NotAfter was accepted")
	}
	if bus.count() != 0 {
		t.Fatal("elapsed NotAfter reached Redis bus")
	}
}

func TestDeliveryFabricRejectsFarFutureDeadlineBeforeRouting(t *testing.T) {
	bus := &deliveryPlanBus{}
	fabric := NewDeliveryFabric(DeliveryFabricConfig{
		InstanceID: "egress-a",
		Registry:   deliveryPlanRegistry{records: []LocationRecord{{InstanceID: "edge-b", UserID: 42, ReceivesUpdates: true}}},
		Bus:        bus,
	})
	defer fabric.Close()
	raw, err := EncodeDeliveryUpdate(&tg.Updates{Date: 7})
	if err != nil {
		t.Fatal(err)
	}
	ref := DeliveryRef{Domain: OrderingDomain{Kind: QueueAccountPTS, StreamID: 42}, OutboxID: 91, TargetUserID: 42, PTS: 8, LeaseFence: 11, Attempt: 2}
	_, err = fabric.PrepareDelivery(context.Background(), testDeliveryRequestEvidence(DeliveryRequest{
		TargetUserID: 42, NotAfter: time.Now().Add(MaxDeliveryNotAfterHorizon + time.Second),
		Items: []DeliveryItem{{Ref: ref, MessageType: proto.MessageFromServer, PayloadHash: DeliveryPayloadHash(raw), UpdateBytes: raw}},
	}))
	if err == nil {
		t.Fatal("far-future NotAfter was accepted")
	}
	if bus.count() != 0 {
		t.Fatal("far-future NotAfter reached Redis bus")
	}
}

func TestDeliveryEnvelopeCountsExplicitChannelRouteBytes(t *testing.T) {
	domain := OrderingDomain{Kind: QueueChannelPTS, StreamID: 99}
	payload := make([]byte, MaxDeliveryBatchBytes-7)
	item := DeliveryItem{
		Ref: DeliveryRef{
			Domain: domain, OutboxID: 91, TargetUserID: 0, PTS: 8,
			LeaseFence: 11, Attempt: 2,
		},
		MessageType: proto.MessageFromServer,
		UpdateBytes: payload[:MaxDeliveryBatchBytes-8],
		Channel: ChannelDeliveryRoute{
			ChannelID: 99, Audience: ChannelAudienceMembers, AffectedUsers: []int64{42},
		},
	}
	request := testDeliveryRequestEvidence(DeliveryRequest{TargetUserID: 0, NotAfter: time.Now().Add(time.Minute), Items: []DeliveryItem{item}})
	if err := validateDeliveryRequestEnvelope(request); err != nil {
		t.Fatalf("exact request envelope limit rejected: %v", err)
	}
	batch := testDeliveryBatchEvidence(DeliveryBatch{
		BatchID: BatchID{1}, CommandID: CommandID{2}, SourceInstanceID: "egress-a",
		TargetInstanceID: "edge-a", TargetUserID: 0, NotAfter: request.NotAfter, Items: request.Items,
	})
	if err := ValidateDeliveryBatchEnvelope(batch); err != nil {
		t.Fatalf("exact batch envelope limit rejected: %v", err)
	}

	request.Items[0].UpdateBytes = payload
	if err := validateDeliveryRequestEnvelope(request); err == nil {
		t.Fatal("request exceeding limit only through channel route metadata was accepted")
	}
	batch.Items = request.Items
	if err := ValidateDeliveryBatchEnvelope(batch); err == nil {
		t.Fatal("batch exceeding limit only through channel route metadata was accepted")
	}
}

func TestDeliveryFabricRejectsUnencodableOrUnboundedTargetTopology(t *testing.T) {
	raw, err := EncodeDeliveryUpdate(&tg.Updates{Date: 7})
	if err != nil {
		t.Fatal(err)
	}
	request := DeliveryRequest{
		TargetUserID: 42, NotAfter: time.Now().Add(time.Minute),
		Items: []DeliveryItem{{
			Ref: DeliveryRef{
				Domain: OrderingDomain{Kind: QueueAccountPTS, StreamID: 42}, OutboxID: 91,
				TargetUserID: 42, PTS: 8, LeaseFence: 11, Attempt: 2,
			},
			MessageType: proto.MessageFromServer, PayloadHash: DeliveryPayloadHash(raw), UpdateBytes: raw,
		}},
	}
	request.EvidenceDeadline = request.NotAfter.Add(time.Second)

	t.Run("source identity", func(t *testing.T) {
		fabric := NewDeliveryFabric(DeliveryFabricConfig{
			InstanceID: strings.Repeat("s", MaxDeliveryInstanceIDBytes+1),
			Registry:   deliveryPlanRegistry{}, Bus: &deliveryPlanBus{},
		})
		defer fabric.Close()
		if _, err := fabric.PrepareDelivery(context.Background(), request); err == nil {
			t.Fatal("oversized source instance identity was accepted")
		}
	})

	t.Run("non-canonical source identity", func(t *testing.T) {
		fabric := NewDeliveryFabric(DeliveryFabricConfig{
			InstanceID: " egress-a", Registry: deliveryPlanRegistry{}, Bus: &deliveryPlanBus{},
		})
		defer fabric.Close()
		if _, err := fabric.PrepareDelivery(context.Background(), request); err == nil {
			t.Fatal("non-canonical source instance identity was accepted")
		}
	})

	t.Run("target identity", func(t *testing.T) {
		fabric := NewDeliveryFabric(DeliveryFabricConfig{
			InstanceID: "egress-a",
			Registry: deliveryPlanRegistry{records: []LocationRecord{{
				InstanceID: strings.Repeat("t", MaxDeliveryInstanceIDBytes+1), UserID: 42, ReceivesUpdates: true,
			}}},
			Bus: &deliveryPlanBus{},
		})
		defer fabric.Close()
		if _, err := fabric.PrepareDelivery(context.Background(), request); err == nil {
			t.Fatal("oversized target instance identity was accepted")
		}
	})

	t.Run("non-canonical target identity", func(t *testing.T) {
		fabric := NewDeliveryFabric(DeliveryFabricConfig{
			InstanceID: "egress-a",
			Registry: deliveryPlanRegistry{records: []LocationRecord{{
				InstanceID: "edge-a ", UserID: 42, ReceivesUpdates: true,
			}}},
			Bus: &deliveryPlanBus{},
		})
		defer fabric.Close()
		if _, err := fabric.PrepareDelivery(context.Background(), request); err == nil {
			t.Fatal("non-canonical target instance identity was accepted")
		}
	})

	t.Run("non-canonical channel target", func(t *testing.T) {
		channelRequest := request
		channelRequest.TargetUserID = 0
		channelRequest.Items = append([]DeliveryItem(nil), request.Items...)
		channelRequest.Items[0].Ref.Domain = OrderingDomain{Kind: QueueChannelPTS, StreamID: 99}
		channelRequest.Items[0].Ref.TargetUserID = 0
		channelRequest.Items[0].Channel = ChannelDeliveryRoute{ChannelID: 99, Audience: ChannelAudienceMembers}
		fabric := NewDeliveryFabric(DeliveryFabricConfig{
			InstanceID: "egress-a",
			Registry:   deliveryPlanRegistry{channelTargets: []string{"edge-a", " edge-b"}},
			Bus:        &deliveryPlanBus{},
		})
		defer fabric.Close()
		if _, err := fabric.PrepareDelivery(context.Background(), channelRequest); err == nil {
			t.Fatal("non-canonical authoritative channel target was accepted")
		}
	})

	t.Run("target count", func(t *testing.T) {
		records := make([]LocationRecord, MaxDeliveryTargets+1)
		for i := range records {
			records[i] = LocationRecord{
				InstanceID: fmt.Sprintf("edge-%04d", i), UserID: 42, ReceivesUpdates: true,
			}
		}
		fabric := NewDeliveryFabric(DeliveryFabricConfig{
			InstanceID: "egress-a", Registry: deliveryPlanRegistry{records: records}, Bus: &deliveryPlanBus{},
		})
		defer fabric.Close()
		if _, err := fabric.PrepareDelivery(context.Background(), request); err == nil {
			t.Fatalf("target topology larger than %d was accepted", MaxDeliveryTargets)
		}
	})
}

func TestDeliveryEvidenceIdentityUsesWireStringBound(t *testing.T) {
	tracking := DeliveryTracking{
		BatchID: BatchID{1}, CommandID: CommandID{2}, SourceInstanceID: "egress-a",
		Ref: DeliveryRef{
			Domain: OrderingDomain{Kind: QueueAccountPTS, StreamID: 42}, OutboxID: 91,
			TargetUserID: 42, PTS: 8, LeaseFence: 11, Attempt: 2,
		},
	}
	if !tracking.Valid() {
		t.Fatal("valid delivery tracking rejected")
	}
	tracking.SourceInstanceID = strings.Repeat("s", MaxDeliveryInstanceIDBytes+1)
	if tracking.Valid() {
		t.Fatal("oversized tracking source instance identity was accepted")
	}
	tracking.SourceInstanceID = " egress-a"
	if tracking.Valid() {
		t.Fatal("non-canonical tracking source instance identity was accepted")
	}
	tracking.SourceInstanceID = "egress-a"
	observation := ClientAckObservation{
		Tracking: tracking, TargetInstanceID: "edge-a", AuthKeyID: [8]byte{1},
		SessionID: 1, ServerMsgID: 2, ObservedAt: time.Now().UTC(),
	}
	if !observation.Valid() {
		t.Fatal("valid client ACK observation rejected")
	}
	observation.TargetInstanceID = strings.Repeat("t", MaxDeliveryInstanceIDBytes+1)
	if observation.Valid() {
		t.Fatal("oversized ACK target instance identity was accepted")
	}
	observation.TargetInstanceID = "edge-a "
	if observation.Valid() {
		t.Fatal("non-canonical ACK target instance identity was accepted")
	}
}

func TestDeliveryFabricSameOwnerRehydrateAndDoesNotFilterSameNamedEdge(t *testing.T) {
	bus := &deliveryPlanBus{}
	registry := deliveryPlanRegistry{records: []LocationRecord{
		{InstanceID: "egress-a", UserID: 42, ReceivesUpdates: true},
		{InstanceID: "edge-b", UserID: 42, ReceivesUpdates: true},
	}}
	preparer := NewDeliveryFabric(DeliveryFabricConfig{InstanceID: "egress-a", Registry: registry, Bus: bus})
	defer preparer.Close()
	raw, err := EncodeDeliveryUpdate(&tg.Updates{Date: 9})
	if err != nil {
		t.Fatal(err)
	}
	request := testDeliveryRequestEvidence(DeliveryRequest{TargetUserID: 42, NotAfter: time.Now().Add(time.Minute), Items: []DeliveryItem{{
		Ref: DeliveryRef{
			Domain: OrderingDomain{Kind: QueueAccountPTS, StreamID: 42}, OutboxID: 92,
			TargetUserID: 42, PTS: 9, LeaseFence: 12, Attempt: 3,
		},
		MessageType: proto.MessageFromServer, PayloadHash: DeliveryPayloadHash(raw), UpdateBytes: raw,
	}}})
	prepared, err := preparer.PrepareDelivery(context.Background(), request)
	if err != nil {
		t.Fatalf("PrepareDelivery: %v", err)
	}
	if targets := prepared.Targets(); len(targets) != 2 || targets[0].TargetInstanceID != "edge-b" || targets[1].TargetInstanceID != "egress-a" {
		t.Fatalf("same-named Edge was filtered from targets: %+v", targets)
	}

	rehydrated, err := RehydrateDeliveryPlan(prepared.SourceInstanceID(), prepared.BatchID(), request, prepared.Targets())
	if err != nil {
		t.Fatalf("RehydrateDeliveryPlan: %v", err)
	}
	recoveryOwner := NewDeliveryFabric(DeliveryFabricConfig{InstanceID: "egress-a", Registry: registry, Bus: bus})
	defer recoveryOwner.Close()
	result, err := recoveryOwner.AdmitPreparedDelivery(context.Background(), rehydrated)
	if err != nil {
		t.Fatalf("same-owner AdmitPreparedDelivery: %v", err)
	}
	if len(result.Targets) != 2 || bus.count() != 2 {
		t.Fatalf("result=%+v publishes=%d", result, bus.count())
	}
	bus.mu.Lock()
	defer bus.mu.Unlock()
	for i, batch := range bus.batches {
		if batch.SourceInstanceID != "egress-a" {
			t.Fatalf("batch[%d] source=%q, want persisted source egress-a", i, batch.SourceInstanceID)
		}
	}
	wrongOwner := NewDeliveryFabric(DeliveryFabricConfig{InstanceID: "egress-recovery", Registry: registry, Bus: bus})
	defer wrongOwner.Close()
	if _, err := wrongOwner.AdmitPreparedDelivery(context.Background(), rehydrated); err == nil {
		t.Fatal("cross-owner exact frozen-plan replay was accepted")
	}
}

func TestDeliveryRefQueueKindsAreExplicit(t *testing.T) {
	base := DeliveryRef{OutboxID: 1, TargetUserID: 42, LeaseFence: 1, Attempt: 1}
	account := base
	account.Domain = OrderingDomain{Kind: QueueAccountPTS, StreamID: 42}
	account.PTS = 1
	nonPTS := base
	nonPTS.Domain = OrderingDomain{Kind: QueueAccountNonPTS, StreamID: 42}
	channel := base
	channel.Domain = OrderingDomain{Kind: QueueChannelPTS, StreamID: 99}
	channel.TargetUserID = 0
	channel.PTS = 2
	for _, ref := range []DeliveryRef{account, nonPTS, channel} {
		if !ref.Valid() {
			t.Fatalf("valid ref rejected: %+v", ref)
		}
	}
	inferred := base
	inferred.Domain = OrderingDomain{Kind: QueueAccountPTS, StreamID: 42}
	if inferred.Valid() {
		t.Fatal("AccountPTS accepted PTS=0 inference")
	}
}

func TestDeliveryFabricChannelPlanTargetsEdgesWithoutPerUserCommands(t *testing.T) {
	bus := &deliveryPlanBus{}
	fabric := NewDeliveryFabric(DeliveryFabricConfig{InstanceID: "egress-a", Registry: deliveryPlanRegistry{channelTargets: []string{"edge-b", "edge-b", "edge-c"}}, Bus: bus})
	defer fabric.Close()
	raw, err := EncodeDeliveryUpdate(&tg.Updates{Date: 7})
	if err != nil {
		t.Fatal(err)
	}
	ref := DeliveryRef{Domain: OrderingDomain{Kind: QueueChannelPTS, StreamID: 99}, OutboxID: 91, TargetUserID: 0, PTS: 8, LeaseFence: 11, Attempt: 2}
	request := testDeliveryRequestEvidence(DeliveryRequest{TargetUserID: 0, NotAfter: time.Now().Add(time.Minute), Items: []DeliveryItem{{
		Ref: ref, MessageType: proto.MessageFromServer, PayloadHash: DeliveryPayloadHash(raw), UpdateBytes: raw,
		Channel: ChannelDeliveryRoute{ChannelID: 99, Audience: ChannelAudienceMembers, AffectedUsers: []int64{42}},
	}}})
	plan, err := fabric.PrepareDelivery(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Targets()) != 2 {
		t.Fatalf("targets=%+v", plan.Targets())
	}
	if _, err := fabric.AdmitPreparedDelivery(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	bus.mu.Lock()
	defer bus.mu.Unlock()
	if len(bus.batches) != 2 {
		t.Fatalf("batches=%+v", bus.batches)
	}
	for _, batch := range bus.batches {
		if batch.TargetUserID != 0 || batch.Items[0].Ref.TargetUserID != 0 || batch.Items[0].Channel.ChannelID != 99 {
			t.Fatalf("per-user or missing channel route batch=%+v", batch)
		}
	}
}

func TestDeliveryFabricUsesFixedBoundedAdmissionActors(t *testing.T) {
	records := make([]LocationRecord, 64)
	for i := range records {
		records[i] = LocationRecord{InstanceID: fmt.Sprintf("edge-%d", i), UserID: 42, ReceivesUpdates: true}
	}
	bus := &blockingDeliveryPlanBus{entered: make(chan struct{}, 64), release: make(chan struct{})}
	fabric := NewDeliveryFabric(DeliveryFabricConfig{InstanceID: "egress-a", Registry: deliveryPlanRegistry{records: records}, Bus: bus, ExecutorShards: 8, MailboxSize: 64})
	defer fabric.Close()
	raw, err := EncodeDeliveryUpdate(&tg.Updates{Date: 7})
	if err != nil {
		t.Fatal(err)
	}
	ref := DeliveryRef{Domain: OrderingDomain{Kind: QueueAccountPTS, StreamID: 42}, OutboxID: 1, TargetUserID: 42, PTS: 1, LeaseFence: 1, Attempt: 1}
	plan, err := fabric.PrepareDelivery(context.Background(), testDeliveryRequestEvidence(DeliveryRequest{TargetUserID: 42, NotAfter: time.Now().Add(time.Minute), Items: []DeliveryItem{{Ref: ref, MessageType: proto.MessageFromServer, PayloadHash: DeliveryPayloadHash(raw), UpdateBytes: raw}}}))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := fabric.AdmitPreparedDelivery(context.Background(), plan); done <- err }()
	deadline := time.After(time.Second)
	for bus.peak.Load() < 2 {
		select {
		case <-bus.entered:
		case <-deadline:
			t.Fatal("admission actors did not run concurrently")
		}
	}
	if peak := bus.peak.Load(); peak > 8 {
		t.Fatalf("peak=%d exceeds fixed shard count", peak)
	}
	close(bus.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestDeliveryFabricAdmitsLargeMultiEdgePlanInBoundedByteWaves(t *testing.T) {
	raw, err := EncodeDeliveryUpdate(&tg.Updates{Date: 7})
	if err != nil {
		t.Fatal(err)
	}
	ref := DeliveryRef{
		Domain: OrderingDomain{Kind: QueueAccountPTS, StreamID: 42}, OutboxID: 1,
		TargetUserID: 42, PTS: 1, LeaseFence: 1, Attempt: 1,
	}
	item := DeliveryItem{
		Ref: ref, MessageType: proto.MessageFromServer,
		PayloadHash: DeliveryPayloadHash(raw), UpdateBytes: raw,
	}
	// Exactly one target command fits the byte budget. The other targets must
	// wait for the same fixed workers, not fail admission at the first burst.
	taskBytes := deliveryBatchPayloadBytes(DeliveryBatch{Items: []DeliveryItem{item}})
	bus := &blockingDeliveryPlanBus{entered: make(chan struct{}, 3), release: make(chan struct{})}
	fabric := NewDeliveryFabric(DeliveryFabricConfig{
		InstanceID: "egress-a",
		Registry: deliveryPlanRegistry{records: []LocationRecord{
			{InstanceID: "edge-a", UserID: 42, ReceivesUpdates: true},
			{InstanceID: "edge-b", UserID: 42, ReceivesUpdates: true},
			{InstanceID: "edge-c", UserID: 42, ReceivesUpdates: true},
		}},
		Bus: bus, ExecutorShards: 2, MailboxSize: 1, MailboxMaxBytes: int64(taskBytes),
	})
	defer fabric.Close()
	plan, err := fabric.PrepareDelivery(context.Background(), testDeliveryRequestEvidence(DeliveryRequest{
		TargetUserID: 42, NotAfter: time.Now().Add(time.Minute), Items: []DeliveryItem{item},
	}))
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := fabric.AdmitPreparedDelivery(context.Background(), plan)
		done <- err
	}()
	select {
	case <-bus.entered:
	case <-time.After(time.Second):
		t.Fatal("first byte-budgeted admission did not start")
	}
	if peak := bus.peak.Load(); peak != 1 {
		t.Fatalf("peak admissions=%d want exactly one under byte budget", peak)
	}
	close(bus.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("bounded wave admission: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("bounded wave admission did not finish")
	}
	if calls := bus.calls.Load(); calls != 3 {
		t.Fatalf("Redis admission calls=%d want all 3 frozen targets", calls)
	}
	if peak := bus.peak.Load(); peak > 1 {
		t.Fatalf("peak admissions=%d crossed one-command byte budget", peak)
	}
}

func TestDeliveryFabricDoesNotSerializeDifferentDomainsForSameSlowEdge(t *testing.T) {
	bus := &blockingDeliveryPlanBus{entered: make(chan struct{}, 2), release: make(chan struct{})}
	registry := deliveryPlanRegistry{records: []LocationRecord{{InstanceID: "edge-slow", UserID: 42, ReceivesUpdates: true}, {InstanceID: "edge-slow", UserID: 43, ReceivesUpdates: true}}}
	fabric := NewDeliveryFabric(DeliveryFabricConfig{InstanceID: "egress-a", Registry: registry, Bus: bus, ExecutorShards: 2, MailboxSize: 2})
	defer fabric.Close()
	raw, err := EncodeDeliveryUpdate(&tg.Updates{Date: 7})
	if err != nil {
		t.Fatal(err)
	}
	plans := make([]FrozenDeliveryPlan, 2)
	for i := range plans {
		userID := int64(42 + i)
		ref := DeliveryRef{Domain: OrderingDomain{Kind: QueueAccountPTS, StreamID: userID}, OutboxID: int64(i + 1), TargetUserID: userID, PTS: i + 1, LeaseFence: 1, Attempt: 1}
		plans[i], err = fabric.PrepareDelivery(context.Background(), testDeliveryRequestEvidence(DeliveryRequest{TargetUserID: userID, NotAfter: time.Now().Add(time.Minute), Items: []DeliveryItem{{Ref: ref, MessageType: proto.MessageFromServer, PayloadHash: DeliveryPayloadHash(raw), UpdateBytes: raw}}}))
		if err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 2)
	for i := range plans {
		go func(plan FrozenDeliveryPlan) {
			_, err := fabric.AdmitPreparedDelivery(context.Background(), plan)
			done <- err
		}(plans[i])
	}
	deadline := time.After(time.Second)
	for bus.peak.Load() < 2 {
		select {
		case <-bus.entered:
		case <-deadline:
			t.Fatal("same-target different domains were serialized")
		}
	}
	close(bus.release)
	for range plans {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}
