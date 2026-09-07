package mtprotoedge

import (
	"container/heap"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/iamxvbaba/td/proto"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tlprofile"
	"go.uber.org/zap/zaptest"

	"telesrv/internal/edgecontrol"
)

type capturePhysicalReceiptSink struct {
	receipts chan edgecontrol.PhysicalReceipt
}

func TestDeliveryFinalSocketGuardRejectsReboundAndReplacedSnapshot(t *testing.T) {
	manager := NewSessionManager(zaptest.NewLogger(t))
	key := sessionKey{authKeyID: [8]byte{7}, sessionID: 91}
	conn := &Conn{
		authKeyID: key.authKeyID, sessionID: key.sessionID,
		outbound: make(chan *outboundOp, 1), outboundControl: make(chan *outboundOp, 1),
	}
	conn.lifecycle.Store(uint32(connLifecycleActive))
	conn.userID.Store(42)
	conn.userIDResolved.Store(true)
	conn.receivesUpdates.Store(true)
	if err := conn.FreezeLayerProfile(tlprofile.ProfileCanonical); err != nil {
		t.Fatal(err)
	}
	manager.bySession[key] = conn
	manager.byUser[42] = map[sessionKey]*Conn{key: conn}

	snapshot := manager.snapshotReadyDeliveryConns(42, [8]byte{}, 0)
	if len(snapshot) != 1 {
		t.Fatalf("snapshot count = %d, want 1", len(snapshot))
	}
	fence := &deliveryFenceState{}
	fence.max.Store(3)
	hook := &deliverySessionWriteHook{
		aggregate: &deliveryWriteAggregate{fence: 3, fenceState: fence, notAfter: time.Now().Add(time.Minute)},
		frozen:    snapshot[0],
	}
	if !hook.allowPhysicalWrite(time.Now()) {
		t.Fatal("fresh frozen identity rejected")
	}

	// Returning to the same numeric user cannot resurrect a stale snapshot: the
	// immutable generation proves that an intervening account identity existed.
	manager.mu.Lock()
	manager.bindUserLocked(conn, key, 84)
	manager.bindUserLocked(conn, key, 42)
	manager.mu.Unlock()
	if hook.allowPhysicalWrite(time.Now()) {
		t.Fatal("rebound session passed final socket guard")
	}

	replacementSnapshot := manager.snapshotReadyDeliveryConns(42, [8]byte{}, 0)
	if len(replacementSnapshot) != 1 {
		t.Fatalf("replacement snapshot count = %d, want 1", len(replacementSnapshot))
	}
	replacementHook := &deliverySessionWriteHook{
		aggregate: &deliveryWriteAggregate{fence: 3, fenceState: fence, notAfter: time.Now().Add(time.Minute)},
		frozen:    replacementSnapshot[0],
	}
	conn.retire()
	if replacementHook.allowPhysicalWrite(time.Now()) {
		t.Fatal("retired/replaced physical generation passed final socket guard")
	}
}

func TestDeliveryFinalSocketGuardRejectsWorkAfterTerminalReceipt(t *testing.T) {
	fence := &deliveryFenceState{}
	fence.max.Store(9)
	aggregate := &deliveryWriteAggregate{fence: 9, fenceState: fence, notAfter: time.Now().Add(time.Minute)}
	if !aggregate.allowPhysicalWrite(time.Now()) {
		t.Fatal("live delivery aggregate rejected before terminal receipt")
	}
	aggregate.done.Store(true)
	if aggregate.allowPhysicalWrite(time.Now()) {
		t.Fatal("queued write remained eligible after aggregate became terminal")
	}
}

func (s *capturePhysicalReceiptSink) ReservePhysicalReceipts(batch edgecontrol.BatchID, source, target string, command edgecontrol.CommandID, _ time.Time, refs []edgecontrol.DeliveryRef) (edgecontrol.PhysicalReceiptReservation, error) {
	return &capturePhysicalReservation{sink: s, batch: batch, source: source, target: target, command: command, refs: append([]edgecontrol.DeliveryRef(nil), refs...), done: make([]bool, len(refs))}, nil
}

type capturePhysicalReservation struct {
	mu      sync.Mutex
	sink    *capturePhysicalReceiptSink
	batch   edgecontrol.BatchID
	source  string
	target  string
	command edgecontrol.CommandID
	refs    []edgecontrol.DeliveryRef
	done    []bool
}

func (r *capturePhysicalReservation) Commit(index int, receipt edgecontrol.PhysicalReceipt) {
	r.mu.Lock()
	if index < 0 || index >= len(r.done) || r.done[index] || receipt.BatchID != r.batch || receipt.CommandID != r.command || receipt.SourceInstanceID != r.source || receipt.TargetInstanceID != r.target || receipt.Ref != r.refs[index] {
		r.mu.Unlock()
		return
	}
	r.done[index] = true
	r.mu.Unlock()
	r.sink.receipts <- receipt
}

func (r *capturePhysicalReservation) Release(index int) {
	r.mu.Lock()
	if index >= 0 && index < len(r.done) {
		r.done[index] = true
	}
	r.mu.Unlock()
}

func TestDeliveryExecutorSeparatesAdmissionFromPhysicalReceiptAndDeduplicates(t *testing.T) {
	manager := NewSessionManager(zaptest.NewLogger(t))
	sink := &capturePhysicalReceiptSink{receipts: make(chan edgecontrol.PhysicalReceipt, 2)}
	if err := manager.SetPhysicalReceiptSink(sink); err != nil {
		t.Fatal(err)
	}
	defer manager.CloseDeliveryExecutor()
	payload, err := edgecontrol.EncodeDeliveryUpdate(&tg.Updates{Date: 7})
	if err != nil {
		t.Fatal(err)
	}
	ref := edgecontrol.DeliveryRef{Domain: edgecontrol.OrderingDomain{Kind: edgecontrol.QueueAccountPTS, StreamID: 42}, OutboxID: 9, TargetUserID: 42, PTS: 3, LeaseFence: 8, Attempt: 2}
	batch := edgecontrol.DeliveryBatch{
		BatchID: edgecontrol.BatchID{1}, CommandID: edgecontrol.CommandID{2}, SourceInstanceID: "egress-a", TargetInstanceID: "edge-b",
		TargetUserID: 42, NotAfter: time.Now().Add(time.Second), EvidenceDeadline: time.Now().Add(2 * time.Second),
		Items: []edgecontrol.DeliveryItem{{Ref: ref, MessageType: proto.MessageFromServer, PayloadHash: edgecontrol.DeliveryPayloadHash(payload), UpdateBytes: payload}},
	}
	admission := manager.AdmitDeliveryBatch(context.Background(), batch)
	if admission.Outcome != edgecontrol.AdmissionAccepted {
		t.Fatalf("admission=%+v", admission)
	}
	select {
	case receipt := <-sink.receipts:
		if receipt.Outcome != edgecontrol.PhysicalNoEligibleSessions || receipt.BatchID != batch.BatchID || receipt.CommandID != batch.CommandID || receipt.ObservedAt.IsZero() {
			t.Fatalf("receipt=%+v", receipt)
		}
	case <-time.After(time.Second):
		t.Fatal("physical receipt not committed")
	}
	duplicate := manager.AdmitDeliveryBatch(context.Background(), batch)
	if duplicate.Outcome != edgecontrol.AdmissionDuplicateTerminal {
		t.Fatalf("duplicate=%+v", duplicate)
	}
	select {
	case replayed := <-sink.receipts:
		if replayed.Ref != ref || replayed.Outcome != edgecontrol.PhysicalNoEligibleSessions {
			t.Fatalf("replayed terminal receipt=%+v", replayed)
		}
	case <-time.After(time.Second):
		t.Fatal("duplicate terminal admission did not republish exact receipt")
	}
	conflict := batch
	conflict.Items = append([]edgecontrol.DeliveryItem(nil), batch.Items...)
	conflict.Items[0].UpdateBytes = append([]byte(nil), payload...)
	conflict.Items[0].UpdateBytes = append(conflict.Items[0].UpdateBytes, 0)
	conflict.Items[0].PayloadHash = edgecontrol.DeliveryPayloadHash(conflict.Items[0].UpdateBytes)
	conflicting := manager.AdmitDeliveryBatch(context.Background(), conflict)
	if conflicting.Outcome != edgecontrol.AdmissionRejected || conflicting.Detail != edgecontrol.DetailAttemptConflict {
		t.Fatalf("conflicting=%+v", conflicting)
	}
	manager.mu.RLock()
	pending := len(manager.pending)
	manager.mu.RUnlock()
	if pending != 0 {
		t.Fatalf("durable delivery entered SessionManager.pending: %d", pending)
	}
}

func TestDeliveryExecutorAfterCommandDeadlineOnlyRepublishesExactTerminalReceipt(t *testing.T) {
	manager := NewSessionManager(zaptest.NewLogger(t))
	sink := &capturePhysicalReceiptSink{receipts: make(chan edgecontrol.PhysicalReceipt, 2)}
	if err := manager.SetPhysicalReceiptSink(sink); err != nil {
		t.Fatal(err)
	}
	defer manager.CloseDeliveryExecutor()
	payload, err := edgecontrol.EncodeDeliveryUpdate(&tg.Updates{Date: 8})
	if err != nil {
		t.Fatal(err)
	}
	ref := edgecontrol.DeliveryRef{
		Domain:   edgecontrol.OrderingDomain{Kind: edgecontrol.QueueAccountPTS, StreamID: 43},
		OutboxID: 10, TargetUserID: 43, PTS: 4, LeaseFence: 9, Attempt: 1,
	}
	notAfter := time.Now().Add(40 * time.Millisecond)
	batch := edgecontrol.DeliveryBatch{
		BatchID: edgecontrol.BatchID{3}, CommandID: edgecontrol.CommandID{4},
		SourceInstanceID: "egress-a", TargetInstanceID: "edge-b", TargetUserID: 43,
		NotAfter: notAfter, EvidenceDeadline: notAfter.Add(time.Second),
		Items: []edgecontrol.DeliveryItem{{
			Ref: ref, MessageType: proto.MessageFromServer,
			PayloadHash: edgecontrol.DeliveryPayloadHash(payload), UpdateBytes: payload,
		}},
	}
	if admission := manager.AdmitDeliveryBatch(context.Background(), batch); admission.Outcome != edgecontrol.AdmissionAccepted {
		t.Fatalf("initial admission=%+v", admission)
	}
	select {
	case receipt := <-sink.receipts:
		if receipt.Outcome != edgecontrol.PhysicalNoEligibleSessions {
			t.Fatalf("initial receipt=%+v", receipt)
		}
	case <-time.After(time.Second):
		t.Fatal("initial terminal receipt not committed")
	}
	time.Sleep(time.Until(notAfter) + 10*time.Millisecond)
	if admission := manager.AdmitDeliveryBatch(context.Background(), batch); admission.Outcome != edgecontrol.AdmissionDuplicateTerminal {
		t.Fatalf("receipt-only replay admission=%+v", admission)
	}
	select {
	case receipt := <-sink.receipts:
		if receipt.Ref != ref || receipt.Outcome != edgecontrol.PhysicalNoEligibleSessions {
			t.Fatalf("replayed receipt=%+v", receipt)
		}
	case <-time.After(time.Second):
		t.Fatal("receipt-only replay did not republish terminal evidence")
	}
}

func TestDeliveryExecutorAfterCommandDeadlineCannotCreateAttemptFenceOrSocketWork(t *testing.T) {
	payload, err := edgecontrol.EncodeDeliveryUpdate(&tg.Updates{Date: 9})
	if err != nil {
		t.Fatal(err)
	}
	sink := &capturePhysicalReceiptSink{receipts: make(chan edgecontrol.PhysicalReceipt, 1)}
	executor := &deliveryExecutor{sink: sink}
	shard := &deliveryExecutorShard{
		owner: executor, attempts: make(map[edgecontrol.DeliveryRef]*deliveryAttemptEntry),
		fences: make(map[edgecontrol.OrderingDomain]*deliveryFenceState),
	}
	heap.Init(&shard.deadlines)
	now := time.Now()
	batch := edgecontrol.DeliveryBatch{
		BatchID: edgecontrol.BatchID{5}, CommandID: edgecontrol.CommandID{6},
		SourceInstanceID: "egress-a", TargetInstanceID: "edge-b", TargetUserID: 44,
		NotAfter: now.Add(-time.Millisecond), EvidenceDeadline: now.Add(time.Second),
		Items: []edgecontrol.DeliveryItem{{
			Ref: edgecontrol.DeliveryRef{
				Domain:   edgecontrol.OrderingDomain{Kind: edgecontrol.QueueAccountPTS, StreamID: 44},
				OutboxID: 11, TargetUserID: 44, PTS: 5, LeaseFence: 10, Attempt: 1,
			},
			MessageType: proto.MessageFromServer,
			PayloadHash: edgecontrol.DeliveryPayloadHash(payload), UpdateBytes: payload,
		}},
	}
	done := make(chan edgecontrol.DeliveryAdmission, 1)
	shard.handle(deliveryCommandTask{
		ctx: context.Background(), batch: batch, replayOnly: true, done: done,
	})
	if admission := <-done; admission.Outcome != edgecontrol.AdmissionRejected || admission.Detail != edgecontrol.DetailDeadline {
		t.Fatalf("missing exact replay admission=%+v", admission)
	}
	if len(shard.attempts) != 0 || len(shard.fences) != 0 || len(shard.deadlines) != 0 || shard.active != 0 || shard.retainedBytes != 0 {
		t.Fatalf("receipt-only miss mutated actor: attempts=%d fences=%d deadlines=%d active=%d retained=%d",
			len(shard.attempts), len(shard.fences), len(shard.deadlines), shard.active, shard.retainedBytes)
	}
	select {
	case receipt := <-sink.receipts:
		t.Fatalf("receipt-only miss emitted physical receipt: %+v", receipt)
	default:
	}
}

func TestHigherFenceAdvancesBeforeCapacityOverload(t *testing.T) {
	payload, err := edgecontrol.EncodeDeliveryUpdate(&tg.Updates{Date: 7})
	if err != nil {
		t.Fatal(err)
	}
	domain := edgecontrol.OrderingDomain{Kind: edgecontrol.QueueAccountPTS, StreamID: 42}
	oldExpiresAt := time.Now().Add(time.Minute)
	oldFence := &deliveryFenceState{expiresAt: oldExpiresAt}
	oldFence.max.Store(7)
	executor := &deliveryExecutor{}
	shard := &deliveryExecutorShard{
		owner: executor, attempts: make(map[edgecontrol.DeliveryRef]*deliveryAttemptEntry),
		fences: map[edgecontrol.OrderingDomain]*deliveryFenceState{domain: oldFence},
		active: deliveryAttemptsPerShard,
	}
	heap.Init(&shard.deadlines)
	batch := edgecontrol.DeliveryBatch{
		BatchID: edgecontrol.BatchID{21}, CommandID: edgecontrol.CommandID{22},
		SourceInstanceID: "egress-new", TargetInstanceID: "edge-a", TargetUserID: 42,
		NotAfter: time.Now().Add(time.Second), EvidenceDeadline: time.Now().Add(2 * time.Second),
		Items: []edgecontrol.DeliveryItem{{
			Ref: edgecontrol.DeliveryRef{
				Domain: domain, OutboxID: 23, TargetUserID: 42, PTS: 24, LeaseFence: 8, Attempt: 2,
			},
			MessageType: proto.MessageFromServer, PayloadHash: edgecontrol.DeliveryPayloadHash(payload), UpdateBytes: payload,
		}},
	}
	done := make(chan edgecontrol.DeliveryAdmission, 1)
	shard.handle(deliveryCommandTask{ctx: context.Background(), batch: batch, done: done})
	admission := <-done
	if admission.Outcome != edgecontrol.AdmissionOverloaded || admission.Detail != edgecontrol.DetailCapacity {
		t.Fatalf("admission = %+v, want capacity overload", admission)
	}
	if got := oldFence.max.Load(); got != 8 {
		t.Fatalf("max fence = %d, want higher validated fence 8 despite overload", got)
	}
	guard := &deliveryWriteAggregate{fence: 7, fenceState: oldFence, notAfter: time.Now().Add(time.Minute)}
	if guard.allowPhysicalWrite(time.Now()) {
		t.Fatal("old socket guard remained writable after higher-fence overload")
	}
	if len(shard.deadlines) != 1 || shard.deadlines[0].kind != deliveryDeadlineFence ||
		shard.deadlines[0].fence != 8 || !shard.deadlines[0].at.Equal(oldExpiresAt) {
		t.Fatalf("higher fence cleanup deadline = %+v, want fence 8 at %s", shard.deadlines, oldExpiresAt)
	}
	for fence := uint64(9); fence < 10_000; fence++ {
		batch.Items[0].Ref.LeaseFence = fence
		batch.NotAfter = time.Now().Add(time.Second)
		done = make(chan edgecontrol.DeliveryAdmission, 1)
		shard.handle(deliveryCommandTask{ctx: context.Background(), batch: batch, done: done})
		if admission := <-done; admission.Outcome != edgecontrol.AdmissionOverloaded {
			t.Fatalf("higher fence %d admission = %+v, want capacity overload", fence, admission)
		}
		if len(shard.deadlines) != 1 {
			t.Fatalf("higher fence %d grew cleanup heap to %d entries", fence, len(shard.deadlines))
		}
	}
	if got := oldFence.max.Load(); got != 9_999 {
		t.Fatalf("max fence after sustained overload = %d, want 9999", got)
	}
	if shard.deadlines[0].fence != 9_999 || shard.deadlines[0].fenceState != oldFence || oldFence.deadlineSlot != 1 {
		t.Fatalf("final indexed fence deadline = %+v slot=%d", shard.deadlines[0], oldFence.deadlineSlot)
	}
	shard.expire(oldExpiresAt.Add(time.Nanosecond))
	if _, ok := shard.fences[domain]; ok {
		t.Fatal("higher fence with shorter deadline remained resident after retained expiry")
	}
	if len(shard.deadlines) != 0 || oldFence.deadlineSlot != 0 {
		t.Fatalf("expired fence retained heap state: deadlines=%d slot=%d", len(shard.deadlines), oldFence.deadlineSlot)
	}
}

func TestDeliveryFanoutRetainsCommandPayloadByteBudgetUntilTerminal(t *testing.T) {
	payload, err := edgecontrol.EncodeDeliveryUpdate(&tg.Updates{Date: 7})
	if err != nil {
		t.Fatal(err)
	}
	sink := &capturePhysicalReceiptSink{receipts: make(chan edgecontrol.PhysicalReceipt, 1)}
	executor := &deliveryExecutor{sink: sink}
	shard := &deliveryExecutorShard{
		owner:  executor,
		fanout: make(chan *deliveryFanoutTask, 1), terminal: make(chan deliveryTerminalEvent, 1),
		attempts: make(map[edgecontrol.DeliveryRef]*deliveryAttemptEntry),
		fences:   make(map[edgecontrol.OrderingDomain]*deliveryFenceState),
	}
	heap.Init(&shard.deadlines)
	ref := edgecontrol.DeliveryRef{
		Domain:   edgecontrol.OrderingDomain{Kind: edgecontrol.QueueAccountPTS, StreamID: 62},
		OutboxID: 41, TargetUserID: 62, PTS: 42, LeaseFence: 11, Attempt: 1,
	}
	batch := edgecontrol.DeliveryBatch{
		BatchID: edgecontrol.BatchID{41}, CommandID: edgecontrol.CommandID{42},
		SourceInstanceID: "egress-a", TargetInstanceID: "edge-a", TargetUserID: 62,
		NotAfter: time.Now().Add(time.Second), EvidenceDeadline: time.Now().Add(2 * time.Second),
		Items: []edgecontrol.DeliveryItem{{
			Ref: ref, MessageType: proto.MessageFromServer,
			PayloadHash: edgecontrol.DeliveryPayloadHash(payload), UpdateBytes: payload,
		}},
	}
	bytes := deliveryBatchBytes(batch)
	executor.used.Store(int64(bytes))
	done := make(chan edgecontrol.DeliveryAdmission, 1)
	shard.handle(deliveryCommandTask{ctx: context.Background(), batch: batch, bytes: bytes, done: done})
	if admission := <-done; admission.Outcome != edgecontrol.AdmissionAccepted {
		t.Fatalf("admission = %+v, want accepted", admission)
	}
	if got := executor.used.Load(); got != int64(bytes) {
		t.Fatalf("payload budget after admission = %d, want retained %d", got, bytes)
	}
	fanout := <-shard.fanout
	shard.failFanout(fanout, edgecontrol.DetailCapacity)
	if got := executor.used.Load(); got != 0 {
		t.Fatalf("payload budget after fanout terminal = %d, want 0", got)
	}
}

func TestTerminalRetentionBudgetBackpressuresAndExpiresWithoutEviction(t *testing.T) {
	payload, err := edgecontrol.EncodeDeliveryUpdate(&tg.Updates{Date: 7})
	if err != nil {
		t.Fatal(err)
	}
	ref := edgecontrol.DeliveryRef{
		Domain:   edgecontrol.OrderingDomain{Kind: edgecontrol.QueueAccountPTS, StreamID: 52},
		OutboxID: 31, TargetUserID: 52, PTS: 32, LeaseFence: 9, Attempt: 1,
	}
	deadline := time.Now().Add(time.Second)
	evidenceDeadline := deadline.Add(time.Second)
	entry := &deliveryAttemptEntry{
		deadline: deadline, evidenceUntil: evidenceDeadline, terminalAt: time.Now(), retainedBytes: deliveryRetainedBytesPerShard,
		terminal: edgecontrol.PhysicalReceipt{Ref: ref, Outcome: edgecontrol.PhysicalNoEligibleSessions},
	}
	executor := &deliveryExecutor{}
	shard := &deliveryExecutorShard{
		owner: executor, attempts: map[edgecontrol.DeliveryRef]*deliveryAttemptEntry{ref: entry},
		fences:        make(map[edgecontrol.OrderingDomain]*deliveryFenceState),
		retainedBytes: deliveryRetainedBytesPerShard,
	}
	heap.Init(&shard.deadlines)
	heap.Push(&shard.deadlines, deliveryDeadlineEntry{at: evidenceDeadline, kind: deliveryDeadlineTerminal, ref: ref})

	newBatch := edgecontrol.DeliveryBatch{
		BatchID: edgecontrol.BatchID{31}, CommandID: edgecontrol.CommandID{32},
		SourceInstanceID: "egress-a", TargetInstanceID: "edge-a", TargetUserID: 53,
		NotAfter: deadline, EvidenceDeadline: evidenceDeadline,
		Items: []edgecontrol.DeliveryItem{{
			Ref: edgecontrol.DeliveryRef{
				Domain:   edgecontrol.OrderingDomain{Kind: edgecontrol.QueueAccountPTS, StreamID: 53},
				OutboxID: 33, TargetUserID: 53, PTS: 34, LeaseFence: 10, Attempt: 1,
			},
			MessageType: proto.MessageFromServer, PayloadHash: edgecontrol.DeliveryPayloadHash(payload), UpdateBytes: payload,
		}},
	}
	done := make(chan edgecontrol.DeliveryAdmission, 1)
	shard.handle(deliveryCommandTask{ctx: context.Background(), batch: newBatch, done: done})
	if admission := <-done; admission.Outcome != edgecontrol.AdmissionOverloaded || admission.Detail != edgecontrol.DetailCapacity {
		t.Fatalf("retention budget admission = %+v, want capacity overload", admission)
	}
	if shard.attempts[ref] != entry || shard.retainedBytes != deliveryRetainedBytesPerShard {
		t.Fatal("live terminal identity was evicted to admit new physical work")
	}
	shard.expire(evidenceDeadline)
	if _, ok := shard.attempts[ref]; ok || shard.retainedBytes != 0 {
		t.Fatalf("expired terminal retained: present=%t bytes=%d", ok, shard.retainedBytes)
	}
}

func TestDeliveryExecutorCommandNotAfterBoundsMissingSocketReceipt(t *testing.T) {
	manager := NewSessionManager(zaptest.NewLogger(t))
	key := sessionKey{authKeyID: [8]byte{8}, sessionID: 92}
	conn := &Conn{
		authKeyID: key.authKeyID, sessionID: key.sessionID,
		outbound: make(chan *outboundOp, 1), outboundControl: make(chan *outboundOp, 1),
		outboundCritical: make(chan *outboundOp, 1), outboundBulk: make(chan *outboundOp, 1),
		outboundStop: make(chan struct{}),
	}
	conn.lifecycle.Store(uint32(connLifecycleActive))
	conn.userID.Store(42)
	conn.userIDResolved.Store(true)
	conn.receivesUpdates.Store(true)
	if err := conn.FreezeLayerProfile(tlprofile.ProfileCanonical); err != nil {
		t.Fatal(err)
	}
	manager.bySession[key] = conn
	manager.byUser[42] = map[sessionKey]*Conn{key: conn}

	sink := &capturePhysicalReceiptSink{receipts: make(chan edgecontrol.PhysicalReceipt, 1)}
	if err := manager.SetPhysicalReceiptSink(sink); err != nil {
		t.Fatal(err)
	}
	defer manager.CloseDeliveryExecutor()
	payload, err := edgecontrol.EncodeDeliveryUpdate(&tg.Updates{Date: 7})
	if err != nil {
		t.Fatal(err)
	}
	ref := edgecontrol.DeliveryRef{Domain: edgecontrol.OrderingDomain{Kind: edgecontrol.QueueAccountPTS, StreamID: 42}, OutboxID: 10, TargetUserID: 42, PTS: 4, LeaseFence: 9, Attempt: 1}
	notAfter := time.Now().Add(80 * time.Millisecond)
	batch := edgecontrol.DeliveryBatch{
		BatchID: edgecontrol.BatchID{3}, CommandID: edgecontrol.CommandID{4},
		SourceInstanceID: "egress-a", TargetInstanceID: "edge-b", TargetUserID: 42,
		NotAfter: notAfter, EvidenceDeadline: notAfter.Add(100 * time.Millisecond),
		Items: []edgecontrol.DeliveryItem{{Ref: ref, MessageType: proto.MessageFromServer, PayloadHash: edgecontrol.DeliveryPayloadHash(payload), UpdateBytes: payload}},
	}
	if admission := manager.AdmitDeliveryBatch(context.Background(), batch); admission.Outcome != edgecontrol.AdmissionAccepted {
		t.Fatalf("admission=%+v", admission)
	}
	select {
	case receipt := <-sink.receipts:
		if receipt.Outcome != edgecontrol.PhysicalIndeterminate || receipt.Detail != edgecontrol.DetailDeadline {
			t.Fatalf("receipt=%+v", receipt)
		}
		if receipt.ObservedAt.Before(notAfter.Add(-10 * time.Millisecond)) {
			t.Fatalf("receipt arrived before command NotAfter: observed=%v not_after=%v", receipt.ObservedAt, notAfter)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("missing socket receipt waited beyond command NotAfter")
	}
}

func TestChannelDeliverySnapshotMergesLocalPresenceAndFreezesIdentity(t *testing.T) {
	manager := NewSessionManager(zaptest.NewLogger(t))
	newConn := func(auth byte, sessionID, userID int64) (sessionKey, *Conn) {
		key := sessionKey{authKeyID: [8]byte{auth}, sessionID: sessionID}
		conn := &Conn{
			authKeyID: key.authKeyID, sessionID: sessionID,
			outbound: make(chan *outboundOp, 1), outboundControl: make(chan *outboundOp, 1),
		}
		conn.lifecycle.Store(uint32(connLifecycleActive))
		conn.userID.Store(userID)
		conn.userIDResolved.Store(true)
		conn.receivesUpdates.Store(true)
		if err := conn.FreezeLayerProfile(tlprofile.ProfileCanonical); err != nil {
			t.Fatal(err)
		}
		manager.bySession[key] = conn
		if manager.byUser[userID] == nil {
			manager.byUser[userID] = make(map[sessionKey]*Conn)
		}
		manager.byUser[userID][key] = conn
		return key, conn
	}
	memberKey, member := newConn(1, 1, 101)
	interestKey, _ := newConn(2, 2, 102)
	subscribedKey, _ := newConn(3, 3, 103)
	expiredKey, _ := newConn(4, 4, 104)
	_, _ = newConn(5, 5, 105)
	const channelID = int64(900)
	manager.byMemberChannel[channelID] = map[sessionKey]int64{memberKey: 101}
	manager.byChannel[channelID] = map[sessionKey]int64{memberKey: 101, interestKey: 102}
	manager.bySubscribedChannel[channelID] = map[sessionKey]channelSubscription{
		subscribedKey: {userID: 103, expiresAt: time.Now().Add(time.Minute).UnixNano()},
		expiredKey:    {userID: 104, expiresAt: time.Now().Add(-time.Second).UnixNano()},
	}
	snapshot := manager.snapshotReadyChannelDeliveryConns(edgecontrol.ChannelDeliveryRoute{
		ChannelID: channelID, Audience: edgecontrol.ChannelAudienceMembers, AffectedUsers: []int64{105},
	})
	if len(snapshot) != 4 {
		t.Fatalf("snapshot count = %d, want member+interest+subscription+affected", len(snapshot))
	}
	seen := make(map[int64]frozenDeliveryConn, len(snapshot))
	for _, frozen := range snapshot {
		seen[frozen.expectedUserID] = frozen
	}
	for _, userID := range []int64{101, 102, 103, 105} {
		if !seen[userID].stillEligible([8]byte{}, 0) {
			t.Fatalf("user %d missing or not frozen eligible", userID)
		}
	}
	if _, ok := seen[104]; ok {
		t.Fatal("expired subscription entered delivery snapshot")
	}
	member.deliveryIdentityGeneration.Add(1)
	if seen[101].stillEligible([8]byte{}, 0) {
		t.Fatal("snapshot survived an identity-generation change")
	}
}

func TestMonoforumFrozenAudienceExpandsOnlyExplicitLocalSessions(t *testing.T) {
	manager := NewSessionManager(zaptest.NewLogger(t))
	newConn := func(auth byte, sessionID, userID int64) sessionKey {
		key := sessionKey{authKeyID: [8]byte{auth}, sessionID: sessionID}
		conn := &Conn{
			authKeyID: key.authKeyID, sessionID: sessionID,
			outbound: make(chan *outboundOp, 1), outboundControl: make(chan *outboundOp, 1),
		}
		conn.lifecycle.Store(uint32(connLifecycleActive))
		conn.userID.Store(userID)
		conn.userIDResolved.Store(true)
		conn.receivesUpdates.Store(true)
		if err := conn.FreezeLayerProfile(tlprofile.ProfileCanonical); err != nil {
			t.Fatal(err)
		}
		manager.bySession[key] = conn
		if manager.byUser[userID] == nil {
			manager.byUser[userID] = make(map[sessionKey]*Conn)
		}
		manager.byUser[userID][key] = conn
		return key
	}
	savedPeerKey := newConn(11, 11, 201)
	managerKey := newConn(12, 12, 202)
	ordinaryKey := newConn(13, 13, 203)
	const channelID = int64(902)
	manager.byMemberChannel[channelID] = map[sessionKey]int64{
		savedPeerKey: 201, managerKey: 202, ordinaryKey: 203,
	}
	manager.byChannel[channelID] = map[sessionKey]int64{
		savedPeerKey: 201, managerKey: 202, ordinaryKey: 203,
	}

	snapshot := manager.snapshotReadyChannelDeliveryConns(edgecontrol.ChannelDeliveryRoute{
		ChannelID: channelID, Audience: edgecontrol.ChannelAudienceMessageBox,
		AudienceUsers: []int64{201, 202},
	})
	if len(snapshot) != 2 {
		t.Fatalf("message-box snapshot count = %d, want saved peer + frozen manager", len(snapshot))
	}
	seen := make(map[int64]struct{}, len(snapshot))
	for _, frozen := range snapshot {
		seen[frozen.expectedUserID] = struct{}{}
	}
	if _, ok := seen[201]; !ok {
		t.Fatal("saved peer missing from frozen message-box audience")
	}
	if _, ok := seen[202]; !ok {
		t.Fatal("authorized manager missing from frozen message-box audience")
	}
	if _, ok := seen[203]; ok {
		t.Fatal("ordinary channel member leaked into frozen message-box audience")
	}

	postRevocation := manager.snapshotReadyChannelDeliveryConns(edgecontrol.ChannelDeliveryRoute{
		ChannelID: channelID, Audience: edgecontrol.ChannelAudienceMessageBox,
		AudienceUsers: []int64{201},
	})
	if len(postRevocation) != 1 || postRevocation[0].expectedUserID != 201 {
		t.Fatalf("post-revocation snapshot = %+v, want saved peer only", postRevocation)
	}
}
