package edgecontrol

import (
	"context"
	"crypto/rand"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iamxvbaba/td/proto"
)

const (
	defaultDeliveryCommandTimeout = 2 * time.Second
	minDeliveryCommandTimeout     = 500 * time.Millisecond
	deliverySubscribeRetry        = time.Second
	defaultFabricExecutorShards   = 16
	defaultFabricMailboxSize      = 256
	defaultFabricMailboxBytes     = int64(64 << 20)
)

type DeliveryFabricConfig struct {
	InstanceID      string
	Registry        LocationRegistry
	Bus             DeliveryCommandBus
	CommandTimeout  time.Duration
	ExecutorShards  int
	MailboxSize     int
	MailboxMaxBytes int64
}

type DeliveryFabric struct {
	instanceID     string
	registry       LocationRegistry
	bus            DeliveryCommandBus
	commandTimeout time.Duration
	executor       *fabricAdmissionExecutor
}

func NewDeliveryFabric(cfg DeliveryFabricConfig) *DeliveryFabric {
	f := &DeliveryFabric{
		instanceID:     cfg.InstanceID,
		registry:       cfg.Registry,
		bus:            cfg.Bus,
		commandTimeout: cfg.CommandTimeout,
	}
	f.executor = newFabricAdmissionExecutor(f, cfg.ExecutorShards, cfg.MailboxSize, cfg.MailboxMaxBytes)
	return f
}

func (f *DeliveryFabric) Close() {
	if f != nil && f.executor != nil {
		f.executor.close()
	}
}

// PrepareDelivery performs the read-only routing snapshot and allocates every
// protocol identity. It never publishes; Egress must durably bind Targets()
// before calling AdmitPreparedDelivery.
func (f *DeliveryFabric) PrepareDelivery(ctx context.Context, req DeliveryRequest) (FrozenDeliveryPlan, error) {
	var plan FrozenDeliveryPlan
	if f == nil || f.registry == nil || f.bus == nil || f.executor == nil {
		return plan, ErrDeliveryIndeterminate
	}
	if !ValidDeliveryInstanceID(f.instanceID) {
		return plan, fmt.Errorf("edgecontrol: invalid delivery source instance identity")
	}
	if err := validateDeliveryRequest(req); err != nil {
		return plan, err
	}
	if !time.Now().Before(req.NotAfter) {
		return plan, fmt.Errorf("edgecontrol: delivery request deadline elapsed")
	}
	if req.Items[0].Ref.Domain.Kind != QueueChannelPTS {
		route, err := f.PrepareAccountDeliveryRoute(ctx, AccountDeliveryRouteRequest{
			TargetUserID: req.TargetUserID, ExcludeAuthKeyID: req.ExcludeAuthKeyID,
			ExcludeSessionID: req.ExcludeSessionID, NotAfter: req.NotAfter,
			EvidenceDeadline: req.EvidenceDeadline,
		})
		if err != nil {
			return plan, err
		}
		return route.BindDelivery(req)
	}
	var targets []string
	registry, ok := f.registry.(ChannelDeliveryTargetRegistry)
	if !ok {
		return plan, fmt.Errorf("edgecontrol: channel delivery target registry is required")
	}
	routes := make([]ChannelDeliveryRoute, len(req.Items))
	for i := range req.Items {
		routes[i] = req.Items[i].Channel
	}
	var err error
	targets, err = registry.ListChannelDeliveryTargets(ctx, routes)
	if err != nil {
		return plan, err
	}
	targets, err = canonicalDeliveryInstanceIDs(targets)
	if err != nil {
		return plan, err
	}
	if len(targets) > MaxDeliveryTargets {
		return plan, fmt.Errorf("edgecontrol: delivery target count %d exceeds %d", len(targets), MaxDeliveryTargets)
	}
	for _, target := range targets {
		if !ValidDeliveryInstanceID(target) {
			return plan, fmt.Errorf("edgecontrol: invalid delivery target instance identity")
		}
	}
	batchID, err := newDeliveryBatchID()
	if err != nil {
		return plan, fmt.Errorf("edgecontrol: allocate delivery batch id: %w", err)
	}
	preparedTargets := make([]PreparedDeliveryTarget, len(targets))
	for i, target := range targets {
		commandID, err := newDeliveryCommandID()
		if err != nil {
			return FrozenDeliveryPlan{}, fmt.Errorf("edgecontrol: allocate delivery command id: %w", err)
		}
		preparedTargets[i] = PreparedDeliveryTarget{TargetInstanceID: target, CommandID: commandID}
	}
	plan = buildFrozenDeliveryPlan(f.instanceID, batchID, req, preparedTargets)
	return plan, validateFrozenDeliveryPlanEnvelope(plan)
}

// PrepareAccountDeliveryRoute freezes the account registry snapshot before
// PTS projection. It performs no payload work and never publishes.
func (f *DeliveryFabric) PrepareAccountDeliveryRoute(
	ctx context.Context,
	req AccountDeliveryRouteRequest,
) (FrozenAccountDeliveryRoute, error) {
	var route FrozenAccountDeliveryRoute
	if f == nil || f.registry == nil || f.bus == nil || f.executor == nil {
		return route, ErrDeliveryIndeterminate
	}
	if !ValidDeliveryInstanceID(f.instanceID) {
		return route, fmt.Errorf("edgecontrol: invalid delivery source instance identity")
	}
	if err := validateAccountDeliveryRouteRequest(req); err != nil {
		return route, err
	}
	if !time.Now().Before(req.NotAfter) {
		return route, fmt.Errorf("edgecontrol: delivery request deadline elapsed")
	}
	records, err := f.registry.ListUser(ctx, req.TargetUserID)
	if err != nil {
		return route, err
	}
	for _, record := range records {
		if record.UserID == req.TargetUserID && record.ReceivesUpdates && !ValidDeliveryInstanceID(record.InstanceID) {
			return route, fmt.Errorf("edgecontrol: invalid delivery target instance identity")
		}
	}
	// Egress is not an Edge. Its instance ID must never become a local target
	// exclusion even if deployment names happen to collide.
	targetIDs := remoteDeliveryTargets("", req.TargetUserID, req.ExcludeAuthKeyID, req.ExcludeSessionID, records)
	sort.Strings(targetIDs)
	if len(targetIDs) > MaxDeliveryTargets {
		return route, fmt.Errorf("edgecontrol: delivery target count %d exceeds %d", len(targetIDs), MaxDeliveryTargets)
	}
	for _, target := range targetIDs {
		if !ValidDeliveryInstanceID(target) {
			return route, fmt.Errorf("edgecontrol: invalid delivery target instance identity")
		}
	}
	batchID, err := newDeliveryBatchID()
	if err != nil {
		return route, fmt.Errorf("edgecontrol: allocate delivery batch id: %w", err)
	}
	targets := make([]PreparedDeliveryTarget, len(targetIDs))
	for i, target := range targetIDs {
		commandID, err := newDeliveryCommandID()
		if err != nil {
			return FrozenAccountDeliveryRoute{}, fmt.Errorf("edgecontrol: allocate delivery command id: %w", err)
		}
		targets[i] = PreparedDeliveryTarget{TargetInstanceID: target, CommandID: commandID}
	}
	route = FrozenAccountDeliveryRoute{
		batchID: batchID, sourceInstanceID: f.instanceID, targetUserID: req.TargetUserID,
		excludeAuthKeyID: req.ExcludeAuthKeyID, excludeSessionID: req.ExcludeSessionID,
		notAfter: req.NotAfter.UTC(), evidenceDeadline: req.EvidenceDeadline.UTC(), targets: targets,
	}
	return route, validateFrozenAccountDeliveryRoute(route)
}

func validateAccountDeliveryRouteRequest(req AccountDeliveryRouteRequest) error {
	if req.TargetUserID <= 0 || (req.ExcludeAuthKeyID != ([8]byte{})) != (req.ExcludeSessionID != 0) {
		return fmt.Errorf("edgecontrol: invalid account delivery route identity")
	}
	if req.NotAfter.IsZero() || req.NotAfter.UnixNano() <= 0 ||
		!req.EvidenceDeadline.After(req.NotAfter) ||
		req.EvidenceDeadline.After(time.Now().Add(MaxDeliveryNotAfterHorizon)) {
		return fmt.Errorf("edgecontrol: invalid account delivery route deadline")
	}
	return nil
}

func validateFrozenAccountDeliveryRoute(route FrozenAccountDeliveryRoute) error {
	if route.batchID.Empty() || !ValidDeliveryInstanceID(route.sourceInstanceID) || route.targetUserID <= 0 ||
		(route.excludeAuthKeyID != ([8]byte{})) != (route.excludeSessionID != 0) ||
		route.notAfter.IsZero() || route.notAfter.UnixNano() <= 0 ||
		!route.evidenceDeadline.After(route.notAfter) || len(route.targets) > MaxDeliveryTargets {
		return fmt.Errorf("edgecontrol: invalid frozen account delivery route")
	}
	seen := make(map[string]struct{}, len(route.targets))
	for _, target := range route.targets {
		if !ValidDeliveryInstanceID(target.TargetInstanceID) || target.CommandID.Empty() {
			return fmt.Errorf("edgecontrol: invalid frozen account delivery target")
		}
		if _, exists := seen[target.TargetInstanceID]; exists {
			return fmt.Errorf("edgecontrol: duplicate frozen account delivery target")
		}
		seen[target.TargetInstanceID] = struct{}{}
	}
	return nil
}

// AdmitPreparedDelivery publishes only an already-frozen plan. It does not
// consult the registry and therefore cannot race Egress's target-ledger bind.
func (f *DeliveryFabric) AdmitPreparedDelivery(ctx context.Context, plan FrozenDeliveryPlan) (FrozenDelivery, error) {
	result := FrozenDelivery{BatchID: plan.batchID}
	if f == nil || f.bus == nil || f.executor == nil {
		return result, ErrDeliveryIndeterminate
	}
	if err := validateFrozenDeliveryPlanEnvelope(plan); err != nil {
		return result, err
	}
	now := time.Now()
	if !now.Before(plan.evidenceDeadline) {
		return result, fmt.Errorf("edgecontrol: prepared delivery evidence deadline elapsed")
	}
	// Exact frozen-plan replay is legal only for the same stable Egress owner
	// while its lease remains live. Cross-owner/expired recovery must resolve the
	// old attempt and PrepareDelivery a fresh fence/batch/command set.
	if plan.sourceInstanceID != f.instanceID {
		return result, fmt.Errorf("edgecontrol: prepared delivery source mismatch")
	}
	admissionDeadline := plan.notAfter
	if !now.Before(plan.notAfter) {
		admissionDeadline = plan.evidenceDeadline
	}
	admitCtx, cancel := context.WithDeadline(ctx, admissionDeadline)
	defer cancel()
	result.Targets = make([]TargetAdmission, len(plan.targets))
	if len(plan.targets) == 0 {
		return result, nil
	}

	tasks := make([]fabricAdmissionTask, len(plan.targets))
	for i, target := range plan.targets {
		batch := DeliveryBatch{
			BatchID: plan.batchID, CommandID: target.CommandID,
			SourceInstanceID: plan.sourceInstanceID, TargetInstanceID: target.TargetInstanceID,
			TargetUserID: plan.targetUserID, ExcludeAuthKeyID: plan.excludeAuthKeyID,
			ExcludeSessionID: plan.excludeSessionID,
			NotAfter:         plan.notAfter,
			EvidenceDeadline: plan.evidenceDeadline,
			Items:            plan.items,
		}
		result.Targets[i] = TargetAdmission{TargetInstanceID: target.TargetInstanceID, CommandID: target.CommandID}
		tasks[i] = fabricAdmissionTask{
			ctx: admitCtx, target: target.TargetInstanceID, batch: batch,
			bytes: deliveryBatchPayloadBytes(batch), done: make(chan fabricAdmissionResult, 1),
		}
		if err := f.executor.submitContext(admitCtx, &tasks[i]); err != nil {
			result.Targets[i].Err = err
			detail := DetailCapacity
			if admitCtx.Err() != nil {
				detail = DetailDeadline
			}
			result.Targets[i].Admission = bindDeliveryAdmission(batch, DeliveryAdmission{
				Outcome: AdmissionOverloaded, Detail: detail,
			})
		}
	}

	indeterminate := false
	for i := range tasks {
		if result.Targets[i].Err != nil {
			indeterminate = true
			continue
		}
		select {
		case got := <-tasks[i].done:
			result.Targets[i].Admission = got.admission
			result.Targets[i].Err = got.err
			if got.err != nil || got.admission.Outcome == AdmissionOverloaded {
				indeterminate = true
			}
		case <-admitCtx.Done():
			result.Targets[i].Err = admitCtx.Err()
			indeterminate = true
		}
	}
	if indeterminate {
		return result, ErrDeliveryIndeterminate
	}
	return result, nil
}

func validateDeliveryRequestEnvelope(req DeliveryRequest) error {
	if req.TargetUserID < 0 || len(req.Items) == 0 || len(req.Items) > MaxDeliveryBatchItems ||
		req.NotAfter.IsZero() || req.NotAfter.UnixNano() <= 0 || !req.EvidenceDeadline.After(req.NotAfter) {
		return fmt.Errorf("edgecontrol: invalid delivery request")
	}
	if req.EvidenceDeadline.After(time.Now().Add(MaxDeliveryNotAfterHorizon)) {
		return fmt.Errorf("edgecontrol: delivery request deadline exceeds maximum horizon")
	}
	if (req.ExcludeAuthKeyID != ([8]byte{})) != (req.ExcludeSessionID != 0) {
		return fmt.Errorf("edgecontrol: invalid delivery exclusion pair")
	}
	domain := req.Items[0].Ref.Domain
	channel := domain.Kind == QueueChannelPTS
	if channel != (req.TargetUserID == 0) || (channel && (req.ExcludeAuthKeyID != ([8]byte{}) || req.ExcludeSessionID != 0)) {
		return fmt.Errorf("edgecontrol: invalid delivery request target")
	}
	seenRefs := make(map[DeliveryRef]struct{}, len(req.Items))
	seenItems := make(map[int64]struct{}, len(req.Items))
	totalBytes := 0
	for i, item := range req.Items {
		if !item.Ref.Valid() || item.Ref.TargetUserID != req.TargetUserID || item.Ref.Domain != domain ||
			(channel && !item.Channel.ValidFor(domain)) || (!channel && !item.Channel.Empty()) {
			return fmt.Errorf("edgecontrol: invalid delivery request ref at index %d", i)
		}
		if item.MessageType != proto.MessageFromServer || len(item.UpdateBytes) == 0 {
			return fmt.Errorf("edgecontrol: invalid delivery request payload at index %d", i)
		}
		if _, exists := seenRefs[item.Ref]; exists {
			return fmt.Errorf("edgecontrol: duplicate delivery request ref at index %d", i)
		}
		if _, exists := seenItems[item.Ref.OutboxID]; exists {
			return fmt.Errorf("edgecontrol: duplicate delivery request item at index %d", i)
		}
		seenRefs[item.Ref] = struct{}{}
		seenItems[item.Ref.OutboxID] = struct{}{}
		totalBytes += deliveryItemEnvelopeBytes(item)
		if totalBytes > MaxDeliveryBatchBytes {
			return fmt.Errorf("edgecontrol: delivery request payload and route metadata exceed batch byte limit")
		}
	}
	return nil
}

func validateDeliveryRequest(req DeliveryRequest) error {
	if err := validateDeliveryRequestEnvelope(req); err != nil {
		return err
	}
	for i, item := range req.Items {
		if item.PayloadHash != DeliveryPayloadHash(item.UpdateBytes) {
			return fmt.Errorf("edgecontrol: invalid delivery request payload hash at index %d", i)
		}
	}
	return nil
}

func validateFrozenDeliveryPlan(plan FrozenDeliveryPlan) error {
	if err := validateFrozenDeliveryPlanEnvelope(plan); err != nil {
		return err
	}
	for i, item := range plan.items {
		if item.PayloadHash != DeliveryPayloadHash(item.UpdateBytes) {
			return fmt.Errorf("edgecontrol: invalid frozen delivery payload hash at index %d", i)
		}
	}
	return nil
}

func validateFrozenDeliveryPlanEnvelope(plan FrozenDeliveryPlan) error {
	if plan.batchID.Empty() || plan.sourceInstanceID == "" || plan.targetUserID < 0 ||
		len(plan.items) == 0 || plan.notAfter.IsZero() || !plan.evidenceDeadline.After(plan.notAfter) {
		return fmt.Errorf("edgecontrol: invalid frozen delivery plan")
	}
	if !ValidDeliveryInstanceID(plan.sourceInstanceID) || len(plan.targets) > MaxDeliveryTargets {
		return fmt.Errorf("edgecontrol: frozen delivery plan exceeds identity limits")
	}
	request := DeliveryRequest{
		TargetUserID: plan.targetUserID, ExcludeAuthKeyID: plan.excludeAuthKeyID,
		ExcludeSessionID: plan.excludeSessionID,
		NotAfter:         plan.notAfter,
		EvidenceDeadline: plan.evidenceDeadline,
		Items:            plan.items,
	}
	if err := validateDeliveryRequestEnvelope(request); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(plan.targets))
	for i, target := range plan.targets {
		if !ValidDeliveryInstanceID(target.TargetInstanceID) || target.CommandID.Empty() {
			return fmt.Errorf("edgecontrol: invalid prepared target at index %d", i)
		}
		if _, ok := seen[target.TargetInstanceID]; ok {
			return fmt.Errorf("edgecontrol: duplicate prepared target %q", target.TargetInstanceID)
		}
		seen[target.TargetInstanceID] = struct{}{}
	}
	return nil
}

func (f *DeliveryFabric) sendAdmission(ctx context.Context, target string, batch DeliveryBatch) (DeliveryAdmission, error) {
	timeout := f.commandTimeout
	if timeout <= 0 {
		timeout = defaultDeliveryCommandTimeout
	}
	if timeout < minDeliveryCommandTimeout {
		timeout = minDeliveryCommandTimeout
	}
	now := time.Now()
	deadline := now.Add(timeout)
	capDeadline := batch.NotAfter
	if !now.Before(batch.NotAfter) {
		capDeadline = batch.EvidenceDeadline
	}
	if capDeadline.Before(deadline) {
		deadline = capDeadline
	}
	if !time.Now().Before(deadline) {
		return bindDeliveryAdmission(batch, DeliveryAdmission{}), fmt.Errorf("edgecontrol: delivery deadline elapsed before admission")
	}
	sendCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	admission, err := f.bus.SendDeliveryBatch(sendCtx, target, batch)
	if err != nil {
		return bindDeliveryAdmission(batch, DeliveryAdmission{}), err
	}
	if admission.BatchID != batch.BatchID || admission.CommandID != batch.CommandID ||
		admission.SourceInstanceID != batch.SourceInstanceID || admission.TargetInstanceID != target {
		return bindDeliveryAdmission(batch, DeliveryAdmission{}), fmt.Errorf("edgecontrol: delivery admission identity mismatch")
	}
	if !admission.Valid() {
		return bindDeliveryAdmission(batch, DeliveryAdmission{}), fmt.Errorf("edgecontrol: invalid delivery admission")
	}
	return admission, nil
}

type fabricAdmissionTask struct {
	ctx    context.Context
	target string
	batch  DeliveryBatch
	bytes  int
	done   chan fabricAdmissionResult
}

type fabricAdmissionResult struct {
	admission DeliveryAdmission
	err       error
}

type fabricAdmissionShard struct {
	queue chan *fabricAdmissionTask
	stop  <-chan struct{}
	owner *fabricAdmissionExecutor
}

type fabricAdmissionExecutor struct {
	fabric    *DeliveryFabric
	shards    []fabricAdmissionShard
	maxBytes  int64
	used      atomic.Int64
	next      atomic.Uint64
	available chan struct{}
	stop      chan struct{}
	done      sync.WaitGroup
	closeOne  sync.Once
}

func newFabricAdmissionExecutor(f *DeliveryFabric, shards, mailbox int, maxBytes int64) *fabricAdmissionExecutor {
	if shards <= 0 {
		shards = defaultFabricExecutorShards
	}
	shards = nextPowerOfTwo(shards)
	if mailbox <= 0 {
		mailbox = defaultFabricMailboxSize
	}
	if maxBytes <= 0 {
		maxBytes = defaultFabricMailboxBytes
	}
	e := &fabricAdmissionExecutor{
		fabric: f, maxBytes: maxBytes, available: make(chan struct{}, 1), stop: make(chan struct{}),
	}
	e.shards = make([]fabricAdmissionShard, shards)
	for i := range e.shards {
		e.shards[i] = fabricAdmissionShard{queue: make(chan *fabricAdmissionTask, mailbox), stop: e.stop, owner: e}
		e.done.Add(1)
		go e.shards[i].run()
	}
	return e
}

func (e *fabricAdmissionExecutor) submit(task *fabricAdmissionTask) error {
	if e == nil || task == nil || task.bytes <= 0 || !e.reserve(task.bytes) {
		return ErrDeliveryOverloaded
	}
	// Redis admission is external I/O. Ordering is already owned by the Egress
	// domain actor, so serializing every domain headed to one slow Edge creates
	// pure head-of-line blocking. Fixed workers are selected round-robin.
	shard := &e.shards[(e.next.Add(1)-1)&uint64(len(e.shards)-1)]
	select {
	case shard.queue <- task:
		return nil
	case <-e.stop:
		e.rollback(task.bytes)
		return ErrDeliveryIndeterminate
	default:
		e.rollback(task.bytes)
		return ErrDeliveryOverloaded
	}
}

// submitContext applies bounded backpressure to the fixed admission pool. A
// large multi-Edge plan is admitted in waves as earlier Redis I/O releases
// bytes/mailbox slots; targets are not declared overloaded merely because all
// were frozen at once. No goroutine or timer is created per target.
func (e *fabricAdmissionExecutor) submitContext(ctx context.Context, task *fabricAdmissionTask) error {
	if e == nil || task == nil || task.bytes <= 0 || int64(task.bytes) > e.maxBytes {
		return ErrDeliveryOverloaded
	}
	for {
		err := e.submit(task)
		if err != ErrDeliveryOverloaded {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-e.stop:
			return ErrDeliveryIndeterminate
		case <-e.available:
		}
	}
}

func (e *fabricAdmissionExecutor) reserve(bytes int) bool {
	if bytes <= 0 || int64(bytes) > e.maxBytes {
		return false
	}
	for {
		used := e.used.Load()
		if used > e.maxBytes-int64(bytes) {
			return false
		}
		if e.used.CompareAndSwap(used, used+int64(bytes)) {
			return true
		}
	}
}

func (e *fabricAdmissionExecutor) rollback(bytes int) { e.used.Add(-int64(bytes)) }

func (e *fabricAdmissionExecutor) release(bytes int) {
	e.used.Add(-int64(bytes))
	e.notifyAvailable()
}

func (e *fabricAdmissionExecutor) notifyAvailable() {
	select {
	case e.available <- struct{}{}:
	default:
	}
}

func (e *fabricAdmissionExecutor) close() {
	if e == nil {
		return
	}
	e.closeOne.Do(func() { close(e.stop) })
	e.done.Wait()
}

func (s *fabricAdmissionShard) run() {
	defer s.owner.done.Done()
	for {
		select {
		case <-s.stop:
			for {
				select {
				case task := <-s.queue:
					s.owner.release(task.bytes)
					task.done <- fabricAdmissionResult{admission: bindDeliveryAdmission(task.batch, DeliveryAdmission{}), err: ErrDeliveryIndeterminate}
				default:
					return
				}
			}
		case task := <-s.queue:
			s.owner.notifyAvailable()
			admission, err := s.owner.fabric.sendAdmission(task.ctx, task.target, task.batch)
			s.owner.release(task.bytes)
			task.done <- fabricAdmissionResult{admission: admission, err: err}
		}
	}
}

func bindDeliveryAdmission(batch DeliveryBatch, admission DeliveryAdmission) DeliveryAdmission {
	admission.BatchID = batch.BatchID
	admission.CommandID = batch.CommandID
	admission.SourceInstanceID = batch.SourceInstanceID
	admission.TargetInstanceID = batch.TargetInstanceID
	return admission
}

func deliveryBatchPayloadBytes(batch DeliveryBatch) int {
	bytes := 256 + len(batch.Items)*96
	for _, item := range batch.Items {
		bytes += deliveryItemEnvelopeBytes(item)
	}
	return bytes
}

func ValidDeliveryInstanceID(value string) bool {
	return value != "" && len(value) <= MaxDeliveryInstanceIDBytes && strings.TrimSpace(value) == value
}

func canonicalDeliveryInstanceIDs(values []string) ([]string, error) {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if !ValidDeliveryInstanceID(value) {
			return nil, fmt.Errorf("edgecontrol: invalid delivery target instance identity")
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out, nil
}

func newDeliveryCommandID() (CommandID, error) {
	var id CommandID
	_, err := rand.Read(id[:])
	return id, err
}

func newDeliveryBatchID() (BatchID, error) {
	var id BatchID
	_, err := rand.Read(id[:])
	return id, err
}

func remoteDeliveryTargets(localInstanceID string, targetUserID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, records []LocationRecord) []string {
	seen := make(map[string]struct{})
	out := make([]string, 0, len(records))
	for _, record := range records {
		if record.UserID != targetUserID || !record.ReceivesUpdates || record.InstanceID == "" || (localInstanceID != "" && record.InstanceID == localInstanceID) {
			continue
		}
		if excludeSessionID != 0 && record.RawAuthKeyID == excludeAuthKeyID && record.SessionID == excludeSessionID {
			continue
		}
		if _, ok := seen[record.InstanceID]; ok {
			continue
		}
		seen[record.InstanceID] = struct{}{}
		out = append(out, record.InstanceID)
	}
	return out
}

func RunDeliveryBatchSubscriber(ctx context.Context, bus DeliveryCommandBus, instanceID string, local DeliveryBatchAcceptor) {
	if bus == nil || instanceID == "" || local == nil {
		return
	}
	for {
		err := bus.SubscribeDeliveryBatches(ctx, instanceID, func(ctx context.Context, batch DeliveryBatch) DeliveryAdmission {
			return bindDeliveryAdmission(batch, local.AdmitDeliveryBatch(ctx, batch))
		})
		if ctx.Err() != nil {
			return
		}
		_ = err
		timer := time.NewTimer(deliverySubscribeRetry)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-timer.C:
		}
	}
}

func nextPowerOfTwo(value int) int {
	if value <= 1 {
		return 1
	}
	value--
	for shift := 1; shift < 32; shift <<= 1 {
		value |= value >> shift
	}
	return value + 1
}

func hashInstance(value string) uint64 {
	const prime = uint64(1099511628211)
	h := uint64(1469598103934665603)
	for i := 0; i < len(value); i++ {
		h ^= uint64(value[i])
		h *= prime
	}
	return h
}
