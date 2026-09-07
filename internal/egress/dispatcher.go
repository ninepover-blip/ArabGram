package egress

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/iamxvbaba/td/proto"
	"go.uber.org/zap"

	"telesrv/internal/domain"
	"telesrv/internal/edgecontrol"
	"telesrv/internal/store"
)

const (
	defaultOutboxBatch              = 64
	defaultOutboxWorkers            = 4
	defaultOutboxWindowSize         = 32
	defaultOutboxWindowBytes        = 2 << 20
	maxOnlineDeliveryAttempts       = 5
	outboxLogicalShards             = store.DispatchOutboxLogicalShards
	minimumOutboxRetryDelay         = 4 * time.Millisecond
	maximumOutboxRetryDelay         = time.Second
	projectedItemRetainedBytes      = int64(256)
	deliveryPlanItemBytes           = int64(384) // request item plus immutable plan item
	deliveryPlanTargetBytes         = int64(128 + edgecontrol.MaxDeliveryInstanceIDBytes)
	attemptTargetLedgerBytes        = int64(64)
	replayTargetIndexBytes          = int64(256)
	replayBatchIndexBytes           = int64(128)
	maxProjectedWindowRetainedBytes = int64(edgecontrol.MaxDeliveryBatchBytes) +
		int64(edgecontrol.MaxDeliveryBatchItems)*projectedItemRetainedBytes
	maxDeliveryPlanRetainedBytes = int64(edgecontrol.MaxDeliveryBatchBytes) +
		int64(edgecontrol.MaxDeliveryBatchItems)*deliveryPlanItemBytes +
		int64(edgecontrol.MaxDeliveryTargets)*deliveryPlanTargetBytes
	maxAttemptTargetLedgerRetainedBytes = int64(edgecontrol.MaxDeliveryBatchItems) *
		int64(edgecontrol.MaxDeliveryTargets) * attemptTargetLedgerBytes
	maxReplayIndexRetainedBytes = int64(edgecontrol.MaxDeliveryTargets)*replayTargetIndexBytes +
		int64(edgecontrol.MaxDeliveryTargets)*replayBatchIndexBytes
)

var (
	errMissingOutboxEvent         = errors.New("egress: missing durable update event")
	errOutboxUpdateBuilderMissing = errors.New("egress: update builder is required")
	errOutboxUpdateBuilderCount   = errors.New("egress: update builder returned mismatched count")
	errOutboxUpdateBuilderEmpty   = errors.New("egress: update builder returned empty non-noop payload")
	ErrInvalidOutboxExclusionPair = errors.New("egress: exclusion requires both raw auth key and session id")
	errProjectedDeliveryTooLarge  = errors.New("egress: projected delivery exceeds v3 batch limits")
	errActorCapacity              = errors.New("egress: domain actor mailbox capacity exhausted")
)

type boundDeliveryPendingError struct{ cause error }

func (e *boundDeliveryPendingError) Error() string { return e.cause.Error() }
func (e *boundDeliveryPendingError) Unwrap() error { return e.cause }

func markBoundDeliveryPending(err error) error {
	if err == nil {
		return nil
	}
	return &boundDeliveryPendingError{cause: err}
}

type Metrics interface {
	OutboxClaimed(count int)
	OutboxDelivered(time.Duration)
	OutboxFailed(error)
	OutboxStage(queue, stage string, duration time.Duration, items int)
}

type nopMetrics struct{}

func (nopMetrics) OutboxClaimed(int)                              {}
func (nopMetrics) OutboxDelivered(time.Duration)                  {}
func (nopMetrics) OutboxFailed(error)                             {}
func (nopMetrics) OutboxStage(string, string, time.Duration, int) {}

type OutboxUpdateRequest struct {
	TargetUserID int64
	Event        domain.UpdateEvent
}

type OutboxUpdateBuilder func(context.Context, []OutboxUpdateRequest) ([][]byte, error)

type ChannelUpdateRequest struct {
	ChannelID int64
	PTS       int
}

type ChannelUpdateBuilder func(context.Context, []ChannelUpdateRequest) ([][]byte, error)

type deliveryCoordinator struct {
	dispatch               store.DispatchOutboxStore
	absolute               store.DeliveryOutboxStore
	channel                store.ChannelDeliveryStore
	mutations              AttemptMutationWriter
	planner                edgecontrol.DeliveryPlanner
	ptsProjector           ptsWindowProjector
	readModelInvalidations store.ReadModelInvalidationPublisher
	channelBuilder         ChannelUpdateBuilder
	metrics                Metrics
	log                    *zap.Logger
	instanceID             string
}

type projectedOutboxItem struct {
	claimed      store.OutboxClaimedItem
	payload      []byte
	targetUsers  []int64
	channelRoute edgecontrol.ChannelDeliveryRoute
}

func (c *deliveryCoordinator) stateStore(kind store.OutboxQueueKind) store.DurableOutboxStateStore {
	switch kind {
	case store.OutboxQueueDispatchPTS:
		return c.dispatch
	case store.OutboxQueueAbsoluteDelivery:
		return c.absolute
	case store.OutboxQueueChannelPTS:
		return c.channel
	default:
		return nil
	}
}

func (c *deliveryCoordinator) processWindow(ctx context.Context, window store.OutboxClaimWindow) {
	c.processWindowBudgeted(ctx, window, nil)
}

func (c *deliveryCoordinator) processWindowBudgeted(
	ctx context.Context,
	window store.OutboxClaimWindow,
	reservation *deliveryActorReservation,
) {
	stateStore := c.stateStore(window.QueueKind)
	if stateStore == nil || len(window.Items) == 0 {
		return
	}
	totalStarted := time.Now()
	defer func() { c.observeStage(window.QueueKind, "window", totalStarted, len(window.Items)) }()
	if err := validateClaimWindow(window); err != nil {
		c.resolveItems(ctx, stateStore, window.Owner, window.Items, err, true)
		return
	}
	notAfter, evidenceDeadline, err := claimDeadlines(window)
	if err != nil {
		c.resolveItems(ctx, stateStore, window.Owner, window.Items, err, true)
		return
	}
	if !time.Now().Before(notAfter) {
		// recover_after is later than this wire fence by the configured skew
		// allowance. The database sweep owns retry so old and new fences cannot
		// overlap across hosts.
		return
	}
	recoveryCtx, cancelRecovery := context.WithDeadline(ctx, evidenceDeadline)
	defer cancelRecovery()
	deliveryCtx, cancelDelivery := context.WithDeadline(recoveryCtx, notAfter)
	defer cancelDelivery()
	if window.QueueKind == store.OutboxQueueDispatchPTS {
		invalidationStarted := time.Now()
		if err := c.publishDispatchReadModelInvalidations(deliveryCtx, window); err != nil {
			c.observeStage(window.QueueKind, "read_model_invalidation", invalidationStarted, len(window.Items))
			c.log.Warn("durable dialog read-model invalidation remains pending", zap.Error(err))
			return
		}
		c.observeStage(window.QueueKind, "read_model_invalidation", invalidationStarted, len(window.Items))
	}
	var accountRoute *edgecontrol.FrozenAccountDeliveryRoute
	if window.QueueKind == store.OutboxQueueDispatchPTS && window.Items[0].SourceInstanceID == "" {
		routeRequest, err := accountDeliveryRouteRequest(window, notAfter, evidenceDeadline)
		if err != nil {
			c.resolveItems(deliveryCtx, stateStore, window.Owner, window.Items, err, true)
			return
		}
		routeStarted := time.Now()
		route, err := c.planner.PrepareAccountDeliveryRoute(deliveryCtx, routeRequest)
		c.observeStage(window.QueueKind, "route", routeStarted, len(window.Items))
		if err != nil {
			if deliveryCtx.Err() == nil {
				c.resolveItems(deliveryCtx, stateStore, window.Owner, window.Items, err, false)
			}
			return
		}
		if route.SourceInstanceID() != c.instanceID || route.TargetUserID() != window.StreamID {
			c.resolveItems(deliveryCtx, stateStore, window.Owner, window.Items, errors.New("egress: frozen account route identity mismatch"), true)
			return
		}
		if len(route.Targets()) == 0 {
			bindStarted := time.Now()
			if err := c.bindClaimedAuthoritativeEmpty(deliveryCtx, window.Items, route.SourceInstanceID()); err != nil {
				if deliveryCtx.Err() == nil {
					c.resolveItems(deliveryCtx, stateStore, window.Owner, window.Items, err, false)
				}
			}
			c.observeStage(window.QueueKind, "bind_empty", bindStarted, len(window.Items))
			return
		}
		accountRoute = &route
	}
	projectionBytes := int64(0)
	if reservation != nil && window.QueueKind != store.OutboxQueueAbsoluteDelivery {
		if !reservation.retain(maxProjectedWindowRetainedBytes) {
			// Claim already froze a durable evidence deadline. Do not amplify local
			// memory pressure into an immediate retry+finalize write storm; the
			// deadline sweep will fence and retry this exact unbound attempt.
			c.log.Debug("defer durable delivery for actor byte capacity",
				zap.Uint8("queue_kind", uint8(window.QueueKind)), zap.Int64("stream_id", window.StreamID))
			return
		}
		projectionBytes = maxProjectedWindowRetainedBytes
		defer func() {
			if projectionBytes > 0 {
				reservation.releaseRetained(projectionBytes)
			}
		}()
	}
	projectionStarted := time.Now()
	projected, err := c.projectWindow(deliveryCtx, window)
	c.observeStage(window.QueueKind, "projection", projectionStarted, len(window.Items))
	if err != nil {
		if deliveryCtx.Err() == nil {
			c.resolveItems(deliveryCtx, stateStore, window.Owner, window.Items, err, projectionErrorIsTerminal(err))
		}
		return
	}
	if len(projected) == 0 {
		c.resolveItems(deliveryCtx, stateStore, window.Owner, window.Items, errors.New("egress: empty projected window"), true)
		return
	}
	if projectionBytes > 0 {
		retained := projectedWindowRetainedBytes(projected)
		if retained < projectionBytes {
			reservation.releaseRetained(projectionBytes - retained)
			projectionBytes = retained
		}
	}

	// ClaimWindows is required to freeze one homogeneous physical command.
	// Splitting a malformed window here would burn attempt numbers on an
	// unsent suffix and hide a durable-store invariant violation.
	if err := validateProjectedWindowHomogeneous(projected); err != nil {
		c.resolveItems(deliveryCtx, stateStore, window.Owner, window.Items, err, true)
		return
	}
	if projected[0].claimed.SourceInstanceID != "" {
		if err := c.replayBoundItems(deliveryCtx, recoveryCtx, projected, notAfter, evidenceDeadline, reservation); err != nil {
			c.log.Warn("replay bound durable delivery remains pending", zap.Error(err), zap.Time("not_after", notAfter))
		}
		return
	}
	bindAdmitStarted := time.Now()
	if err := c.freezeBindAndAdmit(deliveryCtx, recoveryCtx, projected, projected[0].targetUsers, notAfter, evidenceDeadline, reservation, accountRoute); err != nil {
		c.observeStage(window.QueueKind, "bind_admit", bindAdmitStarted, len(projected))
		var pending *boundDeliveryPendingError
		if errors.As(err, &pending) || errors.Is(err, errActorCapacity) || deliveryCtx.Err() != nil {
			c.log.Warn("bound durable delivery remains pending", zap.Error(err), zap.Time("not_after", notAfter))
			return
		}
		c.resolveItems(deliveryCtx, stateStore, window.Owner, claimedItems(projected), err, false)
		return
	}
	c.observeStage(window.QueueKind, "bind_admit", bindAdmitStarted, len(projected))
}

func (c *deliveryCoordinator) publishDispatchReadModelInvalidations(ctx context.Context, window store.OutboxClaimWindow) error {
	if c == nil || window.QueueKind != store.OutboxQueueDispatchPTS {
		return nil
	}
	latest := make(map[store.ReadModelKey]int64)
	for _, item := range window.Items {
		payload, ok := item.Payload.(store.DispatchOutboxPayload)
		if !ok || payload.ReadModelPeer.ID <= 0 {
			continue
		}
		key := store.ReadModelKey{
			Model: "dialog_light", OwnerUserID: window.StreamID,
			PeerType: payload.ReadModelPeer.Type, PeerID: payload.ReadModelPeer.ID,
		}
		if item.Ref.Sequence > latest[key] {
			latest[key] = item.Ref.Sequence
		}
	}
	if len(latest) == 0 {
		return nil
	}
	if c.readModelInvalidations == nil {
		return errors.New("egress: Redis read-model invalidation publisher is required")
	}
	items := make([]store.ReadModelInvalidation, 0, len(latest))
	for key, sequence := range latest {
		items = append(items, store.NewSequencedReadModelInvalidation(key, sequence))
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].Key.OwnerUserID != items[j].Key.OwnerUserID {
			return items[i].Key.OwnerUserID < items[j].Key.OwnerUserID
		}
		if items[i].Key.PeerType != items[j].Key.PeerType {
			return items[i].Key.PeerType < items[j].Key.PeerType
		}
		return items[i].Key.PeerID < items[j].Key.PeerID
	})
	return c.readModelInvalidations.PublishReadModelInvalidations(ctx, items)
}

func (c *deliveryCoordinator) observeStage(kind store.OutboxQueueKind, stage string, started time.Time, items int) {
	if c == nil || c.metrics == nil || started.IsZero() {
		return
	}
	c.metrics.OutboxStage(outboxQueueMetric(kind), stage, time.Since(started), items)
}

func outboxQueueMetric(kind store.OutboxQueueKind) string {
	switch kind {
	case store.OutboxQueueDispatchPTS:
		return "account_pts"
	case store.OutboxQueueAbsoluteDelivery:
		return "account_absolute"
	case store.OutboxQueueChannelPTS:
		return "channel_pts"
	default:
		return "invalid"
	}
}

func accountDeliveryRouteRequest(window store.OutboxClaimWindow, notAfter, evidenceDeadline time.Time) (edgecontrol.AccountDeliveryRouteRequest, error) {
	if window.QueueKind != store.OutboxQueueDispatchPTS || window.StreamID <= 0 || len(window.Items) == 0 ||
		notAfter.IsZero() || !evidenceDeadline.After(notAfter) {
		return edgecontrol.AccountDeliveryRouteRequest{}, errors.New("egress: invalid account route window")
	}
	firstAuth := window.Items[0].ExcludeAuthKeyID
	firstSession := window.Items[0].ExcludeSessionID
	for i := 1; i < len(window.Items); i++ {
		if window.Items[i].ExcludeAuthKeyID != firstAuth || window.Items[i].ExcludeSessionID != firstSession {
			return edgecontrol.AccountDeliveryRouteRequest{}, fmt.Errorf("egress: non-homogeneous account route at item %d", window.Items[i].Ref.ItemID)
		}
	}
	return edgecontrol.AccountDeliveryRouteRequest{
		TargetUserID: window.StreamID, ExcludeAuthKeyID: firstAuth,
		ExcludeSessionID: firstSession, NotAfter: notAfter, EvidenceDeadline: evidenceDeadline,
	}, nil
}

func claimDeadlines(window store.OutboxClaimWindow) (time.Time, time.Time, error) {
	if window.LeaseUntil.IsZero() || len(window.Items) == 0 {
		return time.Time{}, time.Time{}, errors.New("egress: missing persisted command deadline")
	}
	deadline := window.Items[0].CommandNotAfter.UTC()
	if deadline.IsZero() || !deadline.Before(window.LeaseUntil) {
		return time.Time{}, time.Time{}, errors.New("egress: invalid persisted command deadline")
	}
	evidenceDeadline := window.Items[0].EvidenceDeadline.UTC()
	if !evidenceDeadline.After(deadline) || !evidenceDeadline.Before(window.LeaseUntil) {
		return time.Time{}, time.Time{}, errors.New("egress: invalid persisted evidence deadline")
	}
	for _, item := range window.Items {
		if !item.CommandNotAfter.Equal(deadline) || !item.EvidenceDeadline.Equal(evidenceDeadline) {
			return time.Time{}, time.Time{}, errors.New("egress: inconsistent persisted command/evidence deadline")
		}
	}
	return deadline, evidenceDeadline, nil
}

func validateClaimWindow(window store.OutboxClaimWindow) error {
	if window.QueueKind == 0 || window.StreamID <= 0 || window.LeaseFence == 0 || window.Owner == "" || len(window.Items) == 0 {
		return errors.New("egress: invalid claim window")
	}
	boundSource := window.Items[0].SourceInstanceID
	for i, item := range window.Items {
		if item.Ref.QueueKind != window.QueueKind || item.Ref.StreamID != window.StreamID || item.Ref.LeaseFence != window.LeaseFence || item.Ref.ItemID <= 0 || item.Ref.Attempt <= 0 {
			return fmt.Errorf("egress: invalid claim item at index %d", i)
		}
		if item.SourceInstanceID != boundSource || strings.TrimSpace(item.SourceInstanceID) != item.SourceInstanceID ||
			len(item.SourceInstanceID) > edgecontrol.MaxDeliveryInstanceIDBytes || len(item.Targets) > edgecontrol.MaxDeliveryTargets {
			return fmt.Errorf("egress: claimed target ledger source/topology exceeds v3 limits at item %d", item.Ref.ItemID)
		}
		if item.SourceInstanceID == "" && len(item.Targets) != 0 {
			return fmt.Errorf("egress: unbound claim contains target commands at item %d", item.Ref.ItemID)
		}
		for _, target := range item.Targets {
			if target.TargetInstanceID == "" || strings.TrimSpace(target.TargetInstanceID) != target.TargetInstanceID ||
				len(target.TargetInstanceID) > edgecontrol.MaxDeliveryInstanceIDBytes ||
				target.BatchID == ([16]byte{}) || target.CommandID == ([16]byte{}) {
				return fmt.Errorf("egress: invalid claimed target identity at item %d", item.Ref.ItemID)
			}
			switch window.QueueKind {
			case store.OutboxQueueDispatchPTS, store.OutboxQueueAbsoluteDelivery:
				if target.TargetUserID != window.StreamID {
					return fmt.Errorf("egress: claimed account target owns another ordering domain at item %d", item.Ref.ItemID)
				}
			case store.OutboxQueueChannelPTS:
				if target.TargetUserID != 0 {
					return fmt.Errorf("egress: claimed channel target contains an account lane at item %d", item.Ref.ItemID)
				}
			}
		}
		if (item.ExcludeAuthKeyID != ([8]byte{})) != (item.ExcludeSessionID != 0) {
			return fmt.Errorf("%w at item %d", ErrInvalidOutboxExclusionPair, item.Ref.ItemID)
		}
	}
	return nil
}

func (c *deliveryCoordinator) projectWindow(ctx context.Context, window store.OutboxClaimWindow) ([]projectedOutboxItem, error) {
	switch window.QueueKind {
	case store.OutboxQueueDispatchPTS:
		return c.projectPTSWindow(ctx, window)
	case store.OutboxQueueAbsoluteDelivery:
		return projectAbsoluteWindow(window)
	case store.OutboxQueueChannelPTS:
		return c.projectChannelWindow(ctx, window)
	default:
		return nil, fmt.Errorf("egress: unsupported queue kind %d", window.QueueKind)
	}
}

func (c *deliveryCoordinator) projectPTSWindow(ctx context.Context, window store.OutboxClaimWindow) ([]projectedOutboxItem, error) {
	if c.ptsProjector == nil {
		return nil, errOutboxUpdateBuilderMissing
	}
	return c.ptsProjector.Project(ctx, window)
}

func projectAbsoluteWindow(window store.OutboxClaimWindow) ([]projectedOutboxItem, error) {
	out := make([]projectedOutboxItem, len(window.Items))
	for i, item := range window.Items {
		if item.Ref.Sequence != 0 || item.RecoveryPolicy != store.OutboxRecoveryAbsoluteReload {
			return nil, fmt.Errorf("egress: invalid absolute delivery identity item=%d", item.Ref.ItemID)
		}
		payload, ok := item.Payload.(store.AbsoluteDeliveryPayload)
		if !ok || len(payload.TL) == 0 {
			return nil, fmt.Errorf("egress: empty absolute delivery item=%d", item.Ref.ItemID)
		}
		out[i] = projectedOutboxItem{claimed: item, payload: payload.TL, targetUsers: []int64{window.StreamID}}
	}
	return out, nil
}

func validateProjectedWindowHomogeneous(items []projectedOutboxItem) error {
	if len(items) == 0 || len(items) > edgecontrol.MaxDeliveryBatchItems {
		return errors.New("egress: empty projected window")
	}
	totalEnvelopeBytes := 0
	firstAuth := items[0].claimed.ExcludeAuthKeyID
	firstSession := items[0].claimed.ExcludeSessionID
	firstHasPayload := len(items[0].payload) > 0
	for i := 1; i < len(items); i++ {
		item := items[i]
		if item.claimed.ExcludeAuthKeyID != firstAuth || item.claimed.ExcludeSessionID != firstSession ||
			(len(item.payload) > 0) != firstHasPayload || !sameInt64s(item.targetUsers, items[0].targetUsers) {
			return fmt.Errorf("egress: non-homogeneous claimed window at index %d", i)
		}
	}
	for _, item := range items {
		totalEnvelopeBytes += len(item.payload) +
			8*(len(item.channelRoute.AudienceUsers)+len(item.channelRoute.AffectedUsers))
		if totalEnvelopeBytes > edgecontrol.MaxDeliveryBatchBytes {
			return errProjectedDeliveryTooLarge
		}
	}
	return nil
}

func projectedWindowRetainedBytes(items []projectedOutboxItem) int64 {
	bytes := projectedItemRetainedBytes * int64(len(items))
	for _, item := range items {
		bytes += int64(len(item.payload) + 8*(len(item.targetUsers)+len(item.channelRoute.AudienceUsers)+len(item.channelRoute.AffectedUsers)))
	}
	return bytes
}

func deliveryPlanRetainedBytes(items []projectedOutboxItem, targetCapacity int) int64 {
	bytes := deliveryPlanItemBytes * int64(len(items))
	for _, item := range items {
		bytes += int64(len(item.payload) + 8*(len(item.channelRoute.AudienceUsers)+len(item.channelRoute.AffectedUsers)))
	}
	if targetCapacity > edgecontrol.MaxDeliveryTargets {
		targetCapacity = edgecontrol.MaxDeliveryTargets
	}
	if targetCapacity > 0 {
		bytes += int64(targetCapacity) * deliveryPlanTargetBytes
	}
	return bytes
}

func (c *deliveryCoordinator) freezeBindAndAdmit(
	ctx context.Context,
	recoveryCtx context.Context,
	items []projectedOutboxItem,
	targetUsers []int64,
	notAfter time.Time,
	evidenceDeadline time.Time,
	reservation *deliveryActorReservation,
	accountRoute *edgecontrol.FrozenAccountDeliveryRoute,
) error {
	if c.planner == nil || c.instanceID == "" {
		return ErrMissingDependency
	}
	if len(items) == 0 {
		return errors.New("egress: empty delivery group")
	}
	if len(items[0].payload) == 0 {
		return c.bindAuthoritativeEmpty(ctx, items, c.instanceID)
	}
	targetUsers = uniquePositiveIDs(targetUsers)
	type userPlan struct {
		targetUserID int64
		plan         edgecontrol.FrozenDeliveryPlan
		targets      []edgecontrol.PreparedDeliveryTarget
	}
	plans := make([]userPlan, 0, len(targetUsers))
	targetCapacity := edgecontrol.MaxDeliveryTargets
	if accountRoute != nil {
		targetCapacity = len(accountRoute.Targets())
	}
	planBytes := deliveryPlanRetainedBytes(items, targetCapacity)
	retainedPlanBytes := int64(0)
	retainedLedgerBytes := int64(0)
	defer func() {
		if reservation != nil {
			if retainedLedgerBytes > 0 {
				reservation.releaseRetained(retainedLedgerBytes)
			}
			if retainedPlanBytes > 0 {
				reservation.releaseRetained(retainedPlanBytes)
			}
		}
	}()
	if items[0].claimed.Ref.QueueKind == store.OutboxQueueChannelPTS {
		targetUsers = []int64{0}
	}
	for _, targetUserID := range targetUsers {
		if reservation != nil && planBytes > 0 {
			if !reservation.retain(planBytes) {
				return errorsNewActorCapacity()
			}
			retainedPlanBytes += planBytes
		}
		request, err := c.deliveryRequest(items, targetUserID, notAfter, evidenceDeadline)
		if err != nil {
			return err
		}
		var plan edgecontrol.FrozenDeliveryPlan
		if accountRoute != nil {
			if len(targetUsers) != 1 || targetUserID != accountRoute.TargetUserID() {
				return errors.New("egress: frozen account route target mismatch")
			}
			plan, err = accountRoute.BindDelivery(request)
		} else {
			plan, err = c.planner.PrepareDelivery(ctx, request)
		}
		if err != nil {
			return fmt.Errorf("egress: freeze delivery target user %d: %w", targetUserID, err)
		}
		plans = append(plans, userPlan{targetUserID: targetUserID, plan: plan, targets: plan.Targets()})
	}
	targetCount := 0
	for _, prepared := range plans {
		targetCount += len(prepared.targets)
	}
	ledgerBytes := int64(len(items))*int64(targetCount)*attemptTargetLedgerBytes + int64(len(items))*128
	if reservation != nil && ledgerBytes > 0 {
		if !reservation.retain(ledgerBytes) {
			return errorsNewActorCapacity()
		}
		retainedLedgerBytes = ledgerBytes
	}
	sets := make([]store.OutboxAttemptTargetSet, len(items))
	for i, item := range items {
		sets[i] = store.OutboxAttemptTargetSet{
			Ref: item.claimed.Ref, SourceInstanceID: c.instanceID,
			Targets: make([]store.OutboxAttemptTarget, 0, targetCount),
		}
		for _, prepared := range plans {
			for _, target := range prepared.targets {
				sets[i].Targets = append(sets[i].Targets, store.OutboxAttemptTarget{
					TargetInstanceID: target.TargetInstanceID,
					TargetUserID:     prepared.targetUserID,
					BatchID:          [16]byte(prepared.plan.BatchID()),
					CommandID:        [16]byte(target.CommandID),
				})
			}
		}
	}
	if c.mutations == nil {
		return ErrMissingDependency
	}
	bindStarted := time.Now()
	bound, err := c.mutations.BindAttemptTargets(ctx, items[0].claimed.Ref.QueueKind, sets)
	c.observeStage(items[0].claimed.Ref.QueueKind, "bind", bindStarted, len(items))
	if err != nil {
		return fmt.Errorf("egress: bind frozen target set: %w", err)
	}
	if len(bound) != len(sets) {
		return markBoundDeliveryPending(fmt.Errorf("egress: bind result count %d want %d", len(bound), len(sets)))
	}
	for _, result := range bound {
		if result.Outcome != store.OutboxBindTargetBound && result.Outcome != store.OutboxBindTargetDuplicate {
			return markBoundDeliveryPending(fmt.Errorf("egress: bind frozen target set outcome %d", result.Outcome))
		}
	}
	if len(plans) == 0 || len(sets[0].Targets) == 0 {
		// Binding an authoritative empty set atomically records confirmed
		// completion. There is no second evidence mutation to race the finalizer.
		c.metrics.OutboxDelivered(0)
		return nil
	}
	admitStarted := time.Now()
	for _, prepared := range plans {
		if err := c.admitFrozenPlan(ctx, recoveryCtx, prepared.plan, notAfter, evidenceDeadline); err != nil {
			c.observeStage(items[0].claimed.Ref.QueueKind, "admit", admitStarted, len(items))
			return markBoundDeliveryPending(err)
		}
	}
	c.observeStage(items[0].claimed.Ref.QueueKind, "admit", admitStarted, len(items))
	return nil
}

func (c *deliveryCoordinator) deliveryRequest(items []projectedOutboxItem, targetUserID int64, notAfter, evidenceDeadline time.Time) (edgecontrol.DeliveryRequest, error) {
	if targetUserID < 0 || len(items) == 0 || notAfter.IsZero() || !evidenceDeadline.After(notAfter) {
		return edgecontrol.DeliveryRequest{}, errors.New("egress: invalid delivery target")
	}
	channel := items[0].claimed.Ref.QueueKind == store.OutboxQueueChannelPTS
	if channel != (targetUserID == 0) {
		return edgecontrol.DeliveryRequest{}, errors.New("egress: delivery target does not match ordering domain")
	}
	request := edgecontrol.DeliveryRequest{
		TargetUserID: targetUserID, NotAfter: notAfter, EvidenceDeadline: evidenceDeadline,
		ExcludeAuthKeyID: items[0].claimed.ExcludeAuthKeyID,
		ExcludeSessionID: items[0].claimed.ExcludeSessionID,
		Items:            make([]edgecontrol.DeliveryItem, len(items)),
	}
	for i, item := range items {
		ref, ok := edgeDeliveryRef(item.claimed.Ref, targetUserID)
		if !ok || len(item.payload) == 0 {
			return edgecontrol.DeliveryRequest{}, fmt.Errorf("egress: invalid delivery item %d", item.claimed.Ref.ItemID)
		}
		request.Items[i] = edgecontrol.DeliveryItem{
			Ref: ref, MessageType: proto.MessageFromServer,
			PayloadHash: edgecontrol.DeliveryPayloadHash(item.payload), UpdateBytes: item.payload,
			Channel: item.channelRoute,
		}
	}
	return request, nil
}

func (c *deliveryCoordinator) bindAuthoritativeEmpty(ctx context.Context, items []projectedOutboxItem, source string) error {
	return c.bindClaimedAuthoritativeEmpty(ctx, claimedItems(items), source)
}

func (c *deliveryCoordinator) bindClaimedAuthoritativeEmpty(ctx context.Context, items []store.OutboxClaimedItem, source string) error {
	if len(items) == 0 {
		return errors.New("egress: empty authoritative target bind")
	}
	sets := make([]store.OutboxAttemptTargetSet, len(items))
	for i, item := range items {
		sets[i] = store.OutboxAttemptTargetSet{Ref: item.Ref, SourceInstanceID: source, Targets: []store.OutboxAttemptTarget{}}
	}
	if c.mutations == nil {
		return ErrMissingDependency
	}
	bindStarted := time.Now()
	results, err := c.mutations.BindAttemptTargets(ctx, items[0].Ref.QueueKind, sets)
	c.observeStage(items[0].Ref.QueueKind, "bind", bindStarted, len(items))
	if err != nil {
		return fmt.Errorf("egress: bind authoritative empty set: %w", err)
	}
	if len(results) != len(sets) {
		return markBoundDeliveryPending(fmt.Errorf("egress: bind empty result count %d want %d", len(results), len(sets)))
	}
	for _, result := range results {
		if result.Outcome != store.OutboxBindTargetBound && result.Outcome != store.OutboxBindTargetDuplicate {
			return markBoundDeliveryPending(fmt.Errorf("egress: bind empty outcome %d", result.Outcome))
		}
		if result.Outcome == store.OutboxBindTargetBound {
			c.metrics.OutboxDelivered(0)
		}
	}
	return nil
}

func (c *deliveryCoordinator) replayBoundItems(
	ctx context.Context,
	recoveryCtx context.Context,
	items []projectedOutboxItem,
	notAfter time.Time,
	evidenceDeadline time.Time,
	reservation *deliveryActorReservation,
) error {
	stateStore := c.stateStore(items[0].claimed.Ref.QueueKind)
	if stateStore == nil {
		return ErrMissingDependency
	}
	for _, item := range items {
		if item.claimed.SourceInstanceID == "" {
			return errors.New("egress: partially bound recovery window")
		}
	}
	if len(items[0].claimed.Targets) == 0 {
		// Empty binding and confirmed resolution are one durable fact. Recovery
		// only needs to wait for (or re-wake) the fixed finalizer.
		return nil
	}
	type batchKey struct {
		batch        [16]byte
		targetUserID int64
	}
	type targetKey struct {
		instance string
		userID   int64
		batch    [16]byte
		command  [16]byte
	}
	firstTargets := items[0].claimed.Targets
	indexBytes := int64(len(firstTargets))*replayTargetIndexBytes + int64(len(firstTargets))*replayBatchIndexBytes
	if reservation != nil && indexBytes > 0 {
		if !reservation.retain(indexBytes) {
			return errorsNewActorCapacity()
		}
		defer reservation.releaseRetained(indexBytes)
	}
	expected := make(map[targetKey]struct{}, len(firstTargets))
	targetsByBatch := make(map[batchKey][]store.OutboxAttemptTarget, len(firstTargets))
	for _, target := range firstTargets {
		identity := targetKey{
			instance: target.TargetInstanceID, userID: target.TargetUserID,
			batch: target.BatchID, command: target.CommandID,
		}
		if identity.instance == "" || identity.batch == ([16]byte{}) || identity.command == ([16]byte{}) {
			return errors.New("egress: recovered target ledger contains an invalid command identity")
		}
		if _, duplicate := expected[identity]; duplicate {
			return errors.New("egress: recovered target ledger contains a duplicate command identity")
		}
		expected[identity] = struct{}{}
		key := batchKey{batch: target.BatchID, targetUserID: target.TargetUserID}
		targetsByBatch[key] = append(targetsByBatch[key], target)
	}
	seen := make(map[targetKey]int, len(firstTargets))
	for itemIndex := 1; itemIndex < len(items); itemIndex++ {
		targets := items[itemIndex].claimed.Targets
		if len(targets) != len(expected) {
			return fmt.Errorf("egress: recovered target topology differs at item %d", items[itemIndex].claimed.Ref.ItemID)
		}
		for _, target := range targets {
			identity := targetKey{
				instance: target.TargetInstanceID, userID: target.TargetUserID,
				batch: target.BatchID, command: target.CommandID,
			}
			if _, exists := expected[identity]; !exists {
				return fmt.Errorf("egress: recovered target topology differs at item %d", items[itemIndex].claimed.Ref.ItemID)
			}
			if seen[identity] == itemIndex {
				return fmt.Errorf("egress: recovered target topology duplicates a command at item %d", items[itemIndex].claimed.Ref.ItemID)
			}
			seen[identity] = itemIndex
		}
	}
	keys := make([]batchKey, 0, len(targetsByBatch))
	for key := range targetsByBatch {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].targetUserID != keys[j].targetUserID {
			return keys[i].targetUserID < keys[j].targetUserID
		}
		return string(keys[i].batch[:]) < string(keys[j].batch[:])
	})
	for _, key := range keys {
		if err := func() error {
			batchItems := items
			planBytes := deliveryPlanRetainedBytes(batchItems, len(targetsByBatch[key]))
			if reservation != nil && planBytes > 0 {
				if !reservation.retain(planBytes) {
					return errorsNewActorCapacity()
				}
				defer reservation.releaseRetained(planBytes)
			}
			request, err := c.deliveryRequest(batchItems, key.targetUserID, notAfter, evidenceDeadline)
			if err != nil {
				return err
			}
			persisted := make([]edgecontrol.PreparedDeliveryTarget, 0, len(targetsByBatch[key]))
			for _, target := range targetsByBatch[key] {
				persisted = append(persisted, edgecontrol.PreparedDeliveryTarget{
					TargetInstanceID: target.TargetInstanceID, CommandID: edgecontrol.CommandID(target.CommandID),
				})
			}
			sort.Slice(persisted, func(i, j int) bool { return persisted[i].TargetInstanceID < persisted[j].TargetInstanceID })
			plan, err := edgecontrol.RehydrateDeliveryPlan(
				batchItems[0].claimed.SourceInstanceID,
				edgecontrol.BatchID(key.batch), request, persisted,
			)
			if err != nil {
				return fmt.Errorf("egress: rehydrate frozen delivery: %w", err)
			}
			if err := c.admitFrozenPlan(ctx, recoveryCtx, plan, notAfter, evidenceDeadline); err != nil {
				return fmt.Errorf("egress: replay frozen delivery: %w", err)
			}
			return nil
		}(); err != nil {
			return err
		}
	}
	return nil
}

func (c *deliveryCoordinator) admitFrozenPlan(
	ctx context.Context,
	recoveryCtx context.Context,
	plan edgecontrol.FrozenDeliveryPlan,
	notAfter, evidenceDeadline time.Time,
) error {
	if c == nil || c.planner == nil || notAfter.IsZero() || !evidenceDeadline.After(notAfter) {
		return ErrMissingDependency
	}
	admitCtx, cancel := context.WithDeadline(ctx, notAfter)
	backoff := minimumOutboxRetryDelay
	var lastErr error
	for {
		admitted, err := c.planner.AdmitPreparedDelivery(admitCtx, plan)
		if err == nil {
			cancel()
			if err := validateFrozenAdmission(plan, admitted); err != nil {
				return fmt.Errorf("egress: validate frozen delivery admission: %w", err)
			}
			return nil
		}
		if !errors.Is(err, edgecontrol.ErrDeliveryIndeterminate) {
			cancel()
			return fmt.Errorf("egress: admit frozen delivery: %w", err)
		}
		lastErr = err
		if admitCtx.Err() != nil {
			break
		}
		timer := time.NewTimer(backoff)
		select {
		case <-admitCtx.Done():
			stopMutationTimer(timer)
		case <-timer.C:
		}
		if admitCtx.Err() != nil {
			break
		}
		if backoff < 64*time.Millisecond {
			backoff *= 2
			if backoff > 64*time.Millisecond {
				backoff = 64 * time.Millisecond
			}
		}
	}
	cancel()

	// The command write boundary is now closed. Reusing the same frozen plan is
	// an exact receipt probe: DeliveryFabric and Edge enforce replay-only until
	// evidenceDeadline, so this path cannot create a new attempt or socket write.
	if recoveryCtx == nil || recoveryCtx.Err() != nil || !time.Now().Before(evidenceDeadline) {
		return fmt.Errorf("egress: admit frozen delivery before command deadline: %w", lastErr)
	}
	replayCtx, cancelReplay := context.WithDeadline(recoveryCtx, evidenceDeadline)
	defer cancelReplay()
	backoff = minimumOutboxRetryDelay
	for {
		admitted, err := c.planner.AdmitPreparedDelivery(replayCtx, plan)
		if err == nil {
			if validateErr := validateFrozenAdmission(plan, admitted); validateErr != nil {
				return fmt.Errorf("egress: validate frozen receipt replay: %w", validateErr)
			}
			return nil
		}
		lastErr = err
		if !errors.Is(err, edgecontrol.ErrDeliveryIndeterminate) {
			return fmt.Errorf("egress: replay frozen delivery receipt: %w", err)
		}
		timer := time.NewTimer(backoff)
		select {
		case <-replayCtx.Done():
			stopMutationTimer(timer)
			return fmt.Errorf("egress: replay frozen delivery receipt before evidence deadline: %w", lastErr)
		case <-timer.C:
		}
		if backoff < 64*time.Millisecond {
			backoff *= 2
			if backoff > 64*time.Millisecond {
				backoff = 64 * time.Millisecond
			}
		}
	}
}

func validateFrozenAdmission(plan edgecontrol.FrozenDeliveryPlan, admitted edgecontrol.FrozenDelivery) error {
	want := plan.Targets()
	if admitted.BatchID != plan.BatchID() || len(admitted.Targets) != len(want) {
		return fmt.Errorf("egress: frozen delivery admission identity/count mismatch")
	}
	byInstance := make(map[string]edgecontrol.PreparedDeliveryTarget, len(want))
	for _, target := range want {
		byInstance[target.TargetInstanceID] = target
	}
	for _, target := range admitted.Targets {
		expected, ok := byInstance[target.TargetInstanceID]
		if !ok || target.CommandID != expected.CommandID || target.Err != nil ||
			target.Admission.BatchID != admitted.BatchID || target.Admission.CommandID != target.CommandID ||
			target.Admission.TargetInstanceID != target.TargetInstanceID || target.Admission.SourceInstanceID != plan.SourceInstanceID() {
			return fmt.Errorf("egress: frozen delivery admission target mismatch target=%q", target.TargetInstanceID)
		}
		switch target.Admission.Outcome {
		case edgecontrol.AdmissionAccepted, edgecontrol.AdmissionDuplicateInFlight, edgecontrol.AdmissionDuplicateTerminal:
		default:
			return fmt.Errorf("egress: delivery admission outcome %d target=%s", target.Admission.Outcome, target.TargetInstanceID)
		}
		delete(byInstance, target.TargetInstanceID)
	}
	if len(byInstance) != 0 {
		return fmt.Errorf("egress: frozen delivery admission omitted target")
	}
	return nil
}

func (c *deliveryCoordinator) resolveItems(
	ctx context.Context,
	stateStore store.DurableOutboxStateStore,
	owner string,
	items []store.OutboxClaimedItem,
	cause error,
	terminal bool,
) {
	if stateStore == nil || len(items) == 0 {
		return
	}
	if owner == "" {
		return
	}
	resolutions := make([]store.OutboxOwnedAttemptResolution, len(items))
	for i, item := range items {
		kind := store.OutboxResolutionRetry
		retryDelay := retryDelayForAttempt(item.Ref.Attempt)
		if terminal {
			// A deterministic online-projection poison must not permanently halt a
			// durable ordering lane. PTS queues advance through explicit resync;
			// absolute non-PTS delivery is auditably abandoned.
			kind = exhaustedResolutionKind(item.Ref.QueueKind)
			retryDelay = 0
		} else if item.Ref.Attempt >= maxOnlineDeliveryAttempts {
			kind = exhaustedResolutionKind(item.Ref.QueueKind)
			retryDelay = 0
		}
		resolutions[i] = store.OutboxOwnedAttemptResolution{
			Ref: item.Ref, Owner: owner, Kind: kind, RetryDelay: retryDelay, LastError: cause.Error(),
		}
	}
	results, err := c.mutations.ResolveOwnedAttemptBatch(ctx, items[0].Ref.QueueKind, resolutions)
	if err != nil {
		c.log.Error("resolve durable delivery attempt", zap.Error(err), zap.Error(cause))
		c.metrics.OutboxFailed(err)
		return
	}
	for _, result := range results {
		if result.Outcome == store.OutboxResolutionRejected {
			c.log.Error("durable delivery resolution rejected", zap.Int64("item_id", result.Ref.ItemID), zap.Error(cause))
		}
	}
	// The mutation actor transfers every recorded exact ref to its bounded
	// finalizer lane before completing this call. PostgreSQL remains the durable
	// recovery fact if the process exits between commit and that in-memory handoff.
	if terminal {
		c.metrics.OutboxFailed(cause)
	}
}

func exhaustedResolutionKind(kind store.OutboxQueueKind) store.OutboxResolutionKind {
	switch kind {
	case store.OutboxQueueDispatchPTS, store.OutboxQueueChannelPTS:
		return store.OutboxResolutionTerminalResync
	case store.OutboxQueueAbsoluteDelivery:
		return store.OutboxResolutionAbandoned
	default:
		// Unknown ordering domains have no recovery contract. Returning the zero
		// value makes the durable store reject the resolution instead of inventing
		// a generic compatibility terminal.
		return 0
	}
}

func retryDelayForAttempt(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	shift := attempt - 1
	if shift > 8 {
		shift = 8
	}
	delay := minimumOutboxRetryDelay * time.Duration(1<<shift)
	if delay > maximumOutboxRetryDelay {
		delay = maximumOutboxRetryDelay
	}
	return delay
}

func claimedItems(items []projectedOutboxItem) []store.OutboxClaimedItem {
	out := make([]store.OutboxClaimedItem, len(items))
	for i := range items {
		out[i] = items[i].claimed
	}
	return out
}

func uniquePositiveIDs(values []int64) []int64 {
	seen := make(map[int64]struct{}, len(values))
	out := make([]int64, 0, len(values))
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func sameInt64s(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func projectionErrorIsTerminal(err error) bool {
	return errors.Is(err, errMissingOutboxEvent) || errors.Is(err, errOutboxUpdateBuilderCount) ||
		errors.Is(err, errOutboxUpdateBuilderEmpty) || errors.Is(err, ErrInvalidOutboxExclusionPair)
}

// OutboxLogicalShards exposes the stable durable ordering-domain hash space.
const OutboxLogicalShards = outboxLogicalShards

func LogicalShardsForWorker(worker, workers int) []int {
	return logicalShardsForWorker(worker, workers)
}
func NormalizedOutboxWorkers(workers int) int { return normalizedOutboxWorkers(workers) }

func normalizedOutboxWorkers(workers int) int {
	if workers < 1 {
		workers = 1
	}
	if workers > outboxLogicalShards {
		workers = outboxLogicalShards
	}
	return workers
}

func logicalShardsForWorker(worker, workers int) []int {
	workers = normalizedOutboxWorkers(workers)
	if worker < 0 || worker >= workers {
		return nil
	}
	out := make([]int, 0, (outboxLogicalShards+workers-1)/workers)
	for shard := worker; shard < outboxLogicalShards; shard += workers {
		out = append(out, shard)
	}
	return out
}
