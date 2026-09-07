package memory

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"telesrv/internal/store"
)

type memoryOutboxLane struct {
	streamID    int64
	headItemID  int64
	state       string
	readyAt     time.Time
	fence       uint64
	owner       string
	leaseUntil  time.Time
	windowEnd   int64
	headAttempt int
	lastError   string
}

type memoryAttemptKey struct {
	itemID int64
	fence  uint64
}

type memoryTargetKey struct {
	instance string
	userID   int64
	batch    [16]byte
	command  [16]byte
}

type memoryAttemptTarget struct {
	evidence                 store.OutboxEvidenceKind
	evidenceAt               time.Time
	eligibleSessions         int
	writtenSessions          int
	physicalFirstServerMsgID int64
}

type memoryOutboxAttempt struct {
	ref              store.OutboxAttemptRef
	owner            string
	issuedAt         time.Time
	leaseUntil       time.Time
	commandNotAfter  time.Time
	evidenceDeadline time.Time
	ordinal          int
	targetsBound     bool
	sourceInstanceID string
	targets          map[memoryTargetKey]*memoryAttemptTarget
	emptyEvidence    bool
	resolution       store.OutboxResolutionKind
	retryAt          time.Time
	lastError        string
}

const memoryOutboxLeaseSafetyMargin = 100 * time.Millisecond

// DeliveryOutboxStore is a production-isomorphic in-memory absolute delivery
// state machine. Its mutex replaces PostgreSQL row locks only for tests; every
// fence, evidence and prefix transition is retained. Finalized attempts are
// deleted in the same critical section as their item/lane transition.
type DeliveryOutboxStore struct {
	mu        sync.Mutex
	nextID    int64
	nextFence uint64
	items     map[int64]store.DeliveryOutboxItem
	lanes     map[int64]*memoryOutboxLane
	attempts  map[memoryAttemptKey]*memoryOutboxAttempt
}

func NewDeliveryOutboxStore() *DeliveryOutboxStore {
	return &DeliveryOutboxStore{
		nextID: 1, nextFence: 1, items: make(map[int64]store.DeliveryOutboxItem),
		lanes: make(map[int64]*memoryOutboxLane), attempts: make(map[memoryAttemptKey]*memoryOutboxAttempt),
	}
}

func (s *DeliveryOutboxStore) Enqueue(_ context.Context, input store.DeliveryOutboxEnqueue) (store.DeliveryOutboxItem, error) {
	if input.TargetUserID <= 0 {
		return store.DeliveryOutboxItem{}, fmt.Errorf("delivery outbox target user id is required")
	}
	if len(input.Payload) == 0 {
		return store.DeliveryOutboxItem{}, fmt.Errorf("delivery outbox payload is required")
	}
	if len(input.Payload) > store.MaxDeliveryBatchBytes {
		return store.DeliveryOutboxItem{}, fmt.Errorf("delivery outbox payload exceeds %d bytes", store.MaxDeliveryBatchBytes)
	}
	if input.RecoveryPolicy != store.OutboxRecoveryAbsoluteReload {
		return store.DeliveryOutboxItem{}, fmt.Errorf("delivery outbox recovery policy must be absolute_reload")
	}
	if (input.ExcludeAuthKeyID == ([8]byte{})) != (input.ExcludeSessionID == 0) {
		return store.DeliveryOutboxItem{}, fmt.Errorf("delivery outbox exclusion requires both raw auth key and session id")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	item := store.DeliveryOutboxItem{
		ID: s.nextID, TargetUserID: input.TargetUserID,
		ExcludeAuthKeyID: input.ExcludeAuthKeyID, ExcludeSessionID: input.ExcludeSessionID,
		Payload: append([]byte(nil), input.Payload...), RecoveryPolicy: input.RecoveryPolicy,
	}
	s.nextID++
	s.items[item.ID] = cloneDeliveryOutboxItem(item)
	// The active-lane path is exactly INSERT ... ON CONFLICT DO NOTHING: never
	// rewrite its head/readiness while appending a successor.
	if _, exists := s.lanes[item.TargetUserID]; !exists {
		s.lanes[item.TargetUserID] = &memoryOutboxLane{
			streamID: item.TargetUserID, headItemID: item.ID,
			state: "ready", readyAt: time.Now(),
		}
	}
	return cloneDeliveryOutboxItem(item), nil
}

func (s *DeliveryOutboxStore) ClaimWindows(_ context.Context, req store.OutboxClaimRequest) ([]store.OutboxClaimWindow, error) {
	if req.QueueKind != store.OutboxQueueAbsoluteDelivery {
		return nil, fmt.Errorf("claim absolute delivery windows: queue kind %d", req.QueueKind)
	}
	if strings.TrimSpace(req.Owner) == "" || strings.TrimSpace(req.Owner) != req.Owner || len(req.Owner) > store.MaxDeliveryLeaseOwnerBytes {
		return nil, fmt.Errorf("claim absolute delivery windows: owner is required")
	}
	if req.LaneLimit <= 0 {
		req.LaneLimit = 100
	}
	if req.WindowSize <= 0 {
		req.WindowSize = 16
	}
	if req.WindowByteLimit <= 0 {
		req.WindowByteLimit = 1 << 20
	}
	if req.WindowByteLimit > store.MaxDeliveryBatchBytes {
		req.WindowByteLimit = store.MaxDeliveryBatchBytes
	}
	if req.LeaseDuration <= 0 {
		req.LeaseDuration = 30 * time.Second
	}
	if req.PhysicalDuration <= 0 {
		return nil, fmt.Errorf("claim absolute delivery windows: physical duration is required")
	}
	if req.ClockSkewAllowance <= 0 {
		return nil, fmt.Errorf("claim absolute delivery windows: clock skew allowance is required")
	}
	if req.LeaseDuration <= memoryOutboxLeaseSafetyMargin ||
		req.PhysicalDuration >= req.LeaseDuration-memoryOutboxLeaseSafetyMargin ||
		req.ClockSkewAllowance >= req.LeaseDuration-memoryOutboxLeaseSafetyMargin-req.PhysicalDuration {
		return nil, fmt.Errorf("claim absolute delivery windows: physical duration plus clock skew allowance and lease safety must be shorter than lease duration")
	}
	selectedShards, scoped, err := memorySelectedShards(req.LogicalShardCount, req.LogicalShardIDs)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.nextFence > uint64(math.MaxInt64) {
		return nil, fmt.Errorf("claim absolute delivery windows: lease fence sequence exhausted")
	}
	now := time.Now()
	lanes := make([]*memoryOutboxLane, 0, len(s.lanes))
	for _, lane := range s.lanes {
		if scoped {
			if _, ok := selectedShards[int(lane.streamID%store.DispatchOutboxLogicalShards)]; !ok {
				continue
			}
		}
		eligible := lane.state == "ready" && !lane.readyAt.After(now)
		if lane.state == "leased" && !lane.leaseUntil.After(now) &&
			!s.hasPendingFinalization(lane) && !s.hasBoundRecovery(lane) {
			eligible = true
		}
		if !eligible || lane.fence == math.MaxInt64 {
			continue
		}
		_, ok := s.items[lane.headItemID]
		if !ok {
			continue
		}
		lanes = append(lanes, lane)
	}
	sort.Slice(lanes, func(i, j int) bool {
		left, right := lanes[i].readyAt, lanes[j].readyAt
		if lanes[i].state == "leased" {
			left = lanes[i].leaseUntil
		}
		if lanes[j].state == "leased" {
			right = lanes[j].leaseUntil
		}
		if !left.Equal(right) {
			return left.Before(right)
		}
		return lanes[i].streamID < lanes[j].streamID
	})
	if len(lanes) > req.LaneLimit {
		lanes = lanes[:req.LaneLimit]
	}
	windows := make([]store.OutboxClaimWindow, 0, len(lanes))
	for _, lane := range lanes {
		items := s.streamItems(lane.streamID)
		windowItems := make([]store.DeliveryOutboxItem, 0, req.WindowSize)
		bytesUsed := 0
		for _, item := range items {
			if item.ID < lane.headItemID {
				continue
			}
			if len(windowItems) >= req.WindowSize {
				break
			}
			if len(windowItems) > 0 {
				head := windowItems[0]
				if item.ExcludeAuthKeyID != head.ExcludeAuthKeyID || item.ExcludeSessionID != head.ExcludeSessionID {
					break
				}
				if bytesUsed+len(item.Payload) > req.WindowByteLimit {
					break
				}
			}
			bytesUsed += len(item.Payload)
			windowItems = append(windowItems, item)
		}
		if len(windowItems) == 0 {
			continue
		}
		oldFence := lane.fence
		lane.fence = s.nextFence
		s.nextFence++
		if oldFence > 0 {
			for key, old := range s.attempts {
				if old.ref.StreamID == lane.streamID && key.fence == oldFence {
					delete(s.attempts, key)
				}
			}
		}
		lane.state = "leased"
		lane.owner = req.Owner
		lane.leaseUntil = now.Add(req.LeaseDuration)
		lane.windowEnd = windowItems[len(windowItems)-1].ID
		lane.headAttempt++
		window := store.OutboxClaimWindow{
			QueueKind: store.OutboxQueueAbsoluteDelivery, StreamID: lane.streamID,
			Owner: lane.owner, LeaseFence: lane.fence, LeaseUntil: lane.leaseUntil,
		}
		for ordinal, item := range windowItems {
			attemptNo := lane.headAttempt
			ref := store.OutboxAttemptRef{
				QueueKind: store.OutboxQueueAbsoluteDelivery, StreamID: item.TargetUserID,
				ItemID: item.ID, Sequence: 0, LeaseFence: lane.fence, Attempt: attemptNo,
			}
			commandNotAfter := now.Add(req.PhysicalDuration)
			evidenceDeadline := commandNotAfter.Add(req.ClockSkewAllowance)
			s.attempts[memoryAttemptKey{item.ID, lane.fence}] = &memoryOutboxAttempt{
				ref: ref, owner: lane.owner, issuedAt: now, leaseUntil: lane.leaseUntil,
				commandNotAfter: commandNotAfter, evidenceDeadline: evidenceDeadline, ordinal: ordinal,
			}
			claimed := claimedAbsoluteItem(item, ref)
			claimed.CommandNotAfter = commandNotAfter
			claimed.EvidenceDeadline = evidenceDeadline
			window.Items = append(window.Items, claimed)
		}
		windows = append(windows, window)
	}
	return windows, nil
}

func (s *DeliveryOutboxStore) RecoverBoundWindows(_ context.Context, req store.OutboxRecoverBoundRequest) ([]store.OutboxClaimWindow, error) {
	if req.QueueKind != store.OutboxQueueAbsoluteDelivery {
		return nil, fmt.Errorf("recover absolute delivery windows: queue kind %d", req.QueueKind)
	}
	if req.Mode != store.OutboxBoundRecoveryExactOwnerLive {
		return nil, fmt.Errorf("recover absolute delivery windows: recovery mode is required")
	}
	if strings.TrimSpace(req.Owner) == "" || strings.TrimSpace(req.Owner) != req.Owner || len(req.Owner) > store.MaxDeliveryLeaseOwnerBytes {
		return nil, fmt.Errorf("recover absolute delivery windows: exact-owner mode requires owner")
	}
	if req.LeaseDuration <= 0 {
		req.LeaseDuration = 30 * time.Second
	}
	if req.LaneLimit <= 0 {
		req.LaneLimit = 100
	}
	selectedShards, scoped, err := memorySelectedShards(req.LogicalShardCount, req.LogicalShardIDs)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	lanes := make([]*memoryOutboxLane, 0)
	for _, lane := range s.lanes {
		exactLive := lane.leaseUntil.After(now) && lane.owner == req.Owner
		if lane.state != "leased" || lane.windowEnd == 0 || !exactLive ||
			!s.memoryLaneRecoverable(lane, req.Mode) {
			continue
		}
		if scoped {
			if _, ok := selectedShards[int(lane.streamID%store.DispatchOutboxLogicalShards)]; !ok {
				continue
			}
		}
		lanes = append(lanes, lane)
	}
	sort.Slice(lanes, func(i, j int) bool {
		if !lanes[i].leaseUntil.Equal(lanes[j].leaseUntil) {
			return lanes[i].leaseUntil.Before(lanes[j].leaseUntil)
		}
		return lanes[i].streamID < lanes[j].streamID
	})
	if len(lanes) > req.LaneLimit {
		lanes = lanes[:req.LaneLimit]
	}
	windows := make([]store.OutboxClaimWindow, 0, len(lanes))
	for _, lane := range lanes {
		lane.owner = req.Owner
		if renewed := now.Add(req.LeaseDuration); renewed.After(lane.leaseUntil) {
			lane.leaseUntil = renewed
		}
		for _, attempt := range s.memoryCurrentAttempts(lane.streamID, lane.fence) {
			attempt.owner = req.Owner
			if lane.leaseUntil.After(attempt.leaseUntil) {
				attempt.leaseUntil = lane.leaseUntil
			}
		}
		window := store.OutboxClaimWindow{
			QueueKind: store.OutboxQueueAbsoluteDelivery, StreamID: lane.streamID,
			Owner: req.Owner, LeaseFence: lane.fence, LeaseUntil: lane.leaseUntil,
		}
		for _, attempt := range s.memoryCurrentAttempts(lane.streamID, lane.fence) {
			item, ok := s.items[attempt.ref.ItemID]
			if !ok {
				continue
			}
			claimed := claimedAbsoluteItem(item, attempt.ref)
			claimed.CommandNotAfter = attempt.commandNotAfter
			claimed.EvidenceDeadline = attempt.evidenceDeadline
			claimed.SourceInstanceID = attempt.sourceInstanceID
			claimed.Targets = make([]store.OutboxAttemptTarget, 0, len(attempt.targets))
			for key := range attempt.targets {
				claimed.Targets = append(claimed.Targets, store.OutboxAttemptTarget{
					TargetInstanceID: key.instance, TargetUserID: key.userID,
					BatchID: key.batch, CommandID: key.command,
				})
			}
			sort.Slice(claimed.Targets, func(i, j int) bool {
				left, right := claimed.Targets[i], claimed.Targets[j]
				if left.TargetInstanceID != right.TargetInstanceID {
					return left.TargetInstanceID < right.TargetInstanceID
				}
				if left.TargetUserID != right.TargetUserID {
					return left.TargetUserID < right.TargetUserID
				}
				return string(left.CommandID[:]) < string(right.CommandID[:])
			})
			window.Items = append(window.Items, claimed)
		}
		windows = append(windows, window)
	}
	return windows, nil
}

func (s *DeliveryOutboxStore) memoryCurrentAttempts(streamID int64, fence uint64) []*memoryOutboxAttempt {
	result := make([]*memoryOutboxAttempt, 0)
	for _, attempt := range s.attempts {
		if attempt.ref.StreamID == streamID && attempt.ref.LeaseFence == fence {
			result = append(result, attempt)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ordinal < result[j].ordinal })
	return result
}

func (s *DeliveryOutboxStore) memoryLaneRecoverable(lane *memoryOutboxLane, mode store.OutboxBoundRecoveryMode) bool {
	attempts := s.memoryCurrentAttempts(lane.streamID, lane.fence)
	if len(attempts) == 0 || attempts[0].ref.ItemID != lane.headItemID ||
		!attempts[0].targetsBound || attempts[0].resolution != 0 {
		return false
	}
	for _, attempt := range attempts {
		if !attempt.targetsBound || attempt.resolution != 0 ||
			!attempt.commandNotAfter.After(time.Now()) || !attempt.evidenceDeadline.After(time.Now()) {
			return false
		}
	}
	return true
}

func (s *DeliveryOutboxStore) RecoverFinalizableAttempts(_ context.Context, req store.OutboxRecoverFinalizableRequest) ([]store.OutboxFinalizeRequest, error) {
	if req.QueueKind != store.OutboxQueueAbsoluteDelivery {
		return nil, fmt.Errorf("recover finalizable absolute delivery attempts: queue kind %d", req.QueueKind)
	}
	if req.AttemptLimit <= 0 || req.AttemptLimit > store.MaxDeliveryBatchItems {
		return nil, fmt.Errorf("recover finalizable absolute delivery attempts: attempt limit must be between 1 and %d", store.MaxDeliveryBatchItems)
	}
	if req.LaneLimit <= 0 {
		req.LaneLimit = 100
	}
	selectedShards, scoped, err := memorySelectedShards(req.LogicalShardCount, req.LogicalShardIDs)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	lanes := make([]*memoryOutboxLane, 0)
	for _, lane := range s.lanes {
		if lane.state != "leased" {
			continue
		}
		if scoped {
			if _, ok := selectedShards[int(lane.streamID%store.DispatchOutboxLogicalShards)]; !ok {
				continue
			}
		}
		finalizable := false
		for _, attempt := range s.memoryCurrentAttempts(lane.streamID, lane.fence) {
			if attempt.resolution != 0 && (attempt.retryAt.IsZero() || !attempt.retryAt.After(time.Now())) {
				finalizable = true
				break
			}
		}
		if finalizable {
			lanes = append(lanes, lane)
		}
	}
	sort.Slice(lanes, func(i, j int) bool { return lanes[i].streamID < lanes[j].streamID })
	if len(lanes) > req.LaneLimit {
		lanes = lanes[:req.LaneLimit]
	}
	requests := make([]store.OutboxFinalizeRequest, 0)
	for _, lane := range lanes {
		for _, attempt := range s.memoryCurrentAttempts(lane.streamID, lane.fence) {
			if attempt.resolution != 0 && (attempt.retryAt.IsZero() || !attempt.retryAt.After(time.Now())) {
				requests = append(requests, store.OutboxFinalizeRequest{Ref: attempt.ref})
				if len(requests) == req.AttemptLimit {
					return requests, nil
				}
			}
		}
	}
	return requests, nil
}

func (s *DeliveryOutboxStore) ExpireEvidenceDeadlines(_ context.Context, req store.OutboxEvidenceExpiryRequest) ([]store.OutboxFinalizeRequest, error) {
	if req.QueueKind != store.OutboxQueueAbsoluteDelivery {
		return nil, fmt.Errorf("expire absolute delivery evidence deadlines: queue kind %d", req.QueueKind)
	}
	if req.MaxAttempts <= 0 {
		return nil, fmt.Errorf("expire absolute delivery evidence deadlines: max attempts is required")
	}
	if req.LaneLimit <= 0 {
		req.LaneLimit = 100
	}
	selectedShards, scoped, err := memorySelectedShards(req.LogicalShardCount, req.LogicalShardIDs)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	type expiredAttempt struct {
		attempt  *memoryOutboxAttempt
		deadline time.Time
	}
	expired := make([]expiredAttempt, 0)
	for _, lane := range s.lanes {
		if lane.state != "leased" {
			continue
		}
		if scoped {
			if _, ok := selectedShards[int(lane.streamID%store.DispatchOutboxLogicalShards)]; !ok {
				continue
			}
		}
		attempt := s.attempts[memoryAttemptKey{itemID: lane.headItemID, fence: lane.fence}]
		if attempt == nil || attempt.resolution != 0 ||
			attempt.evidenceDeadline.IsZero() || attempt.evidenceDeadline.After(now) {
			continue
		}
		expired = append(expired, expiredAttempt{attempt: attempt, deadline: attempt.evidenceDeadline})
	}
	sort.Slice(expired, func(i, j int) bool {
		if !expired[i].deadline.Equal(expired[j].deadline) {
			return expired[i].deadline.Before(expired[j].deadline)
		}
		return expired[i].attempt.ref.StreamID < expired[j].attempt.ref.StreamID
	})
	if len(expired) > req.LaneLimit {
		expired = expired[:req.LaneLimit]
	}
	requests := make([]store.OutboxFinalizeRequest, 0, len(expired))
	for _, item := range expired {
		kind := store.OutboxResolutionRetry
		if item.attempt.ref.Attempt >= req.MaxAttempts {
			kind = store.OutboxResolutionAbandoned
		}
		item.attempt.resolution = kind
		if kind == store.OutboxResolutionRetry {
			item.attempt.retryAt = now
		}
		item.attempt.lastError = "physical evidence deadline expired"
		requests = append(requests, store.OutboxFinalizeRequest{Ref: item.attempt.ref})
	}
	return requests, nil
}

func (s *DeliveryOutboxStore) BindAttemptTargets(_ context.Context, sets []store.OutboxAttemptTargetSet) ([]store.OutboxBindTargetResult, error) {
	if len(sets) > store.MaxDeliveryBatchItems {
		return nil, fmt.Errorf("bind absolute delivery targets: batch exceeds %d items", store.MaxDeliveryBatchItems)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	results := make([]store.OutboxBindTargetResult, len(sets))
	for i, set := range sets {
		results[i].Ref = set.Ref
		sourceInstanceID := strings.TrimSpace(set.SourceInstanceID)
		if sourceInstanceID == "" || sourceInstanceID != set.SourceInstanceID || len(sourceInstanceID) > store.MaxDeliveryInstanceIDBytes || len(set.Targets) > store.MaxDeliveryTargets {
			results[i].Outcome = store.OutboxBindTargetRejected
			continue
		}
		attempt, lane, outcome := s.currentAttempt(set.Ref)
		if outcome != 0 {
			results[i].Outcome = bindOutcomeFromAttempt(outcome)
			continue
		}
		_ = lane
		wanted := make(map[memoryTargetKey]*memoryAttemptTarget, len(set.Targets))
		seenTargets := make(map[struct {
			instance string
			userID   int64
		}]struct{}, len(set.Targets))
		valid := true
		for _, target := range set.Targets {
			key := memoryTargetKey{strings.TrimSpace(target.TargetInstanceID), target.TargetUserID, target.BatchID, target.CommandID}
			if key.instance == "" || key.instance != target.TargetInstanceID || len(key.instance) > store.MaxDeliveryInstanceIDBytes || key.userID != set.Ref.StreamID || key.batch == ([16]byte{}) || key.command == ([16]byte{}) {
				valid = false
				break
			}
			identity := struct {
				instance string
				userID   int64
			}{instance: key.instance, userID: key.userID}
			if _, duplicate := seenTargets[identity]; duplicate {
				valid = false
				break
			}
			seenTargets[identity] = struct{}{}
			if _, duplicate := wanted[key]; duplicate {
				valid = false
				break
			}
			wanted[key] = &memoryAttemptTarget{}
		}
		if !valid {
			results[i].Outcome = store.OutboxBindTargetRejected
			continue
		}
		if attempt.targetsBound {
			equal := attempt.sourceInstanceID == sourceInstanceID && sameMemoryTargetSet(attempt.targets, wanted)
			if equal && len(wanted) == 0 && (!attempt.emptyEvidence || attempt.resolution != memoryConfirmedResolution) {
				equal = false
			}
			if equal {
				results[i].Outcome = store.OutboxBindTargetDuplicate
			} else {
				results[i].Outcome = store.OutboxBindTargetRejected
			}
			continue
		}
		if attempt.commandNotAfter.IsZero() || attempt.evidenceDeadline.IsZero() ||
			!attempt.commandNotAfter.Before(attempt.evidenceDeadline) || !attempt.evidenceDeadline.Before(attempt.leaseUntil) {
			results[i].Outcome = store.OutboxBindTargetRejected
			continue
		}
		attempt.targetsBound = true
		attempt.sourceInstanceID = sourceInstanceID
		attempt.targets = wanted
		if len(wanted) == 0 {
			attempt.emptyEvidence = true
			attempt.resolution = memoryConfirmedResolution
		}
		results[i].Outcome = store.OutboxBindTargetBound
	}
	return results, nil
}

func (s *DeliveryOutboxStore) RecordAttemptEvidenceBatch(_ context.Context, evidence []store.OutboxAttemptEvidence) ([]store.OutboxEvidenceResult, error) {
	if len(evidence) > store.MaxDeliveryBatchItems {
		return nil, fmt.Errorf("record absolute delivery evidence: batch exceeds %d items", store.MaxDeliveryBatchItems)
	}
	type evidenceKey struct {
		ref              store.OutboxAttemptRef
		targetInstanceID string
		targetUserID     int64
		batchID          [16]byte
		commandID        [16]byte
	}
	seen := make(map[evidenceKey]store.OutboxAttemptEvidence, len(evidence))
	for i, item := range evidence {
		key := evidenceKey{
			ref: item.Ref, targetInstanceID: item.TargetInstanceID, targetUserID: item.TargetUserID,
			batchID: item.BatchID, commandID: item.CommandID,
		}
		if previous, ok := seen[key]; ok &&
			(previous.Kind != item.Kind || previous.SourceInstanceID != item.SourceInstanceID ||
				previous.EligibleSessions != item.EligibleSessions || previous.WrittenSessions != item.WrittenSessions ||
				previous.ServerMsgID != item.ServerMsgID) {
			return nil, fmt.Errorf("record absolute delivery evidence: conflicting duplicate at index %d", i)
		}
		seen[key] = item
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	results := make([]store.OutboxEvidenceResult, len(evidence))
	for i, item := range evidence {
		results[i].Ref = item.Ref
		if item.ObservedAt.IsZero() {
			results[i].Outcome = store.OutboxEvidenceRejected
			continue
		}
		switch item.Kind {
		case store.OutboxEvidenceEdgeWritten:
			if item.EligibleSessions <= 0 || item.WrittenSessions != item.EligibleSessions || item.ServerMsgID <= 0 {
				results[i].Outcome = store.OutboxEvidenceRejected
				continue
			}
		case store.OutboxEvidenceEdgeNoEligible, store.OutboxEvidenceAuthoritativeNoTargets:
			if item.EligibleSessions != 0 || item.WrittenSessions != 0 || item.ServerMsgID != 0 {
				results[i].Outcome = store.OutboxEvidenceRejected
				continue
			}
		default:
			results[i].Outcome = store.OutboxEvidenceRejected
			continue
		}
		attempt, _, outcome := s.currentAttempt(item.Ref)
		if outcome != 0 {
			results[i].Outcome = evidenceOutcomeFromAttempt(outcome)
			continue
		}
		if !attempt.targetsBound {
			results[i].Outcome = store.OutboxEvidenceRejected
			continue
		}
		sourceInstanceID := strings.TrimSpace(item.SourceInstanceID)
		if sourceInstanceID == "" || sourceInstanceID != item.SourceInstanceID || len(sourceInstanceID) > store.MaxDeliveryInstanceIDBytes || sourceInstanceID != attempt.sourceInstanceID {
			results[i].Outcome = store.OutboxEvidenceFenced
			continue
		}
		// Match the durable stores: only the store clock authorizes physical
		// completion. ObservedAt is diagnostic and cannot revive an expired
		// command.
		if !time.Now().Before(attempt.evidenceDeadline) {
			results[i].Outcome = store.OutboxEvidenceFenced
			continue
		}
		if item.Kind == store.OutboxEvidenceAuthoritativeNoTargets {
			if len(attempt.targets) != 0 {
				results[i].Outcome = store.OutboxEvidenceRejected
			} else if attempt.emptyEvidence {
				results[i].Outcome = store.OutboxEvidenceDuplicate
			} else if attempt.resolution != 0 {
				results[i].Outcome = store.OutboxEvidenceRejected
			} else {
				attempt.emptyEvidence = true
				attempt.resolution = memoryConfirmedResolution
				results[i].Outcome = store.OutboxEvidenceRecorded
			}
			continue
		}
		targetInstanceID := strings.TrimSpace(item.TargetInstanceID)
		if targetInstanceID == "" || targetInstanceID != item.TargetInstanceID || len(targetInstanceID) > store.MaxDeliveryInstanceIDBytes {
			results[i].Outcome = store.OutboxEvidenceRejected
			continue
		}
		key := memoryTargetKey{targetInstanceID, item.TargetUserID, item.BatchID, item.CommandID}
		target, ok := attempt.targets[key]
		if !ok {
			results[i].Outcome = store.OutboxEvidenceRejected
			continue
		}
		if item.Kind != store.OutboxEvidenceEdgeWritten && item.Kind != store.OutboxEvidenceEdgeNoEligible {
			results[i].Outcome = store.OutboxEvidenceRejected
			continue
		}
		if target.evidence != 0 {
			if target.evidence == item.Kind && target.eligibleSessions == item.EligibleSessions &&
				target.writtenSessions == item.WrittenSessions && target.physicalFirstServerMsgID == item.ServerMsgID {
				results[i].Outcome = store.OutboxEvidenceDuplicate
			} else {
				results[i].Outcome = store.OutboxEvidenceRejected
			}
			continue
		}
		if attempt.resolution != 0 {
			results[i].Outcome = store.OutboxEvidenceRejected
			continue
		}
		target.evidence = item.Kind
		target.evidenceAt = item.ObservedAt
		target.eligibleSessions = item.EligibleSessions
		target.writtenSessions = item.WrittenSessions
		target.physicalFirstServerMsgID = item.ServerMsgID
		results[i].Outcome = store.OutboxEvidenceRecorded
		allConfirmed := true
		for _, candidate := range attempt.targets {
			if candidate.evidence == 0 {
				allConfirmed = false
				break
			}
		}
		if allConfirmed {
			attempt.resolution = memoryConfirmedResolution
		}
	}
	return results, nil
}

// memoryConfirmedResolution is internal evidence state, not a public failure
// resolution. Public enum values start at retry and are never persisted here.
const memoryConfirmedResolution store.OutboxResolutionKind = 255

func (s *DeliveryOutboxStore) ResolveTargetAttemptBatch(_ context.Context, resolutions []store.OutboxTargetAttemptResolution) ([]store.OutboxResolutionResult, error) {
	if len(resolutions) > store.MaxDeliveryBatchItems {
		return nil, fmt.Errorf("resolve absolute delivery target: batch exceeds %d items", store.MaxDeliveryBatchItems)
	}
	if err := validateMemoryTargetResolutionBatch(resolutions); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	results := make([]store.OutboxResolutionResult, len(resolutions))
	for i, resolution := range resolutions {
		results[i].Ref = resolution.Ref
		attempt, _, outcome := s.currentAttempt(resolution.Ref)
		if outcome != 0 {
			results[i].Outcome = resolutionOutcomeFromAttempt(outcome)
			continue
		}
		sourceInstanceID := strings.TrimSpace(resolution.SourceInstanceID)
		targetInstanceID := strings.TrimSpace(resolution.TargetInstanceID)
		if !validMemoryResolution(resolution.Kind, resolution.RetryDelay) ||
			sourceInstanceID == "" || sourceInstanceID != resolution.SourceInstanceID || len(sourceInstanceID) > store.MaxDeliveryInstanceIDBytes ||
			targetInstanceID == "" || targetInstanceID != resolution.TargetInstanceID || len(targetInstanceID) > store.MaxDeliveryInstanceIDBytes ||
			resolution.TargetUserID != resolution.Ref.StreamID || resolution.BatchID == ([16]byte{}) ||
			resolution.CommandID == ([16]byte{}) {
			results[i].Outcome = store.OutboxResolutionRejected
			continue
		}
		if sourceInstanceID != attempt.sourceInstanceID {
			results[i].Outcome = store.OutboxResolutionFenced
			continue
		}
		target, exists := attempt.targets[memoryTargetKey{
			instance: targetInstanceID, userID: resolution.TargetUserID,
			batch: resolution.BatchID, command: resolution.CommandID,
		}]
		if !attempt.targetsBound || !exists || target.evidence != 0 {
			results[i].Outcome = store.OutboxResolutionRejected
			continue
		}
		results[i].Outcome = applyMemoryResolution(attempt, resolution.Kind, resolution.RetryDelay, resolution.LastError)
	}
	return results, nil
}

func (s *DeliveryOutboxStore) ResolveOwnedAttemptBatch(_ context.Context, resolutions []store.OutboxOwnedAttemptResolution) ([]store.OutboxResolutionResult, error) {
	if len(resolutions) > store.MaxDeliveryBatchItems {
		return nil, fmt.Errorf("resolve absolute delivery owner: batch exceeds %d items", store.MaxDeliveryBatchItems)
	}
	if err := validateMemoryOwnedResolutionBatch(resolutions); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	results := make([]store.OutboxResolutionResult, len(resolutions))
	now := time.Now()
	for i, resolution := range resolutions {
		results[i].Ref = resolution.Ref
		attempt, lane, outcome := s.currentAttempt(resolution.Ref)
		if outcome != 0 {
			results[i].Outcome = resolutionOutcomeFromAttempt(outcome)
			continue
		}
		owner := strings.TrimSpace(resolution.Owner)
		if !validMemoryResolution(resolution.Kind, resolution.RetryDelay) || owner == "" || owner != resolution.Owner || len(owner) > store.MaxDeliveryLeaseOwnerBytes {
			results[i].Outcome = store.OutboxResolutionRejected
			continue
		}
		if attempt.owner != owner || lane.owner != owner || !attempt.leaseUntil.After(now) || !lane.leaseUntil.After(now) {
			results[i].Outcome = store.OutboxResolutionFenced
			continue
		}
		results[i].Outcome = applyMemoryResolution(attempt, resolution.Kind, resolution.RetryDelay, resolution.LastError)
	}
	return results, nil
}

type memoryResolutionDecision struct {
	kind       store.OutboxResolutionKind
	retryDelay time.Duration
}

func validateMemoryTargetResolutionBatch(resolutions []store.OutboxTargetAttemptResolution) error {
	seen := make(map[store.OutboxAttemptRef]memoryResolutionDecision, len(resolutions))
	for i, resolution := range resolutions {
		decision := memoryResolutionDecision{kind: resolution.Kind, retryDelay: resolution.RetryDelay}
		if previous, ok := seen[resolution.Ref]; ok && previous != decision {
			return fmt.Errorf("resolve absolute delivery target: conflicting duplicate at index %d", i)
		}
		seen[resolution.Ref] = decision
	}
	return nil
}

func validateMemoryOwnedResolutionBatch(resolutions []store.OutboxOwnedAttemptResolution) error {
	seen := make(map[store.OutboxAttemptRef]memoryResolutionDecision, len(resolutions))
	for i, resolution := range resolutions {
		decision := memoryResolutionDecision{kind: resolution.Kind, retryDelay: resolution.RetryDelay}
		if previous, ok := seen[resolution.Ref]; ok && previous != decision {
			return fmt.Errorf("resolve absolute delivery owner: conflicting duplicate at index %d", i)
		}
		seen[resolution.Ref] = decision
	}
	return nil
}

func validMemoryResolution(kind store.OutboxResolutionKind, retryDelay time.Duration) bool {
	if kind != store.OutboxResolutionRetry && kind != store.OutboxResolutionAbandoned {
		return false
	}
	return (kind == store.OutboxResolutionRetry) == (retryDelay > 0)
}

func applyMemoryResolution(attempt *memoryOutboxAttempt, kind store.OutboxResolutionKind, retryDelay time.Duration, lastError string) store.OutboxResolutionOutcome {
	if attempt.resolution != 0 {
		if attempt.resolution == kind {
			return store.OutboxResolutionDuplicate
		}
		return store.OutboxResolutionRejected
	}
	attempt.resolution = kind
	retryAt := time.Time{}
	if kind == store.OutboxResolutionRetry {
		retryAt = time.Now().Add(retryDelay)
	}
	if attempt.targetsBound && (retryAt.IsZero() || retryAt.Before(attempt.evidenceDeadline)) {
		retryAt = attempt.evidenceDeadline
	}
	attempt.retryAt = retryAt
	attempt.lastError = lastError
	return store.OutboxResolutionRecorded
}

func (s *DeliveryOutboxStore) FinalizeAttempts(_ context.Context, requests []store.OutboxFinalizeRequest) (store.OutboxFinalizeBatch, error) {
	if len(requests) > store.MaxDeliveryBatchItems {
		return store.OutboxFinalizeBatch{}, fmt.Errorf("finalize absolute delivery attempts: batch exceeds %d items", store.MaxDeliveryBatchItems)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	results := make([]store.OutboxFinalizeResult, len(requests))
	byStream := make(map[int64][]int)
	for i, request := range requests {
		results[i].Ref = request.Ref
		byStream[request.Ref.StreamID] = append(byStream[request.Ref.StreamID], i)
	}
	streams := make([]int64, 0, len(byStream))
	for streamID := range byStream {
		streams = append(streams, streamID)
	}
	sort.Slice(streams, func(i, j int) bool { return streams[i] < streams[j] })
	newly := make(map[memoryAttemptKey]store.OutboxFinalizeOutcome)
	var next []store.OutboxClaimWindow
	for _, streamID := range streams {
		lane := s.lanes[streamID]
		if lane == nil || lane.state != "leased" {
			continue
		}
		requestedCurrent := false
		for _, index := range byStream[streamID] {
			if requests[index].Ref.LeaseFence == lane.fence {
				requestedCurrent = true
				break
			}
		}
		if !requestedCurrent {
			continue
		}
		items := s.streamItems(streamID)
		var current []store.DeliveryOutboxItem
		for _, item := range items {
			if item.ID >= lane.headItemID && item.ID <= lane.windowEnd {
				current = append(current, item)
			}
		}
		consumed := 0
		stopped := false
		stopIndex := -1
		var physicalDuration, clockSkewAllowance time.Duration
		for itemIndex, item := range current {
			attempt := s.attempts[memoryAttemptKey{item.ID, lane.fence}]
			if attempt == nil || attempt.resolution == 0 {
				break
			}
			if physicalDuration == 0 {
				physicalDuration = attempt.commandNotAfter.Sub(attempt.issuedAt)
				clockSkewAllowance = attempt.evidenceDeadline.Sub(attempt.commandNotAfter)
			}
			if !attempt.retryAt.IsZero() && attempt.retryAt.After(time.Now()) {
				break
			}
			key := memoryAttemptKey{item.ID, lane.fence}
			switch attempt.resolution {
			case memoryConfirmedResolution:
				newly[key] = store.OutboxFinalizeApplied
				delete(s.attempts, key)
				delete(s.items, item.ID)
				consumed++
			case store.OutboxResolutionAbandoned:
				newly[key] = store.OutboxFinalizeAbandoned
				delete(s.attempts, key)
				delete(s.items, item.ID)
				consumed++
			case store.OutboxResolutionRetry:
				newly[key] = store.OutboxFinalizeScheduledRetry
				lane.state = "ready"
				lane.readyAt = attempt.retryAt
				lane.owner = ""
				lane.leaseUntil = time.Time{}
				lane.windowEnd = 0
				lane.headItemID = item.ID
				stopped = true
				stopIndex = itemIndex
			}
			if stopped {
				break
			}
		}
		if stopped {
			for _, suffix := range current[stopIndex:] {
				delete(s.attempts, memoryAttemptKey{suffix.ID, lane.fence})
			}
			continue
		}
		remaining := s.streamItems(streamID)
		if len(remaining) == 0 {
			delete(s.lanes, streamID)
			continue
		}
		lane.headItemID = remaining[0].ID
		if consumed < len(current) {
			continue
		}
		lane.headAttempt = 0
		var retain *store.OutboxFinalizeRequest
		for _, index := range byStream[streamID] {
			request := &requests[index]
			if request.RetainLease && request.Owner == lane.owner && request.Ref.LeaseFence == lane.fence {
				retain = request
				break
			}
		}
		if retain == nil {
			lane.state = "ready"
			lane.readyAt = time.Now()
			lane.owner = ""
			lane.leaseUntil = time.Time{}
			lane.windowEnd = 0
			continue
		}
		window, err := s.issueMemoryHandoff(lane, *retain, physicalDuration, clockSkewAllowance)
		if err != nil {
			return store.OutboxFinalizeBatch{}, err
		}
		if len(window.Items) > 0 {
			next = append(next, window)
		}
	}
	for i, request := range requests {
		key := memoryAttemptKey{request.Ref.ItemID, request.Ref.LeaseFence}
		if outcome, ok := newly[key]; ok {
			results[i].Outcome = outcome
			continue
		}
		attempt := s.attempts[key]
		if attempt == nil || attempt.ref != request.Ref {
			results[i].Outcome = store.OutboxFinalizeFenced
		} else if lane := s.lanes[request.Ref.StreamID]; lane != nil && lane.fence == request.Ref.LeaseFence {
			results[i].Outcome = store.OutboxFinalizeWaitingForPredecessor
		} else {
			results[i].Outcome = store.OutboxFinalizeFenced
		}
	}
	return store.OutboxFinalizeBatch{Results: results, Next: next}, nil
}

func (s *DeliveryOutboxStore) NextReadyAt(_ context.Context, kind store.OutboxQueueKind, shardCount int, shardIDs []int) (store.OutboxNextReady, bool, error) {
	if kind != store.OutboxQueueAbsoluteDelivery {
		return store.OutboxNextReady{}, false, fmt.Errorf("next absolute delivery ready at: queue kind %d", kind)
	}
	selected, scoped, err := memorySelectedShards(shardCount, shardIDs)
	if err != nil {
		return store.OutboxNextReady{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	observed := time.Now()
	var earliest time.Time
	recoverEarliest := false
	for _, lane := range s.lanes {
		if scoped {
			if _, ok := selected[int(lane.streamID%store.DispatchOutboxLogicalShards)]; !ok {
				continue
			}
		}
		candidate := lane.readyAt
		if lane.state == "leased" {
			candidate = lane.leaseUntil
			if attempt := s.attempts[memoryAttemptKey{itemID: lane.headItemID, fence: lane.fence}]; attempt != nil {
				if attempt.resolution != 0 {
					if attempt.retryAt.IsZero() || !attempt.retryAt.After(observed) {
						continue
					}
					if attempt.retryAt.Before(candidate) {
						candidate = attempt.retryAt
					}
				} else if !attempt.evidenceDeadline.IsZero() && attempt.evidenceDeadline.Before(candidate) {
					candidate = attempt.evidenceDeadline
				}
			}
		} else if lane.state != "ready" {
			continue
		}
		recoverLane := lane.state == "leased"
		if earliest.IsZero() || candidate.Before(earliest) || (candidate.Equal(earliest) && recoverLane) {
			earliest = candidate
			recoverEarliest = recoverLane
		}
	}
	if earliest.IsZero() {
		return store.OutboxNextReady{}, false, nil
	}
	readyKind := store.OutboxReadyClaim
	if recoverEarliest {
		readyKind = store.OutboxReadyRecoverLease
	}
	return store.OutboxNextReady{ObservedAt: observed, ReadyAt: earliest, Kind: readyKind}, true, nil
}

func (s *DeliveryOutboxStore) Snapshot() []store.DeliveryOutboxItem {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := make([]store.DeliveryOutboxItem, 0, len(s.items))
	for _, item := range s.items {
		items = append(items, cloneDeliveryOutboxItem(item))
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return items
}

func (s *DeliveryOutboxStore) currentAttempt(ref store.OutboxAttemptRef) (*memoryOutboxAttempt, *memoryOutboxLane, store.OutboxFinalizeOutcome) {
	if ref.QueueKind != store.OutboxQueueAbsoluteDelivery || ref.StreamID <= 0 || ref.ItemID <= 0 ||
		ref.Sequence != 0 || ref.LeaseFence == 0 || ref.Attempt <= 0 {
		return nil, nil, store.OutboxFinalizeRejected
	}
	attempt := s.attempts[memoryAttemptKey{ref.ItemID, ref.LeaseFence}]
	if attempt == nil || attempt.ref != ref {
		return nil, nil, store.OutboxFinalizeFenced
	}
	lane := s.lanes[ref.StreamID]
	if lane == nil || lane.state != "leased" || lane.fence != ref.LeaseFence {
		return attempt, lane, store.OutboxFinalizeFenced
	}
	return attempt, lane, 0
}

func (s *DeliveryOutboxStore) hasPendingFinalization(lane *memoryOutboxLane) bool {
	for key, attempt := range s.attempts {
		if key.fence == lane.fence && attempt.ref.StreamID == lane.streamID &&
			attempt.resolution != 0 {
			return true
		}
	}
	return false
}

func (s *DeliveryOutboxStore) hasBoundRecovery(lane *memoryOutboxLane) bool {
	for key, attempt := range s.attempts {
		if key.fence == lane.fence && attempt.ref.StreamID == lane.streamID &&
			attempt.ref.ItemID == lane.headItemID &&
			attempt.targetsBound && attempt.resolution == 0 {
			return true
		}
	}
	return false
}

func (s *DeliveryOutboxStore) streamItems(streamID int64) []store.DeliveryOutboxItem {
	items := make([]store.DeliveryOutboxItem, 0)
	for _, item := range s.items {
		if item.TargetUserID == streamID {
			items = append(items, item)
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return items
}

func (s *DeliveryOutboxStore) issueMemoryHandoff(lane *memoryOutboxLane, request store.OutboxFinalizeRequest, physicalDuration, clockSkewAllowance time.Duration) (store.OutboxClaimWindow, error) {
	if request.WindowSize <= 0 {
		request.WindowSize = 16
	}
	if request.WindowByteLimit <= 0 {
		request.WindowByteLimit = 1 << 20
	}
	if request.WindowByteLimit > store.MaxDeliveryBatchBytes {
		request.WindowByteLimit = store.MaxDeliveryBatchBytes
	}
	if request.LeaseDuration <= 0 {
		request.LeaseDuration = 30 * time.Second
	}
	if physicalDuration <= 0 || clockSkewAllowance <= 0 ||
		request.LeaseDuration <= memoryOutboxLeaseSafetyMargin ||
		physicalDuration >= request.LeaseDuration-memoryOutboxLeaseSafetyMargin ||
		clockSkewAllowance >= request.LeaseDuration-memoryOutboxLeaseSafetyMargin-physicalDuration {
		return store.OutboxClaimWindow{}, fmt.Errorf("issue absolute delivery handoff: persisted deadline intervals exceed retained lease")
	}
	items := s.streamItems(lane.streamID)
	window := store.OutboxClaimWindow{
		QueueKind: store.OutboxQueueAbsoluteDelivery, StreamID: lane.streamID,
		Owner: lane.owner, LeaseFence: lane.fence,
	}
	now := time.Now()
	leaseUntil := now.Add(request.LeaseDuration)
	commandNotAfter := now.Add(physicalDuration)
	evidenceDeadline := commandNotAfter.Add(clockSkewAllowance)
	bytesUsed := 0
	lane.headAttempt = 1
	for _, item := range items {
		if len(window.Items) >= request.WindowSize {
			break
		}
		if len(window.Items) > 0 {
			head := window.Items[0]
			if item.ExcludeAuthKeyID != head.ExcludeAuthKeyID || item.ExcludeSessionID != head.ExcludeSessionID {
				break
			}
			if bytesUsed+len(item.Payload) > request.WindowByteLimit {
				break
			}
		}
		bytesUsed += len(item.Payload)
		attemptNo := lane.headAttempt
		ref := store.OutboxAttemptRef{
			QueueKind: store.OutboxQueueAbsoluteDelivery, StreamID: item.TargetUserID,
			ItemID: item.ID, LeaseFence: lane.fence, Attempt: attemptNo,
		}
		s.attempts[memoryAttemptKey{item.ID, lane.fence}] = &memoryOutboxAttempt{
			ref: ref, owner: lane.owner, issuedAt: now, leaseUntil: leaseUntil,
			commandNotAfter: commandNotAfter, evidenceDeadline: evidenceDeadline,
			ordinal: len(window.Items),
		}
		window.LeaseUntil = leaseUntil
		claimed := claimedAbsoluteItem(item, ref)
		claimed.CommandNotAfter = commandNotAfter
		claimed.EvidenceDeadline = evidenceDeadline
		window.Items = append(window.Items, claimed)
		lane.windowEnd = item.ID
	}
	if len(window.Items) == 0 {
		lane.state = "ready"
		lane.readyAt = time.Now()
		lane.owner = ""
		lane.leaseUntil = time.Time{}
		lane.windowEnd = 0
		return window, nil
	}
	lane.leaseUntil = window.LeaseUntil
	return window, nil
}

func claimedAbsoluteItem(item store.DeliveryOutboxItem, ref store.OutboxAttemptRef) store.OutboxClaimedItem {
	return store.OutboxClaimedItem{
		Ref: ref, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		ExcludeAuthKeyID: item.ExcludeAuthKeyID, ExcludeSessionID: item.ExcludeSessionID,
		Payload: store.AbsoluteDeliveryPayload{TL: append([]byte(nil), item.Payload...)},
	}
}

func cloneDeliveryOutboxItem(item store.DeliveryOutboxItem) store.DeliveryOutboxItem {
	item.Payload = append([]byte(nil), item.Payload...)
	return item
}

func sameMemoryTargetSet(left, right map[memoryTargetKey]*memoryAttemptTarget) bool {
	if len(left) != len(right) {
		return false
	}
	for key := range left {
		if _, ok := right[key]; !ok {
			return false
		}
	}
	return true
}

func memorySelectedShards(count int, ids []int) (map[int]struct{}, bool, error) {
	if count == 0 && len(ids) == 0 {
		return nil, false, nil
	}
	if count != store.DispatchOutboxLogicalShards {
		return nil, true, fmt.Errorf("shard count %d, want stable %d", count, store.DispatchOutboxLogicalShards)
	}
	selected := make(map[int]struct{}, len(ids))
	for _, id := range ids {
		if id >= 0 && id < count {
			selected[id] = struct{}{}
		}
	}
	return selected, true, nil
}

func bindOutcomeFromAttempt(outcome store.OutboxFinalizeOutcome) store.OutboxBindTargetOutcome {
	switch outcome {
	case store.OutboxFinalizeFenced:
		return store.OutboxBindTargetFenced
	default:
		return store.OutboxBindTargetRejected
	}
}

func evidenceOutcomeFromAttempt(outcome store.OutboxFinalizeOutcome) store.OutboxEvidenceOutcome {
	switch outcome {
	case store.OutboxFinalizeFenced:
		return store.OutboxEvidenceFenced
	default:
		return store.OutboxEvidenceRejected
	}
}

func resolutionOutcomeFromAttempt(outcome store.OutboxFinalizeOutcome) store.OutboxResolutionOutcome {
	switch outcome {
	case store.OutboxFinalizeFenced:
		return store.OutboxResolutionFenced
	default:
		return store.OutboxResolutionRejected
	}
}
