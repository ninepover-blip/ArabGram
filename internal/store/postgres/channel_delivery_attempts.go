package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/store"
)

type channelDeliveryAttemptState struct {
	channelID                int64
	minPTS                   int
	maxPTS                   int
	attempt                  int
	targetsBound             bool
	targetCount              *int
	deliverySourceInstanceID *string
	attemptOwner             string
	attemptLeaseUntil        time.Time
	attemptLeaseLive         bool
	commandNotAfter          time.Time
	evidenceDeadline         time.Time
	evidenceLive             bool
	resolution               string
	laneFence                *int64
	laneState                *string
	laneOwner                *string
	laneLeaseUntil           *time.Time
	laneLeaseLive            bool
}

func loadChannelDeliveryAttemptForUpdate(ctx context.Context, tx pgx.Tx, ref store.OutboxAttemptRef) (channelDeliveryAttemptState, error) {
	var out channelDeliveryAttemptState
	err := tx.QueryRow(ctx, `
WITH db_clock AS MATERIALIZED (
  SELECT clock_timestamp() AS observed_now
)
SELECT a.channel_id, a.min_pts, a.max_pts, a.attempt_no,
       a.targets_bound, a.target_count, a.delivery_source_instance_id,
       a.owner, a.lease_until, a.lease_until > c.observed_now,
       a.command_not_after, a.evidence_deadline, a.evidence_deadline > c.observed_now,
       a.resolution,
       l.lease_fence, l.state, l.lease_owner, l.lease_until,
       COALESCE(l.lease_until > c.observed_now, false)
FROM channel_delivery_attempts a
LEFT JOIN channel_delivery_lanes l ON l.channel_id = a.channel_id
CROSS JOIN db_clock c
WHERE a.item_id = $1 AND a.lease_fence = $2
FOR UPDATE OF a`, ref.ItemID, int64(ref.LeaseFence)).Scan(
		&out.channelID,
		&out.minPTS,
		&out.maxPTS,
		&out.attempt,
		&out.targetsBound,
		&out.targetCount,
		&out.deliverySourceInstanceID,
		&out.attemptOwner,
		&out.attemptLeaseUntil,
		&out.attemptLeaseLive,
		&out.commandNotAfter,
		&out.evidenceDeadline,
		&out.evidenceLive,
		&out.resolution,
		&out.laneFence,
		&out.laneState,
		&out.laneOwner,
		&out.laneLeaseUntil,
		&out.laneLeaseLive,
	)
	return out, err
}

func exactChannelAttemptRef(ref store.OutboxAttemptRef, state channelDeliveryAttemptState) bool {
	return state.channelID == ref.StreamID && int64(state.maxPTS) == ref.Sequence && state.attempt == ref.Attempt
}

func channelAttemptIsCurrent(ref store.OutboxAttemptRef, state channelDeliveryAttemptState) bool {
	return state.laneFence != nil && *state.laneFence == int64(ref.LeaseFence) && state.laneState != nil && *state.laneState == "leased"
}

func (s *ChannelDeliveryStore) BindAttemptTargets(ctx context.Context, sets []store.OutboxAttemptTargetSet) ([]store.OutboxBindTargetResult, error) {
	if len(sets) > store.MaxDeliveryBatchItems {
		return nil, fmt.Errorf("bind channel attempt targets: batch exceeds %d items", store.MaxDeliveryBatchItems)
	}
	results := make([]store.OutboxBindTargetResult, len(sets))
	indices := make([]int, 0, len(sets))
	for i := range sets {
		results[i].Ref = sets[i].Ref
		results[i].Outcome = store.OutboxBindTargetRejected
		if err := validateChannelAttemptRef(sets[i].Ref); err != nil {
			return nil, fmt.Errorf("bind channel attempt targets: invalid ref at index %d: %w", i, err)
		}
		if strings.TrimSpace(sets[i].SourceInstanceID) == "" ||
			strings.TrimSpace(sets[i].SourceInstanceID) != sets[i].SourceInstanceID ||
			len(sets[i].SourceInstanceID) > store.MaxDeliveryInstanceIDBytes {
			return nil, fmt.Errorf("bind channel attempt targets: source instance is required at index %d", i)
		}
		if err := validateChannelAttemptTargets(sets[i].Targets); err != nil {
			return nil, fmt.Errorf("bind channel attempt targets: invalid target set at index %d: %w", i, err)
		}
		indices = append(indices, i)
	}
	if len(indices) == 0 {
		return results, nil
	}
	sort.SliceStable(indices, func(i, j int) bool {
		a, b := sets[indices[i]].Ref, sets[indices[j]].Ref
		if a.StreamID != b.StreamID {
			return a.StreamID < b.StreamID
		}
		if a.ItemID != b.ItemID {
			return a.ItemID < b.ItemID
		}
		return a.LeaseFence < b.LeaseFence
	})

	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, idx := range indices {
		set := sets[idx]
		state, err := loadChannelDeliveryAttemptForUpdate(ctx, tx, set.Ref)
		if errors.Is(err, pgx.ErrNoRows) {
			results[idx].Outcome = store.OutboxBindTargetFenced
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("bind channel attempt targets: load attempt: %w", err)
		}
		if !exactChannelAttemptRef(set.Ref, state) {
			results[idx].Outcome = store.OutboxBindTargetRejected
			continue
		}
		if !channelAttemptIsCurrent(set.Ref, state) {
			results[idx].Outcome = store.OutboxBindTargetFenced
			continue
		}
		if state.targetsBound {
			equal := state.deliverySourceInstanceID != nil && *state.deliverySourceInstanceID == set.SourceInstanceID
			if equal {
				equal, err = channelAttemptTargetsEqual(ctx, tx, set.Ref, set.Targets)
			}
			if equal && len(set.Targets) == 0 && state.resolution != "confirmed" {
				equal = false
			}
			if err != nil {
				return nil, err
			}
			if equal {
				results[idx].Outcome = store.OutboxBindTargetDuplicate
			} else {
				results[idx].Outcome = store.OutboxBindTargetRejected
			}
			continue
		}
		if state.laneLeaseUntil == nil || state.commandNotAfter.IsZero() || state.evidenceDeadline.IsZero() ||
			!state.commandNotAfter.Before(state.evidenceDeadline) ||
			!state.evidenceDeadline.Before(state.attemptLeaseUntil) ||
			!state.evidenceDeadline.Before(*state.laneLeaseUntil) {
			results[idx].Outcome = store.OutboxBindTargetRejected
			continue
		}
		if len(set.Targets) > 0 {
			instances := make([]string, len(set.Targets))
			users := make([]int64, len(set.Targets))
			batches := make([][]byte, len(set.Targets))
			commands := make([][]byte, len(set.Targets))
			for i := range set.Targets {
				instances[i] = set.Targets[i].TargetInstanceID
				users[i] = set.Targets[i].TargetUserID
				batches[i] = append([]byte(nil), set.Targets[i].BatchID[:]...)
				commands[i] = append([]byte(nil), set.Targets[i].CommandID[:]...)
			}
			if _, err := tx.Exec(ctx, `
INSERT INTO channel_delivery_attempt_targets (
    item_id, lease_fence, target_instance_id, target_user_id, batch_id, command_id
)
SELECT $1, $2, u.target_instance_id, u.target_user_id, u.batch_id, u.command_id
FROM unnest($3::text[], $4::bigint[], $5::bytea[], $6::bytea[])
     AS u(target_instance_id, target_user_id, batch_id, command_id)`,
				set.Ref.ItemID,
				int64(set.Ref.LeaseFence),
				instances,
				users,
				batches,
				commands,
			); err != nil {
				return nil, fmt.Errorf("bind channel attempt targets: insert target set: %w", err)
			}
		}
		if _, err := tx.Exec(ctx, `
UPDATE channel_delivery_attempts
SET targets_bound = true,
    target_count = $3,
    delivery_source_instance_id = $4,
    empty_evidence_kind = CASE WHEN $3 = 0 THEN 'authoritative_no_targets' ELSE NULL END,
    evidence_at = CASE WHEN $3 = 0 THEN clock_timestamp() ELSE NULL END,
    resolution = CASE WHEN $3 = 0 THEN 'confirmed' ELSE 'pending' END,
    resolved_at = CASE WHEN $3 = 0 THEN clock_timestamp() ELSE NULL END
WHERE item_id = $1 AND lease_fence = $2`,
			set.Ref.ItemID,
			int64(set.Ref.LeaseFence),
			len(set.Targets),
			set.SourceInstanceID,
		); err != nil {
			return nil, fmt.Errorf("bind channel attempt targets: freeze target count: %w", err)
		}
		results[idx].Outcome = store.OutboxBindTargetBound
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("bind channel attempt targets: commit: %w", err)
	}
	return results, nil
}

func validateChannelAttemptTargets(targets []store.OutboxAttemptTarget) error {
	if len(targets) > store.MaxDeliveryTargets {
		return fmt.Errorf("channel target count exceeds %d", store.MaxDeliveryTargets)
	}
	type identity struct {
		instance string
		userID   int64
	}
	seen := make(map[identity]struct{}, len(targets))
	for _, target := range targets {
		if strings.TrimSpace(target.TargetInstanceID) == "" || strings.TrimSpace(target.TargetInstanceID) != target.TargetInstanceID || len(target.TargetInstanceID) > store.MaxDeliveryInstanceIDBytes {
			return errors.New("invalid channel target instance")
		}
		if target.TargetUserID != 0 {
			return errors.New("channel target user must be zero")
		}
		if target.BatchID == ([16]byte{}) || target.CommandID == ([16]byte{}) {
			return errors.New("channel target batch and command identities are required")
		}
		key := identity{instance: target.TargetInstanceID, userID: target.TargetUserID}
		if _, ok := seen[key]; ok {
			return errors.New("duplicate channel target command")
		}
		seen[key] = struct{}{}
	}
	return nil
}

func channelAttemptTargetsEqual(ctx context.Context, tx pgx.Tx, ref store.OutboxAttemptRef, targets []store.OutboxAttemptTarget) (bool, error) {
	rows, err := tx.Query(ctx, `
SELECT target_instance_id, target_user_id, batch_id, command_id
FROM channel_delivery_attempt_targets
WHERE item_id = $1 AND lease_fence = $2
ORDER BY target_instance_id, target_user_id, batch_id, command_id`, ref.ItemID, int64(ref.LeaseFence))
	if err != nil {
		return false, fmt.Errorf("compare channel attempt targets: %w", err)
	}
	defer rows.Close()
	actual := make([]store.OutboxAttemptTarget, 0, len(targets))
	for rows.Next() {
		var target store.OutboxAttemptTarget
		var batch, command []byte
		if err := rows.Scan(&target.TargetInstanceID, &target.TargetUserID, &batch, &command); err != nil {
			return false, fmt.Errorf("scan channel attempt target: %w", err)
		}
		if len(batch) != len(target.BatchID) || len(command) != len(target.CommandID) {
			return false, nil
		}
		copy(target.BatchID[:], batch)
		copy(target.CommandID[:], command)
		actual = append(actual, target)
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate channel attempt targets: %w", err)
	}
	expected := append([]store.OutboxAttemptTarget(nil), targets...)
	sortChannelAttemptTargets(expected)
	sortChannelAttemptTargets(actual)
	if len(actual) != len(expected) {
		return false, nil
	}
	for i := range actual {
		if actual[i] != expected[i] {
			return false, nil
		}
	}
	return true, nil
}

func sortChannelAttemptTargets(targets []store.OutboxAttemptTarget) {
	sort.Slice(targets, func(i, j int) bool {
		if targets[i].TargetInstanceID != targets[j].TargetInstanceID {
			return targets[i].TargetInstanceID < targets[j].TargetInstanceID
		}
		if targets[i].TargetUserID != targets[j].TargetUserID {
			return targets[i].TargetUserID < targets[j].TargetUserID
		}
		if cmp := bytes.Compare(targets[i].BatchID[:], targets[j].BatchID[:]); cmp != 0 {
			return cmp < 0
		}
		return bytes.Compare(targets[i].CommandID[:], targets[j].CommandID[:]) < 0
	})
}

func (s *ChannelDeliveryStore) RecordAttemptEvidenceBatch(ctx context.Context, evidence []store.OutboxAttemptEvidence) ([]store.OutboxEvidenceResult, error) {
	if len(evidence) > store.MaxDeliveryBatchItems {
		return nil, fmt.Errorf("record channel attempt evidence: batch exceeds %d items", store.MaxDeliveryBatchItems)
	}
	results := make([]store.OutboxEvidenceResult, len(evidence))
	indices := make([]int, 0, len(evidence))
	type completionKey struct {
		ref              store.OutboxAttemptRef
		targetInstanceID string
		targetUserID     int64
		batchID          [16]byte
		commandID        [16]byte
	}
	seen := make(map[completionKey]store.OutboxAttemptEvidence, len(evidence))
	for i := range evidence {
		results[i] = store.OutboxEvidenceResult{Ref: evidence[i].Ref, Outcome: store.OutboxEvidenceRejected}
		if err := validateChannelEvidence(evidence[i]); err != nil {
			return nil, fmt.Errorf("record channel evidence: invalid evidence at index %d: %w", i, err)
		}
		key := completionKey{
			ref: evidence[i].Ref, targetInstanceID: evidence[i].TargetInstanceID,
			targetUserID: evidence[i].TargetUserID, batchID: evidence[i].BatchID,
			commandID: evidence[i].CommandID,
		}
		if previous, ok := seen[key]; ok && !sameOutboxCompletionEvidence(previous, evidence[i]) {
			return nil, fmt.Errorf("record channel evidence: conflicting duplicate at index %d", i)
		}
		seen[key] = evidence[i]
		indices = append(indices, i)
	}
	if len(indices) == 0 {
		return results, nil
	}
	sort.SliceStable(indices, func(i, j int) bool {
		a, b := evidence[indices[i]].Ref, evidence[indices[j]].Ref
		if a.StreamID != b.StreamID {
			return a.StreamID < b.StreamID
		}
		if a.ItemID != b.ItemID {
			return a.ItemID < b.ItemID
		}
		return a.LeaseFence < b.LeaseFence
	})
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, idx := range indices {
		ev := evidence[idx]
		state, err := loadChannelDeliveryAttemptForUpdate(ctx, tx, ev.Ref)
		if errors.Is(err, pgx.ErrNoRows) {
			results[idx].Outcome = store.OutboxEvidenceFenced
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("record channel evidence: load attempt: %w", err)
		}
		if !exactChannelAttemptRef(ev.Ref, state) {
			results[idx].Outcome = store.OutboxEvidenceRejected
			continue
		}
		if !channelAttemptIsCurrent(ev.Ref, state) {
			results[idx].Outcome = store.OutboxEvidenceFenced
			continue
		}
		if state.deliverySourceInstanceID == nil || *state.deliverySourceInstanceID != ev.SourceInstanceID {
			results[idx].Outcome = store.OutboxEvidenceFenced
			continue
		}
		// Physical completion authority is the database receipt deadline. The
		// Edge timestamp is diagnostic only and must never admit a late receipt.
		if !state.evidenceLive {
			results[idx].Outcome = store.OutboxEvidenceFenced
			continue
		}
		observedAt := any(nil)
		if !ev.ObservedAt.IsZero() {
			observedAt = ev.ObservedAt
		}
		if ev.Kind == store.OutboxEvidenceAuthoritativeNoTargets {
			if !state.targetsBound || state.targetCount == nil || *state.targetCount != 0 {
				results[idx].Outcome = store.OutboxEvidenceRejected
				continue
			}
			if state.resolution == "confirmed" {
				results[idx].Outcome = store.OutboxEvidenceDuplicate
				continue
			}
			if state.resolution != "pending" {
				results[idx].Outcome = store.OutboxEvidenceRejected
				continue
			}
			commandTag, err := tx.Exec(ctx, `
UPDATE channel_delivery_attempts
SET empty_evidence_kind = 'authoritative_no_targets',
    evidence_at = COALESCE($3::timestamptz, clock_timestamp()),
    resolution = 'confirmed',
    resolved_at = COALESCE($3::timestamptz, clock_timestamp())
WHERE item_id = $1 AND lease_fence = $2
  AND resolution = 'pending'
  AND evidence_deadline > clock_timestamp()`, ev.Ref.ItemID, int64(ev.Ref.LeaseFence), observedAt)
			if err != nil {
				return nil, fmt.Errorf("record channel no-target evidence: %w", err)
			}
			if commandTag.RowsAffected() != 1 {
				results[idx].Outcome = store.OutboxEvidenceFenced
				continue
			}
			results[idx].Outcome = store.OutboxEvidenceRecorded
			continue
		}
		status, evidenceKind := channelEvidenceStatus(ev.Kind)
		commandTag, err := tx.Exec(ctx, `
UPDATE channel_delivery_attempt_targets
SET status = $7,
    admitted_sessions = $8,
    written_sessions = $9,
    evidence_at = COALESCE($10::timestamptz, clock_timestamp()),
    evidence_kind = $11,
    physical_first_server_msg_id = $12,
    updated_at = clock_timestamp()
WHERE item_id = $1 AND lease_fence = $2
  AND target_instance_id = $3 AND target_user_id = $4
  AND batch_id = $5 AND command_id = $6
  AND status IN ('pending', 'admitted')
  AND EXISTS (
    SELECT 1
    FROM channel_delivery_attempts a
    WHERE a.item_id = $1 AND a.lease_fence = $2
      AND a.resolution = 'pending'
      AND a.evidence_deadline > clock_timestamp()
  )`,
			ev.Ref.ItemID,
			int64(ev.Ref.LeaseFence),
			ev.TargetInstanceID,
			ev.TargetUserID,
			ev.BatchID[:],
			ev.CommandID[:],
			status,
			ev.EligibleSessions,
			ev.WrittenSessions,
			observedAt,
			evidenceKind,
			ev.ServerMsgID,
		)
		if err != nil {
			return nil, fmt.Errorf("record channel target evidence: %w", err)
		}
		if commandTag.RowsAffected() == 0 {
			var existingStatus string
			var existingEvidenceKind *string
			var eligibleSessions, writtenSessions int
			var physicalFirstServerMsgID int64
			var evidenceLive bool
			err := tx.QueryRow(ctx, `
SELECT t.status, t.evidence_kind, t.admitted_sessions, t.written_sessions,
       t.physical_first_server_msg_id, a.evidence_deadline > clock_timestamp()
FROM channel_delivery_attempt_targets
  AS t
JOIN channel_delivery_attempts a
  ON a.item_id = t.item_id AND a.lease_fence = t.lease_fence
WHERE t.item_id = $1 AND t.lease_fence = $2
  AND t.target_instance_id = $3 AND t.target_user_id = $4
  AND t.batch_id = $5 AND t.command_id = $6`,
				ev.Ref.ItemID,
				int64(ev.Ref.LeaseFence),
				ev.TargetInstanceID,
				ev.TargetUserID,
				ev.BatchID[:],
				ev.CommandID[:],
			).Scan(&existingStatus, &existingEvidenceKind, &eligibleSessions, &writtenSessions, &physicalFirstServerMsgID, &evidenceLive)
			if errors.Is(err, pgx.ErrNoRows) {
				results[idx].Outcome = store.OutboxEvidenceRejected
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("classify channel target evidence: %w", err)
			}
			if !evidenceLive {
				results[idx].Outcome = store.OutboxEvidenceFenced
				continue
			}
			if existingStatus == status && existingEvidenceKind != nil && *existingEvidenceKind == evidenceKind &&
				eligibleSessions == ev.EligibleSessions && writtenSessions == ev.WrittenSessions &&
				physicalFirstServerMsgID == ev.ServerMsgID {
				results[idx].Outcome = store.OutboxEvidenceDuplicate
			} else {
				results[idx].Outcome = store.OutboxEvidenceRejected
			}
			continue
		}
		if _, err := tx.Exec(ctx, `
UPDATE channel_delivery_attempts a
SET resolution = 'confirmed', evidence_at = clock_timestamp(), resolved_at = clock_timestamp()
WHERE a.item_id = $1 AND a.lease_fence = $2
  AND a.resolution = 'pending'
  AND a.targets_bound AND a.target_count > 0
  AND a.target_count = (
      SELECT count(*) FROM channel_delivery_attempt_targets t
      WHERE t.item_id = a.item_id AND t.lease_fence = a.lease_fence
  )
  AND a.target_count = (
      SELECT count(*) FROM channel_delivery_attempt_targets t
      WHERE t.item_id = a.item_id AND t.lease_fence = a.lease_fence
        AND t.status IN ('written', 'no_eligible')
  )`, ev.Ref.ItemID, int64(ev.Ref.LeaseFence)); err != nil {
			return nil, fmt.Errorf("confirm channel delivery attempt: %w", err)
		}
		results[idx].Outcome = store.OutboxEvidenceRecorded
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("record channel attempt evidence: commit: %w", err)
	}
	return results, nil
}

func validateChannelEvidence(ev store.OutboxAttemptEvidence) error {
	if err := validateChannelAttemptRef(ev.Ref); err != nil {
		return err
	}
	if strings.TrimSpace(ev.SourceInstanceID) == "" || strings.TrimSpace(ev.SourceInstanceID) != ev.SourceInstanceID || len(ev.SourceInstanceID) > store.MaxDeliveryInstanceIDBytes {
		return errors.New("channel evidence requires exact source instance identity")
	}
	if ev.ObservedAt.IsZero() {
		return errors.New("channel evidence requires observed_at")
	}
	switch ev.Kind {
	case store.OutboxEvidenceAuthoritativeNoTargets:
		if ev.TargetInstanceID != "" || ev.TargetUserID != 0 || ev.BatchID != ([16]byte{}) || ev.CommandID != ([16]byte{}) || ev.EligibleSessions != 0 || ev.WrittenSessions != 0 || ev.ServerMsgID != 0 {
			return errors.New("authoritative no-target evidence must not name a command")
		}
	case store.OutboxEvidenceEdgeWritten, store.OutboxEvidenceEdgeNoEligible:
		if strings.TrimSpace(ev.TargetInstanceID) == "" || strings.TrimSpace(ev.TargetInstanceID) != ev.TargetInstanceID || len(ev.TargetInstanceID) > store.MaxDeliveryInstanceIDBytes || ev.TargetUserID != 0 || ev.BatchID == ([16]byte{}) || ev.CommandID == ([16]byte{}) {
			return errors.New("channel edge evidence requires exact target command identity")
		}
		if ev.Kind == store.OutboxEvidenceEdgeWritten && (ev.EligibleSessions <= 0 || ev.WrittenSessions != ev.EligibleSessions || ev.ServerMsgID <= 0) {
			return errors.New("channel written evidence requires all eligible sessions written")
		}
		if ev.Kind == store.OutboxEvidenceEdgeNoEligible && (ev.EligibleSessions != 0 || ev.WrittenSessions != 0 || ev.ServerMsgID != 0) {
			return errors.New("channel no-eligible evidence cannot report sessions")
		}
	default:
		return errors.New("unknown channel evidence kind")
	}
	return nil
}

func channelEvidenceStatus(kind store.OutboxEvidenceKind) (string, string) {
	switch kind {
	case store.OutboxEvidenceEdgeNoEligible:
		return "no_eligible", "edge_no_eligible"
	default:
		return "written", "edge_written"
	}
}

type channelAuthorizedResolution struct {
	ref              store.OutboxAttemptRef
	owner            string
	sourceInstanceID string
	target           store.OutboxAttemptTarget
	kind             store.OutboxResolutionKind
	retryDelay       time.Duration
	lastError        string
	targetAuthority  bool
}

func (s *ChannelDeliveryStore) ResolveTargetAttemptBatch(ctx context.Context, resolutions []store.OutboxTargetAttemptResolution) ([]store.OutboxResolutionResult, error) {
	if len(resolutions) > store.MaxDeliveryBatchItems {
		return nil, fmt.Errorf("resolve channel target attempt: batch exceeds %d items", store.MaxDeliveryBatchItems)
	}
	authorized := make([]channelAuthorizedResolution, len(resolutions))
	decisions := make(map[store.OutboxAttemptRef]channelResolutionDecision, len(resolutions))
	for i, resolution := range resolutions {
		if err := validateChannelResolutionKind(resolution.Ref, resolution.Kind, resolution.RetryDelay); err != nil {
			return nil, fmt.Errorf("resolve channel target attempt: invalid resolution at index %d: %w", i, err)
		}
		if strings.TrimSpace(resolution.SourceInstanceID) == "" || strings.TrimSpace(resolution.SourceInstanceID) != resolution.SourceInstanceID || len(resolution.SourceInstanceID) > store.MaxDeliveryInstanceIDBytes ||
			strings.TrimSpace(resolution.TargetInstanceID) == "" || strings.TrimSpace(resolution.TargetInstanceID) != resolution.TargetInstanceID || len(resolution.TargetInstanceID) > store.MaxDeliveryInstanceIDBytes ||
			resolution.TargetUserID != 0 || resolution.BatchID == ([16]byte{}) || resolution.CommandID == ([16]byte{}) {
			return nil, fmt.Errorf("resolve channel target attempt: invalid exact target identity at index %d", i)
		}
		decision := channelResolutionDecision{kind: resolution.Kind, retryDelay: resolution.RetryDelay}
		if previous, ok := decisions[resolution.Ref]; ok && previous != decision {
			return nil, fmt.Errorf("resolve channel target attempt: conflicting duplicate at index %d", i)
		}
		decisions[resolution.Ref] = decision
		authorized[i] = channelAuthorizedResolution{
			ref: resolution.Ref, sourceInstanceID: resolution.SourceInstanceID,
			target: store.OutboxAttemptTarget{
				TargetInstanceID: resolution.TargetInstanceID, TargetUserID: resolution.TargetUserID,
				BatchID: resolution.BatchID, CommandID: resolution.CommandID,
			},
			kind: resolution.Kind, retryDelay: resolution.RetryDelay, lastError: resolution.LastError, targetAuthority: true,
		}
	}
	return s.resolveAuthorizedChannelAttempts(ctx, authorized)
}

func (s *ChannelDeliveryStore) ResolveOwnedAttemptBatch(ctx context.Context, resolutions []store.OutboxOwnedAttemptResolution) ([]store.OutboxResolutionResult, error) {
	if len(resolutions) > store.MaxDeliveryBatchItems {
		return nil, fmt.Errorf("resolve channel owned attempt: batch exceeds %d items", store.MaxDeliveryBatchItems)
	}
	authorized := make([]channelAuthorizedResolution, len(resolutions))
	decisions := make(map[store.OutboxAttemptRef]channelResolutionDecision, len(resolutions))
	for i, resolution := range resolutions {
		if err := validateChannelResolutionKind(resolution.Ref, resolution.Kind, resolution.RetryDelay); err != nil {
			return nil, fmt.Errorf("resolve channel owned attempt: invalid resolution at index %d: %w", i, err)
		}
		if strings.TrimSpace(resolution.Owner) == "" || strings.TrimSpace(resolution.Owner) != resolution.Owner || len(resolution.Owner) > store.MaxDeliveryLeaseOwnerBytes {
			return nil, fmt.Errorf("resolve channel owned attempt: owner is required at index %d", i)
		}
		decision := channelResolutionDecision{kind: resolution.Kind, retryDelay: resolution.RetryDelay}
		if previous, ok := decisions[resolution.Ref]; ok && previous != decision {
			return nil, fmt.Errorf("resolve channel owned attempt: conflicting duplicate at index %d", i)
		}
		decisions[resolution.Ref] = decision
		authorized[i] = channelAuthorizedResolution{
			ref: resolution.Ref, owner: resolution.Owner, kind: resolution.Kind,
			retryDelay: resolution.RetryDelay, lastError: resolution.LastError,
		}
	}
	return s.resolveAuthorizedChannelAttempts(ctx, authorized)
}

type channelResolutionDecision struct {
	kind       store.OutboxResolutionKind
	retryDelay time.Duration
}

func (s *ChannelDeliveryStore) resolveAuthorizedChannelAttempts(ctx context.Context, resolutions []channelAuthorizedResolution) ([]store.OutboxResolutionResult, error) {
	results := make([]store.OutboxResolutionResult, len(resolutions))
	indices := make([]int, len(resolutions))
	for i := range resolutions {
		indices[i] = i
		results[i] = store.OutboxResolutionResult{Ref: resolutions[i].ref, Outcome: store.OutboxResolutionRejected}
	}
	sort.SliceStable(indices, func(i, j int) bool {
		a, b := resolutions[indices[i]].ref, resolutions[indices[j]].ref
		if a.StreamID != b.StreamID {
			return a.StreamID < b.StreamID
		}
		if a.ItemID != b.ItemID {
			return a.ItemID < b.ItemID
		}
		return a.LeaseFence < b.LeaseFence
	})
	tx, err := s.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, idx := range indices {
		resolution := resolutions[idx]
		state, err := loadChannelDeliveryAttemptForUpdate(ctx, tx, resolution.ref)
		if errors.Is(err, pgx.ErrNoRows) {
			results[idx].Outcome = store.OutboxResolutionFenced
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("resolve channel attempt: load attempt: %w", err)
		}
		if !exactChannelAttemptRef(resolution.ref, state) {
			results[idx].Outcome = store.OutboxResolutionRejected
			continue
		}
		if !channelAttemptIsCurrent(resolution.ref, state) {
			results[idx].Outcome = store.OutboxResolutionFenced
			continue
		}
		if resolution.targetAuthority {
			if state.deliverySourceInstanceID == nil || *state.deliverySourceInstanceID != resolution.sourceInstanceID {
				results[idx].Outcome = store.OutboxResolutionFenced
				continue
			}
			if !state.targetsBound {
				results[idx].Outcome = store.OutboxResolutionRejected
				continue
			}
			var evidenceKind *string
			err = tx.QueryRow(ctx, `
SELECT evidence_kind
FROM channel_delivery_attempt_targets
WHERE item_id = $1 AND lease_fence = $2
  AND target_instance_id = $3 AND target_user_id = $4
  AND batch_id = $5 AND command_id = $6`,
				resolution.ref.ItemID, int64(resolution.ref.LeaseFence),
				resolution.target.TargetInstanceID, resolution.target.TargetUserID,
				resolution.target.BatchID[:], resolution.target.CommandID[:],
			).Scan(&evidenceKind)
			if errors.Is(err, pgx.ErrNoRows) || evidenceKind != nil {
				results[idx].Outcome = store.OutboxResolutionRejected
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("resolve channel target attempt: load exact target: %w", err)
			}
		} else if state.attemptOwner != resolution.owner || !state.attemptLeaseLive ||
			state.laneOwner == nil || *state.laneOwner != resolution.owner || !state.laneLeaseLive {
			results[idx].Outcome = store.OutboxResolutionFenced
			continue
		}
		value := channelResolutionValue(resolution.kind)
		if state.resolution == value {
			results[idx].Outcome = store.OutboxResolutionDuplicate
			continue
		}
		if state.resolution != "pending" {
			results[idx].Outcome = store.OutboxResolutionRejected
			continue
		}
		commandTag, err := tx.Exec(ctx, `
UPDATE channel_delivery_attempts
SET resolution = $3,
    resolution_error = $4,
    retry_at = CASE
      WHEN targets_bound THEN GREATEST(
        clock_timestamp() + ($5::bigint * interval '1 millisecond'), evidence_deadline
      )
      WHEN $5::bigint > 0 THEN clock_timestamp() + ($5::bigint * interval '1 millisecond')
      ELSE NULL
    END,
    resolved_at = clock_timestamp()
WHERE item_id = $1 AND lease_fence = $2
  AND resolution = 'pending'
  AND EXISTS (
    SELECT 1
    FROM channel_delivery_lanes l
    WHERE l.channel_id = channel_delivery_attempts.channel_id
      AND l.lease_fence = $2 AND l.state = 'leased'
      AND (
        $6::boolean
        OR (l.lease_owner = $7 AND l.lease_until > clock_timestamp())
      )
  )
  AND (
    $6::boolean
    OR (owner = $7 AND lease_until > clock_timestamp())
  )`,
			resolution.ref.ItemID,
			int64(resolution.ref.LeaseFence),
			value,
			resolution.lastError,
			durationMillis(resolution.retryDelay),
			resolution.targetAuthority,
			resolution.owner,
		)
		if err != nil {
			return nil, fmt.Errorf("resolve channel delivery attempt: %w", err)
		}
		if commandTag.RowsAffected() != 1 {
			results[idx].Outcome = store.OutboxResolutionFenced
			continue
		}
		results[idx].Outcome = store.OutboxResolutionRecorded
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("resolve channel delivery attempts: commit: %w", err)
	}
	return results, nil
}

func validateChannelResolutionKind(ref store.OutboxAttemptRef, kind store.OutboxResolutionKind, retryDelay time.Duration) error {
	if err := validateChannelAttemptRef(ref); err != nil {
		return err
	}
	switch kind {
	case store.OutboxResolutionRetry:
		if retryDelay <= 0 {
			return errors.New("channel retry resolution requires a positive retry delay")
		}
	case store.OutboxResolutionTerminalResync:
		if retryDelay != 0 {
			return errors.New("channel terminal resolution cannot carry a retry delay")
		}
	default:
		return errors.New("unknown channel attempt resolution")
	}
	return nil
}

func channelResolutionValue(kind store.OutboxResolutionKind) string {
	switch kind {
	case store.OutboxResolutionRetry:
		return "retry"
	case store.OutboxResolutionTerminalResync:
		return "terminal_resync"
	default:
		return ""
	}
}
