package postgres

import (
	"context"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/store"
)

type outboxBatchLanePlan struct {
	lane     outboxLaneRow
	rows     []outboxWindowAttemptRow
	itemIDs  []int64
	outcomes []string
}

type outboxBatchSuccessor struct {
	itemID   int64
	sequence int64
	found    bool
}

func (s *durableOutboxState) finalizeLockedLaneGroup(
	ctx context.Context,
	tx pgx.Tx,
	streamIDs []int64,
	byStream map[int64][]store.OutboxFinalizeRequest,
) (map[finalizedAttemptKey]string, error) {
	lanes, err := s.loadLockedLaneGroup(ctx, tx, streamIDs)
	if err != nil {
		return nil, err
	}
	eligible := make([]outboxLaneRow, 0, len(streamIDs))
	for _, streamID := range streamIDs {
		lane, ok := lanes[streamID]
		if !ok || lane.state != "leased" || lane.windowEndItemID == nil || lane.windowEndSequence == nil {
			continue
		}
		for _, request := range byStream[streamID] {
			if request.Ref.LeaseFence == lane.leaseFence {
				eligible = append(eligible, lane)
				break
			}
		}
	}
	if len(eligible) == 0 {
		return map[finalizedAttemptKey]string{}, nil
	}
	windows, err := s.loadCurrentWindowGroup(ctx, tx, eligible)
	if err != nil {
		return nil, err
	}

	plans := make([]outboxBatchLanePlan, 0, len(eligible))
	requiresStrictPath := false
	for _, lane := range eligible {
		rows := windows[lane.streamID]
		if len(rows) == 0 || rows[0].itemID != lane.headItemID {
			continue
		}
		plan := outboxBatchLanePlan{lane: lane, rows: rows}
		for i := range rows {
			row := &rows[i]
			if row.attempt == nil || row.resolution == nil ||
				row.resolutionReady == nil || !*row.resolutionReady {
				break
			}
			switch *row.resolution {
			case "confirmed":
				plan.itemIDs = append(plan.itemIDs, row.itemID)
				plan.outcomes = append(plan.outcomes, "applied")
			case "abandoned", "terminal_resync":
				plan.itemIDs = append(plan.itemIDs, row.itemID)
				plan.outcomes = append(plan.outcomes, *row.resolution)
			case "retry":
				requiresStrictPath = true
			default:
				return nil, fmt.Errorf("finalize %s lane %d: unknown resolution %q", s.queueName(), lane.streamID, *row.resolution)
			}
			if requiresStrictPath {
				break
			}
		}
		plans = append(plans, plan)
		if requiresStrictPath {
			break
		}
	}
	if requiresStrictPath {
		return s.finalizeLockedLaneGroupStrict(ctx, tx, streamIDs, byStream)
	}

	finalized := make(map[finalizedAttemptKey]string)
	itemIDs := make([]int64, 0)
	itemStreams := make([]int64, 0)
	itemFences := make([]int64, 0)
	itemOutcomes := make([]string, 0)
	for _, plan := range plans {
		for i, itemID := range plan.itemIDs {
			itemIDs = append(itemIDs, itemID)
			itemStreams = append(itemStreams, plan.lane.streamID)
			itemFences = append(itemFences, int64(plan.lane.leaseFence))
			itemOutcomes = append(itemOutcomes, plan.outcomes[i])
		}
	}
	partial := make([]outboxBatchLanePlan, 0, len(plans))
	complete := make([]outboxBatchLanePlan, 0, len(plans))
	for _, plan := range plans {
		if len(plan.itemIDs) == 0 {
			continue
		}
		if len(plan.itemIDs) < len(plan.rows) {
			partial = append(partial, plan)
		} else {
			complete = append(complete, plan)
		}
	}
	empty, err := s.applyTerminalLaneGroup(ctx, tx, itemIDs, itemStreams, itemFences, itemOutcomes, partial, complete)
	if err != nil {
		return nil, err
	}
	for i, itemID := range itemIDs {
		finalized[finalizedAttemptKey{itemID: itemID, fence: uint64(itemFences[i])}] = itemOutcomes[i]
	}
	if err := s.finishEmptyConsumedLaneGroup(ctx, tx, complete, empty); err != nil {
		return nil, err
	}
	return finalized, nil
}

func (s *durableOutboxState) finalizeLockedLaneGroupStrict(
	ctx context.Context,
	tx pgx.Tx,
	streamIDs []int64,
	byStream map[int64][]store.OutboxFinalizeRequest,
) (map[finalizedAttemptKey]string, error) {
	finalized := make(map[finalizedAttemptKey]string)
	for _, streamID := range streamIDs {
		_, laneFinalized, err := s.finalizeLockedLane(ctx, tx, streamID, byStream[streamID])
		if err != nil {
			return nil, err
		}
		for key, outcome := range laneFinalized {
			finalized[key] = outcome
		}
	}
	return finalized, nil
}

func (s *durableOutboxState) loadLockedLaneGroup(ctx context.Context, tx pgx.Tx, streamIDs []int64) (map[int64]outboxLaneRow, error) {
	rows, err := tx.Query(ctx, s.cfg.finalizeLockLaneGroupSQL, streamIDs)
	if err != nil {
		return nil, fmt.Errorf("lock %s lane group: %w", s.queueName(), err)
	}
	defer rows.Close()
	result := make(map[int64]outboxLaneRow, len(streamIDs))
	for rows.Next() {
		var lane outboxLaneRow
		var fence int64
		if err := rows.Scan(&lane.streamID, &lane.headItemID, &lane.headSequence, &lane.state, &fence,
			&lane.leaseOwner, &lane.leaseUntil, &lane.windowEndItemID, &lane.windowEndSequence); err != nil {
			return nil, fmt.Errorf("scan locked %s lane group: %w", s.queueName(), err)
		}
		if fence < 0 {
			return nil, fmt.Errorf("lock %s lane group: negative fence", s.queueName())
		}
		lane.leaseFence = uint64(fence)
		result[lane.streamID] = lane
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate locked %s lane group: %w", s.queueName(), err)
	}
	return result, nil
}

func (s *durableOutboxState) loadCurrentWindowGroup(ctx context.Context, tx pgx.Tx, lanes []outboxLaneRow) (map[int64][]outboxWindowAttemptRow, error) {
	streamIDs := make([]int64, len(lanes))
	headSequences := make([]int64, len(lanes))
	headItemIDs := make([]int64, len(lanes))
	endSequences := make([]int64, len(lanes))
	endItemIDs := make([]int64, len(lanes))
	fences := make([]int64, len(lanes))
	for i, lane := range lanes {
		streamIDs[i] = lane.streamID
		headSequences[i] = lane.headSequence
		headItemIDs[i] = lane.headItemID
		endSequences[i] = *lane.windowEndSequence
		endItemIDs[i] = *lane.windowEndItemID
		fences[i] = int64(lane.leaseFence)
	}
	rows, err := tx.Query(ctx, s.cfg.finalizeLoadCurrentWindowSQL,
		streamIDs, headSequences, headItemIDs, endSequences, endItemIDs, fences)
	if err != nil {
		return nil, fmt.Errorf("load current %s window group: %w", s.queueName(), err)
	}
	defer rows.Close()
	result := make(map[int64][]outboxWindowAttemptRow, len(lanes))
	for rows.Next() {
		var streamID int64
		var row outboxWindowAttemptRow
		if err := rows.Scan(&streamID, &row.itemID, &row.sequence, &row.attempt,
			&row.issuedAt, &row.commandNotAfter, &row.evidenceDeadline, &row.resolution,
			&row.retryAt, &row.resolutionReady); err != nil {
			return nil, fmt.Errorf("scan current %s window group: %w", s.queueName(), err)
		}
		result[streamID] = append(result[streamID], row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate current %s window group: %w", s.queueName(), err)
	}
	return result, nil
}

// applyTerminalLaneGroup commits the common exact-finalizer mutations in one
// set-based statement after lanes/windows have been locked and classified in
// Go. Sibling data-modifying CTEs share one snapshot, so successor selection
// explicitly excludes item_input instead of assuming it sees deleted rows.
func (s *durableOutboxState) applyTerminalLaneGroup(
	ctx context.Context,
	tx pgx.Tx,
	itemIDs, streamIDs, fences []int64,
	outcomes []string,
	partial, complete []outboxBatchLanePlan,
) ([]int64, error) {
	if len(itemIDs) == 0 {
		return nil, nil
	}
	lanePlans := make([]outboxBatchLanePlan, 0, len(partial)+len(complete))
	lanePlans = append(lanePlans, partial...)
	lanePlans = append(lanePlans, complete...)
	sort.Slice(lanePlans, func(i, j int) bool { return lanePlans[i].lane.streamID < lanePlans[j].lane.streamID })
	laneStreamIDs := make([]int64, len(lanePlans))
	laneFences := make([]int64, len(lanePlans))
	lanePartial := make([]bool, len(lanePlans))
	nextItemIDs := make([]int64, len(lanePlans))
	nextSequences := make([]int64, len(lanePlans))
	for i, plan := range lanePlans {
		laneStreamIDs[i] = plan.lane.streamID
		laneFences[i] = int64(plan.lane.leaseFence)
		if len(plan.itemIDs) < len(plan.rows) {
			next := &plan.rows[len(plan.itemIDs)]
			lanePartial[i] = true
			nextItemIDs[i] = next.itemID
			nextSequences[i] = s.laneSequence(next)
		}
	}
	var finalizedCount, deletedCount, updatedLaneCount int64
	var empty []int64
	err := tx.QueryRow(ctx, s.cfg.finalizeApplyTerminalGroupSQL,
		itemIDs, streamIDs, fences, outcomes,
		laneStreamIDs, laneFences, lanePartial, nextItemIDs, nextSequences,
		s.cfg.kind == store.OutboxQueueDispatchPTS,
	).Scan(&finalizedCount, &deletedCount, &updatedLaneCount, &empty)
	if err != nil {
		return nil, fmt.Errorf("apply terminal %s lane group: %w", s.queueName(), err)
	}
	if finalizedCount != int64(len(itemIDs)) || deletedCount != int64(len(itemIDs)) {
		return nil, fmt.Errorf("apply terminal %s lane group: finalized/deleted %d/%d of %d", s.queueName(), finalizedCount, deletedCount, len(itemIDs))
	}
	expectedLaneUpdates := int64(len(lanePlans) - len(empty))
	if updatedLaneCount != expectedLaneUpdates {
		return nil, fmt.Errorf("apply terminal %s lane group: updated %d lanes want %d", s.queueName(), updatedLaneCount, expectedLaneUpdates)
	}
	return empty, nil
}

func (s *durableOutboxState) finishEmptyConsumedLaneGroup(ctx context.Context, tx pgx.Tx, plans []outboxBatchLanePlan, empty []int64) error {
	if len(empty) == 0 {
		return nil
	}
	lanes := make(map[int64]outboxLaneRow, len(plans))
	for _, plan := range plans {
		lanes[plan.lane.streamID] = plan.lane
	}
	sort.Slice(empty, func(i, j int) bool { return empty[i] < empty[j] })
	if _, err := tx.Exec(ctx, `
SELECT pg_advisory_xact_lock(outbox_lane_advisory_key($1, stream_id))
FROM unnest($2::bigint[]) AS requested(stream_id)
ORDER BY stream_id`, s.cfg.laneLockScope, empty); err != nil {
		return fmt.Errorf("lock empty %s lane group transition: %w", s.queueName(), err)
	}
	rechecked, err := s.loadSuccessorGroup(ctx, tx, empty)
	if err != nil {
		return err
	}
	restored := make(map[int64]outboxBatchSuccessor)
	stillEmpty := make([]int64, 0, len(empty))
	for _, streamID := range empty {
		if successor := rechecked[streamID]; successor.found {
			restored[streamID] = successor
		} else {
			stillEmpty = append(stillEmpty, streamID)
		}
	}
	if err := s.releaseLaneGroupToSuccessors(ctx, tx, lanes, restored); err != nil {
		return err
	}
	return s.deleteEmptyLaneGroup(ctx, tx, lanes, stillEmpty)
}

func (s *durableOutboxState) loadSuccessorGroup(ctx context.Context, tx pgx.Tx, streamIDs []int64) (map[int64]outboxBatchSuccessor, error) {
	rows, err := tx.Query(ctx, s.cfg.finalizeLoadSuccessorGroupSQL,
		streamIDs, s.cfg.kind == store.OutboxQueueDispatchPTS)
	if err != nil {
		return nil, fmt.Errorf("load %s successor group: %w", s.queueName(), err)
	}
	defer rows.Close()
	result := make(map[int64]outboxBatchSuccessor, len(streamIDs))
	for rows.Next() {
		var streamID int64
		var itemID, sequence *int64
		if err := rows.Scan(&streamID, &itemID, &sequence); err != nil {
			return nil, fmt.Errorf("scan %s successor group: %w", s.queueName(), err)
		}
		if itemID != nil && sequence != nil {
			result[streamID] = outboxBatchSuccessor{itemID: *itemID, sequence: *sequence, found: true}
		} else {
			result[streamID] = outboxBatchSuccessor{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s successor group: %w", s.queueName(), err)
	}
	return result, nil
}

func (s *durableOutboxState) releaseLaneGroupToSuccessors(
	ctx context.Context,
	tx pgx.Tx,
	lanes map[int64]outboxLaneRow,
	successors map[int64]outboxBatchSuccessor,
) error {
	if len(successors) == 0 {
		return nil
	}
	streamIDs := make([]int64, 0, len(successors))
	for streamID := range successors {
		streamIDs = append(streamIDs, streamID)
	}
	sort.Slice(streamIDs, func(i, j int) bool { return streamIDs[i] < streamIDs[j] })
	fences := make([]int64, len(streamIDs))
	itemIDs := make([]int64, len(streamIDs))
	sequences := make([]int64, len(streamIDs))
	for i, streamID := range streamIDs {
		fences[i] = int64(lanes[streamID].leaseFence)
		itemIDs[i] = successors[streamID].itemID
		sequences[i] = successors[streamID].sequence
	}
	tag, err := tx.Exec(ctx, s.cfg.finalizeReleaseSuccessorGroupSQL,
		streamIDs, fences, itemIDs, sequences)
	if err != nil {
		return fmt.Errorf("release %s lane successor group: %w", s.queueName(), err)
	}
	if tag.RowsAffected() != int64(len(streamIDs)) {
		return fmt.Errorf("release %s lane successor group: updated %d of %d", s.queueName(), tag.RowsAffected(), len(streamIDs))
	}
	return nil
}

func (s *durableOutboxState) deleteEmptyLaneGroup(ctx context.Context, tx pgx.Tx, lanes map[int64]outboxLaneRow, streamIDs []int64) error {
	if len(streamIDs) == 0 {
		return nil
	}
	fences := make([]int64, len(streamIDs))
	for i, streamID := range streamIDs {
		fences[i] = int64(lanes[streamID].leaseFence)
	}
	tag, err := tx.Exec(ctx, s.cfg.finalizeDeleteEmptyLaneGroupSQL, streamIDs, fences)
	if err != nil {
		return fmt.Errorf("delete empty %s lane group: %w", s.queueName(), err)
	}
	if tag.RowsAffected() != int64(len(streamIDs)) {
		return fmt.Errorf("delete empty %s lane group: deleted %d of %d", s.queueName(), tag.RowsAffected(), len(streamIDs))
	}
	return nil
}
