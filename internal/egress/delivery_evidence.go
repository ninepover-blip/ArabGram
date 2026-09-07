package egress

import (
	"context"
	"fmt"
	"time"

	"telesrv/internal/edgecontrol"
	"telesrv/internal/store"
)

type deliveryEvidenceStores struct {
	dispatch   store.DispatchOutboxStore
	absolute   store.DeliveryOutboxStore
	channel    store.ChannelDeliveryStore
	mutations  AttemptMutationWriter
	clientAcks ClientAckObservationSink
}

func (s deliveryEvidenceStores) forEdgeKind(kind edgecontrol.QueueKind) (store.DurableOutboxStateStore, store.OutboxQueueKind, bool) {
	switch kind {
	case edgecontrol.QueueAccountPTS:
		return s.dispatch, store.OutboxQueueDispatchPTS, s.dispatch != nil
	case edgecontrol.QueueAccountNonPTS:
		return s.absolute, store.OutboxQueueAbsoluteDelivery, s.absolute != nil
	case edgecontrol.QueueChannelPTS:
		return s.channel, store.OutboxQueueChannelPTS, s.channel != nil
	default:
		return nil, 0, false
	}
}

func edgeQueueKind(kind store.OutboxQueueKind) (edgecontrol.QueueKind, bool) {
	switch kind {
	case store.OutboxQueueDispatchPTS:
		return edgecontrol.QueueAccountPTS, true
	case store.OutboxQueueAbsoluteDelivery:
		return edgecontrol.QueueAccountNonPTS, true
	case store.OutboxQueueChannelPTS:
		return edgecontrol.QueueChannelPTS, true
	default:
		return 0, false
	}
}

func storeAttemptRef(ref edgecontrol.DeliveryRef) (store.OutboxAttemptRef, bool) {
	var kind store.OutboxQueueKind
	var ok bool
	switch ref.Domain.Kind {
	case edgecontrol.QueueAccountPTS:
		kind, ok = store.OutboxQueueDispatchPTS, true
	case edgecontrol.QueueAccountNonPTS:
		kind, ok = store.OutboxQueueAbsoluteDelivery, true
	case edgecontrol.QueueChannelPTS:
		kind, ok = store.OutboxQueueChannelPTS, true
	}
	if !ok || !ref.Valid() {
		return store.OutboxAttemptRef{}, false
	}
	return store.OutboxAttemptRef{
		QueueKind: kind, StreamID: ref.Domain.StreamID, ItemID: ref.OutboxID,
		Sequence: int64(ref.PTS), LeaseFence: ref.LeaseFence, Attempt: int(ref.Attempt),
	}, true
}

func edgeDeliveryRef(ref store.OutboxAttemptRef, targetUserID int64) (edgecontrol.DeliveryRef, bool) {
	kind, ok := edgeQueueKind(ref.QueueKind)
	if !ok || ref.ItemID <= 0 || ref.StreamID <= 0 || ref.LeaseFence == 0 || ref.Attempt <= 0 || targetUserID < 0 {
		return edgecontrol.DeliveryRef{}, false
	}
	if (kind == edgecontrol.QueueChannelPTS) != (targetUserID == 0) {
		return edgecontrol.DeliveryRef{}, false
	}
	pts := 0
	if kind != edgecontrol.QueueAccountNonPTS {
		if ref.Sequence <= 0 || ref.Sequence > int64(^uint(0)>>1) {
			return edgecontrol.DeliveryRef{}, false
		}
		pts = int(ref.Sequence)
	}
	return edgecontrol.DeliveryRef{
		Domain:   edgecontrol.OrderingDomain{Kind: kind, StreamID: ref.StreamID},
		OutboxID: ref.ItemID, TargetUserID: targetUserID, PTS: pts,
		LeaseFence: ref.LeaseFence, Attempt: uint32(ref.Attempt),
	}, true
}

func applyPhysicalReceiptBatch(
	ctx context.Context,
	stores deliveryEvidenceStores,
	receipts []edgecontrol.PhysicalReceipt,
) []edgecontrol.PhysicalReceiptResult {
	results := make([]edgecontrol.PhysicalReceiptResult, len(receipts))
	type queueBatch struct {
		store             store.DurableOutboxStateStore
		evidenceIndices   []int
		evidence          []store.OutboxAttemptEvidence
		resolutionIndices []int
		resolutions       []store.OutboxTargetAttemptResolution
	}
	groups := make(map[store.OutboxQueueKind]*queueBatch, 3)
	for i, receipt := range receipts {
		stateStore, queueKind, ok := stores.forEdgeKind(receipt.Ref.Domain.Kind)
		ref, refOK := storeAttemptRef(receipt.Ref)
		if !ok || !refOK {
			results[i] = edgecontrol.PhysicalReceiptResult{Outcome: edgecontrol.PhysicalReceiptRejected, Detail: edgecontrol.DetailInvalidIdentity}
			continue
		}
		group := groups[queueKind]
		if group == nil {
			group = &queueBatch{store: stateStore}
			groups[queueKind] = group
		}
		switch receipt.Outcome {
		case edgecontrol.PhysicalWritten, edgecontrol.PhysicalNoEligibleSessions:
			kind := store.OutboxEvidenceEdgeWritten
			if receipt.Outcome == edgecontrol.PhysicalNoEligibleSessions {
				kind = store.OutboxEvidenceEdgeNoEligible
			}
			group.evidenceIndices = append(group.evidenceIndices, i)
			group.evidence = append(group.evidence, store.OutboxAttemptEvidence{
				Ref: ref, Kind: kind, SourceInstanceID: receipt.SourceInstanceID, TargetInstanceID: receipt.TargetInstanceID,
				TargetUserID: receipt.Ref.TargetUserID,
				BatchID:      [16]byte(receipt.BatchID), CommandID: [16]byte(receipt.CommandID),
				EligibleSessions: receipt.EligibleSessions, WrittenSessions: receipt.WrittenSessions,
				ServerMsgID: receipt.FirstServerMsgID, ObservedAt: receipt.ObservedAt,
			})
		case edgecontrol.PhysicalIndeterminate, edgecontrol.PhysicalRejected:
			resolution := store.OutboxResolutionRetry
			if receipt.Outcome == edgecontrol.PhysicalRejected {
				resolution = exhaustedResolutionKind(ref.QueueKind)
			} else if int(receipt.Ref.Attempt) >= maxOnlineDeliveryAttempts {
				resolution = exhaustedResolutionKind(ref.QueueKind)
			}
			var retryDelay time.Duration
			if resolution == store.OutboxResolutionRetry {
				retryDelay = retryDelayForAttempt(int(receipt.Ref.Attempt))
			}
			group.resolutionIndices = append(group.resolutionIndices, i)
			group.resolutions = append(group.resolutions, store.OutboxTargetAttemptResolution{
				Ref: ref, SourceInstanceID: receipt.SourceInstanceID,
				TargetInstanceID: receipt.TargetInstanceID, TargetUserID: receipt.Ref.TargetUserID,
				BatchID: [16]byte(receipt.BatchID), CommandID: [16]byte(receipt.CommandID),
				Kind: resolution, RetryDelay: retryDelay,
				LastError: fmt.Sprintf("physical receipt outcome=%d detail=%d", receipt.Outcome, receipt.Detail),
			})
		default:
			results[i] = edgecontrol.PhysicalReceiptResult{Outcome: edgecontrol.PhysicalReceiptRejected, Detail: edgecontrol.DetailInvalidIdentity}
		}
	}

	for _, kind := range [...]store.OutboxQueueKind{
		store.OutboxQueueDispatchPTS, store.OutboxQueueAbsoluteDelivery, store.OutboxQueueChannelPTS,
	} {
		group := groups[kind]
		if group == nil {
			continue
		}
		applyPhysicalEvidenceGroup(ctx, stores.mutations, kind, group.store, group.evidenceIndices, group.evidence, group.resolutionIndices, group.resolutions, results)
	}
	return results
}

func applyPhysicalEvidenceGroup(
	ctx context.Context,
	mutations AttemptMutationWriter,
	queueKind store.OutboxQueueKind,
	stateStore store.DurableOutboxStateStore,
	evidenceIndices []int,
	evidence []store.OutboxAttemptEvidence,
	resolutionIndices []int,
	resolutions []store.OutboxTargetAttemptResolution,
	results []edgecontrol.PhysicalReceiptResult,
) {
	if stateStore == nil || mutations == nil {
		markPhysicalResultsRetryable(evidenceIndices, results)
		markPhysicalResultsRetryable(resolutionIndices, results)
		return
	}
	if len(evidence) > 0 {
		got, err := mutations.RecordAttemptEvidenceBatch(ctx, queueKind, evidence)
		if err != nil || len(got) != len(evidence) {
			markPhysicalResultsRetryable(evidenceIndices, results)
		} else {
			for i, result := range got {
				index := evidenceIndices[i]
				switch result.Outcome {
				case store.OutboxEvidenceRecorded, store.OutboxEvidenceDuplicate:
					results[index] = edgecontrol.PhysicalReceiptResult{Outcome: edgecontrol.PhysicalReceiptApplied}
				case store.OutboxEvidenceFenced:
					results[index] = edgecontrol.PhysicalReceiptResult{Outcome: edgecontrol.PhysicalReceiptStale, Detail: edgecontrol.DetailAttemptConflict}
				case store.OutboxEvidenceRejected:
					results[index] = edgecontrol.PhysicalReceiptResult{Outcome: edgecontrol.PhysicalReceiptRejected, Detail: edgecontrol.DetailAttemptConflict}
				default:
					results[index] = edgecontrol.PhysicalReceiptResult{Outcome: edgecontrol.PhysicalReceiptRetryable}
				}
			}
		}
	}
	if len(resolutions) > 0 {
		got, err := mutations.ResolveTargetAttemptBatch(ctx, queueKind, resolutions)
		if err != nil || len(got) != len(resolutions) {
			markPhysicalResultsRetryable(resolutionIndices, results)
		} else {
			for i, result := range got {
				index := resolutionIndices[i]
				switch result.Outcome {
				case store.OutboxResolutionRecorded, store.OutboxResolutionDuplicate:
					results[index] = edgecontrol.PhysicalReceiptResult{Outcome: edgecontrol.PhysicalReceiptApplied}
				case store.OutboxResolutionFenced:
					results[index] = edgecontrol.PhysicalReceiptResult{Outcome: edgecontrol.PhysicalReceiptStale, Detail: edgecontrol.DetailAttemptConflict}
				case store.OutboxResolutionRejected:
					results[index] = edgecontrol.PhysicalReceiptResult{Outcome: edgecontrol.PhysicalReceiptRejected, Detail: edgecontrol.DetailAttemptConflict}
				default:
					results[index] = edgecontrol.PhysicalReceiptResult{Outcome: edgecontrol.PhysicalReceiptRetryable}
				}
			}
		}
	}
	// The mutation actor hands successful exact refs to its bounded finalizer
	// lane. This handler returns after that reliable handoff; it never performs
	// per-lane finalization or a queue-wide recovery scan.
}

func markPhysicalResultsRetryable(indices []int, results []edgecontrol.PhysicalReceiptResult) {
	for _, index := range indices {
		results[index] = edgecontrol.PhysicalReceiptResult{Outcome: edgecontrol.PhysicalReceiptRetryable}
	}
}
