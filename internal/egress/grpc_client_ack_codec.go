package egress

import (
	"encoding/binary"
	"fmt"
	"time"

	"telesrv/internal/edgecontrol"
	"telesrv/internal/egress/egresspb"
)

func clientAckBatchRequest(observations []edgecontrol.ClientAckObservation) (*egresspb.ClientAckBatchRequest, error) {
	if len(observations) == 0 || len(observations) > maxGRPCDeliveryBatchSize {
		return nil, fmt.Errorf("%w: client ACK batch size must be in [1,%d]", ErrGRPCDeliveryInvalidDelivery, maxGRPCDeliveryBatchSize)
	}
	count := len(observations)
	int64Columns := make([]int64, count*6)
	uint64Columns := make([]uint64, count*6)
	uint32Columns := make([]uint32, count*2)
	req := &egresspb.ClientAckBatchRequest{
		StreamIds:         int64Columns[0:count:count],
		ItemIds:           int64Columns[count : count*2 : count*2],
		Sequences:         int64Columns[count*2 : count*3 : count*3],
		SessionIds:        int64Columns[count*3 : count*4 : count*4],
		ServerMsgIds:      int64Columns[count*4 : count*5 : count*5],
		AckedAtUnixNanos:  int64Columns[count*5 : count*6 : count*6],
		LeaseFences:       uint64Columns[0:count:count],
		BatchIdLow:        uint64Columns[count : count*2 : count*2],
		BatchIdHigh:       uint64Columns[count*2 : count*3 : count*3],
		CommandIdLow:      uint64Columns[count*3 : count*4 : count*4],
		CommandIdHigh:     uint64Columns[count*4 : count*5 : count*5],
		AuthKeyIds:        uint64Columns[count*5 : count*6 : count*6],
		QueueKinds:        uint32Columns[0:count:count],
		Attempts:          uint32Columns[count : count*2 : count*2],
		TargetInstanceIds: make([]string, count),
		SourceInstanceIds: make([]string, count),
		TargetUserIds:     make([]int64, count),
	}
	for i, observation := range observations {
		if !observation.Valid() {
			return nil, fmt.Errorf("client ACK index %d: %w", i, ErrGRPCDeliveryInvalidDelivery)
		}
		tracking := observation.Tracking
		req.QueueKinds[i] = uint32(tracking.Ref.Domain.Kind)
		req.StreamIds[i] = tracking.Ref.Domain.StreamID
		req.ItemIds[i] = tracking.Ref.OutboxID
		req.TargetUserIds[i] = tracking.Ref.TargetUserID
		req.Sequences[i] = int64(tracking.Ref.PTS)
		req.LeaseFences[i] = tracking.Ref.LeaseFence
		req.Attempts[i] = tracking.Ref.Attempt
		req.BatchIdLow[i], req.BatchIdHigh[i] = splitDeliveryID(tracking.BatchID)
		req.CommandIdLow[i], req.CommandIdHigh[i] = splitDeliveryID(tracking.CommandID)
		req.TargetInstanceIds[i] = observation.TargetInstanceID
		req.SourceInstanceIds[i] = tracking.SourceInstanceID
		req.AuthKeyIds[i] = binary.LittleEndian.Uint64(observation.AuthKeyID[:])
		req.SessionIds[i] = observation.SessionID
		req.ServerMsgIds[i] = observation.ServerMsgID
		req.AckedAtUnixNanos[i] = observation.ObservedAt.UnixNano()
	}
	return req, nil
}

func clientAckFromPB(req *egresspb.ClientAckBatchRequest, index int) (edgecontrol.ClientAckObservation, error) {
	var authKeyID [8]byte
	binary.LittleEndian.PutUint64(authKeyID[:], req.GetAuthKeyIds()[index])
	observation := edgecontrol.ClientAckObservation{
		Tracking: edgecontrol.DeliveryTracking{
			BatchID:          joinBatchID(req.GetBatchIdLow()[index], req.GetBatchIdHigh()[index]),
			CommandID:        joinCommandID(req.GetCommandIdLow()[index], req.GetCommandIdHigh()[index]),
			SourceInstanceID: req.GetSourceInstanceIds()[index],
			Ref: edgecontrol.DeliveryRef{
				Domain: edgecontrol.OrderingDomain{
					Kind: edgecontrol.QueueKind(req.GetQueueKinds()[index]), StreamID: req.GetStreamIds()[index],
				},
				OutboxID: req.GetItemIds()[index], TargetUserID: req.GetTargetUserIds()[index],
				PTS: int(req.GetSequences()[index]), LeaseFence: req.GetLeaseFences()[index], Attempt: req.GetAttempts()[index],
			},
		},
		TargetInstanceID: req.GetTargetInstanceIds()[index],
		AuthKeyID:        authKeyID, SessionID: req.GetSessionIds()[index], ServerMsgID: req.GetServerMsgIds()[index],
		ObservedAt: time.Unix(0, req.GetAckedAtUnixNanos()[index]),
	}
	if !observation.Valid() {
		return edgecontrol.ClientAckObservation{}, ErrGRPCDeliveryInvalidDelivery
	}
	return observation, nil
}

func clientAckBatchSize(req *egresspb.ClientAckBatchRequest) (int, error) {
	if req == nil {
		return 0, fmt.Errorf("%w: nil client ACK batch", ErrGRPCDeliveryInvalidDelivery)
	}
	count := len(req.GetItemIds())
	if count == 0 || count > maxGRPCDeliveryBatchSize {
		return 0, fmt.Errorf("%w: client ACK batch size must be in [1,%d]", ErrGRPCDeliveryInvalidDelivery, maxGRPCDeliveryBatchSize)
	}
	lengths := [...]int{
		len(req.GetQueueKinds()), len(req.GetStreamIds()), len(req.GetSequences()), len(req.GetLeaseFences()),
		len(req.GetAttempts()), len(req.GetBatchIdLow()), len(req.GetBatchIdHigh()), len(req.GetCommandIdLow()),
		len(req.GetCommandIdHigh()), len(req.GetTargetInstanceIds()), len(req.GetAuthKeyIds()), len(req.GetSessionIds()),
		len(req.GetServerMsgIds()), len(req.GetAckedAtUnixNanos()), len(req.GetTargetUserIds()),
		len(req.GetSourceInstanceIds()),
	}
	for _, length := range lengths {
		if length != count {
			return 0, fmt.Errorf("%w: positional column length %d, want %d", ErrGRPCDeliveryInvalidDelivery, length, count)
		}
	}
	return count, nil
}

func clientAckResultsFromPB(response *egresspb.DeliveryEvidenceBatchResponse, expected int) ([]edgecontrol.ClientAckObservationResult, error) {
	if response == nil || len(response.GetOutcomes()) != expected {
		return nil, fmt.Errorf("%w: result count %d, want %d", ErrGRPCDeliveryProtocolMismatch, len(response.GetOutcomes()), expected)
	}
	results := make([]edgecontrol.ClientAckObservationResult, expected)
	for rawIndex := range response.GetDetails() {
		if int(rawIndex) >= expected {
			return nil, fmt.Errorf("%w: invalid detail index %d", ErrGRPCDeliveryProtocolMismatch, rawIndex)
		}
	}
	for i, outcome := range response.GetOutcomes() {
		switch outcome {
		case egresspb.DeliveryEvidenceOutcome_DELIVERY_EVIDENCE_OUTCOME_RECORDED,
			egresspb.DeliveryEvidenceOutcome_DELIVERY_EVIDENCE_OUTCOME_DUPLICATE,
			egresspb.DeliveryEvidenceOutcome_DELIVERY_EVIDENCE_OUTCOME_ALREADY_FINALIZED:
			results[i].Outcome = edgecontrol.ClientAckObservationApplied
		case egresspb.DeliveryEvidenceOutcome_DELIVERY_EVIDENCE_OUTCOME_FENCED:
			results[i].Outcome = edgecontrol.ClientAckObservationStale
		case egresspb.DeliveryEvidenceOutcome_DELIVERY_EVIDENCE_OUTCOME_REJECTED:
			results[i].Outcome = edgecontrol.ClientAckObservationRejected
		case egresspb.DeliveryEvidenceOutcome_DELIVERY_EVIDENCE_OUTCOME_RETRYABLE:
			results[i].Outcome = edgecontrol.ClientAckObservationRetryable
		default:
			return nil, fmt.Errorf("%w: unknown client ACK result %d at index %d", ErrGRPCDeliveryProtocolMismatch, outcome, i)
		}
	}
	return results, nil
}
