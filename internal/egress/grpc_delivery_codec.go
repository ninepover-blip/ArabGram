package egress

import (
	"encoding/binary"
	"fmt"
	"time"

	"telesrv/internal/edgecontrol"
	"telesrv/internal/egress/egresspb"
)

func physicalReceiptBatchRequest(receipts []edgecontrol.PhysicalReceipt) (*egresspb.PhysicalReceiptBatchRequest, error) {
	if len(receipts) == 0 || len(receipts) > maxGRPCDeliveryBatchSize {
		return nil, fmt.Errorf("%w: physical receipt batch size must be in [1,%d]", ErrGRPCDeliveryInvalidDelivery, maxGRPCDeliveryBatchSize)
	}
	count := len(receipts)
	int64Columns := make([]int64, count*6)
	uint64Columns := make([]uint64, count*5)
	uint32Columns := make([]uint32, count*6)
	req := &egresspb.PhysicalReceiptBatchRequest{
		StreamIds:           int64Columns[0:count:count],
		ItemIds:             int64Columns[count : count*2 : count*2],
		Sequences:           int64Columns[count*2 : count*3 : count*3],
		FirstServerMsgIds:   int64Columns[count*3 : count*4 : count*4],
		ObservedAtUnixNanos: int64Columns[count*4 : count*5 : count*5],
		TargetUserIds:       int64Columns[count*5 : count*6 : count*6],
		LeaseFences:         uint64Columns[0:count:count],
		BatchIdLow:          uint64Columns[count : count*2 : count*2],
		BatchIdHigh:         uint64Columns[count*2 : count*3 : count*3],
		CommandIdLow:        uint64Columns[count*3 : count*4 : count*4],
		CommandIdHigh:       uint64Columns[count*4 : count*5 : count*5],
		QueueKinds:          uint32Columns[0:count:count],
		Attempts:            uint32Columns[count : count*2 : count*2],
		Outcomes:            uint32Columns[count*2 : count*3 : count*3],
		EligibleSessions:    uint32Columns[count*3 : count*4 : count*4],
		WrittenSessions:     uint32Columns[count*4 : count*5 : count*5],
		DetailCodes:         uint32Columns[count*5 : count*6 : count*6],
		SourceInstanceIds:   make([]string, count),
		TargetInstanceIds:   make([]string, count),
	}
	for i, receipt := range receipts {
		if err := validatePhysicalReceipt(receipt); err != nil {
			return nil, fmt.Errorf("physical receipt index %d: %w", i, err)
		}
		req.QueueKinds[i] = uint32(receipt.Ref.Domain.Kind)
		req.StreamIds[i] = receipt.Ref.Domain.StreamID
		req.ItemIds[i] = receipt.Ref.OutboxID
		req.TargetUserIds[i] = receipt.Ref.TargetUserID
		req.Sequences[i] = int64(receipt.Ref.PTS)
		req.LeaseFences[i] = receipt.Ref.LeaseFence
		req.Attempts[i] = receipt.Ref.Attempt
		req.BatchIdLow[i], req.BatchIdHigh[i] = splitDeliveryID(receipt.BatchID)
		req.CommandIdLow[i], req.CommandIdHigh[i] = splitDeliveryID(receipt.CommandID)
		req.SourceInstanceIds[i] = receipt.SourceInstanceID
		req.TargetInstanceIds[i] = receipt.TargetInstanceID
		req.Outcomes[i] = uint32(receipt.Outcome)
		req.EligibleSessions[i] = uint32(receipt.EligibleSessions)
		req.WrittenSessions[i] = uint32(receipt.WrittenSessions)
		req.FirstServerMsgIds[i] = receipt.FirstServerMsgID
		req.ObservedAtUnixNanos[i] = receipt.ObservedAt.UnixNano()
		req.DetailCodes[i] = uint32(receipt.Detail)
	}
	return req, nil
}

func physicalReceiptFromPB(req *egresspb.PhysicalReceiptBatchRequest, index int) (edgecontrol.PhysicalReceipt, error) {
	receipt := edgecontrol.PhysicalReceipt{
		BatchID:          joinBatchID(req.GetBatchIdLow()[index], req.GetBatchIdHigh()[index]),
		CommandID:        joinCommandID(req.GetCommandIdLow()[index], req.GetCommandIdHigh()[index]),
		SourceInstanceID: req.GetSourceInstanceIds()[index],
		TargetInstanceID: req.GetTargetInstanceIds()[index],
		Ref: edgecontrol.DeliveryRef{
			Domain: edgecontrol.OrderingDomain{
				Kind:     edgecontrol.QueueKind(req.GetQueueKinds()[index]),
				StreamID: req.GetStreamIds()[index],
			},
			OutboxID:     req.GetItemIds()[index],
			TargetUserID: req.GetTargetUserIds()[index],
			PTS:          int(req.GetSequences()[index]),
			LeaseFence:   req.GetLeaseFences()[index],
			Attempt:      req.GetAttempts()[index],
		},
		Outcome:          edgecontrol.PhysicalOutcome(req.GetOutcomes()[index]),
		Detail:           edgecontrol.DetailCode(req.GetDetailCodes()[index]),
		EligibleSessions: int(req.GetEligibleSessions()[index]),
		WrittenSessions:  int(req.GetWrittenSessions()[index]),
		FirstServerMsgID: req.GetFirstServerMsgIds()[index],
		ObservedAt:       time.Unix(0, req.GetObservedAtUnixNanos()[index]),
	}
	if err := validatePhysicalReceipt(receipt); err != nil {
		return edgecontrol.PhysicalReceipt{}, err
	}
	return receipt, nil
}

func validatePhysicalReceipt(receipt edgecontrol.PhysicalReceipt) error {
	if receipt.BatchID.Empty() || receipt.CommandID.Empty() || !edgecontrol.ValidDeliveryInstanceID(receipt.SourceInstanceID) ||
		!edgecontrol.ValidDeliveryInstanceID(receipt.TargetInstanceID) ||
		!receipt.Ref.Valid() || receipt.ObservedAt.IsZero() || !receipt.Detail.Valid() || receipt.EligibleSessions < 0 || receipt.WrittenSessions < 0 ||
		receipt.WrittenSessions > receipt.EligibleSessions {
		return ErrGRPCDeliveryInvalidDelivery
	}
	switch receipt.Outcome {
	case edgecontrol.PhysicalWritten:
		if receipt.WrittenSessions == 0 || receipt.WrittenSessions != receipt.EligibleSessions || receipt.FirstServerMsgID <= 0 {
			return ErrGRPCDeliveryInvalidDelivery
		}
	case edgecontrol.PhysicalNoEligibleSessions:
		if receipt.EligibleSessions != 0 || receipt.WrittenSessions != 0 || receipt.FirstServerMsgID != 0 {
			return ErrGRPCDeliveryInvalidDelivery
		}
	case edgecontrol.PhysicalIndeterminate, edgecontrol.PhysicalRejected:
		if receipt.WrittenSessions != 0 || receipt.FirstServerMsgID != 0 {
			return ErrGRPCDeliveryInvalidDelivery
		}
	default:
		return ErrGRPCDeliveryInvalidDelivery
	}
	return nil
}

func physicalReceiptBatchSize(req *egresspb.PhysicalReceiptBatchRequest) (int, error) {
	if req == nil {
		return 0, fmt.Errorf("%w: nil physical receipt batch", ErrGRPCDeliveryInvalidDelivery)
	}
	count := len(req.GetItemIds())
	if count == 0 || count > maxGRPCDeliveryBatchSize {
		return 0, fmt.Errorf("%w: physical receipt batch size must be in [1,%d]", ErrGRPCDeliveryInvalidDelivery, maxGRPCDeliveryBatchSize)
	}
	lengths := [...]int{
		len(req.GetQueueKinds()), len(req.GetStreamIds()), len(req.GetSequences()), len(req.GetLeaseFences()),
		len(req.GetAttempts()), len(req.GetBatchIdLow()), len(req.GetBatchIdHigh()), len(req.GetCommandIdLow()),
		len(req.GetCommandIdHigh()), len(req.GetSourceInstanceIds()), len(req.GetTargetInstanceIds()),
		len(req.GetOutcomes()), len(req.GetEligibleSessions()), len(req.GetWrittenSessions()),
		len(req.GetFirstServerMsgIds()), len(req.GetObservedAtUnixNanos()), len(req.GetDetailCodes()), len(req.GetTargetUserIds()),
	}
	for _, length := range lengths {
		if length != count {
			return 0, fmt.Errorf("%w: positional column length %d, want %d", ErrGRPCDeliveryInvalidDelivery, length, count)
		}
	}
	return count, nil
}

func physicalReceiptResultsFromPB(response *egresspb.DeliveryEvidenceBatchResponse, expected int) ([]edgecontrol.PhysicalReceiptResult, error) {
	if response == nil || len(response.GetOutcomes()) != expected {
		return nil, fmt.Errorf("%w: result count %d, want %d", ErrGRPCDeliveryProtocolMismatch, len(response.GetOutcomes()), expected)
	}
	results := make([]edgecontrol.PhysicalReceiptResult, expected)
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
			results[i].Outcome = edgecontrol.PhysicalReceiptApplied
		case egresspb.DeliveryEvidenceOutcome_DELIVERY_EVIDENCE_OUTCOME_FENCED:
			results[i].Outcome = edgecontrol.PhysicalReceiptStale
		case egresspb.DeliveryEvidenceOutcome_DELIVERY_EVIDENCE_OUTCOME_REJECTED:
			results[i].Outcome = edgecontrol.PhysicalReceiptRejected
		case egresspb.DeliveryEvidenceOutcome_DELIVERY_EVIDENCE_OUTCOME_RETRYABLE:
			results[i].Outcome = edgecontrol.PhysicalReceiptRetryable
		default:
			return nil, fmt.Errorf("%w: unknown receipt result %d at index %d", ErrGRPCDeliveryProtocolMismatch, outcome, i)
		}
	}
	return results, nil
}

func splitDeliveryID[T ~[16]byte](id T) (uint64, uint64) {
	return binary.LittleEndian.Uint64(id[:8]), binary.LittleEndian.Uint64(id[8:])
}

func joinBatchID(low, high uint64) edgecontrol.BatchID {
	var id edgecontrol.BatchID
	binary.LittleEndian.PutUint64(id[:8], low)
	binary.LittleEndian.PutUint64(id[8:], high)
	return id
}

func joinCommandID(low, high uint64) edgecontrol.CommandID {
	var id edgecontrol.CommandID
	binary.LittleEndian.PutUint64(id[:8], low)
	binary.LittleEndian.PutUint64(id[8:], high)
	return id
}
