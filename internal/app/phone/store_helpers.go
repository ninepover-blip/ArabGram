package phone

import (
	"time"

	"telesrv/internal/domain"
)

func newRequestedCall(id, accessHash, callerID int64, in domain.PhoneCallRequest, nowUnix int64) domain.PhoneCall {
	return domain.PhoneCall{
		ID:             id,
		AccessHash:     accessHash,
		AdminID:        callerID,
		ParticipantID:  in.CalleeID,
		Video:          in.Video,
		State:          domain.PhoneCallStateRequested,
		Date:           int(nowUnix),
		GAHash:         append([]byte(nil), in.GAHash...),
		CallerProtocol: in.Protocol,
		Protocol:       in.Protocol,
		RandomID:       in.RandomID,
		CallerDevice:   in.CallerDevice,
		PrivacyP2P:     in.PrivacyP2P,
		Connections:    append([]domain.PhoneCallConnection(nil), in.Connections...),
	}
}

func ensureAcceptable(call domain.PhoneCall) error {
	switch call.State {
	case domain.PhoneCallStateRequested, domain.PhoneCallStateRinging:
		return nil
	case domain.PhoneCallStateAccepted, domain.PhoneCallStateConfirmed:
		return ErrAlreadyAccepted
	case domain.PhoneCallStateDiscarded:
		return ErrAlreadyDeclined
	default:
		return ErrPeerInvalid
	}
}

func applyAcceptedCall(call *domain.PhoneCall, gb []byte, proto, negotiated domain.PhoneCallProtocol, device domain.SessionRef) {
	call.GB = append([]byte(nil), gb...)
	call.CalleeProtocol = proto
	call.Protocol = negotiated
	call.CalleeDevice = device
	call.State = domain.PhoneCallStateAccepted
}

func ensureConfirmable(call domain.PhoneCall) error {
	switch call.State {
	case domain.PhoneCallStateAccepted:
		return nil
	case domain.PhoneCallStateConfirmed:
		return ErrAlreadyAccepted
	case domain.PhoneCallStateDiscarded:
		return ErrAlreadyDeclined
	default:
		return ErrPeerInvalid
	}
}

func applyConfirmedCall(call *domain.PhoneCall, ga []byte, keyFingerprint int64, now time.Time) {
	call.GA = append([]byte(nil), ga...)
	call.KeyFingerprint = keyFingerprint
	call.StartDate = int(now.Unix())
	call.P2PAllowed = call.CallerProtocol.UDPP2P && call.CalleeProtocol.UDPP2P && call.PrivacyP2P
	call.State = domain.PhoneCallStateConfirmed
}

func normalizedCallDuration(call domain.PhoneCall, duration int) int {
	if call.StartDate == 0 || duration < 0 {
		return 0
	}
	return duration
}

func callExpired(call domain.PhoneCall, nowUnix, ringSec int64) bool {
	if call.Terminal() || call.State == domain.PhoneCallStateConfirmed {
		return false
	}
	return nowUnix-int64(call.Date) > ringSec
}

func expireReason(call domain.PhoneCall) domain.PhoneCallDiscardReason {
	if call.State == domain.PhoneCallStateAccepted {
		return domain.PhoneCallDiscardReasonDisconnect
	}
	return domain.PhoneCallDiscardReasonMissed
}

func peerDeviceFor(call domain.PhoneCall, peer int64) domain.SessionRef {
	if peer == call.AdminID {
		return call.CallerDevice
	}
	return call.CalleeDevice
}

func signalRateExceeded(windowSec *int64, count *int, nowSec int64, rateLimit int) bool {
	if rateLimit <= 0 {
		rateLimit = 1
	}
	if *windowSec != nowSec {
		*windowSec = nowSec
		*count = 0
	}
	if *count >= rateLimit {
		return true
	}
	*count++
	return false
}
