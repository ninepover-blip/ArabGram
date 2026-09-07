package phone

import (
	"context"
	"time"

	"telesrv/internal/domain"
)

// NewInMemoryActiveCallStoreForTest returns the process-local active call store
// used by unit tests. Production Core must inject NewRedisActiveCallStore.
func NewInMemoryActiveCallStoreForTest() ActiveCallStore {
	return &memoryActiveCallStore{reg: newRegistry()}
}

type memoryActiveCallStore struct {
	reg *registry
}

func (s *memoryActiveCallStore) RequestCall(_ context.Context, callerID int64, in domain.PhoneCallRequest, now time.Time, cfg Config) (domain.PhoneCall, error) {
	nowUnix := now.Unix()
	s.reg.mu.Lock()
	defer s.reg.mu.Unlock()
	s.reg.sweepTombstonesLocked(nowUnix, int64(cfg.TombstoneTTL/time.Second))

	key := randomKey{callerID: callerID, randomID: in.RandomID}
	if id, ok := s.reg.byRandom[key]; ok {
		if e, ok := s.reg.byID[id]; ok && !e.call.Terminal() {
			return e.call, nil
		}
	}
	if len(s.reg.byID) >= cfg.MaxRegistryEntries {
		return domain.PhoneCall{}, ErrOccupyFailed
	}
	if s.reg.active[callerID] >= cfg.MaxActivePerUser {
		return domain.PhoneCall{}, ErrOccupyFailed
	}

	id, err := s.reg.newIDLocked()
	if err != nil {
		return domain.PhoneCall{}, err
	}
	accessHash, err := randomPositiveInt64()
	if err != nil {
		return domain.PhoneCall{}, err
	}
	call := newRequestedCall(id, accessHash, callerID, in, nowUnix)
	s.reg.byID[id] = &entry{call: call}
	s.reg.byRandom[key] = id
	s.reg.active[callerID]++
	return call, nil
}

func (s *memoryActiveCallStore) ReceivedCall(_ context.Context, userID, callID, accessHash int64, now time.Time) (domain.PhoneCall, bool, error) {
	s.reg.mu.Lock()
	defer s.reg.mu.Unlock()
	e, err := s.lookupLocked(callID, accessHash)
	if err != nil {
		return domain.PhoneCall{}, false, err
	}
	if userID != e.call.ParticipantID {
		return domain.PhoneCall{}, false, ErrPeerInvalid
	}
	if e.call.State == domain.PhoneCallStateRequested {
		e.call.State = domain.PhoneCallStateRinging
		e.call.ReceiveDate = int(now.Unix())
		return e.call, true, nil
	}
	return e.call, false, nil
}

func (s *memoryActiveCallStore) AcceptCall(_ context.Context, userID, callID, accessHash int64, gb []byte, proto domain.PhoneCallProtocol, device domain.SessionRef) (domain.PhoneCall, error) {
	s.reg.mu.Lock()
	defer s.reg.mu.Unlock()
	e, err := s.lookupLocked(callID, accessHash)
	if err != nil {
		return domain.PhoneCall{}, err
	}
	if userID != e.call.ParticipantID {
		return domain.PhoneCall{}, ErrPeerInvalid
	}
	if err := ensureAcceptable(e.call); err != nil {
		return domain.PhoneCall{}, err
	}
	negotiated, err := negotiateProtocol(e.call.CallerProtocol, proto)
	if err != nil {
		return domain.PhoneCall{}, err
	}
	applyAcceptedCall(&e.call, gb, proto, negotiated, device)
	return e.call, nil
}

func (s *memoryActiveCallStore) ConfirmCall(_ context.Context, userID, callID, accessHash int64, ga []byte, keyFingerprint int64, proto domain.PhoneCallProtocol, now time.Time, _ Config) (domain.PhoneCall, bool, error) {
	s.reg.mu.Lock()
	defer s.reg.mu.Unlock()
	e, err := s.lookupLocked(callID, accessHash)
	if err != nil {
		return domain.PhoneCall{}, false, err
	}
	if userID != e.call.AdminID {
		return domain.PhoneCall{}, false, ErrPeerInvalid
	}
	if err := ensureConfirmable(e.call); err != nil {
		return domain.PhoneCall{}, false, err
	}
	if err := validateProtocol(proto); err != nil {
		return domain.PhoneCall{}, false, err
	}
	if len(ga) != dhPubSize || sha256Mismatch(ga, e.call.GAHash) {
		s.reg.markDiscardedLocked(e, domain.PhoneCallDiscardReasonDisconnect, 0, int(now.Unix()))
		return e.call, true, ErrGAHashMismatch
	}
	applyConfirmedCall(&e.call, ga, keyFingerprint, now)
	return e.call, false, nil
}

func (s *memoryActiveCallStore) DiscardCallWithSlug(_ context.Context, userID, callID, accessHash int64, reason domain.PhoneCallDiscardReason, reasonSlug string, duration int, now time.Time, _ Config) (domain.PhoneCall, bool, error) {
	s.reg.mu.Lock()
	defer s.reg.mu.Unlock()
	e, err := s.lookupLocked(callID, accessHash)
	if err != nil {
		return domain.PhoneCall{}, false, err
	}
	if !e.call.HasParticipant(userID) {
		return domain.PhoneCall{}, false, ErrPeerInvalid
	}
	if e.call.Terminal() {
		return e.call, true, nil
	}
	duration = normalizedCallDuration(e.call, duration)
	s.reg.markDiscardedWithSlugLocked(e, reason, reasonSlug, duration, int(now.Unix()))
	return e.call, false, nil
}

func (s *memoryActiveCallStore) ExpireDue(_ context.Context, now time.Time, cfg Config) []domain.PhoneCall {
	nowUnix := now.Unix()
	ringSec := int64(cfg.RingTimeout / time.Second)
	s.reg.mu.Lock()
	defer s.reg.mu.Unlock()
	var expired []domain.PhoneCall
	for _, e := range s.reg.byID {
		if !callExpired(e.call, nowUnix, ringSec) {
			continue
		}
		reason := expireReason(e.call)
		s.reg.markDiscardedLocked(e, reason, 0, int(nowUnix))
		expired = append(expired, e.call)
	}
	s.reg.sweepTombstonesLocked(nowUnix, int64(cfg.TombstoneTTL/time.Second))
	return expired
}

func (s *memoryActiveCallStore) Signal(_ context.Context, userID, callID, accessHash int64, now time.Time, rateLimit int, forward func(peerUserID int64, peerDevice domain.SessionRef)) (bool, error) {
	s.reg.mu.Lock()
	e, lookupErr := s.lookupLocked(callID, accessHash)
	if lookupErr != nil {
		s.reg.mu.Unlock()
		return false, lookupErr
	}
	if !e.call.HasParticipant(userID) {
		s.reg.mu.Unlock()
		return false, ErrPeerInvalid
	}
	state := e.call.State
	peer := e.call.PeerOf(userID)
	peerDevice := peerDeviceFor(e.call, peer)
	s.reg.mu.Unlock()

	switch state {
	case domain.PhoneCallStateAccepted, domain.PhoneCallStateConfirmed:
	case domain.PhoneCallStateDiscarded:
		return true, nil
	default:
		return false, ErrPeerInvalid
	}

	e.sigMu.Lock()
	defer e.sigMu.Unlock()
	if signalRateExceeded(&e.sigWindowSec, &e.sigCount, now.Unix(), rateLimit) {
		return true, nil
	}
	forward(peer, peerDevice)
	return false, nil
}

func (s *memoryActiveCallStore) Lookup(_ context.Context, callID, accessHash int64) (domain.PhoneCall, bool) {
	s.reg.mu.Lock()
	defer s.reg.mu.Unlock()
	e, err := s.lookupLocked(callID, accessHash)
	if err != nil {
		return domain.PhoneCall{}, false
	}
	return e.call, true
}

func (s *memoryActiveCallStore) lookupLocked(callID, accessHash int64) (*entry, error) {
	e, ok := s.reg.byID[callID]
	if !ok || e.call.AccessHash != accessHash {
		return nil, ErrPeerInvalid
	}
	return e, nil
}
