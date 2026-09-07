package rpc

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"

	"go.uber.org/zap"

	"github.com/iamxvbaba/td/tlprofile"
	"telesrv/internal/store"
)

const (
	durableInheritedAuthKeyLayerSingleflightPrefix = "durable-inherited-layer:"
)

const authLayerCommitStripes = 4096

const (
	maxAuthLayerEvidenceEntries = maxAuthInfoEntries
)

type inheritedAuthKeyLayerResult struct {
	layer int
	found bool
}

// PublishAdmittedLayerProfileEvidence commits protocol evidence before an
// admitted request can be delayed by the business scheduler or replaced by an
// initConnection rewrap alias. admissionSeq is allocated by mtprotoedge when
// the fresh request obtains flight ownership and supplies the active-admission
// safe floor. The Redis store's ObservationID is the cross-session/process
// publication order.
// Replays reuse the owner's admission sequence and do not publish again.
func (r *Router) PublishAdmittedLayerProfileEvidence(
	ctx context.Context,
	rawAuthKeyID [8]byte,
	sessionID int64,
	msgID int64,
	admissionSeq uint64,
	safeFloor uint64,
	layer int,
) error {
	if r == nil || rawAuthKeyID == ([8]byte{}) || sessionID == 0 {
		return fmt.Errorf("invalid admitted layer evidence identity")
	}
	if admissionSeq == 0 {
		return fmt.Errorf("invalid admitted layer evidence sequence")
	}
	if safeFloor == 0 || safeFloor > admissionSeq {
		return fmt.Errorf("invalid admitted layer evidence safe floor %d for sequence %d", safeFloor, admissionSeq)
	}
	if !isSupportedLayer(layer) {
		return fmt.Errorf("unsupported admitted layer evidence %d", layer)
	}
	if r.deps.AuthKeySessionLayers == nil {
		return store.ErrAuthKeySessionLayerStoreRequired
	}
	// The edge tracker owns admission lifecycle and supplies an exact global
	// retirement floor. Advance it even if this particular proof became stale:
	// the floor itself remains valid capacity-reclamation evidence.
	r.clientInfoMu.Lock()
	if safeFloor > r.authLayerSafeEvictionFloor {
		r.authLayerSafeEvictionFloor = safeFloor
	}
	r.clientInfoMu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	current, found, err := r.deps.AuthKeySessionLayers.GetSessionLayer(ctx, rawAuthKeyID, sessionID)
	if err != nil {
		return err
	}
	if !found || !current.SharedDefault || current.Layer != layer || current.MessageID != msgID {
		// Another session/process has already committed a newer shared default.
		// The request keeps its immutable result codec but cannot publish stale
		// mutable inherited state into this process.
		return nil
	}
	if current.ObservationID <= 0 {
		return store.ErrAuthKeySessionLayerInvalid
	}
	publicationSequence := uint64(current.ObservationID)
	publicationDurable := true

	unlockCommit := r.lockAuthLayerCommit(rawAuthKeyID)
	defer unlockCommit()
	r.clientInfoMu.Lock()
	applied, err := r.claimAuthLayerDefaultEvidenceLocked(
		layer, publicationSequence, publicationDurable, rawAuthKeyID,
	)
	if err == nil && applied {
		if r.clientInfo == nil {
			r.clientInfo = make(map[clientInfoSessionKey]clientSessionInfo)
		}
		key := clientInfoSessionKey{rawAuthKeyID: rawAuthKeyID, sessionID: sessionID}
		info, exists := r.clientInfo[key]
		if !exists {
			evictMapEntryIfFullLocked(r.clientInfo, maxClientInfoEntries)
		}
		info.layer = layer
		if publicationDurable {
			info.layerObservationID = int64(publicationSequence)
		} else {
			info.layerAdmissionSeq = admissionSeq
		}
		r.clientInfo[key] = info
	}
	r.clientInfoMu.Unlock()
	if err != nil || !applied {
		return err
	}
	if binder, ok := r.deps.Sessions.(AuthKeyLayerBinder); ok {
		binder.SeedInheritedLayerForRawAuthKey(rawAuthKeyID, layer)
	}

	return nil
}

func (r *Router) claimAuthLayerDefaultEvidenceLocked(
	layer int,
	sequence uint64,
	durable bool,
	authKeyIDs ...[8]byte,
) (bool, error) {
	if r.authLayerEvidence == nil {
		r.authLayerEvidence = make(map[[8]byte]authLayerDefaultEvidence)
	}
	ids := make([][8]byte, 0, len(authKeyIDs))
	for _, authKeyID := range authKeyIDs {
		if authKeyID == ([8]byte{}) {
			continue
		}
		duplicate := false
		for _, existing := range ids {
			if existing == authKeyID {
				duplicate = true
				break
			}
		}
		if !duplicate {
			ids = append(ids, authKeyID)
		}
	}
	for _, authKeyID := range ids {
		info := r.authInfo[authKeyID]
		if durable {
			if info.layerObservationID < 0 {
				return false, fmt.Errorf("invalid cached auth-key layer observation %d", info.layerObservationID)
			}
			if uint64(info.layerObservationID) > sequence {
				return false, nil
			}
			if info.layerObservationID > 0 && uint64(info.layerObservationID) == sequence && info.layer != layer {
				return false, fmt.Errorf("auth-key layer evidence conflict at durable observation %d: current=%d requested=%d", sequence, info.layer, layer)
			}
		} else {
			if info.layerAdmissionSeq > sequence {
				return false, nil
			}
			if info.layerAdmissionSeq == sequence && info.layerAdmissionSeq > 0 && info.layer != layer {
				return false, fmt.Errorf("auth-key layer evidence conflict at admission sequence %d: current=%d requested=%d", sequence, info.layer, layer)
			}
		}
		current, exists := r.authLayerEvidence[authKeyID]
		if !exists {
			continue
		}
		if current.durable && !durable {
			return false, nil
		}
		if durable && !current.durable {
			continue
		}
		if current.sequence > sequence {
			return false, nil
		}
		if current.sequence == sequence && current.layer != layer {
			kind := "admission sequence"
			if durable {
				kind = "durable observation"
			}
			return false, fmt.Errorf("auth-key layer evidence conflict at %s %d: current=%d requested=%d", kind, sequence, current.layer, layer)
		}
	}

	missing := 0
	for _, authKeyID := range ids {
		if _, exists := r.authLayerEvidence[authKeyID]; !exists {
			missing++
		}
	}
	if len(r.authLayerEvidence)+missing > maxAuthLayerEvidenceEntries {
		for len(r.authLayerEvidence)+missing > maxAuthLayerEvidenceEntries {
			var (
				candidate     [8]byte
				candidateSeq  uint64
				haveCandidate bool
			)
			for currentID, current := range r.authLayerEvidence {
				keep := false
				for _, id := range ids {
					if currentID == id {
						keep = true
						break
					}
				}
				if keep {
					continue
				}
				if !current.durable && current.sequence >= r.authLayerSafeEvictionFloor {
					continue
				}
				if !haveCandidate || current.sequence < candidateSeq {
					candidate, candidateSeq, haveCandidate = currentID, current.sequence, true
				}
			}
			if !haveCandidate {
				if r.log != nil {
					r.log.Warn("auth-key layer evidence capacity exhausted; skip shared default",
						zap.Int("entries", len(r.authLayerEvidence)),
						zap.Uint64("evidence_sequence", sequence),
						zap.Bool("durable", durable),
						zap.Int("layer", layer))
				}
				return false, nil
			}
			delete(r.authLayerEvidence, candidate)
		}
	}

	for _, authKeyID := range ids {
		r.authLayerEvidence[authKeyID] = authLayerDefaultEvidence{
			layer: layer, sequence: sequence, durable: durable,
		}
		if durable {
			r.rememberAuthClientLayerObservationLocked(authKeyID, layer, int64(sequence))
		} else {
			r.rememberAuthClientLayerAtLocked(authKeyID, layer, sequence)
		}
	}
	return true, nil
}

func (r *Router) lockAuthLayerCommit(authKeyIDs ...[8]byte) func() {
	indices := make([]int, 0, len(authKeyIDs))
	for _, authKeyID := range authKeyIDs {
		if authKeyID == ([8]byte{}) {
			continue
		}
		index := authLayerCommitIndex(authKeyID)
		duplicate := false
		for _, existing := range indices {
			if existing == index {
				duplicate = true
				break
			}
		}
		if !duplicate {
			indices = append(indices, index)
		}
	}
	sort.Ints(indices)
	for _, index := range indices {
		r.authLayerCommit[index].Lock()
	}
	return func() {
		for index := len(indices) - 1; index >= 0; index-- {
			r.authLayerCommit[indices[index]].Unlock()
		}
	}
}

func authLayerCommitIndex(authKeyID [8]byte) int {
	return int(binary.LittleEndian.Uint64(authKeyID[:]) % authLayerCommitStripes)
}

// ResolveInheritedAuthKeyLayer resolves the durable last-known default for a
// newly-created connection. The input is always the physical/raw auth key. A
// bound temporary key is normalized to its permanent identity before reading
// Layer, so an old raw shadow can never outrank the current permanent default.
//
// found=false means no canonical default exists. found=true with layer=0 means
// the canonical record carries a future Layer not generated into this binary;
// it is authoritative-but-
// unusable and therefore blocks clamping until fresh supported explicit
// invokeWithLayer evidence or a newer binary establishes a usable profile.
func (r *Router) ResolveInheritedAuthKeyLayer(ctx context.Context, rawAuthKeyID [8]byte) (int, bool, error) {
	if r == nil || rawAuthKeyID == ([8]byte{}) {
		return 0, false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	layer, found, err := r.resolveAuthKeyLayerDefault(ctx, rawAuthKeyID)
	if err != nil {
		return 0, false, wrapLayerEvidenceStoreAvailability(err)
	}
	if !found {
		return 0, false, nil
	}
	// A Redis read can start before a newer same-process explicit observation
	// commits and return after it. resolveDurableAuthKeyLayerDefault merges the
	// read by observation ID, so recheck that merged tuple before returning; the
	// older read must not select a stale codec for the new connection.
	r.clientInfoMu.RLock()
	current := r.authInfo[rawAuthKeyID]
	r.clientInfoMu.RUnlock()
	if current.layerObservationID > 0 {
		switch {
		case isSupportedLayer(current.layer):
			layer = current.layer
		case current.layerBlocked:
			layer = 0
			found = true
		}
	}
	return layer, found, nil
}

func (r *Router) resolveAuthKeyLayerDefault(ctx context.Context, authKeyID [8]byte) (int, bool, error) {
	if r == nil || authKeyID == ([8]byte{}) {
		return 0, false, nil
	}
	if r.deps.AuthKeySessionLayers == nil {
		return 0, false, store.ErrAuthKeySessionLayerStoreRequired
	}
	return r.resolveDurableAuthKeyLayerDefault(ctx, authKeyID)
}

// resolveDurableAuthKeyLayerDefault deliberately re-reads Redis for each new
// logical session. A process-local cache has no way to observe a newer
// observation committed by another server process; using it as authority would
// make new sessions inherit an old supported Layer forever (and could hide a
// future authoritative Layer). singleflight only coalesces the concurrent
// startup burst; it does not turn the result into an unbounded authority.
func (r *Router) resolveDurableAuthKeyLayerDefault(ctx context.Context, authKeyID [8]byte) (int, bool, error) {
	if r.deps.AuthKeySessionLayers == nil {
		return 0, false, store.ErrAuthKeySessionLayerStoreRequired
	}
	v, err, _ := r.authUserSF.Do(durableInheritedAuthKeyLayerSingleflightPrefix+string(authKeyID[:]), func() (any, error) {
		value, found, err := r.deps.AuthKeySessionLayers.GetAuthKeyLayerDefault(ctx, authKeyID)
		if err != nil {
			return inheritedAuthKeyLayerResult{}, err
		}
		if !found {
			return inheritedAuthKeyLayerResult{}, nil
		}
		if value.Layer <= 0 || value.ObservationID <= 0 {
			return inheritedAuthKeyLayerResult{}, store.ErrAuthKeySessionLayerInvalid
		}
		info := clientSessionInfo{
			layer: value.Layer, layerObservationID: value.ObservationID,
			authKeyInfoChecked: true,
		}
		if !isSupportedLayer(value.Layer) {
			info.layerBlocked = true
			info.layerBlockedByAuthKey = true
			r.cacheAuthKeyClientInfo(authKeyID, info)
			return inheritedAuthKeyLayerResult{found: true}, nil
		}
		r.cacheAuthKeyClientInfo(authKeyID, info)
		return inheritedAuthKeyLayerResult{layer: value.Layer, found: true}, nil
	})
	if err != nil {
		return 0, false, err
	}
	result := v.(inheritedAuthKeyLayerResult)
	return result.layer, result.found, nil
}

func (r *Router) cachedAuthKeyLayerDefault(authKeyID [8]byte) (int, bool) {
	r.clientInfoMu.RLock()
	layer := r.authInfo[authKeyID].layer
	r.clientInfoMu.RUnlock()
	return layer, isSupportedLayer(layer)
}

func (r *Router) cachedAuthKeyLayerResolution(authKeyID [8]byte) (layer int, found, resolved bool) {
	r.clientInfoMu.RLock()
	info, ok := r.authInfo[authKeyID]
	r.clientInfoMu.RUnlock()
	if !ok {
		return 0, false, false
	}
	if isSupportedLayer(info.layer) {
		return info.layer, true, true
	}
	if info.layerBlocked {
		return 0, true, true
	}
	if info.authKeyInfoChecked {
		return 0, false, true
	}
	return 0, false, false
}

func (r *Router) cacheAuthKeyClientInfo(authKeyID [8]byte, info clientSessionInfo) {
	r.clientInfoMu.Lock()
	r.rememberAuthClientInfoLocked(authKeyID, info)
	r.clientInfoMu.Unlock()
}

func isSupportedLayer(layer int) bool {
	if layer <= 0 {
		return false
	}
	profile, ok := tlprofile.ResolveProfile(layer)
	return ok && int(profile) == layer
}
