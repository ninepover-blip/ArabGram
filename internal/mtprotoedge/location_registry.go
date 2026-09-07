package mtprotoedge

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/edgecontrol"
)

const (
	locationMutationBatch          = 512
	channelMembershipMutationBatch = 16
	locationRetryInterval          = time.Second
)

var errLocationLeaseExpired = errors.New("edge location lease expired")

type locationRegistryState struct {
	ctx        context.Context
	registry   edgecontrol.MutableLocationRegistry
	instanceID string
	leaseID    string
	ttl        time.Duration
	interval   time.Duration

	flushMu sync.Mutex
	leaseMu sync.Mutex
	expires time.Time

	lost     atomic.Bool
	failOnce sync.Once
}

// StartLocationRegistry acquires the fenced Edge instance lease before the
// listener is exposed, publishes any already-active sessions once, and starts
// the O(1) lease renewal loop. Session records are subsequently updated only
// from the dirty-session set.
func (m *SessionManager) StartLocationRegistry(ctx context.Context, registry edgecontrol.MutableLocationRegistry, instanceID string, ttl, interval time.Duration) error {
	if m == nil || registry == nil || instanceID == "" {
		return fmt.Errorf("start edge location registry: invalid dependency")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ttl <= 0 {
		ttl = time.Minute
	}
	if interval <= 0 || interval >= ttl {
		interval = ttl / 3
		if interval <= 0 {
			interval = time.Second
		}
	}
	leaseID, err := newLocationLeaseID()
	if err != nil {
		return fmt.Errorf("generate edge location lease id: %w", err)
	}
	state := &locationRegistryState{
		ctx:        ctx,
		registry:   registry,
		instanceID: instanceID,
		leaseID:    leaseID,
		ttl:        ttl,
		interval:   interval,
	}
	acquiredAt := time.Now().UTC()
	callCtx, cancel := context.WithTimeout(ctx, locationOperationTimeout(ttl))
	err = registry.AcquireInstanceLease(callCtx, instanceID, leaseID, ttl)
	cancel()
	if err != nil {
		return fmt.Errorf("acquire edge location lease: %w", err)
	}
	// Redis starts the TTL while processing the request, so anchor the local
	// deadline before the call. This keeps the local fail-closed boundary from
	// ever outliving the authoritative lease because of response latency.
	state.setLeaseExpiry(acquiredAt.Add(ttl))
	if !m.locationState.CompareAndSwap(nil, state) {
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), locationOperationTimeout(ttl))
		_ = registry.ReleaseInstanceLease(releaseCtx, instanceID, leaseID)
		releaseCancel()
		return fmt.Errorf("start edge location registry: already started")
	}

	m.mu.Lock()
	clear(m.locationMembershipPublished)
	for key := range m.bySession {
		m.markLocationDirtyLocked(key)
	}
	for userID := range m.byUser {
		m.markChannelMembershipDirtyLocked(userID)
	}
	m.mu.Unlock()
	if err := state.flushAll(m); err != nil {
		m.locationState.CompareAndSwap(state, nil)
		releaseCtx, releaseCancel := context.WithTimeout(context.Background(), locationOperationTimeout(ttl))
		_ = registry.ReleaseInstanceLease(releaseCtx, instanceID, leaseID)
		releaseCancel()
		return fmt.Errorf("publish initial edge locations: %w", err)
	}

	go state.run(m)
	return nil
}

func (m *SessionManager) triggerLocationRefresh(priority ...sessionKey) error {
	if m == nil {
		return nil
	}
	if len(priority) > 0 {
		m.mu.Lock()
		for _, key := range priority {
			m.markLocationDirtyLocked(key)
		}
		m.mu.Unlock()
	}
	state := m.locationState.Load()
	if state == nil {
		return nil
	}
	if err := state.flush(m, priority); err != nil {
		return err
	}
	if m.hasDirtyChannelMemberships() {
		return state.flushMemberships(m)
	}
	return nil
}

func (m *SessionManager) markLocationDirtyLocked(key sessionKey) {
	if m == nil || key.authKeyID == ([8]byte{}) || key.sessionID == 0 {
		return
	}
	m.locationVersion++
	if m.locationVersion == 0 {
		m.locationVersion++
	}
	m.locationDirty[key] = m.locationVersion
}

func (m *SessionManager) markLocationDirty(key sessionKey) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.markLocationDirtyLocked(key)
	m.mu.Unlock()
}

func (m *SessionManager) triggerChannelMembershipRefresh(userIDs ...int64) error {
	if m == nil {
		return nil
	}
	if len(userIDs) > 0 {
		m.mu.Lock()
		for _, userID := range userIDs {
			m.markChannelMembershipDirtyLocked(userID)
		}
		m.mu.Unlock()
	}
	state := m.locationState.Load()
	if state == nil {
		return nil
	}
	return state.flushMemberships(m)
}

func (m *SessionManager) markChannelMembershipDirtyLocked(userID int64) {
	if m == nil || userID <= 0 {
		return
	}
	m.locationMembershipVersion++
	if m.locationMembershipVersion == 0 {
		m.locationMembershipVersion++
	}
	m.locationMembershipDirty[userID] = m.locationMembershipVersion
}

func (m *SessionManager) locationRecordLocked(instanceID string, key sessionKey, nowUnixNano int64) (edgecontrol.LocationRecord, bool) {
	c := m.bySession[key]
	if c == nil || !c.isActive() {
		return edgecontrol.LocationRecord{}, false
	}
	record := edgecontrol.LocationRecord{
		InstanceID:           instanceID,
		RawAuthKeyID:         key.authKeyID,
		SessionID:            key.sessionID,
		UserID:               c.userID.Load(),
		ReceivesUpdates:      c.receivesUpdates.Load(),
		ActiveChannelIDs:     snapshotChannelIDSet(m.bySessionChannels[key]),
		ChannelSubscriptions: snapshotChannelSubscriptions(m.bySessionSubscriptions[key], nowUnixNano),
	}
	if businessAuthKeyID, ok := c.BusinessAuthKeyID(); ok {
		record.BusinessAuthKeyID = businessAuthKeyID
	}
	if state := c.LayerProfileState(); state.Origin != LayerProfileUnknown {
		record.Layer = int(state.Profile)
	}
	return record, true
}

func (m *SessionManager) channelMembershipRecordLocked(instanceID string, userID int64) (edgecontrol.ChannelMembershipRecord, bool) {
	if userID <= 0 {
		return edgecontrol.ChannelMembershipRecord{}, false
	}
	sessions := m.byUser[userID]
	if len(sessions) == 0 {
		return edgecontrol.ChannelMembershipRecord{InstanceID: instanceID, UserID: userID}, false
	}
	channels := make(map[int64]struct{})
	hasSyncedSession := false
	for key, c := range sessions {
		if c == nil || c.userID.Load() != userID {
			continue
		}
		if !c.membershipsSynced.Load() {
			continue
		}
		hasSyncedSession = true
		for channelID := range m.bySessionMembers[key] {
			if channelID > 0 {
				channels[channelID] = struct{}{}
			}
		}
	}
	if !hasSyncedSession || len(channels) == 0 {
		return edgecontrol.ChannelMembershipRecord{InstanceID: instanceID, UserID: userID}, false
	}
	channelIDs := make([]int64, 0, len(channels))
	for channelID := range channels {
		channelIDs = append(channelIDs, channelID)
	}
	slices.Sort(channelIDs)
	return edgecontrol.ChannelMembershipRecord{InstanceID: instanceID, UserID: userID, ChannelIDs: channelIDs}, true
}

func (m *SessionManager) dirtyLocationMutations(instanceID string, priority []sessionKey, limit int) ([]edgecontrol.LocationMutation, map[sessionKey]uint64) {
	if limit <= 0 {
		limit = locationMutationBatch
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	selected := make([]sessionKey, 0, min(limit, len(m.locationDirty)))
	seen := make(map[sessionKey]struct{}, cap(selected))
	for _, key := range priority {
		if len(selected) >= limit {
			break
		}
		if _, dirty := m.locationDirty[key]; !dirty {
			continue
		}
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		selected = append(selected, key)
	}
	for key := range m.locationDirty {
		if len(selected) >= limit {
			break
		}
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		selected = append(selected, key)
	}
	versions := make(map[sessionKey]uint64, len(selected))
	mutations := make([]edgecontrol.LocationMutation, 0, len(selected))
	now := time.Now().UnixNano()
	for _, key := range selected {
		versions[key] = m.locationDirty[key]
		record, active := m.locationRecordLocked(instanceID, key, now)
		if !active {
			record = edgecontrol.LocationRecord{InstanceID: instanceID, RawAuthKeyID: key.authKeyID, SessionID: key.sessionID}
		}
		mutations = append(mutations, edgecontrol.LocationMutation{Record: record, Deleted: !active})
	}
	return mutations, versions
}

func (m *SessionManager) clearPublishedLocationVersions(versions map[sessionKey]uint64) {
	if len(versions) == 0 {
		return
	}
	m.mu.Lock()
	for key, version := range versions {
		if m.locationDirty[key] == version {
			delete(m.locationDirty, key)
		}
	}
	m.mu.Unlock()
}

func (m *SessionManager) hasDirtyLocations() bool {
	m.mu.RLock()
	dirty := len(m.locationDirty) > 0
	m.mu.RUnlock()
	return dirty
}

func (m *SessionManager) dirtyChannelMembershipMutations(instanceID string, limit int) ([]edgecontrol.ChannelMembershipMutation, map[int64]uint64) {
	if limit <= 0 {
		limit = channelMembershipMutationBatch
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	userIDs := make([]int64, 0, min(limit, len(m.locationMembershipDirty)))
	for userID := range m.locationMembershipDirty {
		userIDs = append(userIDs, userID)
		if len(userIDs) >= limit {
			break
		}
	}
	versions := make(map[int64]uint64, len(userIDs))
	mutations := make([]edgecontrol.ChannelMembershipMutation, 0, len(userIDs))
	for _, userID := range userIDs {
		version := m.locationMembershipDirty[userID]
		record, active := m.channelMembershipRecordLocked(instanceID, userID)
		published, publishedBefore := m.locationMembershipPublished[userID]
		if (!publishedBefore && !active) || (publishedBefore && published.active == active && slices.Equal(published.channelIDs, record.ChannelIDs)) {
			if m.locationMembershipDirty[userID] == version {
				delete(m.locationMembershipDirty, userID)
			}
			continue
		}
		versions[userID] = version
		mutations = append(mutations, edgecontrol.ChannelMembershipMutation{Record: record, Deleted: !active})
	}
	return mutations, versions
}

func (m *SessionManager) recordPublishedChannelMembershipMutations(mutations []edgecontrol.ChannelMembershipMutation) {
	if len(mutations) == 0 {
		return
	}
	m.mu.Lock()
	for _, mutation := range mutations {
		userID := mutation.Record.UserID
		if userID <= 0 {
			continue
		}
		if mutation.Deleted {
			delete(m.locationMembershipPublished, userID)
			continue
		}
		m.locationMembershipPublished[userID] = publishedChannelMembership{
			active:     true,
			channelIDs: append([]int64(nil), mutation.Record.ChannelIDs...),
		}
	}
	m.mu.Unlock()
}

func (m *SessionManager) clearPublishedChannelMembershipVersions(versions map[int64]uint64) {
	if len(versions) == 0 {
		return
	}
	m.mu.Lock()
	for userID, version := range versions {
		if m.locationMembershipDirty[userID] == version {
			delete(m.locationMembershipDirty, userID)
		}
	}
	m.mu.Unlock()
}

func (m *SessionManager) hasDirtyChannelMemberships() bool {
	m.mu.RLock()
	dirty := len(m.locationMembershipDirty) > 0
	m.mu.RUnlock()
	return dirty
}

func (s *locationRegistryState) flush(m *SessionManager, priority []sessionKey) error {
	if s == nil || m == nil {
		return nil
	}
	if s.lost.Load() {
		return edgecontrol.ErrLocationLeaseLost
	}
	s.flushMu.Lock()
	mutations, versions := m.dirtyLocationMutations(s.instanceID, priority, locationMutationBatch)
	if len(mutations) == 0 {
		s.flushMu.Unlock()
		return nil
	}
	callCtx, cancel := s.callContext()
	err := s.registry.ApplyLocationMutations(callCtx, s.instanceID, s.leaseID, mutations)
	cancel()
	if err == nil {
		m.clearPublishedLocationVersions(versions)
	}
	s.flushMu.Unlock()
	if err != nil {
		s.handleFailure(m, "edge location mutation failed", err)
	}
	return err
}

func (s *locationRegistryState) flushMemberships(m *SessionManager) error {
	if s == nil || m == nil {
		return nil
	}
	if s.lost.Load() {
		return edgecontrol.ErrLocationLeaseLost
	}
	s.flushMu.Lock()
	mutations, versions := m.dirtyChannelMembershipMutations(s.instanceID, channelMembershipMutationBatch)
	if len(mutations) == 0 {
		s.flushMu.Unlock()
		return nil
	}
	callCtx, cancel := s.callContext()
	err := s.registry.ApplyChannelMembershipMutations(callCtx, s.instanceID, s.leaseID, mutations)
	cancel()
	if err == nil {
		m.recordPublishedChannelMembershipMutations(mutations)
		m.clearPublishedChannelMembershipVersions(versions)
	}
	s.flushMu.Unlock()
	if err != nil {
		s.handleFailure(m, "edge channel membership mutation failed", err)
	}
	return err
}

func (s *locationRegistryState) flushAll(m *SessionManager) error {
	for m.hasDirtyLocations() || m.hasDirtyChannelMemberships() {
		if m.hasDirtyLocations() {
			if err := s.flush(m, nil); err != nil {
				return err
			}
		}
		if m.hasDirtyChannelMemberships() {
			if err := s.flushMemberships(m); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *locationRegistryState) run(m *SessionManager) {
	cadence := min(s.interval, locationRetryInterval)
	if cadence <= 0 {
		cadence = time.Second
	}
	ticker := time.NewTicker(cadence)
	defer ticker.Stop()
	nextRenew := time.Now().Add(s.interval)
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), locationOperationTimeout(s.ttl))
		err := s.registry.ReleaseInstanceLease(releaseCtx, s.instanceID, s.leaseID)
		cancel()
		if err != nil && !errors.Is(err, edgecontrol.ErrLocationLeaseLost) && m.log != nil {
			m.log.Debug("release edge location lease", zap.String("instance_id", s.instanceID), zap.Error(err))
		}
	}()
	defer m.locationState.CompareAndSwap(s, nil)
	for {
		select {
		case <-s.ctx.Done():
			return
		case now := <-ticker.C:
			if !now.Before(s.leaseExpiry()) {
				s.failClosed(m, errLocationLeaseExpired)
				continue
			}
			if !s.lost.Load() && !now.Before(nextRenew) {
				if s.renew(m) {
					nextRenew = now.Add(s.interval)
				} else {
					nextRenew = now.Add(cadence)
				}
			}
			if m.hasDirtyLocations() && !s.lost.Load() {
				_ = s.flush(m, nil)
			}
			if m.hasDirtyChannelMemberships() && !s.lost.Load() {
				_ = s.flushMemberships(m)
			}
		}
	}
}

func (s *locationRegistryState) renew(m *SessionManager) bool {
	if s.lost.Load() {
		return false
	}
	if !time.Now().UTC().Before(s.leaseExpiry()) {
		s.failClosed(m, errLocationLeaseExpired)
		return false
	}
	renewedAt := time.Now().UTC()
	callCtx, cancel := s.callContext()
	err := s.registry.RenewInstanceLease(callCtx, s.instanceID, s.leaseID, s.ttl)
	cancel()
	if err == nil {
		s.setLeaseExpiry(renewedAt.Add(s.ttl))
		return true
	}
	s.handleFailure(m, "edge location lease renewal failed", err)
	return false
}

func (s *locationRegistryState) handleFailure(m *SessionManager, message string, cause error) {
	if cause == nil || s == nil || m == nil || s.ctx.Err() != nil {
		return
	}
	if m.log != nil {
		m.log.Debug(message, zap.String("instance_id", s.instanceID), zap.Error(cause))
	}
	if errors.Is(cause, edgecontrol.ErrLocationLeaseLost) || !time.Now().UTC().Before(s.leaseExpiry()) {
		s.failClosed(m, cause)
	}
}

func (s *locationRegistryState) failClosed(m *SessionManager, cause error) {
	s.failOnce.Do(func() {
		s.lost.Store(true)
		fatalErr := edgecontrol.ErrLocationLeaseLost
		if cause != nil && !errors.Is(cause, edgecontrol.ErrLocationLeaseLost) {
			fatalErr = fmt.Errorf("%w: %v", edgecontrol.ErrLocationLeaseLost, cause)
		}
		active := m.ActiveRawAuthKeyIDs()
		closed := 0
		for _, authKeyID := range active {
			closed += m.CloseSessionsForRawAuthKeyExcept(authKeyID, 0)
		}
		if m.log != nil {
			m.log.Error("edge location lease lost; closed active raw-key sessions and terminating Edge",
				zap.String("signal", "edge_location_lease_lost"),
				zap.String("instance_id", s.instanceID),
				zap.Int("active_raw_auth_keys", len(active)),
				zap.Int("closed_sessions", closed),
				zap.Error(cause),
			)
		}
		select {
		case m.locationFatal <- fatalErr:
		default:
		}
	})
}

func (s *locationRegistryState) setLeaseExpiry(expires time.Time) {
	s.leaseMu.Lock()
	s.expires = expires
	s.leaseMu.Unlock()
}

func (s *locationRegistryState) leaseExpiry() time.Time {
	s.leaseMu.Lock()
	expires := s.expires
	s.leaseMu.Unlock()
	return expires
}

func (s *locationRegistryState) callContext() (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(locationOperationTimeout(s.ttl))
	if expires := s.leaseExpiry(); !expires.IsZero() && expires.Before(deadline) {
		deadline = expires
	}
	return context.WithDeadline(s.ctx, deadline)
}

func newLocationLeaseID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

func locationOperationTimeout(ttl time.Duration) time.Duration {
	timeout := defaultLocationRegistryOperationTimeout
	if half := ttl / 2; half > 0 && half < timeout {
		timeout = half
	}
	if timeout <= 0 {
		return time.Second
	}
	return timeout
}
