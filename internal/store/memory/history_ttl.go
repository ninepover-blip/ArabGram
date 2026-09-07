package memory

import (
	"context"
	"fmt"
	"sort"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func (s *MessageStore) GetPrivateHistoryTTL(_ context.Context, ownerUserID int64, peer domain.Peer) (int, error) {
	if s == nil || ownerUserID == 0 || peer.Type != domain.PeerTypeUser || peer.ID == 0 {
		return 0, fmt.Errorf("private history ttl: invalid peer")
	}
	if s.dialogs == nil {
		return 0, fmt.Errorf("private history ttl: dialog store is required")
	}
	s.dialogs.mu.RLock()
	defer s.dialogs.mu.RUnlock()
	for _, dialog := range s.dialogs.m[ownerUserID].Dialogs {
		if dialog.Peer == peer {
			return dialog.TTLPeriod, nil
		}
	}
	return 0, nil
}

func (s *MessageStore) SetPrivateHistoryTTL(ctx context.Context, ownerUserID int64, peer domain.Peer, period int, effects store.DeliveryEffectsBuilder[domain.PrivateHistoryTTLResult]) error {
	if s == nil || ownerUserID == 0 || peer.Type != domain.PeerTypeUser || peer.ID == 0 {
		return fmt.Errorf("set private history ttl: invalid peer")
	}
	if period < 0 {
		return fmt.Errorf("set private history ttl: invalid period")
	}
	if effects == nil {
		return store.ErrDeliveryOutboxRequired
	}
	if s.dialogs == nil {
		return fmt.Errorf("set private history ttl: dialog store is required")
	}
	s.dialogs.mu.Lock()
	defer s.dialogs.mu.Unlock()
	result := domain.PrivateHistoryTTLResult{OwnerUserID: ownerUserID, Peer: peer, Period: period}
	intents, err := effects(result)
	if err != nil {
		return fmt.Errorf("build private history ttl delivery effects: %w", err)
	}
	want := 1
	if peer.ID != ownerUserID {
		want = 2
	}
	if len(intents) != want {
		return store.ErrDeliveryOutboxRequired
	}
	if _, err := applyDeliveryEffects(ctx, intents, s.deliveryOutbox, nil); err != nil {
		return fmt.Errorf("apply private history ttl delivery effects: %w", err)
	}
	setMemoryDialogTTL(s.dialogs, ownerUserID, peer, period)
	if peer.ID != ownerUserID {
		setMemoryDialogTTL(s.dialogs, peer.ID, domain.Peer{Type: domain.PeerTypeUser, ID: ownerUserID}, period)
	}
	return nil
}

func (s *MessageStore) DefaultHistoryTTL(_ context.Context, userID int64) (int, error) {
	if s == nil || userID == 0 {
		return 0, nil
	}
	s.mu.RLock()
	period := s.defaultHistoryTTL[userID]
	s.mu.RUnlock()
	if period < 0 {
		return 0, nil
	}
	return period, nil
}

func (s *MessageStore) SetDefaultHistoryTTL(_ context.Context, userID int64, period int) error {
	if s == nil || userID == 0 {
		return fmt.Errorf("set default history ttl: user is required")
	}
	if period < 0 {
		return fmt.Errorf("set default history ttl: invalid period")
	}
	s.mu.Lock()
	s.defaultHistoryTTL[userID] = period
	s.mu.Unlock()
	return nil
}

func (s *MessageStore) ClaimExpiredPrivateMessages(_ context.Context, now, limit int) ([]domain.DeleteMessagesRequest, error) {
	if s == nil || now <= 0 || limit <= 0 {
		return nil, nil
	}
	if limit > domain.MaxDeleteHistoryBatch {
		limit = domain.MaxDeleteHistoryBatch
	}
	type logicalMessageKey struct {
		uid   int64
		owner int64
		id    int
	}
	type candidate struct {
		message domain.Message
		key     logicalMessageKey
	}
	s.mu.RLock()
	candidates := make([]candidate, 0)
	for _, messages := range s.m {
		for _, msg := range messages {
			if msg.Deleted || msg.ExpiresAt <= 0 || msg.ExpiresAt > now || msg.Peer.Type != domain.PeerTypeUser {
				continue
			}
			key := logicalMessageKey{owner: msg.OwnerUserID, id: msg.ID}
			if msg.UID != 0 {
				key = logicalMessageKey{uid: msg.UID}
			}
			candidates = append(candidates, candidate{message: cloneMessage(msg), key: key})
		}
	}
	s.mu.RUnlock()
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].message.ExpiresAt != candidates[j].message.ExpiresAt {
			return candidates[i].message.ExpiresAt < candidates[j].message.ExpiresAt
		}
		if candidates[i].message.OwnerUserID != candidates[j].message.OwnerUserID {
			return candidates[i].message.OwnerUserID < candidates[j].message.OwnerUserID
		}
		return candidates[i].message.ID < candidates[j].message.ID
	})
	seen := make(map[logicalMessageKey]struct{}, len(candidates))
	selected := make([]domain.Message, 0, limit)
	for _, item := range candidates {
		if _, ok := seen[item.key]; ok {
			continue
		}
		seen[item.key] = struct{}{}
		selected = append(selected, item.message)
		if len(selected) == limit {
			break
		}
	}
	type requestKey struct {
		owner int64
		peer  int64
	}
	index := make(map[requestKey]int)
	out := make([]domain.DeleteMessagesRequest, 0)
	for _, msg := range selected {
		key := requestKey{owner: msg.OwnerUserID, peer: msg.Peer.ID}
		pos, ok := index[key]
		if !ok {
			pos = len(out)
			index[key] = pos
			out = append(out, domain.DeleteMessagesRequest{OwnerUserID: msg.OwnerUserID, Revoke: true, Date: now})
		}
		out[pos].IDs = append(out[pos].IDs, msg.ID)
	}
	return out, nil
}

func setMemoryDialogTTL(dialogs *DialogStore, ownerUserID int64, peer domain.Peer, period int) {
	list := dialogs.m[ownerUserID]
	for i := range list.Dialogs {
		if list.Dialogs[i].Peer == peer {
			list.Dialogs[i].TTLPeriod = period
			dialogs.m[ownerUserID] = list
			return
		}
	}
	list.Dialogs = append(list.Dialogs, domain.Dialog{Peer: peer, TTLPeriod: period})
	dialogs.m[ownerUserID] = list
}
