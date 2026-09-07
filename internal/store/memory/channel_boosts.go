package memory

import (
	"context"
	"sort"
	"strconv"
	"telesrv/internal/domain"
)

func (s *ChannelStore) GetPremiumBoostStatus(_ context.Context, viewerUserID, channelID int64, now int) (domain.PremiumBoostStatus, error) {
	if viewerUserID == 0 || channelID == 0 {
		return domain.PremiumBoostStatus{}, domain.ErrChannelInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if _, _, _, err := s.channelForViewerLocked(viewerUserID, channelID); err != nil {
		return domain.PremiumBoostStatus{}, err
	}
	peer := domain.Peer{Type: domain.PeerTypeChannel, ID: channelID}
	total := 0
	my := make([]domain.PremiumBoostSlot, 0, 1)
	for _, slot := range s.boostSlots {
		if slot.Peer != peer {
			continue
		}
		total += slot.Weight(now)
		if slot.UserID == viewerUserID && slot.Assigned(now) {
			my = append(my, clonePremiumBoostSlot(slot))
		}
	}
	sortPremiumBoostSlots(my)
	return domain.PremiumBoostStatusForCount(peer, total, my), nil
}

func (s *ChannelStore) ListPremiumBoosts(_ context.Context, viewerUserID, channelID int64, gifts bool, offset string, limit, now int) (domain.PremiumBoostList, error) {
	if viewerUserID == 0 || channelID == 0 {
		return domain.PremiumBoostList{}, domain.ErrChannelInvalid
	}
	if limit <= 0 || limit > domain.MaxPremiumBoostsListLimit {
		limit = domain.MaxPremiumBoostsListLimit
	}
	start, err := boostOffsetIndex(offset)
	if err != nil {
		return domain.PremiumBoostList{}, domain.ErrChannelInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, member, err := s.channelAndMemberLocked(viewerUserID, channelID)
	if err != nil {
		return domain.PremiumBoostList{}, err
	}
	if member.Role != domain.ChannelRoleCreator && member.Role != domain.ChannelRoleAdmin {
		return domain.PremiumBoostList{}, domain.ErrChannelAdminRequired
	}
	items := s.activeBoostSlotsForPeerLocked(domain.Peer{Type: domain.PeerTypeChannel, ID: channelID}, now, func(slot domain.PremiumBoostSlot) bool {
		return !gifts || slot.Gift || slot.Giveaway
	})
	return premiumBoostPage(items, start, limit), nil
}

func (s *ChannelStore) GetPremiumMyBoosts(_ context.Context, userID int64, now, premiumUntil int) (domain.PremiumMyBoosts, error) {
	if userID == 0 {
		return domain.PremiumMyBoosts{}, domain.ErrChannelInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.premiumMyBoostsLocked(userID, now, premiumUntil), nil
}

func (s *ChannelStore) ApplyPremiumBoost(_ context.Context, userID, channelID int64, slots []int, now, premiumUntil int) (domain.PremiumMyBoosts, error) {
	if userID == 0 || channelID == 0 || len(slots) == 0 || premiumUntil <= now {
		if premiumUntil <= now {
			return domain.PremiumMyBoosts{}, domain.ErrPremiumRequired
		}
		return domain.PremiumMyBoosts{}, domain.ErrChannelInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, _, err := s.channelAndMemberLocked(userID, channelID); err != nil {
		return domain.PremiumMyBoosts{}, err
	}
	peer := domain.Peer{Type: domain.PeerTypeChannel, ID: channelID}
	for _, slotID := range slots {
		if slotID != domain.DefaultPremiumBoostSlotID {
			return domain.PremiumMyBoosts{}, domain.ErrChannelInvalid
		}
		key := boostSlotKey{userID: userID, slot: slotID}
		slot, changed, err := domain.ApplyPremiumBoostSlot(
			s.boostSlots[key],
			userID,
			slotID,
			peer,
			now,
			premiumUntil,
			domain.DefaultPremiumBoostReassignCooldownSeconds,
		)
		if err != nil {
			return domain.PremiumMyBoosts{}, err
		}
		if !changed {
			continue
		}
		s.boostSlots[key] = slot
	}
	return s.premiumMyBoostsLocked(userID, now, premiumUntil), nil
}

func (s *ChannelStore) GetPremiumUserBoosts(_ context.Context, viewerUserID, channelID, targetUserID int64, now int) (domain.PremiumBoostList, error) {
	if viewerUserID == 0 || channelID == 0 || targetUserID == 0 {
		return domain.PremiumBoostList{}, domain.ErrChannelInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, member, err := s.channelAndMemberLocked(viewerUserID, channelID)
	if err != nil {
		return domain.PremiumBoostList{}, err
	}
	if member.Role != domain.ChannelRoleCreator && member.Role != domain.ChannelRoleAdmin {
		return domain.PremiumBoostList{}, domain.ErrChannelAdminRequired
	}
	items := s.activeBoostSlotsForPeerLocked(domain.Peer{Type: domain.PeerTypeChannel, ID: channelID}, now, func(slot domain.PremiumBoostSlot) bool {
		return slot.UserID == targetUserID
	})
	return premiumBoostPage(items, 0, len(items)), nil
}

func (s *ChannelStore) activeBoostSlotsForPeerLocked(peer domain.Peer, now int, keep func(domain.PremiumBoostSlot) bool) []domain.PremiumBoostSlot {
	items := make([]domain.PremiumBoostSlot, 0)
	for _, slot := range s.boostSlots {
		if slot.Peer != peer || !slot.Assigned(now) {
			continue
		}
		if keep != nil && !keep(slot) {
			continue
		}
		items = append(items, clonePremiumBoostSlot(slot))
	}
	sortPremiumBoostSlots(items)
	return items
}

func (s *ChannelStore) selfBoostsAppliedLocked(userID, channelID int64, now int) int {
	if userID == 0 || channelID == 0 {
		return 0
	}
	peer := domain.Peer{Type: domain.PeerTypeChannel, ID: channelID}
	total := 0
	for _, slot := range s.boostSlots {
		if slot.UserID == userID && slot.Peer == peer {
			total += slot.Weight(now)
		}
	}
	return total
}

func (s *ChannelStore) premiumMyBoostsLocked(userID int64, now, premiumUntil int) domain.PremiumMyBoosts {
	out := domain.PremiumMyBoosts{}
	if premiumUntil <= now {
		return out
	}
	foundBase := false
	for _, slot := range s.boostSlots {
		if slot.UserID != userID || !slot.Active(now) {
			continue
		}
		if slot.Slot == domain.DefaultPremiumBoostSlotID {
			foundBase = true
			if slot.Expires < premiumUntil {
				slot.Expires = premiumUntil
			}
		}
		out.Slots = append(out.Slots, clonePremiumBoostSlot(slot))
	}
	if !foundBase {
		out.Slots = append(out.Slots, domain.PremiumBoostSlot{
			UserID:     userID,
			Slot:       domain.DefaultPremiumBoostSlotID,
			Multiplier: 1,
		})
	}
	sortPremiumBoostSlots(out.Slots)
	channelIDs := make(map[int64]struct{})
	for _, slot := range out.Slots {
		if slot.Peer.Type == domain.PeerTypeChannel && slot.Peer.ID != 0 {
			channelIDs[slot.Peer.ID] = struct{}{}
		}
	}
	for channelID := range channelIDs {
		if channel, ok := s.channels[channelID]; ok && !channel.Deleted {
			out.Channels = append(out.Channels, cloneChannel(channel))
		}
	}
	sort.Slice(out.Channels, func(i, j int) bool { return out.Channels[i].ID < out.Channels[j].ID })
	return out
}

func premiumBoostPage(items []domain.PremiumBoostSlot, start, limit int) domain.PremiumBoostList {
	if start < 0 || start > len(items) {
		start = len(items)
	}
	if limit < 0 {
		limit = 0
	}
	end := start + limit
	if end > len(items) {
		end = len(items)
	}
	out := domain.PremiumBoostList{
		Count:  len(items),
		Boosts: append([]domain.PremiumBoostSlot(nil), items[start:end]...),
	}
	if end < len(items) {
		out.NextOffset = strconv.Itoa(end)
	}
	return out
}

func boostOffsetIndex(offset string) (int, error) {
	if offset == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(offset)
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, strconv.ErrSyntax
	}
	return n, nil
}

func sortPremiumBoostSlots(items []domain.PremiumBoostSlot) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].Date != items[j].Date {
			return items[i].Date > items[j].Date
		}
		if items[i].UserID != items[j].UserID {
			return items[i].UserID < items[j].UserID
		}
		return items[i].Slot < items[j].Slot
	})
}

func clonePremiumBoostSlot(in domain.PremiumBoostSlot) domain.PremiumBoostSlot {
	return in
}
