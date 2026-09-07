package memory

import (
	"context"
	"fmt"
	"reflect"
	"slices"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

// Tests must wire the same concrete stores used by their reads and updates.
// No private fallback stores are created by the blocklist aggregate.
func (s *ContactStore) AttachBlocklistStores(stories *StoryStore, privacy *PrivacyStore, users *UserStore, channels *ChannelStore) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blocklistStories = stories
	s.privacy = privacy
	s.blocklistUsers = users
	s.blocklistChannels = channels
}
func (s *ContactStore) ReadBlocklistIDs(_ context.Context, owner int64) ([]int64, error) {
	if owner <= 0 {
		return nil, store.ErrBlocklistInvalid
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]int64, 0, len(s.blocks[owner]))
	for id := range s.blocks[owner] {
		ids = append(ids, id)
	}
	if len(ids) > store.MaxDeliveryTargets {
		return nil, store.ErrBlocklistLimit
	}
	return store.BlocklistIDs(ids), nil
}

func (s *ContactStore) MutateBlocklist(_ context.Context, m store.BlocklistMutation, build store.DeliveryEffectsBuilder[store.BlocklistMutationSnapshot]) (store.BlocklistMutationSnapshot, error) {
	m, err := m.Prepare()
	if err != nil {
		return store.BlocklistMutationSnapshot{}, err
	}
	if build == nil {
		return store.BlocklistMutationSnapshot{}, store.ErrBlocklistRequired
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	stories, privacy, users, channels, events := s.blocklistStories, s.privacy, s.blocklistUsers, s.blocklistChannels, s.updateEvents
	if stories == nil || privacy == nil || users == nil || channels == nil || events == nil {
		return store.BlocklistMutationSnapshot{}, store.ErrBlocklistRequired
	}
	// The write lock also publishes the shared block visibility read model at the
	// same commit as contact rows/events; story mutations cannot cross snapshot.
	stories.mu.Lock()
	defer stories.mu.Unlock()
	privacy.mu.RLock()
	defer privacy.mu.RUnlock()
	users.mu.RLock()
	defer users.mu.RUnlock()
	channels.mu.RLock()
	defer channels.mu.RUnlock()
	current := make([]int64, 0, len(s.blocks[m.OwnerUserID]))
	for id := range s.blocks[m.OwnerUserID] {
		current = append(current, id)
	}
	changes, err := store.BlocklistDiff(m, current)
	if err != nil {
		return store.BlocklistMutationSnapshot{}, err
	}
	active := []domain.Story{}
	for _, story := range stories.stories {
		if story.Owner == (domain.Peer{Type: domain.PeerTypeUser, ID: m.OwnerUserID}) && story.Active(m.Date) {
			active = append(active, fanoutStorySnapshot(story))
		}
	}
	sortStoriesOldestFirst(active)
	if len(active) > domain.MaxStoryListLimit {
		return store.BlocklistMutationSnapshot{}, store.ErrBlocklistLimit
	}
	if !slices.EqualFunc(active, m.Stories, func(a, b domain.Story) bool { return reflect.DeepEqual(a, b) }) {
		return store.BlocklistMutationSnapshot{}, store.ErrBlocklistConflict
	}
	if len(changes) == 0 {
		return store.BlocklistMutationSnapshot{OwnerUserID: m.OwnerUserID, Date: m.Date}, nil
	}
	phone, found := privacy.rules[privacyStoreKey{ownerUserID: m.OwnerUserID, key: domain.PrivacyKeyPhoneNumber}]
	if !found {
		phone = domain.PrivacyRules{OwnerUserID: m.OwnerUserID, Key: domain.PrivacyKeyPhoneNumber, Rules: domain.DefaultPrivacyRules(domain.PrivacyKeyPhoneNumber)}
	}
	chats := store.BlocklistPrivacyChatIDs(phone)
	if len(chats) > store.MaxActiveChannelMemberPairs/len(changes) {
		return store.BlocklistMutationSnapshot{}, store.ErrActiveChannelMemberPairsLimit
	}
	facts := make([]store.BlocklistPeerFacts, 0, len(changes))
	for _, c := range changes {
		u, found := users.byID[c.PeerUserID]
		if !found {
			return store.BlocklistMutationSnapshot{}, store.ErrBlocklistInvalid
		}
		f := store.BlocklistPeerFacts{PeerUserID: c.PeerUserID, Bot: u.Bot, Premium: u.PremiumActiveAt(int64(m.Date))}
		for _, contact := range s.m[m.OwnerUserID].Contacts {
			if contact.User.ID == c.PeerUserID {
				f.Contact = true
				f.CloseFriend = contact.CloseFriend || contact.User.CloseFriend
			}
		}
		for _, contact := range s.m[c.PeerUserID].Contacts {
			if contact.User.ID == m.OwnerUserID {
				f.KnowsPhone = contact.Phone != ""
			}
		}
		for _, chat := range chats {
			if member, ok := channels.members[chat][c.PeerUserID]; ok && member.Status == domain.ChannelMemberActive {
				f.SharedChatIDs = append(f.SharedChatIDs, chat)
			}
		}
		facts = append(facts, f)
	}
	snapshot, err := store.BuildBlocklistSnapshot(m, changes, phone, facts)
	if err != nil {
		return store.BlocklistMutationSnapshot{}, err
	}
	frozen, err := store.CloneBlocklistSnapshot(snapshot)
	if err != nil {
		return store.BlocklistMutationSnapshot{}, err
	}
	effects, err := build(frozen)
	if err != nil {
		return store.BlocklistMutationSnapshot{}, fmt.Errorf("build blocklist delivery: %w", err)
	}
	if err := store.ValidateBlocklistEffects(snapshot, effects); err != nil {
		return store.BlocklistMutationSnapshot{}, err
	}
	working := make(map[int64]domain.BlockedContact, len(current)+len(changes))
	for id, v := range s.blocks[m.OwnerUserID] {
		working[id] = v
	}
	for _, c := range changes {
		if c.Blocked {
			working[c.PeerUserID] = domain.BlockedContact{User: domain.User{ID: c.PeerUserID}, Date: m.Date}
		} else {
			delete(working, c.PeerUserID)
		}
	}
	events.mu.Lock()
	defer events.mu.Unlock()
	for _, effect := range effects {
		event := events.appendLocked(effect.TargetUserID, effect.Event, true)
		events.dispatches[effect.TargetUserID] = append(events.dispatches[effect.TargetUserID], memoryUpdateDispatch{Pts: event.Pts})
	}
	s.blocks[m.OwnerUserID] = working
	shared := make(map[int64]bool, len(working))
	for id := range working {
		shared[id] = true
	}
	stories.blocked[m.OwnerUserID] = shared
	return store.CloneBlocklistSnapshot(snapshot)
}
