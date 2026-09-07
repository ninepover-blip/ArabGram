package memory

import (
	"context"
	"fmt"
	"sort"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func (s *StoryStore) CreateStoryWithDelivery(
	ctx context.Context,
	actorUserID int64,
	req domain.StoryCreateRequest,
	build store.DeliveryEffectsBuilder[store.StoryMutationSnapshot],
) (domain.StoryCreateResult, error) {
	return runMemoryStoryDelivery(ctx, s, build,
		func(working *StoryStore) (domain.StoryCreateResult, store.StoryMutationSnapshot, error) {
			result, err := working.createStory(ctx, req)
			snapshot := store.StoryMutationSnapshot{
				Kind: store.StoryMutationCreate, ActorUserID: actorUserID,
				Changed: err == nil && !result.Duplicate, Create: result,
			}
			if err != nil || result.Duplicate {
				return result, snapshot, err
			}
			members, err := working.storyChannelDeliveryMembersLocked(result.Story.Owner)
			if err != nil {
				return domain.StoryCreateResult{}, snapshot, err
			}
			store.AppendStoryChangeRequirements(&snapshot, result.Story, members)
			return result, snapshot, nil
		})
}

func (s *StoryStore) EditStoryWithDelivery(
	ctx context.Context,
	actorUserID int64,
	req domain.StoryEditRequest,
	build store.DeliveryEffectsBuilder[store.StoryMutationSnapshot],
) (domain.StoryEditResult, error) {
	return runMemoryStoryDelivery(ctx, s, build,
		func(working *StoryStore) (domain.StoryEditResult, store.StoryMutationSnapshot, error) {
			result, err := working.editStory(ctx, req)
			snapshot := store.StoryMutationSnapshot{
				Kind: store.StoryMutationEdit, ActorUserID: actorUserID,
				Changed: err == nil, Edit: result,
			}
			if err != nil {
				return result, snapshot, err
			}
			members, err := working.storyChannelDeliveryMembersLocked(result.Story.Owner)
			if err != nil {
				return domain.StoryEditResult{}, snapshot, err
			}
			store.AppendStoryChangeRequirements(&snapshot, result.Story, members)
			if req.UpdatePrivacy && result.Story.Owner.Type == domain.PeerTypeUser {
				facts, err := working.storyUserDeliveryFactsLocked(result.Story.Owner.ID,
					[]int{result.Story.ID}, memoryStoryAudienceSeedIDs(result.Previous, result.Story))
				if err != nil {
					return domain.StoryEditResult{}, snapshot, err
				}
				store.AppendStoryPrivacyFanoutRequirements(&snapshot, result.Previous, result.Story, facts, result.Story.Date)
			}
			return result, snapshot, nil
		})
}

func (s *StoryStore) DeleteStoriesWithDelivery(
	ctx context.Context,
	actorUserID int64,
	peer domain.Peer,
	ids []int,
	date int,
	build store.DeliveryEffectsBuilder[store.StoryMutationSnapshot],
) (domain.StoryMutationResult, error) {
	return runMemoryStoryDelivery(ctx, s, build,
		func(working *StoryStore) (domain.StoryMutationResult, store.StoryMutationSnapshot, error) {
			result, err := working.deleteStories(ctx, peer, ids, date)
			snapshot := store.StoryMutationSnapshot{
				Kind: store.StoryMutationDelete, ActorUserID: actorUserID,
				Changed: err == nil && len(result.Stories) > 0, Mutation: result,
			}
			if err != nil || len(result.Stories) == 0 {
				return result, snapshot, err
			}
			members, facts, err := working.storyMutationAudienceLocked(peer, result.Previous, result.Stories)
			if err != nil {
				return domain.StoryMutationResult{}, snapshot, err
			}
			previous := memoryStoriesByID(result.Previous)
			for _, story := range result.Stories {
				store.AppendStoryChangeRequirements(&snapshot, story, members)
				if peer.Type == domain.PeerTypeUser {
					store.AppendDeletedStoryFanoutRequirements(&snapshot, previous[story.ID], story, facts, date)
				}
			}
			return result, snapshot, nil
		})
}

func (s *StoryStore) TogglePinnedWithDelivery(
	ctx context.Context,
	actorUserID int64,
	peer domain.Peer,
	ids []int,
	pinned bool,
	date int,
	build store.DeliveryEffectsBuilder[store.StoryMutationSnapshot],
) (domain.StoryMutationResult, error) {
	return runMemoryStoryDelivery(ctx, s, build,
		func(working *StoryStore) (domain.StoryMutationResult, store.StoryMutationSnapshot, error) {
			result, err := working.togglePinned(ctx, peer, ids, pinned, date)
			snapshot := store.StoryMutationSnapshot{
				Kind: store.StoryMutationTogglePinned, ActorUserID: actorUserID,
				Changed: err == nil && len(result.Stories) > 0, Mutation: result,
			}
			if err != nil || len(result.Stories) == 0 {
				return result, snapshot, err
			}
			members, facts, err := working.storyMutationAudienceLocked(peer, result.Previous, result.Stories)
			if err != nil {
				return domain.StoryMutationResult{}, snapshot, err
			}
			previous := memoryStoriesByID(result.Previous)
			for _, story := range result.Stories {
				store.AppendStoryChangeRequirements(&snapshot, story, members)
				if peer.Type == domain.PeerTypeUser {
					store.AppendUnpinnedExpiredStoryFanoutRequirements(&snapshot, previous[story.ID], story, facts, date)
				}
			}
			return result, snapshot, nil
		})
}

func (s *StoryStore) MarkReadWithDelivery(
	ctx context.Context,
	viewerUserID int64,
	peer domain.Peer,
	maxID, date int,
	build store.DeliveryEffectsBuilder[store.StoryMutationSnapshot],
) (domain.StoryReadResult, error) {
	return runMemoryStoryDelivery(ctx, s, build,
		func(working *StoryStore) (domain.StoryReadResult, store.StoryMutationSnapshot, error) {
			result, err := working.markRead(ctx, viewerUserID, peer, maxID, date)
			snapshot := store.StoryMutationSnapshot{
				Kind: store.StoryMutationRead, ActorUserID: viewerUserID,
				Changed: err == nil && result.Advanced, Read: result,
			}
			if err == nil && result.Advanced {
				snapshot.Required = append(snapshot.Required, store.StoryDeliveryRequirement{
					TargetUserID: viewerUserID, Event: store.ReadStoriesEvent(result), ExcludeOrigin: true,
				})
			}
			return result, snapshot, err
		})
}

func (s *StoryStore) SetReactionWithDelivery(
	ctx context.Context,
	viewerUserID int64,
	peer domain.Peer,
	storyID int,
	reaction *domain.MessageReaction,
	date int,
	build store.DeliveryEffectsBuilder[store.StoryMutationSnapshot],
) (domain.StoryReactionResult, error) {
	return runMemoryStoryDelivery(ctx, s, build,
		func(working *StoryStore) (domain.StoryReactionResult, store.StoryMutationSnapshot, error) {
			result, err := working.setReaction(ctx, viewerUserID, peer, storyID, reaction, date)
			snapshot := store.StoryMutationSnapshot{
				Kind: store.StoryMutationReaction, ActorUserID: viewerUserID,
				Changed: err == nil && result.Changed, Reaction: result,
			}
			if err != nil || !result.Changed {
				return result, snapshot, err
			}
			snapshot.Required = append(snapshot.Required, store.StoryDeliveryRequirement{
				TargetUserID: viewerUserID, Event: store.SentStoryReactionEvent(result), ExcludeOrigin: true,
			})
			owner := result.Story.Owner
			if owner.ID == 0 {
				owner = result.Peer
			}
			if owner.Type == domain.PeerTypeUser && owner.ID > 0 && owner.ID != viewerUserID && result.Reaction != nil {
				snapshot.Required = append(snapshot.Required, store.StoryDeliveryRequirement{
					TargetUserID: owner.ID, Event: store.NewStoryReactionEvent(result),
				})
			}
			return result, snapshot, nil
		})
}

type memoryStoryDeliveryMutation[T any] func(*StoryStore) (T, store.StoryMutationSnapshot, error)

func runMemoryStoryDelivery[T any](
	ctx context.Context,
	s *StoryStore,
	build store.DeliveryEffectsBuilder[store.StoryMutationSnapshot],
	mutate memoryStoryDeliveryMutation[T],
) (T, error) {
	var zero T
	if s == nil || build == nil {
		return zero, store.ErrDeliveryOutboxRequired
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	events := s.updateEvents
	if events == nil {
		return zero, store.ErrDeliveryOutboxRequired
	}
	working := cloneMemoryStoryStoreLocked(s)
	result, snapshot, err := mutate(working)
	if err != nil {
		return zero, err
	}
	store.SortStoryDeliveryRequirements(snapshot.Required)
	effects, err := build(store.StoryMutationSnapshotForDelivery(snapshot))
	if err != nil {
		return zero, fmt.Errorf("build story delivery effects: %w", err)
	}
	if err := store.ValidateStoryMutationDeliveryEffects(snapshot, effects); err != nil {
		return zero, err
	}

	events.mu.Lock()
	defer events.mu.Unlock()
	applied := append([]store.DeliveryEffect(nil), effects...)
	for i := range applied {
		applied[i].Event = events.appendLocked(applied[i].TargetUserID, applied[i].Event, true)
		events.dispatches[applied[i].TargetUserID] = append(events.dispatches[applied[i].TargetUserID], memoryUpdateDispatch{
			Pts: applied[i].Event.Pts,
			ExcludeAuthKeyID: applied[i].ExcludeAuthKeyID,
			ExcludeSessionID: applied[i].ExcludeSessionID,
		})
	}
	snapshot.Effects = applied
	installMemoryStoryStoreLocked(s, working)
	return result, nil
}

func cloneMemoryStoryStoreLocked(s *StoryStore) *StoryStore {
	out := &StoryStore{
		stories: make(map[storyKey]domain.Story, len(s.stories)),
		read: make(map[storyReadKey]domain.StoryReadState, len(s.read)),
		views: make(map[storyViewKey]domain.StoryView, len(s.views)),
		exposures: make(map[storyViewKey]int, len(s.exposures)),
		hidden: make(map[storyHiddenKey]bool, len(s.hidden)),
		profiles: make(map[int64]domain.User, len(s.profiles)),
		contacts: make(map[int64]map[int64]domain.Contact, len(s.contacts)),
		blocked: make(map[int64]map[int64]bool, len(s.blocked)),
		channels: s.channels,
		members: make(map[int64]map[int64]bool, len(s.members)),
		updateEvents: s.updateEvents,
	}
	for key, story := range s.stories {
		out.stories[key] = cloneStory(story)
	}
	for key, state := range s.read {
		out.read[key] = state
	}
	for key, view := range s.views {
		view.Reaction = cloneReactionPtr(view.Reaction)
		out.views[key] = view
	}
	for key, date := range s.exposures {
		out.exposures[key] = date
	}
	for key, hidden := range s.hidden {
		out.hidden[key] = hidden
	}
	for id, profile := range s.profiles {
		out.profiles[id] = profile
	}
	for ownerID, contacts := range s.contacts {
		copyContacts := make(map[int64]domain.Contact, len(contacts))
		for userID, contact := range contacts {
			copyContacts[userID] = cloneContact(contact)
		}
		out.contacts[ownerID] = copyContacts
	}
	for ownerID, blocked := range s.blocked {
		copyBlocked := make(map[int64]bool, len(blocked))
		for userID, value := range blocked {
			copyBlocked[userID] = value
		}
		out.blocked[ownerID] = copyBlocked
	}
	for channelID, members := range s.members {
		copyMembers := make(map[int64]bool, len(members))
		for userID, value := range members {
			copyMembers[userID] = value
		}
		out.members[channelID] = copyMembers
	}
	return out
}

func installMemoryStoryStoreLocked(dst, src *StoryStore) {
	dst.stories, dst.read, dst.views = src.stories, src.read, src.views
	dst.exposures, dst.hidden = src.exposures, src.hidden
	dst.profiles, dst.contacts, dst.blocked = src.profiles, src.contacts, src.blocked
	dst.members = src.members
}

func (s *StoryStore) storyChannelDeliveryMembersLocked(peer domain.Peer) ([]int64, error) {
	if peer.Type != domain.PeerTypeChannel || peer.ID <= 0 {
		return nil, nil
	}
	ids := make([]int64, 0)
	if s.channels != nil {
		s.channels.mu.RLock()
		channel, exists := s.channels.channels[peer.ID]
		if exists && !channel.Deleted {
			for userID, member := range s.channels.members[peer.ID] {
				if userID > 0 && member.Status == domain.ChannelMemberActive && !member.BannedRights.ViewMessages {
					ids = append(ids, userID)
				}
			}
		}
		s.channels.mu.RUnlock()
	} else {
		for userID, active := range s.members[peer.ID] {
			if userID > 0 && active {
				ids = append(ids, userID)
			}
		}
	}
	if len(ids) > domain.MaxChannelRealtimeFanout {
		return nil, fmt.Errorf("channel story delivery audience exceeds %d", domain.MaxChannelRealtimeFanout)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

func (s *StoryStore) storyMutationAudienceLocked(peer domain.Peer, before, after []domain.Story) ([]int64, []store.StoryFanoutFact, error) {
	if peer.Type == domain.PeerTypeChannel {
		members, err := s.storyChannelDeliveryMembersLocked(peer)
		return members, nil, err
	}
	storyIDs := make([]int, 0, len(before))
	seedIDs := make([]int64, 0)
	for _, story := range before {
		storyIDs = append(storyIDs, story.ID)
		seedIDs = append(seedIDs, memoryStoryAudienceSeedIDs(story)...)
	}
	for _, story := range after {
		seedIDs = append(seedIDs, memoryStoryAudienceSeedIDs(story)...)
	}
	facts, err := s.storyUserDeliveryFactsLocked(peer.ID, storyIDs, seedIDs)
	return nil, facts, err
}

func (s *StoryStore) storyUserDeliveryFactsLocked(ownerUserID int64, storyIDs []int, seedIDs []int64) ([]store.StoryFanoutFact, error) {
	candidates := make(map[int64]struct{})
	for _, id := range seedIDs {
		if id > 0 && id != ownerUserID {
			candidates[id] = struct{}{}
		}
	}
	for id := range s.contacts[ownerUserID] {
		if id > 0 && id != ownerUserID {
			candidates[id] = struct{}{}
		}
	}
	wantedStories := make(map[int]struct{}, len(storyIDs))
	for _, id := range storyIDs {
		wantedStories[id] = struct{}{}
	}
	for key := range s.views {
		if key.peerType == domain.PeerTypeUser && key.peerID == ownerUserID {
			if _, ok := wantedStories[key.storyID]; ok && key.viewerID > 0 && key.viewerID != ownerUserID {
				candidates[key.viewerID] = struct{}{}
			}
		}
	}
	for key := range s.exposures {
		if key.peerType == domain.PeerTypeUser && key.peerID == ownerUserID {
			if _, ok := wantedStories[key.storyID]; ok && key.viewerID > 0 && key.viewerID != ownerUserID {
				candidates[key.viewerID] = struct{}{}
			}
		}
	}
	if len(candidates) > domain.MaxStoryPrivacyFanoutTargets {
		return nil, fmt.Errorf("user story delivery audience exceeds %d", domain.MaxStoryPrivacyFanoutTargets)
	}
	ids := make([]int64, 0, len(candidates))
	for id := range candidates {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	facts := make([]store.StoryFanoutFact, 0, len(ids))
	for _, id := range ids {
		contact, isContact := s.contacts[ownerUserID][id]
		facts = append(facts, store.StoryFanoutFact{
			UserID: id, IsContact: isContact,
			CloseFriend: contact.CloseFriend || contact.User.CloseFriend,
			StoryBlocked: s.blocked[ownerUserID][id],
		})
	}
	return facts, nil
}

func memoryStoryAudienceSeedIDs(stories ...domain.Story) []int64 {
	out := make([]int64, 0)
	for _, story := range stories {
		out = append(out, story.AllowUserIDs...)
		out = append(out, story.DisallowUserIDs...)
	}
	return out
}

func memoryStoriesByID(items []domain.Story) map[int]domain.Story {
	out := make(map[int]domain.Story, len(items))
	for _, story := range items {
		out[story.ID] = story
	}
	return out
}
