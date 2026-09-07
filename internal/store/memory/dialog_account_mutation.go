package memory

import (
	"context"
	"fmt"
	"reflect"
	"sort"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

// MutateAccountDialogs mirrors the PostgreSQL aggregate without mutating live
// maps until the mandatory builder and every effect have been validated.
func (s *DialogStore) MutateAccountDialogs(
	_ context.Context,
	mutation store.DialogAccountMutation,
	build store.DeliveryEffectsBuilder[store.DialogAccountMutationSnapshot],
) (store.DialogAccountMutationSnapshot, error) {
	if build == nil {
		return store.DialogAccountMutationSnapshot{}, store.ErrDeliveryOutboxRequired
	}
	if err := mutation.Validate(); err != nil {
		return store.DialogAccountMutationSnapshot{}, err
	}
	s.mu.RLock()
	events, channels, communities := s.updateEvents, s.channels, s.communities
	s.mu.RUnlock()
	if events == nil {
		return store.DialogAccountMutationSnapshot{}, store.ErrDeliveryOutboxRequired
	}
	// Community already owns Community -> Dialog/Channel in its view and link
	// transactions, so the aggregate follows that order and never introduces
	// the inverse Dialog -> Community edge.
	if communities != nil {
		communities.mu.Lock()
		defer communities.mu.Unlock()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.updateEvents != events || s.channels != channels || s.communities != communities {
		return store.DialogAccountMutationSnapshot{}, fmt.Errorf("dialog aggregate dependencies changed during mutation")
	}
	if channels != nil {
		channels.mu.Lock()
		defer channels.mu.Unlock()
	}

	state := cloneMemoryDialogAccountState(s, channels, communities, mutation.UserID)
	snapshot, err := state.mutate(mutation, channels, communities)
	if err != nil {
		return store.DialogAccountMutationSnapshot{}, err
	}
	effects, err := build(cloneMemoryDialogSnapshot(snapshot))
	if err != nil {
		return store.DialogAccountMutationSnapshot{}, fmt.Errorf("build dialog account delivery effects: %w", err)
	}
	if err := store.ValidateDialogAccountEffects(snapshot, effects); err != nil {
		return store.DialogAccountMutationSnapshot{}, err
	}

	events.mu.Lock()
	defer events.mu.Unlock()
	applied := cloneMemoryDialogEffects(effects)
	for i := range applied {
		applied[i].Event = events.appendLocked(mutation.UserID, applied[i].Event, true)
		events.dispatches[mutation.UserID] = append(events.dispatches[mutation.UserID], memoryUpdateDispatch{Pts: applied[i].Event.Pts})
	}
	state.install(s, channels, communities, mutation.UserID)
	snapshot.Effects = applied
	return cloneMemoryDialogSnapshot(snapshot), nil
}

type memoryDialogAccountState struct {
	dialogs       domain.DialogList
	drafts        map[dialogDraftKey]domain.DialogDraft
	folders       map[int]domain.DialogFolder
	folderOrder   []int
	folderTags    bool
	archivePinned bool
	archiveSet    bool

	channelDialogs map[int64]domain.ChannelDialog
	channelMembers map[int64]domain.ChannelMember
	communityState map[int64]domain.CommunityUserState
}

func cloneMemoryDialogAccountState(s *DialogStore, channels *ChannelStore, communities *CommunityStore, userID int64) memoryDialogAccountState {
	list := s.m[userID]
	list.Dialogs = cloneDialogs(list.Dialogs)
	list.Messages = cloneMessages(list.Messages)
	list.Users = append([]domain.User(nil), list.Users...)
	drafts := make(map[dialogDraftKey]domain.DialogDraft, len(s.drafts[userID]))
	for key, draft := range s.drafts[userID] {
		drafts[key] = cloneDialogDraft(draft)
	}
	folders := make(map[int]domain.DialogFolder, len(s.folders[userID]))
	for id, folder := range s.folders[userID] {
		folders[id] = cloneDialogFolder(folder)
	}
	state := memoryDialogAccountState{
		dialogs: list, drafts: drafts, folders: folders,
		folderOrder: append([]int(nil), s.folderOrder[userID]...), folderTags: s.folderTags[userID],
		channelDialogs: map[int64]domain.ChannelDialog{}, channelMembers: map[int64]domain.ChannelMember{},
		communityState: map[int64]domain.CommunityUserState{},
	}
	state.archivePinned, state.archiveSet = s.archivePinned[userID]
	if channels != nil {
		for id, dialog := range channels.dialogs[userID] {
			state.channelDialogs[id] = dialog
		}
		for id, byUser := range channels.members {
			if member, ok := byUser[userID]; ok {
				state.channelMembers[id] = member
			}
		}
	}
	if communities != nil {
		for id, byUser := range communities.states {
			if item, ok := byUser[userID]; ok {
				state.communityState[id] = item
			}
		}
	}
	return state
}

func (s *memoryDialogAccountState) mutate(m store.DialogAccountMutation, channels *ChannelStore, communities *CommunityStore) (store.DialogAccountMutationSnapshot, error) {
	snapshot := store.DialogAccountMutationSnapshot{Mutation: cloneMemoryDialogMutation(m)}
	switch m.Kind {
	case store.DialogAccountSaveDraft:
		key := draftKey(m.Draft.Peer, m.Draft.TopMessageID)
		current, found := s.drafts[key]
		if found {
			current.Date = 0
			candidate := cloneDialogDraft(m.Draft)
			candidate.Date = 0
			if reflect.DeepEqual(current, candidate) {
				break
			}
		}
		s.drafts[key] = cloneDialogDraft(m.Draft)
		snapshot.Changed = true
	case store.DialogAccountDeleteDraft:
		key := draftKey(m.Peer, m.TopMessageID)
		if _, ok := s.drafts[key]; ok {
			delete(s.drafts, key)
			snapshot.Changed = true
		}
	case store.DialogAccountClearDrafts:
		items := make([]domain.DialogDraft, 0, len(s.drafts))
		for _, draft := range s.drafts {
			items = append(items, cloneDialogDraft(draft))
		}
		sortDialogDrafts(items)
		if len(items) > m.Limit {
			items = items[:m.Limit]
		}
		for _, draft := range items {
			delete(s.drafts, draftKey(draft.Peer, draft.TopMessageID))
		}
		snapshot.Drafts, snapshot.Changed = items, len(items) > 0
	case store.DialogAccountSetPinned:
		changed, folderID, err := s.setPinned(m, channels, communities)
		if err != nil {
			return snapshot, err
		}
		snapshot.Changed, snapshot.FolderID = changed, folderID
	case store.DialogAccountReorderPinned:
		before := s.pinnedState(m.FolderID)
		s.reorderPinned(m)
		snapshot.Changed = !reflect.DeepEqual(before, s.pinnedState(m.FolderID))
	case store.DialogAccountSetUnreadMark:
		if m.Peer.Type == domain.PeerTypeChannel {
			member, ok := s.channelMembers[m.Peer.ID]
			if !ok || member.Status != domain.ChannelMemberActive {
				break
			}
			dialog := s.channelDialogs[m.Peer.ID]
			if dialog.UnreadMark != m.Value {
				dialog.UserID, dialog.ChannelID, dialog.UnreadMark = m.UserID, m.Peer.ID, m.Value
				member.UnreadMark = m.Value
				s.channelDialogs[m.Peer.ID], s.channelMembers[m.Peer.ID] = dialog, member
				snapshot.Changed = true
			}
		} else if i := findMemoryDialog(s.dialogs.Dialogs, m.Peer); i >= 0 && s.dialogs.Dialogs[i].UnreadMark != m.Value {
			s.dialogs.Dialogs[i].UnreadMark = m.Value
			snapshot.Changed = true
		}
	case store.DialogAccountHidePeerSettings:
		if i := findMemoryDialog(s.dialogs.Dialogs, m.Peer); i >= 0 && !s.dialogs.Dialogs[i].PeerSettingsBarHidden {
			s.dialogs.Dialogs[i].PeerSettingsBarHidden = true
			snapshot.Changed = true
		}
	case store.DialogAccountUpsertFolder:
		current, ok := s.folders[m.Folder.ID]
		if !ok || !reflect.DeepEqual(current, m.Folder) {
			s.folders[m.Folder.ID] = cloneDialogFolder(m.Folder)
			if !containsInt(s.folderOrder, m.Folder.ID) {
				s.folderOrder = append(s.folderOrder, m.Folder.ID)
			}
			snapshot.Changed = true
		}
	case store.DialogAccountDeleteFolder:
		if _, ok := s.folders[m.FolderID]; ok {
			delete(s.folders, m.FolderID)
			s.folderOrder = removeInt(s.folderOrder, m.FolderID)
			snapshot.Changed = true
		}
	case store.DialogAccountReorderFolders:
		before := append([]int(nil), s.folderOrder...)
		s.reorderFolders(m.FolderOrder)
		snapshot.Changed = !reflect.DeepEqual(before, s.folderOrder)
	case store.DialogAccountSetFolderTags:
		if s.folderTags != m.Value {
			s.folderTags, snapshot.Changed = m.Value, true
		}
	case store.DialogAccountEditPeerFolders:
		before := s.folderPeerState(m.FolderPeers)
		for _, item := range m.FolderPeers {
			if item.Peer.Type == domain.PeerTypeChannel {
				member, ok := s.channelMembers[item.Peer.ID]
				if !ok || member.Status != domain.ChannelMemberActive {
					continue
				}
				dialog := s.channelDialogs[item.Peer.ID]
				dialog.UserID, dialog.ChannelID = m.UserID, item.Peer.ID
				if dialog.FolderID != item.FolderID {
					dialog.Pinned, dialog.PinnedOrder = false, 0
				}
				dialog.FolderID = item.FolderID
				s.channelDialogs[item.Peer.ID] = dialog
				continue
			}
			if i := findMemoryDialog(s.dialogs.Dialogs, item.Peer); i >= 0 {
				if s.dialogs.Dialogs[i].FolderID != item.FolderID {
					s.dialogs.Dialogs[i].Pinned, s.dialogs.Dialogs[i].PinnedOrder = false, 0
				}
				s.dialogs.Dialogs[i].FolderID = item.FolderID
			}
		}
		snapshot.Changed = !reflect.DeepEqual(before, s.folderPeerState(m.FolderPeers))
	case store.DialogAccountSetChannelViewForum:
		if channels == nil {
			break
		}
		channel, channelOK := channels.channels[m.Peer.ID]
		member, memberOK := s.channelMembers[m.Peer.ID]
		if !channelOK || channel.Deleted || !memberOK || member.Status != domain.ChannelMemberActive {
			break
		}
		dialog := s.channelDialogs[m.Peer.ID]
		if dialog.ViewForumAsMessages != m.Value {
			dialog.UserID, dialog.ChannelID = m.UserID, m.Peer.ID
			dialog.ViewForumAsMessages = m.Value
			s.channelDialogs[m.Peer.ID] = dialog
			snapshot.Changed = true
		}
	}
	return snapshot, nil
}

func (s *memoryDialogAccountState) setPinned(m store.DialogAccountMutation, channels *ChannelStore, communities *CommunityStore) (bool, int, error) {
	if m.Peer.Type == domain.PeerTypeFolder {
		current := true
		if s.archiveSet {
			current = s.archivePinned
		}
		if current == m.Value {
			return false, 0, nil
		}
		s.archivePinned, s.archiveSet = m.Value, true
		return true, 0, nil
	}
	folderID, pinned, valid := s.onePinState(m.Peer, communities)
	if !valid || pinned == m.Value {
		return false, folderID, nil
	}
	if m.Value && len(s.pinnedState(folderID)) >= m.PinnedLimit {
		return false, folderID, domain.ErrPinnedDialogsTooMuch
	}
	maxOrder := 0
	for _, item := range s.pinnedState(folderID) {
		if item.Order > maxOrder {
			maxOrder = item.Order
		}
	}
	order := 0
	if m.Value {
		order = maxOrder + 1
	}
	switch m.Peer.Type {
	case domain.PeerTypeChannel:
		dialog := s.channelDialogs[m.Peer.ID]
		dialog.UserID, dialog.ChannelID, dialog.Pinned, dialog.PinnedOrder = m.UserID, m.Peer.ID, m.Value, order
		s.channelDialogs[m.Peer.ID] = dialog
	case domain.PeerTypeCommunity:
		state := s.communityState[m.Peer.ID]
		state.Pinned, state.PinnedOrder = m.Value, order
		s.communityState[m.Peer.ID] = state
	default:
		i := findMemoryDialog(s.dialogs.Dialogs, m.Peer)
		s.dialogs.Dialogs[i].Pinned, s.dialogs.Dialogs[i].PinnedOrder = m.Value, order
	}
	return true, folderID, nil
}

func (s *memoryDialogAccountState) onePinState(peer domain.Peer, communities *CommunityStore) (int, bool, bool) {
	switch peer.Type {
	case domain.PeerTypeChannel:
		member, ok := s.channelMembers[peer.ID]
		if !ok || member.Status != domain.ChannelMemberActive {
			return 0, false, false
		}
		dialog := s.channelDialogs[peer.ID]
		return dialog.FolderID, dialog.Pinned, true
	case domain.PeerTypeCommunity:
		state, ok := s.communityState[peer.ID]
		if !ok || !state.Collapsed || communities == nil {
			return 0, false, false
		}
		member, ok := communities.members[peer.ID][state.UserID]
		if !ok || member.Status != domain.CommunityMemberActive {
			return 0, false, false
		}
		return domain.DialogMainFolderID, state.Pinned, true
	default:
		i := findMemoryDialog(s.dialogs.Dialogs, peer)
		if i < 0 {
			return 0, false, false
		}
		return s.dialogs.Dialogs[i].FolderID, s.dialogs.Dialogs[i].Pinned, true
	}
}

func (s *memoryDialogAccountState) reorderPinned(m store.DialogAccountMutation) {
	positions := make(map[domain.Peer]int, len(m.Peers))
	for i, peer := range m.Peers {
		positions[peer] = len(m.Peers) - i
	}
	for i := range s.dialogs.Dialogs {
		dialog := &s.dialogs.Dialogs[i]
		if dialog.FolderID != m.FolderID {
			continue
		}
		if pos, ok := positions[dialog.Peer]; ok {
			dialog.Pinned, dialog.PinnedOrder = true, pos
		} else if m.Force {
			dialog.Pinned, dialog.PinnedOrder = false, 0
		}
	}
	for id, dialog := range s.channelDialogs {
		if dialog.FolderID != m.FolderID {
			continue
		}
		peer := domain.Peer{Type: domain.PeerTypeChannel, ID: id}
		if pos, ok := positions[peer]; ok {
			dialog.Pinned, dialog.PinnedOrder = true, pos
		} else if m.Force {
			dialog.Pinned, dialog.PinnedOrder = false, 0
		}
		s.channelDialogs[id] = dialog
	}
	if m.FolderID == domain.DialogMainFolderID {
		for id, state := range s.communityState {
			if !state.Collapsed {
				continue
			}
			peer := domain.Peer{Type: domain.PeerTypeCommunity, ID: id}
			if pos, ok := positions[peer]; ok {
				state.Pinned, state.PinnedOrder = true, pos
			} else if m.Force {
				state.Pinned, state.PinnedOrder = false, 0
			}
			s.communityState[id] = state
		}
	}
}

type memoryPinnedState struct {
	Peer  domain.Peer
	Order int
}

func (s *memoryDialogAccountState) pinnedState(folderID int) []memoryPinnedState {
	out := []memoryPinnedState{}
	for _, dialog := range s.dialogs.Dialogs {
		if dialog.FolderID == folderID && dialog.Pinned {
			out = append(out, memoryPinnedState{dialog.Peer, dialog.PinnedOrder})
		}
	}
	for id, dialog := range s.channelDialogs {
		if dialog.FolderID == folderID && dialog.Pinned {
			out = append(out, memoryPinnedState{domain.Peer{Type: domain.PeerTypeChannel, ID: id}, dialog.PinnedOrder})
		}
	}
	if folderID == domain.DialogMainFolderID {
		for id, state := range s.communityState {
			if state.Collapsed && state.Pinned {
				out = append(out, memoryPinnedState{domain.Peer{Type: domain.PeerTypeCommunity, ID: id}, state.PinnedOrder})
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Peer.Type != out[j].Peer.Type {
			return out[i].Peer.Type < out[j].Peer.Type
		}
		return out[i].Peer.ID < out[j].Peer.ID
	})
	return out
}

func (s *memoryDialogAccountState) reorderFolders(order []int) {
	seen := map[int]struct{}{}
	next := make([]int, 0, len(s.folders))
	for _, id := range order {
		if id >= domain.DialogCustomFolderMinID {
			if _, ok := s.folders[id]; ok {
				if _, dup := seen[id]; !dup {
					seen[id] = struct{}{}
					next = append(next, id)
				}
			}
		}
	}
	remaining := make([]int, 0, len(s.folders))
	for id := range s.folders {
		if _, ok := seen[id]; !ok {
			remaining = append(remaining, id)
		}
	}
	sort.Ints(remaining)
	s.folderOrder = append(next, remaining...)
}

func (s *memoryDialogAccountState) folderPeerState(items []domain.FolderPeerUpdate) map[domain.Peer]int {
	out := make(map[domain.Peer]int, len(items))
	for _, item := range items {
		if item.Peer.Type == domain.PeerTypeChannel {
			if dialog, ok := s.channelDialogs[item.Peer.ID]; ok {
				out[item.Peer] = dialog.FolderID
			} else {
				out[item.Peer] = -1
			}
		} else if i := findMemoryDialog(s.dialogs.Dialogs, item.Peer); i >= 0 {
			out[item.Peer] = s.dialogs.Dialogs[i].FolderID
		} else {
			out[item.Peer] = -1
		}
	}
	return out
}

func (s *memoryDialogAccountState) install(dialogs *DialogStore, channels *ChannelStore, communities *CommunityStore, userID int64) {
	dialogs.m[userID], dialogs.drafts[userID], dialogs.folders[userID] = s.dialogs, s.drafts, s.folders
	dialogs.folderOrder[userID], dialogs.folderTags[userID] = append([]int(nil), s.folderOrder...), s.folderTags
	if s.archiveSet {
		dialogs.archivePinned[userID] = s.archivePinned
	} else {
		delete(dialogs.archivePinned, userID)
	}
	if channels != nil {
		channels.dialogs[userID] = s.channelDialogs
		for id, member := range s.channelMembers {
			if channels.members[id] == nil {
				channels.members[id] = map[int64]domain.ChannelMember{}
			}
			channels.members[id][userID] = member
		}
	}
	if communities != nil {
		for id, state := range s.communityState {
			if communities.states[id] == nil {
				communities.states[id] = map[int64]domain.CommunityUserState{}
			}
			communities.states[id][userID] = state
		}
	}
}

func findMemoryDialog(items []domain.Dialog, peer domain.Peer) int {
	for i := range items {
		if items[i].Peer == peer {
			return i
		}
	}
	return -1
}

func cloneMemoryDialogMutation(in store.DialogAccountMutation) store.DialogAccountMutation {
	in.Draft = cloneDialogDraft(in.Draft)
	in.Peers = append([]domain.Peer(nil), in.Peers...)
	in.Folder = cloneDialogFolder(in.Folder)
	in.FolderOrder = append([]int(nil), in.FolderOrder...)
	in.FolderPeers = append([]domain.FolderPeerUpdate(nil), in.FolderPeers...)
	return in
}

func cloneMemoryDialogSnapshot(in store.DialogAccountMutationSnapshot) store.DialogAccountMutationSnapshot {
	in.Mutation = cloneMemoryDialogMutation(in.Mutation)
	in.Drafts = make([]domain.DialogDraft, len(in.Drafts))
	for i := range in.Drafts {
		in.Drafts[i] = cloneDialogDraft(in.Drafts[i])
	}
	in.Effects = cloneMemoryDialogEffects(in.Effects)
	return in
}

func cloneMemoryDialogEffects(in []store.DeliveryEffect) []store.DeliveryEffect {
	out := append([]store.DeliveryEffect(nil), in...)
	for i := range out {
		out[i].Payload = append([]byte(nil), out[i].Payload...)
		out[i].Event.Peers = append([]domain.Peer(nil), out[i].Event.Peers...)
		out[i].Event.FilterOrder = append([]int(nil), out[i].Event.FilterOrder...)
		out[i].Event.FolderPeers = append([]domain.FolderPeerUpdate(nil), out[i].Event.FolderPeers...)
	}
	return out
}
