package rpc

import (
	"context"
	"sync"
	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func (s *captureDialogs) MutateAccountDialogs(_ context.Context, mutation store.DialogAccountMutation, effects store.DeliveryEffectsBuilder[store.DialogAccountMutationSnapshot]) (store.DialogAccountMutationSnapshot, error) {
	snapshot := store.DialogAccountMutationSnapshot{Mutation: mutation, Changed: true, FolderID: mutation.FolderID}
	switch mutation.Kind {
	case store.DialogAccountSaveDraft:
		s.savedDraft = mutation.Draft
	case store.DialogAccountDeleteDraft:
		s.deletedDraft.peer, s.deletedDraft.topMessageID = mutation.Peer, mutation.TopMessageID
	case store.DialogAccountClearDrafts:
		snapshot.Drafts = append([]domain.DialogDraft(nil), s.drafts...)
		s.drafts = nil
		snapshot.Changed = len(snapshot.Drafts) > 0
	case store.DialogAccountSetPinned:
		s.peers = []domain.Peer{mutation.Peer}
		if mutation.Peer.Type == domain.PeerTypeFolder {
			s.archivePinned = mutation.Value
		}
		if snapshot.FolderID == 0 {
			snapshot.FolderID = s.folderID
		}
	case store.DialogAccountReorderPinned:
		s.folderID, s.peers = mutation.FolderID, append([]domain.Peer(nil), mutation.Peers...)
		snapshot.Changed = !s.reorderNoChange
	case store.DialogAccountSetUnreadMark, store.DialogAccountHidePeerSettings:
		s.peers = []domain.Peer{mutation.Peer}
	case store.DialogAccountUpsertFolder:
		s.savedFolder = mutation.Folder
	case store.DialogAccountDeleteFolder:
		s.deletedFolderID = mutation.FolderID
	case store.DialogAccountReorderFolders:
		s.folderOrder = append([]int(nil), mutation.FolderOrder...)
	case store.DialogAccountSetFolderTags:
		s.tagsEnabled = mutation.Value
	case store.DialogAccountEditPeerFolders:
		s.folderPeers = append([]domain.FolderPeerUpdate(nil), mutation.FolderPeers...)
	}
	intents, err := effects(snapshot)
	if err != nil {
		return store.DialogAccountMutationSnapshot{}, err
	}
	for i := range intents {
		intents[i].Event.Pts = i + 1
	}
	snapshot.Effects = intents
	s.mutations = append(s.mutations, mutation)
	s.effects = append(s.effects, intents...)
	return snapshot, nil
}

type captureDialogs struct {
	mu              sync.Mutex
	list            domain.DialogList
	pinnedList      domain.DialogList
	hasPinnedList   bool
	peerList        domain.DialogList
	hashCheck       domain.DialogHashCheck
	folderList      domain.DialogFolderList
	filter          domain.DialogFilter
	filters         []domain.DialogFilter
	hashFilter      domain.DialogFilter
	getDialogsCalls int
	hashCalls       int
	peers           []domain.Peer
	folderID        int
	archivePinned   bool
	savedFolder     domain.DialogFolder
	deletedFolderID int
	folderOrder     []int
	tagsEnabled     bool
	folderPeers     []domain.FolderPeerUpdate
	drafts          []domain.DialogDraft
	savedDraft      domain.DialogDraft
	deletedDraft    struct {
		peer         domain.Peer
		topMessageID int
	}
	invalidatedDialogs []struct {
		userID int64
		peer   domain.Peer
	}
	reorderNoChange bool
	mutations       []store.DialogAccountMutation
	effects         []store.DeliveryEffect
}

func (s *captureDialogs) capturedEvents() []domain.UpdateEvent {
	events := make([]domain.UpdateEvent, 0, len(s.effects))
	for _, effect := range s.effects {
		events = append(events, effect.Event)
	}
	return events
}

func (s *captureDialogs) capturedEffectsHaveNoExclusion() bool {
	for _, effect := range s.effects {
		if effect.ExcludeAuthKeyID != ([8]byte{}) || effect.ExcludeSessionID != 0 {
			return false
		}
	}
	return true
}

func (s *captureDialogs) InvalidateDialog(userID int64, peer domain.Peer) {
	s.invalidatedDialogs = append(s.invalidatedDialogs, struct {
		userID int64
		peer   domain.Peer
	}{userID: userID, peer: peer})
}

func (s *captureDialogs) GetDialogsHash(_ context.Context, _ int64, filter domain.DialogFilter) (domain.DialogHashCheck, error) {
	s.hashCalls++
	s.hashFilter = filter
	return s.hashCheck, nil
}

func (s *captureDialogs) GetDialogs(_ context.Context, _ int64, filter domain.DialogFilter) (domain.DialogList, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getDialogsCalls++
	s.filter = filter
	s.filters = append(s.filters, filter)
	if filter.PinnedOnly && s.hasPinnedList {
		return s.pinnedList, nil
	}
	return s.list, nil
}

func (s *captureDialogs) GetPeerDialogs(_ context.Context, _ int64, peers []domain.Peer) (domain.DialogList, error) {
	s.peers = append([]domain.Peer(nil), peers...)
	return s.peerList, nil
}

func (s *captureDialogs) SaveDraft(_ context.Context, _ int64, draft domain.DialogDraft) (bool, error) {
	s.savedDraft = draft
	return true, nil
}

func (s *captureDialogs) DeleteDraft(_ context.Context, _ int64, peer domain.Peer, topMessageID int) (bool, error) {
	s.deletedDraft.peer = peer
	s.deletedDraft.topMessageID = topMessageID
	return true, nil
}

func (s *captureDialogs) GetDraft(_ context.Context, _ int64, peer domain.Peer, topMessageID int) (domain.DialogDraft, bool, error) {
	for _, draft := range s.drafts {
		if draft.Peer == peer && draft.TopMessageID == topMessageID {
			return draft, true, nil
		}
	}
	return domain.DialogDraft{}, false, nil
}

func (s *captureDialogs) ListDrafts(_ context.Context, _ int64, _ int) ([]domain.DialogDraft, error) {
	return append([]domain.DialogDraft(nil), s.drafts...), nil
}

func (s *captureDialogs) ClearDrafts(_ context.Context, _ int64, _ int) ([]domain.DialogDraft, error) {
	drafts := append([]domain.DialogDraft(nil), s.drafts...)
	s.drafts = nil
	return drafts, nil
}

func (s *captureDialogs) TogglePinned(_ context.Context, _ int64, peer domain.Peer, _ bool) (bool, int, error) {
	s.peers = []domain.Peer{peer}
	return true, s.folderID, nil
}

func (s *captureDialogs) ToggleArchivePinned(_ context.Context, _ int64, pinned bool) (bool, error) {
	s.archivePinned = pinned
	return true, nil
}

func (s *captureDialogs) ReorderPinned(_ context.Context, _ int64, folderID int, order []domain.Peer, _ bool) (bool, error) {
	s.folderID = folderID
	s.peers = append([]domain.Peer(nil), order...)
	return !s.reorderNoChange, nil
}

func (s *captureDialogs) MarkUnread(_ context.Context, _ int64, peer domain.Peer, _ bool) (bool, error) {
	s.peers = []domain.Peer{peer}
	return true, nil
}

func (s *captureDialogs) UnreadMarks(_ context.Context, _ int64) ([]domain.Peer, error) {
	return s.peers, nil
}

func (s *captureDialogs) HidePeerSettingsBar(_ context.Context, _ int64, peer domain.Peer) (bool, error) {
	s.peers = []domain.Peer{peer}
	return true, nil
}

func (s *captureDialogs) PeerSettingsBarHidden(_ context.Context, _ int64, _ domain.Peer) (bool, error) {
	return false, nil
}

func (s *captureDialogs) GetDialogFolders(_ context.Context, _ int64) (domain.DialogFolderList, error) {
	return s.folderList, nil
}

func (s *captureDialogs) SaveDialogFolder(_ context.Context, _ int64, folder domain.DialogFolder) error {
	s.savedFolder = folder
	return nil
}

func (s *captureDialogs) DeleteDialogFolder(_ context.Context, _ int64, folderID int) error {
	s.deletedFolderID = folderID
	return nil
}

func (s *captureDialogs) ReorderDialogFolders(_ context.Context, _ int64, order []int) error {
	s.folderOrder = append([]int(nil), order...)
	return nil
}

func (s *captureDialogs) ToggleDialogFolderTags(_ context.Context, _ int64, enabled bool) error {
	s.tagsEnabled = enabled
	return nil
}

func (s *captureDialogs) EditPeerFolders(_ context.Context, _ int64, peers []domain.FolderPeerUpdate) error {
	s.folderPeers = append([]domain.FolderPeerUpdate(nil), peers...)
	return nil
}
