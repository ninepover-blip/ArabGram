package store

import (
	"fmt"

	"telesrv/internal/domain"
)

// DialogAccountMutationKind is a closed set of account-scoped dialog state
// transitions. Every successful transition is owned by one store transaction
// together with its account PTS event and dispatch row.
type DialogAccountMutationKind string

const (
	DialogAccountSaveDraft           DialogAccountMutationKind = "save_draft"
	DialogAccountDeleteDraft         DialogAccountMutationKind = "delete_draft"
	DialogAccountClearDrafts         DialogAccountMutationKind = "clear_drafts"
	DialogAccountSetPinned           DialogAccountMutationKind = "set_pinned"
	DialogAccountReorderPinned       DialogAccountMutationKind = "reorder_pinned"
	DialogAccountSetUnreadMark       DialogAccountMutationKind = "set_unread_mark"
	DialogAccountHidePeerSettings    DialogAccountMutationKind = "hide_peer_settings"
	DialogAccountUpsertFolder        DialogAccountMutationKind = "upsert_folder"
	DialogAccountDeleteFolder        DialogAccountMutationKind = "delete_folder"
	DialogAccountReorderFolders      DialogAccountMutationKind = "reorder_folders"
	DialogAccountSetFolderTags       DialogAccountMutationKind = "set_folder_tags"
	DialogAccountEditPeerFolders     DialogAccountMutationKind = "edit_peer_folders"
	DialogAccountSetChannelViewForum DialogAccountMutationKind = "set_channel_view_forum_as_messages"
)

// DialogAccountMutation is deliberately a tagged union rather than a bag of
// optional callbacks. Validate rejects fields which do not make sense for the
// selected transition before a durable store starts a transaction.
type DialogAccountMutation struct {
	Kind   DialogAccountMutationKind
	UserID int64
	Date   int

	Draft domain.DialogDraft
	Peer  domain.Peer
	Peers []domain.Peer

	Folder       domain.DialogFolder
	FolderID     int
	TopMessageID int
	FolderOrder  []int
	FolderPeers  []domain.FolderPeerUpdate
	Value        bool
	Force        bool
	Limit        int
	PinnedLimit  int

	// ExcludeAuthKeyID and ExcludeSessionID identify the physical origin which
	// already receives a real updateChannelViewForumAsMessages in the RPC
	// response. They are forbidden for dialog mutations whose RPC response does
	// not carry the corresponding update.
	ExcludeAuthKeyID [8]byte
	ExcludeSessionID int64
}

// DialogAccountMutationSnapshot is the immutable post-mutation projection
// supplied to the mandatory delivery builder. Effects contains the allocated
// events only on successful return from the owning store.
type DialogAccountMutationSnapshot struct {
	Mutation DialogAccountMutation
	Changed  bool
	FolderID int
	Drafts   []domain.DialogDraft
	Effects  []DeliveryEffect
}

func (m DialogAccountMutation) Validate() error {
	if m.UserID <= 0 {
		return fmt.Errorf("dialog account mutation user id is required")
	}
	hasAuthKey := m.ExcludeAuthKeyID != ([8]byte{})
	hasSession := m.ExcludeSessionID != 0
	if hasAuthKey != hasSession {
		return fmt.Errorf("dialog account mutation exclusion requires both raw auth key and session id")
	}
	switch m.Kind {
	case DialogAccountSaveDraft:
		if m.Draft.Peer.ID == 0 || m.Draft.Peer.Type == "" || m.Draft.Empty() {
			return fmt.Errorf("save draft mutation requires a non-empty draft")
		}
	case DialogAccountDeleteDraft:
		if m.Peer.ID == 0 || m.Peer.Type == "" || m.TopMessageID < 0 {
			return fmt.Errorf("delete draft mutation requires peer and non-negative top message id")
		}
	case DialogAccountClearDrafts:
		if m.Limit <= 0 || m.Limit > domain.MaxDialogDraftsPerUser {
			return fmt.Errorf("clear drafts mutation limit is invalid")
		}
	case DialogAccountSetPinned:
		if m.Peer.ID == 0 || (m.Peer.Type != domain.PeerTypeUser && m.Peer.Type != domain.PeerTypeChannel && m.Peer.Type != domain.PeerTypeCommunity && m.Peer.Type != domain.PeerTypeFolder) {
			return fmt.Errorf("set pinned mutation peer is invalid")
		}
		if m.Peer.Type == domain.PeerTypeFolder && m.Peer.ID != int64(domain.DialogArchiveFolderID) {
			return fmt.Errorf("only archive folder may be pinned")
		}
		if m.Value && m.Peer.Type != domain.PeerTypeFolder && m.PinnedLimit <= 0 {
			return fmt.Errorf("set pinned mutation requires a positive limit")
		}
	case DialogAccountReorderPinned:
		if m.FolderID != domain.DialogMainFolderID && m.FolderID != domain.DialogArchiveFolderID {
			return fmt.Errorf("reorder pinned mutation folder is invalid")
		}
		if m.PinnedLimit <= 0 || len(m.Peers) > m.PinnedLimit {
			return fmt.Errorf("reorder pinned mutation exceeds limit")
		}
		seen := make(map[domain.Peer]struct{}, len(m.Peers))
		for _, peer := range m.Peers {
			if peer.ID == 0 || peer.Type == "" {
				return fmt.Errorf("reorder pinned mutation contains invalid peer")
			}
			if m.FolderID != domain.DialogMainFolderID && peer.Type == domain.PeerTypeCommunity {
				return fmt.Errorf("community cannot be pinned outside the main folder")
			}
			if _, ok := seen[peer]; ok {
				return fmt.Errorf("reorder pinned mutation contains duplicate peer")
			}
			seen[peer] = struct{}{}
		}
	case DialogAccountSetUnreadMark, DialogAccountHidePeerSettings:
		if m.Peer.ID == 0 || (m.Peer.Type != domain.PeerTypeUser && m.Peer.Type != domain.PeerTypeChannel) {
			return fmt.Errorf("dialog state mutation peer is invalid")
		}
	case DialogAccountUpsertFolder:
		if m.Folder.ID < domain.DialogCustomFolderMinID {
			return fmt.Errorf("upsert folder mutation id is invalid")
		}
	case DialogAccountDeleteFolder:
		if m.FolderID < domain.DialogCustomFolderMinID {
			return fmt.Errorf("delete folder mutation id is invalid")
		}
	case DialogAccountReorderFolders:
		if len(m.FolderOrder) > domain.MaxDialogFolders {
			return fmt.Errorf("reorder folders mutation exceeds limit")
		}
	case DialogAccountSetFolderTags:
	case DialogAccountEditPeerFolders:
		if len(m.FolderPeers) == 0 || len(m.FolderPeers) > domain.MaxDialogFolderPeers {
			return fmt.Errorf("edit peer folders mutation size is invalid")
		}
		seen := make(map[domain.Peer]struct{}, len(m.FolderPeers))
		for _, item := range m.FolderPeers {
			if item.Peer.ID == 0 || (item.Peer.Type != domain.PeerTypeUser && item.Peer.Type != domain.PeerTypeChannel) ||
				(item.FolderID != domain.DialogMainFolderID && item.FolderID != domain.DialogArchiveFolderID) {
				return fmt.Errorf("edit peer folders mutation contains invalid item")
			}
			if _, ok := seen[item.Peer]; ok {
				return fmt.Errorf("edit peer folders mutation contains duplicate peer")
			}
			seen[item.Peer] = struct{}{}
		}
	case DialogAccountSetChannelViewForum:
		if m.Peer.ID == 0 || m.Peer.Type != domain.PeerTypeChannel {
			return fmt.Errorf("channel view-forum mutation requires a channel peer")
		}
		if !hasAuthKey {
			return fmt.Errorf("channel view-forum mutation requires an exact response origin exclusion")
		}
	default:
		return fmt.Errorf("unknown dialog account mutation kind %q", m.Kind)
	}
	if m.Kind != DialogAccountSetChannelViewForum && hasAuthKey {
		return fmt.Errorf("dialog account mutation %q must not exclude a response origin", m.Kind)
	}
	return nil
}

// ValidateDialogAccountEffects proves that a builder cannot redirect, exclude,
// pre-allocate or silently omit any account PTS fact belonging to the mutation.
func ValidateDialogAccountEffects(snapshot DialogAccountMutationSnapshot, effects []DeliveryEffect) error {
	want := 0
	if snapshot.Changed {
		want = 1
		if snapshot.Mutation.Kind == DialogAccountClearDrafts {
			want = len(snapshot.Drafts)
		}
	}
	if len(effects) != want {
		return fmt.Errorf("dialog account mutation delivery effects = %d, want %d", len(effects), want)
	}
	wantTypes := dialogAccountExpectedEvents(snapshot)
	for i, effect := range effects {
		if err := effect.Validate(); err != nil {
			return fmt.Errorf("dialog account mutation effect %d: %w", i, err)
		}
		if effect.Kind != DeliveryEffectAccountPTS || effect.TargetUserID != snapshot.Mutation.UserID {
			return fmt.Errorf("dialog account mutation effect %d has wrong kind or target", i)
		}
		wantAuthKey, wantSession := [8]byte{}, int64(0)
		if snapshot.Mutation.Kind == DialogAccountSetChannelViewForum {
			wantAuthKey, wantSession = snapshot.Mutation.ExcludeAuthKeyID, snapshot.Mutation.ExcludeSessionID
		}
		if effect.ExcludeAuthKeyID != wantAuthKey || effect.ExcludeSessionID != wantSession {
			return fmt.Errorf("dialog account mutation effect %d has wrong response origin exclusion", i)
		}
		if i >= len(wantTypes) || effect.Event.Type != wantTypes[i] || effect.Event.Pts != 0 || effect.Event.PtsCount != 1 {
			return fmt.Errorf("dialog account mutation effect %d has wrong event", i)
		}
		if effect.Event.Date != snapshot.Mutation.Date {
			return fmt.Errorf("dialog account mutation effect %d has wrong event date", i)
		}
		if err := validateDialogAccountEventSnapshot(snapshot, i, effect.Event); err != nil {
			return fmt.Errorf("dialog account mutation effect %d: %w", i, err)
		}
	}
	return nil
}

func dialogAccountExpectedEvents(snapshot DialogAccountMutationSnapshot) []domain.UpdateEventType {
	if !snapshot.Changed {
		return nil
	}
	switch snapshot.Mutation.Kind {
	case DialogAccountSaveDraft, DialogAccountDeleteDraft:
		return []domain.UpdateEventType{domain.UpdateEventDraftMessage}
	case DialogAccountClearDrafts:
		out := make([]domain.UpdateEventType, len(snapshot.Drafts))
		for i := range out {
			out[i] = domain.UpdateEventDraftMessage
		}
		return out
	case DialogAccountSetPinned:
		return []domain.UpdateEventType{domain.UpdateEventDialogPinned}
	case DialogAccountReorderPinned:
		return []domain.UpdateEventType{domain.UpdateEventPinnedDialogs}
	case DialogAccountSetUnreadMark:
		return []domain.UpdateEventType{domain.UpdateEventDialogUnreadMark}
	case DialogAccountHidePeerSettings:
		return []domain.UpdateEventType{domain.UpdateEventPeerSettings}
	case DialogAccountUpsertFolder, DialogAccountDeleteFolder:
		return []domain.UpdateEventType{domain.UpdateEventDialogFilter}
	case DialogAccountReorderFolders:
		return []domain.UpdateEventType{domain.UpdateEventDialogFilterOrder}
	case DialogAccountSetFolderTags:
		return []domain.UpdateEventType{domain.UpdateEventDialogFilters}
	case DialogAccountEditPeerFolders:
		return []domain.UpdateEventType{domain.UpdateEventFolderPeers}
	case DialogAccountSetChannelViewForum:
		return []domain.UpdateEventType{domain.UpdateEventChannelViewForum}
	default:
		return nil
	}
}

func validateDialogAccountEventSnapshot(snapshot DialogAccountMutationSnapshot, index int, event domain.UpdateEvent) error {
	m := snapshot.Mutation
	if event.UserID != 0 && event.UserID != m.UserID {
		return fmt.Errorf("event user id does not match mutation owner")
	}
	switch m.Kind {
	case DialogAccountSaveDraft:
		if event.Peer != m.Draft.Peer || event.MaxID != m.Draft.TopMessageID {
			return fmt.Errorf("draft event key does not match saved draft")
		}
	case DialogAccountDeleteDraft:
		if event.Peer != m.Peer || event.MaxID != m.TopMessageID {
			return fmt.Errorf("draft event key does not match deleted draft")
		}
	case DialogAccountClearDrafts:
		if index >= len(snapshot.Drafts) || event.Peer != snapshot.Drafts[index].Peer || event.MaxID != snapshot.Drafts[index].TopMessageID {
			return fmt.Errorf("draft event key does not match cleared draft")
		}
	case DialogAccountSetPinned:
		if event.Peer != m.Peer {
			return fmt.Errorf("event peer does not match mutation peer")
		}
		if event.Bool != m.Value || event.FolderID != snapshot.FolderID {
			return fmt.Errorf("dialog pinned event state does not match mutation")
		}
	case DialogAccountSetUnreadMark:
		if event.Peer != m.Peer || event.Bool != m.Value {
			return fmt.Errorf("dialog unread-mark event does not match mutation")
		}
	case DialogAccountHidePeerSettings:
		if event.Peer != m.Peer || !event.Settings.HiddenPeerSettingsBar {
			return fmt.Errorf("peer settings event does not match mutation")
		}
	case DialogAccountReorderPinned:
		if event.FolderID != m.FolderID || !samePeers(event.Peers, m.Peers) {
			return fmt.Errorf("pinned order event does not match mutation")
		}
	case DialogAccountUpsertFolder:
		if event.FilterID != m.Folder.ID || event.DialogFilter == nil || event.DialogFilter.ID != m.Folder.ID {
			return fmt.Errorf("dialog filter event does not match upsert")
		}
	case DialogAccountDeleteFolder:
		if event.FilterID != m.FolderID || event.DialogFilter != nil {
			return fmt.Errorf("dialog filter event does not match delete")
		}
	case DialogAccountReorderFolders:
		if !sameInts(event.FilterOrder, m.FolderOrder) {
			return fmt.Errorf("dialog filter order event does not match mutation")
		}
	case DialogAccountSetFolderTags:
		if event.TagsEnabled != m.Value {
			return fmt.Errorf("dialog filter tags event does not match mutation")
		}
	case DialogAccountEditPeerFolders:
		if !sameFolderPeers(event.FolderPeers, m.FolderPeers) {
			return fmt.Errorf("folder peers event does not match mutation")
		}
	case DialogAccountSetChannelViewForum:
		if event.Peer != m.Peer || event.Bool != m.Value {
			return fmt.Errorf("channel view-forum event does not match mutation")
		}
	}
	return nil
}

func samePeers(a, b []domain.Peer) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sameInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sameFolderPeers(a, b []domain.FolderPeerUpdate) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
