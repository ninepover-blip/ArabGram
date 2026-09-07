package rpc

import (
	"fmt"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func dialogAccountDeliveryEffects(snapshot store.DialogAccountMutationSnapshot) ([]store.DeliveryEffect, error) {
	if !snapshot.Changed {
		return nil, nil
	}
	m := snapshot.Mutation
	events := make([]domain.UpdateEvent, 0, 1)
	base := domain.UpdateEvent{Date: m.Date, PtsCount: 1}
	switch m.Kind {
	case store.DialogAccountSaveDraft:
		base.Type, base.Peer, base.MaxID = domain.UpdateEventDraftMessage, m.Draft.Peer, m.Draft.TopMessageID
		events = append(events, base)
	case store.DialogAccountDeleteDraft:
		base.Type, base.Peer, base.MaxID = domain.UpdateEventDraftMessage, m.Peer, m.TopMessageID
		events = append(events, base)
	case store.DialogAccountClearDrafts:
		for _, draft := range snapshot.Drafts {
			event := base
			event.Type, event.Peer, event.MaxID = domain.UpdateEventDraftMessage, draft.Peer, draft.TopMessageID
			events = append(events, event)
		}
	case store.DialogAccountSetPinned:
		base.Type, base.Peer, base.Bool, base.FolderID = domain.UpdateEventDialogPinned, m.Peer, m.Value, snapshot.FolderID
		events = append(events, base)
	case store.DialogAccountReorderPinned:
		base.Type, base.FolderID, base.Peers = domain.UpdateEventPinnedDialogs, m.FolderID, append([]domain.Peer(nil), m.Peers...)
		events = append(events, base)
	case store.DialogAccountSetUnreadMark:
		base.Type, base.Peer, base.Bool = domain.UpdateEventDialogUnreadMark, m.Peer, m.Value
		events = append(events, base)
	case store.DialogAccountHidePeerSettings:
		base.Type, base.Peer = domain.UpdateEventPeerSettings, m.Peer
		base.Settings = domain.PeerSettings{HiddenPeerSettingsBar: true}
		events = append(events, base)
	case store.DialogAccountUpsertFolder:
		folder := m.Folder
		base.Type, base.FilterID, base.DialogFilter = domain.UpdateEventDialogFilter, folder.ID, &folder
		events = append(events, base)
	case store.DialogAccountDeleteFolder:
		base.Type, base.FilterID = domain.UpdateEventDialogFilter, m.FolderID
		events = append(events, base)
	case store.DialogAccountReorderFolders:
		base.Type, base.FilterOrder = domain.UpdateEventDialogFilterOrder, append([]int(nil), m.FolderOrder...)
		events = append(events, base)
	case store.DialogAccountSetFolderTags:
		base.Type, base.TagsEnabled = domain.UpdateEventDialogFilters, m.Value
		events = append(events, base)
	case store.DialogAccountEditPeerFolders:
		base.Type, base.FolderPeers = domain.UpdateEventFolderPeers, append([]domain.FolderPeerUpdate(nil), m.FolderPeers...)
		events = append(events, base)
	case store.DialogAccountSetChannelViewForum:
		base.Type, base.Peer, base.Bool = domain.UpdateEventChannelViewForum, m.Peer, m.Value
		events = append(events, base)
	default:
		return nil, fmt.Errorf("unsupported dialog account mutation %q", m.Kind)
	}
	effects := make([]store.DeliveryEffect, 0, len(events))
	excludeAuthKeyID, excludeSessionID := [8]byte{}, int64(0)
	if m.Kind == store.DialogAccountSetChannelViewForum {
		excludeAuthKeyID, excludeSessionID = m.ExcludeAuthKeyID, m.ExcludeSessionID
	}
	for _, event := range events {
		effects = append(effects, store.AccountPTSDeliveryEffect(m.UserID, event, excludeAuthKeyID, excludeSessionID))
	}
	return effects, nil
}

func allocatedDialogEvent(snapshot store.DialogAccountMutationSnapshot) domain.UpdateEvent {
	if len(snapshot.Effects) == 0 {
		return domain.UpdateEvent{}
	}
	return snapshot.Effects[0].Event
}
