package tdesktop

import "telesrv/internal/domain"

// ShouldMergePinnedIntoInitialDialogs reports whether a TDesktop main-list
// messages.getDialogs request should carry pinned/archive rows inline.
//
// TDesktop starts the main list with getDialogs(exclude_pinned=true) and loads
// pinned rows through getPinnedDialogs in parallel. If the exclude_pinned result
// is rendered first, the first visible list temporarily lacks pinned/archive
// rows. Keep the workaround scoped to the first main-folder page with no hash so
// pagination and cache probes retain normal Telegram semantics.
func ShouldMergePinnedIntoInitialDialogs(filter domain.DialogFilter) bool {
	if !filter.ExcludePinned || filter.Hash != 0 {
		return false
	}
	if filter.HasFolderID && filter.FolderID != domain.DialogMainFolderID {
		return false
	}
	if filter.OffsetID != 0 || filter.OffsetDate != 0 || filter.HasOffsetPeer {
		return false
	}
	return true
}

// MergeInitialDialogsWithPinned prepends the pinned getPinnedDialogs view to an
// initial getDialogs(exclude_pinned=true) main-list response. The returned list
// remains a normal messages.dialogs/messages.dialogsSlice projection; only the
// first response carries extra pinned/archive rows for TDesktop startup.
func MergeInitialDialogsWithPinned(main, pinned domain.DialogList) domain.DialogList {
	if pinned.ArchiveSummary == nil && len(pinned.Dialogs) == 0 {
		return main
	}
	out := main
	out.ArchiveSummary = pinned.ArchiveSummary
	out.Dialogs = mergeDialogsByPeer(pinned.Dialogs, main.Dialogs)
	out.Messages = mergePrivateMessages(pinned.Messages, main.Messages)
	out.ChannelMessages = mergeChannelMessages(pinned.ChannelMessages, main.ChannelMessages)
	out.Users = mergeUsersByID(pinned.Users, main.Users)
	out.Channels = mergeChannelsByID(pinned.Channels, main.Channels)
	pinnedEntries := len(pinned.Dialogs)
	if pinned.ArchiveSummary != nil {
		pinnedEntries++
	}
	mainCount := main.Count
	if mainCount == 0 {
		mainCount = len(main.Dialogs)
	}
	out.Count = mainCount + pinnedEntries
	out.Hash = 0
	return out
}

func mergeDialogsByPeer(first, second []domain.Dialog) []domain.Dialog {
	out := make([]domain.Dialog, 0, len(first)+len(second))
	seen := make(map[domain.Peer]struct{}, len(first)+len(second))
	for _, d := range first {
		if d.Peer.ID == 0 {
			continue
		}
		if _, ok := seen[d.Peer]; ok {
			continue
		}
		seen[d.Peer] = struct{}{}
		out = append(out, d)
	}
	for _, d := range second {
		if d.Peer.ID == 0 {
			continue
		}
		if _, ok := seen[d.Peer]; ok {
			continue
		}
		seen[d.Peer] = struct{}{}
		out = append(out, d)
	}
	return out
}

type privateMessageKey struct {
	peer domain.Peer
	id   int
}

func mergePrivateMessages(first, second []domain.Message) []domain.Message {
	out := make([]domain.Message, 0, len(first)+len(second))
	seen := make(map[privateMessageKey]struct{}, len(first)+len(second))
	for _, msg := range first {
		key := privateMessageKey{peer: msg.Peer, id: msg.ID}
		if key.peer.ID == 0 || key.id == 0 {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, msg)
	}
	for _, msg := range second {
		key := privateMessageKey{peer: msg.Peer, id: msg.ID}
		if key.peer.ID == 0 || key.id == 0 {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, msg)
	}
	return out
}

type channelMessageKey struct {
	channelID int64
	id        int
}

func mergeChannelMessages(first, second []domain.ChannelMessage) []domain.ChannelMessage {
	out := make([]domain.ChannelMessage, 0, len(first)+len(second))
	seen := make(map[channelMessageKey]struct{}, len(first)+len(second))
	for _, msg := range first {
		key := channelMessageKey{channelID: msg.ChannelID, id: msg.ID}
		if key.channelID == 0 || key.id == 0 {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, msg)
	}
	for _, msg := range second {
		key := channelMessageKey{channelID: msg.ChannelID, id: msg.ID}
		if key.channelID == 0 || key.id == 0 {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, msg)
	}
	return out
}

func mergeUsersByID(first, second []domain.User) []domain.User {
	out := make([]domain.User, 0, len(first)+len(second))
	seen := make(map[int64]struct{}, len(first)+len(second))
	for _, user := range first {
		if user.ID == 0 {
			continue
		}
		if _, ok := seen[user.ID]; ok {
			continue
		}
		seen[user.ID] = struct{}{}
		out = append(out, user)
	}
	for _, user := range second {
		if user.ID == 0 {
			continue
		}
		if _, ok := seen[user.ID]; ok {
			continue
		}
		seen[user.ID] = struct{}{}
		out = append(out, user)
	}
	return out
}

func mergeChannelsByID(first, second []domain.Channel) []domain.Channel {
	out := make([]domain.Channel, 0, len(first)+len(second))
	seen := make(map[int64]struct{}, len(first)+len(second))
	for _, channel := range first {
		if channel.ID == 0 {
			continue
		}
		if _, ok := seen[channel.ID]; ok {
			continue
		}
		seen[channel.ID] = struct{}{}
		out = append(out, channel)
	}
	for _, channel := range second {
		if channel.ID == 0 {
			continue
		}
		if _, ok := seen[channel.ID]; ok {
			continue
		}
		seen[channel.ID] = struct{}{}
		out = append(out, channel)
	}
	return out
}
