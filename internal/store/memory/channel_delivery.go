package memory

import (
	"context"
	"errors"
	"sort"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

type memoryChannelDeliveryRollback struct {
	channel      domain.Channel
	members      map[int64]domain.ChannelMember
	dialogs      map[int64]map[int64]domain.ChannelDialog
	messages     []domain.ChannelMessage
	events       []domain.ChannelUpdateEvent
	retention    domain.ChannelUpdateRetentionCheckpoint
	historyDates map[int64]map[int64]int
	mentions     map[int64]map[int64]map[int]memoryMention
	adminLogs    []domain.ChannelAdminLogEvent
	invites      map[string]domain.ChannelInvite
	importers    map[int64]domain.ChannelInviteImporter
	msgSeq       int
	ptsSeq       int
	logSeq       int64
}

func cloneMemoryChannelDialogs(in map[int64]map[int64]domain.ChannelDialog) map[int64]map[int64]domain.ChannelDialog {
	out := make(map[int64]map[int64]domain.ChannelDialog, len(in))
	for userID, byChannel := range in {
		cloned := make(map[int64]domain.ChannelDialog, len(byChannel))
		for channelID, dialog := range byChannel {
			cloned[channelID] = dialog
		}
		out[userID] = cloned
	}
	return out
}

func cloneMemoryHistoryDates(in map[int64]map[int64]int) map[int64]map[int64]int {
	out := make(map[int64]map[int64]int, len(in))
	for channelID, byUser := range in {
		cloned := make(map[int64]int, len(byUser))
		for userID, date := range byUser {
			cloned[userID] = date
		}
		out[channelID] = cloned
	}
	return out
}

func cloneMemoryMentions(in map[int64]map[int64]map[int]memoryMention) map[int64]map[int64]map[int]memoryMention {
	out := make(map[int64]map[int64]map[int]memoryMention, len(in))
	for channelID, byUser := range in {
		users := make(map[int64]map[int]memoryMention, len(byUser))
		for userID, byMessage := range byUser {
			messages := make(map[int]memoryMention, len(byMessage))
			for messageID, mention := range byMessage {
				messages[messageID] = mention
			}
			users[userID] = messages
		}
		out[channelID] = users
	}
	return out
}

func (s *ChannelStore) deliveryRollbackLocked(channelID int64) memoryChannelDeliveryRollback {
	members := make(map[int64]domain.ChannelMember, len(s.members[channelID]))
	for userID, member := range s.members[channelID] {
		members[userID] = member
	}
	importers := make(map[int64]domain.ChannelInviteImporter, len(s.importers[channelID]))
	for userID, importer := range s.importers[channelID] {
		importers[userID] = importer
	}
	invites := make(map[string]domain.ChannelInvite, len(s.invites))
	for hash, invite := range s.invites {
		invites[hash] = invite
	}
	messages := make([]domain.ChannelMessage, len(s.messages[channelID]))
	for i := range s.messages[channelID] {
		messages[i] = cloneChannelMessage(s.messages[channelID][i])
	}
	events := make([]domain.ChannelUpdateEvent, len(s.events[channelID]))
	for i := range s.events[channelID] {
		events[i] = cloneChannelEvent(s.events[channelID][i])
	}
	return memoryChannelDeliveryRollback{
		channel: cloneChannel(s.channels[channelID]), members: members,
		dialogs: cloneMemoryChannelDialogs(s.dialogs), messages: messages, events: events,
		retention: s.retention[channelID], historyDates: cloneMemoryHistoryDates(s.historyClearDates),
		mentions: cloneMemoryMentions(s.mentions), adminLogs: append([]domain.ChannelAdminLogEvent(nil), s.adminLogs[channelID]...),
		invites: invites, importers: importers, msgSeq: s.msgSeq[channelID], ptsSeq: s.ptsSeq[channelID], logSeq: s.logSeq[channelID],
	}
}

func (s *ChannelStore) restoreDeliveryRollbackLocked(channelID int64, rollback memoryChannelDeliveryRollback) {
	s.channels[channelID] = rollback.channel
	s.members[channelID] = rollback.members
	s.dialogs = rollback.dialogs
	s.messages[channelID] = rollback.messages
	s.events[channelID] = rollback.events
	s.retention[channelID] = rollback.retention
	s.historyClearDates = rollback.historyDates
	s.mentions = rollback.mentions
	s.adminLogs[channelID] = rollback.adminLogs
	s.invites = rollback.invites
	s.importers[channelID] = rollback.importers
	s.msgSeq[channelID], s.ptsSeq[channelID], s.logSeq[channelID] = rollback.msgSeq, rollback.ptsSeq, rollback.logSeq
}

func (s *ChannelStore) pendingJoinDeliverySnapshotLocked(channel domain.Channel) store.ChannelPendingJoinDeliverySnapshot {
	all := make([]domain.ChannelInviteImporter, 0, len(s.importers[channel.ID]))
	for _, importer := range s.importers[channel.ID] {
		if importer.Requested {
			all = append(all, importer)
		}
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].Date != all[j].Date {
			return all[i].Date > all[j].Date
		}
		return all[i].UserID > all[j].UserID
	})
	recentLimit := len(all)
	if recentLimit > domain.MaxChannelPendingJoinRecentRequesters {
		recentLimit = domain.MaxChannelPendingJoinRecentRequesters
	}
	recent := make([]int64, 0, recentLimit)
	recentUsers := make([]domain.User, 0, recentLimit)
	for _, importer := range all[:recentLimit] {
		recent = append(recent, importer.UserID)
		recentUsers = append(recentUsers, domain.User{ID: importer.UserID})
	}
	adminIDs := make([]int64, 0)
	for userID, member := range s.members[channel.ID] {
		if member.Status == domain.ChannelMemberActive && canViewChannelJoinRequests(member) {
			adminIDs = append(adminIDs, userID)
		}
	}
	sort.Slice(adminIDs, func(i, j int) bool { return adminIDs[i] < adminIDs[j] })
	if len(adminIDs) > domain.MaxChannelRealtimeFanout {
		adminIDs = adminIDs[:domain.MaxChannelRealtimeFanout]
	}
	snapshot := store.ChannelPendingJoinDeliverySnapshot{Targets: make([]store.ChannelPendingJoinDeliveryTarget, 0, len(adminIDs))}
	for _, userID := range adminIDs {
		snapshot.Targets = append(snapshot.Targets, store.ChannelPendingJoinDeliveryTarget{
			TargetUserID: userID, Channel: cloneChannel(channel),
			Pending:     domain.ChannelPendingJoinRequests{ChannelID: channel.ID, Count: len(all), RecentRequesters: append([]int64(nil), recent...)},
			RecentUsers: append([]domain.User(nil), recentUsers...),
		})
	}
	return snapshot
}

func canViewChannelJoinRequests(member domain.ChannelMember) bool {
	return member.Role == domain.ChannelRoleCreator ||
		(member.Role == domain.ChannelRoleAdmin && (member.AdminRights.InviteUsers || member.AdminRights.ChangeInfo))
}

func applyMemoryPendingJoinDelivery(ctx context.Context, outbox *DeliveryOutboxStore, snapshot store.ChannelPendingJoinDeliverySnapshot, build store.DeliveryEffectsBuilder[store.ChannelPendingJoinDeliverySnapshot]) error {
	if build == nil || outbox == nil {
		return store.ErrDeliveryOutboxRequired
	}
	expected := make(map[int64]struct{}, len(snapshot.Targets))
	for _, target := range snapshot.Targets {
		if target.TargetUserID <= 0 || target.Channel.ID == 0 || target.Pending.ChannelID != target.Channel.ID {
			return errors.New("pending join delivery snapshot is invalid")
		}
		if len(target.RecentUsers) != len(target.Pending.RecentRequesters) {
			return errors.New("pending join delivery requester projection is incomplete")
		}
		for i, userID := range target.Pending.RecentRequesters {
			if userID <= 0 || target.RecentUsers[i].ID != userID {
				return errors.New("pending join delivery requester projection does not match summary")
			}
		}
		if _, duplicate := expected[target.TargetUserID]; duplicate {
			return errors.New("pending join delivery snapshot has duplicate target")
		}
		expected[target.TargetUserID] = struct{}{}
	}
	effects, err := build(snapshot)
	if err != nil {
		return err
	}
	if len(effects) != len(snapshot.Targets) {
		return errors.New("pending join delivery effect count does not match target count")
	}
	for _, effect := range effects {
		if effect.Kind != store.DeliveryEffectAbsolute {
			return errors.New("pending join delivery effect must be absolute")
		}
		if _, ok := expected[effect.TargetUserID]; !ok {
			return errors.New("pending join delivery effect has unexpected target")
		}
		delete(expected, effect.TargetUserID)
	}
	if len(expected) != 0 {
		return errors.New("pending join delivery effect omits target")
	}
	_, err = applyDeliveryEffects(ctx, effects, outbox, nil)
	return err
}

func applyMemoryAvailableMinDelivery(ctx context.Context, outbox *DeliveryOutboxStore, snapshot store.ChannelAvailableMinDeliverySnapshot, build store.DeliveryEffectsBuilder[store.ChannelAvailableMinDeliverySnapshot]) error {
	if build == nil || outbox == nil {
		return store.ErrDeliveryOutboxRequired
	}
	effects, err := build(snapshot)
	if err != nil {
		return err
	}
	if snapshot.TargetUserID == 0 {
		if len(effects) != 0 {
			return errors.New("no-op available-min snapshot produced delivery effects")
		}
		return nil
	}
	if snapshot.Channel.ID == 0 || snapshot.AvailableMinID <= 0 || len(effects) != 1 || effects[0].Kind != store.DeliveryEffectAbsolute || effects[0].TargetUserID != snapshot.TargetUserID {
		return errors.New("available-min delivery effect does not match target")
	}
	_, err = applyDeliveryEffects(ctx, effects, outbox, nil)
	return err
}
