package rpc

import (
	"context"
	"fmt"
	"sort"

	"github.com/iamxvbaba/td/tg"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func (r *Router) channelMessageContentsDeliveryEffects(ctx context.Context, viewerUserID int64, date int) store.DeliveryEffectsBuilder[domain.ReadChannelMessageContentsResult] {
	excludeAuthKeyID, excludeSessionID := deliveryExclusionFromContext(ctx)
	return func(read domain.ReadChannelMessageContentsResult) ([]store.DeliveryEffect, error) {
		updates := make([]tg.UpdateClass, 0, 1+len(read.ClearedUnreadReactionMessageIDs))
		if ids := readChannelMessageContentIDs(read.Messages); len(ids) > 0 {
			updates = append(updates, &tg.UpdateChannelReadMessagesContents{
				ChannelID: read.Channel.ID,
				Messages:  ids,
			})
		}
		updates = append(updates, channelMessageContentReactionUpdates(
			viewerUserID,
			read.Channel,
			read.Messages,
			read.ClearedUnreadReactionMessageIDs,
		)...)
		if len(updates) == 0 {
			return nil, nil
		}
		payload, err := encodeDeliveryUpdate(&tg.Updates{
			Updates: updates,
			Users:   []tg.UserClass{},
			Chats:   []tg.ChatClass{tgChannelChatMin(viewerUserID, read.Channel)},
			Date:    date,
			Seq:     0,
		})
		if err != nil {
			return nil, fmt.Errorf("encode channel message contents delivery for user %d: %w", viewerUserID, err)
		}
		return []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: viewerUserID, ExcludeAuthKeyID: excludeAuthKeyID,
			ExcludeSessionID: excludeSessionID, Payload: payload,
			RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		})}, nil
	}
}

func channelMessageContentReactionUpdates(viewerUserID int64, channel domain.Channel, messages []domain.ChannelMessage, ids []int) []tg.UpdateClass {
	messagesByID := make(map[int]domain.ChannelMessage, len(messages))
	for _, msg := range messages {
		if msg.ID > 0 {
			messagesByID[msg.ID] = msg
		}
	}
	updates := make([]tg.UpdateClass, 0, len(ids))
	for _, id := range ids {
		msg, ok := messagesByID[id]
		if !ok {
			continue
		}
		reactions := domain.ChannelMessageReactions{
			CanSeeList: !channel.Broadcast || channel.Megagroup,
			Results:    []domain.ChannelMessageReactionCount{},
			Recent:     []domain.ChannelMessagePeerReaction{},
		}
		if msg.Reactions != nil {
			reactions = *msg.Reactions
		}
		converted := tgMessageReactions(viewerUserID, &reactions)
		if converted == nil {
			converted = &tg.MessageReactions{Results: []tg.ReactionCount{}}
		}
		update := &tg.UpdateMessageReactions{
			Peer: &tg.PeerChannel{ChannelID: channel.ID}, MsgID: id, Reactions: *converted,
		}
		if topID := channelMessageThreadRootID(msg); topID > 0 && topID != id {
			update.SetTopMsgID(topID)
		}
		if msg.SavedPeer.ID != 0 {
			update.SetSavedPeerID(tgPeer(msg.SavedPeer))
		}
		updates = append(updates, update)
	}
	return updates
}

// channelReadDeliveryEffects projects one committed channel read transaction
// into per-user absolute delivery payloads. These constructors have no account
// pts/pts_count; UpdateReadChannelInbox carries only the existing channel pts.
func channelReadDeliveryEffects(date int) store.DeliveryEffectsBuilder[domain.ReadChannelHistoryResult] {
	return func(read domain.ReadChannelHistoryResult) ([]store.DeliveryEffect, error) {
		byUser := make(map[int64][]tg.UpdateClass)
		if read.Changed && read.Dialog.UserID > 0 {
			update := &tg.UpdateReadChannelInbox{
				ChannelID: read.ChannelID, MaxID: read.MaxID,
				StillUnreadCount: read.StillUnreadCount, Pts: read.Pts,
			}
			if read.Dialog.FolderID > 0 {
				update.SetFolderID(read.Dialog.FolderID)
			}
			byUser[read.Dialog.UserID] = append(byUser[read.Dialog.UserID], update)
		}
		appendChannelReadOutboxUpdates(byUser, read.ChannelID, read.OutboxUpdates)
		if general := read.GeneralTopic; general != nil && general.Changed {
			byUser[read.Dialog.UserID] = append(byUser[read.Dialog.UserID], &tg.UpdateReadChannelDiscussionInbox{
				ChannelID: read.ChannelID, TopMsgID: general.TopicID, ReadMaxID: general.MaxID,
			})
			appendChannelDiscussionOutboxUpdates(byUser, read.ChannelID, general.TopicID, general.OutboxUpdates)
		}
		return encodeChannelReadEffects(byUser, date)
	}
}

func channelTopicReadDeliveryEffects(readerUserID int64, date int) store.DeliveryEffectsBuilder[domain.ReadChannelTopicHistoryResult] {
	return func(read domain.ReadChannelTopicHistoryResult) ([]store.DeliveryEffect, error) {
		byUser := make(map[int64][]tg.UpdateClass)
		if read.Changed {
			byUser[readerUserID] = append(byUser[readerUserID], &tg.UpdateReadChannelDiscussionInbox{
				ChannelID: read.Channel.ID, TopMsgID: read.TopicID, ReadMaxID: read.MaxID,
			})
			appendChannelDiscussionOutboxUpdates(byUser, read.Channel.ID, read.TopicID, read.OutboxUpdates)
		}
		return encodeChannelReadEffects(byUser, date)
	}
}

func appendChannelReadOutboxUpdates(byUser map[int64][]tg.UpdateClass, channelID int64, updates []domain.ChannelReadOutboxUpdate) {
	maxByUser := make(map[int64]int, len(updates))
	for _, update := range updates {
		if update.UserID > 0 && update.MaxID > maxByUser[update.UserID] {
			maxByUser[update.UserID] = update.MaxID
		}
	}
	for userID, maxID := range maxByUser {
		byUser[userID] = append(byUser[userID], &tg.UpdateReadChannelOutbox{ChannelID: channelID, MaxID: maxID})
	}
}

func appendChannelDiscussionOutboxUpdates(byUser map[int64][]tg.UpdateClass, channelID int64, topicID int, updates []domain.ChannelReadOutboxUpdate) {
	maxByUser := make(map[int64]int, len(updates))
	for _, update := range updates {
		if update.UserID > 0 && update.MaxID > maxByUser[update.UserID] {
			maxByUser[update.UserID] = update.MaxID
		}
	}
	for userID, maxID := range maxByUser {
		byUser[userID] = append(byUser[userID], &tg.UpdateReadChannelDiscussionOutbox{
			ChannelID: channelID, TopMsgID: topicID, ReadMaxID: maxID,
		})
	}
}

func encodeChannelReadEffects(byUser map[int64][]tg.UpdateClass, date int) ([]store.DeliveryEffect, error) {
	userIDs := make([]int64, 0, len(byUser))
	for userID := range byUser {
		if userID > 0 && len(byUser[userID]) > 0 {
			userIDs = append(userIDs, userID)
		}
	}
	sort.Slice(userIDs, func(i, j int) bool { return userIDs[i] < userIDs[j] })
	effects := make([]store.DeliveryEffect, 0, len(userIDs))
	for _, userID := range userIDs {
		payload, err := encodeDeliveryUpdate(&tg.Updates{
			Updates: byUser[userID], Users: []tg.UserClass{}, Chats: []tg.ChatClass{}, Date: date, Seq: 0,
		})
		if err != nil {
			return nil, fmt.Errorf("encode channel read delivery for user %d: %w", userID, err)
		}
		effects = append(effects, store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: userID, Payload: payload, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		}))
	}
	return effects, nil
}
