package memory

import (
	"telesrv/internal/domain"
	storepkg "telesrv/internal/store"
)

func newChannelStoreWithDelivery() *ChannelStore {
	channels := NewChannelStore()
	channels.AttachDeliveryOutbox(NewDeliveryOutboxStore())
	return channels
}

func newMessageStoreWithDelivery(dialogs ...*DialogStore) *MessageStore {
	messages := NewMessageStore(dialogs...)
	messages.AttachDeliveryOutbox(NewDeliveryOutboxStore())
	return messages
}

func testBotLifecycleEffects(snapshot storepkg.BotLifecycleDeliverySnapshot) ([]storepkg.DeliveryEffect, error) {
	return []storepkg.DeliveryEffect{storepkg.AbsoluteDeliveryEffect(storepkg.DeliveryOutboxEnqueue{
		TargetUserID: snapshot.OwnerUserID, Payload: []byte{1}, RecoveryPolicy: storepkg.OutboxRecoveryAbsoluteReload,
	})}, nil
}

func testChannelReadEffects(read domain.ReadChannelHistoryResult) ([]storepkg.DeliveryEffect, error) {
	userID := read.Dialog.UserID
	if userID == 0 {
		userID = 1
	}
	return []storepkg.DeliveryEffect{storepkg.AbsoluteDeliveryEffect(storepkg.DeliveryOutboxEnqueue{
		TargetUserID: userID, Payload: []byte{1}, RecoveryPolicy: storepkg.OutboxRecoveryAbsoluteReload,
	})}, nil
}

func testChannelTopicReadEffects(read domain.ReadChannelTopicHistoryResult) ([]storepkg.DeliveryEffect, error) {
	userID := read.Channel.CreatorUserID
	if userID == 0 {
		userID = 1
	}
	return []storepkg.DeliveryEffect{storepkg.AbsoluteDeliveryEffect(storepkg.DeliveryOutboxEnqueue{
		TargetUserID: userID, Payload: []byte{1}, RecoveryPolicy: storepkg.OutboxRecoveryAbsoluteReload,
	})}, nil
}

func testChannelMessageContentsEffects(read domain.ReadChannelMessageContentsResult) ([]storepkg.DeliveryEffect, error) {
	userID := read.Channel.CreatorUserID
	if userID == 0 {
		userID = 1
	}
	return []storepkg.DeliveryEffect{storepkg.AbsoluteDeliveryEffect(storepkg.DeliveryOutboxEnqueue{
		TargetUserID: userID, Payload: []byte{1}, RecoveryPolicy: storepkg.OutboxRecoveryAbsoluteReload,
	})}, nil
}

func testPendingJoinEffects(snapshot storepkg.ChannelPendingJoinDeliverySnapshot) ([]storepkg.DeliveryEffect, error) {
	effects := make([]storepkg.DeliveryEffect, 0, len(snapshot.Targets))
	for _, target := range snapshot.Targets {
		effects = append(effects, storepkg.AbsoluteDeliveryEffect(storepkg.DeliveryOutboxEnqueue{
			TargetUserID: target.TargetUserID, Payload: []byte{1}, RecoveryPolicy: storepkg.OutboxRecoveryAbsoluteReload,
		}))
	}
	return effects, nil
}

func testAvailableMinEffects(snapshot storepkg.ChannelAvailableMinDeliverySnapshot) ([]storepkg.DeliveryEffect, error) {
	if snapshot.TargetUserID == 0 {
		return nil, nil
	}
	return []storepkg.DeliveryEffect{storepkg.AbsoluteDeliveryEffect(storepkg.DeliveryOutboxEnqueue{
		TargetUserID: snapshot.TargetUserID, Payload: []byte{1}, RecoveryPolicy: storepkg.OutboxRecoveryAbsoluteReload,
	})}, nil
}

func testPrivateReactionEffects(read domain.PrivateMessageReactionsResult) ([]storepkg.DeliveryEffect, error) {
	userID := int64(1)
	if len(read.Messages) > 0 && read.Messages[0].OwnerUserID > 0 {
		userID = read.Messages[0].OwnerUserID
	}
	return []storepkg.DeliveryEffect{storepkg.AbsoluteDeliveryEffect(storepkg.DeliveryOutboxEnqueue{
		TargetUserID: userID, Payload: []byte{1}, RecoveryPolicy: storepkg.OutboxRecoveryAbsoluteReload,
	})}, nil
}

func testSavedReactionTagEffects(tag domain.SavedReactionTag) ([]storepkg.DeliveryEffect, error) {
	return []storepkg.DeliveryEffect{storepkg.AbsoluteDeliveryEffect(storepkg.DeliveryOutboxEnqueue{
		TargetUserID: tag.UserID, Payload: []byte{1}, RecoveryPolicy: storepkg.OutboxRecoveryAbsoluteReload,
	})}, nil
}
