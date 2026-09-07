package channels

import (
	"telesrv/internal/domain"
	"telesrv/internal/store"
	"telesrv/internal/store/memory"
)

func newServiceTestChannelStore() *memory.ChannelStore {
	channels := memory.NewChannelStore()
	channels.AttachDeliveryOutbox(memory.NewDeliveryOutboxStore())
	return channels
}

func testChannelReadEffects(read domain.ReadChannelHistoryResult) ([]store.DeliveryEffect, error) {
	return []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
		TargetUserID: read.Dialog.UserID, Payload: []byte{1}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
	})}, nil
}

func testPendingJoinEffects(snapshot store.ChannelPendingJoinDeliverySnapshot) ([]store.DeliveryEffect, error) {
	effects := make([]store.DeliveryEffect, 0, len(snapshot.Targets))
	for _, target := range snapshot.Targets {
		effects = append(effects, store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: target.TargetUserID, Payload: []byte{1}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		}))
	}
	return effects, nil
}

func testAvailableMinEffects(snapshot store.ChannelAvailableMinDeliverySnapshot) ([]store.DeliveryEffect, error) {
	if snapshot.TargetUserID == 0 {
		return nil, nil
	}
	return []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
		TargetUserID: snapshot.TargetUserID, Payload: []byte{1}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
	})}, nil
}
