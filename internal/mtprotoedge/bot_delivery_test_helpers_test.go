package mtprotoedge

import "telesrv/internal/store"

func edgeTestBotLifecycleEffects(snapshot store.BotLifecycleDeliverySnapshot) ([]store.DeliveryEffect, error) {
	return []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
		TargetUserID: snapshot.OwnerUserID, Payload: []byte{1}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
	})}, nil
}
