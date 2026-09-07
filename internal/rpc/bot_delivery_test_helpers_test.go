package rpc

import "telesrv/internal/store"

func rpcTestBotLifecycleEffects(snapshot store.BotLifecycleDeliverySnapshot) ([]store.DeliveryEffect, error) {
	return []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
		TargetUserID: snapshot.OwnerUserID, Payload: []byte{1}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
	})}, nil
}

func rpcTestBotInfoEffects(snapshot store.UserAudienceDeliverySnapshot) ([]store.DeliveryEffect, error) {
	effects := make([]store.DeliveryEffect, len(snapshot.Audience))
	for i, viewerID := range snapshot.Audience {
		effects[i] = store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: viewerID, Payload: []byte{1}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		})
	}
	return effects, nil
}
