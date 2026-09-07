package auth

import "telesrv/internal/store"

var testAuthorizationDelivery store.DeliveryEffectsBuilder[store.AuthorizationDeliverySnapshot] = func(snapshot store.AuthorizationDeliverySnapshot) ([]store.DeliveryEffect, error) {
	return []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
		TargetUserID:     snapshot.User.ID,
		ExcludeAuthKeyID: [8]byte{0x7f},
		ExcludeSessionID: 1,
		Payload:          []byte{1},
		RecoveryPolicy:   store.OutboxRecoveryAbsoluteReload,
	})}, nil
}
