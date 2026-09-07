package rpc

import "telesrv/internal/store"

func rpcTestContactMutationEffects(snapshot store.ContactMutationSnapshot) ([]store.DeliveryEffect, error) {
	out := make([]store.DeliveryEffect, 0, len(snapshot.RequiredEvents)+1)
	for _, required := range snapshot.RequiredEvents {
		out = append(out, store.AccountPTSDeliveryEffect(required.TargetUserID, required.Event, [8]byte{}, 0))
	}
	if snapshot.PhonePrivacyChanged {
		out = append(out, store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: snapshot.OwnerUserID, Payload: []byte{1}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		}))
	}
	return out, nil
}
