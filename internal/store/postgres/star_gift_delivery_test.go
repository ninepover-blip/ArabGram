package postgres

import (
	"telesrv/internal/domain"
	storepkg "telesrv/internal/store"
)

func starGiftPurchaseTestEffects(result domain.StarGiftPurchaseResult) ([]storepkg.DeliveryEffect, error) {
	return []storepkg.DeliveryEffect{storepkg.AbsoluteDeliveryEffect(storepkg.DeliveryOutboxEnqueue{
		TargetUserID:   result.Balance.UserID,
		Payload:        []byte("star-gift-balance-test"),
		RecoveryPolicy: storepkg.OutboxRecoveryAbsoluteReload,
	})}, nil
}

func starGiftConvertTestEffects(result domain.StarGiftConvertResult) ([]storepkg.DeliveryEffect, error) {
	if result.Saved.Owner.Type == domain.PeerTypeChannel {
		return nil, nil
	}
	if result.Saved.ConvertStars <= 0 {
		return nil, nil
	}
	return []storepkg.DeliveryEffect{storepkg.AbsoluteDeliveryEffect(storepkg.DeliveryOutboxEnqueue{
		TargetUserID:   result.Saved.Owner.ID,
		Payload:        []byte("star-gift-convert-balance-test"),
		RecoveryPolicy: storepkg.OutboxRecoveryAbsoluteReload,
	})}, nil
}
