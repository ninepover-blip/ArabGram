package store

import (
	"context"
	"fmt"

	"telesrv/internal/domain"
)

// PhoneChangeStore owns the complete non-PTS phone-change aggregate boundary.
// The updated users row and the absolute edge delivery effect commit together.
type PhoneChangeStore interface {
	ChangePhoneWithDelivery(ctx context.Context, req domain.PhoneChangeRequest, effects DeliveryEffectsBuilder[UserDeliverySnapshot]) (domain.PhoneChangeResult, error)
}

func ValidatePhoneChangeDeliveryEffects(req domain.PhoneChangeRequest, snapshot UserDeliverySnapshot, effects []DeliveryEffect) error {
	if snapshot.User.ID != req.UserID || snapshot.User.Phone != req.Phone || len(effects) != 1 {
		return fmt.Errorf("phone change requires exactly one updated-user delivery effect")
	}
	effect := effects[0]
	if err := effect.Validate(); err != nil {
		return fmt.Errorf("phone change delivery effect: %w", err)
	}
	if effect.Kind != DeliveryEffectAbsolute || effect.TargetUserID != req.UserID ||
		effect.ExcludeAuthKeyID != req.ExcludeAuthKeyID || effect.ExcludeSessionID != req.ExcludeSessionID {
		return fmt.Errorf("phone change delivery effect does not match target or origin exclusion")
	}
	return nil
}
