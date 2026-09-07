package store

import (
	"context"
	"fmt"

	"telesrv/internal/domain"
)

// PremiumStore owns the durable Premium catalog, forms, ledger-linked
// entitlements and their atomic purchase/refund transitions.
type PremiumStore interface {
	Plans(ctx context.Context) ([]domain.PremiumPlan, error)
	Plan(ctx context.Context, months int) (domain.PremiumPlan, bool, error)
	SyncPlans(ctx context.Context, plans []domain.PremiumPlan) error
	UpsertPremiumPlan(ctx context.Context, req domain.PremiumPlanUpsertRequest) (domain.PremiumPlan, error)
	IssuePremiumPaymentForm(ctx context.Context, form domain.PremiumPaymentForm) (domain.PremiumPaymentForm, error)
	PurchasePremiumWithDelivery(ctx context.Context, req domain.PremiumPurchaseRequest, effects DeliveryEffectsBuilder[domain.PremiumPurchaseResult]) (domain.PremiumPurchaseResult, error)
	ActivePremiumEntitlements(ctx context.Context, userID int64, now int) ([]domain.PremiumEntitlement, error)
	PremiumEntitlements(ctx context.Context, userID int64, limit int) ([]domain.PremiumEntitlement, error)
	PremiumPurchaseHistory(ctx context.Context, userID int64, limit int) ([]domain.PremiumEntitlement, error)
	PremiumPayment(ctx context.Context, paymentIntentID int64) (domain.PremiumPaymentDetails, bool, error)
	SweepPremiumEntitlements(ctx context.Context, now, limit int) ([]domain.User, error)
	GrantPremiumEntitlement(ctx context.Context, req domain.PremiumAdminGrantRequest) (domain.PremiumEntitlement, domain.User, error)
	RevokePremiumEntitlements(ctx context.Context, req domain.PremiumAdminRevokeRequest) (domain.User, error)
	RefundPremiumPayment(ctx context.Context, req domain.PremiumRefundRequest) (domain.PremiumPurchaseResult, error)
}

func ValidatePremiumPurchaseDeliveryEffects(req domain.PremiumPurchaseRequest, result domain.PremiumPurchaseResult, effects []DeliveryEffect) error {
	if result.User.ID <= 0 || result.User.ID != req.RecipientUserID || len(effects) != 1 {
		return fmt.Errorf("premium purchase requires exactly one recipient updateUser effect")
	}
	effect := effects[0]
	if err := effect.Validate(); err != nil {
		return fmt.Errorf("premium purchase delivery effect: %w", err)
	}
	if effect.Kind != DeliveryEffectAbsolute || effect.TargetUserID != result.User.ID ||
		effect.ExcludeAuthKeyID != req.OriginAuthKeyID || effect.ExcludeSessionID != req.OriginSessionID {
		return fmt.Errorf("premium purchase delivery effect does not match recipient or origin exclusion")
	}
	return nil
}

type PremiumExpiryDeliveryStore interface {
	SweepPremiumEntitlementsWithDelivery(ctx context.Context, now, limit int, effects DeliveryEffectsBuilder[[]domain.User]) ([]domain.User, error)
}

type PremiumAdminDeliveryStore interface {
	GrantPremiumEntitlementWithDelivery(ctx context.Context, req domain.PremiumAdminGrantRequest, effects DeliveryEffectsBuilder[[]domain.User]) (domain.PremiumEntitlement, domain.User, error)
	RevokePremiumEntitlementsWithDelivery(ctx context.Context, req domain.PremiumAdminRevokeRequest, effects DeliveryEffectsBuilder[[]domain.User]) (domain.User, error)
	RefundPremiumPaymentWithDelivery(ctx context.Context, req domain.PremiumRefundRequest, effects DeliveryEffectsBuilder[domain.PremiumPurchaseResult]) (domain.PremiumPurchaseResult, error)
}
