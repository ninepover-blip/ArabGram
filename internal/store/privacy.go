package store

import (
	"context"
	"errors"

	"telesrv/internal/domain"
)

var ErrPrivacyDeliveryStoreMissing = errors.New("privacy delivery store is required")

// PrivacyStore persists account privacy rules by owner user and privacy key.
type PrivacyStore interface {
	GetPrivacyRules(ctx context.Context, ownerUserID int64, key domain.PrivacyKey) (domain.PrivacyRules, bool, error)
	SetPrivacyRules(ctx context.Context, rules domain.PrivacyRules) error
	ListPrivacyRules(ctx context.Context, ownerUserIDs []int64, keys []domain.PrivacyKey) ([]domain.PrivacyRules, error)
}

type PrivacyDeliveryPayloadBuilder func(domain.PrivacyRules) ([]byte, error)

// PrivacyDeliveryStore persists privacy rules and the non-PTS online delivery
// row as one aggregate transaction.
type PrivacyDeliveryStore interface {
	SetPrivacyRulesWithDelivery(ctx context.Context, rules domain.PrivacyRules, build PrivacyDeliveryPayloadBuilder, excludeAuthKeyID [8]byte, excludeSessionID int64) error
}
