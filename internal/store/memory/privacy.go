package memory

import (
	"context"
	"sync"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

type privacyStoreKey struct {
	ownerUserID int64
	key         domain.PrivacyKey
}

// PrivacyStore is an in-memory account privacy rule store for tests/dev mode.
type PrivacyStore struct {
	mu             sync.RWMutex
	rules          map[privacyStoreKey]domain.PrivacyRules
	deliveryOutbox *DeliveryOutboxStore
}

func NewPrivacyStore() *PrivacyStore {
	return &PrivacyStore{rules: make(map[privacyStoreKey]domain.PrivacyRules)}
}

func (s *PrivacyStore) AttachDeliveryOutbox(outbox *DeliveryOutboxStore) {
	s.mu.Lock()
	s.deliveryOutbox = outbox
	s.mu.Unlock()
}

func (s *PrivacyStore) GetPrivacyRules(_ context.Context, ownerUserID int64, key domain.PrivacyKey) (domain.PrivacyRules, bool, error) {
	s.mu.RLock()
	rules, ok := s.rules[privacyStoreKey{ownerUserID: ownerUserID, key: key}]
	s.mu.RUnlock()
	return clonePrivacyRules(rules), ok, nil
}

func (s *PrivacyStore) SetPrivacyRules(_ context.Context, rules domain.PrivacyRules) error {
	s.mu.Lock()
	s.rules[privacyStoreKey{ownerUserID: rules.OwnerUserID, key: rules.Key}] = clonePrivacyRules(rules)
	s.mu.Unlock()
	return nil
}

func (s *PrivacyStore) SetPrivacyRulesWithDelivery(ctx context.Context, rules domain.PrivacyRules, build store.PrivacyDeliveryPayloadBuilder, excludeAuthKeyID [8]byte, excludeSessionID int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.deliveryOutbox == nil || build == nil {
		return store.ErrPrivacyDeliveryStoreMissing
	}
	payload, err := build(clonePrivacyRules(rules))
	if err != nil {
		return err
	}
	if _, err := s.deliveryOutbox.Enqueue(ctx, store.DeliveryOutboxEnqueue{
		TargetUserID:     rules.OwnerUserID,
		ExcludeAuthKeyID: excludeAuthKeyID,
		ExcludeSessionID: excludeSessionID,
		Payload:          payload,
		RecoveryPolicy:   store.OutboxRecoveryAbsoluteReload,
	}); err != nil {
		return err
	}
	s.rules[privacyStoreKey{ownerUserID: rules.OwnerUserID, key: rules.Key}] = clonePrivacyRules(rules)
	return nil
}

func (s *PrivacyStore) ListPrivacyRules(_ context.Context, ownerUserIDs []int64, keys []domain.PrivacyKey) ([]domain.PrivacyRules, error) {
	if len(ownerUserIDs) == 0 || len(keys) == 0 {
		return nil, nil
	}
	owners := make(map[int64]struct{}, len(ownerUserIDs))
	for _, id := range ownerUserIDs {
		owners[id] = struct{}{}
	}
	keySet := make(map[domain.PrivacyKey]struct{}, len(keys))
	for _, key := range keys {
		keySet[key] = struct{}{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domain.PrivacyRules, 0, len(s.rules))
	for k, rules := range s.rules {
		if _, ok := owners[k.ownerUserID]; !ok {
			continue
		}
		if _, ok := keySet[k.key]; !ok {
			continue
		}
		out = append(out, clonePrivacyRules(rules))
	}
	return out, nil
}

func clonePrivacyRules(in domain.PrivacyRules) domain.PrivacyRules {
	out := in
	out.Rules = make([]domain.PrivacyRule, len(in.Rules))
	for i, rule := range in.Rules {
		out.Rules[i] = rule
		out.Rules[i].UserIDs = append([]int64(nil), rule.UserIDs...)
		out.Rules[i].ChatIDs = append([]int64(nil), rule.ChatIDs...)
	}
	return out
}
