package memory

import (
	"context"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

// PhoneChangeStore 是测试用内存实现。用户唯一性在 UserStore 锁内维护；
// updateUserPhone 无 PTS，所以不向 UpdateEventStore 写 durable event。
type PhoneChangeStore struct {
	users *UserStore
}

func NewPhoneChangeStore(users *UserStore) *PhoneChangeStore {
	return &PhoneChangeStore{users: users}
}

func (s *PhoneChangeStore) ChangePhoneWithDelivery(ctx context.Context, req domain.PhoneChangeRequest, effects store.DeliveryEffectsBuilder[store.UserDeliverySnapshot]) (domain.PhoneChangeResult, error) {
	if s == nil || s.users == nil || req.UserID == 0 || !domain.ValidPhone(req.Phone) {
		return domain.PhoneChangeResult{}, domain.ErrPhoneNumberInvalid
	}
	if effects == nil {
		return domain.PhoneChangeResult{}, store.ErrDeliveryOutboxRequired
	}
	s.users.mu.Lock()
	defer s.users.mu.Unlock()
	u, ok := s.users.byID[req.UserID]
	if !ok {
		return domain.PhoneChangeResult{}, domain.ErrUserNotFound
	}
	if u.Phone == req.Phone {
		return domain.PhoneChangeResult{User: u}, nil
	}
	for id, existing := range s.users.byID {
		if id != req.UserID && existing.Phone == req.Phone {
			return domain.PhoneChangeResult{}, domain.ErrPhoneNumberOccupied
		}
	}
	if s.users.deliveryOutbox == nil {
		return domain.PhoneChangeResult{}, store.ErrDeliveryOutboxRequired
	}
	u.Phone = req.Phone
	snapshot := s.users.deliverySnapshotLocked(ctx, u)
	intents, err := effects(snapshot)
	if err != nil {
		return domain.PhoneChangeResult{}, err
	}
	if err := store.ValidatePhoneChangeDeliveryEffects(req, snapshot, intents); err != nil {
		return domain.PhoneChangeResult{}, err
	}
	if _, err := applyDeliveryEffects(ctx, intents, s.users.deliveryOutbox, nil); err != nil {
		return domain.PhoneChangeResult{}, err
	}
	s.users.byID[req.UserID] = u
	return domain.PhoneChangeResult{User: u, Changed: true}, nil
}
