package rpc

import (
	"context"
	"telesrv/internal/domain"
	"telesrv/internal/store"
)

type staticUsersService struct {
	user domain.User
}

type mapUsersService struct {
	users map[int64]domain.User
}

type countingMapUsersService struct {
	mapUsersService
	selfCalls      int
	byIDCalls      int
	byIDsCalls     int
	baseByIDsCalls int
	lastByIDs      []int64
	byIDsBatches   [][]int64
}

func (s staticUsersService) Self(context.Context, int64) (domain.User, error) {
	return s.user, nil
}

func (s staticUsersService) ByID(_ context.Context, _, userID int64) (domain.User, bool, error) {
	if userID == s.user.ID {
		return s.user, true, nil
	}
	if userID == domain.OfficialSystemUserID {
		return domain.OfficialSystemUser(), true, nil
	}
	return domain.User{}, false, nil
}

func (s staticUsersService) BotStatus(_ context.Context, userID int64) (bool, bool, error) {
	u, found, err := s.ByID(context.Background(), userID, userID)
	return u.Bot, found, err
}

func (s staticUsersService) ByIDs(_ context.Context, _ int64, userIDs []int64) ([]domain.User, error) {
	out := make([]domain.User, 0, len(userIDs))
	seen := map[int64]struct{}{}
	for _, userID := range userIDs {
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		if userID == s.user.ID {
			out = append(out, s.user)
		} else if userID == domain.OfficialSystemUserID {
			out = append(out, domain.OfficialSystemUser())
		}
	}
	return out, nil
}

func (s staticUsersService) BaseUsersByIDs(ctx context.Context, userIDs []int64) ([]domain.User, error) {
	return s.ByIDs(ctx, 1, userIDs)
}

func (s staticUsersService) UpdateEmojiStatusWithEvent(_ context.Context, userID int64, status domain.UserEmojiStatus, date int) (domain.User, domain.UpdateEvent, error) {
	if userID != s.user.ID {
		return domain.User{}, domain.UpdateEvent{}, domain.ErrUserNotFound
	}
	u := testUserWithEmojiStatus(s.user, status)
	return u, testEmojiStatusUpdateEvent(userID, status, date), nil
}

func (s staticUsersService) UpdatePersonalChannelWithDelivery(_ context.Context, userID int64, channelID int64, effects store.DeliveryEffectsBuilder[store.UserDeliverySnapshot]) (domain.User, error) {
	if userID != s.user.ID {
		return domain.User{}, domain.ErrUserNotFound
	}
	return testUserPersonalChannelMutation(s.user, channelID, effects)
}

func (s mapUsersService) Self(_ context.Context, userID int64) (domain.User, error) {
	return s.users[userID], nil
}

func (s mapUsersService) ByID(_ context.Context, _, userID int64) (domain.User, bool, error) {
	u, ok := s.users[userID]
	return u, ok, nil
}

func (s mapUsersService) BotStatus(_ context.Context, userID int64) (bool, bool, error) {
	u, found := s.users[userID]
	return u.Bot, found, nil
}

func (s mapUsersService) ByIDs(_ context.Context, _ int64, userIDs []int64) ([]domain.User, error) {
	out := make([]domain.User, 0, len(userIDs))
	seen := map[int64]struct{}{}
	for _, userID := range userIDs {
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		if u, ok := s.users[userID]; ok {
			out = append(out, u)
		}
	}
	return out, nil
}

func (s mapUsersService) BaseUsersByIDs(ctx context.Context, userIDs []int64) ([]domain.User, error) {
	return s.ByIDs(ctx, 1, userIDs)
}

func (s mapUsersService) ByIDsForViewers(ctx context.Context, viewerUserIDs []int64, userIDs []int64) (map[int64][]domain.User, error) {
	users, err := s.ByIDs(ctx, 0, userIDs)
	if err != nil {
		return nil, err
	}
	out := make(map[int64][]domain.User, len(viewerUserIDs))
	for _, viewerID := range viewerUserIDs {
		if viewerID == 0 {
			continue
		}
		out[viewerID] = append([]domain.User(nil), users...)
	}
	return out, nil
}

func (s mapUsersService) UpdateEmojiStatusWithEvent(_ context.Context, userID int64, status domain.UserEmojiStatus, date int) (domain.User, domain.UpdateEvent, error) {
	u, ok := s.users[userID]
	if !ok {
		return domain.User{}, domain.UpdateEvent{}, domain.ErrUserNotFound
	}
	u = testUserWithEmojiStatus(u, status)
	s.users[userID] = u
	return u, testEmojiStatusUpdateEvent(userID, status, date), nil
}

func (s mapUsersService) UpdatePersonalChannelWithDelivery(_ context.Context, userID int64, channelID int64, effects store.DeliveryEffectsBuilder[store.UserDeliverySnapshot]) (domain.User, error) {
	u, ok := s.users[userID]
	if !ok {
		return domain.User{}, domain.ErrUserNotFound
	}
	u, err := testUserPersonalChannelMutation(u, channelID, effects)
	if err != nil {
		return domain.User{}, err
	}
	s.users[userID] = u
	return u, nil
}

func (s *countingMapUsersService) ByID(ctx context.Context, currentUserID, userID int64) (domain.User, bool, error) {
	s.byIDCalls++
	return s.mapUsersService.ByID(ctx, currentUserID, userID)
}

func (s *countingMapUsersService) Self(ctx context.Context, userID int64) (domain.User, error) {
	s.selfCalls++
	return s.mapUsersService.Self(ctx, userID)
}

func (s *countingMapUsersService) ByIDs(ctx context.Context, currentUserID int64, userIDs []int64) ([]domain.User, error) {
	s.byIDsCalls++
	s.lastByIDs = append([]int64(nil), userIDs...)
	s.byIDsBatches = append(s.byIDsBatches, append([]int64(nil), userIDs...))
	return s.mapUsersService.ByIDs(ctx, currentUserID, userIDs)
}

func (s *countingMapUsersService) BaseUsersByIDs(ctx context.Context, userIDs []int64) ([]domain.User, error) {
	s.baseByIDsCalls++
	return s.mapUsersService.ByIDs(ctx, 1, userIDs)
}

type captureUsersService struct {
	user   domain.User
	userID int64
}

func (s *captureUsersService) Self(_ context.Context, userID int64) (domain.User, error) {
	s.userID = userID
	return s.user, nil
}

func (s *captureUsersService) ByID(_ context.Context, currentUserID, userID int64) (domain.User, bool, error) {
	s.userID = currentUserID
	if userID == s.user.ID {
		return s.user, true, nil
	}
	return domain.User{}, false, nil
}

func (s *captureUsersService) ByIDs(_ context.Context, currentUserID int64, userIDs []int64) ([]domain.User, error) {
	s.userID = currentUserID
	out := make([]domain.User, 0, len(userIDs))
	for _, userID := range userIDs {
		if userID == s.user.ID {
			out = append(out, s.user)
		}
	}
	return out, nil
}

func (s *captureUsersService) BaseUsersByIDs(ctx context.Context, userIDs []int64) ([]domain.User, error) {
	return s.ByIDs(ctx, 1, userIDs)
}

func (s *captureUsersService) UpdateEmojiStatusWithEvent(_ context.Context, userID int64, status domain.UserEmojiStatus, date int) (domain.User, domain.UpdateEvent, error) {
	s.userID = userID
	if userID != s.user.ID {
		return domain.User{}, domain.UpdateEvent{}, domain.ErrUserNotFound
	}
	s.user = testUserWithEmojiStatus(s.user, status)
	return s.user, testEmojiStatusUpdateEvent(userID, status, date), nil
}

func (s *captureUsersService) UpdatePersonalChannelWithDelivery(_ context.Context, userID int64, channelID int64, effects store.DeliveryEffectsBuilder[store.UserDeliverySnapshot]) (domain.User, error) {
	s.userID = userID
	if userID != s.user.ID {
		return domain.User{}, domain.ErrUserNotFound
	}
	u, err := testUserPersonalChannelMutation(s.user, channelID, effects)
	if err != nil {
		return domain.User{}, err
	}
	s.user = u
	return u, nil
}

func testUserWithEmojiStatus(user domain.User, status domain.UserEmojiStatus) domain.User {
	user.EmojiStatusDocumentID = status.DocumentID
	user.EmojiStatusUntil = status.Until
	user.EmojiStatusCollectible = status.Collectible
	return user
}

func testEmojiStatusUpdateEvent(userID int64, status domain.UserEmojiStatus, date int) domain.UpdateEvent {
	return domain.UpdateEvent{
		UserID:      userID,
		Type:        domain.UpdateEventUserEmojiStatus,
		Pts:         1,
		PtsCount:    1,
		Date:        date,
		Peer:        domain.Peer{Type: domain.PeerTypeUser, ID: userID},
		EmojiStatus: status,
	}
}

func testUserPersonalChannelMutation(user domain.User, channelID int64, effects store.DeliveryEffectsBuilder[store.UserDeliverySnapshot]) (domain.User, error) {
	if effects == nil {
		return domain.User{}, store.ErrDeliveryOutboxRequired
	}
	user.PersonalChannelID = channelID
	snapshot := store.UserDeliverySnapshot{User: user}
	intents, err := effects(snapshot)
	if err != nil {
		return domain.User{}, err
	}
	if err := store.ValidateUserSelfDeliveryEffects(snapshot, intents); err != nil {
		return domain.User{}, err
	}
	return user, nil
}
