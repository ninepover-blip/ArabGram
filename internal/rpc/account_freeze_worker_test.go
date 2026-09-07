package rpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iamxvbaba/td/clock"
	"github.com/iamxvbaba/td/tg"
	"go.uber.org/zap/zaptest"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestAccountFreezeNotificationPushesCurrentViewerProjection(t *testing.T) {
	const (
		viewerID = int64(1001)
		frozenID = int64(1002)
	)
	freezeSvc := &freezeWorkerService{}
	users := &freezeWorkerUsers{user: domain.User{
		ID:                 frozenID,
		FirstName:          "Frozen",
		RestrictionReasons: domain.AccountFrozenRestrictionReasons(),
	}}
	r := New(Config{}, Deps{
		AccountFreeze: freezeSvc,
		Users:         users,
	}, zaptest.NewLogger(t), clock.System)

	r.dispatchAccountFreezeNotification(context.Background(), freezeSvc, domain.AccountFreezeNotification{
		ID: 7, TargetUserID: viewerID, FrozenUserID: frozenID, Version: 4, Frozen: true,
	})

	if len(freezeSvc.committed) != 1 || freezeSvc.committed[0].id != 7 || freezeSvc.committed[0].version != 4 {
		t.Fatalf("committed = %+v, want id=7 version=4", freezeSvc.committed)
	}
	decoded, err := decodeDeliveryUpdate(freezeSvc.committed[0].payload)
	if err != nil {
		t.Fatal(err)
	}
	updates, ok := decoded.(*tg.Updates)
	if !ok || len(updates.Updates) != 1 || len(updates.Users) != 1 {
		t.Fatalf("payload = %#v, want updateUser plus projected user", decoded)
	}
	if update, ok := updates.Updates[0].(*tg.UpdateUser); !ok || update.UserID != frozenID {
		t.Fatalf("update = %#v, want updateUser(%d)", updates.Updates[0], frozenID)
	}
	projected, ok := updates.Users[0].(*tg.User)
	if !ok || !projected.Restricted {
		t.Fatalf("projected user = %#v, want restricted user", updates.Users[0])
	}
	reasons, ok := projected.GetRestrictionReason()
	if !ok || len(reasons) != 1 || reasons[0].Reason != "frozen" {
		t.Fatalf("projected restriction = %+v ok=%v", reasons, ok)
	}
}

func TestAccountFreezeNotificationLoadsCurrentStateAndRetriesLoadFailure(t *testing.T) {
	const (
		viewerID = int64(2001)
		frozenID = int64(2002)
	)
	freezeSvc := &freezeWorkerService{}
	users := &freezeWorkerUsers{err: errors.New("projection unavailable")}
	r := New(Config{}, Deps{
		AccountFreeze: freezeSvc,
		Users:         users,
	}, zaptest.NewLogger(t), clock.System)
	notification := domain.AccountFreezeNotification{
		ID: 8, TargetUserID: viewerID, FrozenUserID: frozenID, Version: 5, Frozen: true,
	}

	r.dispatchAccountFreezeNotification(context.Background(), freezeSvc, notification)
	if len(freezeSvc.committed) != 0 {
		t.Fatalf("failed load committed=%v, want retry without commit", freezeSvc.committed)
	}

	// The queued payload may say frozen, but delivery must hydrate the latest
	// viewer projection so a newer unfreeze can never be overwritten by stale work.
	users.err = nil
	users.user = domain.User{ID: frozenID, FirstName: "Active"}
	r.dispatchAccountFreezeNotification(context.Background(), freezeSvc, notification)
	decoded, err := decodeDeliveryUpdate(freezeSvc.committed[0].payload)
	if err != nil {
		t.Fatal(err)
	}
	updates, ok := decoded.(*tg.Updates)
	if !ok || len(updates.Users) != 1 {
		t.Fatalf("payload = %#v", decoded)
	}
	projected, ok := updates.Users[0].(*tg.User)
	if !ok || projected.Restricted {
		t.Fatalf("latest projected user = %#v, want unrestricted", updates.Users[0])
	}
	if len(freezeSvc.committed) != 1 || freezeSvc.committed[0].id != 8 || freezeSvc.committed[0].version != 5 {
		t.Fatalf("committed = %+v, want id=8 version=5", freezeSvc.committed)
	}
}

type freezeWorkerService struct {
	committed []freezeWorkerCommit
}

type freezeWorkerCommit struct {
	id, version int64
	payload     []byte
}

func (*freezeWorkerService) AccountFreeze(context.Context, int64) (domain.AccountFreeze, bool, error) {
	return domain.AccountFreeze{}, false, nil
}

func (*freezeWorkerService) ClaimAccountFreezeNotifications(context.Context, time.Time, int, time.Duration) ([]domain.AccountFreezeNotification, error) {
	return nil, nil
}

func (s *freezeWorkerService) CommitAccountFreezeNotificationDelivery(_ context.Context, id, version int64, payload []byte, _ time.Time) error {
	s.committed = append(s.committed, freezeWorkerCommit{id: id, version: version, payload: append([]byte(nil), payload...)})
	return nil
}

type freezeWorkerUsers struct {
	user domain.User
	err  error
}

func (s *freezeWorkerUsers) Self(context.Context, int64) (domain.User, error) {
	return s.user, s.err
}

func (s *freezeWorkerUsers) ByID(context.Context, int64, int64) (domain.User, bool, error) {
	return s.user, s.err == nil, s.err
}

func (s *freezeWorkerUsers) ByIDs(context.Context, int64, []int64) ([]domain.User, error) {
	if s.err != nil {
		return nil, s.err
	}
	return []domain.User{s.user}, nil
}

func (s *freezeWorkerUsers) UpdateEmojiStatusWithEvent(_ context.Context, userID int64, status domain.UserEmojiStatus, date int) (domain.User, domain.UpdateEvent, error) {
	if s.err != nil {
		return domain.User{}, domain.UpdateEvent{}, s.err
	}
	if userID != s.user.ID {
		return domain.User{}, domain.UpdateEvent{}, domain.ErrUserNotFound
	}
	s.user = testUserWithEmojiStatus(s.user, status)
	return s.user, testEmojiStatusUpdateEvent(userID, status, date), nil
}

func (s *freezeWorkerUsers) UpdatePersonalChannelWithDelivery(_ context.Context, userID int64, channelID int64, effects store.DeliveryEffectsBuilder[store.UserDeliverySnapshot]) (domain.User, error) {
	if s.err != nil {
		return domain.User{}, s.err
	}
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
