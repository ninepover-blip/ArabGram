package rpc

import (
	"context"
	"testing"

	"github.com/iamxvbaba/td/clock"
	"github.com/iamxvbaba/td/proto"
	"github.com/iamxvbaba/td/tg"
	"go.uber.org/zap/zaptest"
)

type ordinaryOnlySessions struct {
	rawAuthKeyID [8]byte
	sessionID    int64
	authKeyID    [8]byte
	authFound    bool
	userID       int64
	userFound    bool
	message      tg.UpdatesClass
	pushUserIDs  []int64
}

func (s *ordinaryOnlySessions) BindAuthKeyForSession(rawAuthKeyID [8]byte, sessionID int64, authKeyID [8]byte) {
	s.rawAuthKeyID, s.sessionID, s.authKeyID, s.authFound = rawAuthKeyID, sessionID, authKeyID, true
}

func (s *ordinaryOnlySessions) AuthKeyIDForSession([8]byte, int64) ([8]byte, bool) {
	return s.authKeyID, s.authFound
}

func (s *ordinaryOnlySessions) BindUserForAuthKey(rawAuthKeyID [8]byte, sessionID, userID int64) {
	s.rawAuthKeyID, s.sessionID, s.userID, s.userFound = rawAuthKeyID, sessionID, userID, true
}

func (s *ordinaryOnlySessions) UserIDResolvedForAuthKey([8]byte, int64) (int64, bool) {
	return s.userID, s.userFound
}

func (s *ordinaryOnlySessions) UnbindAuthKey(authKeyID [8]byte) int {
	if s.authKeyID != authKeyID {
		return 0
	}
	s.userID, s.userFound = 0, true
	return 1
}

func (s *ordinaryOnlySessions) SetReceivesUpdatesForAuthKey(rawAuthKeyID [8]byte, sessionID int64, _ bool) {
	s.rawAuthKeyID, s.sessionID = rawAuthKeyID, sessionID
}

func (s *ordinaryOnlySessions) PushToSessionForAuthKey(_ context.Context, rawAuthKeyID [8]byte, sessionID int64, _ proto.MessageType, msg tg.UpdatesClass) error {
	s.rawAuthKeyID, s.sessionID, s.message = rawAuthKeyID, sessionID, msg
	return nil
}

func (s *ordinaryOnlySessions) PushToUserExceptAuthKeySession(_ context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, _ proto.MessageType, msg tg.UpdatesClass) (int, error) {
	s.userID, s.rawAuthKeyID, s.sessionID, s.message = userID, excludeAuthKeyID, excludeSessionID, msg
	s.pushUserIDs = append(s.pushUserIDs, userID)
	return 1, nil
}

func TestPushUserMessageTransientWithoutCapabilityDoesNotFallbackToOrdinaryPush(t *testing.T) {
	sessions := &ordinaryOnlySessions{}
	router := &Router{
		log:  zaptest.NewLogger(t),
		deps: Deps{Sessions: sessions},
	}
	ctx := WithAuthKeyID(WithSessionID(context.Background(), 77), [8]byte{1, 2, 3})

	sent := router.pushUserMessageTransient(ctx, 1000000001, "push transient test", &tg.UpdateShort{
		Update: &tg.UpdateUserTyping{UserID: 1000000002, Action: &tg.SendMessageTypingAction{}},
		Date:   1,
	})
	if sent != 0 {
		t.Fatalf("sent = %d, want 0 without TransientSessionPusher", sent)
	}
	if got := sessions.message; got != nil {
		t.Fatalf("ordinary push fallback captured %#v, want nil", got)
	}
	if ids := sessions.pushUserIDs; len(ids) != 0 {
		t.Fatalf("ordinary push fallback target IDs = %v, want none", ids)
	}
}

func TestPushLoginTokenAcceptedWithoutImmediateCapabilityDoesNotFallbackToOrdinaryPush(t *testing.T) {
	sessions := &ordinaryOnlySessions{}
	router := &Router{
		log:   zaptest.NewLogger(t),
		clock: clock.System,
		deps:  Deps{Sessions: sessions},
	}
	target := loginTokenTarget{
		rawAuthKeyID: [8]byte{9, 8, 7},
		sessionID:    77,
	}

	router.pushLoginTokenAccepted(context.Background(), target)

	if got := sessions.message; got != nil {
		t.Fatalf("ordinary push fallback captured %#v, want nil", got)
	}
}
