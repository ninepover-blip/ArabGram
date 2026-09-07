package rpc

import (
	"context"
	"sync"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/proto"
	"github.com/iamxvbaba/td/tg"
)

type captureScopedSessions struct {
	*captureSessions
	// scopedMu protects fields that async push goroutines may mutate while the
	// test goroutine is asserting scoped delivery.
	scopedMu        sync.Mutex
	scopedAuthKeyID [8]byte
	immediatePush   bool
	immediateType   proto.MessageType
	immediateMsg    bin.Encoder
}

func (s *captureScopedSessions) setScopedAuthKeyID(rawAuthKeyID [8]byte) {
	s.scopedMu.Lock()
	s.scopedAuthKeyID = rawAuthKeyID
	s.scopedMu.Unlock()
}

func (s *captureScopedSessions) scopedAuthKey() [8]byte {
	s.scopedMu.Lock()
	defer s.scopedMu.Unlock()
	return s.scopedAuthKeyID
}

func (s *captureScopedSessions) immediatePushSeen() bool {
	s.scopedMu.Lock()
	defer s.scopedMu.Unlock()
	return s.immediatePush
}

func (s *captureScopedSessions) immediatePushSnapshot() (proto.MessageType, bin.Encoder) {
	s.scopedMu.Lock()
	defer s.scopedMu.Unlock()
	return s.immediateType, s.immediateMsg
}

func (s *captureScopedSessions) BindAuthKeyForSession(rawAuthKeyID [8]byte, sessionID int64, authKeyID [8]byte) {
	s.captureSessions.BindAuthKeyForSession(rawAuthKeyID, sessionID, authKeyID)
	s.setScopedAuthKeyID(rawAuthKeyID)
}

func (s *captureScopedSessions) AuthKeyIDForSession(rawAuthKeyID [8]byte, sessionID int64) ([8]byte, bool) {
	return s.captureSessions.AuthKeyIDForSession(rawAuthKeyID, sessionID)
}

func (s *captureScopedSessions) BindUserForAuthKey(rawAuthKeyID [8]byte, sessionID, userID int64) {
	s.captureSessions.BindUserForAuthKey(rawAuthKeyID, sessionID, userID)
	s.setScopedAuthKeyID(rawAuthKeyID)
}

func (s *captureScopedSessions) UserIDResolvedForAuthKey(rawAuthKeyID [8]byte, sessionID int64) (int64, bool) {
	return s.captureSessions.UserIDResolvedForAuthKey(rawAuthKeyID, sessionID)
}

func (s *captureScopedSessions) SetReceivesUpdatesForAuthKey(rawAuthKeyID [8]byte, sessionID int64, receives bool) {
	s.captureSessions.SetReceivesUpdatesForAuthKey(rawAuthKeyID, sessionID, receives)
}

func (s *captureScopedSessions) PushToSessionForAuthKey(_ context.Context, rawAuthKeyID [8]byte, sessionID int64, t proto.MessageType, msg tg.UpdatesClass) error {
	s.setScopedAuthKeyID(rawAuthKeyID)
	return s.captureSessions.PushToSessionForAuthKey(context.Background(), rawAuthKeyID, sessionID, t, msg)
}

func (s *captureScopedSessions) PushToSessionForAuthKeyImmediate(_ context.Context, rawAuthKeyID [8]byte, sessionID int64, t proto.MessageType, msg tg.UpdatesClass) error {
	s.scopedMu.Lock()
	s.immediatePush = true
	s.scopedAuthKeyID = rawAuthKeyID
	s.immediateType = t
	s.immediateMsg = msg
	s.scopedMu.Unlock()
	return s.captureSessions.PushToSessionForAuthKey(context.Background(), rawAuthKeyID, sessionID, t, msg)
}

func (s *captureScopedSessions) PushToUserExceptAuthKeySession(_ context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg tg.UpdatesClass) (int, error) {
	s.setScopedAuthKeyID(excludeAuthKeyID)
	return s.captureSessions.PushToUserExceptAuthKeySession(context.Background(), userID, excludeAuthKeyID, excludeSessionID, t, msg)
}

func (s *captureScopedSessions) PushToUserTransientExceptAuthKeySession(_ context.Context, userID int64, excludeAuthKeyID [8]byte, excludeSessionID int64, t proto.MessageType, msg tg.UpdatesClass, timeout time.Duration) (int, error) {
	s.setScopedAuthKeyID(excludeAuthKeyID)
	return s.captureSessions.PushToUserTransientExceptAuthKeySession(context.Background(), userID, excludeAuthKeyID, excludeSessionID, t, msg, timeout)
}

func (s *captureScopedSessions) PushToUserLocationBatches(ctx context.Context, pushes []LocationTargetedUserPush) (int, error) {
	sent := 0
	var firstErr error
	for _, push := range pushes {
		messageType := push.MessageType
		if messageType == proto.MessageUnknown {
			messageType = proto.MessageFromServer
		}
		n, err := s.PushToUserExceptAuthKeySession(ctx, push.TargetUserID, push.ExcludeAuthKeyID, push.ExcludeSessionID, messageType, push.Update)
		sent += n
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return sent, firstErr
}
