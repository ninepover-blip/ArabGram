package rpc

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/iamxvbaba/td/clock"
	"github.com/iamxvbaba/td/proto"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tgerr"
	"go.uber.org/zap/zaptest"

	"telesrv/internal/domain"
	"telesrv/internal/store"
	"telesrv/internal/store/memory"
)

func TestAuthLoginTokenAcceptedByAndroidBindsTargetSession(t *testing.T) {
	const (
		targetSession  = int64(101)
		scannerSession = int64(202)
		scannerUserID  = int64(1000000001)
	)
	targetRawAuthKeyID := [8]byte{0x10, 0x20, 0x30, 0x40, 0x50, 0x60, 0x70, 0x80}
	targetAuthKeyID := [8]byte{0x81, 0x71, 0x61, 0x51, 0x41, 0x31, 0x21, 0x11}
	scannerRawAuthKeyID := [8]byte{0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99}
	scannerAuthKeyID := [8]byte{0x99, 0x88, 0x77, 0x66, 0x55, 0x44, 0x33, 0x22}

	auth := &captureAuthService{}
	sessions := &captureScopedSessions{captureSessions: &captureSessions{}}
	users := mapUsersService{users: map[int64]domain.User{
		scannerUserID: {ID: scannerUserID, FirstName: "Alice", Phone: "15550001001"},
	}}
	r := New(Config{DC: 2, IP: "127.0.0.1", Port: 2398}, Deps{
		Auth:           auth,
		Sessions:       sessions,
		Users:          users,
		LoginTokens:    newTestLoginTokenStore(),
		DeliveryOutbox: memory.NewDeliveryOutboxStore(),
	}, zaptest.NewLogger(t), clock.System)

	targetCtx := WithClientInfo(
		WithLayer(
			WithSessionID(
				WithAuthKeyID(
					WithRawAuthKeyID(context.Background(), targetRawAuthKeyID),
					targetAuthKeyID,
				),
				targetSession,
			),
			currentClientLayer,
		),
		ClientInfo{
			APIID:         2040,
			DeviceModel:   "WebA",
			SystemVersion: "Chrome",
			AppVersion:    "1.0",
			LangPack:      "tdesktop",
		},
	)
	exported, err := r.onAuthExportLoginToken(targetCtx, &tg.AuthExportLoginTokenRequest{APIID: 2040, APIHash: "hash"})
	if err != nil {
		t.Fatalf("export login token: %v", err)
	}
	loginToken, ok := exported.(*tg.AuthLoginToken)
	if !ok {
		t.Fatalf("export type = %T, want *tg.AuthLoginToken", exported)
	}
	if len(loginToken.Token) != loginTokenBytes {
		t.Fatalf("token length = %d, want %d", len(loginToken.Token), loginTokenBytes)
	}

	unauthorizedScannerCtx := WithSessionID(
		WithAuthKeyID(
			WithRawAuthKeyID(context.Background(), scannerRawAuthKeyID),
			scannerAuthKeyID,
		),
		scannerSession,
	)
	if _, err := r.onAuthAcceptLoginToken(unauthorizedScannerCtx, loginToken.Token); !tgerr.Is(err, "AUTH_KEY_UNREGISTERED") {
		t.Fatalf("unauthorized accept err = %v, want AUTH_KEY_UNREGISTERED", err)
	}

	scannerCtx := WithUserID(unauthorizedScannerCtx, scannerUserID)
	authorization, err := r.onAuthAcceptLoginToken(scannerCtx, loginToken.Token)
	if err != nil {
		t.Fatalf("accept login token: %v", err)
	}
	if authorization == nil {
		t.Fatal("accept login token returned nil authorization")
	}
	if auth.acceptedUserID != scannerUserID {
		t.Fatalf("accepted user id = %d, want %d", auth.acceptedUserID, scannerUserID)
	}
	if auth.acceptedAuth.AuthKeyID != targetAuthKeyID {
		t.Fatalf("accepted auth key = %x, want %x", auth.acceptedAuth.AuthKeyID, targetAuthKeyID)
	}
	if auth.acceptedAuth.DeviceModel != "WebA" {
		t.Fatalf("accepted device = %q, want WebA", auth.acceptedAuth.DeviceModel)
	}
	if len(auth.authorizationEffects) != 1 || auth.authorizationEffects[0].ExcludeAuthKeyID != targetRawAuthKeyID || auth.authorizationEffects[0].ExcludeSessionID != targetSession {
		t.Fatalf("QR authorization effects = %+v, want target raw key/session exclusion", auth.authorizationEffects)
	}
	if authorization.Hash == 0 {
		t.Fatal("authorization hash is zero")
	}

	snap := sessions.snapshot()
	if got := sessions.scopedAuthKey(); got != targetRawAuthKeyID {
		t.Fatalf("scoped raw auth key = %x, want %x", got, targetRawAuthKeyID)
	}
	if snap.sessionID != targetSession || snap.userID != scannerUserID || !snap.userResolved {
		t.Fatalf("target session snapshot = %+v, want session/user/resolved %d/%d/true", snap, targetSession, scannerUserID)
	}
	if !sessions.immediatePushSeen() {
		t.Fatal("login token update was not pushed through the immediate pre-auth path")
	}
	immediateType, immediateMessage := sessions.immediatePushSnapshot()
	if immediateType != proto.MessageFromServer {
		t.Fatalf("immediate push message type = %v, want MessageFromServer", immediateType)
	}
	short, ok := immediateMessage.(*tg.UpdateShort)
	if !ok {
		t.Fatalf("immediate push message = %T, want *tg.UpdateShort", immediateMessage)
	}
	if _, ok := short.Update.(*tg.UpdateLoginToken); !ok {
		t.Fatalf("pushed update = %T, want *tg.UpdateLoginToken", short.Update)
	}

	success, err := r.onAuthExportLoginToken(targetCtx, &tg.AuthExportLoginTokenRequest{APIID: 2040, APIHash: "hash"})
	if err != nil {
		t.Fatalf("export after accept: %v", err)
	}
	loginSuccess, ok := success.(*tg.AuthLoginTokenSuccess)
	if !ok {
		t.Fatalf("export after accept type = %T, want *tg.AuthLoginTokenSuccess", success)
	}
	authz, ok := loginSuccess.Authorization.(*tg.AuthAuthorization)
	if !ok {
		t.Fatalf("success authorization = %T, want *tg.AuthAuthorization", loginSuccess.Authorization)
	}
	user, ok := authz.User.(*tg.User)
	if !ok {
		t.Fatalf("success user = %T, want *tg.User", authz.User)
	}
	if user.ID != scannerUserID {
		t.Fatalf("success user id = %d, want %d", user.ID, scannerUserID)
	}

	if _, err := r.onAuthAcceptLoginToken(scannerCtx, loginToken.Token); !tgerr.Is(err, "AUTH_TOKEN_ALREADY_ACCEPTED") {
		t.Fatalf("duplicate accept err = %v, want AUTH_TOKEN_ALREADY_ACCEPTED", err)
	}
}

func TestAuthLoginTokenExpires(t *testing.T) {
	now := time.Unix(1700000000, 0)
	reg := newLoginTokenRegistry(newTestLoginTokenStore())
	target := loginTokenTarget{
		rawAuthKeyID: [8]byte{1},
		authKeyID:    [8]byte{2},
		sessionID:    3,
	}
	exported, err := reg.export(now, target, domain.Authorization{AuthKeyID: target.authKeyID}, nil)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if _, err := reg.beginAccept(now.Add(loginTokenTTL+time.Second), exported.token, 1000000001); !tgerr.Is(err, "AUTH_TOKEN_EXPIRED") {
		t.Fatalf("expired accept err = %v, want AUTH_TOKEN_EXPIRED", err)
	}
}

func TestLoginTokenRegistrySharedStoreCrossInstance(t *testing.T) {
	now := time.Unix(1700000000, 0)
	shared := newTestLoginTokenStore()
	target := loginTokenTarget{
		rawAuthKeyID: [8]byte{0xa1},
		authKeyID:    [8]byte{0xb1},
		sessionID:    301,
	}
	authz := domain.Authorization{AuthKeyID: target.authKeyID, DeviceModel: "QR target"}
	const (
		deniedUserID  = int64(1000000001)
		scannerUserID = int64(1000000002)
	)
	exporter := newLoginTokenRegistry(shared)
	scanner := newLoginTokenRegistry(shared)

	exported, err := exporter.exportContext(context.Background(), now, target, authz, []int64{deniedUserID})
	if err != nil {
		t.Fatalf("export shared token: %v", err)
	}
	if len(exported.token) != loginTokenBytes {
		t.Fatalf("token length = %d, want %d", len(exported.token), loginTokenBytes)
	}
	imported, err := scanner.lookupContext(context.Background(), now, exported.token)
	if err != nil {
		t.Fatalf("lookup shared token: %v", err)
	}
	if !bytes.Equal(imported.token, exported.token) || !imported.expires.Equal(exported.expires) {
		t.Fatalf("imported token = %x/%v, want %x/%v", imported.token, imported.expires, exported.token, exported.expires)
	}
	if _, err := scanner.beginAcceptContext(context.Background(), now, exported.token, deniedUserID); !tgerr.Is(err, "AUTH_TOKEN_ALREADY_ACCEPTED") {
		t.Fatalf("except-id accept err = %v, want AUTH_TOKEN_ALREADY_ACCEPTED", err)
	}
	accept, err := scanner.beginAcceptContext(context.Background(), now, exported.token, scannerUserID)
	if err != nil {
		t.Fatalf("begin shared accept: %v", err)
	}
	if accept.target != target || accept.authz.AuthKeyID != target.authKeyID || accept.authz.DeviceModel != authz.DeviceModel {
		t.Fatalf("accept start = %+v, want target/authz", accept)
	}
	if _, err := exporter.beginAcceptContext(context.Background(), now, exported.token, scannerUserID+1); !tgerr.Is(err, "AUTH_TOKEN_ALREADY_ACCEPTED") {
		t.Fatalf("second concurrent accept err = %v, want AUTH_TOKEN_ALREADY_ACCEPTED", err)
	}
	acceptedAuth := accept.authz
	acceptedAuth.UserID = scannerUserID
	scanner.finishAcceptContext(context.Background(), now.Add(time.Second), exported.token, scannerUserID, acceptedAuth)

	success, err := exporter.exportContext(context.Background(), now.Add(2*time.Second), target, authz, nil)
	if err != nil {
		t.Fatalf("export accepted shared token: %v", err)
	}
	if !success.accepted || success.acceptedAuth.UserID != scannerUserID || success.acceptedAuth.AuthKeyID != target.authKeyID {
		t.Fatalf("accepted export = %+v, want user/auth", success)
	}
	if _, err := exporter.beginAcceptContext(context.Background(), now.Add(2*time.Second), exported.token, scannerUserID); !tgerr.Is(err, "AUTH_TOKEN_ALREADY_ACCEPTED") {
		t.Fatalf("duplicate accepted err = %v, want AUTH_TOKEN_ALREADY_ACCEPTED", err)
	}
}

func TestLoginTokenRegistryRequiresSharedStore(t *testing.T) {
	now := time.Unix(1700000000, 0)
	reg := newLoginTokenRegistry()
	target := loginTokenTarget{rawAuthKeyID: [8]byte{1}, authKeyID: [8]byte{2}, sessionID: 3}
	if _, err := reg.export(now, target, domain.Authorization{AuthKeyID: target.authKeyID}, nil); err == nil {
		t.Fatal("export without shared store succeeded")
	}
	if _, err := reg.lookup(now, []byte{1}); err == nil {
		t.Fatal("lookup without shared store succeeded")
	}
	if _, err := reg.beginAccept(now, []byte{1}, 1000000001); err == nil {
		t.Fatal("begin accept without shared store succeeded")
	}
}

type testLoginTokenStore struct {
	mu       sync.Mutex
	byToken  map[string]store.LoginTokenRecord
	byTarget map[store.LoginTokenTarget]string
}

func newTestLoginTokenStore() *testLoginTokenStore {
	return &testLoginTokenStore{
		byToken:  make(map[string]store.LoginTokenRecord),
		byTarget: make(map[store.LoginTokenTarget]string),
	}
}

func (s *testLoginTokenStore) GetLoginTokenByTarget(_ context.Context, target store.LoginTokenTarget) (store.LoginTokenRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tokenKey, ok := s.byTarget[target]
	if !ok {
		return store.LoginTokenRecord{}, false, nil
	}
	rec, ok := s.byToken[tokenKey]
	if !ok {
		delete(s.byTarget, target)
		return store.LoginTokenRecord{}, false, nil
	}
	return cloneTestLoginTokenRecord(rec), true, nil
}

func (s *testLoginTokenStore) PutLoginToken(_ context.Context, record store.LoginTokenRecord, _ time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tokenKey := string(record.Token)
	if _, ok := s.byToken[tokenKey]; ok {
		return false, nil
	}
	if _, ok := s.byTarget[record.Target]; ok {
		return false, nil
	}
	s.byToken[tokenKey] = cloneTestLoginTokenRecord(record)
	s.byTarget[record.Target] = tokenKey
	return true, nil
}

func (s *testLoginTokenStore) GetLoginToken(_ context.Context, token []byte) (store.LoginTokenRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.byToken[string(token)]
	if !ok {
		return store.LoginTokenRecord{}, false, nil
	}
	return cloneTestLoginTokenRecord(rec), true, nil
}

func (s *testLoginTokenStore) BeginLoginTokenAccept(_ context.Context, token []byte, userID int64, now time.Time) (store.LoginTokenRecord, store.LoginTokenAcceptStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.byToken[string(token)]
	if !ok {
		return store.LoginTokenRecord{}, store.LoginTokenAcceptMissing, nil
	}
	if !rec.ExpiresAt().After(now) {
		return cloneTestLoginTokenRecord(rec), store.LoginTokenAcceptExpired, nil
	}
	if rec.Accepted || rec.Accepting {
		return cloneTestLoginTokenRecord(rec), store.LoginTokenAcceptAlreadyAccepted, nil
	}
	for _, denied := range rec.ExceptIDs {
		if denied == userID {
			return cloneTestLoginTokenRecord(rec), store.LoginTokenAcceptAlreadyAccepted, nil
		}
	}
	rec.Accepting = true
	s.byToken[string(token)] = rec
	return cloneTestLoginTokenRecord(rec), store.LoginTokenAcceptStarted, nil
}

func (s *testLoginTokenStore) FinishLoginTokenAccept(_ context.Context, token []byte, userID int64, accepted domain.Authorization, now time.Time) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.byToken[string(token)]
	if !ok || rec.Accepted || !rec.Accepting {
		return false, nil
	}
	rec.Accepting = false
	rec.Accepted = true
	rec.AcceptedUserID = userID
	rec.AcceptedAuthorization = accepted
	rec.AcceptedAtUnixMilli = now.UnixMilli()
	s.byToken[string(token)] = rec
	return true, nil
}

func (s *testLoginTokenStore) FailLoginTokenAccept(_ context.Context, token []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.byToken[string(token)]
	if !ok || rec.Accepted {
		return nil
	}
	rec.Accepting = false
	s.byToken[string(token)] = rec
	return nil
}

func (s *testLoginTokenStore) DeleteLoginToken(_ context.Context, token []byte, target store.LoginTokenTarget) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.byToken, string(token))
	if s.byTarget[target] == string(token) {
		delete(s.byTarget, target)
	}
	return nil
}

func cloneTestLoginTokenRecord(rec store.LoginTokenRecord) store.LoginTokenRecord {
	rec.Token = append([]byte(nil), rec.Token...)
	rec.ExceptIDs = append([]int64(nil), rec.ExceptIDs...)
	return rec
}
