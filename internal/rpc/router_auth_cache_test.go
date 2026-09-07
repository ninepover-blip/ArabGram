package rpc

import (
	"context"
	"encoding/binary"
	"errors"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/clock"
	"github.com/iamxvbaba/td/proto"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tgerr"
	"go.uber.org/zap/zaptest"
	"testing"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

// authBindingCaptureSessions keeps session authorization state separate from the target of an
// asynchronous presence push. The broad captureSessions fake intentionally records the latest
// PushToUser target in userID, which is useful to most RPC tests but can race a stale-auth-key
// assertion and make an old presence echo look like the session was rebound.
type authBindingCaptureSessions struct {
	*captureSessions
}

func newAuthBindingCaptureSessions() *authBindingCaptureSessions {
	return &authBindingCaptureSessions{captureSessions: &captureSessions{}}
}

func (s *authBindingCaptureSessions) PushToUserExceptAuthKeySession(_ context.Context, userID int64, _ [8]byte, _ int64, t proto.MessageType, msg tg.UpdatesClass) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messageType = t
	s.message = msg
	s.userMessage = msg
	s.pushUserIDs = append(s.pushUserIDs, userID)
	return 1, nil
}

// PushToUserTransientExceptAuthKeySession must also be overridden: the presence
// announcer (announceSessionOnline -> pushSessionOnlineAsync) delivers status
// pushes through this transient variant on a background goroutine. Without
// this override the embedded captureSessions.PushToUserTransientExceptAuthKeySession
// promotion calls its own PushToUserExceptAuthKeySession (not this type's
// override, since Go embedding has no virtual dispatch), which writes userID
// as a side effect. That async write can land after revokeAuthKeySessions has
// already cleared the session, flakily resurrecting a revoked identity.
func (s *authBindingCaptureSessions) PushToUserTransientExceptAuthKeySession(_ context.Context, userID int64, _ [8]byte, _ int64, t proto.MessageType, msg tg.UpdatesClass, _ time.Duration) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messageType = t
	s.message = msg
	s.userMessage = msg
	s.pushUserIDs = append(s.pushUserIDs, userID)
	return 1, nil
}

func TestDispatchPromotesNegativeSessionCacheFromPositiveAuthCache(t *testing.T) {
	authKeyID := [8]byte{0x91, 0x91, 0x91, 0x91, 0x91, 0x91, 0x91, 0x91}
	const (
		sessionID = int64(300)
		userID    = int64(1000000001)
	)
	sessions := newAuthBindingCaptureSessions()
	sessions.BindAuthKeyForSession(authKeyID, sessionID, authKeyID)
	sessions.BindUserForAuthKey(authKeyID, sessionID, 0)
	auth := &captureAuthService{}
	r := New(Config{}, Deps{
		Auth:     auth,
		Sessions: sessions,
	}, zaptest.NewLogger(t), clock.System)
	r.setAuthUserCache(authKeyID, userID, true)

	var in bin.Buffer
	if err := (&tg.AccountGetAuthorizationsRequest{}).Encode(&in); err != nil {
		t.Fatalf("encode authenticated probe: %v", err)
	}
	enc, err := r.Dispatch(context.Background(), authKeyID, sessionID, &in)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if _, ok := dispatchCanonicalValue(enc).(*tg.AccountAuthorizations); !ok {
		t.Fatalf("dispatch result = %#v (%T), want *tg.AccountAuthorizations", dispatchCanonicalValue(enc), enc)
	}
	gotSession := sessions.snapshot()
	if gotSession.userID != userID || !gotSession.userResolved {
		t.Fatalf("session user = %d resolved %v, want %d/true", gotSession.userID, gotSession.userResolved, userID)
	}
	if auth.userIDCount != 0 {
		t.Fatalf("auth UserID lookups = %d, want 0", auth.userIDCount)
	}
}

func TestAuthUserLookupDoesNotCacheNegativeResult(t *testing.T) {
	authKeyID := [8]byte{0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42, 0x42}
	const userID = int64(1000000042)
	auth := &captureAuthService{}
	r := New(Config{}, Deps{
		Auth: auth,
	}, zaptest.NewLogger(t), clock.System)

	gotUserID, found, err := r.lookupAuthUser(context.Background(), authKeyID)
	if err != nil {
		t.Fatalf("first lookup: %v", err)
	}
	if gotUserID != 0 || found {
		t.Fatalf("first lookup = %d/%v, want 0/false", gotUserID, found)
	}
	if _, _, ok := r.cachedAuthUser(authKeyID); ok {
		t.Fatal("negative auth user lookup was cached")
	}

	auth.userID = userID
	gotUserID, found, err = r.lookupAuthUser(context.Background(), authKeyID)
	if err != nil {
		t.Fatalf("second lookup: %v", err)
	}
	if gotUserID != userID || !found {
		t.Fatalf("second lookup = %d/%v, want %d/true", gotUserID, found, userID)
	}
	if auth.userIDCount != 2 {
		t.Fatalf("auth UserID lookups = %d, want miss then recheck", auth.userIDCount)
	}
}

func TestAuthUserCacheTTLRechecksDurableAuthorization(t *testing.T) {
	authKeyID := [8]byte{0x43, 0x43, 0x43, 0x43, 0x43, 0x43, 0x43, 0x43}
	const userID = int64(1000000043)
	now := time.Unix(1700000000, 0)
	auth := &captureAuthService{userID: userID}
	r := New(Config{AuthUserCacheTTL: time.Minute}, Deps{
		Auth: auth,
	}, zaptest.NewLogger(t), fixedClock{now: now})

	gotUserID, found, err := r.lookupAuthUser(context.Background(), authKeyID)
	if err != nil || !found || gotUserID != userID {
		t.Fatalf("first lookup = %d/%v err=%v, want %d/true", gotUserID, found, err, userID)
	}
	auth.userID = 0
	gotUserID, found, err = r.lookupAuthUser(context.Background(), authKeyID)
	if err != nil || !found || gotUserID != userID {
		t.Fatalf("cached lookup = %d/%v err=%v, want cached %d/true", gotUserID, found, err, userID)
	}
	if auth.userIDCount != 1 {
		t.Fatalf("auth UserID lookups before TTL = %d, want 1", auth.userIDCount)
	}

	r.clock = fixedClock{now: now.Add(time.Minute)}
	gotUserID, found, err = r.lookupAuthUser(context.Background(), authKeyID)
	if err != nil {
		t.Fatalf("expired lookup: %v", err)
	}
	if gotUserID != 0 || found {
		t.Fatalf("expired lookup = %d/%v, want revoked 0/false", gotUserID, found)
	}
	if auth.userIDCount != 2 {
		t.Fatalf("auth UserID lookups after TTL = %d, want recheck", auth.userIDCount)
	}
	if _, _, ok := r.cachedAuthUser(authKeyID); ok {
		t.Fatal("expired revoked auth user cache was retained")
	}
}

func TestDispatchRevalidatesExpiredSessionAuthUserCache(t *testing.T) {
	authKeyID := [8]byte{0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44}
	const (
		sessionID = int64(301)
		userID    = int64(1000000044)
	)
	now := time.Unix(1700001000, 0)
	sessions := newAuthBindingCaptureSessions()
	sessions.BindAuthKeyForSession(authKeyID, sessionID, authKeyID)
	sessions.BindUserForAuthKey(authKeyID, sessionID, userID)
	auth := &captureAuthService{userID: userID}
	r := New(Config{AuthUserCacheTTL: time.Minute}, Deps{
		Auth:     auth,
		Sessions: sessions,
	}, zaptest.NewLogger(t), fixedClock{now: now})
	r.setAuthUserCache(authKeyID, userID, true)

	var first bin.Buffer
	if err := (&tg.AccountGetAuthorizationsRequest{}).Encode(&first); err != nil {
		t.Fatalf("encode first authenticated probe: %v", err)
	}
	auth.userID = 0
	if _, err := r.Dispatch(context.Background(), authKeyID, sessionID, &first); err != nil {
		t.Fatalf("fresh session auth cache dispatch: %v", err)
	}
	if auth.userIDCount != 0 {
		t.Fatalf("auth lookups before TTL = %d, want fresh cache hit", auth.userIDCount)
	}

	r.clock = fixedClock{now: now.Add(time.Minute)}
	var second bin.Buffer
	if err := (&tg.AccountGetAuthorizationsRequest{}).Encode(&second); err != nil {
		t.Fatalf("encode second authenticated probe: %v", err)
	}
	if _, err := r.Dispatch(context.Background(), authKeyID, sessionID, &second); err == nil || !tgerr.Is(err, "AUTH_KEY_UNREGISTERED") {
		t.Fatalf("expired session auth dispatch err = %v, want AUTH_KEY_UNREGISTERED", err)
	}
	if auth.userIDCount != 1 {
		t.Fatalf("auth lookups after TTL = %d, want durable recheck", auth.userIDCount)
	}
	gotSession := sessions.snapshot()
	if gotSession.userID != 0 || !gotSession.userResolved {
		t.Fatalf("session user after TTL revoke = %d resolved %v, want 0/true", gotSession.userID, gotSession.userResolved)
	}
}

func TestBindTempAuthKeyClearsUnauthenticatedSessionUser(t *testing.T) {
	var tempAuthKeyID = [8]byte{0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55, 0x55}
	var permAuthKeyID = [8]byte{0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11, 0x11}
	sessions := newAuthBindingCaptureSessions()
	auth := &captureAuthService{}
	layers := newLayerEvidenceTestStore()
	r := New(Config{}, Deps{
		Auth:                 auth,
		Sessions:             sessions,
		AuthKeySessionLayers: layers,
	}, zaptest.NewLogger(t), clock.System)
	req := &tg.AuthBindTempAuthKeyRequest{
		PermAuthKeyID: int64(binary.LittleEndian.Uint64(permAuthKeyID[:])),
		Nonce:         2,
		ExpiresAt:     int(time.Now().Add(time.Hour).Unix()),
	}
	var in bin.Buffer
	if err := req.Encode(&in); err != nil {
		t.Fatalf("encode request: %v", err)
	}

	if _, err := r.Dispatch(context.Background(), tempAuthKeyID, 123, &in); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if auth.userIDCount != 1 {
		t.Fatalf("user lookup count = %d, want one negative lookup before temp binding", auth.userIDCount)
	}
	gotSession := sessions.snapshot()
	if gotSession.authKeyID != permAuthKeyID || !gotSession.authKeyResolved {
		t.Fatalf("session auth key = %x resolved %v, want perm %x", gotSession.authKeyID, gotSession.authKeyResolved, permAuthKeyID)
	}
	if gotSession.userResolved || gotSession.userID != 0 {
		t.Fatalf("session user = user %d resolved %v, want cleared after auth key switch", gotSession.userID, gotSession.userResolved)
	}
}

func TestDispatchRevalidatesCachedTempAuthKeyBinding(t *testing.T) {
	var tempAuthKeyID = [8]byte{0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65, 0x65}
	var permAuthKeyID = [8]byte{0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21, 0x21}
	sessions := newAuthBindingCaptureSessions()
	auth := &captureAuthService{
		resolvedAuthKeyID: permAuthKeyID,
		hasResolved:       true,
		userID:            1000000001,
	}
	r := New(Config{}, Deps{
		Auth:     auth,
		Sessions: sessions,
	}, zaptest.NewLogger(t), clock.System)

	var first bin.Buffer
	if err := (&tg.AccountGetAuthorizationsRequest{}).Encode(&first); err != nil {
		t.Fatalf("encode first authenticated probe: %v", err)
	}
	if enc, err := r.Dispatch(context.Background(), tempAuthKeyID, 123, &first); err != nil {
		t.Fatalf("first dispatch: %v", err)
	} else if _, ok := dispatchCanonicalValue(enc).(*tg.AccountAuthorizations); !ok {
		t.Fatalf("first dispatch result = %#v (%T), want *tg.AccountAuthorizations", dispatchCanonicalValue(enc), enc)
	}
	gotSession := sessions.snapshot()
	if gotSession.authKeyID != permAuthKeyID || gotSession.userID != 1000000001 {
		t.Fatalf("session after valid temp binding = auth %x user %d, want perm/user", gotSession.authKeyID, gotSession.userID)
	}

	auth.hasResolved = false
	auth.resolvedAuthKeyID = [8]byte{}
	auth.userID = 0
	var second bin.Buffer
	if err := (&tg.AccountGetAuthorizationsRequest{}).Encode(&second); err != nil {
		t.Fatalf("encode second authenticated probe: %v", err)
	}
	if _, err := r.Dispatch(context.Background(), tempAuthKeyID, 123, &second); err == nil || !tgerr.Is(err, "AUTH_KEY_UNREGISTERED") {
		t.Fatalf("second dispatch err = %v, want AUTH_KEY_UNREGISTERED after temp binding no longer resolves", err)
	}
	gotSession = sessions.snapshot()
	if gotSession.authKeyID != tempAuthKeyID || !gotSession.authKeyResolved {
		t.Fatalf("session auth after stale temp binding = %x resolved %v, want raw temp", gotSession.authKeyID, gotSession.authKeyResolved)
	}
	if gotSession.userID != 0 || !gotSession.userResolved {
		t.Fatalf("session user after stale temp binding = %d resolved %v, want 0/true", gotSession.userID, gotSession.userResolved)
	}
	if auth.resolveCount != 2 {
		t.Fatalf("ResolveAuthKey calls = %d, want revalidation on cached temp mapping", auth.resolveCount)
	}
}

func TestDispatchUsesCachedTempAuthKeyUserUntilWriteSideInvalidation(t *testing.T) {
	var tempAuthKeyID = [8]byte{0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66, 0x66}
	var permAuthKeyID = [8]byte{0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22, 0x22}
	sessions := newAuthBindingCaptureSessions()
	auth := &captureAuthService{
		resolvedAuthKeyID: permAuthKeyID,
		hasResolved:       true,
		userID:            1000000001,
	}
	r := New(Config{}, Deps{
		Auth:     auth,
		Sessions: sessions,
	}, zaptest.NewLogger(t), clock.System)

	var first bin.Buffer
	if err := (&tg.AccountGetAuthorizationsRequest{}).Encode(&first); err != nil {
		t.Fatalf("encode first authenticated probe: %v", err)
	}
	if _, err := r.Dispatch(context.Background(), tempAuthKeyID, 124, &first); err != nil {
		t.Fatalf("first dispatch: %v", err)
	}

	auth.userID = 0
	var second bin.Buffer
	if err := (&tg.AccountGetAuthorizationsRequest{}).Encode(&second); err != nil {
		t.Fatalf("encode second authenticated probe: %v", err)
	}
	if _, err := r.Dispatch(context.Background(), tempAuthKeyID, 124, &second); err != nil {
		t.Fatalf("second dispatch should use cached user until write-side invalidation: %v", err)
	}
	gotSession := sessions.snapshot()
	if gotSession.authKeyID != permAuthKeyID || !gotSession.authKeyResolved {
		t.Fatalf("session auth = %x resolved %v, want still mapped perm", gotSession.authKeyID, gotSession.authKeyResolved)
	}
	if gotSession.userID != 1000000001 || !gotSession.userResolved {
		t.Fatalf("session user = %d resolved %v, want cached user", gotSession.userID, gotSession.userResolved)
	}
	if auth.resolveCount != 2 || auth.userIDCount != 1 {
		t.Fatalf("lookups = resolve %d user %d, want temp mapping checks and cached user identity", auth.resolveCount, auth.userIDCount)
	}

	r.revokeAuthKeySessions(permAuthKeyID)
	var third bin.Buffer
	if err := (&tg.AccountGetAuthorizationsRequest{}).Encode(&third); err != nil {
		t.Fatalf("encode third authenticated probe: %v", err)
	}
	if _, err := r.Dispatch(context.Background(), tempAuthKeyID, 124, &third); err == nil || !tgerr.Is(err, "AUTH_KEY_UNREGISTERED") {
		t.Fatalf("third dispatch err = %v, want AUTH_KEY_UNREGISTERED after write-side invalidation", err)
	}
	gotSession = sessions.snapshot()
	if gotSession.userID != 0 || !gotSession.userResolved {
		t.Fatalf("session user after write-side invalidation = %d resolved %v, want 0/true", gotSession.userID, gotSession.userResolved)
	}
}

func TestRemoteAuthInvalidationClearsAuthAndTempCaches(t *testing.T) {
	var tempAuthKeyID = [8]byte{0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77, 0x77}
	var permAuthKeyID = [8]byte{0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33}
	r := New(Config{TempKeyResolveCacheTTL: time.Hour}, Deps{}, zaptest.NewLogger(t), clock.System)
	now := r.clock.Now()
	r.setAuthUserCache(permAuthKeyID, 1000000001, true)
	r.setAuthUserCache(tempAuthKeyID, 1000000001, true)
	r.tempKeyResolveCache.Store(tempAuthKeyID, permAuthKeyID, now.Add(time.Hour), now)

	r.handleRemoteAuthInvalidation(context.Background(), store.AuthInvalidationEvent{
		SourceID:   "remote-core",
		AuthKeyIDs: [][8]byte{permAuthKeyID},
		DateUnix:   now.Unix(),
	})

	if _, _, ok := r.cachedAuthUser(permAuthKeyID); ok {
		t.Fatal("permanent auth user cache survived remote invalidation")
	}
	if _, _, ok := r.cachedAuthUser(tempAuthKeyID); ok {
		t.Fatal("temp auth user cache survived remote invalidation")
	}
	if _, ok := r.tempKeyResolveCache.Get(tempAuthKeyID, permAuthKeyID, r.clock.Now()); ok {
		t.Fatal("temp auth-key resolve cache survived remote invalidation")
	}
}

func TestRevokeAuthKeySessionsPublishesAuthInvalidationWithTempAliases(t *testing.T) {
	var tempAuthKeyID = [8]byte{0x78, 0x78, 0x78, 0x78, 0x78, 0x78, 0x78, 0x78}
	var permAuthKeyID = [8]byte{0x34, 0x34, 0x34, 0x34, 0x34, 0x34, 0x34, 0x34}
	broker := &captureAuthInvalidationBroker{}
	r := New(Config{TempKeyResolveCacheTTL: time.Hour}, Deps{
		AuthInvalidations: broker,
		Sessions:          &captureSessions{},
	}, zaptest.NewLogger(t), clock.System)
	now := r.clock.Now()
	r.tempKeyResolveCache.Store(tempAuthKeyID, permAuthKeyID, now.Add(time.Hour), now)

	r.revokeAuthKeySessions(permAuthKeyID)

	if len(broker.events) != 1 {
		t.Fatalf("published events = %d, want 1", len(broker.events))
	}
	got := map[[8]byte]bool{}
	for _, id := range broker.events[0].AuthKeyIDs {
		got[id] = true
	}
	if !got[permAuthKeyID] || !got[tempAuthKeyID] {
		t.Fatalf("published auth key ids = %+v, want perm and temp aliases", broker.events[0].AuthKeyIDs)
	}
	if broker.events[0].SourceID != r.instanceID || broker.events[0].DateUnix == 0 {
		t.Fatalf("event metadata = %+v", broker.events[0])
	}
}

func TestAuthRevocationEntrypointsPublishInvalidation(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, r *Router, ctx context.Context, revoked [8]byte)
	}{
		{
			name: "account.resetAuthorization",
			run: func(t *testing.T, r *Router, ctx context.Context, revoked [8]byte) {
				t.Helper()
				if ok, err := r.onAccountResetAuthorization(ctx, 7001); err != nil || !ok {
					t.Fatalf("account.resetAuthorization ok=%v err=%v", ok, err)
				}
			},
		},
		{
			name: "auth.resetAuthorizations",
			run: func(t *testing.T, r *Router, ctx context.Context, revoked [8]byte) {
				t.Helper()
				if ok, err := r.onAuthResetAuthorizations(ctx); err != nil || !ok {
					t.Fatalf("auth.resetAuthorizations ok=%v err=%v", ok, err)
				}
			},
		},
		{
			name: "account.deleteAccount",
			run: func(t *testing.T, r *Router, ctx context.Context, revoked [8]byte) {
				t.Helper()
				deletionSvc := r.deps.Account.(*authRevocationMatrixAccountService)
				deletionSvc.outcome = domain.AccountDeleteOutcome{
					Kind: domain.AccountDeleteImmediate,
					Deletion: domain.AccountDeletionResult{
						Changed: true,
						RevokedAuthorizations: []domain.Authorization{
							{AuthKeyID: revoked, UserID: 1000000100},
						},
					},
				}
				if ok, err := r.onAccountDeleteAccount(ctx, &tg.AccountDeleteAccountRequest{Reason: "matrix"}); err != nil || !ok {
					t.Fatalf("account.deleteAccount ok=%v err=%v", ok, err)
				}
			},
		},
		{
			name: "admin hook",
			run: func(t *testing.T, r *Router, ctx context.Context, revoked [8]byte) {
				t.Helper()
				if err := r.RevokeAuthorizationAuthKey(ctx, revoked, 1000000100); err != nil {
					t.Fatalf("admin revoke hook: %v", err)
				}
			},
		},
		{
			name: "bot revoke hook",
			run: func(t *testing.T, r *Router, ctx context.Context, revoked [8]byte) {
				t.Helper()
				if err := r.RevokeBotSessions(ctx, 1000000101); err != nil {
					t.Fatalf("bot revoke hook: %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			current := [8]byte{0x91, 0x91, 0x91, 0x91, 0x91, 0x91, 0x91, 0x91}
			revoked := [8]byte{0x92, 0x92, 0x92, 0x92, 0x92, 0x92, 0x92, 0x92}
			broker := &captureAuthInvalidationBroker{}
			sessions := &authRevocationMatrixSessions{}
			authSvc := &authRevocationMatrixAuthService{revoked: revoked}
			accountSvc := &authRevocationMatrixAccountService{}
			r := New(Config{}, Deps{
				Auth:              authSvc,
				Account:           accountSvc,
				AuthInvalidations: broker,
				Sessions:          sessions,
			}, zaptest.NewLogger(t), clock.System)
			ctx := WithUserID(WithAuthKeyID(WithSessionID(context.Background(), 77), current), 1000000100)

			tt.run(t, r, ctx, revoked)

			if !authRevocationEventsContain(broker.events, revoked) {
				t.Fatalf("auth invalidation events = %+v, want revoked %x", broker.events, revoked)
			}
			if !sessions.wasClosed(revoked) {
				t.Fatalf("closed auth keys = %+v, want %x", sessions.closed, revoked)
			}
		})
	}
}

type captureAuthInvalidationBroker struct {
	events []store.AuthInvalidationEvent
}

func (b *captureAuthInvalidationBroker) PublishAuthInvalidation(_ context.Context, event store.AuthInvalidationEvent) error {
	b.events = append(b.events, event)
	return nil
}

func (b *captureAuthInvalidationBroker) SubscribeAuthInvalidations(context.Context, func(context.Context, store.AuthInvalidationEvent)) error {
	return nil
}

func authRevocationEventsContain(events []store.AuthInvalidationEvent, want [8]byte) bool {
	for _, event := range events {
		for _, id := range event.AuthKeyIDs {
			if id == want {
				return true
			}
		}
	}
	return false
}

type authRevocationMatrixAuthService struct {
	captureAuthService
	revoked [8]byte
}

func (s *authRevocationMatrixAuthService) ResetAuthorization(context.Context, int64, int64) (domain.Authorization, bool, error) {
	return domain.Authorization{AuthKeyID: s.revoked, UserID: 1000000100}, true, nil
}

func (s *authRevocationMatrixAuthService) ResetAuthorizations(context.Context, int64, [8]byte) ([]domain.Authorization, error) {
	return []domain.Authorization{{AuthKeyID: s.revoked, UserID: 1000000100}}, nil
}

type authRevocationMatrixAccountService struct {
	rpcDeletionAccountService
	outcome domain.AccountDeleteOutcome
}

func (s *authRevocationMatrixAccountService) DeleteAccount(context.Context, int64, [8]byte, string, *domain.PasswordCheck, time.Time) (domain.AccountDeleteOutcome, error) {
	return s.outcome, nil
}

type authRevocationMatrixSessions struct {
	captureSessions
	closed [][8]byte
}

func (s *authRevocationMatrixSessions) CloseSessionsForBusinessAuthKey(id [8]byte) int {
	s.closed = append(s.closed, id)
	return 1
}

func (s *authRevocationMatrixSessions) wasClosed(id [8]byte) bool {
	for _, closed := range s.closed {
		if closed == id {
			return true
		}
	}
	return false
}

type boundedFailureSessions struct {
	authRevocationMatrixSessions
	boundedCalls int
	boundedErr   error
}

func (s *boundedFailureSessions) CloseSessionsForBusinessAuthKeyBounded(context.Context, [8]byte) (int, error) {
	s.boundedCalls++
	return 0, s.boundedErr
}

func TestAdminAuthorizationRevokeReportsBoundedEdgeCloseFailure(t *testing.T) {
	wantErr := errors.New("edge close acknowledgement failed")
	sessions := &boundedFailureSessions{boundedErr: wantErr}
	r := New(Config{}, Deps{Sessions: sessions}, zaptest.NewLogger(t), clock.System)

	err := r.RevokeAuthorizationAuthKey(context.Background(), [8]byte{0x91}, 1000000100)
	if !errors.Is(err, wantErr) {
		t.Fatalf("admin revoke err=%v, want bounded Edge failure", err)
	}
	if sessions.boundedCalls != 1 || len(sessions.closed) != 0 {
		t.Fatalf("bounded calls=%d legacy closes=%v, want bounded path only", sessions.boundedCalls, sessions.closed)
	}
}
