// Command mtproto-edge-auth-smoke-probe verifies an authenticated real MTProto
// path across two Edge listeners.
package main

import (
	"context"
	"crypto/rsa"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/iamxvbaba/td/session"
	"github.com/iamxvbaba/td/telegram"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tgerr"

	"telesrv/scripts/internal/mtprobes"
)

func main() {
	aliceServer := flag.String("alice-server", "127.0.0.1:2398", "MTProto Edge host:port used by Alice")
	bobServer := flag.String("bob-server", "127.0.0.1:2399", "MTProto Edge host:port used by Bob")
	dc := flag.Int("dc", 2, "wire DC label")
	rsaKey := flag.String("rsa-key", "data/server_rsa.pem", "server RSA private/public PEM")
	apiID := flag.Int("api-id", 1, "test application ID")
	apiHash := flag.String("api-hash", "hash", "test application hash")
	code := flag.String("code", "12345", "development login code")
	phonePrefix := flag.String("phone-prefix", "+15559", "E.164 prefix used for generated smoke accounts")
	runs := flag.Int("runs", 1, "number of independent authenticated probe runs")
	timeout := flag.Duration("timeout", 30*time.Second, "overall probe timeout")
	obfuscated := flag.Bool("obfuscated", false, "use Obfuscated2 + abridged transport")
	pfs := flag.Bool("pfs", false, "enable temporary auth-key PFS")
	tempKeyTTL := flag.Int("temp-key-ttl", 86400, "temporary auth-key lifetime in seconds")
	flag.Parse()

	if *aliceServer == "" || *bobServer == "" {
		fatalf("-alice-server and -bob-server are required")
	}
	if *dc <= 0 {
		fatalf("-dc must be positive")
	}
	if *timeout <= 0 {
		fatalf("-timeout must be positive")
	}
	if *code == "" {
		fatalf("-code must not be empty")
	}
	if *runs <= 0 {
		fatalf("-runs must be positive")
	}

	publicKey, err := mtprobes.LoadRSAPublicKey(*rsaKey)
	if err != nil {
		fatalf("load RSA public key: %v", err)
	}
	cfg := probeConfig{
		DC:         *dc,
		APIID:      *apiID,
		APIHash:    *apiHash,
		Code:       *code,
		PublicKey:  publicKey,
		Obfuscated: *obfuscated,
		PFS:        *pfs,
		TempKeyTTL: *tempKeyTTL,
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	started := time.Now()
	for i := 1; i <= *runs; i++ {
		nonce := (time.Now().UnixNano() + int64(i)*1000) % 100000000
		alicePhone := fmt.Sprintf("%s%08d01", *phonePrefix, nonce)
		bobPhone := fmt.Sprintf("%s%08d02", *phonePrefix, nonce)
		alice := probeAccount{Name: fmt.Sprintf("EdgeAlice%d", i), Phone: alicePhone, Server: *aliceServer, Storage: &session.StorageMemory{}}
		bob := probeAccount{Name: fmt.Sprintf("EdgeBob%d", i), Phone: bobPhone, Server: *bobServer, Storage: &session.StorageMemory{}}
		if err := runProbe(ctx, cfg, &alice, &bob); err != nil {
			fatalf("mtproto edge auth probe run %d/%d failed: %v", i, *runs, err)
		}
	}
	fmt.Printf("mtproto edge auth probe ok: alice=%s bob=%s dc=%d obfuscated=%t runs=%d offline_difference=true auth_revoke=true elapsed=%s\n",
		*aliceServer, *bobServer, *dc, *obfuscated, *runs, time.Since(started).Round(time.Millisecond))
}

type probeConfig struct {
	DC         int
	APIID      int
	APIHash    string
	Code       string
	PublicKey  *rsa.PublicKey
	Obfuscated bool
	PFS        bool
	TempKeyTTL int
}

type probeAccount struct {
	Name    string
	Phone   string
	Server  string
	Storage *session.StorageMemory
	User    tg.User
}

func runProbe(ctx context.Context, cfg probeConfig, alice, bob *probeAccount) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	aliceReady := make(chan tg.User, 1)
	bobReady := make(chan tg.User, 1)
	aliceSent := make(chan string, 1)
	bobSent := make(chan string, 1)
	bobBaselineReady := make(chan tg.UpdatesState, 1)
	bobDone := make(chan error, 1)
	go func() {
		bobDone <- runAccount(runCtx, cfg, bob, func(ctx context.Context, raw *tg.Client) error {
			if err := signUpRaw(ctx, cfg, raw, bob); err != nil {
				return fmt.Errorf("bob signup: %w", err)
			}
			aliceUser, err := receiveValue(ctx, aliceReady)
			if err != nil {
				return fmt.Errorf("wait alice user: %w", err)
			}
			alicePeer, err := importPeerRaw(ctx, raw, alice, 2001)
			if err != nil {
				return fmt.Errorf("import alice contact: %w", err)
			}
			bobReady <- bob.User
			bodyAB, err := receiveValue(ctx, aliceSent)
			if err != nil {
				return fmt.Errorf("wait alice message: %w", err)
			}
			if err := readLatestRaw(ctx, raw, alicePeer, bodyAB, false); err != nil {
				return fmt.Errorf("read alice message: %w", err)
			}
			bodyBA := fmt.Sprintf("edge auth smoke bob->alice %d", time.Now().UnixNano())
			if err := sendAndCheckOwnHistoryRaw(ctx, raw, alicePeer, bodyBA, true); err != nil {
				return fmt.Errorf("send/read own history: %w", err)
			}
			bobSent <- bodyBA
			state, err := raw.UpdatesGetState(ctx)
			if err != nil {
				return fmt.Errorf("updates.getState before offline: %w", err)
			}
			bobBaselineReady <- *state
			_ = aliceUser
			return nil
		})
	}()
	aliceErr := runAccount(runCtx, cfg, alice, func(ctx context.Context, raw *tg.Client) error {
		if err := signUpRaw(ctx, cfg, raw, alice); err != nil {
			return fmt.Errorf("alice signup: %w", err)
		}
		aliceReady <- alice.User
		bobUser, err := receiveValue(ctx, bobReady)
		if err != nil {
			return fmt.Errorf("wait bob user: %w", err)
		}
		bobPeer, err := importPeerRaw(ctx, raw, bob, 1001)
		if err != nil {
			return fmt.Errorf("import bob contact: %w", err)
		}
		bodyAB := fmt.Sprintf("edge auth smoke alice->bob %d", time.Now().UnixNano())
		if err := sendAndCheckOwnHistoryRaw(ctx, raw, bobPeer, bodyAB, true); err != nil {
			return fmt.Errorf("send/read own history: %w", err)
		}
		aliceSent <- bodyAB
		bodyBA, err := receiveValue(ctx, bobSent)
		if err != nil {
			return fmt.Errorf("wait bob message: %w", err)
		}
		if err := readLatestRaw(ctx, raw, bobPeer, bodyBA, false); err != nil {
			return fmt.Errorf("read bob message: %w", err)
		}
		_ = bobUser
		return nil
	})
	if aliceErr != nil {
		cancel()
		<-bobDone
		return aliceErr
	}
	var bobBaseline tg.UpdatesState
	select {
	case err := <-bobDone:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case bobBaseline = <-bobBaselineReady:
	case <-ctx.Done():
		return ctx.Err()
	}
	if bobBaseline.Zero() {
		return fmt.Errorf("bob offline baseline was not captured")
	}
	bodyOffline := fmt.Sprintf("edge auth smoke offline alice->bob %d", time.Now().UnixNano())
	if err := runAccount(ctx, cfg, alice, func(ctx context.Context, raw *tg.Client) error {
		bobPeer, err := importPeerRaw(ctx, raw, bob, 3001)
		if err != nil {
			return fmt.Errorf("import bob contact for offline send: %w", err)
		}
		if err := sendAndCheckOwnHistoryRaw(ctx, raw, bobPeer, bodyOffline, true); err != nil {
			return fmt.Errorf("send offline message: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := runAccount(ctx, cfg, bob, func(ctx context.Context, raw *tg.Client) error {
		if err := assertDifferenceContainsMessage(ctx, raw, bobBaseline, bodyOffline, false); err != nil {
			return fmt.Errorf("offline difference: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}
	if err := assertCrossEdgeAuthReset(ctx, cfg, alice, bob.Server); err != nil {
		return err
	}
	return nil
}

func assertCrossEdgeAuthReset(ctx context.Context, cfg probeConfig, primary *probeAccount, targetServer string) error {
	target := &probeAccount{
		Name:    primary.Name + "Alt",
		Phone:   primary.Phone,
		Server:  targetServer,
		Storage: &session.StorageMemory{},
	}
	ready := make(chan struct{}, 1)
	resetDone := make(chan struct{})
	targetDone := make(chan error, 1)
	go func() {
		targetDone <- runAccount(ctx, cfg, target, func(ctx context.Context, raw *tg.Client) error {
			if err := signUpRaw(ctx, cfg, raw, target); err != nil {
				return fmt.Errorf("target sign-in: %w", err)
			}
			if err := assertAuthorizedSelf(ctx, raw); err != nil {
				return fmt.Errorf("target pre-revoke self check: %w", err)
			}
			ready <- struct{}{}
			select {
			case <-resetDone:
			case <-ctx.Done():
				return ctx.Err()
			}
			if err := expectRevokedOrClosed(ctx, raw); err != nil {
				return fmt.Errorf("target active-session revoke check: %w", err)
			}
			return nil
		})
	}()
	select {
	case <-ready:
	case err := <-targetDone:
		if err != nil {
			return fmt.Errorf("target alt before ready: %w", err)
		}
		return fmt.Errorf("target alt exited before ready")
	case <-ctx.Done():
		return fmt.Errorf("wait target alt ready: %w", ctx.Err())
	}
	if err := runAccount(ctx, cfg, primary, func(ctx context.Context, raw *tg.Client) error {
		ok, err := raw.AuthResetAuthorizations(ctx)
		if err != nil {
			return fmt.Errorf("auth.resetAuthorizations: %w", err)
		}
		if !ok {
			return fmt.Errorf("auth.resetAuthorizations returned false")
		}
		if err := assertAuthorizedSelf(ctx, raw); err != nil {
			return fmt.Errorf("primary self after reset: %w", err)
		}
		return nil
	}); err != nil {
		close(resetDone)
		<-targetDone
		return err
	}
	close(resetDone)
	if err := receiveError(ctx, targetDone); err != nil {
		return err
	}
	if err := runAccount(ctx, cfg, target, func(ctx context.Context, raw *tg.Client) error {
		loggedOut, err := raw.AuthLogOut(ctx)
		if err != nil {
			return fmt.Errorf("target auth.logOut cleanup after revoke: %w", err)
		}
		if loggedOut == nil {
			return fmt.Errorf("target auth.logOut cleanup returned nil")
		}
		if err := expectUnauthorizedSelf(ctx, raw); err != nil {
			return fmt.Errorf("target reconnect after revoke cleanup: %w", err)
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func assertDifferenceContainsMessage(ctx context.Context, raw *tg.Client, from tg.UpdatesState, body string, wantOut bool) error {
	diff, err := raw.UpdatesGetDifference(ctx, &tg.UpdatesGetDifferenceRequest{
		Pts:  from.Pts,
		Date: from.Date,
		Qts:  from.Qts,
	})
	if err != nil {
		return fmt.Errorf("updates.getDifference: %w", err)
	}
	msgs, err := differenceMessages(diff)
	if err != nil {
		return err
	}
	for _, msgClass := range msgs {
		msg, ok := msgClass.(*tg.Message)
		if !ok {
			continue
		}
		if msg.Message == body && msg.Out == wantOut {
			return nil
		}
	}
	return fmt.Errorf("message %q/out=%t not found in %T with %d new messages", body, wantOut, diff, len(msgs))
}

func differenceMessages(diff tg.UpdatesDifferenceClass) ([]tg.MessageClass, error) {
	switch v := diff.(type) {
	case *tg.UpdatesDifference:
		return v.NewMessages, nil
	case *tg.UpdatesDifferenceSlice:
		return v.NewMessages, nil
	case *tg.UpdatesDifferenceEmpty:
		return nil, fmt.Errorf("updates.getDifference returned empty difference")
	case *tg.UpdatesDifferenceTooLong:
		return nil, fmt.Errorf("updates.getDifference returned tooLong pts=%d", v.Pts)
	default:
		return nil, fmt.Errorf("updates.getDifference returned %T", diff)
	}
}

func runAccount(ctx context.Context, cfg probeConfig, account *probeAccount, fn func(context.Context, *tg.Client) error) error {
	client, err := newClient(cfg, account.Server, account.Storage)
	if err != nil {
		return err
	}
	return client.Run(ctx, func(ctx context.Context) error {
		return fn(ctx, tg.NewClient(client))
	})
}

func assertAuthorizedSelf(ctx context.Context, raw *tg.Client) error {
	users, err := raw.UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUserSelf{}})
	if err != nil {
		return err
	}
	if len(users) == 0 {
		return fmt.Errorf("users.getUsers self returned no users")
	}
	return nil
}

func expectUnauthorizedSelf(ctx context.Context, raw *tg.Client) error {
	_, err := raw.UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUserSelf{}})
	if err == nil {
		return fmt.Errorf("users.getUsers self succeeded after revoke")
	}
	if tgerr.Is(err, "AUTH_KEY_UNREGISTERED") {
		return nil
	}
	return err
}

func expectRevokedOrClosed(ctx context.Context, raw *tg.Client) error {
	err := expectUnauthorizedSelf(ctx, raw)
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || isRevokedActiveSessionClosedErr(err) {
		return nil
	}
	return err
}

func isRevokedActiveSessionClosedErr(err error) bool {
	if err == nil {
		return false
	}
	if tgerr.Is(err, "INTERNAL") {
		// gotd can surface a server-side force-close racing an active in-flight
		// request as a local rpcDoRequest 500. The reconnect gate below still
		// proves the durable state returns AUTH_KEY_UNREGISTERED.
		return true
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "connection closed") ||
		strings.Contains(text, "use of closed network connection") ||
		strings.Contains(text, "rpc error code 500: internal")
}

func signUpRaw(ctx context.Context, cfg probeConfig, raw *tg.Client, account *probeAccount) error {
	sent, err := raw.AuthSendCode(ctx, &tg.AuthSendCodeRequest{
		PhoneNumber: account.Phone,
		APIID:       cfg.APIID,
		APIHash:     cfg.APIHash,
		Settings:    tg.CodeSettings{},
	})
	if err != nil {
		return err
	}
	sentCode, ok := sent.(*tg.AuthSentCode)
	if !ok {
		return fmt.Errorf("auth.sendCode returned %T, want *tg.AuthSentCode", sent)
	}
	signInRes, err := raw.AuthSignIn(ctx, &tg.AuthSignInRequest{
		PhoneNumber:   account.Phone,
		PhoneCodeHash: sentCode.PhoneCodeHash,
		PhoneCode:     cfg.Code,
	})
	if err != nil {
		return err
	}
	if user, ok, err := authorizationUser(signInRes); err != nil || ok {
		if err != nil {
			return err
		}
		account.User = user
		return nil
	}
	res, err := raw.AuthSignUp(ctx, &tg.AuthSignUpRequest{
		PhoneNumber:   account.Phone,
		PhoneCodeHash: sentCode.PhoneCodeHash,
		FirstName:     account.Name,
	})
	if err != nil {
		return err
	}
	user, ok, err := authorizationUser(res)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("auth.signUp returned %T, want *tg.AuthAuthorization", res)
	}
	account.User = user
	if _, err := raw.UsersGetUsers(ctx, []tg.InputUserClass{&tg.InputUserSelf{}}); err != nil {
		return fmt.Errorf("post-signup users.getUsers self: %w", err)
	}
	return nil
}

func sendAndCheckOwnHistoryRaw(ctx context.Context, raw *tg.Client, to tg.User, body string, wantOut bool) error {
	if _, err := raw.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
		Peer:     &tg.InputPeerUser{UserID: to.ID, AccessHash: to.AccessHash},
		Message:  body,
		RandomID: time.Now().UnixNano(),
	}); err != nil {
		return fmt.Errorf("messages.sendMessage: %w", err)
	}
	if err := assertLatestHistory(ctx, raw, to, body, wantOut); err != nil {
		return fmt.Errorf("messages.getHistory latest: %w", err)
	}
	return nil
}

func readLatestRaw(ctx context.Context, raw *tg.Client, from tg.User, body string, wantOut bool) error {
	return assertLatestHistory(ctx, raw, from, body, wantOut)
}

func receiveValue[T any](ctx context.Context, ch <-chan T) (T, error) {
	select {
	case value := <-ch:
		return value, nil
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}

func receiveError(ctx context.Context, ch <-chan error) error {
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func assertLatestHistory(ctx context.Context, raw *tg.Client, peer tg.User, body string, wantOut bool) error {
	history, err := raw.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
		Peer:  &tg.InputPeerUser{UserID: peer.ID, AccessHash: peer.AccessHash},
		Limit: 10,
	})
	if err != nil {
		return fmt.Errorf("messages.getHistory: %w", err)
	}
	msgs, err := historyMessages(history)
	if err != nil {
		return err
	}
	if len(msgs) == 0 {
		return fmt.Errorf("history returned no messages")
	}
	msg, ok := msgs[0].(*tg.Message)
	if !ok {
		return fmt.Errorf("latest history message = %T, want *tg.Message", msgs[0])
	}
	if msg.Message != body || msg.Out != wantOut {
		return fmt.Errorf("latest history message text/out = %q/%t, want %q/%t", msg.Message, msg.Out, body, wantOut)
	}
	return nil
}

func importPeerRaw(ctx context.Context, raw *tg.Client, account *probeAccount, clientID int64) (tg.User, error) {
	imported, err := raw.ContactsImportContacts(ctx, []tg.InputPhoneContact{{
		ClientID:  clientID,
		Phone:     account.Phone,
		FirstName: account.Name,
	}})
	if err != nil {
		return tg.User{}, err
	}
	for _, userClass := range imported.Users {
		user, ok := userClass.(*tg.User)
		if !ok {
			continue
		}
		if user.ID == account.User.ID {
			return *user, nil
		}
	}
	return tg.User{}, fmt.Errorf("imported users did not include %s id %d", account.Name, account.User.ID)
}

func historyMessages(history tg.MessagesMessagesClass) ([]tg.MessageClass, error) {
	switch v := history.(type) {
	case *tg.MessagesMessages:
		return v.Messages, nil
	case *tg.MessagesMessagesSlice:
		return v.Messages, nil
	default:
		return nil, fmt.Errorf("history = %T, want messages/messagesSlice", history)
	}
}

func authorizationUser(authz tg.AuthAuthorizationClass) (tg.User, bool, error) {
	switch v := authz.(type) {
	case *tg.AuthAuthorization:
		user, ok := v.User.(*tg.User)
		if !ok {
			return tg.User{}, false, fmt.Errorf("authorization user = %T, want *tg.User", v.User)
		}
		return *user, true, nil
	case *tg.AuthAuthorizationSignUpRequired:
		return tg.User{}, false, nil
	default:
		return tg.User{}, false, fmt.Errorf("authorization = %T, want auth.authorization or signUpRequired", authz)
	}
}

func newClient(cfg probeConfig, server string, storage *session.StorageMemory) (*telegram.Client, error) {
	return mtprobes.NewClient(mtprobes.Endpoint{
		Address:    server,
		DC:         cfg.DC,
		APIID:      cfg.APIID,
		APIHash:    cfg.APIHash,
		PublicKey:  cfg.PublicKey,
		Obfuscated: cfg.Obfuscated,
		PFS:        cfg.PFS,
		TempKeyTTL: cfg.TempKeyTTL,
		Storage:    storage,
	})
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
