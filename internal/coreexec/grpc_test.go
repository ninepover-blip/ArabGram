package coreexec

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/clock"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tgerr"
	"github.com/iamxvbaba/td/tlprofile"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"
	"golang.org/x/net/dns/dnsmessage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	appaccount "telesrv/internal/app/account"
	"telesrv/internal/app/auth"
	"telesrv/internal/app/users"
	"telesrv/internal/coreexec/coreexecpb"
	"telesrv/internal/domain"
	"telesrv/internal/edgecontrol"
	"telesrv/internal/mtprotoedge"
	"telesrv/internal/observability/dbtrace"
	"telesrv/internal/postresponse"
	"telesrv/internal/rpc"
	"telesrv/internal/store"
	"telesrv/internal/store/memory"
)

var grpcTestPhoneSequence atomic.Uint32

type floodWaitDispatchHandler struct {
	Handler
}

type databaseTraceDispatchHandler struct {
	Handler
}

func (h *databaseTraceDispatchHandler) DispatchAdmitted(
	ctx context.Context,
	authKeyID [8]byte,
	sessionID int64,
	msgID int64,
	admissionSeq uint64,
	request tlprofile.Admission,
) (tlprofile.Result, string, error) {
	stats, ok := dbtrace.FromContext(ctx)
	if !ok {
		return nil, "", errors.New("CoreExec database trace missing")
	}
	stats.Add(2*time.Millisecond, nil)
	stats.Add(3*time.Millisecond, nil)
	return h.Handler.DispatchAdmitted(ctx, authKeyID, sessionID, msgID, admissionSeq, request)
}

func (h *floodWaitDispatchHandler) DispatchAdmitted(
	context.Context,
	[8]byte,
	int64,
	int64,
	uint64,
	tlprofile.Admission,
) (tlprofile.Result, string, error) {
	return nil, "messages.sendMessage", tgerr.New(420, "FLOOD_WAIT_51")
}

func nextGRPCTestPhone() string {
	return fmt.Sprintf("+1555%07d", grpcTestPhoneSequence.Add(1))
}

func TestGRPCRemoteDispatchRoundTripExactLayer(t *testing.T) {
	coreRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	remote, cleanup := newBufGRPCRemote(t, coreRouter, edgeRouter, "secret", "secret")
	defer cleanup()

	if err := remote.Check(context.Background()); err != nil {
		t.Fatalf("Check: %v", err)
	}
	var body bin.Buffer
	if err := tlprofile.EncodeObject(tlprofile.Profile225, &tg.HelpGetConfigRequest{}, &body); err != nil {
		t.Fatal(err)
	}
	admitted, err := remote.AdmitLayer(tlprofile.Profile225, &body, tlprofile.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if body.Len() != 0 {
		t.Fatalf("admission left %d bytes", body.Len())
	}
	ctx := remote.WithLayerRPCProfileEvidenceFresh(context.Background(), false)
	result, method, err := remote.DispatchAdmitted(ctx, [8]byte{1, 2, 3}, 10, 20, 30, admitted)
	if err != nil {
		t.Fatal(err)
	}
	if method != "help.getConfig" {
		t.Fatalf("method = %q", method)
	}
	if result.Prepared().Identity() != admitted.Prepared().Identity() {
		t.Fatal("remote result is not bound to the original admission")
	}
	var exact bin.Buffer
	if err := result.Encode(&exact); err != nil {
		t.Fatal(err)
	}
	decodedObject, err := tlprofile.DecodeObject(tlprofile.Profile225, &exact, tlprofile.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	decoded, ok := decodedObject.(*tg.Config)
	if !ok {
		t.Fatalf("decoded result = %T", decodedObject)
	}
	if exact.Len() != 0 || decoded.ThisDC != 2 {
		t.Fatalf("decoded config = dc:%d remaining:%d", decoded.ThisDC, exact.Len())
	}
}

func TestGRPCRemotePreservesFloodWaitCodeAndMessage(t *testing.T) {
	coreRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	remote, cleanup := newBufGRPCRemote(t, &floodWaitDispatchHandler{Handler: coreRouter}, edgeRouter, "secret", "secret")
	defer cleanup()

	admitted := admitHelpGetConfigForGRPCTest(t, remote)
	result, method, err := remote.DispatchAdmitted(context.Background(), [8]byte{1}, 10, 20, 30, admitted)
	if result != nil || method != "messages.sendMessage" {
		t.Fatalf("dispatch result=%T method=%q, want nil/messages.sendMessage", result, method)
	}
	var rpcErr *tgerr.Error
	if !errors.As(err, &rpcErr) {
		t.Fatalf("dispatch error=%T %v, want *tgerr.Error", err, err)
	}
	if rpcErr.Code != 420 || rpcErr.Message != "FLOOD_WAIT_51" {
		t.Fatalf("dispatch rpc error=(%d,%q), want (420,FLOOD_WAIT_51)", rpcErr.Code, rpcErr.Message)
	}
}

func TestGRPCRemoteDispatchCarriesIdentityHint(t *testing.T) {
	coreRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	coreCapture := &captureIdentityHintHandler{Handler: coreRouter}
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	remote, cleanup := newBufGRPCRemote(t, coreCapture, edgeRouter, "secret", "secret")
	defer cleanup()

	hint := mtprotoedge.LayerRPCIdentityHint{
		RawAuthKeyID:                [8]byte{1, 2, 3},
		SessionID:                   10,
		BusinessAuthKeyID:           [8]byte{9, 8, 7, 6, 5, 4, 3, 2},
		BusinessAuthKeyResolved:     true,
		UserID:                      1000000001,
		UserIDResolved:              true,
		RawAuthKeyExpiresAt:         0,
		RawAuthKeyExpiresAtResolved: true,
		SessionUpdatesReady:         true,
	}
	ctx := remote.WithLayerRPCIdentityHint(context.Background(), hint)
	admitted := admitHelpGetConfigForGRPCTest(t, remote)
	result, method, err := remote.DispatchAdmitted(ctx, hint.RawAuthKeyID, hint.SessionID, 20, 30, admitted)
	if err != nil {
		t.Fatal(err)
	}
	if method != "help.getConfig" || result == nil {
		t.Fatalf("dispatch result = method:%q result:%T", method, result)
	}
	got, ok := coreCapture.identityHint()
	if !ok {
		t.Fatal("core handler did not receive identity hint")
	}
	if got != hint {
		t.Fatalf("identity hint = %+v, want %+v", got, hint)
	}
}

func TestGRPCRemoteAuthenticatedDispatchSeesSignUpBinding(t *testing.T) {
	userStore := memory.NewUserStore()
	authzStore := memory.NewAuthorizationStore()
	deliveryOutbox := memory.NewDeliveryOutboxStore()
	authzStore.AttachDeliveryOutbox(deliveryOutbox)
	authKeyStore := memory.NewAuthKeyStore()
	coreRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{
		Auth:           auth.NewService(userStore, authzStore, memory.NewCodeStore(), authKeyStore, memory.NewTempAuthKeyBindingStore(authKeyStore), "12345"),
		Users:          users.NewService(userStore),
		DeliveryOutbox: deliveryOutbox,
	}, zaptest.NewLogger(t), clock.System)
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	remote, cleanup := newBufGRPCRemote(t, coreRouter, edgeRouter, "secret", "secret")
	defer cleanup()

	ctx := context.Background()
	authKeyID := [8]byte{9, 8, 7, 6, 5, 4, 3, 2}
	const sessionID int64 = 77
	if err := authKeyStore.Save(ctx, store.AuthKeyData{ID: authKeyID, CreatedAt: time.Now().Unix()}); err != nil {
		t.Fatalf("seed auth key: %v", err)
	}
	phone := nextGRPCTestPhone()
	sentValue := decodeBoxedResult(t, dispatchRemoteResult(t, ctx, remote, authKeyID, sessionID, 100, &tg.AuthSendCodeRequest{
		PhoneNumber: phone,
		APIID:       1,
		APIHash:     "hash",
		Settings:    tg.CodeSettings{},
	}))
	sent, ok := sentValue.(*tg.AuthSentCode)
	if !ok {
		t.Fatalf("auth.sendCode result = %T, want *tg.AuthSentCode", sentValue)
	}
	signInValue := decodeBoxedResult(t, dispatchRemoteResult(t, ctx, remote, authKeyID, sessionID, 101, &tg.AuthSignInRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: sent.PhoneCodeHash,
		PhoneCode:     "12345",
	}))
	if _, ok := signInValue.(*tg.AuthAuthorizationSignUpRequired); !ok {
		t.Fatalf("auth.signIn result = %T, want *tg.AuthAuthorizationSignUpRequired", signInValue)
	}
	signUpValue := decodeBoxedResult(t, dispatchRemoteResult(t, ctx, remote, authKeyID, sessionID, 102, &tg.AuthSignUpRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: sent.PhoneCodeHash,
		FirstName:     "CoreExec",
	}))
	authz, ok := signUpValue.(*tg.AuthAuthorization)
	if !ok {
		t.Fatalf("auth.signUp result = %T, want *tg.AuthAuthorization", signUpValue)
	}
	self, ok := authz.User.(*tg.User)
	if !ok || self.ID == 0 {
		t.Fatalf("auth.signUp user = %T %+v, want non-zero *tg.User", authz.User, authz.User)
	}
	usersResult := dispatchRemoteResult(t, ctx, remote, authKeyID, sessionID, 103, &tg.UsersGetUsersRequest{
		ID: []tg.InputUserClass{&tg.InputUserSelf{}},
	})
	var usersWire bin.Buffer
	if err := usersResult.Encode(&usersWire); err != nil {
		t.Fatalf("encode users.getUsers result: %v", err)
	}
	if usersWire.Len() == 0 {
		t.Fatal("users.getUsers returned empty result wire")
	}
}

func TestGRPCRemoteRequiresBearerToken(t *testing.T) {
	coreRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	remote, cleanup := newBufGRPCRemote(t, coreRouter, edgeRouter, "secret", "wrong")
	defer cleanup()

	if err := remote.Check(context.Background()); err == nil {
		t.Fatal("Check with invalid token unexpectedly succeeded")
	}
	var body bin.Buffer
	if err := tlprofile.EncodeObject(tlprofile.Profile225, &tg.HelpGetConfigRequest{}, &body); err != nil {
		t.Fatal(err)
	}
	admitted, err := remote.AdmitLayer(tlprofile.Profile225, &body, tlprofile.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := remote.DispatchAdmitted(context.Background(), [8]byte{1}, 10, 20, 30, admitted); err == nil {
		t.Fatal("dispatch with wrong token unexpectedly succeeded")
	}
}

func TestGRPCRemoteRequiresConfiguredBearerToken(t *testing.T) {
	coreRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	ctx := context.Background()
	if _, err := StartGRPC(ctx, GRPCServerConfig{Addr: "127.0.0.1:0", Handler: coreRouter}); !errors.Is(err, ErrGRPCTokenMissing) {
		t.Fatalf("StartGRPC missing token err = %v, want ErrGRPCTokenMissing", err)
	}
	if _, _, err := DialGRPCRemote(ctx, coreRouter, GRPCClientConfig{Targets: []string{"127.0.0.1:2500"}}); !errors.Is(err, ErrGRPCTokenMissing) {
		t.Fatalf("DialGRPCRemote missing token err = %v, want ErrGRPCTokenMissing", err)
	}
}

func TestGRPCRemoteHasNoLocalDispatchFallback(t *testing.T) {
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	remote := NewGRPCRemote(edgeRouter, nil, nil)
	admitted, err := tryAdmitHelpGetConfigForGRPCTest(remote)
	if err != nil {
		t.Fatalf("admit through edge codec: %v", err)
	}
	if _, _, err := remote.DispatchAdmitted(context.Background(), [8]byte{1}, 10, 20, 30, admitted); !errors.Is(err, ErrRemoteRPCUnavailable) {
		t.Fatalf("dispatch without coreexec client err = %v, want ErrRemoteRPCUnavailable", err)
	}
}

func TestGRPCRemoteDiscardAdmittedReleasesCapturedWire(t *testing.T) {
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	remote := NewGRPCRemote(edgeRouter, nil, nil)
	admitted, err := tryAdmitHelpGetConfigForGRPCTest(remote)
	if err != nil {
		t.Fatalf("admit through edge codec: %v", err)
	}
	if remote.pendingCount != 1 {
		t.Fatalf("pending admissions = %d, want 1", remote.pendingCount)
	}
	if !remote.DiscardAdmitted(admitted.Prepared().Identity()) {
		t.Fatal("DiscardAdmitted did not release captured admission")
	}
	if remote.pendingCount != 0 {
		t.Fatalf("pending admissions after discard = %d, want 0", remote.pendingCount)
	}
	if _, ok := remote.peek(admitted.Prepared().Identity()); ok {
		t.Fatal("discarded admission retained captured wire")
	}
}

func TestGRPCRemoteTLSMutualAuthRoundTrip(t *testing.T) {
	files := writeCoreExecTLSTestFiles(t)
	coreRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	addr := unusedTCPAddr(t)

	serverCtx, stopServer := context.WithCancel(context.Background())
	defer stopServer()
	srv, err := StartGRPC(serverCtx, GRPCServerConfig{
		Addr:            addr,
		Token:           "secret",
		TLSCertFile:     files.serverCert,
		TLSKeyFile:      files.serverKey,
		TLSClientCAFile: files.caCert,
		Handler:         coreRouter,
		Logger:          zaptest.NewLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	remote, conn, err := DialGRPCRemote(ctx, edgeRouter, GRPCClientConfig{
		Targets:       []string{addr},
		Token:         "secret",
		TLSCAFile:     files.caCert,
		TLSServerName: "core.test",
		TLSCertFile:   files.clientCert,
		TLSKeyFile:    files.clientKey,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := dispatchHelpGetConfig(t, ctx, remote, 91); err != nil {
		t.Fatal(err)
	}
}

func TestGRPCRemotePropagatesProvidedRequestID(t *testing.T) {
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	core := &captureRequestIDServer{}
	remote, cleanup := newBufGRPCRemoteWithServer(t, core, edgeRouter, "secret", "secret")
	defer cleanup()

	ctx := WithGRPCRequestID(context.Background(), "edge-call-123")
	if err := remote.Check(ctx); err != nil {
		t.Fatal(err)
	}
	if got := core.requestID(); got != "edge-call-123" {
		t.Fatalf("request id = %q, want edge-call-123", got)
	}
}

func TestGRPCRemoteGeneratesRequestIDWhenMissing(t *testing.T) {
	coreRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	core := &captureRequestIDHandler{Handler: coreRouter}
	remote, cleanup := newBufGRPCRemote(t, core, edgeRouter, "secret", "secret")
	defer cleanup()

	if err := dispatchHelpGetConfig(t, context.Background(), remote, 92); err != nil {
		t.Fatal(err)
	}
	if got := core.requestID(); got == "" {
		t.Fatal("generated request id was not visible in Core context")
	} else if normalizeCoreExecGRPCRequestID(got) != got {
		t.Fatalf("generated request id is not normalized: %q", got)
	}
}

func TestGRPCClientInterceptorLogsTransportErrorWithRequestID(t *testing.T) {
	core, observed := observer.New(zapcore.WarnLevel)
	logger := zap.New(core)
	interceptor := grpcRequestIDUnaryClientInterceptor(logger)
	var metadataRequestID string
	err := interceptor(
		context.Background(),
		coreexecpb.CoreExecService_DispatchAdmitted_FullMethodName,
		nil,
		nil,
		nil,
		func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			md, _ := metadata.FromOutgoingContext(ctx)
			metadataRequestID = firstMetadataValue(md, coreExecGRPCRequestIDMetadataKey)
			return status.Error(codes.Unavailable, "core down")
		},
	)
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("interceptor err = %v, want Unavailable", err)
	}
	if metadataRequestID == "" || normalizeCoreExecGRPCRequestID(metadataRequestID) != metadataRequestID {
		t.Fatalf("metadata request id = %q, want generated normalized id", metadataRequestID)
	}
	entries := observed.All()
	if len(entries) != 1 {
		t.Fatalf("log entries = %d, want 1", len(entries))
	}
	fields := entries[0].ContextMap()
	if fields["request_id"] != metadataRequestID {
		t.Fatalf("logged request id = %v, want %q", fields["request_id"], metadataRequestID)
	}
	if fields["operation"] != "dispatch_admitted" || fields["outcome"] != "unavailable" {
		t.Fatalf("logged fields = %#v, want dispatch_admitted/unavailable", fields)
	}
}

func TestGRPCClientInterceptorLogsCallerCancellationAtDebug(t *testing.T) {
	core, observed := observer.New(zapcore.DebugLevel)
	interceptor := grpcRequestIDUnaryClientInterceptor(zap.New(core))
	err := interceptor(
		context.Background(),
		coreexecpb.CoreExecService_DispatchAdmitted_FullMethodName,
		nil,
		nil,
		nil,
		func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			return status.Error(codes.Canceled, context.Canceled.Error())
		},
	)
	if status.Code(err) != codes.Canceled {
		t.Fatalf("interceptor err = %v, want Canceled", err)
	}
	entries := observed.All()
	if len(entries) != 1 {
		t.Fatalf("log entries = %d, want 1", len(entries))
	}
	if entries[0].Level != zapcore.DebugLevel {
		t.Fatalf("log level = %v, want debug", entries[0].Level)
	}
	fields := entries[0].ContextMap()
	if fields["operation"] != "dispatch_admitted" || fields["outcome"] != "canceled" {
		t.Fatalf("logged fields = %#v, want dispatch_admitted/canceled", fields)
	}
}

func TestGRPCRemoteMetricsObserveClientCalls(t *testing.T) {
	coreRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	remote, cleanup := newBufGRPCRemote(t, coreRouter, edgeRouter, "secret", "secret")
	defer cleanup()
	metrics := &captureCoreExecMetrics{}
	remote.metrics = metrics

	if err := remote.Check(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := dispatchHelpGetConfig(t, context.Background(), remote, 101); err != nil {
		t.Fatal(err)
	}
	if !metrics.has("client", "health", "ok") {
		t.Fatalf("missing client health metric: %+v", metrics.snapshot())
	}
	if !metrics.has("client", "get_info", "ok") {
		t.Fatalf("missing client get_info metric: %+v", metrics.snapshot())
	}
	if !metrics.has("client", "dispatch_admitted", "ok") {
		t.Fatalf("missing client dispatch metric: %+v", metrics.snapshot())
	}
}

func TestCoreExecDispatchExportsCoreDatabaseWork(t *testing.T) {
	coreRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	metrics := &captureCoreExecMetrics{}
	server := newCoreExecGRPCServerWithRuntime(
		&databaseTraceDispatchHandler{Handler: coreRouter}, "core-db", nil, nil, metrics,
	)
	remote, cleanup := newBufGRPCRemoteWithServer(t, server, edgeRouter, "secret", "secret")
	defer cleanup()
	if err := dispatchHelpGetConfig(t, context.Background(), remote, 701); err != nil {
		t.Fatal(err)
	}
	queries, duration, databaseErrors, found := metrics.database("help.getConfig")
	if !found || queries != 2 || duration != 5*time.Millisecond || databaseErrors != 0 {
		t.Fatalf("database metrics = queries:%d duration:%s errors:%d found:%v", queries, duration, databaseErrors, found)
	}
}

func TestGRPCMetricsUnaryServerInterceptorObservesStatus(t *testing.T) {
	metrics := &captureCoreExecMetrics{}
	interceptor := grpcMetricsUnaryServerInterceptor(metrics)
	_, err := interceptor(
		context.Background(),
		nil,
		&grpc.UnaryServerInfo{FullMethod: coreexecpb.CoreExecService_GetInfo_FullMethodName},
		func(context.Context, any) (any, error) {
			return nil, status.Error(codes.Unauthenticated, "bad token")
		},
	)
	if err == nil {
		t.Fatal("interceptor unexpectedly succeeded")
	}
	if !metrics.has("server", "get_info", "unauthenticated") {
		t.Fatalf("missing server unauthenticated metric: %+v", metrics.snapshot())
	}
}

func TestGRPCMetricsUnaryServerInterceptorClassifiesResponseErrors(t *testing.T) {
	tests := []struct {
		name       string
		fullMethod string
		response   any
		wantOp     string
		wantOut    string
	}{
		{
			name:       "dispatch_rpc_error",
			fullMethod: coreexecpb.CoreExecService_DispatchAdmitted_FullMethodName,
			response:   &coreexecpb.DispatchResponse{RpcError: &coreexecpb.RPCError{Code: 400, Message: "PHONE_CODE_INVALID"}},
			wantOp:     "dispatch_admitted",
			wantOut:    "rpc_error",
		},
		{
			name:       "dispatch_control_error",
			fullMethod: coreexecpb.CoreExecService_DispatchAdmitted_FullMethodName,
			response:   &coreexecpb.DispatchResponse{Error: "store unavailable for auth_key_id=123"},
			wantOp:     "dispatch_admitted",
			wantOut:    "control_error",
		},
		{
			name:       "info_protocol_error",
			fullMethod: coreexecpb.CoreExecService_GetInfo_FullMethodName,
			response:   &coreexecpb.CoreExecInfoResponse{Error: "incompatible protocol edge=2..2 core=1..1"},
			wantOp:     "get_info",
			wantOut:    "version_mismatch",
		},
		{
			name:       "profile_control_error",
			fullMethod: coreexecpb.CoreExecService_PublishLayerEvidence_FullMethodName,
			response:   &coreexecpb.ErrorResponse{Error: "profile rejected"},
			wantOp:     "publish_layer_evidence",
			wantOut:    "control_error",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			metrics := &captureCoreExecMetrics{}
			interceptor := grpcMetricsUnaryServerInterceptor(metrics)
			_, err := interceptor(
				context.Background(),
				nil,
				&grpc.UnaryServerInfo{FullMethod: tc.fullMethod},
				func(context.Context, any) (any, error) {
					return tc.response, nil
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			if !metrics.has("server", tc.wantOp, tc.wantOut) {
				t.Fatalf("missing server metric %s/%s: %+v", tc.wantOp, tc.wantOut, metrics.snapshot())
			}
		})
	}
}

func TestGRPCRemoteCheckRejectsIncompatibleProtocolInfo(t *testing.T) {
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	remote, cleanup := newBufGRPCRemoteWithServer(t, &incompatibleInfoServer{}, edgeRouter, "secret", "secret")
	defer cleanup()

	err := remote.Check(context.Background())
	if !errors.Is(err, ErrGRPCProtocolVersionMismatch) {
		t.Fatalf("Check err = %v, want protocol mismatch", err)
	}
}

func TestGRPCRemoteCheckRejectsLegacyV2ProtocolInfo(t *testing.T) {
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	remote, cleanup := newBufGRPCRemoteWithServer(t, &legacyV2InfoServer{}, edgeRouter, "secret", "secret")
	defer cleanup()

	err := remote.Check(context.Background())
	if !errors.Is(err, ErrGRPCProtocolVersionMismatch) {
		t.Fatalf("Check legacy v2 err = %v, want protocol mismatch", err)
	}
}

func TestGRPCRemoteClassifiesStatusErrors(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		want      error
		retryable bool
	}{
		{name: "canceled", err: status.Error(codes.Canceled, "closed"), want: context.Canceled},
		{name: "deadline", err: status.Error(codes.DeadlineExceeded, "slow"), want: ErrRemoteRequestTimeout, retryable: true},
		{name: "unauthenticated", err: status.Error(codes.Unauthenticated, "bad token"), want: ErrRemoteUnauthenticated},
		{name: "invalid", err: status.Error(codes.InvalidArgument, "bad request"), want: ErrRemoteInvalidRequest},
		{name: "unimplemented", err: status.Error(codes.Unimplemented, "old core"), want: ErrGRPCProtocolVersionMismatch},
		{name: "unavailable", err: status.Error(codes.Unavailable, "down"), want: ErrRemoteRPCUnavailable, retryable: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := grpcRemoteCallError("test", tc.err)
			if !errors.Is(got, tc.want) {
				t.Fatalf("grpcRemoteCallError = %v, want %v", got, tc.want)
			}
			if retryable := grpcTransportRetryable(got); retryable != tc.retryable {
				t.Fatalf("grpcTransportRetryable(%v) = %v, want %v", got, retryable, tc.retryable)
			}
		})
	}
}

func TestGRPCRemoteDoesNotImplicitlyRetryDispatchAdmittedUnavailable(t *testing.T) {
	coreRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	core := &countingHandler{Handler: coreRouter}
	var attempts atomic.Int32
	addr, cleanup := newTCPGRPCServerWithExtraInterceptors(
		t,
		core,
		"secret",
		healthpb.HealthCheckResponse_SERVING,
		func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if info.FullMethod == coreexecpb.CoreExecService_DispatchAdmitted_FullMethodName {
				if attempts.Add(1) == 1 {
					return nil, status.Error(codes.Unavailable, "transient dispatch failure")
				}
			}
			return handler(ctx, req)
		},
	)
	defer cleanup()

	retryServiceConfig := fmt.Sprintf(
		`{"methodConfig":[{"name":[{"service":%q,"method":"DispatchAdmitted"}],"retryPolicy":{"MaxAttempts":4,"InitialBackoff":"0.01s","MaxBackoff":"0.01s","BackoffMultiplier":1.0,"RetryableStatusCodes":["UNAVAILABLE"]}}]}`,
		coreexecpb.CoreExecService_ServiceDesc.ServiceName,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	remote, conn, err := DialGRPCRemote(ctx, edgeRouter, GRPCClientConfig{
		Targets: []string{addr},
		Token:   "secret",
		DialOptions: []grpc.DialOption{
			grpc.WithDefaultServiceConfig(retryServiceConfig),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	err = dispatchHelpGetConfig(t, ctx, remote, 970)
	if !errors.Is(err, ErrRemoteRPCUnavailable) {
		t.Fatalf("DispatchAdmitted err = %v, want ErrRemoteRPCUnavailable", err)
	}
	if got := attempts.Load(); got != 1 {
		t.Fatalf("DispatchAdmitted attempts = %d, want exactly 1; implicit retry would re-enter Core", got)
	}
	if got := core.count(); got != 0 {
		t.Fatalf("Core handler dispatches = %d, want 0 after pre-handler Unavailable", got)
	}
}

func TestGRPCRemoteAppliesRequestTimeoutToDispatch(t *testing.T) {
	coreRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	core := &blockingDispatchHandler{
		Handler:    coreRouter,
		deadlineCh: make(chan bool, 1),
	}
	remote, cleanup := newBufGRPCRemote(t, core, edgeRouter, "secret", "secret")
	defer cleanup()
	remote.requestTimeout = 20 * time.Millisecond

	var body bin.Buffer
	if err := tlprofile.EncodeObject(tlprofile.Profile225, &tg.HelpGetConfigRequest{}, &body); err != nil {
		t.Fatal(err)
	}
	admitted, err := remote.AdmitLayer(tlprofile.Profile225, &body, tlprofile.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	errCh := make(chan error, 1)
	go func() {
		_, _, err := remote.DispatchAdmitted(context.Background(), [8]byte{1, 2, 3}, 10, 20, 30, admitted)
		errCh <- err
	}()
	select {
	case got := <-core.deadlineCh:
		if !got {
			t.Fatal("dispatch reached Core without a deadline")
		}
	case <-time.After(time.Second):
		t.Fatal("dispatch did not reach Core")
	}
	select {
	case err := <-errCh:
		if !isDeadlineError(err) {
			t.Fatalf("dispatch err = %v, want deadline", err)
		}
	case <-time.After(time.Second):
		t.Fatal("dispatch did not return after request timeout")
	}
}

func TestGRPCRemoteAppliesRequestTimeoutToProfileRPC(t *testing.T) {
	coreRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	core := &blockingProfileHandler{
		Handler:    coreRouter,
		deadlineCh: make(chan bool, 1),
	}
	remote, cleanup := newBufGRPCRemote(t, core, coreRouter, "secret", "secret")
	defer cleanup()
	remote.requestTimeout = 20 * time.Millisecond

	errCh := make(chan error, 1)
	go func() {
		errCh <- remote.PrimeAuthKeyLayerIdentity(context.Background(), [8]byte{1, 2, 3})
	}()
	select {
	case got := <-core.deadlineCh:
		if !got {
			t.Fatal("profile RPC reached Core without a deadline")
		}
	case <-time.After(time.Second):
		t.Fatal("profile RPC did not reach Core")
	}
	select {
	case err := <-errCh:
		if !isDeadlineError(err) {
			t.Fatalf("profile err = %v, want deadline", err)
		}
	case <-time.After(time.Second):
		t.Fatal("profile RPC did not return after request timeout")
	}
}

func TestGRPCRemoteDurableLayerEvidenceRoundTrip(t *testing.T) {
	layers := memory.NewAuthKeyStore()
	deps := rpc.Deps{AuthKeySessionLayers: layers}
	coreRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, deps, zaptest.NewLogger(t), clock.System)
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, deps, zaptest.NewLogger(t), clock.System)
	remote, cleanup := newBufGRPCRemote(t, coreRouter, edgeRouter, "secret", "secret")
	defer cleanup()

	authKeyID := [8]byte{9, 8, 7}
	const sessionID = int64(44)
	msgID := int64((uint64(time.Now().UTC().Unix()) << 32) | 4)
	if err := layers.Save(context.Background(), store.AuthKeyData{ID: authKeyID}); err != nil {
		t.Fatal(err)
	}

	layer, currentMsgID, publish, err := remote.AdvanceNegotiatedSessionLayerEvidence(context.Background(), authKeyID, sessionID, 225, msgID)
	if err != nil {
		t.Fatal(err)
	}
	if layer != 225 || currentMsgID != msgID || !publish {
		t.Fatalf("advance = layer:%d msg:%d publish:%v", layer, currentMsgID, publish)
	}
	layer, currentMsgID, found, err := remote.ResolveNegotiatedSessionLayerEvidence(context.Background(), authKeyID, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || layer != 225 || currentMsgID != msgID {
		t.Fatalf("resolve session = layer:%d msg:%d found:%v", layer, currentMsgID, found)
	}
	if err := remote.PublishAdmittedLayerProfileEvidence(context.Background(), authKeyID, sessionID, msgID, 2, 1, 225); err != nil {
		t.Fatal(err)
	}
	layer, found, err = remote.ResolveInheritedAuthKeyLayer(context.Background(), authKeyID)
	if err != nil {
		t.Fatal(err)
	}
	if !found || layer != 225 {
		t.Fatalf("resolve inherited = layer:%d found:%v", layer, found)
	}
}

func TestGRPCRemoteObserveInitConnectionRoundTrip(t *testing.T) {
	core := &captureObserveHandler{}
	remote, cleanup := newBufGRPCRemote(t, core, core, "secret", "secret")
	defer cleanup()

	authKeyID := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	err := remote.ObserveInitConnection(
		context.Background(),
		authKeyID,
		77,
		228,
		2040,
		"Desktop",
		"Windows",
		"5.0",
		"en",
		"tdesktop",
		"en",
	)
	if err != nil {
		t.Fatal(err)
	}
	if core.authKeyID != authKeyID || core.sessionID != 77 || core.layer != 228 || core.apiID != 2040 ||
		core.deviceModel != "Desktop" || core.systemVersion != "Windows" || core.appVersion != "5.0" ||
		core.systemLangCode != "en" || core.langPack != "tdesktop" || core.langCode != "en" {
		t.Fatalf("captured observe = %+v", core)
	}
}

func TestGRPCRemotePrepareReplayCommitsAfterDelivery(t *testing.T) {
	coreRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	core := &captureReplayHandler{Handler: coreRouter}
	remote, cleanup := newBufGRPCRemote(t, core, edgeRouter, "secret", "secret")
	defer cleanup()

	var body bin.Buffer
	if err := tlprofile.EncodeObject(tlprofile.Profile225, &tg.HelpGetConfigRequest{}, &body); err != nil {
		t.Fatal(err)
	}
	admitted, err := remote.AdmitLayer(tlprofile.Profile225, &body, tlprofile.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	authKeyID := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	after, err := remote.PrepareAdmittedReplay(context.Background(), authKeyID, 77, 88, 99, admitted)
	if err != nil {
		t.Fatal(err)
	}
	if after == nil {
		t.Fatal("PrepareAdmittedReplay returned nil callback")
	}
	if err := after(); err != nil {
		t.Fatal(err)
	}
	if prepare, commit := core.counts(); prepare != 1 || commit != 1 {
		t.Fatalf("counts = prepare:%d commit:%d", prepare, commit)
	}
}

func TestGRPCRemoteDispatchCommitsPostResponseAfterDelivery(t *testing.T) {
	coreRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	core := &captureDispatchPostResponseHandler{Handler: coreRouter}
	remote, cleanup := newBufGRPCRemote(t, core, edgeRouter, "secret", "secret")
	defer cleanup()

	ctx := postresponse.WithCallbacks(context.Background())
	admitted := admitHelpGetConfigForGRPCTest(t, remote)
	result, method, err := remote.DispatchAdmitted(ctx, [8]byte{1, 2, 3}, 77, 88, 100, admitted)
	if err != nil {
		t.Fatal(err)
	}
	if method != "help.getConfig" || result == nil {
		t.Fatalf("dispatch result = method:%q result:%T", method, result)
	}
	if got := core.count(); got != 0 {
		t.Fatalf("post-response committed before delivery = %d, want 0", got)
	}
	if after := postresponse.TakeWithExecutor(ctx, remote); after != nil {
		after()
	}
	if got := core.count(); got != 1 {
		t.Fatalf("post-response commits after delivery = %d, want 1", got)
	}
	if after := postresponse.TakeWithExecutor(ctx, remote); after != nil {
		after()
	}
	if got := core.count(); got != 1 {
		t.Fatalf("post-response commit was not idempotent: %d", got)
	}
}

func TestGRPCRemoteAccountDeleteTeardownCommitsAfterResultDelivery(t *testing.T) {
	ctx := context.Background()
	authKeyID := [8]byte{9, 8, 7, 6, 5, 4, 3, 2}
	const sessionID int64 = 77

	userStore := memory.NewUserStore()
	user, err := userStore.Create(ctx, domain.User{
		Phone:     nextGRPCTestPhone(),
		FirstName: "Delete",
	})
	if err != nil {
		t.Fatal(err)
	}
	authzStore := memory.NewAuthorizationStore()
	deliveryOutbox := memory.NewDeliveryOutboxStore()
	authzStore.AttachDeliveryOutbox(deliveryOutbox)
	authKeyStore := memory.NewAuthKeyStore()
	authSvc := auth.NewService(userStore, authzStore, memory.NewCodeStore(), authKeyStore, memory.NewTempAuthKeyBindingStore(authKeyStore), "12345")
	if err := authKeyStore.Save(ctx, store.AuthKeyData{ID: authKeyID, CreatedAt: time.Now().Unix()}); err != nil {
		t.Fatal(err)
	}
	if err := authzStore.Bind(ctx, domain.Authorization{AuthKeyID: authKeyID, UserID: user.ID}); err != nil {
		t.Fatal(err)
	}
	accountSvc := &coreExecDeleteAccountService{
		Service: appaccount.NewService(memory.NewPasswordStore()),
		userID:  user.ID,
	}
	sessions := newCoreExecDeleteCaptureSessions()
	coreRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{
		Auth:     authSvc,
		Account:  accountSvc,
		Sessions: sessions,
	}, zaptest.NewLogger(t), clock.System)
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	remote, cleanup := newBufGRPCRemote(t, coreRouter, edgeRouter, "secret", "secret")
	defer cleanup()

	dispatchCtx := postresponse.WithCallbacks(ctx)
	var body bin.Buffer
	if err := tlprofile.EncodeObject(tlprofile.Profile228, &tg.AccountDeleteAccountRequest{Reason: "manual"}, &body); err != nil {
		t.Fatal(err)
	}
	admitted, err := remote.AdmitLayer(tlprofile.Profile228, &body, tlprofile.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	result, method, err := remote.DispatchAdmitted(dispatchCtx, authKeyID, sessionID, 100, 101, admitted)
	if err != nil {
		t.Fatal(err)
	}
	if method != "account.deleteAccount" || result == nil {
		t.Fatalf("dispatch result = method:%q result:%T", method, result)
	}
	if sessions.closeCount(authKeyID) != 0 {
		t.Fatal("current authorization closed before rpc_result delivery")
	}
	decoded := decodeBoxedResult(t, result)
	if _, ok := decoded.(*tg.BoolTrue); !ok {
		t.Fatalf("account.deleteAccount result = %T, want *tg.BoolTrue", decoded)
	}
	if sessions.closeCount(authKeyID) != 0 {
		t.Fatal("current authorization closed while rpc_result was only encoded")
	}
	if callback := postresponse.Take(dispatchCtx); callback != nil {
		t.Fatal("remote deletion returned an in-process post-response callback")
	}
	after := postresponse.TakeWithExecutor(dispatchCtx, remote)
	if after == nil {
		t.Fatal("missing remote account authorization teardown action")
	}
	after()
	if got := sessions.closeCount(authKeyID); got != 1 {
		t.Fatalf("authorization closes after delivery = %d, want 1", got)
	}
	after()
	if got := sessions.closeCount(authKeyID); got != 1 {
		t.Fatalf("delivery callback was not idempotent: closes=%d", got)
	}
}

func TestAccountAuthorizationTeardownPostResponseActionCodec(t *testing.T) {
	want := postresponse.Action{
		Kind: postresponse.ActionAccountAuthorizationTeardown,
		AccountAuthorizationTeardown: postresponse.AccountAuthorizationTeardownAction{
			UserID:    42,
			AuthKeyID: [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
		},
	}
	pbActions, err := postResponseActionsToPB([]postresponse.Action{want})
	if err != nil {
		t.Fatal(err)
	}
	if len(pbActions) != 1 {
		t.Fatalf("encoded actions = %d, want 1", len(pbActions))
	}
	encoded := pbActions[0].GetAccountAuthorizationTeardown()
	if encoded == nil || encoded.GetUserId() != want.AccountAuthorizationTeardown.UserID || string(encoded.GetAuthKeyId()) != string(want.AccountAuthorizationTeardown.AuthKeyID[:]) {
		t.Fatalf("encoded teardown = %+v", encoded)
	}
	roundTrip, err := postResponseActionsFromPB(pbActions)
	if err != nil {
		t.Fatal(err)
	}
	if len(roundTrip) != 1 || roundTrip[0].Kind != want.Kind || roundTrip[0].AccountAuthorizationTeardown != want.AccountAuthorizationTeardown {
		t.Fatalf("round-trip teardown = %+v, want %+v", roundTrip, want)
	}

	if _, err := postResponseActionsToPB([]postresponse.Action{{Kind: postresponse.ActionKind("unknown")}}); err == nil {
		t.Fatal("unknown typed post-response action was silently dropped")
	}
	malformed := &coreexecpb.PostResponseAction{
		Action: &coreexecpb.PostResponseAction_AccountAuthorizationTeardown{
			AccountAuthorizationTeardown: &coreexecpb.AccountAuthorizationTeardownAction{
				UserId:    42,
				AuthKeyId: []byte{1, 2, 3},
			},
		},
	}
	if _, err := postResponseActionsFromPB([]*coreexecpb.PostResponseAction{malformed}); err == nil {
		t.Fatal("malformed account authorization teardown auth key was accepted")
	}
}

func TestUpdatesDeliveryLifecyclePostResponseActionCodec(t *testing.T) {
	want := postresponse.Action{
		Kind: postresponse.ActionUpdatesDelivery,
		UpdatesDelivery: postresponse.UpdatesDeliveryAction{
			MarkSessionReady:    true,
			ReadyRawAuthKey:     [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
			ReadySessionID:      101,
			ReadyUserID:         202,
			ActivationToken:     303,
			PublishBootstrap:    true,
			BootstrapUserID:     404,
			BootstrapAuthKey:    [8]byte{8, 7, 6, 5, 4, 3, 2, 1},
			BootstrapRawAuthKey: [8]byte{9, 8, 7, 6, 5, 4, 3, 2},
			BootstrapSessionID:  505,
			BootstrapProbeToken: 606,
		},
	}
	pbActions, err := postResponseActionsToPB([]postresponse.Action{want})
	if err != nil {
		t.Fatal(err)
	}
	if len(pbActions) != 1 {
		t.Fatalf("encoded actions = %d, want 1", len(pbActions))
	}
	encoded := pbActions[0].GetUpdatesDelivery()
	if encoded == nil || encoded.GetActivationToken() != 303 ||
		encoded.GetBootstrapProbeToken() != 606 || encoded.GetBootstrapSessionId() != 505 ||
		string(encoded.GetBootstrapAuthKeyId()) != string(want.UpdatesDelivery.BootstrapAuthKey[:]) ||
		string(encoded.GetBootstrapRawAuthKeyId()) != string(want.UpdatesDelivery.BootstrapRawAuthKey[:]) {
		t.Fatalf("encoded updates delivery lifecycle = %+v", encoded)
	}
	roundTrip, err := postResponseActionsFromPB(pbActions)
	if err != nil {
		t.Fatal(err)
	}
	if len(roundTrip) != 1 || !reflect.DeepEqual(roundTrip[0], want) {
		t.Fatalf("round-trip updates delivery = %+v, want %+v", roundTrip, want)
	}
}

func TestGRPCRemoteRetriesOnlyAccountTeardownAfterTransientCommitFailure(t *testing.T) {
	authKeyID := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	sessions := newCoreExecDeleteCaptureSessions()
	coreRouter := rpc.New(rpc.Config{}, rpc.Deps{Sessions: sessions}, zaptest.NewLogger(t), clock.System)
	server := &accountTeardownCommitFailureServer{
		CoreExecServiceServer: newCoreExecGRPCServer(coreRouter),
		failAccountCalls:      2,
		failCode:              codes.Unavailable,
	}
	edgeRouter := rpc.New(rpc.Config{}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	remote, cleanup := newBufGRPCRemoteWithServer(t, server, edgeRouter, "secret", "secret")
	defer cleanup()
	metrics := &captureCoreExecMetrics{}
	remote.metrics = metrics
	remote.log = zaptest.NewLogger(t)

	err := remote.RunPostResponseActions(context.Background(), []postresponse.Action{
		{Kind: postresponse.ActionUpdatesDelivery},
		{
			Kind: postresponse.ActionAccountAuthorizationTeardown,
			AccountAuthorizationTeardown: postresponse.AccountAuthorizationTeardownAction{
				UserID:    42,
				AuthKeyID: authKeyID,
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := server.ordinaryCalls.Load(); got != 1 {
		t.Fatalf("ordinary post-response commits = %d, want 1", got)
	}
	if got := server.accountCalls.Load(); got != 3 {
		t.Fatalf("account teardown commit attempts = %d, want 3", got)
	}
	if got := server.mixedCalls.Load(); got != 0 {
		t.Fatalf("mixed ordinary/account commit calls = %d, want 0", got)
	}
	if got := sessions.closeCount(authKeyID); got != 1 {
		t.Fatalf("authorization closes after transient recovery = %d, want 1", got)
	}
	if !metrics.has("client", "commit_post_response_actions", "unavailable") || !metrics.has("client", "commit_post_response_actions", "ok") {
		t.Fatalf("retry metrics = %+v, want unavailable and ok", metrics.snapshot())
	}
}

func TestGRPCRemoteBoundsPersistentAccountTeardownCommitFailure(t *testing.T) {
	authKeyID := [8]byte{8, 7, 6, 5, 4, 3, 2, 1}
	sessions := newCoreExecDeleteCaptureSessions()
	coreRouter := rpc.New(rpc.Config{}, rpc.Deps{Sessions: sessions}, zaptest.NewLogger(t), clock.System)
	server := &accountTeardownCommitFailureServer{
		CoreExecServiceServer: newCoreExecGRPCServer(coreRouter),
		failAccountCalls:      accountTeardownCommitMaxAttempts + 10,
		failCode:              codes.Unavailable,
	}
	edgeRouter := rpc.New(rpc.Config{}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	remote, cleanup := newBufGRPCRemoteWithServer(t, server, edgeRouter, "secret", "secret")
	defer cleanup()
	metrics := &captureCoreExecMetrics{}
	logCore, observed := observer.New(zapcore.DebugLevel)
	remote.metrics = metrics
	remote.log = zap.New(logCore)

	err := remote.RunPostResponseActions(context.Background(), []postresponse.Action{{
		Kind: postresponse.ActionAccountAuthorizationTeardown,
		AccountAuthorizationTeardown: postresponse.AccountAuthorizationTeardownAction{
			UserID:    42,
			AuthKeyID: authKeyID,
		},
	}})
	if !errors.Is(err, ErrRemoteRPCUnavailable) {
		t.Fatalf("persistent commit err = %v, want ErrRemoteRPCUnavailable", err)
	}
	if got := int(server.accountCalls.Load()); got != accountTeardownCommitMaxAttempts {
		t.Fatalf("persistent account teardown calls = %d, want %d", got, accountTeardownCommitMaxAttempts)
	}
	if got := sessions.closeCount(authKeyID); got != 0 {
		t.Fatalf("authorization closes after persistent failure = %d, want 0", got)
	}
	if !metrics.has("client", "commit_post_response_actions", "unavailable") {
		t.Fatalf("persistent failure metrics = %+v", metrics.snapshot())
	}
	if got := observed.FilterMessage("commit account authorization teardown post-response failed").Len(); got != 1 {
		t.Fatalf("terminal failure logs = %d, want 1", got)
	}
}

func TestGRPCRemoteDoesNotRetryAccountTeardownControlOrInternalFailure(t *testing.T) {
	tests := []struct {
		name          string
		responseError string
		failCode      codes.Code
	}{
		{name: "response_error", responseError: "teardown rejected"},
		{name: "internal_status", failCode: codes.Internal},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			authKeyID := [8]byte{8, 6, 7, 5, 3, 0, 9, 1}
			sessions := newCoreExecDeleteCaptureSessions()
			coreRouter := rpc.New(rpc.Config{}, rpc.Deps{Sessions: sessions}, zaptest.NewLogger(t), clock.System)
			server := &accountTeardownCommitFailureServer{
				CoreExecServiceServer: newCoreExecGRPCServer(coreRouter),
				failAccountCalls:      accountTeardownCommitMaxAttempts + 10,
				failCode:              tc.failCode,
				responseError:         tc.responseError,
			}
			edgeRouter := rpc.New(rpc.Config{}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
			remote, cleanup := newBufGRPCRemoteWithServer(t, server, edgeRouter, "secret", "secret")
			defer cleanup()
			remote.log = zaptest.NewLogger(t)

			err := remote.RunPostResponseActions(context.Background(), []postresponse.Action{{
				Kind: postresponse.ActionAccountAuthorizationTeardown,
				AccountAuthorizationTeardown: postresponse.AccountAuthorizationTeardownAction{
					UserID:    42,
					AuthKeyID: authKeyID,
				},
			}})
			if err == nil {
				t.Fatal("account teardown failure returned nil error")
			}
			if got := server.accountCalls.Load(); got != 1 {
				t.Fatalf("account teardown calls = %d, want 1", got)
			}
			if got := sessions.closeCount(authKeyID); got != 0 {
				t.Fatalf("authorization closes after rejected teardown = %d, want 0", got)
			}
		})
	}
}

func TestGRPCRemotePrepareReplayDoesNotConsumeAdmission(t *testing.T) {
	coreRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	core := &captureReplayHandler{Handler: coreRouter}
	remote, cleanup := newBufGRPCRemote(t, core, edgeRouter, "secret", "secret")
	defer cleanup()

	var body bin.Buffer
	if err := tlprofile.EncodeObject(tlprofile.Profile225, &tg.HelpGetConfigRequest{}, &body); err != nil {
		t.Fatal(err)
	}
	admitted, err := remote.AdmitLayer(tlprofile.Profile225, &body, tlprofile.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	authKeyID := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	after, err := remote.PrepareAdmittedReplay(context.Background(), authKeyID, 77, 88, 99, admitted)
	if err != nil {
		t.Fatal(err)
	}
	if after == nil {
		t.Fatal("PrepareAdmittedReplay returned nil callback")
	}
	result, method, err := remote.DispatchAdmitted(context.Background(), authKeyID, 77, 88, 100, admitted)
	if err != nil {
		t.Fatal(err)
	}
	if method != "help.getConfig" || result == nil {
		t.Fatalf("dispatch result = method:%q result:%T", method, result)
	}
	var exact bin.Buffer
	if err := result.Encode(&exact); err != nil {
		t.Fatal(err)
	}
	decodedObject, err := tlprofile.DecodeObject(tlprofile.Profile225, &exact, tlprofile.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if decoded, ok := decodedObject.(*tg.Config); !ok || decoded.ThisDC != 2 {
		t.Fatalf("decoded result = %T %+v", decodedObject, decodedObject)
	}
	if exact.Len() != 0 {
		t.Fatalf("decoded result left %d bytes", exact.Len())
	}
	if prepare, commit := core.counts(); prepare != 1 || commit != 0 {
		t.Fatalf("before delivery counts = prepare:%d commit:%d", prepare, commit)
	}
	if err := after(); err != nil {
		t.Fatal(err)
	}
	if prepare, commit := core.counts(); prepare != 1 || commit != 1 {
		t.Fatalf("after delivery counts = prepare:%d commit:%d", prepare, commit)
	}
}

func TestGRPCRemoteDispatchesDuplicateAdmittedIdentityIndependently(t *testing.T) {
	coreRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	remote, cleanup := newBufGRPCRemote(t, coreRouter, edgeRouter, "secret", "secret")
	defer cleanup()

	first := admitHelpGetConfigForGRPCTest(t, remote)
	second := admitHelpGetConfigForGRPCTest(t, remote)
	if first.Prepared().Identity() != second.Prepared().Identity() {
		t.Fatal("test setup expected identical prepared identities")
	}
	for i, admitted := range []tlprofile.Admission{first, second} {
		result, method, err := remote.DispatchAdmitted(context.Background(), [8]byte{1, 2, 3}, 77, int64(900+i), uint64(1000+i), admitted)
		if err != nil {
			t.Fatalf("dispatch duplicate %d: %v", i, err)
		}
		if method != "help.getConfig" || result == nil {
			t.Fatalf("dispatch duplicate %d result = method:%q result:%T", i, method, result)
		}
	}
}

func TestGRPCRemotePendingAdmissionCapacityAndExpiry(t *testing.T) {
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	metrics := &captureCoreExecMetrics{}
	remote := NewGRPCRemote(edgeRouter, nil, nil)
	remote.metrics = metrics
	remote.maxPendingAdmissions = 1

	if _, err := tryAdmitHelpGetConfigForGRPCTest(remote); err != nil {
		t.Fatal(err)
	}
	if stats := remote.PendingAdmissionStats(); stats.Count != 1 || stats.Capacity != 1 {
		t.Fatalf("stats after first admit = %+v, want count=1 capacity=1", stats)
	}
	if _, err := tryAdmitHelpGetConfigForGRPCTest(remote); !errors.Is(err, ErrRemoteAdmissionCapacity) {
		t.Fatalf("second admit err = %v, want ErrRemoteAdmissionCapacity", err)
	}
	if !metrics.hasPendingReject("grpc", "capacity") {
		t.Fatalf("capacity reject metric missing; events=%+v", metrics.snapshot())
	}

	remote.pendingAdmissionTTL = time.Nanosecond
	time.Sleep(time.Millisecond)
	if stats := remote.PendingAdmissionStats(); stats.Count != 0 || stats.Capacity != 1 || stats.TTL != time.Nanosecond {
		t.Fatalf("stats after expiry = %+v, want count=0 capacity=1 ttl=%s", stats, time.Nanosecond)
	}
	if _, err := tryAdmitHelpGetConfigForGRPCTest(remote); err != nil {
		t.Fatalf("admit after pending TTL cleanup: %v", err)
	}
	if got := remote.pendingCount; got != 1 {
		t.Fatalf("pendingCount = %d, want 1 after expiring stale admission", got)
	}
}

func TestGRPCRemotePrepareReplayCommitErrorIsSticky(t *testing.T) {
	coreRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	core := &captureReplayHandler{Handler: coreRouter, commitErr: errors.New("restore failed")}
	remote, cleanup := newBufGRPCRemote(t, core, edgeRouter, "secret", "secret")
	defer cleanup()

	var body bin.Buffer
	if err := tlprofile.EncodeObject(tlprofile.Profile225, &tg.HelpGetConfigRequest{}, &body); err != nil {
		t.Fatal(err)
	}
	admitted, err := remote.AdmitLayer(tlprofile.Profile225, &body, tlprofile.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	after, err := remote.PrepareAdmittedReplay(context.Background(), [8]byte{1}, 77, 88, 99, admitted)
	if err != nil {
		t.Fatal(err)
	}
	if err := after(); err == nil {
		t.Fatal("first delivery callback unexpectedly succeeded")
	}
	if err := after(); err == nil {
		t.Fatal("second delivery callback did not preserve the first error")
	}
	if prepare, commit := core.counts(); prepare != 1 || commit != 1 {
		t.Fatalf("counts = prepare:%d commit:%d", prepare, commit)
	}
}

func TestParseGRPCTargets(t *testing.T) {
	got, err := ParseGRPCTargets("127.0.0.1:2500, 127.0.0.1:2501,127.0.0.1:2500")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "127.0.0.1:2500" || got[1] != "127.0.0.1:2501" {
		t.Fatalf("targets = %#v", got)
	}
	if _, err := ParseGRPCTargets(" , "); !errors.Is(err, ErrGRPCTargetsMissing) {
		t.Fatalf("empty targets err = %v", err)
	}
	for _, raw := range []string{
		"127.0.0.1",
		"127.0.0.1:0",
		"127.0.0.1:70000",
		"http://127.0.0.1:2500",
		"dns:///coreexec.default.svc.cluster.local:2500",
		"127.0.0.1:not-a-port",
		"local host:2500",
		"::1:2500",
	} {
		if _, err := ParseGRPCTargets(raw); err == nil {
			t.Fatalf("ParseGRPCTargets(%q) unexpectedly succeeded", raw)
		}
	}
	ipv6, err := ParseGRPCTargets("[::1]:2500")
	if err != nil {
		t.Fatalf("ParseGRPCTargets IPv6: %v", err)
	}
	if len(ipv6) != 1 || ipv6[0] != "[::1]:2500" {
		t.Fatalf("IPv6 targets = %#v", ipv6)
	}
	provider, err := NewStaticGRPCResolverProvider(got)
	if err != nil {
		t.Fatal(err)
	}
	if provider.Target() != staticCoreExecResolverTarget {
		t.Fatalf("provider target = %q", provider.Target())
	}
	if len(provider.DialOptions()) == 0 {
		t.Fatal("multi-target static provider did not expose resolver dial options")
	}
	copied := provider.Targets()
	copied[0] = "mutated"
	if provider.Targets()[0] != "127.0.0.1:2500" {
		t.Fatalf("provider targets leaked mutable slice: %#v", provider.Targets())
	}
	singleProvider, err := NewStaticGRPCResolverProvider([]string{"127.0.0.1:2500"})
	if err != nil {
		t.Fatal(err)
	}
	if singleProvider.Target() != staticCoreExecResolverTarget {
		t.Fatalf("single provider target = %q", singleProvider.Target())
	}
	if len(singleProvider.DialOptions()) == 0 {
		t.Fatal("single-target static provider did not expose resolver dial options")
	}
	if err := singleProvider.UpdateTargets([]string{"http://127.0.0.1:2600"}); err == nil {
		t.Fatal("UpdateTargets accepted URL-style target")
	}
	if err := singleProvider.UpdateTargets([]string{"127.0.0.1:2600", "127.0.0.1:2601"}); err != nil {
		t.Fatal(err)
	}
	updated := singleProvider.Targets()
	if len(updated) != 2 || updated[0] != "127.0.0.1:2600" || updated[1] != "127.0.0.1:2601" {
		t.Fatalf("updated targets = %#v", updated)
	}
}

func TestParseGRPCTargetsForResolverDNS(t *testing.T) {
	bare, err := ParseGRPCTargetsForResolver(" coreexec.default.svc.cluster.local:2440 ", GRPCResolverDNS)
	if err != nil {
		t.Fatal(err)
	}
	if len(bare) != 1 || bare[0] != "coreexec.default.svc.cluster.local:2440" {
		t.Fatalf("bare dns targets = %#v", bare)
	}
	uri := "dns://127.0.0.1:5353/coreexec.test:2440"
	got, err := ParseGRPCTargetsForResolver(uri, GRPCResolverDNS)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != uri {
		t.Fatalf("dns uri targets = %#v", got)
	}
	if _, err := ParseGRPCTargetsForResolver(uri, GRPCResolverStatic); err == nil {
		t.Fatal("static resolver accepted dns URI target")
	}
	for _, raw := range []string{
		"dns://127.0.0.1:5353",
		"dns://127.0.0.1/coreexec.test:2440",
		"dns://127.0.0.1:5353/coreexec.test:0",
		"dns://127.0.0.1:5353/coreexec.test:2440?x=1",
		"core-a:2440,core-b:2440",
	} {
		if _, err := ParseGRPCTargetsForResolver(raw, GRPCResolverDNS); err == nil {
			t.Fatalf("ParseGRPCTargetsForResolver(%q, dns) unexpectedly succeeded", raw)
		}
	}
}

func TestGRPCResolverProviderSelection(t *testing.T) {
	staticProvider, err := grpcResolverProvider(GRPCClientConfig{
		ResolverKind: GRPCResolverStatic,
		Targets:      []string{"127.0.0.1:2500", "127.0.0.1:2501"},
	})
	if err != nil {
		t.Fatalf("static resolver provider: %v", err)
	}
	if staticProvider.Target() != staticCoreExecResolverTarget || len(staticProvider.DialOptions()) == 0 {
		t.Fatalf("static provider target/options = %q/%d", staticProvider.Target(), len(staticProvider.DialOptions()))
	}

	dnsProvider, err := grpcResolverProvider(GRPCClientConfig{
		ResolverKind: GRPCResolverDNS,
		Targets:      []string{"coreexec.default.svc.cluster.local:2440"},
	})
	if err != nil {
		t.Fatalf("dns resolver provider: %v", err)
	}
	if dnsProvider.Target() != "dns:///coreexec.default.svc.cluster.local:2440" {
		t.Fatalf("dns provider target = %q", dnsProvider.Target())
	}
	if len(dnsProvider.DialOptions()) != 0 {
		t.Fatalf("dns provider dial options = %d, want gRPC built-in resolver options only", len(dnsProvider.DialOptions()))
	}
	dnsURI := "dns://127.0.0.1:5353/coreexec.test:2440"
	dnsURIProvider, err := grpcResolverProvider(GRPCClientConfig{
		ResolverKind: GRPCResolverDNS,
		Targets:      []string{dnsURI},
	})
	if err != nil {
		t.Fatalf("dns URI resolver provider: %v", err)
	}
	if dnsURIProvider.Target() != dnsURI {
		t.Fatalf("dns URI provider target = %q", dnsURIProvider.Target())
	}

	if _, err := grpcResolverProvider(GRPCClientConfig{
		ResolverKind: GRPCResolverDNS,
		Targets:      []string{"core-a:2440", "core-b:2440"},
	}); err == nil {
		t.Fatal("dns resolver accepted multiple targets")
	}
	if _, err := grpcResolverProvider(GRPCClientConfig{
		ResolverKind: "consul",
		Targets:      []string{"core-a:2440"},
	}); err == nil {
		t.Fatal("unsupported resolver kind accepted")
	}
}

func TestGRPCRemoteStaticTargetsRoundRobinAcrossCores(t *testing.T) {
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	coreA := &countingHandler{Handler: rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)}
	coreB := &countingHandler{Handler: rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)}
	addrA, cleanupA := newTCPGRPCServer(t, coreA, "secret")
	defer cleanupA()
	addrB, cleanupB := newTCPGRPCServer(t, coreB, "secret")
	defer cleanupB()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	remote, conn, err := DialGRPCRemote(ctx, edgeRouter, GRPCClientConfig{
		Targets: []string{addrA, addrB},
		Token:   "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	for i := 0; i < 40 && (coreA.count() == 0 || coreB.count() == 0); i++ {
		var body bin.Buffer
		if err := tlprofile.EncodeObject(tlprofile.Profile225, &tg.HelpGetConfigRequest{}, &body); err != nil {
			t.Fatal(err)
		}
		admitted, err := remote.AdmitLayer(tlprofile.Profile225, &body, tlprofile.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		result, method, err := remote.DispatchAdmitted(ctx, [8]byte{1, 2, 3}, 10, int64(20+i), uint64(30+i), admitted)
		if err != nil {
			t.Fatal(err)
		}
		if method != "help.getConfig" || result == nil {
			t.Fatalf("dispatch result = method:%q result:%T", method, result)
		}
	}
	if coreA.count() == 0 || coreB.count() == 0 {
		t.Fatalf("round_robin did not reach both cores: coreA=%d coreB=%d", coreA.count(), coreB.count())
	}
}

func TestGRPCRemoteStaticTargetsCommitPostResponseActionsAcrossCores(t *testing.T) {
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	coreA := &captureDispatchPostResponseHandler{Handler: rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)}
	coreB := &captureDispatchPostResponseHandler{Handler: rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)}
	addrA, cleanupA := newTCPGRPCServer(t, coreA, "secret")
	defer cleanupA()
	addrB, cleanupB := newTCPGRPCServer(t, coreB, "secret")
	defer cleanupB()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	remote, conn, err := DialGRPCRemote(ctx, edgeRouter, GRPCClientConfig{
		Targets: []string{addrA, addrB},
		Token:   "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	crossCoreCommit := func() bool {
		return (coreA.dispatchCount() > 0 && coreB.count() > 0) ||
			(coreB.dispatchCount() > 0 && coreA.count() > 0)
	}
	for i := 0; i < 40 && !crossCoreCommit(); i++ {
		dispatchCtx := postresponse.WithCallbacks(ctx)
		admitted := admitHelpGetConfigForGRPCTest(t, remote)
		result, method, err := remote.DispatchAdmitted(dispatchCtx, [8]byte{1, 2, 3}, 10, int64(200+i), uint64(300+i), admitted)
		if err != nil {
			t.Fatal(err)
		}
		if method != "help.getConfig" || result == nil {
			t.Fatalf("dispatch result = method:%q result:%T", method, result)
		}
		if after := postresponse.TakeWithExecutor(dispatchCtx, remote); after != nil {
			after()
		}
	}
	if !crossCoreCommit() {
		t.Fatalf("post-response action did not commit through a non-dispatch core: coreA dispatch=%d commit=%d coreB dispatch=%d commit=%d",
			coreA.dispatchCount(), coreA.count(), coreB.dispatchCount(), coreB.count())
	}
	if commits, dispatches := coreA.count()+coreB.count(), coreA.dispatchCount()+coreB.dispatchCount(); commits != dispatches {
		t.Fatalf("post-response action commits = %d, want dispatches %d; coreA dispatch=%d commit=%d coreB dispatch=%d commit=%d",
			commits, dispatches, coreA.dispatchCount(), coreA.count(), coreB.dispatchCount(), coreB.count())
	}
}

func TestGRPCRemoteStaticTargetsShareAuthenticatedSessionAcrossCores(t *testing.T) {
	userStore := memory.NewUserStore()
	authzStore := memory.NewAuthorizationStore()
	deliveryOutbox := memory.NewDeliveryOutboxStore()
	authzStore.AttachDeliveryOutbox(deliveryOutbox)
	authKeyStore := memory.NewAuthKeyStore()
	codeStore := memory.NewCodeStore()
	coreDeps := func() rpc.Deps {
		return rpc.Deps{
			Auth:           auth.NewService(userStore, authzStore, codeStore, authKeyStore, memory.NewTempAuthKeyBindingStore(authKeyStore), "12345"),
			Users:          users.NewService(userStore),
			DeliveryOutbox: deliveryOutbox,
		}
	}
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	coreA := &methodCountingHandler{Handler: rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, coreDeps(), zaptest.NewLogger(t), clock.System)}
	coreB := &methodCountingHandler{Handler: rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, coreDeps(), zaptest.NewLogger(t), clock.System)}
	addrA, cleanupA := newTCPGRPCServer(t, coreA, "secret")
	defer cleanupA()
	addrB, cleanupB := newTCPGRPCServer(t, coreB, "secret")
	defer cleanupB()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	remote, conn, err := DialGRPCRemote(ctx, edgeRouter, GRPCClientConfig{
		Targets: []string{addrA, addrB},
		Token:   "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	authKeyID := [8]byte{4, 3, 2, 1, 8, 7, 6, 5}
	const sessionID int64 = 88
	if err := authKeyStore.Save(ctx, store.AuthKeyData{ID: authKeyID, CreatedAt: time.Now().Unix()}); err != nil {
		t.Fatalf("seed auth key: %v", err)
	}
	phone := nextGRPCTestPhone()
	sentValue := decodeBoxedResult(t, dispatchRemoteResult(t, ctx, remote, authKeyID, sessionID, 1200, &tg.AuthSendCodeRequest{
		PhoneNumber: phone,
		APIID:       1,
		APIHash:     "hash",
		Settings:    tg.CodeSettings{},
	}))
	sent, ok := sentValue.(*tg.AuthSentCode)
	if !ok {
		t.Fatalf("auth.sendCode result = %T, want *tg.AuthSentCode", sentValue)
	}
	signInValue := decodeBoxedResult(t, dispatchRemoteResult(t, ctx, remote, authKeyID, sessionID, 1201, &tg.AuthSignInRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: sent.PhoneCodeHash,
		PhoneCode:     "12345",
	}))
	if _, ok := signInValue.(*tg.AuthAuthorizationSignUpRequired); !ok {
		t.Fatalf("auth.signIn result = %T, want *tg.AuthAuthorizationSignUpRequired", signInValue)
	}
	signUpValue := decodeBoxedResult(t, dispatchRemoteResult(t, ctx, remote, authKeyID, sessionID, 1202, &tg.AuthSignUpRequest{
		PhoneNumber:   phone,
		PhoneCodeHash: sent.PhoneCodeHash,
		FirstName:     "CrossCore",
	}))
	authz, ok := signUpValue.(*tg.AuthAuthorization)
	if !ok {
		t.Fatalf("auth.signUp result = %T, want *tg.AuthAuthorization", signUpValue)
	}
	self, ok := authz.User.(*tg.User)
	if !ok || self.ID == 0 {
		t.Fatalf("auth.signUp user = %T %+v, want non-zero *tg.User", authz.User, authz.User)
	}

	for i := 0; i < 80 && (coreA.countMethod("users.getUsers") == 0 || coreB.countMethod("users.getUsers") == 0); i++ {
		result := dispatchRemoteResult(t, ctx, remote, authKeyID, sessionID, int64(1300+i), &tg.UsersGetUsersRequest{
			ID: []tg.InputUserClass{&tg.InputUserSelf{}},
		})
		var wire bin.Buffer
		if err := result.Encode(&wire); err != nil {
			t.Fatalf("encode users.getUsers result: %v", err)
		}
		if wire.Len() == 0 {
			t.Fatal("users.getUsers returned empty result wire")
		}
	}
	if coreA.countMethod("users.getUsers") == 0 || coreB.countMethod("users.getUsers") == 0 {
		t.Fatalf("authenticated users.getUsers did not reach both cores for one auth/session: coreA=%v coreB=%v", coreA.methodSnapshot(), coreB.methodSnapshot())
	}
}

func TestGRPCRemoteDNSResolverDispatchesThroughBuiltInResolver(t *testing.T) {
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	core := &countingHandler{Handler: rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)}
	addr, cleanup := newTCPGRPCServer(t, core, "secret")
	defer cleanup()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split test grpc addr %q: %v", addr, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	remote, conn, err := DialGRPCRemote(ctx, edgeRouter, GRPCClientConfig{
		ResolverKind: GRPCResolverDNS,
		Targets:      []string{net.JoinHostPort("127.0.0.1", port)},
		Token:        "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	if err := dispatchHelpGetConfig(t, ctx, remote, 160); err != nil {
		t.Fatal(err)
	}
	if core.count() == 0 {
		t.Fatal("dns resolver target was never reached")
	}
}

func TestGRPCRemoteDNSResolverRoundRobinAcrossARecords(t *testing.T) {
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	coreA := &countingHandler{Handler: rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)}
	coreB := &countingHandler{Handler: rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)}
	listeners, port := newLoopbackTCPListenersOnSharedPort(t, []string{"127.0.0.1", "127.0.0.2"})
	addrA, _, cleanupA := newTCPGRPCServerOnListener(t, listeners[0], coreA, "secret", healthpb.HealthCheckResponse_SERVING)
	defer cleanupA()
	addrB, _, cleanupB := newTCPGRPCServerOnListener(t, listeners[1], coreB, "secret", healthpb.HealthCheckResponse_SERVING)
	defer cleanupB()
	dnsAddr := newCoreExecTestDNSServer(t, []net.IP{
		net.ParseIP("127.0.0.1"),
		net.ParseIP("127.0.0.2"),
	})
	target := fmt.Sprintf("dns://%s/%s", dnsAddr, net.JoinHostPort("coreexec.test", port))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	remote, conn, err := DialGRPCRemote(ctx, edgeRouter, GRPCClientConfig{
		ResolverKind: GRPCResolverDNS,
		Targets:      []string{target},
		Token:        "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	for i := 0; i < 120 && (coreA.count() == 0 || coreB.count() == 0); i++ {
		if err := dispatchHelpGetConfig(t, ctx, remote, int64(1160+i)); err != nil {
			t.Fatal(err)
		}
	}
	if coreA.count() == 0 || coreB.count() == 0 {
		t.Fatalf("dns multi-A round_robin did not reach both cores: target=%s addrA=%s addrB=%s coreA=%d coreB=%d", target, addrA, addrB, coreA.count(), coreB.count())
	}
}

func TestGRPCRemoteStaticTargetsSurvivesUnavailableCore(t *testing.T) {
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	core := &countingHandler{Handler: rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)}
	goodAddr, cleanup := newTCPGRPCServer(t, core, "secret")
	defer cleanup()
	badAddr := unusedTCPAddr(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	remote, conn, err := DialGRPCRemote(ctx, edgeRouter, GRPCClientConfig{
		Targets: []string{goodAddr, badAddr},
		Token:   "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	for i := 0; i < 5; i++ {
		var body bin.Buffer
		if err := tlprofile.EncodeObject(tlprofile.Profile225, &tg.HelpGetConfigRequest{}, &body); err != nil {
			t.Fatal(err)
		}
		admitted, err := remote.AdmitLayer(tlprofile.Profile225, &body, tlprofile.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err := remote.DispatchAdmitted(ctx, [8]byte{1, 2, 3}, 10, int64(120+i), uint64(130+i), admitted); err != nil {
			t.Fatalf("dispatch with one unavailable core target failed: %v", err)
		}
	}
	if core.count() == 0 {
		t.Fatal("healthy core was never reached")
	}
}

func TestGRPCRemoteStaticTargetsSkipsNotServingCore(t *testing.T) {
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	coreA := &countingHandler{Handler: rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)}
	coreB := &countingHandler{Handler: rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)}
	addrA, cleanupA := newTCPGRPCServerWithHealthStatus(t, coreA, "secret", healthpb.HealthCheckResponse_NOT_SERVING)
	defer cleanupA()
	addrB, cleanupB := newTCPGRPCServerWithHealthStatus(t, coreB, "secret", healthpb.HealthCheckResponse_SERVING)
	defer cleanupB()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	remote, conn, err := DialGRPCRemote(ctx, edgeRouter, GRPCClientConfig{
		Targets: []string{addrA, addrB},
		Token:   "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	for i := 0; i < 8; i++ {
		if err := dispatchHelpGetConfig(t, ctx, remote, int64(180+i)); err != nil {
			t.Fatal(err)
		}
	}
	if coreA.count() != 0 {
		t.Fatalf("NOT_SERVING core received dispatches: coreA=%d coreB=%d", coreA.count(), coreB.count())
	}
	if coreB.count() == 0 {
		t.Fatal("serving core was never reached")
	}
}

func TestGRPCRemoteStaticTargetsTracksReadinessFlaps(t *testing.T) {
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	coreA := &countingHandler{Handler: rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)}
	coreB := &countingHandler{Handler: rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)}
	addrA, healthA, cleanupA := newTCPGRPCServerWithMutableHealth(t, coreA, "secret", healthpb.HealthCheckResponse_SERVING)
	defer cleanupA()
	addrB, _, cleanupB := newTCPGRPCServerWithMutableHealth(t, coreB, "secret", healthpb.HealthCheckResponse_SERVING)
	defer cleanupB()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	remote, conn, err := DialGRPCRemote(ctx, edgeRouter, GRPCClientConfig{
		Targets: []string{addrA, addrB},
		Token:   "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	for i := 0; i < 80 && (coreA.count() == 0 || coreB.count() == 0); i++ {
		if err := dispatchHelpGetConfig(t, ctx, remote, int64(700+i)); err != nil {
			t.Fatal(err)
		}
	}
	if coreA.count() == 0 || coreB.count() == 0 {
		t.Fatalf("initial round_robin did not reach both cores: coreA=%d coreB=%d", coreA.count(), coreB.count())
	}

	healthA.SetServingStatus(coreexecpb.CoreExecService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_NOT_SERVING)
	baselineB := coreB.count()
	lastA := coreA.count()
	stableAStreak := 0
	var lastErr error
	for i := 0; i < 120; i++ {
		lastErr = dispatchHelpGetConfig(t, ctx, remote, int64(800+i))
		if lastErr != nil {
			if ctx.Err() != nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
		currentA := coreA.count()
		if currentA == lastA {
			stableAStreak++
		} else {
			lastA = currentA
			stableAStreak = 0
		}
		if coreB.count() > baselineB && stableAStreak >= 8 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if coreB.count() <= baselineB || stableAStreak < 8 {
		t.Fatalf("client did not converge away from NOT_SERVING core; coreA=%d coreB=%d stableAStreak=%d lastErr=%v", coreA.count(), coreB.count(), stableAStreak, lastErr)
	}

	beforeRecoverA := coreA.count()
	healthA.SetServingStatus(coreexecpb.CoreExecService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	for i := 0; i < 160 && coreA.count() == beforeRecoverA; i++ {
		lastErr = dispatchHelpGetConfig(t, ctx, remote, int64(1000+i))
		if lastErr != nil {
			if ctx.Err() != nil {
				break
			}
			time.Sleep(50 * time.Millisecond)
			continue
		}
		time.Sleep(50 * time.Millisecond)
	}
	if coreA.count() == beforeRecoverA {
		t.Fatalf("recovered SERVING core was not used again; coreA=%d coreB=%d lastErr=%v", coreA.count(), coreB.count(), lastErr)
	}
}

func TestStartGRPCMarksHealthNotServingBeforeShutdown(t *testing.T) {
	coreRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	ctx, cancel := context.WithCancel(context.Background())
	addr := unusedTCPAddr(t)
	srv, err := StartGRPC(ctx, GRPCServerConfig{
		Addr:    addr,
		Token:   "secret",
		Handler: coreRouter,
		Logger:  zaptest.NewLogger(t),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Stop()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	watchCtx, watchCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer watchCancel()
	stream, err := healthpb.NewHealthClient(conn).Watch(watchCtx, &healthpb.HealthCheckRequest{
		Service: coreexecpb.CoreExecService_ServiceDesc.ServiceName,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if first.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("initial health status = %s, want SERVING", first.GetStatus())
	}

	cancel()
	for {
		res, err := stream.Recv()
		if err != nil {
			t.Fatalf("health watch ended before NOT_SERVING: %v", err)
		}
		if res.GetStatus() == healthpb.HealthCheckResponse_NOT_SERVING {
			return
		}
	}
}

func TestCoreExecGRPCDefaultServiceConfigEnablesHealthChecking(t *testing.T) {
	var parsed struct {
		LoadBalancingConfig []map[string]map[string]any `json:"loadBalancingConfig"`
		HealthCheckConfig   struct {
			ServiceName string `json:"serviceName"`
		} `json:"healthCheckConfig"`
		MethodConfig []json.RawMessage `json:"methodConfig"`
	}
	if err := json.Unmarshal([]byte(coreExecGRPCDefaultServiceConfig()), &parsed); err != nil {
		t.Fatal(err)
	}
	if len(parsed.LoadBalancingConfig) != 1 {
		t.Fatalf("loadBalancingConfig len = %d, want 1", len(parsed.LoadBalancingConfig))
	}
	if _, ok := parsed.LoadBalancingConfig[0]["round_robin"]; !ok {
		t.Fatalf("loadBalancingConfig = %#v, want round_robin", parsed.LoadBalancingConfig)
	}
	if parsed.HealthCheckConfig.ServiceName != coreexecpb.CoreExecService_ServiceDesc.ServiceName {
		t.Fatalf("health serviceName = %q, want %q",
			parsed.HealthCheckConfig.ServiceName,
			coreexecpb.CoreExecService_ServiceDesc.ServiceName)
	}
	if len(parsed.MethodConfig) != 0 {
		t.Fatalf("methodConfig = %s, want none so CoreExec cannot gain implicit retry policy from the default config", parsed.MethodConfig)
	}
}

func TestGRPCRemoteStaticResolverUpdatesTargets(t *testing.T) {
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	coreA := &countingHandler{Handler: rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)}
	coreB := &countingHandler{Handler: rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)}
	addrA, cleanupA := newTCPGRPCServer(t, coreA, "secret")
	defer func() {
		if cleanupA != nil {
			cleanupA()
		}
	}()
	provider, err := NewStaticGRPCResolverProvider([]string{addrA})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	remote, conn, err := DialGRPCRemote(ctx, edgeRouter, GRPCClientConfig{
		Resolver: provider,
		Token:    "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	if err := dispatchHelpGetConfig(t, ctx, remote, 220); err != nil {
		t.Fatal(err)
	}
	if coreA.count() == 0 {
		t.Fatal("initial core target was never reached")
	}

	addrB, cleanupB := newTCPGRPCServer(t, coreB, "secret")
	defer cleanupB()
	if err := provider.UpdateTargets([]string{addrB}); err != nil {
		t.Fatal(err)
	}
	cleanupA()
	cleanupA = nil

	var lastErr error
	for i := 0; i < 80 && coreB.count() == 0; i++ {
		lastErr = dispatchHelpGetConfig(t, ctx, remote, int64(300+i))
		if lastErr == nil && coreB.count() > 0 {
			break
		}
		if ctx.Err() != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if coreB.count() == 0 {
		t.Fatalf("updated resolver target was never reached; coreA=%d coreB=%d lastErr=%v", coreA.count(), coreB.count(), lastErr)
	}
}

func TestGRPCRemoteStaticResolverUpdatesMultipleClientConns(t *testing.T) {
	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)
	coreA := &countingHandler{Handler: rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)}
	coreB := &countingHandler{Handler: rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zaptest.NewLogger(t), clock.System)}
	addrA, cleanupA := newTCPGRPCServer(t, coreA, "secret")
	defer func() {
		if cleanupA != nil {
			cleanupA()
		}
	}()
	provider, err := NewStaticGRPCResolverProvider([]string{addrA})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	remoteA, connA, err := DialGRPCRemote(ctx, edgeRouter, GRPCClientConfig{
		Resolver: provider,
		Token:    "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connA.Close() }()
	remoteB, connB, err := DialGRPCRemote(ctx, edgeRouter, GRPCClientConfig{
		Resolver: provider,
		Token:    "secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = connB.Close() }()

	if err := dispatchHelpGetConfig(t, ctx, remoteA, 420); err != nil {
		t.Fatal(err)
	}
	if err := dispatchHelpGetConfig(t, ctx, remoteB, 421); err != nil {
		t.Fatal(err)
	}
	if coreA.count() < 2 {
		t.Fatalf("initial shared provider target did not reach both clients: coreA=%d", coreA.count())
	}

	addrB, cleanupB := newTCPGRPCServer(t, coreB, "secret")
	defer cleanupB()
	if err := provider.UpdateTargets([]string{addrB}); err != nil {
		t.Fatal(err)
	}
	cleanupA()
	cleanupA = nil

	var lastErr error
	reachedA, reachedB := false, false
	for i := 0; i < 80 && (!reachedA || !reachedB); i++ {
		if !reachedA {
			before := coreB.count()
			if err := dispatchHelpGetConfig(t, ctx, remoteA, int64(500+i)); err != nil {
				lastErr = err
			} else if coreB.count() > before {
				reachedA = true
			}
		}
		if !reachedB {
			before := coreB.count()
			if err := dispatchHelpGetConfig(t, ctx, remoteB, int64(600+i)); err != nil {
				lastErr = err
			} else if coreB.count() > before {
				reachedB = true
			}
		}
		if reachedA && reachedB {
			break
		}
		if ctx.Err() != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !reachedA || !reachedB {
		t.Fatalf("updated resolver target did not reach both client conns; reachedA=%v reachedB=%v coreA=%d coreB=%d lastErr=%v", reachedA, reachedB, coreA.count(), coreB.count(), lastErr)
	}
}

func newBufGRPCRemote(t testing.TB, core Handler, edge Handler, serverToken, clientToken string) (*GRPCRemote, func()) {
	t.Helper()
	return newBufGRPCRemoteWithServer(t, newCoreExecGRPCServer(core), edge, serverToken, clientToken)
}

func newBufGRPCRemoteWithServer(t testing.TB, server coreexecpb.CoreExecServiceServer, edge Handler, serverToken, clientToken string) (*GRPCRemote, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			grpcRequestIDUnaryServerInterceptor(zaptest.NewLogger(t)),
			grpcBearerUnaryServerInterceptor(serverToken),
		),
		grpc.MaxRecvMsgSize(defaultMaxGRPCMessageSize),
		grpc.MaxSendMsgSize(defaultMaxGRPCMessageSize),
	)
	coreexecpb.RegisterCoreExecServiceServer(srv, server)
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus(coreexecpb.CoreExecService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, healthSrv)
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(lis) }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(
			grpcRequestIDUnaryClientInterceptor(zaptest.NewLogger(t)),
			grpcBearerUnaryClientInterceptor(clientToken),
		),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"pick_first":{}}]}`),
	)
	if err != nil {
		t.Fatal(err)
	}
	conn.Connect()
	for state := conn.GetState(); !conn.WaitForStateChange(ctx, state); state = conn.GetState() {
		if ctx.Err() != nil {
			break
		}
	}
	remote := NewGRPCRemote(edge, coreexecpb.NewCoreExecServiceClient(conn), healthpb.NewHealthClient(conn))
	return remote, func() {
		_ = conn.Close()
		srv.Stop()
		_ = lis.Close()
		select {
		case err := <-serveErr:
			if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				t.Errorf("grpc server: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("grpc server did not stop")
		}
	}
}

func dispatchHelpGetConfig(t *testing.T, ctx context.Context, remote *GRPCRemote, msgID int64) error {
	t.Helper()
	admitted := admitHelpGetConfigForGRPCTest(t, remote)
	result, method, err := remote.DispatchAdmitted(ctx, [8]byte{1, 2, 3}, 10, msgID, uint64(msgID+1000), admitted)
	if err != nil {
		return err
	}
	if method != "help.getConfig" || result == nil {
		return fmt.Errorf("dispatch result = method:%q result:%T", method, result)
	}
	return nil
}

func admitHelpGetConfigForGRPCTest(t testing.TB, remote *GRPCRemote) tlprofile.Admission {
	t.Helper()
	admitted, err := tryAdmitHelpGetConfigForGRPCTest(remote)
	if err != nil {
		t.Fatal(err)
	}
	return admitted
}

func tryAdmitHelpGetConfigForGRPCTest(remote *GRPCRemote) (tlprofile.Admission, error) {
	var body bin.Buffer
	if err := tlprofile.EncodeObject(tlprofile.Profile225, &tg.HelpGetConfigRequest{}, &body); err != nil {
		return tlprofile.Admission{}, err
	}
	admitted, err := remote.AdmitLayer(tlprofile.Profile225, &body, tlprofile.Limits{})
	if err != nil {
		return tlprofile.Admission{}, err
	}
	return admitted, nil
}

func dispatchRemoteResult(t *testing.T, ctx context.Context, remote *GRPCRemote, authKeyID [8]byte, sessionID int64, msgID int64, request bin.Object) tlprofile.Result {
	t.Helper()
	var body bin.Buffer
	if err := tlprofile.EncodeObject(tlprofile.Profile228, request, &body); err != nil {
		t.Fatal(err)
	}
	admitted, err := remote.AdmitLayer(tlprofile.Profile228, &body, tlprofile.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	result, _, err := remote.DispatchAdmitted(ctx, authKeyID, sessionID, msgID, uint64(msgID+1000), admitted)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatalf("dispatch %T returned nil result", request)
	}
	return result
}

func decodeBoxedResult(t *testing.T, result tlprofile.Result) bin.Object {
	t.Helper()
	var wire bin.Buffer
	if err := result.Encode(&wire); err != nil {
		t.Fatalf("encode result: %v", err)
	}
	value, err := tlprofile.DecodeObject(tlprofile.Profile228, &wire, tlprofile.Limits{})
	if err != nil {
		t.Fatalf("decode boxed result: %v", err)
	}
	if wire.Len() != 0 {
		t.Fatalf("decode boxed result left %d bytes", wire.Len())
	}
	return value
}

func unusedTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

type countingHandler struct {
	Handler

	mu         sync.Mutex
	dispatches int
}

func (h *countingHandler) DispatchAdmitted(ctx context.Context, authKeyID [8]byte, sessionID int64, msgID int64, admissionSeq uint64, request tlprofile.Admission) (tlprofile.Result, string, error) {
	h.mu.Lock()
	h.dispatches++
	h.mu.Unlock()
	return h.Handler.DispatchAdmitted(ctx, authKeyID, sessionID, msgID, admissionSeq, request)
}

func (h *countingHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.dispatches
}

type captureIdentityHintContextKey struct{}

type captureIdentityHintHandler struct {
	Handler

	mu   sync.Mutex
	hint mtprotoedge.LayerRPCIdentityHint
	ok   bool
}

func (h *captureIdentityHintHandler) WithLayerRPCIdentityHint(ctx context.Context, hint mtprotoedge.LayerRPCIdentityHint) context.Context {
	if h.Handler != nil {
		ctx = h.Handler.WithLayerRPCIdentityHint(ctx, hint)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, captureIdentityHintContextKey{}, hint)
}

func (h *captureIdentityHintHandler) DispatchAdmitted(ctx context.Context, authKeyID [8]byte, sessionID int64, msgID int64, admissionSeq uint64, request tlprofile.Admission) (tlprofile.Result, string, error) {
	if hint, ok := ctx.Value(captureIdentityHintContextKey{}).(mtprotoedge.LayerRPCIdentityHint); ok {
		h.mu.Lock()
		h.hint = hint
		h.ok = true
		h.mu.Unlock()
	}
	return h.Handler.DispatchAdmitted(ctx, authKeyID, sessionID, msgID, admissionSeq, request)
}

func (h *captureIdentityHintHandler) identityHint() (mtprotoedge.LayerRPCIdentityHint, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.hint, h.ok
}

type captureObserveHandler struct {
	Handler

	authKeyID      [8]byte
	sessionID      int64
	layer          int
	apiID          int
	deviceModel    string
	systemVersion  string
	appVersion     string
	systemLangCode string
	langPack       string
	langCode       string
}

func (h *captureObserveHandler) ObserveInitConnection(_ context.Context, authKeyID [8]byte, sessionID int64, layer, apiID int, deviceModel, systemVersion, appVersion, systemLangCode, langPack, langCode string) error {
	h.authKeyID = authKeyID
	h.sessionID = sessionID
	h.layer = layer
	h.apiID = apiID
	h.deviceModel = deviceModel
	h.systemVersion = systemVersion
	h.appVersion = appVersion
	h.systemLangCode = systemLangCode
	h.langPack = langPack
	h.langCode = langCode
	return nil
}

type captureReplayHandler struct {
	Handler

	mu        sync.Mutex
	prepares  int
	commits   int
	commitErr error
}

func (h *captureReplayHandler) PrepareAdmittedReplay(ctx context.Context, authKeyID [8]byte, sessionID int64, msgID int64, admissionSeq uint64, request tlprofile.Admission) (func() error, error) {
	h.mu.Lock()
	h.prepares++
	h.mu.Unlock()

	var baseAfter func() error
	if h.Handler != nil {
		after, err := h.Handler.PrepareAdmittedReplay(ctx, authKeyID, sessionID, msgID, admissionSeq, request)
		if err != nil {
			return nil, err
		}
		baseAfter = after
	}
	return func() error {
		h.mu.Lock()
		h.commits++
		commitErr := h.commitErr
		h.mu.Unlock()
		if commitErr != nil {
			return commitErr
		}
		if baseAfter != nil {
			return baseAfter()
		}
		return nil
	}, nil
}

func (h *captureReplayHandler) counts() (prepare int, commit int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.prepares, h.commits
}

type captureDispatchPostResponseHandler struct {
	Handler

	dispatches atomic.Int32
	commits    atomic.Int32
}

func (h *captureDispatchPostResponseHandler) DispatchAdmitted(ctx context.Context, authKeyID [8]byte, sessionID int64, msgID int64, admissionSeq uint64, request tlprofile.Admission) (tlprofile.Result, string, error) {
	h.dispatches.Add(1)
	result, method, err := h.Handler.DispatchAdmitted(ctx, authKeyID, sessionID, msgID, admissionSeq, request)
	if err == nil && result != nil {
		postresponse.RegisterAction(ctx, postresponse.Action{
			Kind: postresponse.ActionUpdatesDelivery,
			UpdatesDelivery: postresponse.UpdatesDeliveryAction{
				MarkSessionReady: true,
				ReadyRawAuthKey:  authKeyID,
				ReadySessionID:   sessionID,
				ReadyUserID:      1000000001,
			},
		})
	}
	return result, method, err
}

func (h *captureDispatchPostResponseHandler) RunPostResponseActions(_ context.Context, actions []postresponse.Action) error {
	for _, action := range actions {
		if action.Kind == postresponse.ActionUpdatesDelivery {
			h.commits.Add(1)
		}
	}
	return nil
}

func (h *captureDispatchPostResponseHandler) count() int {
	return int(h.commits.Load())
}

func (h *captureDispatchPostResponseHandler) dispatchCount() int {
	return int(h.dispatches.Load())
}

type coreExecDeleteAccountService struct {
	*appaccount.Service
	userID int64
}

func (s *coreExecDeleteAccountService) DeleteAccount(_ context.Context, userID int64, authKeyID [8]byte, _ string, _ *domain.PasswordCheck, _ time.Time) (domain.AccountDeleteOutcome, error) {
	if userID != s.userID {
		return domain.AccountDeleteOutcome{}, fmt.Errorf("unexpected deletion user %d", userID)
	}
	return domain.AccountDeleteOutcome{
		Kind: domain.AccountDeleteImmediate,
		Deletion: domain.AccountDeletionResult{
			Changed: true,
			User:    domain.User{ID: userID, Deleted: true},
			RevokedAuthorizations: []domain.Authorization{{
				AuthKeyID: authKeyID,
				UserID:    userID,
			}},
		},
	}, nil
}

type coreExecDeleteCaptureSessions struct {
	*edgecontrol.NoLocalController

	mu     sync.Mutex
	closed map[[8]byte]int
}

func newCoreExecDeleteCaptureSessions() *coreExecDeleteCaptureSessions {
	return &coreExecDeleteCaptureSessions{
		NoLocalController: edgecontrol.NewNoLocalController(),
		closed:            make(map[[8]byte]int),
	}
}

func (s *coreExecDeleteCaptureSessions) CloseSessionsForBusinessAuthKey(id [8]byte) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed[id]++
	return 1
}

func (s *coreExecDeleteCaptureSessions) closeCount(id [8]byte) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed[id]
}

type accountTeardownCommitFailureServer struct {
	coreexecpb.CoreExecServiceServer

	failAccountCalls int
	failCode         codes.Code
	responseError    string
	accountCalls     atomic.Int32
	ordinaryCalls    atomic.Int32
	mixedCalls       atomic.Int32
}

func (s *accountTeardownCommitFailureServer) CommitPostResponseActions(ctx context.Context, req *coreexecpb.CommitPostResponseActionsRequest) (*coreexecpb.CommitPostResponseActionsResponse, error) {
	var account, ordinary bool
	for _, action := range req.GetActions() {
		if action.GetAccountAuthorizationTeardown() != nil {
			account = true
		} else {
			ordinary = true
		}
	}
	if account {
		call := s.accountCalls.Add(1)
		if ordinary {
			s.mixedCalls.Add(1)
		}
		if int(call) <= s.failAccountCalls {
			if s.responseError != "" {
				return &coreexecpb.CommitPostResponseActionsResponse{Error: s.responseError}, nil
			}
			return nil, status.Error(s.failCode, "account teardown commit failure")
		}
	}
	if ordinary {
		s.ordinaryCalls.Add(1)
	}
	return s.CoreExecServiceServer.CommitPostResponseActions(ctx, req)
}

type methodCountingHandler struct {
	Handler

	mu      sync.Mutex
	methods map[string]int
}

func (h *methodCountingHandler) DispatchAdmitted(ctx context.Context, authKeyID [8]byte, sessionID int64, msgID int64, admissionSeq uint64, request tlprofile.Admission) (tlprofile.Result, string, error) {
	result, method, err := h.Handler.DispatchAdmitted(ctx, authKeyID, sessionID, msgID, admissionSeq, request)
	h.mu.Lock()
	if h.methods == nil {
		h.methods = make(map[string]int)
	}
	h.methods[method]++
	h.mu.Unlock()
	return result, method, err
}

func (h *methodCountingHandler) countMethod(method string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.methods[method]
}

func (h *methodCountingHandler) methodSnapshot() map[string]int {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make(map[string]int, len(h.methods))
	for method, count := range h.methods {
		out[method] = count
	}
	return out
}

type blockingDispatchHandler struct {
	Handler

	deadlineCh chan bool
}

func (h *blockingDispatchHandler) DispatchAdmitted(ctx context.Context, authKeyID [8]byte, sessionID int64, msgID int64, admissionSeq uint64, request tlprofile.Admission) (tlprofile.Result, string, error) {
	_, hasDeadline := ctx.Deadline()
	h.deadlineCh <- hasDeadline
	<-ctx.Done()
	return nil, "help.getConfig", ctx.Err()
}

type blockingProfileHandler struct {
	Handler

	deadlineCh chan bool
}

func (h *blockingProfileHandler) ResolveInheritedAuthKeyLayer(ctx context.Context, rawAuthKeyID [8]byte) (int, bool, error) {
	_, hasDeadline := ctx.Deadline()
	h.deadlineCh <- hasDeadline
	<-ctx.Done()
	return 0, false, ctx.Err()
}

type incompatibleInfoServer struct {
	coreexecpb.UnimplementedCoreExecServiceServer
}

func (s *incompatibleInfoServer) GetInfo(context.Context, *coreexecpb.CoreExecInfoRequest) (*coreexecpb.CoreExecInfoResponse, error) {
	return &coreexecpb.CoreExecInfoResponse{
		ProtocolVersion:             coreExecGRPCProtocolVersion + 1,
		MinSupportedProtocolVersion: coreExecGRPCProtocolVersion + 1,
		Implementation:              "incompatible-test-core",
	}, nil
}

type legacyV2InfoServer struct {
	coreexecpb.UnimplementedCoreExecServiceServer
}

func (*legacyV2InfoServer) GetInfo(context.Context, *coreexecpb.CoreExecInfoRequest) (*coreexecpb.CoreExecInfoResponse, error) {
	return &coreexecpb.CoreExecInfoResponse{
		ProtocolVersion:             2,
		MinSupportedProtocolVersion: 2,
		Capabilities:                coreExecGRPCCapabilitiesCopy(),
		Implementation:              "legacy-v2-test-core",
	}, nil
}

type captureRequestIDServer struct {
	coreexecpb.UnimplementedCoreExecServiceServer

	mu sync.Mutex
	id string
}

func (s *captureRequestIDServer) GetInfo(ctx context.Context, _ *coreexecpb.CoreExecInfoRequest) (*coreexecpb.CoreExecInfoResponse, error) {
	requestID, _ := GRPCRequestIDFromContext(ctx)
	s.mu.Lock()
	s.id = requestID
	s.mu.Unlock()
	return &coreexecpb.CoreExecInfoResponse{
		ProtocolVersion:             coreExecGRPCProtocolVersion,
		MinSupportedProtocolVersion: coreExecGRPCMinSupportedProtocolVersion,
		Capabilities:                coreExecGRPCCapabilitiesCopy(),
		Implementation:              "request-id-test-core",
	}, nil
}

func (s *captureRequestIDServer) requestID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.id
}

type captureRequestIDHandler struct {
	Handler

	mu sync.Mutex
	id string
}

func (h *captureRequestIDHandler) DispatchAdmitted(ctx context.Context, authKeyID [8]byte, sessionID int64, msgID int64, admissionSeq uint64, request tlprofile.Admission) (tlprofile.Result, string, error) {
	requestID, _ := GRPCRequestIDFromContext(ctx)
	h.mu.Lock()
	h.id = requestID
	h.mu.Unlock()
	return h.Handler.DispatchAdmitted(ctx, authKeyID, sessionID, msgID, admissionSeq, request)
}

func (h *captureRequestIDHandler) requestID() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.id
}

type coreExecMetricEvent struct {
	side      string
	operation string
	outcome   string
	transport string
	reason    string
}

type captureCoreExecMetrics struct {
	mu             sync.Mutex
	events         []coreExecMetricEvent
	databaseEvents []coreExecDatabaseMetricEvent
}

type coreExecDatabaseMetricEvent struct {
	method   string
	queries  int64
	duration time.Duration
	errors   int64
}

func (m *captureCoreExecMetrics) CoreExecGRPCCall(side, operation, outcome string, _ time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, coreExecMetricEvent{side: side, operation: operation, outcome: outcome})
}

func (m *captureCoreExecMetrics) CoreExecPendingAdmissionRejected(transport, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, coreExecMetricEvent{operation: "pending_admission_rejected", transport: transport, reason: reason})
}

func (m *captureCoreExecMetrics) RPCDatabase(method string, queries int64, duration time.Duration, databaseErrors int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.databaseEvents = append(m.databaseEvents, coreExecDatabaseMetricEvent{
		method: method, queries: queries, duration: duration, errors: databaseErrors,
	})
}

func (m *captureCoreExecMetrics) database(method string) (int64, time.Duration, int64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, event := range m.databaseEvents {
		if event.method == method {
			return event.queries, event.duration, event.errors, true
		}
	}
	return 0, 0, 0, false
}

func (m *captureCoreExecMetrics) has(side, operation, outcome string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, event := range m.events {
		if event.side == side && event.operation == operation && event.outcome == outcome {
			return true
		}
	}
	return false
}

func (m *captureCoreExecMetrics) hasPendingReject(transport, reason string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, event := range m.events {
		if event.operation == "pending_admission_rejected" && event.transport == transport && event.reason == reason {
			return true
		}
	}
	return false
}

func (m *captureCoreExecMetrics) snapshot() []coreExecMetricEvent {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]coreExecMetricEvent(nil), m.events...)
}

func isDeadlineError(err error) bool {
	if err == nil {
		return false
	}
	if status.Code(err) == codes.DeadlineExceeded {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "deadline")
}

func newTCPGRPCServer(t *testing.T, core Handler, token string) (string, func()) {
	t.Helper()
	return newTCPGRPCServerWithHealthStatus(t, core, token, healthpb.HealthCheckResponse_SERVING)
}

func newTCPGRPCServerWithHealthStatus(t *testing.T, core Handler, token string, healthStatus healthpb.HealthCheckResponse_ServingStatus) (string, func()) {
	t.Helper()
	addr, _, cleanup := newTCPGRPCServerWithMutableHealth(t, core, token, healthStatus)
	return addr, cleanup
}

func newTCPGRPCServerWithMutableHealth(t *testing.T, core Handler, token string, healthStatus healthpb.HealthCheckResponse_ServingStatus) (string, *health.Server, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return newTCPGRPCServerOnListener(t, ln, core, token, healthStatus)
}

func newTCPGRPCServerOnListener(t *testing.T, ln net.Listener, core Handler, token string, healthStatus healthpb.HealthCheckResponse_ServingStatus) (string, *health.Server, func()) {
	t.Helper()
	return newTCPGRPCServerOnListenerWithExtraInterceptors(t, ln, core, token, healthStatus)
}

func newTCPGRPCServerWithExtraInterceptors(t testing.TB, core Handler, token string, healthStatus healthpb.HealthCheckResponse_ServingStatus, extraInterceptors ...grpc.UnaryServerInterceptor) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr, _, cleanup := newTCPGRPCServerOnListenerWithExtraInterceptors(t, ln, core, token, healthStatus, extraInterceptors...)
	return addr, cleanup
}

func newTCPGRPCServerOnListenerWithExtraInterceptors(t testing.TB, ln net.Listener, core Handler, token string, healthStatus healthpb.HealthCheckResponse_ServingStatus, extraInterceptors ...grpc.UnaryServerInterceptor) (string, *health.Server, func()) {
	t.Helper()
	interceptors := []grpc.UnaryServerInterceptor{
		grpcRequestIDUnaryServerInterceptor(zaptest.NewLogger(t)),
		grpcBearerUnaryServerInterceptor(token),
	}
	interceptors = append(interceptors, extraInterceptors...)
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(interceptors...),
		grpc.MaxRecvMsgSize(defaultMaxGRPCMessageSize),
		grpc.MaxSendMsgSize(defaultMaxGRPCMessageSize),
	)
	coreexecpb.RegisterCoreExecServiceServer(srv, newCoreExecGRPCServer(core))
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus(coreexecpb.CoreExecService_ServiceDesc.ServiceName, healthStatus)
	healthpb.RegisterHealthServer(srv, healthSrv)
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	return ln.Addr().String(), healthSrv, func() {
		srv.Stop()
		_ = ln.Close()
		select {
		case err := <-serveErr:
			if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				t.Errorf("grpc server: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("grpc server did not stop")
		}
	}
}

func newLoopbackTCPListenersOnSharedPort(t *testing.T, hosts []string) ([]net.Listener, string) {
	t.Helper()
	if len(hosts) == 0 {
		t.Fatal("hosts must not be empty")
	}
	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		first, err := net.Listen("tcp", net.JoinHostPort(hosts[0], "0"))
		if err != nil {
			t.Fatalf("listen on %s:0: %v", hosts[0], err)
		}
		_, port, err := net.SplitHostPort(first.Addr().String())
		if err != nil {
			_ = first.Close()
			t.Fatalf("split listener addr %q: %v", first.Addr().String(), err)
		}
		listeners := []net.Listener{first}
		for _, host := range hosts[1:] {
			ln, err := net.Listen("tcp", net.JoinHostPort(host, port))
			if err != nil {
				lastErr = err
				closeTCPListeners(listeners)
				listeners = nil
				break
			}
			listeners = append(listeners, ln)
		}
		if len(listeners) == len(hosts) {
			return listeners, port
		}
	}
	t.Skipf("shared loopback port unavailable for %v: %v", hosts, lastErr)
	return nil, ""
}

func closeTCPListeners(listeners []net.Listener) {
	for _, ln := range listeners {
		_ = ln.Close()
	}
}

func newCoreExecTestDNSServer(t *testing.T, ips []net.IP) string {
	t.Helper()
	udpAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(udpConn.LocalAddr().String())
	if err != nil {
		_ = udpConn.Close()
		t.Fatalf("split dns udp addr %q: %v", udpConn.LocalAddr().String(), err)
	}
	tcpLn, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", port))
	if err != nil {
		_ = udpConn.Close()
		t.Fatalf("listen dns tcp %s: %v", port, err)
	}
	t.Cleanup(func() {
		_ = udpConn.Close()
		_ = tcpLn.Close()
	})
	go serveCoreExecTestDNSUDP(udpConn, ips)
	go serveCoreExecTestDNSTCP(tcpLn, ips)
	return net.JoinHostPort("127.0.0.1", port)
}

func serveCoreExecTestDNSUDP(conn *net.UDPConn, ips []net.IP) {
	buf := make([]byte, 1500)
	for {
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		resp, err := buildCoreExecTestDNSResponse(buf[:n], ips)
		if err != nil {
			continue
		}
		_, _ = conn.WriteToUDP(resp, addr)
	}
}

func serveCoreExecTestDNSTCP(ln net.Listener, ips []net.IP) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go handleCoreExecTestDNSTCPConn(conn, ips)
	}
}

func handleCoreExecTestDNSTCPConn(conn net.Conn, ips []net.IP) {
	defer func() { _ = conn.Close() }()
	for {
		var prefix [2]byte
		if _, err := io.ReadFull(conn, prefix[:]); err != nil {
			return
		}
		length := int(binary.BigEndian.Uint16(prefix[:]))
		if length == 0 {
			return
		}
		query := make([]byte, length)
		if _, err := io.ReadFull(conn, query); err != nil {
			return
		}
		resp, err := buildCoreExecTestDNSResponse(query, ips)
		if err != nil {
			return
		}
		if len(resp) > 0xffff {
			return
		}
		var outPrefix [2]byte
		binary.BigEndian.PutUint16(outPrefix[:], uint16(len(resp)))
		if _, err := conn.Write(append(outPrefix[:], resp...)); err != nil {
			return
		}
	}
}

func buildCoreExecTestDNSResponse(query []byte, ips []net.IP) ([]byte, error) {
	var parser dnsmessage.Parser
	header, err := parser.Start(query)
	if err != nil {
		return nil, err
	}
	questions := make([]dnsmessage.Question, 0, 1)
	for {
		question, err := parser.Question()
		if err == dnsmessage.ErrSectionDone {
			break
		}
		if err != nil {
			return nil, err
		}
		questions = append(questions, question)
	}
	builder := dnsmessage.NewBuilder(nil, dnsmessage.Header{
		ID:                 header.ID,
		Response:           true,
		Authoritative:      true,
		RecursionAvailable: true,
		RCode:              dnsmessage.RCodeSuccess,
	})
	builder.EnableCompression()
	if err := builder.StartQuestions(); err != nil {
		return nil, err
	}
	for _, question := range questions {
		if err := builder.Question(question); err != nil {
			return nil, err
		}
	}
	if err := builder.StartAnswers(); err != nil {
		return nil, err
	}
	for _, question := range questions {
		if question.Type != dnsmessage.TypeA {
			continue
		}
		for _, ip := range ips {
			a, ok := dnsAResource(ip)
			if !ok {
				continue
			}
			if err := builder.AResource(dnsmessage.ResourceHeader{
				Name:  question.Name,
				Type:  dnsmessage.TypeA,
				Class: dnsmessage.ClassINET,
				TTL:   1,
			}, a); err != nil {
				return nil, err
			}
		}
	}
	return builder.Finish()
}

func dnsAResource(ip net.IP) (dnsmessage.AResource, bool) {
	ip4 := ip.To4()
	if ip4 == nil {
		return dnsmessage.AResource{}, false
	}
	var a [4]byte
	copy(a[:], ip4)
	return dnsmessage.AResource{A: a}, true
}

type coreExecTLSTestFiles struct {
	caCert     string
	serverCert string
	serverKey  string
	clientCert string
	clientKey  string
}

func writeCoreExecTLSTestFiles(t testing.TB) coreExecTLSTestFiles {
	t.Helper()
	dir := t.TempDir()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          randomSerial(t),
		Subject:               pkix.Name{CommonName: "coreexec test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	files := coreExecTLSTestFiles{
		caCert:     filepath.Join(dir, "ca.pem"),
		serverCert: filepath.Join(dir, "server.pem"),
		serverKey:  filepath.Join(dir, "server-key.pem"),
		clientCert: filepath.Join(dir, "client.pem"),
		clientKey:  filepath.Join(dir, "client-key.pem"),
	}
	writeCertPEM(t, files.caCert, caDER)
	writeSignedCertPair(t, files.serverCert, files.serverKey, caTemplate, caKey, &x509.Certificate{
		SerialNumber: randomSerial(t),
		Subject:      pkix.Name{CommonName: "core.test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"core.test"},
	})
	writeSignedCertPair(t, files.clientCert, files.clientKey, caTemplate, caKey, &x509.Certificate{
		SerialNumber: randomSerial(t),
		Subject:      pkix.Name{CommonName: "edge.test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	return files
}

func writeSignedCertPair(t testing.TB, certPath, keyPath string, caTemplate *x509.Certificate, caKey *rsa.PrivateKey, template *x509.Certificate) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, caTemplate, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	writeCertPEM(t, certPath, certDER)
	writeKeyPEM(t, keyPath, key)
}

func randomSerial(t testing.TB) *big.Int {
	t.Helper()
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}
	return serial
}

func writeCertPEM(t testing.TB, path string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeKeyPEM(t testing.TB, path string, key *rsa.PrivateKey) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0o600); err != nil {
		t.Fatal(err)
	}
}
