package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestParseGRPCOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "valid", args: []string{"--address", "127.0.0.1:2440", "--service", "telesrv.coreexec.v1.CoreExecService", "--token-env", "CORE_TOKEN", "--timeout", "2s"}},
		{name: "missing address", args: []string{"--service", "service", "--token-env", "TOKEN"}, wantErr: "--address is required"},
		{name: "missing service", args: []string{"--address", "127.0.0.1:1", "--token-env", "TOKEN"}, wantErr: "--service is required"},
		{name: "missing token env", args: []string{"--address", "127.0.0.1:1", "--service", "service"}, wantErr: "--token-env is required"},
		{name: "invalid token env", args: []string{"--address", "127.0.0.1:1", "--service", "service", "--token-env", "TOKEN=bad"}, wantErr: "--token-env is invalid"},
		{name: "invalid timeout", args: []string{"--address", "127.0.0.1:1", "--service", "service", "--token-env", "TOKEN", "--timeout", "0s"}, wantErr: "--timeout must be greater than zero"},
		{name: "unexpected argument", args: []string{"--address", "127.0.0.1:1", "--service", "service", "--token-env", "TOKEN", "extra"}, wantErr: "unexpected arguments"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opts, err := parseGRPCOptions(tt.args)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parseGRPCOptions() err = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseGRPCOptions() error = %v", err)
			}
			if opts.address != "127.0.0.1:2440" || opts.service != "telesrv.coreexec.v1.CoreExecService" || opts.tokenEnv != "CORE_TOKEN" || opts.timeout != 2*time.Second {
				t.Fatalf("parseGRPCOptions() = %+v", opts)
			}
		})
	}
}

func TestParseTCPOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "valid", args: []string{"--address", "127.0.0.1:2398", "--timeout", "250ms"}},
		{name: "missing address", args: nil, wantErr: "--address is required"},
		{name: "invalid timeout", args: []string{"--address", "127.0.0.1:2398", "--timeout", "-1s"}, wantErr: "--timeout must be greater than zero"},
		{name: "unexpected argument", args: []string{"--address", "127.0.0.1:2398", "extra"}, wantErr: "unexpected arguments"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			opts, err := parseTCPOptions(tt.args)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("parseTCPOptions() err = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseTCPOptions() error = %v", err)
			}
			if opts.address != "127.0.0.1:2398" || opts.timeout != 250*time.Millisecond {
				t.Fatalf("parseTCPOptions() = %+v", opts)
			}
		})
	}
}

func TestParseAllOptions(t *testing.T) {
	t.Parallel()

	opts, err := parseAllOptions([]string{
		"--timeout", "2s",
		"--tcp", "127.0.0.1:2398",
		"--grpc", "core:2440,telesrv.coreexec.v1.CoreExecService,CORE_TOKEN",
		"--grpc", "file:2520,telesrv.filedata.v1.FileDataService,FILE_TOKEN",
		"--postgres-dsn-env", "POSTGRES_DSN",
		"--redis", "redis:6379,REDIS_PASSWORD,3",
	})
	if err != nil {
		t.Fatalf("parseAllOptions() error = %v", err)
	}
	if len(opts.tcp) != 1 || opts.tcp[0].address != "127.0.0.1:2398" || opts.tcp[0].timeout != 2*time.Second {
		t.Fatalf("parseAllOptions() tcp = %+v", opts.tcp)
	}
	if len(opts.grpc) != 2 || opts.grpc[0].address != "core:2440" || opts.grpc[1].tokenEnv != "FILE_TOKEN" || opts.grpc[1].timeout != 2*time.Second {
		t.Fatalf("parseAllOptions() grpc = %+v", opts.grpc)
	}
	if len(opts.postgres) != 1 || opts.postgres[0].dsnEnv != "POSTGRES_DSN" || opts.postgres[0].timeout != 2*time.Second {
		t.Fatalf("parseAllOptions() postgres = %+v", opts.postgres)
	}
	if len(opts.redis) != 1 || opts.redis[0].address != "redis:6379" || opts.redis[0].passwordEnv != "REDIS_PASSWORD" || opts.redis[0].db != 3 || opts.redis[0].timeout != 2*time.Second {
		t.Fatalf("parseAllOptions() redis = %+v", opts.redis)
	}

	for _, args := range [][]string{
		nil,
		{"--timeout", "0s", "--tcp", "127.0.0.1:1"},
		{"--grpc", "missing-fields"},
		{"--grpc", "core:2440,service,TOKEN=bad"},
		{"--tcp", " "},
		{"--postgres-dsn-env", "BAD=ENV"},
		{"--redis", "missing-fields"},
		{"--redis", "redis:6379,PASSWORD,-1"},
	} {
		if _, err := parseAllOptions(args); err == nil {
			t.Fatalf("parseAllOptions(%q) unexpectedly succeeded", args)
		}
	}
}

func TestRunAllRequiresDatabaseSecrets(t *testing.T) {
	t.Parallel()

	for _, args := range [][]string{
		{"all", "--postgres-dsn-env", "POSTGRES_DSN", "--timeout", "50ms"},
		{"all", "--redis", "127.0.0.1:1,REDIS_PASSWORD,0", "--timeout", "50ms"},
	} {
		var stdout, stderr bytes.Buffer
		exitCode := run(args, func(string) string { return "" }, &stdout, &stderr)
		if exitCode != 1 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "empty or unset") {
			t.Fatalf("run(%q) = code %d stdout %q stderr %q", args, exitCode, stdout.String(), stderr.String())
		}
	}
}

func TestCheckTCP(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	address := listener.Addr().String()

	if err := checkTCP(context.Background(), tcpOptions{address: address, timeout: time.Second}); err != nil {
		_ = listener.Close()
		t.Fatalf("checkTCP() healthy error = %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	if err := checkTCP(context.Background(), tcpOptions{address: address, timeout: 200 * time.Millisecond}); err == nil {
		t.Fatal("checkTCP() succeeded after listener closed")
	}
}

func TestCheckGRPCUsesBearerAndRequiresServing(t *testing.T) {
	const (
		service = "telesrv.test.v1.Health"
		token   = "health-secret"
	)
	address, stop := startAuthenticatedHealthServer(t, service, token)
	defer stop()

	opts := grpcOptions{address: address, service: service, timeout: time.Second}
	if err := checkGRPC(context.Background(), opts, token); err != nil {
		t.Fatalf("checkGRPC() healthy error = %v", err)
	}
	if err := checkGRPC(context.Background(), opts, "wrong-secret"); status.Code(errors.Unwrap(err)) != codes.Unauthenticated {
		t.Fatalf("checkGRPC() wrong token error = %v, want Unauthenticated", err)
	}

	notServing := opts
	notServing.service = service + ".not-serving"
	if err := checkGRPC(context.Background(), notServing, token); err == nil || !strings.Contains(err.Error(), "NOT_SERVING") {
		t.Fatalf("checkGRPC() not-serving error = %v, want NOT_SERVING", err)
	}
}

func TestRunGRPCReadsTokenFromEnvironment(t *testing.T) {
	const (
		service = "telesrv.test.v1.RunHealth"
		token   = "run-health-secret"
	)
	address, stop := startAuthenticatedHealthServer(t, service, token)
	defer stop()

	getenv := func(name string) string {
		if name == "HEALTH_TOKEN" {
			return token
		}
		return ""
	}
	var stdout, stderr bytes.Buffer
	exitCode := run([]string{
		"grpc",
		"--address", address,
		"--service", service,
		"--token-env", "HEALTH_TOKEN",
		"--timeout", "1s",
	}, getenv, &stdout, &stderr)
	if exitCode != 0 || stdout.String() != "ok\n" || stderr.Len() != 0 {
		t.Fatalf("run() = code %d stdout %q stderr %q", exitCode, stdout.String(), stderr.String())
	}
}

func TestRunAllRequiresEveryProbe(t *testing.T) {
	const (
		service = "telesrv.test.v1.AllHealth"
		token   = "all-health-secret"
	)
	grpcAddress, stopGRPC := startAuthenticatedHealthServer(t, service, token)
	defer stopGRPC()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	tcpAddress := listener.Addr().String()
	getenv := func(name string) string {
		if name == "ALL_TOKEN" {
			return token
		}
		return ""
	}
	args := []string{
		"all",
		"--timeout", "1s",
		"--tcp", tcpAddress,
		"--grpc", grpcAddress + "," + service + ",ALL_TOKEN",
	}
	var stdout, stderr bytes.Buffer
	if exitCode := run(args, getenv, &stdout, &stderr); exitCode != 0 || stdout.String() != "ok\n" || stderr.Len() != 0 {
		t.Fatalf("run(all) = code %d stdout %q stderr %q", exitCode, stdout.String(), stderr.String())
	}

	if err := listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	stdout.Reset()
	stderr.Reset()
	if exitCode := run(args, getenv, &stdout, &stderr); exitCode != 1 || !strings.Contains(stderr.String(), "tcp connect") {
		t.Fatalf("run(all unhealthy) = code %d stdout %q stderr %q", exitCode, stdout.String(), stderr.String())
	}
}

func startAuthenticatedHealthServer(t *testing.T, service, token string) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		values := metadata.ValueFromIncomingContext(ctx, "authorization")
		if len(values) != 1 || values[0] != "Bearer "+token {
			return nil, status.Error(codes.Unauthenticated, "invalid bearer token")
		}
		return handler(ctx, req)
	}))
	healthServer := health.NewServer()
	healthServer.SetServingStatus(service, grpc_health_v1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(service+".not-serving", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
	grpc_health_v1.RegisterHealthServer(server, healthServer)
	go func() {
		_ = server.Serve(listener)
	}()
	return listener.Addr().String(), func() {
		server.Stop()
		_ = listener.Close()
	}
}
