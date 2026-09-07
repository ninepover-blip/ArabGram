// Command telesrv-healthcheck provides small container readiness probes for
// telesrv TCP listeners and authenticated gRPC health services.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	"telesrv/internal/store/redisstore"
)

const defaultProbeTimeout = 5 * time.Second

type grpcOptions struct {
	address  string
	service  string
	tokenEnv string
	timeout  time.Duration
}

type tcpOptions struct {
	address string
	timeout time.Duration
}

type postgresOptions struct {
	dsnEnv  string
	timeout time.Duration
}

type redisOptions struct {
	address     string
	passwordEnv string
	db          int
	timeout     time.Duration
}

type allOptions struct {
	grpc     []grpcOptions
	tcp      []tcpOptions
	postgres []postgresOptions
	redis    []redisOptions
}

type repeatedFlag []string

func (values *repeatedFlag) String() string {
	return strings.Join(*values, ",")
}

func (values *repeatedFlag) Set(value string) error {
	*values = append(*values, value)
	return nil
}

func main() {
	os.Exit(run(os.Args[1:], os.Getenv, os.Stdout, os.Stderr))
}

func run(args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		writeUsage(stderr)
		return 2
	}

	var err error
	switch args[0] {
	case "grpc":
		var opts grpcOptions
		opts, err = parseGRPCOptions(args[1:])
		if errors.Is(err, flag.ErrHelp) {
			writeGRPCUsage(stdout)
			return 0
		}
		if err == nil {
			var token string
			token, err = tokenFromEnvironment(getenv, opts.tokenEnv)
			if err == nil {
				err = checkGRPC(context.Background(), opts, token)
			}
		}
	case "tcp":
		var opts tcpOptions
		opts, err = parseTCPOptions(args[1:])
		if errors.Is(err, flag.ErrHelp) {
			writeTCPUsage(stdout)
			return 0
		}
		if err == nil {
			err = checkTCP(context.Background(), opts)
		}
	case "all":
		var opts allOptions
		opts, err = parseAllOptions(args[1:])
		if errors.Is(err, flag.ErrHelp) {
			writeAllUsage(stdout)
			return 0
		}
		if err == nil {
			err = checkAll(context.Background(), opts, getenv)
		}
	case "help", "-h", "--help":
		writeUsage(stdout)
		return 0
	default:
		fmt.Fprintf(stderr, "telesrv-healthcheck: unknown subcommand %q\n", args[0])
		writeUsage(stderr)
		return 2
	}

	if err != nil {
		fmt.Fprintf(stderr, "telesrv-healthcheck: %v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, "ok")
	return 0
}

func parseGRPCOptions(args []string) (grpcOptions, error) {
	opts := grpcOptions{timeout: defaultProbeTimeout}
	fs := flag.NewFlagSet("grpc", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&opts.address, "address", "", "gRPC host:port")
	fs.StringVar(&opts.service, "service", "", "fully-qualified gRPC health service")
	fs.StringVar(&opts.tokenEnv, "token-env", "", "environment variable containing the bearer token")
	fs.DurationVar(&opts.timeout, "timeout", defaultProbeTimeout, "probe timeout")
	if err := fs.Parse(args); err != nil {
		return grpcOptions{}, err
	}
	if fs.NArg() != 0 {
		return grpcOptions{}, fmt.Errorf("grpc: unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	opts.address = strings.TrimSpace(opts.address)
	opts.service = strings.TrimSpace(opts.service)
	opts.tokenEnv = strings.TrimSpace(opts.tokenEnv)
	if opts.address == "" {
		return grpcOptions{}, errors.New("grpc: --address is required")
	}
	if opts.service == "" {
		return grpcOptions{}, errors.New("grpc: --service is required")
	}
	if opts.tokenEnv == "" {
		return grpcOptions{}, errors.New("grpc: --token-env is required")
	}
	if strings.ContainsAny(opts.tokenEnv, "=\x00\r\n") {
		return grpcOptions{}, errors.New("grpc: --token-env is invalid")
	}
	if opts.timeout <= 0 {
		return grpcOptions{}, errors.New("grpc: --timeout must be greater than zero")
	}
	return opts, nil
}

func parseTCPOptions(args []string) (tcpOptions, error) {
	opts := tcpOptions{timeout: defaultProbeTimeout}
	fs := flag.NewFlagSet("tcp", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&opts.address, "address", "", "TCP host:port")
	fs.DurationVar(&opts.timeout, "timeout", defaultProbeTimeout, "probe timeout")
	if err := fs.Parse(args); err != nil {
		return tcpOptions{}, err
	}
	if fs.NArg() != 0 {
		return tcpOptions{}, fmt.Errorf("tcp: unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	opts.address = strings.TrimSpace(opts.address)
	if opts.address == "" {
		return tcpOptions{}, errors.New("tcp: --address is required")
	}
	if opts.timeout <= 0 {
		return tcpOptions{}, errors.New("tcp: --timeout must be greater than zero")
	}
	return opts, nil
}

func parseAllOptions(args []string) (allOptions, error) {
	var grpcSpecs, tcpAddresses, postgresDSNEnvs, redisSpecs repeatedFlag
	timeout := defaultProbeTimeout
	fs := flag.NewFlagSet("all", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Var(&grpcSpecs, "grpc", "repeatable ADDRESS,SERVICE,TOKEN_ENV gRPC probe")
	fs.Var(&tcpAddresses, "tcp", "repeatable TCP host:port probe")
	fs.Var(&postgresDSNEnvs, "postgres-dsn-env", "repeatable environment variable containing a PostgreSQL DSN")
	fs.Var(&redisSpecs, "redis", "repeatable ADDRESS,PASSWORD_ENV,DB Redis probe")
	fs.DurationVar(&timeout, "timeout", defaultProbeTimeout, "timeout for each probe")
	if err := fs.Parse(args); err != nil {
		return allOptions{}, err
	}
	if fs.NArg() != 0 {
		return allOptions{}, fmt.Errorf("all: unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}
	if timeout <= 0 {
		return allOptions{}, errors.New("all: --timeout must be greater than zero")
	}
	if len(grpcSpecs) == 0 && len(tcpAddresses) == 0 && len(postgresDSNEnvs) == 0 && len(redisSpecs) == 0 {
		return allOptions{}, errors.New("all: at least one probe is required")
	}

	opts := allOptions{
		grpc:     make([]grpcOptions, 0, len(grpcSpecs)),
		tcp:      make([]tcpOptions, 0, len(tcpAddresses)),
		postgres: make([]postgresOptions, 0, len(postgresDSNEnvs)),
		redis:    make([]redisOptions, 0, len(redisSpecs)),
	}
	for _, spec := range grpcSpecs {
		parts := strings.Split(spec, ",")
		if len(parts) != 3 {
			return allOptions{}, fmt.Errorf("all: invalid --grpc %q: want ADDRESS,SERVICE,TOKEN_ENV", spec)
		}
		probe := grpcOptions{
			address:  strings.TrimSpace(parts[0]),
			service:  strings.TrimSpace(parts[1]),
			tokenEnv: strings.TrimSpace(parts[2]),
			timeout:  timeout,
		}
		switch {
		case probe.address == "":
			return allOptions{}, fmt.Errorf("all: invalid --grpc %q: address is empty", spec)
		case probe.service == "":
			return allOptions{}, fmt.Errorf("all: invalid --grpc %q: service is empty", spec)
		case probe.tokenEnv == "" || strings.ContainsAny(probe.tokenEnv, "=\x00\r\n"):
			return allOptions{}, fmt.Errorf("all: invalid --grpc %q: token environment variable is invalid", spec)
		}
		opts.grpc = append(opts.grpc, probe)
	}
	for _, address := range tcpAddresses {
		address = strings.TrimSpace(address)
		if address == "" {
			return allOptions{}, errors.New("all: --tcp address is empty")
		}
		opts.tcp = append(opts.tcp, tcpOptions{address: address, timeout: timeout})
	}
	for _, dsnEnv := range postgresDSNEnvs {
		dsnEnv = strings.TrimSpace(dsnEnv)
		if dsnEnv == "" || strings.ContainsAny(dsnEnv, "=\x00\r\n") {
			return allOptions{}, fmt.Errorf("all: invalid --postgres-dsn-env %q", dsnEnv)
		}
		opts.postgres = append(opts.postgres, postgresOptions{dsnEnv: dsnEnv, timeout: timeout})
	}
	for _, spec := range redisSpecs {
		parts := strings.Split(spec, ",")
		if len(parts) != 3 {
			return allOptions{}, fmt.Errorf("all: invalid --redis %q: want ADDRESS,PASSWORD_ENV,DB", spec)
		}
		probe := redisOptions{
			address:     strings.TrimSpace(parts[0]),
			passwordEnv: strings.TrimSpace(parts[1]),
			timeout:     timeout,
		}
		db, err := strconv.Atoi(strings.TrimSpace(parts[2]))
		if err != nil || db < 0 {
			return allOptions{}, fmt.Errorf("all: invalid --redis %q: DB must be a non-negative integer", spec)
		}
		probe.db = db
		switch {
		case probe.address == "":
			return allOptions{}, fmt.Errorf("all: invalid --redis %q: address is empty", spec)
		case probe.passwordEnv == "" || strings.ContainsAny(probe.passwordEnv, "=\x00\r\n"):
			return allOptions{}, fmt.Errorf("all: invalid --redis %q: password environment variable is invalid", spec)
		}
		opts.redis = append(opts.redis, probe)
	}
	return opts, nil
}

func tokenFromEnvironment(getenv func(string) string, name string) (string, error) {
	token := strings.TrimSpace(getenv(name))
	switch {
	case token == "":
		return "", fmt.Errorf("environment variable %s is empty or unset", name)
	case strings.ContainsAny(token, "\r\n"):
		return "", fmt.Errorf("environment variable %s contains a newline", name)
	default:
		return token, nil
	}
}

func checkAll(parent context.Context, opts allOptions, getenv func(string) string) error {
	results := make([]error, len(opts.grpc)+len(opts.tcp)+len(opts.postgres)+len(opts.redis))
	var wait sync.WaitGroup
	wait.Add(len(results))

	for index, probe := range opts.grpc {
		go func() {
			defer wait.Done()
			token, err := tokenFromEnvironment(getenv, probe.tokenEnv)
			if err == nil {
				err = checkGRPC(parent, probe, token)
			}
			results[index] = err
		}()
	}
	for tcpIndex, probe := range opts.tcp {
		index := len(opts.grpc) + tcpIndex
		go func() {
			defer wait.Done()
			results[index] = checkTCP(parent, probe)
		}()
	}
	postgresOffset := len(opts.grpc) + len(opts.tcp)
	for postgresIndex, probe := range opts.postgres {
		index := postgresOffset + postgresIndex
		go func() {
			defer wait.Done()
			results[index] = checkPostgres(parent, probe, getenv)
		}()
	}
	redisOffset := postgresOffset + len(opts.postgres)
	for redisIndex, probe := range opts.redis {
		index := redisOffset + redisIndex
		go func() {
			defer wait.Done()
			results[index] = checkRedis(parent, probe, getenv)
		}()
	}
	wait.Wait()
	return errors.Join(results...)
}

func checkPostgres(parent context.Context, opts postgresOptions, getenv func(string) string) error {
	dsn, err := tokenFromEnvironment(getenv, opts.dsnEnv)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, opts.timeout)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("postgres connect using %s: %w", opts.dsnEnv, err)
	}
	defer conn.Close(context.Background())
	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("postgres ping using %s: %w", opts.dsnEnv, err)
	}
	return nil
}

func checkRedis(parent context.Context, opts redisOptions, getenv func(string) string) error {
	password, err := tokenFromEnvironment(getenv, opts.passwordEnv)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, opts.timeout)
	defer cancel()
	client, err := redisstore.Open(ctx, opts.address, password, opts.db)
	if err != nil {
		return err
	}
	defer client.Close()
	return nil
}

func checkGRPC(parent context.Context, opts grpcOptions, token string) error {
	ctx, cancel := context.WithTimeout(parent, opts.timeout)
	defer cancel()

	conn, err := grpc.NewClient(opts.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("grpc connect %s: %w", opts.address, err)
	}
	defer func() { _ = conn.Close() }()

	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	response, err := grpc_health_v1.NewHealthClient(conn).Check(ctx, &grpc_health_v1.HealthCheckRequest{
		Service: opts.service,
	})
	if err != nil {
		return fmt.Errorf("grpc health %s at %s: %w", opts.service, opts.address, err)
	}
	if response.GetStatus() != grpc_health_v1.HealthCheckResponse_SERVING {
		return fmt.Errorf("grpc health %s at %s returned %s", opts.service, opts.address, response.GetStatus())
	}
	return nil
}

func checkTCP(parent context.Context, opts tcpOptions) error {
	ctx, cancel := context.WithTimeout(parent, opts.timeout)
	defer cancel()

	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", opts.address)
	if err != nil {
		return fmt.Errorf("tcp connect %s: %w", opts.address, err)
	}
	if err := conn.Close(); err != nil {
		return fmt.Errorf("tcp close %s: %w", opts.address, err)
	}
	return nil
}

func writeUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: telesrv-healthcheck <grpc|tcp|all> [options]")
	fmt.Fprintln(w, "run 'telesrv-healthcheck <grpc|tcp|all> --help' for subcommand options")
}

func writeGRPCUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: telesrv-healthcheck grpc --address HOST:PORT --service NAME --token-env ENV [--timeout 5s]")
}

func writeTCPUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: telesrv-healthcheck tcp --address HOST:PORT [--timeout 5s]")
}

func writeAllUsage(w io.Writer) {
	fmt.Fprintln(w, "usage: telesrv-healthcheck all [--tcp HOST:PORT] [--grpc ADDRESS,SERVICE,TOKEN_ENV] [--postgres-dsn-env ENV] [--redis ADDRESS,PASSWORD_ENV,DB] [--timeout 5s]")
}
