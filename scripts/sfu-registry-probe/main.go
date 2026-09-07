package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"telesrv/internal/sfu"
	"telesrv/internal/store/redisstore"
)

func main() {
	redisAddr := flag.String("redis-addr", "", "Redis address")
	redisPassword := flag.String("redis-password", "", "Redis password")
	redisDB := flag.Int("redis-db", 0, "Redis DB")
	expectRaw := flag.String("expect-instances", "", "comma-separated SFU instance IDs that must be present")
	controlToken := flag.String("control-token", "", "SFU control bearer token for gRPC health checks")
	controlTLSCAFile := flag.String("control-grpc-tls-ca-file", "", "Root CA bundle for SFU gRPC control health checks")
	controlTLSServerName := flag.String("control-grpc-tls-server-name", "", "Server name for SFU gRPC control certificate validation")
	controlTLSClientCertFile := flag.String("control-grpc-tls-client-cert-file", "", "Client certificate for SFU gRPC control mTLS health checks")
	controlTLSClientKeyFile := flag.String("control-grpc-tls-client-key-file", "", "Client private key for SFU gRPC control mTLS health checks")
	requireControlHealth := flag.Bool("require-control-health", false, "also check each expected instance control endpoint")
	expectControlFailure := flag.Bool("expect-control-failure", false, "expect optional control health checks to fail")
	timeout := flag.Duration("timeout", 10*time.Second, "overall probe timeout")
	flag.Parse()

	if strings.TrimSpace(*redisAddr) == "" {
		fatalf("-redis-addr is required")
	}
	expect := splitCSV(*expectRaw)
	if len(expect) == 0 {
		fatalf("-expect-instances is required")
	}
	if *timeout <= 0 {
		fatalf("-timeout must be positive")
	}
	if *requireControlHealth && strings.TrimSpace(*controlToken) == "" {
		fatalf("-control-token is required when -require-control-health is set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	client, err := redisstore.Open(ctx, *redisAddr, *redisPassword, *redisDB)
	if err != nil {
		fatalf("connect redis: %v", err)
	}
	defer func() { _ = client.Close() }()

	registry := sfu.NewRedisInstanceRegistry(client)
	var last []sfu.InstanceRecord
	for ctx.Err() == nil {
		records, err := registry.List(ctx)
		if err != nil {
			fatalf("list sfu instances: %v", err)
		}
		last = records
		if missing := missingInstances(records, expect); len(missing) == 0 {
			controlSummary := ""
			if *requireControlHealth {
				cfg := controlConfig{
					token:             *controlToken,
					tlsCAFile:         *controlTLSCAFile,
					tlsServerName:     *controlTLSServerName,
					tlsClientCertFile: *controlTLSClientCertFile,
					tlsClientKeyFile:  *controlTLSClientKeyFile,
				}
				if ok := checkControlHealth(ctx, records, expect, cfg, *expectControlFailure); !ok {
					time.Sleep(200 * time.Millisecond)
					continue
				}
				controlSummary = fmt.Sprintf(" control_plane=grpc control_failure_expected=%v", *expectControlFailure)
			}
			fmt.Printf("sfu registry probe ok: redis=%s db=%d instances=%s%s\n", *redisAddr, *redisDB, strings.Join(instanceIDs(records, expect), ","), controlSummary)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	fatalf("sfu registry probe missing instances %s; last_records=%s", strings.Join(missingInstances(last, expect), ","), describeRecords(last))
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		value := strings.TrimSpace(part)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func missingInstances(records []sfu.InstanceRecord, expect []string) []string {
	seen := make(map[string]struct{}, len(records))
	for _, record := range records {
		seen[strings.TrimSpace(record.InstanceID)] = struct{}{}
	}
	var missing []string
	for _, id := range expect {
		if _, ok := seen[id]; !ok {
			missing = append(missing, id)
		}
	}
	return missing
}

func instanceIDs(records []sfu.InstanceRecord, expect []string) []string {
	want := make(map[string]struct{}, len(expect))
	for _, id := range expect {
		want[id] = struct{}{}
	}
	out := make([]string, 0, len(expect))
	for _, record := range records {
		id := strings.TrimSpace(record.InstanceID)
		if _, ok := want[id]; ok {
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

type remoteControl interface {
	sfu.InstanceHealthChecker
}

type controlConfig struct {
	token             string
	tlsCAFile         string
	tlsServerName     string
	tlsClientCertFile string
	tlsClientKeyFile  string
}

func newRemoteControl(cfg controlConfig) (remoteControl, func() error, error) {
	remote, err := sfu.NewGRPCRemoteService(sfu.GRPCRemoteConfig{
		Token:         cfg.token,
		TLSCAFile:     cfg.tlsCAFile,
		TLSServerName: cfg.tlsServerName,
		TLSCertFile:   cfg.tlsClientCertFile,
		TLSKeyFile:    cfg.tlsClientKeyFile,
	})
	if err != nil {
		return nil, nil, err
	}
	return remote, remote.Close, nil
}

func checkControlHealth(ctx context.Context, records []sfu.InstanceRecord, expect []string, cfg controlConfig, expectFailure bool) bool {
	remote, closeRemote, err := newRemoteControl(cfg)
	if err != nil {
		fatalf("init control health client: %v", err)
	}
	defer func() { _ = closeRemote() }()
	want := make(map[string]struct{}, len(expect))
	for _, id := range expect {
		want[id] = struct{}{}
	}
	for _, record := range records {
		if _, ok := want[strings.TrimSpace(record.InstanceID)]; !ok {
			continue
		}
		err := remote.CheckInstance(ctx, record)
		if expectFailure {
			if err == nil {
				fatalf("control health for %s unexpectedly succeeded without expected failure", record.InstanceID)
			}
			continue
		}
		if err != nil {
			return false
		}
	}
	return true
}

func describeRecords(records []sfu.InstanceRecord) string {
	if len(records) == 0 {
		return "<empty>"
	}
	parts := make([]string, 0, len(records))
	for _, record := range records {
		parts = append(parts, fmt.Sprintf("%s(%s,udp=%d,active=%d,max=%d)", record.InstanceID, record.ControlAddr, record.UDPPort, record.ActiveCalls, record.MaxCalls))
	}
	sort.Strings(parts)
	return strings.Join(parts, ";")
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
