package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/iamxvbaba/td/bin"
	"github.com/iamxvbaba/td/clock"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tlprofile"
	"go.uber.org/zap"

	"telesrv/internal/coreexec"
	"telesrv/internal/coreexec/coreexecpb"
	"telesrv/internal/rpc"
)

func main() {
	targetsRaw := flag.String("targets", "", "CoreExec gRPC targets; static accepts comma-separated host:port, dns accepts one host:port or dns:// authority URI")
	resolver := flag.String("resolver", coreexec.GRPCResolverStatic, "CoreExec gRPC resolver: static or dns")
	token := flag.String("token", "", "CoreExec bearer token")
	count := flag.Int("count", 8, "successful dispatches required")
	duration := flag.Duration("duration", 0, "minimum dispatch observation window; when set, dispatches continue until both count and duration are satisfied")
	interval := flag.Duration("interval", 0, "delay between successful dispatch attempts")
	expectInstances := flag.Int("expect-instances", 0, "minimum distinct CoreExec GetInfo instance_id values expected")
	timeout := flag.Duration("timeout", 10*time.Second, "overall probe timeout")
	caFile := flag.String("tls-ca-file", "", "CoreExec gRPC root CA file")
	serverName := flag.String("tls-server-name", "", "CoreExec gRPC TLS server name")
	certFile := flag.String("tls-client-cert-file", "", "CoreExec gRPC client certificate file")
	keyFile := flag.String("tls-client-key-file", "", "CoreExec gRPC client key file")
	flag.Parse()

	if strings.TrimSpace(*targetsRaw) == "" {
		fatalf("-targets is required")
	}
	if strings.TrimSpace(*token) == "" {
		fatalf("-token is required")
	}
	if *count <= 0 {
		fatalf("-count must be positive")
	}
	if *duration < 0 {
		fatalf("-duration must not be negative")
	}
	if *interval < 0 {
		fatalf("-interval must not be negative")
	}
	if *expectInstances < 0 {
		fatalf("-expect-instances must be non-negative")
	}
	if *timeout <= 0 {
		fatalf("-timeout must be positive")
	}
	durationGoal := *duration
	intervalDelay := *interval
	targets, err := coreexec.ParseGRPCTargetsForResolver(*targetsRaw, *resolver)
	if err != nil {
		fatalf("parse targets: %v", err)
	}

	edgeRouter := rpc.New(rpc.Config{DC: 2, IP: "127.0.0.1", Port: 2398}, rpc.Deps{}, zap.NewNop(), clock.System)
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	remote, conn, err := coreexec.DialGRPCRemote(ctx, edgeRouter, coreexec.GRPCClientConfig{
		Targets:        targets,
		ResolverKind:   strings.TrimSpace(*resolver),
		Token:          *token,
		RequestTimeout: 2 * time.Second,
		TLSCAFile:      *caFile,
		TLSServerName:  *serverName,
		TLSCertFile:    *certFile,
		TLSKeyFile:     *keyFile,
	})
	if err != nil {
		fatalf("dial coreexec: %v", err)
	}
	defer func() { _ = conn.Close() }()

	successes := 0
	attempts := 0
	started := time.Now()
	deadline := started.Add(durationGoal)
	var lastErr error
	for ctx.Err() == nil {
		attempts++
		err := dispatchHelpGetConfig(ctx, remote, int64(10_000+attempts))
		if err != nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		successes++
		if successes >= *count && (durationGoal == 0 || !time.Now().Before(deadline)) {
			break
		}
		if intervalDelay > 0 {
			if err := sleepContext(ctx, intervalDelay); err != nil {
				break
			}
		}
	}
	if successes < *count {
		if lastErr != nil {
			fatalf("coreexec probe reached %d/%d successful dispatches after %d attempts: %v", successes, *count, attempts, lastErr)
		}
		fatalf("coreexec probe reached %d/%d successful dispatches after %d attempts: %v", successes, *count, attempts, ctx.Err())
	}
	observed := time.Since(started)
	if durationGoal > 0 && observed < durationGoal {
		if lastErr != nil {
			fatalf("coreexec probe observed %s/%s after %d successful dispatches and %d attempts: %v", observed.Round(time.Millisecond), durationGoal.String(), successes, attempts, lastErr)
		}
		fatalf("coreexec probe observed %s/%s after %d successful dispatches and %d attempts: %v", observed.Round(time.Millisecond), durationGoal.String(), successes, attempts, ctx.Err())
	}
	instances, err := probeCoreExecInstances(ctx, coreexecpb.NewCoreExecServiceClient(conn), maxInt(*count, *expectInstances*8), *expectInstances)
	if err != nil {
		fatalf("coreexec instance probe: %v", err)
	}
	fmt.Printf("coreexec probe ok: successes=%d attempts=%d duration=%s targets=%s resolver=%s instances=%s\n", successes, attempts, observed.Round(time.Millisecond), strings.Join(targets, ","), strings.TrimSpace(*resolver), strings.Join(instances, ","))
}

func dispatchHelpGetConfig(ctx context.Context, remote *coreexec.GRPCRemote, msgID int64) error {
	var body bin.Buffer
	if err := tlprofile.EncodeObject(tlprofile.Profile225, &tg.HelpGetConfigRequest{}, &body); err != nil {
		return err
	}
	admitted, err := remote.AdmitLayer(tlprofile.Profile225, &body, tlprofile.Limits{})
	if err != nil {
		return err
	}
	result, method, err := remote.DispatchAdmitted(ctx, [8]byte{1, 2, 3}, 10, msgID, uint64(msgID+1000), admitted)
	if err != nil {
		return err
	}
	if method != "help.getConfig" || result == nil {
		return fmt.Errorf("dispatch result = method:%q result:%T", method, result)
	}
	return nil
}

func probeCoreExecInstances(ctx context.Context, client coreexecpb.CoreExecServiceClient, attempts, expect int) ([]string, error) {
	if expect <= 0 {
		return nil, nil
	}
	if attempts < expect {
		attempts = expect
	}
	seen := make(map[string]struct{}, expect)
	var lastErr error
	for i := 0; i < attempts && len(seen) < expect && ctx.Err() == nil; i++ {
		res, err := client.GetInfo(ctx, &coreexecpb.CoreExecInfoRequest{
			ProtocolVersion:             1,
			MinSupportedProtocolVersion: 1,
			Capabilities:                []string{"tl-wire-passthrough"},
		})
		if err != nil {
			lastErr = err
			time.Sleep(50 * time.Millisecond)
			continue
		}
		instanceID := strings.TrimSpace(res.GetInstanceId())
		if instanceID != "" {
			seen[instanceID] = struct{}{}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(seen) < expect {
		instances := sortedKeys(seen)
		if lastErr != nil {
			return instances, fmt.Errorf("saw %d/%d distinct instance_id values after %d attempts: %s: %w", len(seen), expect, attempts, strings.Join(instances, ","), lastErr)
		}
		if ctx.Err() != nil {
			return instances, fmt.Errorf("saw %d/%d distinct instance_id values after %d attempts: %s: %w", len(seen), expect, attempts, strings.Join(instances, ","), ctx.Err())
		}
		return instances, fmt.Errorf("saw %d/%d distinct instance_id values after %d attempts: %s", len(seen), expect, attempts, strings.Join(instances, ","))
	}
	return sortedKeys(seen), nil
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
