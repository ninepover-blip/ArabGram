package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"telesrv/internal/egress"
	"telesrv/internal/egress/egresspb"
)

func main() {
	targetsRaw := flag.String("targets", "", "Egress Delivery gRPC targets; static accepts comma-separated host:port, dns accepts one host:port or dns:// authority URI")
	resolver := flag.String("resolver", "static", "Egress Delivery gRPC resolver: static or dns")
	token := flag.String("token", "", "Egress Delivery bearer token")
	count := flag.Int("count", 8, "successful GetInfo calls required")
	duration := flag.Duration("duration", 0, "minimum observation window; when set, calls continue until both count and duration are satisfied")
	interval := flag.Duration("interval", 0, "delay between successful GetInfo attempts")
	expectInstances := flag.Int("expect-instances", 0, "minimum distinct Egress GetInfo instance_id values expected")
	timeout := flag.Duration("timeout", 10*time.Second, "overall probe timeout")
	caFile := flag.String("tls-ca-file", "", "Egress Delivery gRPC root CA file")
	serverName := flag.String("tls-server-name", "", "Egress Delivery gRPC TLS server name")
	certFile := flag.String("tls-client-cert-file", "", "Egress Delivery gRPC client certificate file")
	keyFile := flag.String("tls-client-key-file", "", "Egress Delivery gRPC client key file")
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

	targets, err := egress.ParseGRPCDeliveryTargetsForResolver(*targetsRaw, *resolver)
	if err != nil {
		fatalf("parse targets: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	_, conn, err := egress.DialGRPCDeliveryRemote(ctx, egress.GRPCDeliveryClientConfig{
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
		fatalf("dial egress delivery: %v", err)
	}
	defer func() { _ = conn.Close() }()

	client := egresspb.NewEgressDeliveryServiceClient(conn)
	successes := 0
	attempts := 0
	seen := map[string]struct{}{}
	started := time.Now()
	deadline := started.Add(*duration)
	var lastErr error
	for ctx.Err() == nil {
		attempts++
		res, err := client.GetInfo(ctx, &egresspb.EgressInfoRequest{
			ProtocolVersion:             3,
			MinSupportedProtocolVersion: 3,
			Capabilities: []string{
				"physical-receipt-batch",
				"client-ack-observation",
				"explicit-ordering-domain",
				"fenced-delivery-ref",
			},
		})
		if err != nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if res.GetError() != "" {
			lastErr = fmt.Errorf("get_info response error: %s", res.GetError())
			time.Sleep(100 * time.Millisecond)
			continue
		}
		successes++
		if instanceID := strings.TrimSpace(res.GetInstanceId()); instanceID != "" {
			seen[instanceID] = struct{}{}
		}
		if successes >= *count && len(seen) >= *expectInstances && (*duration == 0 || !time.Now().Before(deadline)) {
			break
		}
		if *interval > 0 {
			if err := sleepContext(ctx, *interval); err != nil {
				break
			}
		}
	}
	if successes < *count {
		if lastErr != nil {
			fatalf("egress delivery probe reached %d/%d successful GetInfo calls after %d attempts: %v", successes, *count, attempts, lastErr)
		}
		fatalf("egress delivery probe reached %d/%d successful GetInfo calls after %d attempts: %v", successes, *count, attempts, ctx.Err())
	}
	observed := time.Since(started)
	if *duration > 0 && observed < *duration {
		fatalf("egress delivery probe observed %s/%s after %d successful calls and %d attempts", observed.Round(time.Millisecond), duration.String(), successes, attempts)
	}
	if len(seen) < *expectInstances {
		instances := sortedKeys(seen)
		if lastErr != nil {
			fatalf("saw %d/%d distinct Egress instance_id values after %d attempts: %s: %v", len(seen), *expectInstances, attempts, strings.Join(instances, ","), lastErr)
		}
		fatalf("saw %d/%d distinct Egress instance_id values after %d attempts: %s", len(seen), *expectInstances, attempts, strings.Join(instances, ","))
	}
	fmt.Printf("egress delivery probe ok: successes=%d attempts=%d duration=%s targets=%s resolver=%s instances=%s\n",
		successes, attempts, observed.Round(time.Millisecond), strings.Join(targets, ","), strings.TrimSpace(*resolver), strings.Join(sortedKeys(seen), ","))
}

func sortedKeys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
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
