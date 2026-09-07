// Command mtproto-edge-smoke-probe verifies the real MTProto TCP entrypoint.
//
// It intentionally uses gotd's telegram.Client instead of CoreExec internals:
// the probe performs auth-key exchange, encrypted request/response transport,
// Edge TL admission, Edge -> Core dispatch, and rpc_result delivery back to the
// client.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/iamxvbaba/td/tg"

	"telesrv/scripts/internal/mtprobes"
)

func main() {
	server := flag.String("server", "127.0.0.1:2398", "MTProto Edge host:port")
	dc := flag.Int("dc", 2, "wire DC label")
	rsaKey := flag.String("rsa-key", "data/server_rsa.pem", "server RSA private/public PEM")
	apiID := flag.Int("api-id", 1, "test application ID")
	apiHash := flag.String("api-hash", "hash", "test application hash")
	count := flag.Int("count", 2, "help.getConfig RPCs required over one MTProto session")
	timeout := flag.Duration("timeout", 15*time.Second, "overall probe timeout")
	obfuscated := flag.Bool("obfuscated", false, "use Obfuscated2 + abridged transport")
	pfs := flag.Bool("pfs", false, "enable temporary auth-key PFS")
	tempKeyTTL := flag.Int("temp-key-ttl", 86400, "temporary auth-key lifetime in seconds")
	flag.Parse()

	if *server == "" {
		fatalf("-server is required")
	}
	if *dc <= 0 {
		fatalf("-dc must be positive")
	}
	if *count <= 0 {
		fatalf("-count must be positive")
	}
	if *timeout <= 0 {
		fatalf("-timeout must be positive")
	}

	publicKey, err := mtprobes.LoadRSAPublicKey(*rsaKey)
	if err != nil {
		fatalf("load RSA public key: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	client, err := mtprobes.NewClient(mtprobes.Endpoint{
		Address:    *server,
		DC:         *dc,
		APIID:      *apiID,
		APIHash:    *apiHash,
		PublicKey:  publicKey,
		Obfuscated: *obfuscated,
		PFS:        *pfs,
		TempKeyTTL: *tempKeyTTL,
	})
	if err != nil {
		fatalf("create client: %v", err)
	}

	started := time.Now()
	if err := client.Run(ctx, func(ctx context.Context) error {
		raw := tg.NewClient(client)
		for i := 0; i < *count; i++ {
			cfg, err := raw.HelpGetConfig(ctx)
			if err != nil {
				return fmt.Errorf("help.getConfig #%d: %w", i+1, err)
			}
			if cfg.ThisDC != *dc {
				return fmt.Errorf("help.getConfig #%d returned ThisDC=%d, want %d", i+1, cfg.ThisDC, *dc)
			}
		}
		return nil
	}); err != nil {
		fatalf("mtproto edge probe failed: %v", err)
	}

	fmt.Printf("mtproto edge probe ok: server=%s dc=%d count=%d obfuscated=%t elapsed=%s\n",
		*server, *dc, *count, *obfuscated, time.Since(started).Round(time.Millisecond))
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
