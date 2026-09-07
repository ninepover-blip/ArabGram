// Command telesrv-migrate applies the embedded PostgreSQL schema migrations
// once before the long-running Docker roles start.
package main

import (
	"fmt"
	"io"
	"os"
	"strings"

	"telesrv/internal/store/postgres"
)

type migrateFunc func(string) (postgres.MigrationStatus, error)

func main() {
	os.Exit(run(os.Getenv, postgres.MigrateAndStatus, os.Stdout, os.Stderr))
}

func run(getenv func(string) string, migrate migrateFunc, stdout, stderr io.Writer) int {
	dsn := strings.TrimSpace(getenv("TELESRV_POSTGRES_DSN"))
	if dsn == "" {
		fmt.Fprintln(stderr, "telesrv-migrate: TELESRV_POSTGRES_DSN is required")
		return 2
	}
	status, err := migrate(dsn)
	if err != nil {
		fmt.Fprintf(stderr, "telesrv-migrate: migration failed: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "telesrv-migrate: schema ready version=%d dirty=%t empty=%t\n", status.Version, status.Dirty, status.Empty)
	return 0
}
