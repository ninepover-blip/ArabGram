package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"telesrv/internal/store/postgres"
)

func TestRunRequiresDSN(t *testing.T) {
	var stdout, stderr bytes.Buffer
	called := false
	exitCode := run(func(string) string { return "" }, func(string) (postgres.MigrationStatus, error) {
		called = true
		return postgres.MigrationStatus{}, nil
	}, &stdout, &stderr)
	if exitCode != 2 || called || stdout.Len() != 0 || !strings.Contains(stderr.String(), "TELESRV_POSTGRES_DSN is required") {
		t.Fatalf("run() = code %d called %t stdout %q stderr %q", exitCode, called, stdout.String(), stderr.String())
	}
}

func TestRunReportsStatusWithoutPrintingDSN(t *testing.T) {
	const dsn = "postgres://user:very-secret@postgres:5432/telesrv"
	var stdout, stderr bytes.Buffer
	exitCode := run(func(name string) string {
		if name == "TELESRV_POSTGRES_DSN" {
			return dsn
		}
		return ""
	}, func(got string) (postgres.MigrationStatus, error) {
		if got != dsn {
			t.Fatalf("migrate DSN = %q", got)
		}
		return postgres.MigrationStatus{Version: 229}, nil
	}, &stdout, &stderr)
	if exitCode != 0 || stderr.Len() != 0 || !strings.Contains(stdout.String(), "version=229") || strings.Contains(stdout.String(), dsn) {
		t.Fatalf("run() = code %d stdout %q stderr %q", exitCode, stdout.String(), stderr.String())
	}
}

func TestRunMigrationFailure(t *testing.T) {
	var stdout, stderr bytes.Buffer
	exitCode := run(func(string) string { return "postgres://configured" }, func(string) (postgres.MigrationStatus, error) {
		return postgres.MigrationStatus{}, errors.New("broken schema")
	}, &stdout, &stderr)
	if exitCode != 1 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "broken schema") {
		t.Fatalf("run() = code %d stdout %q stderr %q", exitCode, stdout.String(), stderr.String())
	}
}
