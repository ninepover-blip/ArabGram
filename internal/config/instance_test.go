package config

import (
	"strings"
	"testing"

	"telesrv/internal/deliverycontract"
)

func TestRequireInstanceIDHardContract(t *testing.T) {
	if got, err := RequireInstanceID("edge-a"); err != nil || got != "edge-a" {
		t.Fatalf("RequireInstanceID valid = %q, %v", got, err)
	}
	for _, invalid := range []string{
		"",
		" edge-a",
		"edge-a ",
		strings.Repeat("e", deliverycontract.MaxInstanceIDBytes+1),
	} {
		if _, err := RequireInstanceID(invalid); err == nil {
			t.Fatalf("RequireInstanceID(%q) succeeded", invalid)
		}
	}
}
