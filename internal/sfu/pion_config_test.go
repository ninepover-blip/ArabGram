package sfu

import (
	"strings"
	"testing"
)

func TestNewPionRejectsInvalidBindIP(t *testing.T) {
	_, err := NewPion(PionConfig{UDPPort: 12399, BindIP: "not-an-ip", AdvertiseIP: "127.0.0.1"})
	if err == nil || !strings.Contains(err.Error(), "invalid IPv4 bind ip") {
		t.Fatalf("NewPion err = %v, want invalid bind rejection", err)
	}
}
