package turnsrv

import (
	"net"
	"strings"
	"testing"
	"time"
)

func TestCredentialIssuerDoesNotRequireMediaPorts(t *testing.T) {
	issuer, err := NewCredentialIssuer(Config{
		UDPPort:       12400,
		AdvertiseIP:   "203.0.113.10",
		SharedSecret:  "shared-turn-secret",
		CredentialTTL: time.Hour,
	})
	if err != nil {
		t.Fatalf("NewCredentialIssuer: %v", err)
	}
	defer func() { _ = issuer.Close() }()

	username, password, err := issuer.Credentials("42")
	if err != nil {
		t.Fatalf("Credentials: %v", err)
	}
	if !strings.HasSuffix(username, ":42") || password == "" {
		t.Fatalf("credentials = %q/%q", username, password)
	}
	if issuer.IP() != "203.0.113.10" || issuer.Port() != 12400 {
		t.Fatalf("endpoint = %s:%d", issuer.IP(), issuer.Port())
	}
}

func TestCredentialIssuerRequiresSharedSecret(t *testing.T) {
	_, err := NewCredentialIssuer(Config{UDPPort: 12400, AdvertiseIP: "127.0.0.1"})
	if err == nil || !strings.Contains(err.Error(), "shared secret") {
		t.Fatalf("NewCredentialIssuer err = %v, want shared secret rejection", err)
	}
}

func TestNewFailsEagerlyWhenListenerIsUnavailable(t *testing.T) {
	occupied, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve UDP port: %v", err)
	}
	defer func() { _ = occupied.Close() }()
	port := occupied.LocalAddr().(*net.UDPAddr).Port

	_, err = New(Config{
		UDPPort:      port,
		BindIP:       "127.0.0.1",
		AdvertiseIP:  "127.0.0.1",
		SharedSecret: "shared-turn-secret",
		RelayMinPort: 12500,
		RelayMaxPort: 12500,
	})
	if err == nil || !strings.Contains(err.Error(), "listen udp") {
		t.Fatalf("New err = %v, want eager listener failure", err)
	}
}
