package core

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"telesrv/internal/config"
)

func TestLocalStarGiftWithdrawalRequiresPublicListener(t *testing.T) {
	options, err := localStarGiftWithdrawalOptions("not-a-url", "", config.StarGiftExportModeLocal)
	if err != nil || options != nil {
		t.Fatalf("disabled public listener options=%d err=%v, want nil without URL construction", len(options), err)
	}
	options, err = localStarGiftWithdrawalOptions("https://links.example.test", "127.0.0.1:2401", config.StarGiftExportModeLocal)
	if err != nil || len(options) != 2 {
		t.Fatalf("local mode options=%d err=%v, want gift and revenue providers", len(options), err)
	}
	options, err = localStarGiftWithdrawalOptions("https://links.example.test", "127.0.0.1:2401", config.StarGiftExportModeDisabled)
	if err != nil || len(options) != 1 {
		t.Fatalf("disabled gift export options=%d err=%v, want revenue provider", len(options), err)
	}
	if options, err = localStarGiftWithdrawalOptions("not-a-url", "127.0.0.1:2401", config.StarGiftExportModeLocal); err == nil || options != nil {
		t.Fatalf("enabled listener accepted dead public URL: options=%d err=%v", len(options), err)
	}
}

func TestLoadTONCapabilitySecretRequiresBoundedBase64Key(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "capability.key")
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	if err := os.WriteFile(path, []byte(base64.StdEncoding.EncodeToString(key)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := loadTONCapabilitySecret(path)
	if err != nil || string(loaded) != string(key) {
		t.Fatalf("load capability secret = %x err=%v", loaded, err)
	}

	for name, value := range map[string]string{
		"plaintext": "this-is-not-a-base64-capability-secret",
		"short":     base64.StdEncoding.EncodeToString(make([]byte, 31)),
	} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadTONCapabilitySecret(path); err == nil {
				t.Fatal("invalid capability secret accepted")
			}
		})
	}
}

func TestLoadTONClaimBotTokenRequiresSingleValidToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "claim-bot.token")
	if err := os.WriteFile(path, []byte("900001:claim-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if token, err := loadTONClaimBotToken(path); err != nil || token != "900001:claim-secret" {
		t.Fatalf("load claim bot token = %q err=%v", token, err)
	}
	for name, value := range map[string]string{
		"missing secret": "900001",
		"multiple lines": "900001:claim-secret\nextra",
		"oversized":      "900001:" + strings.Repeat("x", 506),
	} {
		t.Run(name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(value), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := loadTONClaimBotToken(path); err == nil {
				t.Fatal("invalid claim bot token accepted")
			}
		})
	}
}
