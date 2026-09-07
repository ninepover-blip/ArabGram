package core

import (
	"os"
	"strings"
	"testing"
	"time"

	telegramloginapp "telesrv/internal/app/telegramlogin"
	"telesrv/internal/config"
)

func TestTelegramLoginRPCDependencyPreservesDisabledNil(t *testing.T) {
	var disabled *telegramloginapp.Service
	if dependency := telegramLoginRPCDependency(disabled); dependency != nil {
		t.Fatalf("disabled Telegram Login dependency = %#v, want nil interface", dependency)
	}
}

func TestTelegramLoginRPCDependencyPreservesEnabledService(t *testing.T) {
	enabled := new(telegramloginapp.Service)
	if dependency := telegramLoginRPCDependency(enabled); dependency != enabled {
		t.Fatalf("enabled Telegram Login dependency = %#v, want %p", dependency, enabled)
	}
}

func TestValidateCoreConfigRequiresStandaloneSFUControlAuth(t *testing.T) {
	base := config.CoreConfig{
		GroupCallControlAddr:  "127.0.0.1:2420",
		GroupCallControlToken: "core-secret",
		SFUControlToken:       "sfu-secret",
		CoreExecGRPCAddr:      "127.0.0.1:2440",
		CoreExecToken:         "coreexec-secret",
		FileGRPCTargets:       "127.0.0.1:2460",
		FileGRPCResolver:      "static",
		FileToken:             "file-secret",
		TURNEnable:            true,
		TURNUDPPort:           12400,
		TURNSecret:            "turn-secret",
		CallTURNCredentialTTL: 6 * time.Hour,
	}
	if err := validateCoreConfig(base); err != nil {
		t.Fatalf("base config rejected: %v", err)
	}

	tests := []struct {
		name    string
		mutate  func(*config.CoreConfig)
		wantErr string
	}{
		{name: "coreexec token", mutate: func(cfg *config.CoreConfig) { cfg.CoreExecToken = " " }, wantErr: "TELESRV_CORE_EXEC_TOKEN"},
		{name: "file targets", mutate: func(cfg *config.CoreConfig) { cfg.FileGRPCTargets = " " }, wantErr: "TELESRV_FILE_GRPC_TARGETS"},
		{name: "file token", mutate: func(cfg *config.CoreConfig) { cfg.FileToken = " " }, wantErr: "TELESRV_FILE_TOKEN"},
		{name: "groupcall control addr", mutate: func(cfg *config.CoreConfig) { cfg.GroupCallControlAddr = " " }, wantErr: "TELESRV_GROUPCALL_CONTROL_ADDR"},
		{name: "groupcall control token", mutate: func(cfg *config.CoreConfig) { cfg.GroupCallControlToken = " " }, wantErr: "TELESRV_GROUPCALL_CONTROL_TOKEN"},
		{name: "sfu control token", mutate: func(cfg *config.CoreConfig) { cfg.SFUControlToken = " " }, wantErr: "TELESRV_SFU_CONTROL_TOKEN"},
		{name: "turn secret", mutate: func(cfg *config.CoreConfig) { cfg.TURNSecret = " " }, wantErr: "TELESRV_TURN_SECRET"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			tt.mutate(&cfg)
			err := validateCoreConfig(cfg)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validateCoreConfig err = %v, want %s", err, tt.wantErr)
			}
		})
	}
}

func TestCoreOrphanAuthKeyRetentionUsesDistributedEdgeSnapshot(t *testing.T) {
	body, err := os.ReadFile("runtime.go")
	if err != nil {
		t.Fatalf("read runtime.go: %v", err)
	}
	source := string(body)
	if strings.Contains(source, "WithOrphanAuthKeyRetention(authKeyStore, coreLocalEdgeControl") {
		t.Fatal("core orphan auth-key retention must not use the local no-session controller")
	}
	if !strings.Contains(source, "NewDistributedActiveRawAuthKeys(edgeLocationRegistry)") ||
		!strings.Contains(source, "WithOrphanAuthKeyRetention(authKeyStore, activeRawAuthKeys") {
		t.Fatal("core orphan auth-key retention must use the distributed Edge raw-key snapshot")
	}
}
