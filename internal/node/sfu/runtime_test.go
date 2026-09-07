package sfu

import (
	"strings"
	"testing"

	"telesrv/internal/config"
)

func TestValidateConfigRequiresControlAuth(t *testing.T) {
	base := config.SFUConfig{
		SFUBindIP:             "127.0.0.1",
		SFUControlGRPCAddr:    "127.0.0.1:2450",
		SFUControlToken:       "sfu-secret",
		GroupCallControlURL:   "http://127.0.0.1:2460",
		GroupCallControlToken: "core-secret",
	}

	tests := []struct {
		name    string
		mutate  func(*config.SFUConfig)
		wantErr string
	}{
		{
			name: "sfu bind ip",
			mutate: func(cfg *config.SFUConfig) {
				cfg.SFUBindIP = "not-an-ip"
			},
			wantErr: "TELESRV_SFU_BIND_IP",
		},
		{
			name: "sfu control grpc addr",
			mutate: func(cfg *config.SFUConfig) {
				cfg.SFUControlGRPCAddr = " "
			},
			wantErr: "TELESRV_SFU_CONTROL_GRPC_ADDR",
		},
		{
			name: "sfu control token",
			mutate: func(cfg *config.SFUConfig) {
				cfg.SFUControlToken = " "
			},
			wantErr: "TELESRV_SFU_CONTROL_TOKEN",
		},
		{
			name: "groupcall control url",
			mutate: func(cfg *config.SFUConfig) {
				cfg.GroupCallControlURL = " "
			},
			wantErr: "TELESRV_GROUPCALL_CONTROL_URL",
		},
		{
			name: "groupcall control token",
			mutate: func(cfg *config.SFUConfig) {
				cfg.GroupCallControlToken = " "
			},
			wantErr: "TELESRV_GROUPCALL_CONTROL_TOKEN",
		},
	}

	if err := validateConfig(&base); err != nil {
		t.Fatalf("base config rejected: %v", err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			tt.mutate(&cfg)
			err := validateConfig(&cfg)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validateConfig err = %v, want %s", err, tt.wantErr)
			}
		})
	}
}

func TestValidateConfigResolvesGRPCControlURL(t *testing.T) {
	cfg := config.SFUConfig{
		SFUBindIP:             "127.0.0.1",
		SFUControlGRPCAddr:    "127.0.0.1:2452",
		SFUControlGRPCURL:     "grpc://sfu-a:2452",
		SFUControlToken:       "sfu-secret",
		GroupCallControlURL:   "http://127.0.0.1:2460",
		GroupCallControlToken: "core-secret",
	}
	if err := validateConfig(&cfg); err != nil {
		t.Fatalf("grpc config rejected: %v", err)
	}
	if got := resolveControlURL(cfg); got != "grpc://sfu-a:2452" {
		t.Fatalf("resolveControlURL = %q, want grpc://sfu-a:2452", got)
	}
}

func TestValidateConfigRequiresGRPCAddr(t *testing.T) {
	cfg := config.SFUConfig{
		SFUControlToken:       "sfu-secret",
		GroupCallControlURL:   "http://127.0.0.1:2460",
		GroupCallControlToken: "core-secret",
	}
	err := validateConfig(&cfg)
	if err == nil || !strings.Contains(err.Error(), "TELESRV_SFU_CONTROL_GRPC_ADDR") {
		t.Fatalf("validateConfig err = %v, want TELESRV_SFU_CONTROL_GRPC_ADDR", err)
	}
}

func TestValidateConfigRequiresSFUOwnedTURNMediaSettings(t *testing.T) {
	base := config.SFUConfig{
		SFUBindIP:             "127.0.0.1",
		SFUControlGRPCAddr:    "127.0.0.1:2450",
		SFUControlToken:       "sfu-secret",
		GroupCallControlURL:   "http://127.0.0.1:2460",
		GroupCallControlToken: "core-secret",
		SFUUDPPort:            12399,
		TURNEnable:            true,
		TURNUDPPort:           12400,
		TURNBindIP:            "0.0.0.0",
		TURNSecret:            "turn-secret",
		TURNRelayMinPort:      12500,
		TURNRelayMaxPort:      12563,
	}
	if err := validateConfig(&base); err != nil {
		t.Fatalf("base config rejected: %v", err)
	}

	tests := []struct {
		name    string
		mutate  func(*config.SFUConfig)
		wantErr string
	}{
		{name: "secret", mutate: func(cfg *config.SFUConfig) { cfg.TURNSecret = " " }, wantErr: "TELESRV_TURN_SECRET"},
		{name: "bind ip", mutate: func(cfg *config.SFUConfig) { cfg.TURNBindIP = "not-an-ip" }, wantErr: "TELESRV_TURN_BIND_IP"},
		{name: "shared udp port", mutate: func(cfg *config.SFUConfig) { cfg.TURNUDPPort = cfg.SFUUDPPort }, wantErr: "must differ"},
		{name: "relay range", mutate: func(cfg *config.SFUConfig) { cfg.TURNRelayMaxPort = cfg.TURNRelayMinPort - 1 }, wantErr: "invalid range"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			tt.mutate(&cfg)
			err := validateConfig(&cfg)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validateConfig err = %v, want %s", err, tt.wantErr)
			}
		})
	}
}
