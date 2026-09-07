package egress

import (
	"strings"
	"testing"

	"telesrv/internal/config"
)

func TestValidateEgressConfigRequiresAckBoundary(t *testing.T) {
	base := config.EgressConfig{
		EgressDeliveryGRPCAddr: "127.0.0.1:2450",
		EgressDeliveryToken:    "egress-secret",
	}
	if err := validateEgressConfig(base); err != nil {
		t.Fatalf("base config rejected: %v", err)
	}

	tests := []struct {
		name    string
		mutate  func(*config.EgressConfig)
		wantErr string
	}{
		{name: "addr", mutate: func(cfg *config.EgressConfig) { cfg.EgressDeliveryGRPCAddr = " " }, wantErr: "TELESRV_EGRESS_DELIVERY_GRPC_ADDR"},
		{name: "token", mutate: func(cfg *config.EgressConfig) { cfg.EgressDeliveryToken = " " }, wantErr: "TELESRV_EGRESS_DELIVERY_TOKEN"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			tt.mutate(&cfg)
			err := validateEgressConfig(cfg)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validateEgressConfig err = %v, want %s", err, tt.wantErr)
			}
		})
	}
}

func TestEgressWakeLanesAreIndependent(t *testing.T) {
	wakes := newEgressWakeLanes()

	wakes.wakeDispatchOutbox()
	assertWakeAvailable(t, "dispatch", wakes.dispatchOutbox)
	assertNoWake(t, "edge delivery", wakes.edgeDeliveryOutbox)

	wakes.wakeEdgeDeliveryOutbox()
	assertWakeAvailable(t, "edge delivery", wakes.edgeDeliveryOutbox)
	assertNoWake(t, "dispatch", wakes.dispatchOutbox)
}

func assertWakeAvailable(t *testing.T, name string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	default:
		t.Fatalf("%s wake was not delivered", name)
	}
}

func assertNoWake(t *testing.T, name string, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s wake should not have been delivered", name)
	default:
	}
}
