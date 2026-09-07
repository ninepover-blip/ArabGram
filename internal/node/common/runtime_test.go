package common

import (
	"math"
	"testing"
	"time"

	"telesrv/internal/branding"
	"telesrv/internal/config"
	"telesrv/internal/coreexec"
	"telesrv/internal/domain"
)

type processGlobalsTestConfig struct {
	globals config.ProcessGlobals
}

func TestRuntimeMetricProcessEpochIsStable(t *testing.T) {
	var first float64
	cumulative := []string{"telesrv_go_total_alloc_bytes", "telesrv_go_total_mallocs", "telesrv_go_total_frees"}
	previous := make(map[string]float64, len(cumulative))
	for range 2 {
		found := false
		values := make(map[string]float64)
		for _, sample := range goRuntimeGaugeSamples() {
			values[sample.Name] = sample.Value
			if sample.Name != "telesrv_process_start_time_seconds" {
				continue
			}
			found = true
			if sample.Value <= 0 || sample.Value > float64(time.Now().UnixNano())/1e9 {
				t.Fatalf("invalid process epoch %v", sample.Value)
			}
			if first != 0 && sample.Value != first {
				t.Fatal("process epoch changed between scrapes")
			}
			first = sample.Value
		}
		if !found {
			t.Fatal("process epoch missing")
		}
		for _, name := range cumulative {
			value, ok := values[name]
			if !ok || value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
				t.Fatalf("cumulative allocation metric %s missing or invalid: %v", name, value)
			}
			if before, seen := previous[name]; seen && value < before {
				t.Fatalf("cumulative allocation metric %s decreased: %v -> %v", name, before, value)
			}
			previous[name] = value
		}
	}
}

func (c processGlobalsTestConfig) ProcessGlobals() config.ProcessGlobals { return c.globals }

func TestConfigureProcessGlobalsInstallsBrandingSnapshot(t *testing.T) {
	previousBranding := branding.Current()
	previousPremiumBotID := domain.PremiumBotConfiguredUserID()
	previousPremiumBotUsername := domain.PremiumBotConfiguredUsername()
	t.Cleanup(func() {
		if err := branding.Configure(previousBranding); err != nil {
			t.Errorf("restore branding: %v", err)
		}
		if !domain.ConfigurePremiumBotUserID(previousPremiumBotID) {
			t.Errorf("restore Premium bot id %d", previousPremiumBotID)
		}
		if !domain.ConfigurePremiumBotUsername(previousPremiumBotUsername) {
			t.Errorf("restore Premium bot username %q", previousPremiumBotUsername)
		}
	})

	brand := branding.DefaultConfig()
	brand.ProductName = "Runtime Chat"
	brand.ProductUsername = "runtime_chat"
	brand.PublicBaseURL = "https://runtime.example.test"
	cfg := processGlobalsTestConfig{globals: config.ProcessGlobals{
		Branding:           brand,
		PremiumBotUserID:   1250000999,
		PremiumBotUsername: "runtimepremiumbot",
	}}
	if err := ConfigureProcessGlobals(cfg); err != nil {
		t.Fatalf("ConfigureProcessGlobals: %v", err)
	}
	if branding.ProductName() != "Runtime Chat" || branding.ProductUsername() != "runtime_chat" || branding.PublicBaseURL() != "https://runtime.example.test" {
		t.Fatalf("installed branding = %+v", branding.Current())
	}
	if domain.PremiumBotConfiguredUserID() != 1250000999 || domain.PremiumBotConfiguredUsername() != "runtimepremiumbot" {
		t.Fatalf("installed Premium bot = %d/%q", domain.PremiumBotConfiguredUserID(), domain.PremiumBotConfiguredUsername())
	}
}

func TestCoreExecPendingAdmissionGaugeSamples(t *testing.T) {
	samples := CoreExecPendingAdmissionGaugeSamples("grpc", coreexec.PendingAdmissionStats{
		Count:     3,
		Capacity:  8192,
		TTL:       2 * time.Minute,
		OldestAge: 1500 * time.Millisecond,
	})
	got := map[string]float64{}
	for _, sample := range samples {
		if len(sample.Labels) != 1 || sample.Labels[0].Name != "transport" || sample.Labels[0].Value != "grpc" {
			t.Fatalf("sample %s labels = %+v, want transport=grpc", sample.Name, sample.Labels)
		}
		got[sample.Name] = sample.Value
	}
	want := map[string]float64{
		"telesrv_coreexec_pending_admissions":                   3,
		"telesrv_coreexec_pending_admission_capacity":           8192,
		"telesrv_coreexec_pending_admission_oldest_age_seconds": 1.5,
		"telesrv_coreexec_pending_admission_ttl_seconds":        120,
	}
	for name, value := range want {
		if got[name] != value {
			t.Fatalf("%s = %v, want %v; samples=%+v", name, got[name], value, samples)
		}
	}
}
