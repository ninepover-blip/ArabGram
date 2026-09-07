package metrics

import (
	"math"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRegistryExpositionContract(t *testing.T) {
	r := New()
	for _, d := range []time.Duration{10 * time.Millisecond, time.Second, time.Minute} {
		r.RPCHandled("quoted\"\\\n雪", d, nil)
	}
	r.AddGaugeProvider(func() []GaugeSample {
		return []GaugeSample{
			{Name: "negative", Value: -7},
			{Name: "infinity", Value: math.Inf(1)},
			{Name: "not_a_number", Value: math.NaN()},
			{Name: "float_max", Value: math.MaxFloat64},
			{Name: "9 invalid metric", Labels: []Label{{Name: "bad:label", Value: "quoted\"\\\n雪"}, {Name: "empty", Value: ""}, {Name: "long", Value: strings.Repeat("x", 100)}, {Name: "omitted", Value: "fourth"}}, Value: 3},
		}
	})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if w.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatal("content type changed")
	}
	want := map[string]bool{
		"# TYPE telesrv_metrics_dropped_observations_total counter":                                            true,
		"telesrv_metrics_dropped_observations_total 0":                                                         true,
		"# TYPE telesrv_mtproto_rpc_handled_total counter":                                                     true,
		`telesrv_mtproto_rpc_handled_total{method="quoted\"\\\n雪",outcome="ok"} 3`:                             true,
		"# TYPE __invalid_metric gauge":                                                                        true,
		`__invalid_metric{bad_label="quoted\"\\\n雪",empty="unknown",long="` + strings.Repeat("x", 96) + `"} 3`: true,
		"# TYPE negative gauge":                                                                                true, "negative -7": true,
		"# TYPE infinity gauge": true, "infinity +Inf": true,
		"# TYPE not_a_number gauge": true, "not_a_number NaN": true,
		"# TYPE float_max gauge": true, "float_max 1.7976931348623157e+308": true,
		"# TYPE telesrv_mtproto_rpc_duration_seconds histogram":                               true,
		`telesrv_mtproto_rpc_duration_seconds_sum{method="quoted\"\\\n雪",outcome="ok"} 61.01`: true,
		`telesrv_mtproto_rpc_duration_seconds_count{method="quoted\"\\\n雪",outcome="ok"} 3`:   true,
	}
	for boundary, count := range map[string]int{"0.001": 0, "0.005": 0, "0.01": 1, "0.025": 1, "0.05": 1, "0.1": 1, "0.25": 1, "0.5": 1, "1": 2, "2": 2, "5": 2, "10": 2, "30": 2, "+Inf": 3} {
		want[`telesrv_mtproto_rpc_duration_seconds_bucket{method="quoted\"\\\n雪",outcome="ok",le="`+boundary+`"} `+strconv.Itoa(count)] = true
	}
	for _, line := range strings.Split(strings.TrimSuffix(w.Body.String(), "\n"), "\n") {
		if !want[line] {
			t.Fatalf("unexpected or duplicated exposition line %q", line)
		}
		delete(want, line)
	}
	if len(want) != 0 {
		t.Fatalf("missing lines: %v", want)
	}
}

func TestRegistryProviderSamplesRemainReadOnly(t *testing.T) {
	labels := []Label{{Name: "bad:label", Value: ""}, {Name: "long", Value: strings.Repeat("x", 100)}, {Name: "third", Value: "v"}, {Name: "fourth", Value: "v"}}
	before := append([]Label(nil), labels...)
	samples := []GaugeSample{{Name: "9 invalid name", Labels: labels, Value: 1}}
	r := New()
	r.AddGaugeProvider(func() []GaugeSample { return samples })
	for range 2 {
		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/metrics", nil))
	}
	if !reflect.DeepEqual(labels, before) || samples[0].Name != "9 invalid name" || len(samples[0].Labels) != 4 {
		t.Fatalf("scrape mutated provider-owned data: %+v", samples)
	}
}

func TestRegistryConcurrentScrapesAndRecords(t *testing.T) {
	labels := []Label{{Name: "role", Value: "shared"}}
	samples := []GaugeSample{{Name: "provider_shared", Labels: labels, Value: 5}}
	r := New()
	r.AddGaugeProvider(func() []GaugeSample { return samples })
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 8 {
		wg.Go(func() {
			<-start
			for range 20 {
				w := httptest.NewRecorder()
				r.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
				if !strings.Contains(w.Body.String(), `provider_shared{role="shared"} 5`+"\n") {
					t.Error("concurrent scrape lost provider sample")
				}
			}
		})
	}
	for range 4 {
		wg.Go(func() {
			<-start
			for range 250 {
				r.RPCHandled("concurrent", time.Millisecond, nil)
			}
		})
	}
	close(start)
	wg.Wait()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	for _, line := range []string{
		`telesrv_mtproto_rpc_handled_total{method="concurrent",outcome="ok"} 1000`,
		`telesrv_mtproto_rpc_duration_seconds_count{method="concurrent",outcome="ok"} 1000`,
		`telesrv_mtproto_rpc_duration_seconds_bucket{method="concurrent",outcome="ok",le="+Inf"} 1000`,
	} {
		if !strings.Contains(w.Body.String(), line+"\n") {
			t.Fatalf("final count mismatch: %s", line)
		}
	}
}

func TestRegistryProviderSampleCap(t *testing.T) {
	samples := make([]GaugeSample, 1025)
	for i := range samples {
		samples[i] = GaugeSample{Name: "provider_" + strconv.Itoa(i), Value: 1}
	}
	r := New()
	r.AddGaugeProvider(func() []GaugeSample { return samples })
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	body := w.Body.String()
	if !strings.Contains(body, "telesrv_metrics_dropped_observations_total 1\n") || strings.Contains(body, "provider_1024 ") {
		t.Fatal("provider cap or rejection count changed")
	}
	if n := strings.Count(body, "# TYPE provider_"); n != 1024 {
		t.Fatalf("provider series = %d, want 1024", n)
	}
}
