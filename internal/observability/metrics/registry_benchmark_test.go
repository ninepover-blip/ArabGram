package metrics

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type discardMetricResponse struct {
	header http.Header
	bytes  int
	writes int
}

func (w *discardMetricResponse) Header() http.Header { return w.header }
func (w *discardMetricResponse) WriteHeader(int)     {}
func (w *discardMetricResponse) Write(p []byte) (int, error) {
	w.bytes += len(p)
	w.writes++
	return len(p), nil
}

// BenchmarkRegistryScrape isolates exporter allocation from HTTP transport and
// live gauge-provider work. The fixed method set follows the private-lifecycle
// wire probe; sample values are synthetic, not production capacity evidence.
func BenchmarkRegistryScrape(b *testing.B) {
	for _, shape := range []string{"runtime_only", "private_lifecycle", "durable_egress"} {
		b.Run(shape, func(b *testing.B) {
			r := New()
			var samples []GaugeSample
			for i := range 24 {
				samples = append(samples, GaugeSample{Name: fmt.Sprintf("telesrv_bench_gauge_%02d", i), Value: float64(i + 1)})
			}
			r.AddGaugeProvider(func() []GaugeSample { return samples })
			switch shape {
			case "private_lifecycle":
				for _, method := range []string{
					"auth.bindTempAuthKey", "help.getConfig", "users.getUsers", "updates.getState",
					"messages.sendMessage", "messages.editMessage", "messages.getMessages",
					"messages.deleteMessages", "messages.getHistory", "messages.readHistory",
					"messages.sendReaction", "updates.getDifference",
				} {
					r.RPCHandled(method, 5*time.Millisecond, nil)
					r.RPCDatabase(method, 3, time.Millisecond, 0)
				}
			case "durable_egress":
				for _, queue := range []string{"account_pts", "account_absolute", "channel_pts"} {
					for _, stage := range []string{"claim", "projection", "targets", "bind", "dispatch", "evidence", "finalize_exact"} {
						r.OutboxStage(queue, stage, time.Millisecond, 1)
					}
				}
			}
			request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
			writer := &discardMetricResponse{header: make(http.Header)}
			r.ServeHTTP(writer, request)
			if writer.bytes == 0 {
				b.Fatal("empty exposition")
			}
			bodyBytes := writer.bytes
			writes := writer.writes
			b.ReportAllocs()
			b.SetBytes(int64(bodyBytes))
			b.ResetTimer()
			for b.Loop() {
				writer.bytes = 0
				writer.writes = 0
				r.ServeHTTP(writer, request)
			}
			b.StopTimer()
			b.ReportMetric(float64(bodyBytes), "body-bytes/op")
			b.ReportMetric(float64(writes), "writes/op")
		})
	}
}
