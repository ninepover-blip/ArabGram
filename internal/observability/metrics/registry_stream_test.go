package metrics

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const streamDroppedText = "# TYPE telesrv_metrics_dropped_observations_total counter\ntelesrv_metrics_dropped_observations_total 0\n"

// The writer copies each offered chunk: ResponseWriter implementations must not
// retain the caller's slice after Write returns, since the exporter reuses it.
type streamResponseWriter struct {
	header   http.Header
	chunks   [][]byte
	accepted bytes.Buffer
	failAt   int
	failure  string
	failedN  int
}

func (w *streamResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (*streamResponseWriter) WriteHeader(int) {}

func (w *streamResponseWriter) Write(p []byte) (int, error) {
	w.chunks = append(w.chunks, bytes.Clone(p))
	if len(w.chunks) != w.failAt {
		return w.accepted.Write(p)
	}
	n, err := 0, errors.New("injected metric response failure")
	switch w.failure {
	case "error":
	case "partial_error":
		n = len(p) / 2
	case "short_without_error":
		n, err = len(p)-1, nil
	case "full_with_error":
		n = len(p)
	default:
		panic("unknown metric response failure")
	}
	w.failedN = n
	_, _ = w.accepted.Write(p[:n])
	return n, err
}

// Expected text is built from the public exposition contract, without the
// production serializer, capacity calculation, or streaming helpers.
func streamRegistryFixture() (*Registry, string) {
	r := New()
	const methods = 128
	for i := range methods {
		r.RPCHandled(fmt.Sprintf("stream.%03d", i), time.Millisecond, nil)
	}
	var gauges []GaugeSample
	for i := range 40 {
		gauges = append(gauges, GaugeSample{
			Name: "telesrv_stream_gauge", Labels: []Label{{Name: "slot", Value: fmt.Sprintf("%03d", i)}}, Value: float64(i),
		})
	}
	r.AddGaugeProvider(func() []GaugeSample { return gauges })
	var want strings.Builder
	want.WriteString(streamDroppedText)
	want.WriteString("# TYPE telesrv_mtproto_rpc_handled_total counter\n")
	for i := range methods {
		fmt.Fprintf(&want, "telesrv_mtproto_rpc_handled_total{method=\"stream.%03d\",outcome=\"ok\"} 1\n", i)
	}
	want.WriteString("# TYPE telesrv_stream_gauge gauge\n")
	for i := range 40 {
		fmt.Fprintf(&want, "telesrv_stream_gauge{slot=\"%03d\"} %d\n", i, i)
	}
	want.WriteString("# TYPE telesrv_mtproto_rpc_duration_seconds histogram\n")
	for i := range methods {
		for _, boundary := range []string{"0.001", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2", "5", "10", "30", "+Inf"} {
			fmt.Fprintf(&want, "telesrv_mtproto_rpc_duration_seconds_bucket{method=\"stream.%03d\",outcome=\"ok\",le=\"%s\"} 1\n", i, boundary)
		}
		fmt.Fprintf(&want, "telesrv_mtproto_rpc_duration_seconds_sum{method=\"stream.%03d\",outcome=\"ok\"} 0.001\n", i)
		fmt.Fprintf(&want, "telesrv_mtproto_rpc_duration_seconds_count{method=\"stream.%03d\",outcome=\"ok\"} 1\n", i)
	}
	return r, want.String()
}

func TestRegistryStreamCompleteOutput(t *testing.T) {
	for _, name := range []string{"empty", "small", "exact_chunk", "oversized_line", "multiple_chunks"} {
		t.Run(name, func(t *testing.T) {
			r, want := New(), streamDroppedText
			if name == "multiple_chunks" {
				r, want = streamRegistryFixture()
			} else if name != "empty" {
				metricName := "telesrv_stream_value"
				switch name {
				case "exact_chunk":
					// The initial dropped series plus this TYPE line fill one
					// chunk exactly; its sample must start the next chunk.
					metricName = strings.Repeat("a", expositionChunkSize-len(streamDroppedText)-len("# TYPE  gauge\n"))
				case "oversized_line":
					metricName = strings.Repeat("a", expositionChunkSize+31)
				}
				r.AddGaugeProvider(func() []GaugeSample { return []GaugeSample{{Name: metricName, Value: 7}} })
				want += "# TYPE " + metricName + " gauge\n" + metricName + " 7\n"
			}
			w := &streamResponseWriter{}
			r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			if !bytes.Equal(w.accepted.Bytes(), []byte(want)) {
				t.Fatalf("exposition bytes changed: got %d bytes, want %d", w.accepted.Len(), len(want))
			}
			if w.Header().Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" {
				t.Fatal("content type changed")
			}
			for i, chunk := range w.chunks {
				if len(chunk) == 0 || chunk[len(chunk)-1] != '\n' {
					t.Fatalf("chunk %d is empty or splits a sample", i)
				}
				if name != "oversized_line" && len(chunk) > expositionChunkSize {
					t.Fatalf("ordinary chunk %d exceeds byte limit: %d", i, len(chunk))
				}
			}
			switch name {
			case "empty", "small":
				if len(w.chunks) != 1 {
					t.Fatalf("small response writes = %d, want 1", len(w.chunks))
				}
			case "exact_chunk":
				if len(w.chunks) != 2 || len(w.chunks[0]) != expositionChunkSize {
					t.Fatalf("exact-boundary response writes = %d, first bytes = %d", len(w.chunks), len(w.chunks[0]))
				}
			case "oversized_line":
				longChunks := 0
				for _, chunk := range w.chunks {
					if len(chunk) > expositionChunkSize {
						longChunks++
					}
				}
				if longChunks == 0 {
					t.Fatal("oversized metric was not delivered intact")
				}
			case "multiple_chunks":
				if len(w.chunks) < 3 {
					t.Fatalf("fixture did not exercise multiple writes: %d", len(w.chunks))
				}
			}
		})
	}
}

func TestRegistryStreamHTTPBody(t *testing.T) {
	r, want := streamRegistryFixture()
	server := httptest.NewServer(r)
	t.Cleanup(server.Close)
	client := server.Client()
	client.Timeout = 5 * time.Second
	response, err := client.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, int64(len(want)+1)))
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/plain; version=0.0.4; charset=utf-8" {
		t.Fatalf("HTTP response changed: status=%d content-type=%q", response.StatusCode, response.Header.Get("Content-Type"))
	}
	if !bytes.Equal(body, []byte(want)) {
		t.Fatalf("HTTP body incomplete or duplicated: got %d bytes, want %d", len(body), len(want))
	}
}

func TestRegistryStreamStopsAfterWriteFailure(t *testing.T) {
	r, want := streamRegistryFixture()
	baseline := &streamResponseWriter{}
	r.ServeHTTP(baseline, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if baseline.accepted.String() != want || len(baseline.chunks) < 3 {
		t.Fatal("failure fixture did not produce a complete multi-chunk response")
	}
	for _, stage := range []struct {
		name string
		at   int
	}{{"first", 1}, {"middle", (len(baseline.chunks) + 1) / 2}, {"tail", len(baseline.chunks)}} {
		for _, failure := range []string{"error", "partial_error", "short_without_error", "full_with_error"} {
			t.Run(stage.name+"/"+failure, func(t *testing.T) {
				w := &streamResponseWriter{failAt: stage.at, failure: failure}
				r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
				if len(w.chunks) != stage.at {
					t.Fatalf("write calls = %d after failure on call %d", len(w.chunks), stage.at)
				}
				for i, chunk := range w.chunks {
					if !bytes.Equal(chunk, baseline.chunks[i]) {
						t.Fatalf("offered prefix changed at chunk %d", i)
					}
				}
				prefix := bytes.Join(baseline.chunks[:stage.at-1], nil)
				prefix = append(prefix, baseline.chunks[stage.at-1][:w.failedN]...)
				if !bytes.Equal(w.accepted.Bytes(), prefix) {
					t.Fatal("failed response repeated or changed accepted bytes")
				}
			})
		}
	}
}
