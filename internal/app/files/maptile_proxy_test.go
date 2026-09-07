package files

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakePNG 是最小合法 PNG 头 + 填充（只需通过魔数识别，不需要可解码）。
var fakePNG = append([]byte{0x89, 'P', 'N', 'G', 0x0D, 0x0A, 0x1A, 0x0A}, bytes.Repeat([]byte{0x42}, 64)...)

func newProxyTestService(t *testing.T, handler http.Handler) (*Service, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	s := NewService(nil, nil, 2, WithMapboxMapTiles("test-token", t.TempDir()))
	if s.mapTiles == nil {
		t.Fatal("map tile proxy not configured")
	}
	s.mapTiles.baseURL = srv.URL
	return s, srv
}

func TestGeoMapTileProxyFetchesAndCaches(t *testing.T) {
	var calls atomic.Int32
	var lastPath, lastQuery string
	s, _ := newProxyTestService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		lastPath = r.URL.Path
		lastQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(fakePNG)
	}))

	first, mime := s.GeoMapTile(39.9042, 116.4074, 256, 128, 15, 2)
	if mime != "image/png" {
		t.Fatalf("mime = %q, want image/png", mime)
	}
	if !bytes.Equal(first, fakePNG) {
		t.Fatal("first fetch did not return upstream bytes")
	}
	// Mapbox 形态：/styles/v1/mapbox/streets-v12/static/{long},{lat},{zoom}/{w}x{h}@2x
	if !strings.Contains(lastPath, "/static/116.40740,39.90420,15/256x128@2x") {
		t.Fatalf("unexpected upstream path: %s", lastPath)
	}
	if !strings.Contains(lastQuery, "access_token=test-token") {
		t.Fatalf("missing access token in query: %s", lastQuery)
	}

	// 第二次（含分片重复读）必须走磁盘缓存，不再触发上游请求。
	second, _ := s.GeoMapTile(39.9042, 116.4074, 256, 128, 15, 2)
	if !bytes.Equal(first, second) {
		t.Fatal("cached bytes differ from first fetch")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1 (second hit must be cached)", got)
	}
}

func TestGeoMapTileProxyScaleOneOmitsRetina(t *testing.T) {
	var lastPath string
	s, _ := newProxyTestService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastPath = r.URL.Path
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(fakePNG)
	}))
	s.GeoMapTile(1.5, 2.5, 100, 100, 16, 1)
	if strings.Contains(lastPath, "@2x") {
		t.Fatalf("scale=1 must not request retina: %s", lastPath)
	}
}

func TestGeoMapTileProxyFallsBackToPlaceholder(t *testing.T) {
	var calls atomic.Int32
	s, _ := newProxyTestService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "boom", http.StatusInternalServerError)
	}))

	data, mime := s.GeoMapTile(39.9042, 116.4074, 256, 128, 15, 2)
	if len(data) == 0 || mime != "image/png" {
		t.Fatalf("fallback placeholder missing: len=%d mime=%q", len(data), mime)
	}
	// 占位图确定性：回退路径必须与纯占位服务字节一致（分片续传一致性）。
	plain := NewService(nil, nil, 2)
	expected, _ := plain.GeoMapTile(39.9042, 116.4074, 256, 128, 15, 2)
	if !bytes.Equal(data, expected) {
		t.Fatal("fallback placeholder differs from deterministic rendering")
	}

	// 负缓存：失败后的后续分片请求不应继续打上游。
	again, _ := s.GeoMapTile(39.9042, 116.4074, 256, 128, 15, 2)
	if !bytes.Equal(again, expected) {
		t.Fatal("placeholder must stay byte-identical during failure backoff")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1 (failure must be negative-cached)", got)
	}

	// 负缓存过期后允许重试并恢复真实地图。
	s.mapTiles.mu.Lock()
	for k := range s.mapTiles.failures {
		s.mapTiles.failures[k] = time.Now().Add(-2 * mapTileFailureTTL)
	}
	s.mapTiles.mu.Unlock()
	// 上游恢复。
	s.mapTiles.baseURL = newRecoveredUpstream(t)
	recovered, _ := s.GeoMapTile(39.9042, 116.4074, 256, 128, 15, 2)
	if !bytes.Equal(recovered, fakePNG) {
		t.Fatal("proxy must recover after failure TTL expires")
	}
}

func newRecoveredUpstream(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(fakePNG)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// 落盘失败时字节必须进内存兜底缓存：顺序分片不再逐片打上游，且字节全程一致。
func TestGeoMapTileProxyStoreFailureServesFromMemory(t *testing.T) {
	var calls atomic.Int32
	s, _ := newProxyTestService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(fakePNG)
	}))
	// 让缓存目录路径指向一个普通文件 → MkdirAll/写盘必然失败。
	blocked := filepath.Join(t.TempDir(), "not-a-dir")
	if err := os.WriteFile(blocked, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	s.mapTiles.dir = blocked

	first, _ := s.GeoMapTile(39.9042, 116.4074, 256, 128, 15, 2)
	second, _ := s.GeoMapTile(39.9042, 116.4074, 256, 128, 15, 2)
	if !bytes.Equal(first, fakePNG) || !bytes.Equal(second, fakePNG) {
		t.Fatal("store-failure path must keep serving upstream bytes")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("upstream calls = %d, want 1 (memory cache must absorb后续分片)", got)
	}
}

// 抓取尺寸量化到 32px 档位，收窄客户端可铸造的缓存 key 空间。
func TestGeoMapTileProxyQuantizesFetchDimensions(t *testing.T) {
	var mu sync.Mutex
	var lastPath string
	s, _ := newProxyTestService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		lastPath = r.URL.Path
		mu.Unlock()
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(fakePNG)
	}))
	s.GeoMapTile(1.5, 2.5, 100, 50, 15, 2)
	mu.Lock()
	defer mu.Unlock()
	if !strings.Contains(lastPath, "/128x64@2x") {
		t.Fatalf("dimensions not quantized to 32px steps: %s", lastPath)
	}
}

// 磁盘缓存超总量上限后按 mtime 从旧到新淘汰。
func TestGeoMapTileProxyDiskSweepEvictsOldest(t *testing.T) {
	s, _ := newProxyTestService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(fakePNG)
	}))
	s.mapTiles.maxDiskBytes = int64(len(fakePNG)*2 + 8) // 容得下 2 张，第 3 张触发淘汰
	for i := 0; i < 3; i++ {
		s.GeoMapTile(10+float64(i), 20, 128, 128, 15, 1)
		time.Sleep(20 * time.Millisecond) // 保证 mtime 可区分
	}
	entries, err := os.ReadDir(s.mapTiles.dir)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	count := 0
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		total += info.Size()
		count++
	}
	if count >= 3 || total > s.mapTiles.maxDiskBytes {
		t.Fatalf("sweep did not evict: files=%d total=%d max=%d", count, total, s.mapTiles.maxDiskBytes)
	}
}

// 全局抓取限速：超限的新 key 退占位图且不打上游。
func TestGeoMapTileProxyFetchRateLimit(t *testing.T) {
	var calls atomic.Int32
	s, _ := newProxyTestService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(fakePNG)
	}))
	// 预填满滑动窗口。
	s.mapTiles.mu.Lock()
	now := time.Now()
	for i := 0; i < mapTileFetchRateLimit; i++ {
		s.mapTiles.fetchTimes = append(s.mapTiles.fetchTimes, now)
	}
	s.mapTiles.mu.Unlock()

	data, mime := s.GeoMapTile(33.3, 44.4, 128, 128, 15, 1)
	plain := NewService(nil, nil, 2)
	expected, _ := plain.GeoMapTile(33.3, 44.4, 128, 128, 15, 1)
	if !bytes.Equal(data, expected) || mime != "image/png" {
		t.Fatal("rate-limited request must fall back to deterministic placeholder")
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("upstream calls = %d, want 0 when rate limited", got)
	}
}

func TestGeoMapTileProxyRejectsNonImageBody(t *testing.T) {
	s, _ := newProxyTestService(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte("<html>not a map</html>"))
	}))
	data, mime := s.GeoMapTile(10, 20, 128, 128, 15, 1)
	plain := NewService(nil, nil, 2)
	expected, _ := plain.GeoMapTile(10, 20, 128, 128, 15, 1)
	if !bytes.Equal(data, expected) || mime != "image/png" {
		t.Fatal("non-image upstream body must fall back to placeholder")
	}
}
