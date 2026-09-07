package files

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestIsBlockedExternalIP(t *testing.T) {
	cases := []struct {
		ip      string
		blocked bool
	}{
		{"127.0.0.1", true},      // loopback
		{"::1", true},            // loopback v6
		{"10.0.0.5", true},       // private
		{"172.16.3.4", true},     // private
		{"192.168.1.1", true},    // private
		{"169.254.1.1", true},    // link-local
		{"fe80::1", true},        // link-local v6
		{"0.0.0.0", true},        // unspecified
		{"100.64.0.1", true},     // CGNAT
		{"100.127.255.1", true},  // CGNAT 上界
		{"224.0.0.1", true},      // multicast
		{"8.8.8.8", false},       // 公网
		{"1.1.1.1", false},       // 公网
		{"100.63.255.1", false},  // CGNAT 下界外（公网）
		{"100.128.0.1", false},   // CGNAT 上界外（公网）
		{"2606:4700:4700::1111", false}, // 公网 v6
	}
	for _, c := range cases {
		ip := net.ParseIP(c.ip)
		if ip == nil {
			t.Fatalf("parse %s failed", c.ip)
		}
		if got := isBlockedExternalIP(ip); got != c.blocked {
			t.Errorf("isBlockedExternalIP(%s) = %v, want %v", c.ip, got, c.blocked)
		}
	}
}

// TestExternalMediaFetcherSSRFGuard 验证 SSRF 防护：httptest 在 loopback 上，
// allowPrivate=false 必须拦截（不连接内网），allowPrivate=true 放行抓取到字节。
func TestExternalMediaFetcherSSRFGuard(t *testing.T) {
	body := []byte("hello-external-bytes")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	// 生产配置（allowPrivate=false）：loopback 目标被 SSRF 防护拦截。
	guarded := newExternalMediaFetcher(DefaultExternalMediaMaxBytes, DefaultExternalMediaRatePerMin, false)
	if _, _, err := guarded.fetch(context.Background(), srv.URL); !errors.Is(err, ErrExternalMediaInvalid) {
		t.Fatalf("SSRF guard fetch err = %v, want ErrExternalMediaInvalid (loopback 应被拦)", err)
	}

	// 测试放行（allowPrivate=true）：抓取成功。
	open := newExternalMediaFetcher(DefaultExternalMediaMaxBytes, DefaultExternalMediaRatePerMin, true)
	data, ct, err := open.fetch(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("open fetch err = %v", err)
	}
	if string(data) != string(body) {
		t.Fatalf("fetched %q, want %q", data, body)
	}
	if ct != "application/octet-stream" {
		t.Fatalf("content-type = %q, want application/octet-stream", ct)
	}
}

// TestExternalMediaFetcherRejectsBadURL 非 http(s)/空 host 直接拒。
func TestExternalMediaFetcherRejectsBadURL(t *testing.T) {
	f := newExternalMediaFetcher(DefaultExternalMediaMaxBytes, DefaultExternalMediaRatePerMin, true)
	for _, bad := range []string{"", "ftp://x/y", "file:///etc/passwd", "javascript:alert(1)", "http://", "not a url"} {
		if _, _, err := f.fetch(context.Background(), bad); !errors.Is(err, ErrExternalMediaInvalid) {
			t.Errorf("fetch(%q) err = %v, want ErrExternalMediaInvalid", bad, err)
		}
	}
}

// TestExternalMediaFetcherSizeLimit 超大小上限拒。
func TestExternalMediaFetcherSizeLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(make([]byte, 2048))
	}))
	defer srv.Close()
	f := newExternalMediaFetcher(1024, DefaultExternalMediaRatePerMin, true)
	if _, _, err := f.fetch(context.Background(), srv.URL); !errors.Is(err, ErrExternalMediaInvalid) {
		t.Fatalf("oversize fetch err = %v, want ErrExternalMediaInvalid", err)
	}
}

// TestExternalMediaDisabled 未启用时 Create*FromURL 返回 ErrExternalMediaDisabled。
func TestExternalMediaDisabled(t *testing.T) {
	s := &Service{}
	if _, err := s.CreatePhotoFromURL(context.Background(), "http://x/y.png"); !errors.Is(err, ErrExternalMediaDisabled) {
		t.Fatalf("disabled photo err = %v, want ErrExternalMediaDisabled", err)
	}
	if _, err := s.CreateDocumentFromURL(context.Background(), "http://x/y.bin"); !errors.Is(err, ErrExternalMediaDisabled) {
		t.Fatalf("disabled doc err = %v, want ErrExternalMediaDisabled", err)
	}
}
