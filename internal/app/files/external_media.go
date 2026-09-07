package files

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"syscall"
	"time"

	"telesrv/internal/domain"
)

// 外链媒体：inputMediaPhotoExternal / inputMediaDocumentExternal——客户端给一个 URL，
// 服务端抓取并铸造 Photo/Document。抓取任意用户可控 URL，安全是核心：
//   - SSRF 防护：自定义 Dialer.Control 在连接前检查**解析出的目标 IP**，挡掉 loopback/
//     私网/link-local/CGNAT/multicast/unspecified。因为每次实际 dial 都查，所以同时防住
//     DNS rebinding（公网域名解析到内网 IP）与重定向（每一跳都重新 dial→重新检查）。
//   - 仅 http/https；重定向上限；响应大小上限（LimitReader）；请求超时；全局抓取限速
//     （防一条消息触发大量服务端外网抓取的放大攻击）。

var (
	// ErrExternalMediaDisabled 表示未启用外链媒体抓取（rpc 层映射为 MEDIA_INVALID）。
	ErrExternalMediaDisabled = errors.New("external media disabled")
	// ErrExternalMediaInvalid 表示 URL 不合法/被 SSRF 防护拦截/上游失败/超限。
	ErrExternalMediaInvalid = errors.New("external media invalid")
)

const (
	externalMediaTimeout      = 15 * time.Second
	externalMediaMaxRedirects = 5
	// DefaultExternalMediaMaxBytes 是抓取响应体上限。
	DefaultExternalMediaMaxBytes = int64(10 << 20)
	// DefaultExternalMediaRatePerMin 是全局每分钟抓取上限（防放大攻击）。
	DefaultExternalMediaRatePerMin = 60
	externalMediaRateWindow        = time.Minute
)

type externalMediaFetcher struct {
	client    *http.Client
	maxBytes  int64
	rateLimit int

	mu         sync.Mutex
	fetchTimes []time.Time
}

// WithExternalMedia 启用外链媒体抓取（inputMediaPhoto/DocumentExternal）。
// maxBytes<=0 用默认；ratePerMin<=0 用默认。SSRF 防护恒开。
func WithExternalMedia(maxBytes int64, ratePerMin int) Option {
	return func(s *Service) {
		if maxBytes <= 0 {
			maxBytes = DefaultExternalMediaMaxBytes
		}
		if ratePerMin <= 0 {
			ratePerMin = DefaultExternalMediaRatePerMin
		}
		s.externalMedia = newExternalMediaFetcher(maxBytes, ratePerMin, false)
	}
}

// newExternalMediaFetcher 构造抓取器。allowPrivate 仅供测试（指向 httptest loopback）；
// 生产恒 false。
func newExternalMediaFetcher(maxBytes int64, ratePerMin int, allowPrivate bool) *externalMediaFetcher {
	dialer := &net.Dialer{Timeout: externalMediaTimeout}
	dialer.Control = func(network, address string, _ syscall.RawConn) error {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return ErrExternalMediaInvalid
		}
		ip := net.ParseIP(host)
		if ip == nil {
			return ErrExternalMediaInvalid
		}
		if !allowPrivate && isBlockedExternalIP(ip) {
			return fmt.Errorf("%w: blocked address %s (SSRF guard)", ErrExternalMediaInvalid, host)
		}
		return nil
	}
	client := &http.Client{
		Timeout:   externalMediaTimeout,
		Transport: &http.Transport{DialContext: dialer.DialContext, DisableKeepAlives: true},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= externalMediaMaxRedirects {
				return fmt.Errorf("%w: too many redirects", ErrExternalMediaInvalid)
			}
			if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
				return fmt.Errorf("%w: blocked redirect scheme %q", ErrExternalMediaInvalid, req.URL.Scheme)
			}
			return nil
		},
	}
	return &externalMediaFetcher{client: client, maxBytes: maxBytes, rateLimit: ratePerMin}
}

// isBlockedExternalIP 报告是否为不可对外抓取的内网/特殊地址（SSRF 防护）。
func isBlockedExternalIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() ||
		ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsMulticast() || ip.IsInterfaceLocalMulticast() {
		return true
	}
	// CGNAT 100.64.0.0/10（运营商级 NAT，常用于内部基础设施）。
	if ip4 := ip.To4(); ip4 != nil && ip4[0] == 100 && ip4[1] >= 64 && ip4[1] <= 127 {
		return true
	}
	return false
}

func (f *externalMediaFetcher) allowFetch() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	kept := f.fetchTimes[:0]
	for _, at := range f.fetchTimes {
		if now.Sub(at) <= externalMediaRateWindow {
			kept = append(kept, at)
		}
	}
	f.fetchTimes = kept
	if len(f.fetchTimes) >= f.rateLimit {
		return false
	}
	f.fetchTimes = append(f.fetchTimes, now)
	return true
}

// fetch 抓取 URL，返回 (字节, content-type)。SSRF 检查在 dial 阶段发生。
func (f *externalMediaFetcher) fetch(ctx context.Context, rawURL string) ([]byte, string, error) {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, "", ErrExternalMediaInvalid
	}
	if !f.allowFetch() {
		return nil, "", fmt.Errorf("%w: rate limited", ErrExternalMediaInvalid)
	}
	ctx, cancel := context.WithTimeout(ctx, externalMediaTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, "", ErrExternalMediaInvalid
	}
	req.Header.Set("User-Agent", "telesrv-media-fetch")
	resp, err := f.client.Do(req)
	if err != nil {
		// 含 SSRF 拦截、超时、传输错误。
		return nil, "", fmt.Errorf("%w: %v", ErrExternalMediaInvalid, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("%w: upstream status %d", ErrExternalMediaInvalid, resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, f.maxBytes+1))
	if err != nil {
		return nil, "", fmt.Errorf("%w: read body: %v", ErrExternalMediaInvalid, err)
	}
	if len(data) == 0 || int64(len(data)) > f.maxBytes {
		return nil, "", fmt.Errorf("%w: body size %d", ErrExternalMediaInvalid, len(data))
	}
	contentType := resp.Header.Get("Content-Type")
	if i := strings.IndexByte(contentType, ';'); i >= 0 {
		contentType = contentType[:i]
	}
	return data, strings.TrimSpace(contentType), nil
}

// CreatePhotoFromURL 抓取 URL 并铸造 Photo（CreatePhotoFromBytes 会解码校验是否为图片）。
func (s *Service) CreatePhotoFromURL(ctx context.Context, rawURL string) (domain.Photo, error) {
	if s == nil || s.externalMedia == nil {
		return domain.Photo{}, ErrExternalMediaDisabled
	}
	data, _, err := s.externalMedia.fetch(ctx, rawURL)
	if err != nil {
		return domain.Photo{}, err
	}
	photo, err := s.CreatePhotoFromBytes(ctx, data)
	if err != nil {
		// 非图片字节 → ErrPhotoInvalid，对外统一为 external invalid。
		return domain.Photo{}, fmt.Errorf("%w: %v", ErrExternalMediaInvalid, err)
	}
	return photo, nil
}

// CreateDocumentFromURL 抓取 URL 并铸造 Document：mime 取 Content-Type，文件名取 URL basename。
func (s *Service) CreateDocumentFromURL(ctx context.Context, rawURL string) (domain.Document, error) {
	if s == nil || s.externalMedia == nil {
		return domain.Document{}, ErrExternalMediaDisabled
	}
	data, contentType, err := s.externalMedia.fetch(ctx, rawURL)
	if err != nil {
		return domain.Document{}, err
	}
	mime := contentType
	if mime == "" {
		mime = "application/octet-stream"
	}
	spec := domain.DocumentSpec{
		MimeType:   mime,
		Attributes: []domain.DocumentAttribute{{Kind: domain.DocAttrFilename, FileName: externalMediaFilename(rawURL)}},
	}
	doc, err := s.CreateDocumentFromBytes(ctx, data, spec)
	if err != nil {
		return domain.Document{}, fmt.Errorf("%w: %v", ErrExternalMediaInvalid, err)
	}
	return doc, nil
}

// externalMediaFilename 从 URL path 取 basename；缺失时回退通用名。
func externalMediaFilename(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err == nil {
		if base := path.Base(u.Path); base != "" && base != "." && base != "/" {
			return base
		}
	}
	return "file"
}
