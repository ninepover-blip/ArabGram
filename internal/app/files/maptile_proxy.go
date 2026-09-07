package files

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"
)

// 本文件实现地图缩略图的真实数据源：upload.getWebFile 命中 geo 坐标时，服务端代理
// Mapbox Static Images API 抓取一张静态地图并落盘缓存。客户端按 offset/limit 分片下载
// 同一文件，字节必须全程一致，因此：
//   - 抓取成功先原子落盘（temp+rename），所有分片一律从缓存文件读；
//   - 落盘失败时字节进短 TTL 进程内缓存兜底——分片是顺序请求、singleflight 合并不了，
//     没有这层缓存会退化成每分片一次外网抓取，且两次抓取的字节无逐字节一致保证；
//   - 抓取失败记一个短 TTL 负缓存，期间该 key 直接走确定性占位图，避免同一次下载
//     前后分片在「真图/占位图」之间翻转，也避免上游故障时每个分片都打一次外网。
//
// 资源边界（key 空间由客户端可控的坐标×尺寸组合构成，必须设防）：
//   - 抓取尺寸量化到 32px 档位，收窄 key 空间与缓存基数；
//   - 磁盘缓存有总量上限，超限按 mtime 从旧到新淘汰（顺带回收崩溃遗留 .tmp）；
//   - 上游抓取有全局速率上限，超限当次退占位图，防止恶意枚举烧 Mapbox 配额。
//
// 地图不内嵌定位针：TDesktop（historyMapPoint icon）与 DrKLO 都在客户端叠加 marker，
// 与官方静态图行为一致。

const (
	mapTileFetchTimeout   = 15 * time.Second
	mapTileMaxFetchBytes  = 8 << 20 // 防御性上限；640x640@2x PNG 远小于此
	mapTileFailureTTL     = time.Minute
	mapboxStaticStyleBase = "/styles/v1/mapbox/streets-v12/static"

	// mapTileEdgeStep 是抓取尺寸的量化步长（向上取整）；客户端拿到略大的图自适应缩放。
	mapTileEdgeStep = 32
	// mapTileMemTTL/mapTileMemMaxBytes 是落盘失败兜底字节缓存的保留期与总量上限。
	mapTileMemTTL      = 10 * time.Minute
	mapTileMemMaxBytes = 32 << 20
	// mapTileDiskMaxBytes 是磁盘缓存总量上限；超限按 mtime 淘汰到 90%。
	mapTileDiskMaxBytes = int64(256 << 20)
	// mapTileFetchRateLimit 是全局每分钟上游抓取上限（防恶意坐标枚举烧配额）。
	mapTileFetchRateLimit  = 120
	mapTileFetchRateWindow = time.Minute
	// mapTileTmpMaxAge 是崩溃遗留 .tmp 的回收阈值。
	mapTileTmpMaxAge = time.Hour
)

type memTileEntry struct {
	data []byte
	at   time.Time
}

type mapTileProxy struct {
	token        string
	baseURL      string // 默认 https://api.mapbox.com；测试注入 httptest 地址
	dir          string
	client       *http.Client
	log          *zap.Logger
	maxDiskBytes int64

	group   singleflight.Group
	sweepMu sync.Mutex

	mu         sync.Mutex
	failures   map[string]time.Time // key → 失败时刻（负缓存）
	memTiles   map[string]memTileEntry
	memBytes   int
	fetchTimes []time.Time // 上游抓取滑动窗口
}

// WithMapboxMapTiles 启用 Mapbox 静态地图代理；token 为空时不启用（保持占位图）。
// logger 由 NewService 在全部 Option 应用后统一注入。
func WithMapboxMapTiles(token, cacheDir string) Option {
	return func(s *Service) {
		if token == "" || cacheDir == "" {
			return
		}
		s.mapTiles = &mapTileProxy{
			token:        token,
			baseURL:      "https://api.mapbox.com",
			dir:          cacheDir,
			client:       &http.Client{Timeout: mapTileFetchTimeout},
			maxDiskBytes: mapTileDiskMaxBytes,
			failures:     make(map[string]time.Time),
			memTiles:     make(map[string]memTileEntry),
		}
	}
}

// tile 返回 (lat,long,zoom,w,h,scale) 对应的静态地图字节；入参须已 clamp。
func (p *mapTileProxy) tile(lat, long float64, w, h, zoom, scale int) ([]byte, string, error) {
	// Mapbox 静态图只支持 @2x；scale 3 同样按 2x 抓（客户端按目标尺寸自适应缩放）。
	retina := ""
	if scale >= 2 {
		retina = "@2x"
	}
	// 尺寸量化收窄客户端可铸造的 key 空间；客户端按目标矩形自适应缩放略大的图。
	w = quantizeTileEdge(w)
	h = quantizeTileEdge(h)
	key := fmt.Sprintf("v1-%.5f-%.5f-%d-%dx%d%s", lat, long, zoom, w, h, retina)
	path := p.cachePath(key)
	if data, err := os.ReadFile(path); err == nil && len(data) > 0 {
		return data, mapTileMime(data), nil
	}
	if data, ok := p.cachedMemTile(key); ok {
		return data, mapTileMime(data), nil
	}
	if p.recentlyFailed(key) {
		return nil, "", errors.New("map tile fetch in failure backoff")
	}
	v, err, _ := p.group.Do(key, func() (any, error) {
		if data, err := os.ReadFile(path); err == nil && len(data) > 0 {
			return data, nil
		}
		if data, ok := p.cachedMemTile(key); ok {
			return data, nil
		}
		if !p.allowFetch() {
			// 全局抓取限速：负缓存让该 key 短期稳定走占位图（保持分片字节一致），不打上游。
			p.markFailed(key)
			return nil, errors.New("map tile fetch rate limited")
		}
		data, err := p.fetch(lat, long, w, h, zoom, retina)
		if err != nil {
			p.markFailed(key)
			return nil, err
		}
		if err := p.store(path, data); err != nil {
			// 分片是顺序请求，singleflight 合并不了后续分片；字节必须进内存缓存兜底，
			// 否则磁盘持续故障会退化成每分片一次外网抓取且字节无一致性保证。
			p.rememberMemTile(key, data)
			p.log.Warn("map tile cache write failed, serving from memory", zap.Error(err), zap.String("key", key))
		} else {
			p.sweepDisk()
		}
		return data, nil
	})
	if err != nil {
		return nil, "", err
	}
	data := v.([]byte)
	return data, mapTileMime(data), nil
}

// quantizeTileEdge 把边长向上量化到 mapTileEdgeStep 的整数倍（caller 已 clamp 到 16..1024）。
func quantizeTileEdge(v int) int {
	q := ((v + mapTileEdgeStep - 1) / mapTileEdgeStep) * mapTileEdgeStep
	if q > mapTileMaxEdge {
		return mapTileMaxEdge
	}
	return q
}

// fetch 请求 Mapbox Static Images API。注意 URL 坐标顺序是 {long},{lat}。
func (p *mapTileProxy) fetch(lat, long float64, w, h, zoom int, retina string) ([]byte, error) {
	endpoint := fmt.Sprintf("%s%s/%.5f,%.5f,%d/%dx%d%s?access_token=%s&attribution=false&logo=false",
		p.baseURL, mapboxStaticStyleBase, long, lat, zoom, w, h, retina, url.QueryEscape(p.token))
	// 不透传 RPC ctx：singleflight 结果被并发分片共享，单个调用方取消不应拖垮整次抓取。
	ctx, cancel := context.WithTimeout(context.Background(), mapTileFetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build map tile request: %w", err)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		// 传输层错误内嵌完整 URL（含 access_token），脱敏后再向上传播/落日志。
		return nil, fmt.Errorf("fetch map tile: %s", p.redactToken(err.Error()))
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch map tile: upstream status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, mapTileMaxFetchBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read map tile body: %w", err)
	}
	if len(data) == 0 || len(data) > mapTileMaxFetchBytes {
		return nil, fmt.Errorf("map tile body size invalid: %d", len(data))
	}
	if mime := mapTileMime(data); mime == "" {
		return nil, errors.New("map tile body is not an image")
	}
	return data, nil
}

func (p *mapTileProxy) cachePath(key string) string {
	sum := sha256.Sum256([]byte(key))
	return filepath.Join(p.dir, hex.EncodeToString(sum[:])+".img")
}

func (p *mapTileProxy) store(path string, data []byte) error {
	if err := os.MkdirAll(p.dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(p.dir, "tile-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return err
	}
	return nil
}

// redactToken 把错误文本中的 access token（原样或 URL 转义形态）替换为占位符。
func (p *mapTileProxy) redactToken(s string) string {
	if p.token == "" {
		return s
	}
	s = strings.ReplaceAll(s, p.token, "***")
	if escaped := url.QueryEscape(p.token); escaped != p.token {
		s = strings.ReplaceAll(s, escaped, "***")
	}
	return s
}

// cachedMemTile 返回落盘失败兜底缓存中的字节（TTL 内）。
func (p *mapTileProxy) cachedMemTile(key string) ([]byte, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	entry, ok := p.memTiles[key]
	if !ok {
		return nil, false
	}
	if time.Since(entry.at) > mapTileMemTTL {
		p.memBytes -= len(entry.data)
		delete(p.memTiles, key)
		return nil, false
	}
	return entry.data, true
}

// rememberMemTile 在落盘失败时暂存字节；超总量按最旧淘汰。
func (p *mapTileProxy) rememberMemTile(key string, data []byte) {
	if len(data) > mapTileMemMaxBytes {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	for k, entry := range p.memTiles {
		if now.Sub(entry.at) > mapTileMemTTL {
			p.memBytes -= len(entry.data)
			delete(p.memTiles, k)
		}
	}
	if old, ok := p.memTiles[key]; ok {
		p.memBytes -= len(old.data)
	}
	for p.memBytes+len(data) > mapTileMemMaxBytes && len(p.memTiles) > 0 {
		oldestKey := ""
		var oldestAt time.Time
		for k, entry := range p.memTiles {
			if oldestKey == "" || entry.at.Before(oldestAt) {
				oldestKey, oldestAt = k, entry.at
			}
		}
		p.memBytes -= len(p.memTiles[oldestKey].data)
		delete(p.memTiles, oldestKey)
	}
	p.memTiles[key] = memTileEntry{data: data, at: now}
	p.memBytes += len(data)
}

// allowFetch 是全局上游抓取限速（滑动窗口）；超限返回 false。
func (p *mapTileProxy) allowFetch() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	kept := p.fetchTimes[:0]
	for _, at := range p.fetchTimes {
		if now.Sub(at) <= mapTileFetchRateWindow {
			kept = append(kept, at)
		}
	}
	p.fetchTimes = kept
	if len(p.fetchTimes) >= mapTileFetchRateLimit {
		return false
	}
	p.fetchTimes = append(p.fetchTimes, now)
	return true
}

// sweepDisk 在新写入后核算缓存目录总量，超限按 mtime 从旧到新淘汰到 90%，
// 顺带回收崩溃遗留的过期 .tmp。store 仅发生在上游抓取后（低频），同步扫描可接受。
func (p *mapTileProxy) sweepDisk() {
	p.sweepMu.Lock()
	defer p.sweepMu.Unlock()
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return
	}
	type tileFile struct {
		name string
		size int64
		mod  time.Time
	}
	var files []tileFile
	var total int64
	now := time.Now()
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if strings.HasSuffix(entry.Name(), ".tmp") {
			if now.Sub(info.ModTime()) > mapTileTmpMaxAge {
				_ = os.Remove(filepath.Join(p.dir, entry.Name()))
			}
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".img") {
			continue
		}
		files = append(files, tileFile{name: entry.Name(), size: info.Size(), mod: info.ModTime()})
		total += info.Size()
	}
	if total <= p.maxDiskBytes {
		return
	}
	sort.Slice(files, func(i, j int) bool { return files[i].mod.Before(files[j].mod) })
	target := p.maxDiskBytes * 9 / 10
	removed := 0
	for _, f := range files {
		if total <= target {
			break
		}
		if err := os.Remove(filepath.Join(p.dir, f.name)); err == nil {
			total -= f.size
			removed++
		}
	}
	if removed > 0 && p.log != nil {
		p.log.Info("map tile cache swept", zap.Int("removed", removed), zap.Int64("remaining_bytes", total))
	}
}

func (p *mapTileProxy) recentlyFailed(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	at, ok := p.failures[key]
	if !ok {
		return false
	}
	if time.Since(at) > mapTileFailureTTL {
		delete(p.failures, key)
		return false
	}
	return true
}

func (p *mapTileProxy) markFailed(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	// 顺手清理过期项，防 map 无界增长（key 空间本身有限：坐标×尺寸枚举）。
	now := time.Now()
	for k, at := range p.failures {
		if now.Sub(at) > mapTileFailureTTL {
			delete(p.failures, k)
		}
	}
	p.failures[key] = now
}

// mapTileMime 按魔数识别图片类型；非图片返回空串。
func mapTileMime(data []byte) string {
	switch {
	case len(data) > 8 && data[0] == 0x89 && data[1] == 'P' && data[2] == 'N' && data[3] == 'G':
		return "image/png"
	case len(data) > 3 && data[0] == 0xFF && data[1] == 0xD8:
		return "image/jpeg"
	case len(data) > 12 && string(data[0:4]) == "RIFF" && string(data[8:12]) == "WEBP":
		return "image/webp"
	default:
		return ""
	}
}
