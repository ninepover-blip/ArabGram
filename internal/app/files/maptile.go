package files

import (
	"bytes"
	"image"
	"image/color"
	"image/png"
	"math"

	"go.uber.org/zap"
)

// 本文件实现 geo 消息地图缩略图（upload.getWebFile / inputWebFileGeoPointLocation）的
// 本地占位渲染：服务端按坐标确定性合成一张「街区网格 + 定位针」风格的静态图，
// 同一 (lat,long,zoom,w,h,scale) 输入字节级可重现，保证客户端分片续传一致。
// 配置了 Mapbox 代理（maptile_proxy.go）时优先返回真实地图，本渲染降级为故障回退。

const (
	mapTileMinEdge  = 16
	mapTileMaxEdge  = 1024
	mapTileMinZoom  = 13
	mapTileMaxZoom  = 20
	mapTileMaxScale = 3
)

// GeoMapTile 返回一张 w×h（逻辑像素，输出按 scale 放大）的静态地图与 mime。
// 入参越界时按协议约束 clamp（w/h 16-1024、zoom 13-20、scale 1-3），不报错。
// 配置了 Mapbox 代理时优先真实地图（落盘缓存），抓取失败回退确定性占位渲染。
func (s *Service) GeoMapTile(lat, long float64, w, h, zoom, scale int) ([]byte, string) {
	w = clampInt(w, mapTileMinEdge, mapTileMaxEdge)
	h = clampInt(h, mapTileMinEdge, mapTileMaxEdge)
	zoom = clampInt(zoom, mapTileMinZoom, mapTileMaxZoom)
	scale = clampInt(scale, 1, mapTileMaxScale)
	// nil receiver 合法：占位渲染是纯函数（既有调用方/测试依赖这一点），代理仅在配置后启用。
	if s != nil && s.mapTiles != nil {
		if data, mime, err := s.mapTiles.tile(lat, long, w, h, zoom, scale); err == nil {
			return data, mime
		} else if s.log != nil {
			s.log.Warn("map tile proxy failed, fallback to placeholder",
				zap.Error(err), zap.Float64("lat", lat), zap.Float64("long", long), zap.Int("zoom", zoom))
		}
	}
	pw, ph := w*scale, h*scale

	img := image.NewRGBA(image.Rect(0, 0, pw, ph))
	background := color.RGBA{R: 0xEB, G: 0xE7, B: 0xDE, A: 0xFF}
	park := color.RGBA{R: 0xCF, G: 0xE4, B: 0xC2, A: 0xFF}
	road := color.RGBA{R: 0xFF, G: 0xFF, B: 0xFF, A: 0xFF}
	roadEdge := color.RGBA{R: 0xD9, G: 0xD3, B: 0xC7, A: 0xFF}
	fillRect(img, 0, 0, pw, ph, background)

	rng := newMapTileRNG(lat, long, zoom)

	// 街区网格：间距与抖动由坐标种子决定，平移随经纬度连续变化，避免所有地点一张脸。
	spacing := (48 + int(rng.next()%32)) * scale
	offX := int(math.Abs(long*1e4)) % spacing
	offY := int(math.Abs(lat*1e4)) % spacing
	for x := -offX; x < pw; x += spacing {
		major := ((x+offX)/spacing)%3 == int(rng.next()%3)
		drawVerticalRoad(img, x+int(rng.next()%uint64(spacing/3)), pw, ph, scale, major, road, roadEdge)
	}
	for y := -offY; y < ph; y += spacing {
		major := ((y+offY)/spacing)%3 == int(rng.next()%3)
		drawHorizontalRoad(img, y+int(rng.next()%uint64(spacing/3)), pw, ph, scale, major, road, roadEdge)
	}

	// 两块「绿地」：取网格内随机街块，铺底色之上、道路之下的视觉层级太复杂，
	// 这里直接半覆盖即可（占位图不追求制图精度）。
	for i := 0; i < 2; i++ {
		bx := int(rng.next() % uint64(pw))
		by := int(rng.next() % uint64(ph))
		bw := (spacing * 3) / 4
		fillRect(img, bx, by, minInt(bx+bw, pw), minInt(by+bw, ph), park)
	}

	drawCenterPin(img, pw, ph, scale)

	var buf bytes.Buffer
	_ = png.Encode(&buf, img)
	return buf.Bytes(), "image/png"
}

// mapTileRNG 是确定性 xorshift64，种子来自量化坐标与 zoom。
type mapTileRNG struct{ state uint64 }

func newMapTileRNG(lat, long float64, zoom int) *mapTileRNG {
	seed := uint64(int64(lat*1e5))*1000003 ^ uint64(int64(long*1e5))*998244353 ^ uint64(zoom)*0x9E3779B97F4A7C15
	if seed == 0 {
		seed = 0x9E3779B97F4A7C15
	}
	return &mapTileRNG{state: seed}
}

func (r *mapTileRNG) next() uint64 {
	r.state ^= r.state << 13
	r.state ^= r.state >> 7
	r.state ^= r.state << 17
	return r.state
}

func fillRect(img *image.RGBA, x0, y0, x1, y1 int, c color.RGBA) {
	bounds := img.Bounds()
	x0, y0 = maxInt(x0, bounds.Min.X), maxInt(y0, bounds.Min.Y)
	x1, y1 = minInt(x1, bounds.Max.X), minInt(y1, bounds.Max.Y)
	for y := y0; y < y1; y++ {
		for x := x0; x < x1; x++ {
			img.SetRGBA(x, y, c)
		}
	}
}

func drawVerticalRoad(img *image.RGBA, x, pw, ph, scale int, major bool, road, edge color.RGBA) {
	width := 2 * scale
	if major {
		width = 4 * scale
	}
	fillRect(img, x-width/2-1, 0, x+width/2+1, ph, edge)
	fillRect(img, x-width/2, 0, x+width/2, ph, road)
}

func drawHorizontalRoad(img *image.RGBA, y, pw, ph, scale int, major bool, road, edge color.RGBA) {
	width := 2 * scale
	if major {
		width = 4 * scale
	}
	fillRect(img, 0, y-width/2-1, pw, y+width/2+1, edge)
	fillRect(img, 0, y-width/2, pw, y+width/2, road)
}

// drawCenterPin 在图中心画红色定位针（圆头 + 下尖三角 + 白色内点），中心即坐标点。
func drawCenterPin(img *image.RGBA, pw, ph, scale int) {
	pin := color.RGBA{R: 0xE5, G: 0x39, B: 0x35, A: 0xFF}
	pinDark := color.RGBA{R: 0xB7, G: 0x1C, B: 0x1C, A: 0xFF}
	white := color.RGBA{R: 0xFF, G: 0xFF, B: 0xFF, A: 0xFF}
	cx, cy := pw/2, ph/2
	headR := 9 * scale
	headCY := cy - 14*scale
	// 尖角三角形：从圆头两侧收敛到坐标点。
	for y := headCY; y <= cy; y++ {
		t := float64(y-headCY) / float64(cy-headCY)
		half := int(float64(headR) * (1 - t) * 0.82)
		for x := cx - half; x <= cx+half; x++ {
			img.SetRGBA(x, y, pin)
		}
	}
	// 圆头（带一圈深色描边）。
	for dy := -headR - scale; dy <= headR+scale; dy++ {
		for dx := -headR - scale; dx <= headR+scale; dx++ {
			d2 := dx*dx + dy*dy
			switch {
			case d2 <= headR*headR:
				img.SetRGBA(cx+dx, headCY+dy, pin)
			case d2 <= (headR+scale)*(headR+scale):
				img.SetRGBA(cx+dx, headCY+dy, pinDark)
			}
		}
	}
	innerR := 3 * scale
	for dy := -innerR; dy <= innerR; dy++ {
		for dx := -innerR; dx <= innerR; dx++ {
			if dx*dx+dy*dy <= innerR*innerR {
				img.SetRGBA(cx+dx, headCY+dy, white)
			}
		}
	}
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
