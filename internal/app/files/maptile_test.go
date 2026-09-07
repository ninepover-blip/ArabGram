package files

import (
	"bytes"
	"image/png"
	"testing"
)

func TestGeoMapTileDeterministicPNG(t *testing.T) {
	var s *Service
	first, mime := s.GeoMapTile(39.9042, 116.4074, 256, 128, 15, 2)
	second, _ := s.GeoMapTile(39.9042, 116.4074, 256, 128, 15, 2)
	if mime != "image/png" {
		t.Fatalf("mime = %q, want image/png", mime)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("map tile must be byte-identical for identical input (chunked download consistency)")
	}
	img, err := png.Decode(bytes.NewReader(first))
	if err != nil {
		t.Fatalf("decode png: %v", err)
	}
	if img.Bounds().Dx() != 512 || img.Bounds().Dy() != 256 {
		t.Fatalf("tile dims = %dx%d, want 512x256 (w*scale x h*scale)", img.Bounds().Dx(), img.Bounds().Dy())
	}
}

func TestGeoMapTileClampsBounds(t *testing.T) {
	var s *Service
	tile, _ := s.GeoMapTile(0, 0, 99999, -5, 99, 9)
	img, err := png.Decode(bytes.NewReader(tile))
	if err != nil {
		t.Fatalf("decode png: %v", err)
	}
	// w clamp 1024、h clamp 16、scale clamp 3。
	if img.Bounds().Dx() != 1024*3 || img.Bounds().Dy() != 16*3 {
		t.Fatalf("tile dims = %dx%d, want 3072x48", img.Bounds().Dx(), img.Bounds().Dy())
	}
}

func TestGeoMapTileDiffersByLocation(t *testing.T) {
	var s *Service
	a, _ := s.GeoMapTile(39.9042, 116.4074, 128, 128, 15, 1)
	b, _ := s.GeoMapTile(31.2304, 121.4737, 128, 128, 15, 1)
	if bytes.Equal(a, b) {
		t.Fatal("different locations should render different tiles")
	}
}
