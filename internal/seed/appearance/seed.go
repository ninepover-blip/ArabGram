package appearance

import (
	"embed"
	"encoding/json"
	"sync"
)

// FS contains the default appearance catalog and wallpaper blobs.
//
//go:embed default_appearance_seed.json default_wallpapers/documents/* default_wallpapers/thumbs/m/*
var FS embed.FS

var (
	loadOnce       sync.Once
	defaultCatalog Catalog
	loadErr        error
)

// Default returns the bundled appearance seed.
func Default() Catalog {
	loadOnce.Do(func() {
		raw, err := FS.ReadFile("default_appearance_seed.json")
		if err != nil {
			loadErr = err
			return
		}
		loadErr = json.Unmarshal(raw, &defaultCatalog)
	})
	if loadErr != nil {
		panic(loadErr)
	}
	return defaultCatalog
}

type Catalog struct {
	Source            string        `json:"source"`
	ExportedAt        string        `json:"exported_at"`
	Notes             Notes         `json:"notes"`
	ChatThemes        []ChatTheme   `json:"chat_themes"`
	Wallpapers        []Wallpaper   `json:"wallpapers"`
	PeerColors        []ColorOption `json:"peer_colors"`
	PeerProfileColors []ColorOption `json:"peer_profile_colors"`
}

type Notes struct {
	Server                   string `json:"server"`
	ThemesTotal              int    `json:"themes_total"`
	ChatThemesTotal          int    `json:"chat_themes_total"`
	ChatThemeSeed            string `json:"chat_theme_seed"`
	WallpaperDocumentsBucket string `json:"wallpaper_documents_bucket"`
	WallpaperThumbsBucket    string `json:"wallpaper_thumbs_bucket"`
}

type ChatTheme struct {
	ID         int64           `json:"id"`
	AccessHash int64           `json:"access_hash"`
	Slug       string          `json:"slug"`
	Title      string          `json:"title"`
	Emoticon   string          `json:"emoticon"`
	Default    bool            `json:"default"`
	Settings   []ThemeSettings `json:"settings"`
}

type ThemeSettings struct {
	BaseTheme         string    `json:"base_theme"`
	AccentColor       int       `json:"accent_color"`
	OutboxAccentColor int       `json:"outbox_accent_color"`
	MessageColors     []int     `json:"message_colors"`
	Wallpaper         Wallpaper `json:"wallpaper"`
}

type Wallpaper struct {
	ID         int64             `json:"id"`
	AccessHash int64             `json:"access_hash"`
	Type       int               `json:"type"`
	Default    bool              `json:"default"`
	Pattern    bool              `json:"pattern"`
	Dark       bool              `json:"dark"`
	Slug       string            `json:"slug"`
	Settings   WallpaperSettings `json:"settings"`
	Document   Document          `json:"document"`
}

type WallpaperSettings struct {
	ID                    int64 `json:"id"`
	Blur                  bool  `json:"blur"`
	Motion                bool  `json:"motion"`
	BackgroundColor       int   `json:"background_color"`
	SecondBackgroundColor int   `json:"second_background_color"`
	ThirdBackgroundColor  int   `json:"third_background_color"`
	FourthBackgroundColor int   `json:"fourth_background_color"`
	Intensity             int   `json:"intensity"`
	Rotation              int   `json:"rotation"`
}

type Document struct {
	ID         int64               `json:"id"`
	AccessHash int64               `json:"access_hash"`
	Date       int                 `json:"date"`
	MimeType   string              `json:"mime_type"`
	Size       int64               `json:"size"`
	DCID       int                 `json:"dc_id"`
	Path       string              `json:"path"`
	SHA256     string              `json:"sha256"`
	Attributes []DocumentAttribute `json:"attributes"`
	Thumbs     []PhotoSize         `json:"thumbs"`
}

type DocumentAttribute struct {
	Kind     string `json:"kind"`
	W        int    `json:"w,omitempty"`
	H        int    `json:"h,omitempty"`
	FileName string `json:"file_name,omitempty"`
}

type PhotoSize struct {
	Kind   string `json:"kind"`
	Type   string `json:"type"`
	W      int    `json:"w,omitempty"`
	H      int    `json:"h,omitempty"`
	Size   int    `json:"size,omitempty"`
	Path   string `json:"path,omitempty"`
	SHA256 string `json:"sha256,omitempty"`
}

type ColorOption struct {
	ID              int       `json:"id"`
	Hidden          bool      `json:"hidden"`
	ChannelMinLevel int       `json:"channel_min_level"`
	GroupMinLevel   int       `json:"group_min_level"`
	Colors          *ColorSet `json:"colors"`
	DarkColors      *ColorSet `json:"dark_colors"`
}

type ColorSet struct {
	PredicateName string `json:"predicate_name"`
	Colors        []int  `json:"colors"`
	PaletteColors []int  `json:"palette_colors"`
	BgColors      []int  `json:"bg_colors"`
	StoryColors   []int  `json:"story_colors"`
}
