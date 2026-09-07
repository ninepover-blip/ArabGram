package domain

import "errors"

// 自定义云主题(account.createTheme 等)相关错误。
var (
	ErrThemeNotFound      = errors.New("theme not found")
	ErrThemeInvalid       = errors.New("theme invalid")
	ErrThemeSlugTaken     = errors.New("theme slug taken")
	ErrThemeFormatInvalid = errors.New("theme format invalid")
)

// ThemeBaseKind 对应 TL baseTheme 的五个变体(classic/day/night/tinted/arctic)。
// 客户端按当前 base theme 选取匹配的 ThemeSettings。
type ThemeBaseKind string

const (
	ThemeBaseClassic ThemeBaseKind = "classic"
	ThemeBaseDay     ThemeBaseKind = "day"
	ThemeBaseNight   ThemeBaseKind = "night"
	ThemeBaseTinted  ThemeBaseKind = "tinted"
	ThemeBaseArctic  ThemeBaseKind = "arctic"
)

// ThemeWallpaperSpec 是一份主题设置里的纯渐变墙纸(对应 wallPaperNoFile + wallPaperSettings)。
// 自定义云主题只用纯色渐变(无下载),与静态目录一致。
type ThemeWallpaperSpec struct {
	BackgroundColors []int  `json:"background_colors,omitempty"` // 最多 4 个
	Intensity        int    `json:"intensity,omitempty"`
	Rotation         int    `json:"rotation,omitempty"`
	Blur             bool   `json:"blur,omitempty"`
	Motion           bool   `json:"motion,omitempty"`
	Emoticon         string `json:"emoticon,omitempty"`
	Dark             bool   `json:"dark,omitempty"`
}

// WallpaperSettings 是频道/私聊 wallpaper 的 domain-only 设置。Has* 字段保留 TL
// conditional flag 语义，避免黑色(0)这类合法取值在持久化时丢失。
type WallpaperSettings struct {
	Blur                     bool   `json:"blur,omitempty"`
	Motion                   bool   `json:"motion,omitempty"`
	HasBackgroundColor       bool   `json:"has_background_color,omitempty"`
	BackgroundColor          int    `json:"background_color,omitempty"`
	HasSecondBackgroundColor bool   `json:"has_second_background_color,omitempty"`
	SecondBackgroundColor    int    `json:"second_background_color,omitempty"`
	HasThirdBackgroundColor  bool   `json:"has_third_background_color,omitempty"`
	ThirdBackgroundColor     int    `json:"third_background_color,omitempty"`
	HasFourthBackgroundColor bool   `json:"has_fourth_background_color,omitempty"`
	FourthBackgroundColor    int    `json:"fourth_background_color,omitempty"`
	HasIntensity             bool   `json:"has_intensity,omitempty"`
	Intensity                int    `json:"intensity,omitempty"`
	HasRotation              bool   `json:"has_rotation,omitempty"`
	Rotation                 int    `json:"rotation,omitempty"`
	HasEmoticon              bool   `json:"has_emoticon,omitempty"`
	Emoticon                 string `json:"emoticon,omitempty"`
}

// Empty reports whether no explicit wallpaper rendering settings are present.
func (s WallpaperSettings) Empty() bool {
	return !s.Blur &&
		!s.Motion &&
		!s.HasBackgroundColor &&
		!s.HasSecondBackgroundColor &&
		!s.HasThirdBackgroundColor &&
		!s.HasFourthBackgroundColor &&
		!s.HasIntensity &&
		!s.HasRotation &&
		!s.HasEmoticon
}

// Wallpaper 是频道/私聊当前 wallpaper 的 domain-only 描述。NoFile=true 对应
// wallPaperNoFile；否则 ID+AccessHash/Slug 引用 catalog/document wallpaper。
type Wallpaper struct {
	ID         int64             `json:"id,omitempty"`
	AccessHash int64             `json:"access_hash,omitempty"`
	Slug       string            `json:"slug,omitempty"`
	NoFile     bool              `json:"no_file,omitempty"`
	Default    bool              `json:"default,omitempty"`
	Pattern    bool              `json:"pattern,omitempty"`
	Dark       bool              `json:"dark,omitempty"`
	Settings   WallpaperSettings `json:"settings,omitempty"`
}

// CloneWallpaperPtr returns a detached wallpaper pointer.
func CloneWallpaperPtr(in *Wallpaper) *Wallpaper {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

// WallpaperEqual compares optional wallpapers by value.
func WallpaperEqual(a, b *Wallpaper) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

// ThemeSettingsSpec 是一个 base theme 下的配色(对应 themeSettings/inputThemeSettings)。
// BaseTheme 与 AccentColor 必填(TL 编码器要求 base_theme 非空)。
type ThemeSettingsSpec struct {
	BaseTheme             ThemeBaseKind       `json:"base_theme"`
	AccentColor           int                 `json:"accent_color"`
	OutboxAccentColor     int                 `json:"outbox_accent_color,omitempty"`
	HasOutboxAccent       bool                `json:"has_outbox_accent,omitempty"`
	MessageColors         []int               `json:"message_colors,omitempty"`
	MessageColorsAnimated bool                `json:"message_colors_animated,omitempty"`
	Wallpaper             *ThemeWallpaperSpec `json:"wallpaper,omitempty"`
}

// Theme 是一份持久化的自定义云主题。document_id 软引用 documents(无硬外键),
// settings 仅 accent 主题非空;完整 .attheme 主题靠 DocumentID 下载。
type Theme struct {
	ID            int64
	AccessHash    int64
	CreatorUserID int64
	Slug          string
	Title         string
	Emoticon      string
	ForChat       bool
	DocumentID    int64
	Settings      []ThemeSettingsSpec
	InstallsCount int
	CreatedAt     int64 // unix 秒
}

// Clone 深拷贝(切片字段),避免内存 store 暴露内部引用。
func (t Theme) Clone() Theme {
	out := t
	out.Settings = cloneThemeSettings(t.Settings)
	return out
}

func cloneThemeSettings(in []ThemeSettingsSpec) []ThemeSettingsSpec {
	if in == nil {
		return nil
	}
	out := make([]ThemeSettingsSpec, len(in))
	for i, s := range in {
		out[i] = s
		out[i].MessageColors = append([]int(nil), s.MessageColors...)
		if s.Wallpaper != nil {
			wp := *s.Wallpaper
			wp.BackgroundColors = append([]int(nil), s.Wallpaper.BackgroundColors...)
			out[i].Wallpaper = &wp
		}
	}
	return out
}

// IsCreator 报告 userID 是否为该主题的创建者(决定客户端再次上传走 create 还是 update)。
func (t Theme) IsCreator(userID int64) bool {
	return t.CreatorUserID != 0 && t.CreatorUserID == userID
}

// ThemeRef 引用一份主题:按 id+access_hash(inputTheme)或 slug(inputThemeSlug,深链)。
type ThemeRef struct {
	ID         int64
	AccessHash int64
	Slug       string
}

// IsZero 报告该引用是否为空(既无 id 也无 slug)。
func (r ThemeRef) IsZero() bool { return r.ID == 0 && r.Slug == "" }

// ThemeSpec 是 createTheme 的输入。Slug 为空表示由服务端自动分配。
type ThemeSpec struct {
	CreatorUserID int64
	Slug          string
	Title         string
	Emoticon      string
	ForChat       bool
	DocumentID    int64
	Settings      []ThemeSettingsSpec
}

// ThemeUpdate 是 updateTheme 的部分更新;nil 字段表示不改。
type ThemeUpdate struct {
	Slug       *string
	Title      *string
	DocumentID *int64
	Settings   *[]ThemeSettingsSpec
}
