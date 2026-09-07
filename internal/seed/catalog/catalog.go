// Package catalog 提供从官方 Telegram 拉取并固化的「目录/参考数据」种子:
// 国家区号(help.getCountriesList)、时区(help.getTimezonesList)、emoji 分组
// (messages.getEmojiGroups)、emoji 关键词(messages.getEmojiKeywords)。
//
// 数据由 cmd/catalogfetch 经官方 API 拉取后写入本目录的 JSON,
// 经 go:embed 进二进制。改了 JSON 即改了内容 hash,客户端发旧 hash 会被驱动重取。
// 各 accessor 在数据为空(未 seed)时返回空集,由调用方决定是否回退到内置最小数据。
package catalog

import (
	_ "embed"
	"encoding/json"
	"hash/fnv"
	"sync"

	"telesrv/internal/domain"
)

//go:embed countries.json
var countriesJSON []byte

//go:embed timezones.json
var timezonesJSON []byte

//go:embed emoji_groups.json
var emojiGroupsJSON []byte

//go:embed emoji_keywords_en.json
var emojiKeywordsENJSON []byte

type countriesFile struct {
	Countries []countryJSON `json:"countries"`
}
type countryJSON struct {
	ISO2         string            `json:"iso2"`
	DefaultName  string            `json:"default_name"`
	Name         string            `json:"name"`
	Hidden       bool              `json:"hidden"`
	CountryCodes []countryCodeJSON `json:"country_codes"`
}
type countryCodeJSON struct {
	CountryCode string   `json:"country_code"`
	Prefixes    []string `json:"prefixes"`
	Patterns    []string `json:"patterns"`
}

// Timezone 是 help.getTimezonesList 的一条时区。
type Timezone struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	UTCOffset int    `json:"utc_offset"`
}
type timezonesFile struct {
	Timezones []Timezone `json:"timezones"`
}

// EmojiGroup 是 messages.getEmojiGroups 的一个分类。IconEmojiID 指向一个 custom-emoji
// 文档(本地若无对应文档,客户端分类图标回退为占位,不影响分组本身)。
type EmojiGroup struct {
	Title       string   `json:"title"`
	IconEmojiID int64    `json:"icon_emoji_id"`
	Emoticons   []string `json:"emoticons"`
}
type emojiGroupsFile struct {
	Groups []EmojiGroup `json:"groups"`
}

// EmojiKeyword 是 emoji 关键词词典的一条:一个关键词 → 一组 emoji。
type EmojiKeyword struct {
	Keyword   string   `json:"keyword"`
	Emoticons []string `json:"emoticons"`
}

// EmojiKeywordSet 是某语言的整份 emoji 关键词词典。
type EmojiKeywordSet struct {
	LangCode string         `json:"lang_code"`
	Version  int            `json:"version"`
	Keywords []EmojiKeyword `json:"keywords"`
}

var (
	once             sync.Once
	countries        domain.CountriesList
	timezones        []Timezone
	timezonesHash    int
	emojiGroups      []EmojiGroup
	emojiGroupsHash  int
	emojiKeywordSets map[string]EmojiKeywordSet
)

func load() {
	once.Do(func() {
		var cf countriesFile
		_ = json.Unmarshal(countriesJSON, &cf)
		list := make([]domain.Country, 0, len(cf.Countries))
		for _, c := range cf.Countries {
			codes := make([]domain.CountryCode, 0, len(c.CountryCodes))
			for _, cc := range c.CountryCodes {
				codes = append(codes, domain.CountryCode{
					CountryCode: cc.CountryCode,
					Prefixes:    cc.Prefixes,
					Patterns:    cc.Patterns,
				})
			}
			list = append(list, domain.Country{
				ISO2:         c.ISO2,
				DefaultName:  c.DefaultName,
				Name:         c.Name,
				Hidden:       c.Hidden,
				CountryCodes: codes,
			})
		}
		countries = domain.CountriesList{Hash: hashBytes(countriesJSON), Countries: list}

		var tf timezonesFile
		_ = json.Unmarshal(timezonesJSON, &tf)
		timezones = tf.Timezones
		timezonesHash = hashBytes(timezonesJSON)

		var ef emojiGroupsFile
		_ = json.Unmarshal(emojiGroupsJSON, &ef)
		emojiGroups = ef.Groups
		emojiGroupsHash = hashBytes(emojiGroupsJSON)

		emojiKeywordSets = map[string]EmojiKeywordSet{}
		for _, raw := range [][]byte{emojiKeywordsENJSON} {
			var ks EmojiKeywordSet
			if err := json.Unmarshal(raw, &ks); err == nil && ks.LangCode != "" && len(ks.Keywords) > 0 {
				emojiKeywordSets[ks.LangCode] = ks
			}
		}
	})
}

// hashBytes 把内容映射为一个稳定的正整数 hash(FNV-1a 32),供 NotModified 协商。
// 内容变了 hash 必变,客户端发旧 hash 即不命中→重取。
func hashBytes(b []byte) int {
	h := fnv.New32a()
	_, _ = h.Write(b)
	return int(h.Sum32() & 0x7fffffff)
}

// Countries 返回固化的国家区号目录(空表示未 seed,调用方应回退内置最小集)。
func Countries() domain.CountriesList {
	load()
	return countries
}

// Timezones 返回时区目录与其内容 hash(空表示未 seed)。
func Timezones() ([]Timezone, int) {
	load()
	return timezones, timezonesHash
}

// EmojiGroups 返回 emoji 分类目录与其内容 hash(空表示未 seed)。
func EmojiGroups() ([]EmojiGroup, int) {
	load()
	return emojiGroups, emojiGroupsHash
}

// EmojiKeywords 返回指定语言的 emoji 关键词词典;ok=false 表示该语言未 seed。
func EmojiKeywords(lang string) (EmojiKeywordSet, bool) {
	load()
	ks, ok := emojiKeywordSets[lang]
	return ks, ok
}
