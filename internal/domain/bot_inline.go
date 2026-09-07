package domain

const (
	MaxBotInlineResults       = 50
	MaxBotInlineResultIDLen   = 64
	MaxBotInlineNextOffsetLen = 64
	MaxBotInlineSwitchTextLen = 256
	MaxBotInlineWebURLLen     = 2048
	MaxBotInlineWebMimeLen    = 128
	MaxBotInlineWebSize       = 20 * 1024 * 1024
	MaxBotPreparedInlineIDLen = 128
)

type BotInlineResult struct {
	ID          string
	Type        string
	Title       string
	Description string
	URL         string
	Thumb       *BotInlineWebDocument
	Content     *BotInlineWebDocument
	Message     string
	Entities    []MessageEntity
	ReplyMarkup *MessageReplyMarkup
	NoWebpage   bool
	MediaAuto   bool
	Media       *MessageMedia
}

type BotInlineWebDocument struct {
	URL        string
	AccessHash int64
	Size       int
	MimeType   string
	Attributes []DocumentAttribute
}

type BotInlineResults struct {
	QueryID    int64
	BotUserID  int64
	UserID     int64
	Peer       Peer
	Query      string
	Geo        *MessageGeoPoint
	Gallery    bool
	Private    bool
	Results    []BotInlineResult
	CacheTime  int
	NextOffset string
	SwitchPM   *BotInlineSwitchPM
	SwitchWeb  *BotInlineSwitchWebView
	PeerTypes  []string
}

type BotInlineSwitchPM struct {
	Text       string
	StartParam string
}

type BotInlineSwitchWebView struct {
	Text string
	URL  string
}
