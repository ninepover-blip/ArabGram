package store

import (
	"context"
	"time"

	"telesrv/internal/domain"
)

type InlineCacheKey struct {
	BotUserID   int64
	UserID      int64
	Peer        domain.Peer
	Query       string
	Offset      string
	HasGeo      bool
	GeoLat      float64
	GeoLong     float64
	GeoAccuracy int
}

type InlinePending struct {
	QueryID   int64
	BotUserID int64
	UserID    int64
	Peer      domain.Peer
	CacheKey  InlineCacheKey
	CreatedAt time.Time
}

type InlineWebDocumentKey struct {
	URL        string
	AccessHash int64
}

type InlineWebDocumentEntry struct {
	Document domain.BotInlineWebDocument
	Bytes    []byte
	MimeType string
}

type PreparedInlineMessage struct {
	ID        string
	BotUserID int64
	UserID    int64
	Results   domain.BotInlineResults
	ExpiresAt time.Time
}

type WebViewSession struct {
	QueryID      int64
	BotQueryID   string
	BotUserID    int64
	UserID       int64
	Peer         domain.Peer
	AppID        int64
	Source       string
	StartParam   string
	WriteAllowed bool
	Silent       bool
	ReplyTo      *domain.MessageReply
	SendAs       *domain.Peer
	CreatedAt    time.Time
	ExpiresAt    time.Time
}

const (
	InlineQueryPeerTypeSameBotPM = "same_bot_pm"
	InlineQueryPeerTypePM        = "pm"
	InlineQueryPeerTypeChat      = "chat"
	InlineQueryPeerTypeMegagroup = "megagroup"
	InlineQueryPeerTypeBroadcast = "broadcast"
	InlineQueryPeerTypeBotPM     = "bot_pm"
)

type BotInlineQueryPush struct {
	SourceID  string
	QueryID   int64
	BotUserID int64
	UserID    int64
	Query     string
	Offset    string
	PeerType  string
	Geo       *domain.MessageGeoPoint
	Date      int
}

type InlineRegistryStore interface {
	PutInlinePending(ctx context.Context, pending InlinePending, ttl time.Duration) error
	GetInlinePending(ctx context.Context, queryID int64) (InlinePending, bool, error)
	DeleteInlinePending(ctx context.Context, queryID int64) error

	PutInlineResult(ctx context.Context, results domain.BotInlineResults, ttl time.Duration) error
	GetInlineResult(ctx context.Context, queryID int64) (domain.BotInlineResults, bool, error)
	WaitInlineResult(ctx context.Context, userID, queryID int64) (domain.BotInlineResults, bool, error)
	DeleteInlineResult(ctx context.Context, queryID int64) error

	PutInlineCache(ctx context.Context, key InlineCacheKey, results domain.BotInlineResults, ttl time.Duration) error
	GetInlineCache(ctx context.Context, key InlineCacheKey) (domain.BotInlineResults, bool, time.Duration, error)

	PutInlineWebDocument(ctx context.Context, document domain.BotInlineWebDocument, ttl time.Duration) error
	GetInlineWebDocument(ctx context.Context, key InlineWebDocumentKey) (InlineWebDocumentEntry, bool, error)
	PutInlineWebDocumentBytes(ctx context.Context, key InlineWebDocumentKey, data []byte, mimeType string, ttl time.Duration) error

	PutPreparedInlineMessage(ctx context.Context, msg PreparedInlineMessage, ttl time.Duration) error
	GetPreparedInlineMessage(ctx context.Context, id string) (PreparedInlineMessage, bool, error)

	ReserveWebViewSession(ctx context.Context, session WebViewSession, ttl time.Duration) (bool, error)
	PutWebViewSession(ctx context.Context, session WebViewSession, ttl time.Duration) error
	GetWebViewSession(ctx context.Context, queryID int64) (WebViewSession, bool, error)
	GetWebViewSessionByBotQuery(ctx context.Context, botQueryID string) (WebViewSession, bool, error)
	DeleteWebViewSession(ctx context.Context, queryID int64, botQueryID string) error
}

type BotInlineQueryPushBroker interface {
	PublishBotInlineQuery(ctx context.Context, event BotInlineQueryPush) error
	SubscribeBotInlineQueries(ctx context.Context, handle func(context.Context, BotInlineQueryPush)) error
}
