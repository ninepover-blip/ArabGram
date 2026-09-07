package redisstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

type InlineRegistryStore struct {
	c *redis.Client
}

func NewInlineRegistryStore(c *redis.Client) *InlineRegistryStore {
	return &InlineRegistryStore{c: c}
}

const inlineBotQueryChannel = "inline:bot_query"

func inlinePendingKey(queryID int64) string {
	return fmt.Sprintf("inline:pending:%d", queryID)
}

func inlineResultKey(queryID int64) string {
	return fmt.Sprintf("inline:result:%d", queryID)
}

func inlineResultWakeKey(queryID int64) string {
	return fmt.Sprintf("inline:result:%d:wake", queryID)
}

func inlineCacheKey(key store.InlineCacheKey) (string, error) {
	raw, err := json.Marshal(key)
	if err != nil {
		return "", fmt.Errorf("marshal inline cache key: %w", err)
	}
	sum := sha256.Sum256(raw)
	return "inline:cache:" + hex.EncodeToString(sum[:]), nil
}

func inlineWebDocumentKey(key store.InlineWebDocumentKey) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d", key.URL, key.AccessHash)))
	return "inline:webdoc:" + hex.EncodeToString(sum[:])
}

func preparedInlineMessageKey(id string) string {
	sum := sha256.Sum256([]byte(id))
	return "inline:prepared:" + hex.EncodeToString(sum[:])
}

func webViewSessionKey(queryID int64) string {
	return fmt.Sprintf("webview:session:%d", queryID)
}

func webViewBotQueryKey(botQueryID string) string {
	sum := sha256.Sum256([]byte(botQueryID))
	return "webview:bot_query:" + hex.EncodeToString(sum[:])
}

var reserveWebViewSessionScript = redis.NewScript(`
local exists = redis.call("EXISTS", KEYS[1], KEYS[2])
if exists ~= 0 then
	return 0
end
redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
redis.call("SET", KEYS[2], ARGV[1], "PX", ARGV[2])
return 1
`)

func (s *InlineRegistryStore) PutInlinePending(ctx context.Context, pending store.InlinePending, ttl time.Duration) error {
	if s == nil || s.c == nil || pending.QueryID == 0 || pending.BotUserID <= 0 || pending.UserID <= 0 || ttl <= 0 {
		return fmt.Errorf("invalid inline pending")
	}
	raw, err := json.Marshal(pending)
	if err != nil {
		return fmt.Errorf("marshal inline pending: %w", err)
	}
	created, err := s.c.SetNX(ctx, inlinePendingKey(pending.QueryID), raw, ttl).Result()
	if err != nil {
		return fmt.Errorf("redis set inline pending: %w", err)
	}
	if !created {
		return fmt.Errorf("inline pending query id already exists")
	}
	return nil
}

func (s *InlineRegistryStore) GetInlinePending(ctx context.Context, queryID int64) (store.InlinePending, bool, error) {
	key := inlinePendingKey(queryID)
	var pending store.InlinePending
	found, err := redisGetJSON(ctx, s.c, key, &pending)
	if err != nil || !found {
		return store.InlinePending{}, false, err
	}
	if pending.QueryID != queryID || pending.BotUserID == 0 || pending.UserID == 0 {
		_ = s.c.Del(ctx, key).Err()
		return store.InlinePending{}, false, nil
	}
	return pending, true, nil
}

func (s *InlineRegistryStore) DeleteInlinePending(ctx context.Context, queryID int64) error {
	if err := s.c.Del(ctx, inlinePendingKey(queryID)).Err(); err != nil {
		return fmt.Errorf("redis delete inline pending: %w", err)
	}
	return nil
}

func (s *InlineRegistryStore) PutInlineResult(ctx context.Context, results domain.BotInlineResults, ttl time.Duration) error {
	if s == nil || s.c == nil || results.QueryID == 0 || results.BotUserID <= 0 || results.UserID <= 0 || ttl <= 0 {
		return fmt.Errorf("invalid inline result")
	}
	raw, err := json.Marshal(results)
	if err != nil {
		return fmt.Errorf("marshal inline result: %w", err)
	}
	if err := putInlineResultScript.Run(ctx, s.c, []string{inlineResultKey(results.QueryID), inlineResultWakeKey(results.QueryID)},
		raw, ttl.Milliseconds()).Err(); err != nil {
		return fmt.Errorf("redis set inline result: %w", err)
	}
	return nil
}

var putInlineResultScript = redis.NewScript(`
redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
redis.call("LPUSH", KEYS[2], "1")
redis.call("PEXPIRE", KEYS[2], ARGV[2])
return 1
`)

func (s *InlineRegistryStore) GetInlineResult(ctx context.Context, queryID int64) (domain.BotInlineResults, bool, error) {
	key := inlineResultKey(queryID)
	var results domain.BotInlineResults
	found, err := redisGetJSON(ctx, s.c, key, &results)
	if err != nil || !found {
		return domain.BotInlineResults{}, false, err
	}
	if results.QueryID != queryID || results.UserID == 0 || results.BotUserID == 0 {
		_ = s.c.Del(ctx, key).Err()
		return domain.BotInlineResults{}, false, nil
	}
	return results, true, nil
}

func (s *InlineRegistryStore) WaitInlineResult(ctx context.Context, userID, queryID int64) (domain.BotInlineResults, bool, error) {
	if s == nil || s.c == nil || userID <= 0 || queryID == 0 {
		return domain.BotInlineResults{}, false, nil
	}
	for {
		results, found, err := s.GetInlineResult(ctx, queryID)
		if err != nil {
			return domain.BotInlineResults{}, false, err
		}
		if found {
			if results.UserID != userID {
				return domain.BotInlineResults{}, false, nil
			}
			return results, true, nil
		}
		_, err = s.c.BLPop(ctx, 0, inlineResultWakeKey(queryID)).Result()
		if err == nil {
			continue
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
			return domain.BotInlineResults{}, false, nil
		}
		if err == redis.Nil {
			continue
		}
		return domain.BotInlineResults{}, false, fmt.Errorf("wait inline result: %w", err)
	}
}

func (s *InlineRegistryStore) DeleteInlineResult(ctx context.Context, queryID int64) error {
	if s == nil || s.c == nil || queryID == 0 {
		return nil
	}
	if err := s.c.Del(ctx, inlineResultKey(queryID), inlineResultWakeKey(queryID)).Err(); err != nil {
		return fmt.Errorf("redis delete inline result: %w", err)
	}
	return nil
}

func (s *InlineRegistryStore) PutInlineCache(ctx context.Context, key store.InlineCacheKey, results domain.BotInlineResults, ttl time.Duration) error {
	redisKey, err := inlineCacheKey(key)
	if err != nil {
		return err
	}
	results.QueryID = 0
	raw, err := json.Marshal(results)
	if err != nil {
		return fmt.Errorf("marshal inline cache: %w", err)
	}
	if err := s.c.Set(ctx, redisKey, raw, ttl).Err(); err != nil {
		return fmt.Errorf("redis set inline cache: %w", err)
	}
	return nil
}

func (s *InlineRegistryStore) GetInlineCache(ctx context.Context, key store.InlineCacheKey) (domain.BotInlineResults, bool, time.Duration, error) {
	redisKey, err := inlineCacheKey(key)
	if err != nil {
		return domain.BotInlineResults{}, false, 0, err
	}
	var results domain.BotInlineResults
	found, err := redisGetJSON(ctx, s.c, redisKey, &results)
	if err != nil || !found {
		return domain.BotInlineResults{}, false, 0, err
	}
	ttl, err := s.c.TTL(ctx, redisKey).Result()
	if err != nil {
		return domain.BotInlineResults{}, false, 0, fmt.Errorf("redis ttl inline cache: %w", err)
	}
	if ttl <= 0 {
		ttl = time.Second
	}
	return results, true, ttl, nil
}

func (s *InlineRegistryStore) PutInlineWebDocument(ctx context.Context, document domain.BotInlineWebDocument, ttl time.Duration) error {
	key := store.InlineWebDocumentKey{URL: document.URL, AccessHash: document.AccessHash}
	redisKey := inlineWebDocumentKey(key)
	entry := store.InlineWebDocumentEntry{Document: document}
	if existing, found, err := s.GetInlineWebDocument(ctx, key); err != nil {
		return err
	} else if found {
		entry.Bytes = append([]byte(nil), existing.Bytes...)
		entry.MimeType = existing.MimeType
	}
	raw, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal inline web document: %w", err)
	}
	if err := s.c.Set(ctx, redisKey, raw, ttl).Err(); err != nil {
		return fmt.Errorf("redis set inline web document: %w", err)
	}
	return nil
}

func (s *InlineRegistryStore) GetInlineWebDocument(ctx context.Context, key store.InlineWebDocumentKey) (store.InlineWebDocumentEntry, bool, error) {
	redisKey := inlineWebDocumentKey(key)
	var entry store.InlineWebDocumentEntry
	found, err := redisGetJSON(ctx, s.c, redisKey, &entry)
	if err != nil || !found {
		return store.InlineWebDocumentEntry{}, false, err
	}
	if entry.Document.URL != key.URL || entry.Document.AccessHash != key.AccessHash || entry.Document.URL == "" || entry.Document.AccessHash == 0 {
		_ = s.c.Del(ctx, redisKey).Err()
		return store.InlineWebDocumentEntry{}, false, nil
	}
	if len(entry.Bytes) > domain.MaxBotInlineWebSize {
		_ = s.c.Del(ctx, redisKey).Err()
		return store.InlineWebDocumentEntry{}, false, nil
	}
	entry.Bytes = append([]byte(nil), entry.Bytes...)
	return entry, true, nil
}

func (s *InlineRegistryStore) PutInlineWebDocumentBytes(ctx context.Context, key store.InlineWebDocumentKey, data []byte, mimeType string, ttl time.Duration) error {
	if len(data) == 0 || len(data) > domain.MaxBotInlineWebSize {
		return fmt.Errorf("inline web document bytes size %d out of range", len(data))
	}
	redisKey := inlineWebDocumentKey(key)
	entry, found, err := s.GetInlineWebDocument(ctx, key)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("inline web document missing")
	}
	entry.Bytes = append([]byte(nil), data...)
	entry.MimeType = mimeType
	raw, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal inline web document bytes: %w", err)
	}
	if currentTTL, err := s.c.TTL(ctx, redisKey).Result(); err == nil && currentTTL > 0 {
		ttl = currentTTL
	}
	if err := s.c.Set(ctx, redisKey, raw, ttl).Err(); err != nil {
		return fmt.Errorf("redis set inline web document bytes: %w", err)
	}
	return nil
}

func (s *InlineRegistryStore) PutPreparedInlineMessage(ctx context.Context, msg store.PreparedInlineMessage, ttl time.Duration) error {
	raw, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal prepared inline message: %w", err)
	}
	if err := s.c.Set(ctx, preparedInlineMessageKey(msg.ID), raw, ttl).Err(); err != nil {
		return fmt.Errorf("redis set prepared inline message: %w", err)
	}
	return nil
}

func (s *InlineRegistryStore) GetPreparedInlineMessage(ctx context.Context, id string) (store.PreparedInlineMessage, bool, error) {
	key := preparedInlineMessageKey(id)
	var msg store.PreparedInlineMessage
	found, err := redisGetJSON(ctx, s.c, key, &msg)
	if err != nil || !found {
		return store.PreparedInlineMessage{}, false, err
	}
	if msg.ID != id || msg.BotUserID == 0 || msg.UserID == 0 || len(msg.Results.Results) != 1 {
		_ = s.c.Del(ctx, key).Err()
		return store.PreparedInlineMessage{}, false, nil
	}
	msg.Results.Results = append([]domain.BotInlineResult(nil), msg.Results.Results...)
	return msg, true, nil
}

func (s *InlineRegistryStore) ReserveWebViewSession(ctx context.Context, session store.WebViewSession, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		return false, fmt.Errorf("webview session ttl must be positive")
	}
	raw, err := json.Marshal(session)
	if err != nil {
		return false, fmt.Errorf("marshal webview session: %w", err)
	}
	result, err := reserveWebViewSessionScript.Run(ctx, s.c,
		[]string{webViewSessionKey(session.QueryID), webViewBotQueryKey(session.BotQueryID)},
		raw, ttl.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("redis reserve webview session: %w", err)
	}
	return result == 1, nil
}

func (s *InlineRegistryStore) PutWebViewSession(ctx context.Context, session store.WebViewSession, ttl time.Duration) error {
	raw, err := json.Marshal(session)
	if err != nil {
		return fmt.Errorf("marshal webview session: %w", err)
	}
	if err := s.c.Set(ctx, webViewSessionKey(session.QueryID), raw, ttl).Err(); err != nil {
		return fmt.Errorf("redis set webview session: %w", err)
	}
	if err := s.c.Set(ctx, webViewBotQueryKey(session.BotQueryID), raw, ttl).Err(); err != nil {
		return fmt.Errorf("redis set webview bot query: %w", err)
	}
	return nil
}

func (s *InlineRegistryStore) GetWebViewSession(ctx context.Context, queryID int64) (store.WebViewSession, bool, error) {
	key := webViewSessionKey(queryID)
	var session store.WebViewSession
	found, err := redisGetJSON(ctx, s.c, key, &session)
	if err != nil || !found {
		return store.WebViewSession{}, false, err
	}
	if !validWebViewSession(session) || session.QueryID != queryID {
		_ = s.c.Del(ctx, key).Err()
		return store.WebViewSession{}, false, nil
	}
	return session, true, nil
}

func (s *InlineRegistryStore) GetWebViewSessionByBotQuery(ctx context.Context, botQueryID string) (store.WebViewSession, bool, error) {
	key := webViewBotQueryKey(botQueryID)
	var session store.WebViewSession
	found, err := redisGetJSON(ctx, s.c, key, &session)
	if err != nil || !found {
		return store.WebViewSession{}, false, err
	}
	if !validWebViewSession(session) || session.BotQueryID != botQueryID {
		_ = s.c.Del(ctx, key).Err()
		return store.WebViewSession{}, false, nil
	}
	return session, true, nil
}

func (s *InlineRegistryStore) DeleteWebViewSession(ctx context.Context, queryID int64, botQueryID string) error {
	if err := s.c.Del(ctx, webViewSessionKey(queryID), webViewBotQueryKey(botQueryID)).Err(); err != nil {
		return fmt.Errorf("redis delete webview session: %w", err)
	}
	return nil
}

func validWebViewSession(session store.WebViewSession) bool {
	return session.QueryID != 0 && session.BotQueryID != "" && session.BotUserID != 0 && session.UserID != 0 && session.Peer.ID != 0
}

func (s *InlineRegistryStore) PublishBotInlineQuery(ctx context.Context, event store.BotInlineQueryPush) error {
	if event.SourceID == "" || event.QueryID == 0 || event.BotUserID == 0 || event.UserID == 0 {
		return fmt.Errorf("inline bot query push missing identity")
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal inline bot query push: %w", err)
	}
	if err := s.c.Publish(ctx, inlineBotQueryChannel, raw).Err(); err != nil {
		return fmt.Errorf("redis publish inline bot query: %w", err)
	}
	return nil
}

func (s *InlineRegistryStore) SubscribeBotInlineQueries(ctx context.Context, handle func(context.Context, store.BotInlineQueryPush)) error {
	if handle == nil {
		return fmt.Errorf("inline bot query handler is nil")
	}
	pubsub := s.c.Subscribe(ctx, inlineBotQueryChannel)
	defer func() { _ = pubsub.Close() }()
	if _, err := pubsub.Receive(ctx); err != nil {
		return fmt.Errorf("redis subscribe inline bot query: %w", err)
	}
	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			var event store.BotInlineQueryPush
			if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
				continue
			}
			handle(ctx, event)
		}
	}
}

func redisGetJSON(ctx context.Context, c *redis.Client, key string, out any) (bool, error) {
	raw, err := c.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, fmt.Errorf("redis get %s: %w", key, err)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		_ = c.Del(ctx, key).Err()
		return false, nil
	}
	return true, nil
}
