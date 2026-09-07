package rpc

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

const webViewSessionTTL = 2 * time.Minute

type webViewRegistry struct {
	ttl    time.Duration
	shared store.InlineRegistryStore
}

func newWebViewRegistry(ttl time.Duration, shared ...store.InlineRegistryStore) *webViewRegistry {
	if ttl <= 0 {
		ttl = webViewSessionTTL
	}
	var sharedStore store.InlineRegistryStore
	if len(shared) > 0 {
		sharedStore = shared[0]
	}
	return &webViewRegistry{
		ttl:    ttl,
		shared: sharedStore,
	}
}

func (r *webViewRegistry) registerContext(ctx context.Context, now time.Time, session store.WebViewSession) (store.WebViewSession, error) {
	if r.shared == nil {
		return store.WebViewSession{}, fmt.Errorf("webview registry shared store is required")
	}
	for attempts := 0; attempts < 32; attempts++ {
		session.QueryID = randomNonZeroInt64()
		session.BotQueryID = strconv.FormatInt(session.QueryID, 10)
		session.CreatedAt = now
		session.ExpiresAt = now.Add(r.ttl)
		created, err := r.shared.ReserveWebViewSession(ctx, cloneWebViewSession(session), r.ttl)
		if err != nil {
			return store.WebViewSession{}, err
		}
		if created {
			return cloneWebViewSession(session), nil
		}
	}
	return store.WebViewSession{}, fmt.Errorf("allocate webview query id")
}

func (r *webViewRegistry) prolongContext(ctx context.Context, now time.Time, queryID int64, userID, botUserID int64, peer domain.Peer, silent bool, replyTo *domain.MessageReply, sendAs *domain.Peer) bool {
	if r.shared == nil {
		return false
	}
	session, found, err := r.shared.GetWebViewSession(ctx, queryID)
	if err != nil || !found || session.UserID != userID || session.BotUserID != botUserID || session.Peer != peer {
		return false
	}
	session.Silent = silent
	session.ReplyTo = cloneMessageReply(replyTo)
	session.SendAs = clonePeerPtr(sendAs)
	session.ExpiresAt = now.Add(r.ttl)
	return r.shared.PutWebViewSession(ctx, cloneWebViewSession(session), r.ttl) == nil
}

func (r *webViewRegistry) sessionForBotQueryContext(ctx context.Context, now time.Time, botUserID int64, botQueryID string) (store.WebViewSession, bool) {
	if r.shared == nil {
		return store.WebViewSession{}, false
	}
	session, found, err := r.shared.GetWebViewSessionByBotQuery(ctx, botQueryID)
	if err != nil || !found || session.BotUserID != botUserID {
		return store.WebViewSession{}, false
	}
	return cloneWebViewSession(session), true
}

func (r *webViewRegistry) consumeContext(ctx context.Context, queryID int64, botQueryID string) {
	if r.shared != nil {
		_ = r.shared.DeleteWebViewSession(ctx, queryID, botQueryID)
	}
}

func cloneWebViewSession(in store.WebViewSession) store.WebViewSession {
	in.ReplyTo = cloneMessageReply(in.ReplyTo)
	in.SendAs = clonePeerPtr(in.SendAs)
	return in
}

func clonePeerPtr(in *domain.Peer) *domain.Peer {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func cloneMessageReply(reply *domain.MessageReply) *domain.MessageReply {
	return domain.CloneMessageReply(reply)
}
