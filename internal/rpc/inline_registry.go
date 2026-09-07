package rpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

var errInlineRegistryStoreRequired = errors.New("INLINE_REGISTRY_STORE_REQUIRED")

type inlineRegistry struct {
	mu              sync.Mutex
	pending         map[int64]*pendingInlineQuery
	cache           map[inlineCacheKey]cachedInlineResults
	prepared        map[string]preparedInlineMessage
	webDocuments    map[inlineWebDocumentKey]registeredInlineWebDocument
	ttl             time.Duration
	maxCacheTTL     time.Duration
	maxCacheEntries int
	shared          store.InlineRegistryStore
}

type pendingInlineQuery struct {
	ch        chan domain.BotInlineResults
	botUserID int64
	userID    int64
	peer      domain.Peer
	createdAt time.Time
	cacheKey  inlineCacheKey
	results   *domain.BotInlineResults
}

type inlineCacheKey struct {
	botUserID   int64
	userID      int64
	peer        domain.Peer
	query       string
	offset      string
	hasGeo      bool
	geoLat      float64
	geoLong     float64
	geoAccuracy int
}

type cachedInlineResults struct {
	results   domain.BotInlineResults
	expiresAt time.Time
	storedAt  time.Time
}

type preparedInlineMessage struct {
	id        string
	botUserID int64
	userID    int64
	results   domain.BotInlineResults
	expiresAt time.Time
}

type inlineWebDocumentKey struct {
	url        string
	accessHash int64
}

type registeredInlineWebDocument struct {
	document  domain.BotInlineWebDocument
	bytes     []byte
	mimeType  string
	expiresAt time.Time
}

func newInlineRegistry(ttl time.Duration, shared ...store.InlineRegistryStore) *inlineRegistry {
	if ttl <= 0 {
		ttl = botInlineQueryTTL
	}
	var sharedStore store.InlineRegistryStore
	if len(shared) > 0 {
		sharedStore = shared[0]
	}
	return &inlineRegistry{
		pending:         make(map[int64]*pendingInlineQuery),
		cache:           make(map[inlineCacheKey]cachedInlineResults),
		prepared:        make(map[string]preparedInlineMessage),
		webDocuments:    make(map[inlineWebDocumentKey]registeredInlineWebDocument),
		ttl:             ttl,
		maxCacheTTL:     botInlineCacheMaxTTL,
		maxCacheEntries: botInlineCacheMaxEntries,
		shared:          sharedStore,
	}
}

func (r *inlineRegistry) register(now time.Time, botUserID, userID int64, peer domain.Peer) (int64, *pendingInlineQuery) {
	queryID, pending, _ := r.registerWithCacheKeyContext(context.Background(), now, botUserID, userID, peer, inlineCacheKey{})
	return queryID, pending
}

func (r *inlineRegistry) registerWithCacheKey(now time.Time, botUserID, userID int64, peer domain.Peer, key inlineCacheKey) (int64, *pendingInlineQuery) {
	queryID, pending, _ := r.registerWithCacheKeyContext(context.Background(), now, botUserID, userID, peer, key)
	return queryID, pending
}

func (r *inlineRegistry) registerWithCacheKeyContext(ctx context.Context, now time.Time, botUserID, userID int64, peer domain.Peer, key inlineCacheKey) (int64, *pendingInlineQuery, error) {
	if r == nil || r.shared == nil {
		return 0, nil, errInlineRegistryStoreRequired
	}
	p := &pendingInlineQuery{
		ch:        make(chan domain.BotInlineResults, 1),
		botUserID: botUserID,
		userID:    userID,
		peer:      peer,
		createdAt: now,
		cacheKey:  key,
	}
	r.mu.Lock()
	r.pruneLocked(now)
	var queryID int64
	for {
		queryID = randomNonZeroInt64()
		if _, exists := r.pending[queryID]; !exists {
			break
		}
	}
	r.pending[queryID] = p
	r.mu.Unlock()
	if err := r.putSharedPending(ctx, queryID, p); err != nil {
		r.removePending(queryID, p)
		return 0, nil, err
	}
	return queryID, p, nil
}

func (r *inlineRegistry) cachedContext(ctx context.Context, now time.Time, key inlineCacheKey) (domain.BotInlineResults, bool) {
	if key.botUserID == 0 {
		return domain.BotInlineResults{}, false
	}
	r.mu.Lock()
	r.pruneLocked(now)
	cached, ok := r.cache[key]
	if ok {
		if !cached.expiresAt.After(now) {
			delete(r.cache, key)
		} else {
			out := cloneInlineResults(cached.results)
			r.mu.Unlock()
			clampInlineCacheTime(&out, cached.expiresAt.Sub(now))
			return out, true
		}
	}
	r.mu.Unlock()
	if r.shared == nil {
		return domain.BotInlineResults{}, false
	}
	out, ok, remaining, err := r.shared.GetInlineCache(ctx, storeInlineCacheKey(key))
	if err != nil || !ok {
		return domain.BotInlineResults{}, false
	}
	out = cloneInlineResults(out)
	clampInlineCacheTime(&out, remaining)
	return out, true
}

func (r *inlineRegistry) registerCachedContext(ctx context.Context, now time.Time, botUserID, userID int64, peer domain.Peer, results domain.BotInlineResults) (domain.BotInlineResults, error) {
	if r == nil || r.shared == nil {
		return domain.BotInlineResults{}, errInlineRegistryStoreRequired
	}
	clone := cloneInlineResults(results)
	clone.BotUserID = botUserID
	clone.UserID = userID
	clone.Peer = peer
	p := &pendingInlineQuery{
		ch:        make(chan domain.BotInlineResults, 1),
		botUserID: botUserID,
		userID:    userID,
		peer:      peer,
		createdAt: now,
		results:   &clone,
	}
	r.mu.Lock()
	r.pruneLocked(now)
	var queryID int64
	for {
		queryID = randomNonZeroInt64()
		if _, exists := r.pending[queryID]; !exists {
			break
		}
	}
	clone.QueryID = queryID
	r.pending[queryID] = p
	r.storeWebDocumentsLocked(now, clone)
	r.mu.Unlock()
	if err := r.putSharedWebDocuments(ctx, now, clone); err != nil {
		r.removePending(queryID, p)
		return domain.BotInlineResults{}, err
	}
	if err := r.putSharedResult(ctx, clone, r.ttl); err != nil {
		r.removePending(queryID, p)
		return domain.BotInlineResults{}, err
	}
	return clone, nil
}

func (r *inlineRegistry) deregisterIfUnansweredContext(ctx context.Context, queryID int64) {
	deleteShared := false
	r.mu.Lock()
	if p, ok := r.pending[queryID]; ok && p.results == nil {
		delete(r.pending, queryID)
		deleteShared = true
	}
	r.mu.Unlock()
	if deleteShared && r.shared != nil {
		_ = r.shared.DeleteInlinePending(ctx, queryID)
	}
}

func (r *inlineRegistry) resolve(now time.Time, callerBotID, queryID int64, results domain.BotInlineResults) bool {
	resolved, _ := r.resolveContext(context.Background(), now, callerBotID, queryID, results)
	return resolved
}

func (r *inlineRegistry) resolveContext(ctx context.Context, now time.Time, callerBotID, queryID int64, results domain.BotInlineResults) (bool, error) {
	if r == nil || r.shared == nil {
		return false, errInlineRegistryStoreRequired
	}
	r.mu.Lock()
	r.pruneLocked(now)
	p, ok := r.pending[queryID]
	if ok && p.botUserID == callerBotID {
		results.QueryID = queryID
		results.BotUserID = p.botUserID
		results.UserID = p.userID
		results.Peer = p.peer
		results.Query = p.cacheKey.query
		cacheKey := p.cacheKey
		if p.cacheKey.hasGeo {
			results.Geo = &domain.MessageGeoPoint{
				Lat:            p.cacheKey.geoLat,
				Long:           p.cacheKey.geoLong,
				AccuracyRadius: p.cacheKey.geoAccuracy,
			}
		}
		clone := cloneInlineResults(results)
		r.mu.Unlock()
		if err := r.putSharedResolved(ctx, now, cacheKey, clone); err != nil {
			return false, err
		}
		r.mu.Lock()
		if current, exists := r.pending[queryID]; exists && current == p {
			current.results = &clone
			r.storeWebDocumentsLocked(now, clone)
			r.storeCacheLocked(now, cacheKey, clone)
		}
		r.mu.Unlock()
		select {
		case p.ch <- clone:
		default:
		}
		return true, nil
	}
	r.mu.Unlock()
	sharedPending, found, err := r.shared.GetInlinePending(ctx, queryID)
	if err != nil || !found || sharedPending.BotUserID != callerBotID {
		return false, err
	}
	results.QueryID = queryID
	results.BotUserID = sharedPending.BotUserID
	results.UserID = sharedPending.UserID
	results.Peer = sharedPending.Peer
	results.Query = sharedPending.CacheKey.Query
	if sharedPending.CacheKey.HasGeo {
		results.Geo = &domain.MessageGeoPoint{
			Lat:            sharedPending.CacheKey.GeoLat,
			Long:           sharedPending.CacheKey.GeoLong,
			AccuracyRadius: sharedPending.CacheKey.GeoAccuracy,
		}
	}
	clone := cloneInlineResults(results)
	if err := r.putSharedResolved(ctx, now, inlineCacheKeyFromStore(sharedPending.CacheKey), clone); err != nil {
		return false, err
	}
	return true, nil
}

func (r *inlineRegistry) resultsForQueryContext(ctx context.Context, now time.Time, userID, queryID int64) (domain.BotInlineResults, bool, error) {
	if r == nil || r.shared == nil {
		return domain.BotInlineResults{}, false, errInlineRegistryStoreRequired
	}
	r.mu.Lock()
	r.pruneLocked(now)
	p, ok := r.pending[queryID]
	if ok && p.userID == userID && p.results != nil {
		results := cloneInlineResults(*p.results)
		r.mu.Unlock()
		return results, true, nil
	}
	r.mu.Unlock()
	results, found, err := r.shared.GetInlineResult(ctx, queryID)
	if err != nil || !found || results.UserID != userID {
		return domain.BotInlineResults{}, false, err
	}
	return cloneInlineResults(results), true, nil
}

func (r *inlineRegistry) waitResultsContext(ctx context.Context, now time.Time, userID, queryID int64) (domain.BotInlineResults, bool, error) {
	results, found, err := r.resultsForQueryContext(ctx, now, userID, queryID)
	if err != nil || found {
		return results, found, err
	}
	if r == nil || r.shared == nil {
		return domain.BotInlineResults{}, false, errInlineRegistryStoreRequired
	}
	results, found, err = r.shared.WaitInlineResult(ctx, userID, queryID)
	if err != nil || !found {
		return domain.BotInlineResults{}, found, err
	}
	return cloneInlineResults(results), true, nil
}

func (r *inlineRegistry) resultForSendContext(ctx context.Context, now time.Time, userID, queryID int64, id string) (domain.BotInlineResults, domain.BotInlineResult, bool, error) {
	if r == nil || r.shared == nil {
		return domain.BotInlineResults{}, domain.BotInlineResult{}, false, errInlineRegistryStoreRequired
	}
	r.mu.Lock()
	r.pruneLocked(now)
	p, ok := r.pending[queryID]
	if ok && p.userID == userID && p.results != nil {
		results, result, found := inlineResultByID(*p.results, id)
		r.mu.Unlock()
		return results, result, found, nil
	}
	r.mu.Unlock()
	results, found, err := r.shared.GetInlineResult(ctx, queryID)
	if err != nil || !found || results.UserID != userID {
		return domain.BotInlineResults{}, domain.BotInlineResult{}, false, err
	}
	results, result, ok := inlineResultByID(results, id)
	return results, result, ok, nil
}

func (r *inlineRegistry) savePreparedInlineContext(ctx context.Context, now time.Time, botUserID, userID int64, result domain.BotInlineResult, peerTypes []string) (string, int, error) {
	if r == nil || r.shared == nil {
		return "", 0, errInlineRegistryStoreRequired
	}
	ttl := r.ttl
	if ttl <= 0 {
		ttl = botInlineQueryTTL
	}
	results := domain.BotInlineResults{
		BotUserID: botUserID,
		UserID:    userID,
		Results:   []domain.BotInlineResult{result},
		CacheTime: int(ttl.Seconds()),
		PeerTypes: append([]string(nil), peerTypes...),
	}
	expiresAt := now.Add(ttl)
	r.mu.Lock()
	r.pruneLocked(now)
	id := ""
	for i := 0; i < 16; i++ {
		candidate := randomPreparedInlineID()
		if _, exists := r.prepared[candidate]; !exists {
			id = candidate
			break
		}
	}
	if id == "" {
		id = fmt.Sprintf("%016x%016x", uint64(now.UnixNano()), uint64(randomNonZeroInt64()))
	}
	msg := preparedInlineMessage{
		id:        id,
		botUserID: botUserID,
		userID:    userID,
		results:   cloneInlineResults(results),
		expiresAt: expiresAt,
	}
	r.prepared[id] = msg
	r.storeWebDocumentsLocked(now, msg.results)
	r.mu.Unlock()
	if err := r.putSharedWebDocuments(ctx, now, msg.results); err != nil {
		r.removePrepared(id)
		return "", 0, err
	}
	if err := r.putSharedPrepared(ctx, msg, ttl); err != nil {
		r.removePrepared(id)
		return "", 0, err
	}
	return id, int(expiresAt.Unix()), nil
}

func (r *inlineRegistry) preparedInlineContext(ctx context.Context, now time.Time, userID, botUserID int64, id string) (domain.BotInlineResults, bool, error) {
	if r == nil || r.shared == nil {
		return domain.BotInlineResults{}, false, errInlineRegistryStoreRequired
	}
	var results domain.BotInlineResults
	found := false
	r.mu.Lock()
	r.pruneLocked(now)
	if msg, ok := r.prepared[id]; ok {
		if msg.expiresAt.After(now) && msg.userID == userID && msg.botUserID == botUserID {
			results = cloneInlineResults(msg.results)
			found = true
		}
	}
	r.mu.Unlock()
	if found {
		registered, err := r.registerCachedContext(ctx, now, botUserID, userID, domain.Peer{}, results)
		return registered, err == nil, err
	}
	shared, ok, err := r.shared.GetPreparedInlineMessage(ctx, id)
	if err != nil || !ok || shared.UserID != userID || shared.BotUserID != botUserID || !shared.ExpiresAt.After(now) {
		return domain.BotInlineResults{}, false, err
	}
	results = cloneInlineResults(shared.Results)
	registered, err := r.registerCachedContext(ctx, now, botUserID, userID, domain.Peer{}, results)
	return registered, err == nil, err
}

func (r *inlineRegistry) consume(queryID int64) {
	r.consumeContext(context.Background(), queryID)
}

func (r *inlineRegistry) consumeContext(ctx context.Context, queryID int64) {
	r.mu.Lock()
	delete(r.pending, queryID)
	r.mu.Unlock()
	if r.shared != nil {
		_ = r.shared.DeleteInlinePending(ctx, queryID)
		_ = r.shared.DeleteInlineResult(ctx, queryID)
	}
}

func (r *inlineRegistry) removePending(queryID int64, pending *pendingInlineQuery) {
	r.mu.Lock()
	if current, ok := r.pending[queryID]; ok && (pending == nil || current == pending) {
		delete(r.pending, queryID)
	}
	r.mu.Unlock()
}

func (r *inlineRegistry) removePrepared(id string) {
	r.mu.Lock()
	delete(r.prepared, id)
	r.mu.Unlock()
}

func (r *inlineRegistry) unansweredSize() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, p := range r.pending {
		if p.results == nil {
			count++
		}
	}
	return count
}

func (r *inlineRegistry) pruneLocked(now time.Time) {
	if r.ttl > 0 {
		for queryID, p := range r.pending {
			if now.Sub(p.createdAt) > r.ttl {
				delete(r.pending, queryID)
			}
		}
	}
	for key, cached := range r.cache {
		if !cached.expiresAt.After(now) {
			delete(r.cache, key)
		}
	}
	for id, msg := range r.prepared {
		if !msg.expiresAt.After(now) {
			delete(r.prepared, id)
		}
	}
	for key, document := range r.webDocuments {
		if !document.expiresAt.After(now) {
			delete(r.webDocuments, key)
		}
	}
}

func (r *inlineRegistry) storeCacheLocked(now time.Time, key inlineCacheKey, results domain.BotInlineResults) {
	ttl, ok := r.cacheTTL(results)
	if key.botUserID == 0 || !ok || r.maxCacheEntries <= 0 {
		return
	}
	cached := cloneInlineResults(results)
	cached.QueryID = 0
	r.dropOldestCacheEntryLocked(now)
	r.cache[key] = cachedInlineResults{
		results:   cached,
		expiresAt: now.Add(ttl),
		storedAt:  now,
	}
}

func (r *inlineRegistry) dropOldestCacheEntryLocked(now time.Time) {
	if len(r.cache) < r.maxCacheEntries {
		return
	}
	var (
		oldestKey inlineCacheKey
		oldestAt  time.Time
		found     bool
	)
	for key, cached := range r.cache {
		if !cached.expiresAt.After(now) {
			delete(r.cache, key)
			continue
		}
		if !found || cached.storedAt.Before(oldestAt) {
			oldestKey = key
			oldestAt = cached.storedAt
			found = true
		}
	}
	if len(r.cache) >= r.maxCacheEntries && found {
		delete(r.cache, oldestKey)
	}
}

func (r *inlineRegistry) storeWebDocumentsLocked(now time.Time, results domain.BotInlineResults) {
	expiresAt := now.Add(r.webDocumentTTL(results))
	for _, item := range results.Results {
		r.storeWebDocumentLocked(item.Thumb, expiresAt)
		r.storeWebDocumentLocked(item.Content, expiresAt)
	}
}

func (r *inlineRegistry) storeWebDocumentLocked(document *domain.BotInlineWebDocument, expiresAt time.Time) {
	if document == nil || document.URL == "" || document.AccessHash == 0 {
		return
	}
	key := inlineWebDocumentKey{url: document.URL, accessHash: document.AccessHash}
	clone := cloneInlineWebDocument(*document)
	existing := r.webDocuments[key]
	r.webDocuments[key] = registeredInlineWebDocument{
		document:  clone,
		bytes:     append([]byte(nil), existing.bytes...),
		mimeType:  existing.mimeType,
		expiresAt: expiresAt,
	}
}

func (r *inlineRegistry) webDocumentForDownloadContext(ctx context.Context, now time.Time, url string, accessHash int64) (domain.BotInlineWebDocument, []byte, string, bool, error) {
	if r == nil || r.shared == nil {
		return domain.BotInlineWebDocument{}, nil, "", false, errInlineRegistryStoreRequired
	}
	r.mu.Lock()
	r.pruneLocked(now)
	registered, ok := r.webDocuments[inlineWebDocumentKey{url: url, accessHash: accessHash}]
	if ok {
		r.mu.Unlock()
		return cloneInlineWebDocument(registered.document), append([]byte(nil), registered.bytes...), registered.mimeType, true, nil
	}
	r.mu.Unlock()
	shared, ok, err := r.shared.GetInlineWebDocument(ctx, store.InlineWebDocumentKey{URL: url, AccessHash: accessHash})
	if err != nil || !ok {
		return domain.BotInlineWebDocument{}, nil, "", false, err
	}
	return cloneInlineWebDocument(shared.Document), append([]byte(nil), shared.Bytes...), shared.MimeType, true, nil
}

func (r *inlineRegistry) cacheWebDocumentBytesContext(ctx context.Context, now time.Time, url string, accessHash int64, data []byte, mimeType string) (bool, error) {
	if r == nil || r.shared == nil {
		return false, errInlineRegistryStoreRequired
	}
	if len(data) > domain.MaxBotInlineWebSize {
		return false, nil
	}
	localUpdated := false
	r.mu.Lock()
	r.pruneLocked(now)
	key := inlineWebDocumentKey{url: url, accessHash: accessHash}
	registered, ok := r.webDocuments[key]
	if ok {
		registered.bytes = append([]byte(nil), data...)
		registered.mimeType = mimeType
		r.webDocuments[key] = registered
		localUpdated = true
	}
	r.mu.Unlock()
	err := r.shared.PutInlineWebDocumentBytes(ctx, store.InlineWebDocumentKey{URL: url, AccessHash: accessHash}, append([]byte(nil), data...), mimeType, r.ttl)
	return localUpdated || err == nil, err
}

func (r *inlineRegistry) registerWebDocumentContext(ctx context.Context, now time.Time, document domain.BotInlineWebDocument, ttl time.Duration) error {
	if r == nil || r.shared == nil {
		return errInlineRegistryStoreRequired
	}
	if document.URL == "" || document.AccessHash == 0 {
		return nil
	}
	if ttl <= 0 {
		ttl = r.ttl
	}
	r.mu.Lock()
	r.pruneLocked(now)
	r.storeWebDocumentLocked(&document, now.Add(ttl))
	r.mu.Unlock()
	return r.putSharedWebDocument(ctx, &document, ttl)
}

func (r *inlineRegistry) putSharedPending(ctx context.Context, queryID int64, p *pendingInlineQuery) error {
	if r.shared == nil {
		return errInlineRegistryStoreRequired
	}
	return r.shared.PutInlinePending(ctx, store.InlinePending{
		QueryID:   queryID,
		BotUserID: p.botUserID,
		UserID:    p.userID,
		Peer:      p.peer,
		CacheKey:  storeInlineCacheKey(p.cacheKey),
		CreatedAt: p.createdAt,
	}, r.ttl)
}

func (r *inlineRegistry) putSharedResolved(ctx context.Context, now time.Time, key inlineCacheKey, results domain.BotInlineResults) error {
	if err := r.putSharedWebDocuments(ctx, now, results); err != nil {
		return err
	}
	if err := r.putSharedCache(ctx, key, results); err != nil {
		return err
	}
	return r.putSharedResult(ctx, results, r.ttl)
}

func (r *inlineRegistry) putSharedResult(ctx context.Context, results domain.BotInlineResults, ttl time.Duration) error {
	if r.shared == nil {
		return errInlineRegistryStoreRequired
	}
	if ttl <= 0 {
		return nil
	}
	return r.shared.PutInlineResult(ctx, cloneInlineResults(results), ttl)
}

func (r *inlineRegistry) putSharedCache(ctx context.Context, key inlineCacheKey, results domain.BotInlineResults) error {
	if r.shared == nil {
		return errInlineRegistryStoreRequired
	}
	if key.botUserID == 0 {
		return nil
	}
	ttl, ok := r.cacheTTL(results)
	if !ok {
		return nil
	}
	cached := cloneInlineResults(results)
	cached.QueryID = 0
	return r.shared.PutInlineCache(ctx, storeInlineCacheKey(key), cached, ttl)
}

func (r *inlineRegistry) putSharedWebDocuments(ctx context.Context, now time.Time, results domain.BotInlineResults) error {
	if r.shared == nil {
		return errInlineRegistryStoreRequired
	}
	ttl := r.webDocumentTTL(results)
	if ttl <= 0 {
		return nil
	}
	for _, item := range results.Results {
		if err := r.putSharedWebDocument(ctx, item.Thumb, ttl); err != nil {
			return err
		}
		if err := r.putSharedWebDocument(ctx, item.Content, ttl); err != nil {
			return err
		}
	}
	_ = now
	return nil
}

func (r *inlineRegistry) putSharedWebDocument(ctx context.Context, document *domain.BotInlineWebDocument, ttl time.Duration) error {
	if r.shared == nil {
		return errInlineRegistryStoreRequired
	}
	if document == nil || document.URL == "" || document.AccessHash == 0 || ttl <= 0 {
		return nil
	}
	return r.shared.PutInlineWebDocument(ctx, cloneInlineWebDocument(*document), ttl)
}

func (r *inlineRegistry) putSharedPrepared(ctx context.Context, msg preparedInlineMessage, ttl time.Duration) error {
	if r.shared == nil {
		return errInlineRegistryStoreRequired
	}
	if ttl <= 0 {
		return nil
	}
	return r.shared.PutPreparedInlineMessage(ctx, store.PreparedInlineMessage{
		ID:        msg.id,
		BotUserID: msg.botUserID,
		UserID:    msg.userID,
		Results:   cloneInlineResults(msg.results),
		ExpiresAt: msg.expiresAt,
	}, ttl)
}

func (r *inlineRegistry) cacheTTL(results domain.BotInlineResults) (time.Duration, bool) {
	if results.Private || results.CacheTime <= 0 || r.maxCacheTTL <= 0 {
		return 0, false
	}
	ttl := time.Duration(results.CacheTime) * time.Second
	if ttl <= 0 {
		return 0, false
	}
	if ttl > r.maxCacheTTL {
		ttl = r.maxCacheTTL
	}
	return ttl, true
}

func (r *inlineRegistry) webDocumentTTL(results domain.BotInlineResults) time.Duration {
	ttl := r.ttl
	if cacheTTL, ok := r.cacheTTL(results); ok && cacheTTL > ttl {
		ttl = cacheTTL
	}
	return ttl
}

func inlineResultByID(results domain.BotInlineResults, id string) (domain.BotInlineResults, domain.BotInlineResult, bool) {
	for _, item := range results.Results {
		if item.ID == id {
			return cloneInlineResults(results), cloneInlineResult(item), true
		}
	}
	return domain.BotInlineResults{}, domain.BotInlineResult{}, false
}

func randomPreparedInlineID() string {
	return fmt.Sprintf("%016x%016x", uint64(randomNonZeroInt64()), uint64(randomNonZeroInt64()))
}

func clampInlineCacheTime(results *domain.BotInlineResults, remaining time.Duration) {
	seconds := int(remaining.Seconds())
	if seconds <= 0 {
		seconds = 1
	}
	if results.CacheTime <= 0 || seconds < results.CacheTime {
		results.CacheTime = seconds
	}
}

func storeInlineCacheKey(key inlineCacheKey) store.InlineCacheKey {
	return store.InlineCacheKey{
		BotUserID:   key.botUserID,
		UserID:      key.userID,
		Peer:        key.peer,
		Query:       key.query,
		Offset:      key.offset,
		HasGeo:      key.hasGeo,
		GeoLat:      key.geoLat,
		GeoLong:     key.geoLong,
		GeoAccuracy: key.geoAccuracy,
	}
}

func inlineCacheKeyFromStore(key store.InlineCacheKey) inlineCacheKey {
	return inlineCacheKey{
		botUserID:   key.BotUserID,
		userID:      key.UserID,
		peer:        key.Peer,
		query:       key.Query,
		offset:      key.Offset,
		hasGeo:      key.HasGeo,
		geoLat:      key.GeoLat,
		geoLong:     key.GeoLong,
		geoAccuracy: key.GeoAccuracy,
	}
}

func cloneInlineResults(in domain.BotInlineResults) domain.BotInlineResults {
	if in.Geo != nil {
		geo := *in.Geo
		in.Geo = &geo
	}
	if in.SwitchPM != nil {
		switchPM := *in.SwitchPM
		in.SwitchPM = &switchPM
	}
	if in.SwitchWeb != nil {
		switchWeb := *in.SwitchWeb
		in.SwitchWeb = &switchWeb
	}
	in.PeerTypes = append([]string(nil), in.PeerTypes...)
	in.Results = append([]domain.BotInlineResult(nil), in.Results...)
	for i := range in.Results {
		in.Results[i] = cloneInlineResult(in.Results[i])
	}
	return in
}

func cloneInlineResult(in domain.BotInlineResult) domain.BotInlineResult {
	in.Entities = append([]domain.MessageEntity(nil), in.Entities...)
	if in.Thumb != nil {
		thumb := cloneInlineWebDocument(*in.Thumb)
		in.Thumb = &thumb
	}
	if in.Content != nil {
		content := cloneInlineWebDocument(*in.Content)
		in.Content = &content
	}
	in.ReplyMarkup = cloneInlineReplyMarkup(in.ReplyMarkup)
	in.Media = cloneInlineMedia(in.Media)
	return in
}

func cloneInlineWebDocument(in domain.BotInlineWebDocument) domain.BotInlineWebDocument {
	in.Attributes = append([]domain.DocumentAttribute(nil), in.Attributes...)
	return in
}

func cloneInlineReplyMarkup(in *domain.MessageReplyMarkup) *domain.MessageReplyMarkup {
	if in == nil {
		return nil
	}
	out := domain.MessageReplyMarkup{}
	if in.Inline != nil {
		out.Inline = make([][]domain.MarkupButton, len(in.Inline))
		for i, row := range in.Inline {
			out.Inline[i] = make([]domain.MarkupButton, len(row))
			for j, button := range row {
				out.Inline[i][j] = button
				out.Inline[i][j].Data = append([]byte(nil), button.Data...)
			}
		}
	}
	return &out
}

func cloneInlineMedia(in *domain.MessageMedia) *domain.MessageMedia {
	if in == nil {
		return nil
	}
	out := *in
	if in.Photo != nil {
		photo := *in.Photo
		photo.FileReference = append([]byte(nil), in.Photo.FileReference...)
		photo.Sizes = cloneInlinePhotoSizes(in.Photo.Sizes)
		out.Photo = &photo
	}
	if in.Document != nil {
		doc := *in.Document
		doc.FileReference = append([]byte(nil), in.Document.FileReference...)
		doc.Attributes = append([]domain.DocumentAttribute(nil), in.Document.Attributes...)
		doc.Thumbs = cloneInlinePhotoSizes(in.Document.Thumbs)
		out.Document = &doc
	}
	if in.Geo != nil {
		geo := *in.Geo
		out.Geo = &geo
	}
	if in.Venue != nil {
		venue := *in.Venue
		out.Venue = &venue
	}
	if in.Contact != nil {
		contact := *in.Contact
		out.Contact = &contact
	}
	return &out
}

func cloneInlinePhotoSizes(in []domain.PhotoSize) []domain.PhotoSize {
	if len(in) == 0 {
		return nil
	}
	out := append([]domain.PhotoSize(nil), in...)
	for i := range out {
		out[i].Bytes = append([]byte(nil), in[i].Bytes...)
		out[i].Sizes = append([]int(nil), in[i].Sizes...)
		out[i].BackgroundColors = append([]int(nil), in[i].BackgroundColors...)
	}
	return out
}
