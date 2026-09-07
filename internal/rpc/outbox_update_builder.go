package rpc

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/iamxvbaba/td/tg"

	appusers "telesrv/internal/app/users"
	"telesrv/internal/domain"
	"telesrv/internal/egress"
	"telesrv/internal/store"
)

const (
	maxSparseOutboxRecoveryCalls      = 64
	maxSparseOutboxAttemptedUserEdges = 524288
)

var (
	ErrSparseOutboxUserProjectionMissing    = errors.New("sparse outbox user projection is required")
	ErrSparseOutboxUserProjectionIncomplete = errors.New("sparse outbox user projection is incomplete")
	ErrOutboxUpdateProjectionEmpty          = errors.New("non-noop outbox event produced no update")
)

// buildOutboxUpdates constructs receiver-specific typed TL updates inside the
// RPC projection package. Cross-package Durable Egress callers must use the
// encoded bytes boundary below.
func (r *Router) buildOutboxUpdates(ctx context.Context, requests []egress.OutboxUpdateRequest) ([]*tg.Updates, error) {
	out := make([]*tg.Updates, len(requests))
	if len(requests) == 0 {
		return out, nil
	}
	cache := newViewerPeerCache(r)
	groups := make(map[int64][]outboxUpdateBuildItem)
	userIDsByViewer := make(map[int64]map[int64]struct{})
	for i, req := range requests {
		viewerUserID := req.TargetUserID
		if viewerUserID == 0 {
			viewerUserID = req.Event.UserID
		}
		event := req.Event
		if event.UserID == 0 {
			event.UserID = viewerUserID
		}
		groups[viewerUserID] = append(groups[viewerUserID], outboxUpdateBuildItem{index: i, event: event})
	}
	// Poll and draft events can replace their message with an authoritative
	// viewer-specific snapshot. Prepare every event before collecting refs so
	// the sparse batch includes peers introduced by that reload.
	for viewerUserID, items := range groups {
		events := make([]domain.UpdateEvent, len(items))
		for i := range items {
			events[i] = items[i].event
		}
		events = r.prepareUpdateEventsForViewer(ctx, viewerUserID, events)
		for i, event := range events {
			items[i].event = event
			refs := collectOutboxEventUserRefs(event)
			if len(refs) == 0 {
				continue
			}
			if userIDsByViewer[viewerUserID] == nil {
				userIDsByViewer[viewerUserID] = make(map[int64]struct{}, len(refs))
			}
			for id := range refs {
				if _, system := domain.SystemUserByID(id); !system {
					userIDsByViewer[viewerUserID][id] = struct{}{}
				}
			}
		}
		groups[viewerUserID] = items
	}
	if len(userIDsByViewer) > 0 {
		requested := make(map[int64][]int64, len(userIDsByViewer))
		for viewerID, ids := range userIDsByViewer {
			requested[viewerID] = sortedOutboxUserIDs(ids)
		}
		projected, err := r.resolveCachedSparseOutboxUsers(ctx, requested)
		if err != nil {
			return nil, fmt.Errorf("sparse outbox user projection: %w", err)
		}
		if r.outboxPresence != nil {
			online, err := r.resolveOutboxPresenceBatch(ctx, requested)
			if err != nil {
				return nil, fmt.Errorf("batch outbox presence projection: %w", err)
			}
			for viewerID, users := range projected {
				projected[viewerID] = r.withUsersPresenceSnapshot(users, online)
			}
			cache.markOutboxPresenceComplete()
		}
		for viewerID, expectedIDs := range requested {
			if missingID, missing := missingProjectedUserID(expectedIDs, projected[viewerID]); missing {
				return nil, fmt.Errorf("%w: viewer_user_id=%d missing_user_id=%d", ErrSparseOutboxUserProjectionIncomplete, viewerID, missingID)
			}
			cache.primeExpectedUsers(viewerID, expectedIDs, projected[viewerID])
		}
	}
	for viewerUserID, items := range groups {
		events := make([]domain.UpdateEvent, len(items))
		for i, item := range items {
			events[i] = item.event
		}
		var err error
		events, err = r.enrichPreparedUpdateEventsWithPeerCacheStrict(ctx, viewerUserID, events, cache)
		if err != nil {
			return nil, fmt.Errorf("strict outbox user projection for viewer %d: %w", viewerUserID, err)
		}
		for i, item := range items {
			update := tgUpdateForOutboxEventForViewer(events[i], viewerUserID)
			if update == nil && events[i].Type != domain.UpdateEventNoop {
				return nil, fmt.Errorf("%w: viewer_user_id=%d event_type=%s pts=%d", ErrOutboxUpdateProjectionEmpty, viewerUserID, events[i].Type, events[i].Pts)
			}
			if peers := storyUpdateEventPeers(events[i]); len(peers) > 0 {
				update = r.withStoryUpdatePeerObjectsForOutboxWithCache(ctx, viewerUserID, update, cache, peers...)
			}
			out[item.index] = update
		}
	}
	// Username rows are viewer-independent. Project the union once after every
	// viewer-specific update has been built so one outbox claim never turns into
	// a registry query per event/session.
	r.applyUsernamesToUpdatesBatch(ctx, out)
	return out, nil
}

func (r *Router) resolveOutboxPresenceBatch(ctx context.Context, requested map[int64][]int64) (map[int64]bool, error) {
	if r == nil || r.outboxPresence == nil {
		return nil, ErrOutboxPresenceBatchUnavailable
	}
	set := make(map[int64]struct{})
	for _, userIDs := range requested {
		for _, userID := range userIDs {
			if userID > 0 {
				set[userID] = struct{}{}
			}
		}
	}
	userIDs := make([]int64, 0, len(set))
	for userID := range set {
		userIDs = append(userIDs, userID)
	}
	sort.Slice(userIDs, func(i, j int) bool { return userIDs[i] < userIDs[j] })
	if len(userIDs) == 0 {
		return map[int64]bool{}, nil
	}
	recordsByUser, err := r.outboxPresence.UserLocationRecordsForUsers(ctx, userIDs)
	if err != nil {
		return nil, err
	}
	online := make(map[int64]bool, len(userIDs))
	for _, userID := range userIDs {
		for _, record := range recordsByUser[userID] {
			if record.UserID != userID {
				return nil, fmt.Errorf("location record user_id=%d requested=%d", record.UserID, userID)
			}
			if record.ReceivesUpdates {
				online[userID] = true
				break
			}
		}
	}
	return online, nil
}

// resolveCachedSparseOutboxUsers resolves the stable portion of each
// (viewer,target) projection. Cached values never contain a runtime presence
// overlay; viewerPeerCache applies current presence after it has been primed.
// Misses remain one strict sparse batch and never fall back to scalar reads.
func (r *Router) resolveCachedSparseOutboxUsers(ctx context.Context, requested map[int64][]int64) (map[int64][]domain.User, error) {
	projected := make(map[int64][]domain.User, len(requested))
	missing := make(map[int64][]int64)
	loadEpoch := uint64(0)
	if r.receiverUserProjectionCache != nil {
		loadEpoch = r.receiverUserProjectionCache.LoadEpoch()
	}
	for viewerID, targetIDs := range requested {
		for _, targetID := range targetIDs {
			if user, ok := r.receiverUserProjectionCache.Lookup(viewerID, targetID); ok {
				projected[viewerID] = append(projected[viewerID], user)
				continue
			}
			missing[viewerID] = append(missing[viewerID], targetID)
		}
	}
	if len(missing) == 0 {
		return projected, nil
	}
	resolver, ok := r.deps.Users.(SparseBatchViewerUsersResolver)
	if !ok {
		return nil, ErrSparseOutboxUserProjectionMissing
	}
	loaded, err := resolveSparseOutboxUsers(ctx, resolver, missing)
	if err != nil {
		return nil, err
	}
	// Validate the complete durable envelope before publishing any part of the
	// batch into the cross-call cache. A partial resolver response is an error,
	// not a negative cache entry or a reason to issue scalar compatibility reads.
	for viewerID, expectedIDs := range missing {
		if missingID, absent := missingProjectedUserID(expectedIDs, loaded[viewerID]); absent {
			return nil, fmt.Errorf("%w: viewer_user_id=%d missing_user_id=%d", ErrSparseOutboxUserProjectionIncomplete, viewerID, missingID)
		}
	}
	for viewerID, users := range loaded {
		for _, user := range users {
			projected[viewerID] = append(projected[viewerID], user)
			r.receiverUserProjectionCache.StoreIfEpoch(viewerID, user.ID, user, loadEpoch)
		}
	}
	return projected, nil
}

func resolveSparseOutboxUsers(ctx context.Context, resolver SparseBatchViewerUsersResolver, requested map[int64][]int64) (map[int64][]domain.User, error) {
	budget := userProjectionRecoveryBudget{
		maxCalls: maxSparseOutboxRecoveryCalls,
		maxItems: maxSparseOutboxAttemptedUserEdges,
	}
	return resolveSparseOutboxUsersWithBudget(ctx, resolver, requested, &budget)
}

func resolveSparseOutboxUsersWithBudget(ctx context.Context, resolver SparseBatchViewerUsersResolver, requested map[int64][]int64, budget *userProjectionRecoveryBudget) (map[int64][]domain.User, error) {
	edges := sparseOutboxRequestedEdgeCount(requested, maxSparseOutboxAttemptedUserEdges+1)
	if err := budget.consume(edges); err != nil {
		return nil, err
	}
	projected, err := resolver.ByIDsForViewerUserIDs(ctx, requested)
	if err == nil {
		return projected, nil
	}
	if !isSparseProjectionCapacityError(err) {
		return nil, err
	}
	left, right, ok := splitSparseOutboxUserEdges(requested)
	if !ok {
		return nil, err
	}
	leftProjected, leftErr := resolveSparseOutboxUsersWithBudget(ctx, resolver, left, budget)
	if leftErr != nil {
		return nil, leftErr
	}
	rightProjected, rightErr := resolveSparseOutboxUsersWithBudget(ctx, resolver, right, budget)
	if rightErr != nil {
		return nil, rightErr
	}
	if leftProjected == nil {
		leftProjected = make(map[int64][]domain.User)
	}
	for viewerID, users := range rightProjected {
		leftProjected[viewerID] = append(leftProjected[viewerID], users...)
	}
	return leftProjected, nil
}

func sparseOutboxRequestedEdgeCount(requested map[int64][]int64, limit int) int {
	if limit <= 0 {
		return 0
	}
	total := 0
	for viewerID, userIDs := range requested {
		if viewerID == 0 || len(userIDs) == 0 {
			continue
		}
		if len(userIDs) >= limit-total {
			return limit
		}
		total += len(userIDs)
	}
	return total
}

func isSparseProjectionCapacityError(err error) bool {
	return errors.Is(err, store.ErrActiveChannelMemberPairsLimit) ||
		errors.Is(err, appusers.ErrBatchUsersLimit) ||
		errors.Is(err, appusers.ErrBatchViewerCells)
}

func splitSparseOutboxUserEdges(requested map[int64][]int64) (map[int64][]int64, map[int64][]int64, bool) {
	viewerIDs := make([]int64, 0, len(requested))
	total := 0
	for viewerID, userIDs := range requested {
		if viewerID == 0 || len(userIDs) == 0 {
			continue
		}
		viewerIDs = append(viewerIDs, viewerID)
		total += len(userIDs)
	}
	if total < 2 {
		return nil, nil, false
	}
	sort.Slice(viewerIDs, func(i, j int) bool { return viewerIDs[i] < viewerIDs[j] })
	left := make(map[int64][]int64)
	right := make(map[int64][]int64)
	leftCount := total / 2
	seen := 0
	for _, viewerID := range viewerIDs {
		for _, userID := range requested[viewerID] {
			dst := right
			if seen < leftCount {
				dst = left
			}
			dst[viewerID] = append(dst[viewerID], userID)
			seen++
		}
	}
	return left, right, len(left) > 0 && len(right) > 0
}

// BuildOutboxUpdateBytes is the Durable Egress projection boundary. It keeps
// typed TL construction inside RPC projection code and gives Egress immutable
// wire bytes to hand to Edge.
func (r *Router) BuildOutboxUpdateBytes(ctx context.Context, requests []egress.OutboxUpdateRequest) ([][]byte, error) {
	updates, err := r.buildOutboxUpdates(ctx, requests)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, len(updates))
	for i, update := range updates {
		if update == nil {
			continue
		}
		raw, err := encodeDeliveryUpdate(update)
		if err != nil {
			return nil, fmt.Errorf("encode outbox update %d: %w", i, err)
		}
		out[i] = raw
	}
	return out, nil
}

// BuildChannelUpdateBytes constructs the deterministic channel-PTS nudge at
// the only allowed typed TL projection boundary. Date is deliberately zero:
// exact-owner replay must reproduce byte-identical payloads from immutable
// channel_id/PTS identity instead of sampling a new wall clock.
func (r *Router) BuildChannelUpdateBytes(ctx context.Context, requests []egress.ChannelUpdateRequest) ([][]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	out := make([][]byte, len(requests))
	for i, request := range requests {
		if request.ChannelID <= 0 || request.PTS <= 0 {
			return nil, fmt.Errorf("invalid channel delivery projection at index %d", i)
		}
		update := &tg.UpdateChannelTooLong{ChannelID: request.ChannelID}
		update.SetPts(request.PTS)
		raw, err := encodeDeliveryUpdate(&tg.Updates{
			Updates: []tg.UpdateClass{update}, Users: []tg.UserClass{}, Chats: []tg.ChatClass{}, Date: 0, Seq: 0,
		})
		if err != nil {
			return nil, fmt.Errorf("encode channel delivery projection %d: %w", i, err)
		}
		out[i] = raw
	}
	return out, nil
}

type outboxUpdateBuildItem struct {
	index int
	event domain.UpdateEvent
}

func collectOutboxEventUserRefs(event domain.UpdateEvent) map[int64]struct{} {
	userIDs := make(map[int64]struct{})
	channelIDs := make(map[int64]struct{})
	// Durable event envelopes can carry users that are not discoverable from
	// the event payload (including rows written by older constructors). Resolve
	// those IDs for the current viewer and replace the raw envelope below rather
	// than allowing account-scoped phone/access-hash fields to pass through.
	for _, user := range event.Users {
		if user.ID != 0 {
			userIDs[user.ID] = struct{}{}
		}
	}
	addDomainPeerRef(event.Peer, 0, userIDs, channelIDs)
	for _, peer := range event.Peers {
		addDomainPeerRef(peer, 0, userIDs, channelIDs)
	}
	addDomainPeerRef(event.Story.Owner, 0, userIDs, channelIDs)
	for _, peer := range storyForwardPeers(event.Story) {
		addDomainPeerRef(peer, 0, userIDs, channelIDs)
	}
	collectMessagePeerRefs(event.Message, 0, userIDs, channelIDs)
	if message := event.EphemeralMessage; message != nil {
		collectEphemeralMessagePeerRefs(*message, userIDs, channelIDs)
		if message.BotAPIReply != nil {
			collectEphemeralMessagePeerRefs(*message.BotAPIReply, userIDs, channelIDs)
		}
	}
	if event.BotCallbackQuery != nil && event.BotCallbackQuery.UserID != 0 {
		userIDs[event.BotCallbackQuery.UserID] = struct{}{}
	}
	collectDialogDraftPeerRefs(event.Draft, userIDs, channelIDs)
	return userIDs
}

func sortedOutboxUserIDs(ids map[int64]struct{}) []int64 {
	out := make([]int64, 0, len(ids))
	for id := range ids {
		if id != 0 {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}
