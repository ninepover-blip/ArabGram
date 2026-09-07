package rpc

import (
	"context"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/domain"
	"telesrv/internal/edgecontrol"
)

const channelMembershipSyncPageSize = edgecontrol.MaxChannelMembershipSyncPage
const publicChannelSubscriptionTTL = 75 * time.Second

func (r *Router) trackChannelInterest(ctx context.Context, userID int64, channelIDs ...int64) {
	if userID == 0 || r.deps.Sessions == nil {
		return
	}
	provider, ok := r.deps.Sessions.(OnlineUserProvider)
	if !ok {
		return
	}
	rawAuthKeyID, ok := RawAuthKeyIDFrom(ctx)
	if !ok {
		return
	}
	sessionID, ok := SessionIDFrom(ctx)
	if !ok {
		return
	}
	if len(channelIDs) == 0 {
		provider.ClearChannelInterest(rawAuthKeyID, sessionID, userID)
		return
	}
	provider.TrackChannelInterest(rawAuthKeyID, sessionID, userID, channelIDs)
}

func (r *Router) clearChannelInterest(ctx context.Context, userID int64) {
	r.trackChannelInterest(ctx, userID)
}

func (r *Router) refreshPublicChannelSubscription(ctx context.Context, userID, channelID int64) {
	if userID == 0 || channelID == 0 || r.deps.Sessions == nil {
		return
	}
	provider, ok := r.deps.Sessions.(ChannelSubscriptionProvider)
	if !ok {
		return
	}
	rawAuthKeyID, ok := RawAuthKeyIDFrom(ctx)
	if !ok {
		return
	}
	sessionID, ok := SessionIDFrom(ctx)
	if !ok {
		return
	}
	provider.RefreshChannelSubscription(rawAuthKeyID, sessionID, userID, channelID, publicChannelSubscriptionTTL)
}

func (r *Router) syncSessionChannelMemberships(ctx context.Context, userID int64) {
	if userID == 0 || r.deps.Sessions == nil || r.deps.Channels == nil {
		return
	}
	rawAuthKeyID, ok := RawAuthKeyIDFrom(ctx)
	if !ok {
		return
	}
	sessionID, ok := SessionIDFrom(ctx)
	if !ok {
		return
	}
	r.syncSessionChannelMembershipsForSession(ctx, userID, rawAuthKeyID, sessionID)
}

func (r *Router) syncSessionChannelMembershipsForSession(ctx context.Context, userID int64, rawAuthKeyID [8]byte, sessionID int64) {
	if userID == 0 || rawAuthKeyID == ([8]byte{}) || sessionID == 0 || r.deps.Sessions == nil {
		return
	}
	provider, ok := r.deps.Sessions.(OnlineUserProvider)
	if !ok {
		return
	}
	// Edge owns the staging generation. Pages are sent as they are read so a
	// large account never builds one unbounded Core heap slice or one oversized
	// Core->Edge control command. Another synced session for the same user on
	// this Edge may satisfy Begin immediately without a PostgreSQL scan.
	syncID, disposition, err := provider.BeginSessionChannelMembershipSync(ctx, rawAuthKeyID, sessionID, userID)
	if err != nil {
		r.log.Warn("begin session channel membership sync failed", zap.Int64("user_id", userID), zap.Error(err))
		return
	}
	switch disposition {
	case edgecontrol.ChannelMembershipSyncPrepared:
		return
	case edgecontrol.ChannelMembershipSyncInProgress:
		r.log.Debug("session channel membership sync already in progress", zap.Int64("user_id", userID))
		return
	case edgecontrol.ChannelMembershipSyncAcquired:
		// This caller exclusively owns syncID and is the only one allowed to
		// stream pages and commit.
	default:
		r.log.Warn("begin session channel membership sync returned invalid disposition",
			zap.Int64("user_id", userID),
			zap.String("disposition", string(disposition)))
		return
	}
	committed := false
	defer func() {
		if !committed {
			provider.AbortSessionChannelMembershipSync(context.Background(), rawAuthKeyID, sessionID, userID, syncID)
		}
	}()
	if r.deps.Channels == nil {
		r.log.Warn("session channel membership sync missing channel authority", zap.Int64("user_id", userID))
		return
	}
	after := int64(0)
	for {
		page, err := r.deps.Channels.ActiveChannelIDsForUser(ctx, userID, after, channelMembershipSyncPageSize)
		if err != nil {
			r.log.Warn("sync session channel memberships failed",
				zap.Int64("user_id", userID),
				zap.Int64("after_channel_id", after),
				zap.Error(err))
			return
		}
		if len(page) == 0 {
			break
		}
		progressed := false
		validPage := make([]int64, 0, len(page))
		for _, channelID := range page {
			if channelID == 0 {
				continue
			}
			validPage = append(validPage, channelID)
			if channelID > after {
				after = channelID
				progressed = true
			}
		}
		if len(validPage) > 0 {
			if err := provider.AppendSessionChannelMembershipSync(ctx, rawAuthKeyID, sessionID, userID, syncID, validPage); err != nil {
				r.log.Warn("append session channel membership sync failed",
					zap.Int64("user_id", userID),
					zap.Int64("after_channel_id", after),
					zap.Int("page_size", len(validPage)),
					zap.Error(err))
				return
			}
		}
		if !progressed || len(page) < channelMembershipSyncPageSize {
			break
		}
	}
	synced, err := provider.CommitSessionChannelMembershipSync(ctx, rawAuthKeyID, sessionID, userID, syncID)
	if err != nil {
		r.log.Warn("commit session channel membership sync failed", zap.Int64("user_id", userID), zap.Error(err))
		return
	}
	committed = true
	if !synced {
		r.log.Debug("session channel membership sync raced; next RPC will retry", zap.Int64("user_id", userID))
	}
}

func (r *Router) addOnlineChannelMemberships(channelID int64, userIDs ...int64) {
	if channelID == 0 || len(userIDs) == 0 || r.deps.Sessions == nil {
		return
	}
	provider, ok := r.deps.Sessions.(OnlineUserProvider)
	if !ok {
		return
	}
	seen := make(map[int64]struct{}, len(userIDs))
	for _, userID := range userIDs {
		if userID == 0 {
			continue
		}
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		provider.AddUserChannelMembership(userID, channelID)
	}
}

func (r *Router) removeOnlineChannelMemberships(channelID int64, userIDs ...int64) {
	if channelID == 0 || len(userIDs) == 0 || r.deps.Sessions == nil {
		return
	}
	provider, ok := r.deps.Sessions.(OnlineUserProvider)
	if !ok {
		return
	}
	seen := make(map[int64]struct{}, len(userIDs))
	for _, userID := range userIDs {
		if userID == 0 {
			continue
		}
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		provider.RemoveUserChannelMembership(userID, channelID)
	}
}

func (r *Router) removeOnlineChannelMembershipsForOnlineMembers(channelID int64) {
	if channelID == 0 || r.deps.Sessions == nil {
		return
	}
	provider, ok := r.deps.Sessions.(OnlineUserProvider)
	if !ok {
		return
	}
	r.removeOnlineChannelMemberships(channelID, provider.OnlineChannelMemberUserIDs(channelID, 0)...)
}

func channelMemberUserIDs(members []domain.ChannelMember) []int64 {
	if len(members) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(members))
	for _, member := range members {
		if member.UserID == 0 || member.Status != domain.ChannelMemberActive {
			continue
		}
		ids = append(ids, member.UserID)
	}
	return ids
}

func channelIDsFromDialogs(list domain.DialogList) []int64 {
	if len(list.Dialogs) == 0 && len(list.Channels) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(list.Dialogs)+len(list.Channels))
	seen := make(map[int64]struct{}, len(list.Dialogs)+len(list.Channels))
	for _, d := range list.Dialogs {
		if d.Peer.Type != domain.PeerTypeChannel || d.Peer.ID == 0 {
			continue
		}
		if _, ok := seen[d.Peer.ID]; ok {
			continue
		}
		seen[d.Peer.ID] = struct{}{}
		ids = append(ids, d.Peer.ID)
	}
	for _, ch := range list.Channels {
		if ch.ID == 0 {
			continue
		}
		if _, ok := seen[ch.ID]; ok {
			continue
		}
		seen[ch.ID] = struct{}{}
		ids = append(ids, ch.ID)
	}
	return ids
}
