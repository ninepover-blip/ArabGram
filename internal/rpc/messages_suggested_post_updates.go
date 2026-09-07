package rpc

import (
	"context"

	"github.com/iamxvbaba/td/tg"

	"telesrv/internal/domain"
)

func (r *Router) suggestedPostApprovalUpdatesStrict(ctx context.Context, viewerUserID int64, result domain.ToggleSuggestedPostApprovalResult) (*tg.Updates, error) {
	return r.suggestedPostApprovalUpdatesWithPeerCacheAndOverlaysStrict(ctx, viewerUserID, result, nil, nil)
}

func (r *Router) suggestedPostApprovalUpdatesWithPeerCacheAndOverlaysStrict(ctx context.Context, viewerUserID int64, result domain.ToggleSuggestedPostApprovalResult, cache *viewerPeerCache, overlays *monoforumPeerOverlays) (*tg.Updates, error) {
	updates := make([]tg.UpdateClass, 0, 4)
	if result.OriginalEvent.Pts > 0 {
		if update := tgChannelUpdate(viewerUserID, result.OriginalEvent); update != nil {
			updates = append(updates, update)
		}
	}
	if result.ServiceEvent.Pts > 0 {
		if update := tgChannelUpdate(viewerUserID, result.ServiceEvent); update != nil {
			updates = append(updates, update)
		}
	}
	if result.Published != nil && result.Published.Event.Pts > 0 {
		if update := tgChannelUpdate(viewerUserID, result.Published.Event); update != nil {
			updates = append(updates, update)
		}
	}
	if result.PayerStarsBalance != nil && result.PayerStarsBalance.UserID == viewerUserID {
		updates = append(updates, &tg.UpdateStarsBalance{Balance: &tg.StarsAmount{Amount: result.PayerStarsBalance.Balance}})
	}
	chats := r.monoforumChats(ctx, viewerUserID, result.Monoforum)
	if result.Parent.ID != 0 {
		chats = appendUniqueTGChats(chats, tgChannelChatMin(viewerUserID, result.Parent))
	}
	messages := make([]domain.ChannelMessage, 0, 3)
	if result.OriginalMessage.ID != 0 {
		messages = append(messages, result.OriginalMessage)
	}
	if result.ServiceMessage.ID != 0 {
		messages = append(messages, result.ServiceMessage)
	}
	if result.Published != nil {
		messages = append(messages, result.Published.Message)
	}
	if cache == nil {
		cache = newViewerPeerCache(r)
	}
	if overlays == nil {
		ids := monoforumSubscriberUserIDs([]domain.MonoforumDialog{{SavedPeer: result.SavedPeer}}, messages)
		overlays = r.loadMonoforumPeerOverlays(ctx, monoforumProjectionPeers(result.Monoforum.ID, result.Parent.ID, ids))
	}
	users, err := r.monoforumSubscriberUsersWithPeerCacheAndOverlaysStrict(ctx, viewerUserID, []domain.MonoforumDialog{{SavedPeer: result.SavedPeer}}, messages, cache, overlays)
	if err != nil {
		return nil, err
	}
	applyMonoforumPeerOverlays(nil, chats, overlays)
	return &tg.Updates{
		Updates: updates,
		Chats:   chats,
		Users:   users,
		Date:    int(r.clock.Now().Unix()),
	}, nil
}

func (r *Router) enqueueSuggestedPostBotAPIUpdate(ctx context.Context, originUserID int64, result domain.ToggleSuggestedPostApprovalResult) error {
	if result.Published != nil && result.Published.Event.Pts > 0 {
		return r.enqueueBotAPIChannelMessageUpdate(ctx, originUserID, *result.Published)
	}
	return nil
}
