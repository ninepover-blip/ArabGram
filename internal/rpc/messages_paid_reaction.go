package rpc

import (
	"context"

	"github.com/iamxvbaba/td/tg"

	"telesrv/internal/domain"
)

// channelPaidReactionService 是 r.deps.Channels 的可选扩展（app/channels.Service 实现），
// 用类型断言接入，避免在 ChannelsService 接口上为付费 reaction 单加方法。
type channelPaidReactionService interface {
	SendPaidReaction(ctx context.Context, userID int64, req domain.SendChannelPaidReactionRequest) (domain.ChannelMessagePaidReactionResult, error)
}

type channelPaidReactionReplayService interface {
	ReplayPaidReaction(ctx context.Context, userID int64, req domain.SendChannelPaidReactionRequest) (domain.ChannelMessagePaidReactionResult, bool, error)
}

// channelPaidReactionResponseUpdates builds only the current RPC caller's
// projection. Egress owns every other session/viewer projection.
func (r *Router) channelPaidReactionResponseUpdates(ctx context.Context, viewerUserID int64, res domain.ChannelMessagePaidReactionResult) *tg.Updates {
	base := domain.ChannelMessageReactions{}
	if res.Message.Reactions != nil {
		base = *res.Message.Reactions
	}
	paid := res.Paid
	base.Paid = &paid
	mr := tgMessageReactions(viewerUserID, &base)
	if mr == nil {
		mr = &tg.MessageReactions{Results: []tg.ReactionCount{}}
	}
	msgID := res.Message.ID
	update := &tg.UpdateMessageReactions{
		Peer:      &tg.PeerChannel{ChannelID: res.Channel.ID},
		MsgID:     msgID,
		Reactions: *mr,
	}
	channelIDs := paidReactorChannelIDs(paid)
	channels := make([]domain.Channel, 0, 1+len(res.DisplayChannels))
	channels = append(channels, res.Channel)
	seenChannels := map[int64]struct{}{res.Channel.ID: {}}
	for _, channel := range res.DisplayChannels {
		if channel.ID == 0 {
			continue
		}
		if _, seen := seenChannels[channel.ID]; seen {
			continue
		}
		seenChannels[channel.ID] = struct{}{}
		channels = append(channels, channel)
	}
	chats := tgChannels(viewerUserID, channels)
	missingChannelIDs := make([]int64, 0, len(channelIDs))
	for _, channelID := range channelIDs {
		if _, seen := seenChannels[channelID]; !seen {
			missingChannelIDs = append(missingChannelIDs, channelID)
		}
	}
	chats = append(chats, r.tgChatsForChannelIDs(ctx, viewerUserID, missingChannelIDs)...)
	return &tg.Updates{
		Updates: []tg.UpdateClass{update},
		Users:   r.tgUsersForIDs(ctx, viewerUserID, paidReactorUserIDs(paid)),
		Chats:   chats,
		Date:    int(r.clock.Now().Unix()),
		Seq:     0,
	}
}

// injectPaidReaction 把付费 reaction 注入 MessageReactions：ReactionPaid 计数置首位、填充
// top reactors 排行。My/chosen 完全由调用者视角的 paid 数据驱动。
func injectPaidReaction(mr *tg.MessageReactions, paid domain.ChannelMessagePaidReactions) {
	if paid.TotalStars <= 0 {
		return
	}
	paidCount := tg.ReactionCount{Reaction: &tg.ReactionPaid{}, Count: int(paid.TotalStars)}
	if paid.MyStars > 0 {
		// chosen_order 仅标记「本人已投」，具体序值不关键，置正。
		paidCount.SetChosenOrder(int(paid.MyStars))
	}
	mr.Results = append([]tg.ReactionCount{paidCount}, mr.Results...)

	reactors := make([]tg.MessageReactor, 0, len(paid.TopReactors))
	for _, rr := range paid.TopReactors {
		item := tg.MessageReactor{Count: int(rr.Stars)}
		if rr.My {
			item.My = true
		} else {
			item.Top = true
		}
		if rr.Anonymous {
			item.Anonymous = true
		}
		// PeerID：非匿名给出；匿名仅本人给出（TL 语义：匿名本人 anonymous+peer 都置）。
		if (!rr.Anonymous || rr.My) && rr.DisplayPeer().ID != 0 {
			if peer := tgPeer(rr.DisplayPeer()); peer != nil {
				item.SetPeerID(peer)
			}
		}
		reactors = append(reactors, item)
	}
	if len(reactors) > 0 {
		mr.SetTopReactors(reactors)
	}
}

// paidReactorUserIDs collects visible user display identities. An anonymous
// reactor is visible only to itself, matching injectPaidReaction.
func paidReactorUserIDs(paid domain.ChannelMessagePaidReactions) []int64 {
	ids := make([]int64, 0, len(paid.TopReactors))
	for _, rr := range paid.TopReactors {
		peer := rr.DisplayPeer()
		if peer.Type == domain.PeerTypeUser && peer.ID != 0 && (!rr.Anonymous || rr.My) {
			ids = append(ids, peer.ID)
		}
	}
	return ids
}

func paidReactorChannelIDs(paid domain.ChannelMessagePaidReactions) []int64 {
	ids := make([]int64, 0, len(paid.TopReactors))
	for _, rr := range paid.TopReactors {
		peer := rr.DisplayPeer()
		if peer.Type == domain.PeerTypeChannel && peer.ID != 0 && (!rr.Anonymous || rr.My) {
			ids = append(ids, peer.ID)
		}
	}
	return uniqueInt64(ids)
}
