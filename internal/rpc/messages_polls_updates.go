package rpc

import (
	"context"
	"errors"
	"fmt"

	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tgerr"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

// This file projects poll mutation results for the current RPC. Reliable
// multi-session delivery belongs to the owning aggregate and delivery lane;
// Core never runs a post-commit online viewer fan-out.

// pollMutationErr 把 poll/消息域错误映射为 RPC error。
func pollMutationErr(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, domain.ErrPollClosed):
		return pollClosedErr()
	case errors.Is(err, domain.ErrPollRevoteNotAllowed):
		return revoteNotAllowedErr()
	case errors.Is(err, domain.ErrPollOptionInvalid):
		return optionInvalidErr()
	case errors.Is(err, domain.ErrPollNotCreator):
		return tgerr.New(403, "MESSAGE_AUTHOR_REQUIRED")
	case errors.Is(err, domain.ErrPollNotFound), errors.Is(err, domain.ErrPollInvalid), errors.Is(err, domain.ErrMessageIDInvalid):
		return messageIDInvalidErr()
	default:
		return channelInvalidErr(err)
	}
}

// loadMessagePoll 按 viewer 视角加载消息上的 poll（已 enrich）。
func (r *Router) loadMessagePoll(ctx context.Context, userID int64, peer domain.Peer, msgID int) (*domain.MessagePoll, domain.Peer, bool) {
	switch peer.Type {
	case domain.PeerTypeChannel:
		if r.deps.Channels == nil {
			return nil, peer, false
		}
		history, err := r.deps.Channels.GetMessages(ctx, userID, peer.ID, []int{msgID})
		if err != nil {
			return nil, peer, false
		}
		for _, msg := range history.Messages {
			if msg.ID == msgID && msg.Media != nil && msg.Media.Kind == domain.MessageMediaKindPoll && msg.Media.Poll != nil {
				return msg.Media.Poll, peer, true
			}
		}
	case domain.PeerTypeUser:
		if r.deps.Messages == nil {
			return nil, peer, false
		}
		list, err := r.deps.Messages.GetMessages(ctx, userID, []int{msgID})
		if err != nil {
			return nil, peer, false
		}
		for _, msg := range list.Messages {
			if msg.ID == msgID && msg.Peer == peer && msg.Media != nil && msg.Media.Kind == domain.MessageMediaKindPoll && msg.Media.Poll != nil {
				return msg.Media.Poll, peer, true
			}
		}
	}
	return nil, peer, false
}

// pollUpdateRefs 收集当前 viewer 的 updateMessagePoll 响应需要的 users/chats
// （recent voters 头像 + channel 实体）。频道 mutation 已携带 res.Channel，因此无需再次查询。
func (r *Router) pollUpdateRefs(ctx context.Context, viewerUserID int64, peer domain.Peer, poll *domain.MessagePoll, channel domain.Channel) ([]tg.UserClass, []tg.ChatClass) {
	userIDs := make([]int64, 0, domain.MaxPollRecentVoters)
	if poll != nil && poll.Results != nil {
		userIDs = append(userIDs, poll.Results.RecentVoters...)
	}
	users := r.tgUsersForIDs(ctx, viewerUserID, userIDs)
	chats := []tg.ChatClass{}
	if peer.Type == domain.PeerTypeChannel {
		if channel.ID != 0 {
			// Mutation result 已带 channel，直接用于当前调用者投影。
			chats = tgChannels(viewerUserID, []domain.Channel{channel})
		} else if r.deps.Channels != nil {
			// getPollResults 是只读单-viewer 路径，需要加载 channel 投影。
			if view, err := r.deps.Channels.GetChannel(ctx, viewerUserID, peer.ID); err == nil && view.Channel.ID != 0 {
				chats = []tg.ChatClass{tgChannelChatForView(viewerUserID, view)}
			}
		}
	}
	return users, chats
}

// privatePollUpdates 只构造当前 RPC 响应。其它 session 的非 PTS
// absolute delivery 已由 privatePollDeliveryEffects 在 poll mutation 事务内写入。
func (r *Router) privatePollUpdates(ctx context.Context, requestUserID int64, res domain.PrivateMessagePollResult) (tg.UpdatesClass, error) {
	for _, msg := range res.Messages {
		if msg.OwnerUserID == requestUserID {
			if updates := r.privatePollUpdateForMessage(ctx, msg); updates != nil {
				return updates, nil
			}
		}
	}
	return tgEmptyUpdates(int(r.clock.Now().Unix())), nil
}

func (r *Router) privatePollUpdateForMessage(ctx context.Context, msg domain.Message) *tg.Updates {
	if msg.OwnerUserID == 0 || msg.ID <= 0 || msg.Media == nil || msg.Media.Poll == nil {
		return nil
	}
	update := tgUpdateMessagePoll(msg.Peer, msg.ID, msg.Media.Poll)
	if update == nil {
		return nil
	}
	users, chats := r.pollUpdateRefs(ctx, msg.OwnerUserID, msg.Peer, msg.Media.Poll, domain.Channel{})
	return &tg.Updates{
		Updates: []tg.UpdateClass{update}, Users: users, Chats: chats,
		Date: int(r.clock.Now().Unix()), Seq: 0,
	}
}

func privatePollUpdateForMessageWithUsers(msg domain.Message, users []tg.UserClass, date int) *tg.Updates {
	if msg.OwnerUserID == 0 || msg.ID <= 0 || msg.Media == nil || msg.Media.Poll == nil {
		return nil
	}
	update := tgUpdateMessagePoll(msg.Peer, msg.ID, msg.Media.Poll)
	if update == nil {
		return nil
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{update}, Users: append([]tg.UserClass(nil), users...), Chats: []tg.ChatClass{},
		Date: date, Seq: 0,
	}
}

func (r *Router) privatePollDeliveryUsers(ctx context.Context, requestUserID, peerUserID int64) map[int64][]tg.UserClass {
	ids := []int64{requestUserID}
	if peerUserID != 0 && peerUserID != requestUserID {
		ids = append(ids, peerUserID)
	}
	out := map[int64][]tg.UserClass{
		requestUserID: r.tgUsersForIDs(ctx, requestUserID, ids),
	}
	if peerUserID != 0 && peerUserID != requestUserID {
		out[peerUserID] = r.tgUsersForIDs(ctx, peerUserID, ids)
	}
	return out
}

func (r *Router) privatePollDeliveryEffects(ctx context.Context, requestUserID int64, usersByViewer map[int64][]tg.UserClass, date int) store.DeliveryEffectsBuilder[domain.PrivateMessagePollResult] {
	excludeAuthKeyID, excludeSessionID := deliveryExclusionFromContext(ctx)
	return func(res domain.PrivateMessagePollResult) ([]store.DeliveryEffect, error) {
		seen := make(map[int64]struct{}, len(res.Messages))
		effects := make([]store.DeliveryEffect, 0, len(res.Messages))
		for _, msg := range res.Messages {
			if _, duplicate := seen[msg.OwnerUserID]; duplicate {
				continue
			}
			users, ok := usersByViewer[msg.OwnerUserID]
			if !ok {
				return nil, fmt.Errorf("private poll delivery users missing for user %d", msg.OwnerUserID)
			}
			updates := privatePollUpdateForMessageWithUsers(msg, users, date)
			if updates == nil {
				return nil, fmt.Errorf("private poll owner projection missing for user %d", msg.OwnerUserID)
			}
			payload, err := encodeDeliveryUpdate(updates)
			if err != nil {
				return nil, fmt.Errorf("encode private poll delivery for user %d: %w", msg.OwnerUserID, err)
			}
			authKeyID, sessionID := [8]byte{}, int64(0)
			if msg.OwnerUserID == requestUserID {
				authKeyID, sessionID = excludeAuthKeyID, excludeSessionID
			}
			effects = append(effects, store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
				TargetUserID: msg.OwnerUserID, ExcludeAuthKeyID: authKeyID, ExcludeSessionID: sessionID,
				Payload: payload, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
			}))
			seen[msg.OwnerUserID] = struct{}{}
		}
		return effects, nil
	}
}

// onEditMessageClosePoll 处理 editMessage + InputMediaPoll：当前唯一支持的 poll 编辑是
// 关闭（closed=true，仅 poll 创建者）；改题/改选项无官方客户端路径，显式拒绝。
func (r *Router) onEditMessageClosePoll(ctx context.Context, req *tg.MessagesEditMessageRequest, media *tg.InputMediaPoll) (tg.UpdatesClass, error) {
	if req.ID <= 0 || req.ID > domain.MaxMessageBoxID {
		return nil, messageIDInvalidErr()
	}
	if !media.Poll.Closed {
		return nil, mediaInvalidErr()
	}
	userID, peer, err := r.reactionPeer(ctx, req.Peer, nil)
	if err != nil {
		return nil, err
	}
	now := int(r.clock.Now().Unix())
	switch peer.Type {
	case domain.PeerTypeChannel:
		if r.deps.Channels == nil {
			return nil, messageIDInvalidErr()
		}
		res, err := r.deps.Channels.CloseMessagePoll(ctx, userID, domain.CloseChannelMessagePollRequest{
			UserID:    userID,
			ChannelID: peer.ID,
			MessageID: req.ID,
			Date:      now,
		})
		if err != nil {
			return nil, pollMutationErr(err)
		}
		return r.channelPollUpdates(ctx, userID, peer, req.ID, res), nil
	case domain.PeerTypeUser:
		if r.deps.Messages == nil {
			return nil, messageIDInvalidErr()
		}
		deliveryUsers := r.privatePollDeliveryUsers(ctx, userID, peer.ID)
		res, err := r.deps.Messages.CloseMessagePoll(ctx, userID, domain.ClosePrivateMessagePollRequest{
			UserID: userID, Peer: peer, MessageID: req.ID, Date: now,
		}, r.privatePollDeliveryEffects(ctx, userID, deliveryUsers, now))
		if err != nil {
			return nil, pollMutationErr(err)
		}
		return r.privatePollUpdates(ctx, userID, res)
	default:
		return nil, peerIDInvalidErr()
	}
}

// channelPollUpdates builds only the caller's post-mutation poll projection.
func (r *Router) channelPollUpdates(ctx context.Context, userID int64, peer domain.Peer, msgID int, res domain.ChannelMessagePollResult) tg.UpdatesClass {
	poll := res.Message.Media.Poll
	update := tgUpdateMessagePoll(peer, msgID, poll)
	if update == nil {
		return tgEmptyUpdates(int(r.clock.Now().Unix()))
	}
	users, chats := r.pollUpdateRefs(ctx, userID, peer, poll, res.Channel)
	updates := &tg.Updates{
		Updates: []tg.UpdateClass{update},
		Users:   users,
		Chats:   chats,
		Date:    int(r.clock.Now().Unix()),
	}
	return updates
}
