package rpc

import (
	"context"
	"github.com/iamxvbaba/td/tg"
	"telesrv/internal/domain"
)

type channelStateMutation func(ctx context.Context, userID, channelID int64) (domain.Channel, error)

func (r *Router) applyChannelAdminStateMutation(ctx context.Context, input tg.InputChannelClass, mutate channelStateMutation) (tg.UpdatesClass, error) {
	if r.deps.Channels == nil {
		return nil, notImplementedErr()
	}
	userID, _, err := r.currentUserID(ctx)
	if err != nil {
		return nil, internalErr()
	}
	channelID, err := r.channelIDFromInput(ctx, userID, input)
	if err != nil {
		return nil, err
	}
	channel, err := mutate(ctx, userID, channelID)
	if err != nil {
		return nil, channelAdminErr(err)
	}
	return r.channelStateMutationUpdates(ctx, userID, channel), nil
}

type channelChangeInfoMutation func(ctx context.Context, userID int64, view domain.ChannelView) (domain.Channel, error)

func (r *Router) applyChannelChangeInfoMutation(ctx context.Context, input tg.InputChannelClass, mutate channelChangeInfoMutation) (tg.UpdatesClass, error) {
	userID, view, err := r.channelChangeInfoView(ctx, input)
	if err != nil {
		return nil, err
	}
	channel, err := mutate(ctx, userID, view)
	if err != nil {
		return nil, channelAdminErr(err)
	}
	return r.channelStateMutationUpdates(ctx, userID, channel), nil
}

func (r *Router) channelStateMutationUpdates(ctx context.Context, userID int64, channel domain.Channel) tg.UpdatesClass {
	projection := r.prepareChannelStateMutationProjection(ctx, userID, channel)
	r.pushTransientChannelStateInvalidationWithLinkedMonoforum(ctx, userID, projection.channel, projection.monoforum, projection.includeMonoforum, projection.botVerificationIcon, projection.usernames)
	return r.renderChannelStateMutationProjection(userID, projection)
}

type channelStateMutationProjection struct {
	channel             domain.Channel
	monoforum           domain.Channel
	includeMonoforum    bool
	botVerificationIcon int64
	usernames           map[domain.Peer][]domain.Username
}

func (r *Router) prepareChannelStateMutationProjection(ctx context.Context, userID int64, channel domain.Channel) channelStateMutationProjection {
	r.invalidateRPCProjectionForChannel(channel.ID)
	if channel.LinkedMonoforumID != 0 {
		r.invalidateRPCProjectionForChannel(channel.LinkedMonoforumID)
	}
	mono, includeMono := r.linkedMonoforumForChannelState(ctx, userID, channel)
	icon := r.peerBotVerificationIcon(ctx, domain.Peer{Type: domain.PeerTypeChannel, ID: channel.ID})
	usernames := r.channelStateUsernameRegistry(ctx, channel, mono, includeMono)
	return channelStateMutationProjection{
		channel: channel, monoforum: mono, includeMonoforum: includeMono,
		botVerificationIcon: icon, usernames: usernames,
	}
}

func (r *Router) renderChannelStateMutationProjection(userID int64, projection channelStateMutationProjection) *tg.Updates {
	return r.channelStateUpdatesWithLinkedMonoforum(
		userID, projection.channel, projection.monoforum, projection.includeMonoforum,
		projection.botVerificationIcon, projection.usernames,
	)
}

func (r *Router) channelPaidMessagesPriceUpdates(ctx context.Context, userID int64, res domain.ChannelPaidMessagesPriceResult) tg.UpdatesClass {
	services := res.ServiceMessages
	if len(services) == 0 && res.ServiceMessage != nil {
		services = []domain.SendChannelMessageResult{*res.ServiceMessage}
	}
	if len(services) == 0 {
		return r.channelStateMutationUpdates(ctx, userID, res.Channel)
	}
	// A service event makes this a channel_pts mutation. Build only the caller
	// response; Egress consumes the signals committed with the service events.
	projection := r.prepareChannelStateMutationProjection(ctx, userID, res.Channel)
	state := r.renderChannelStateMutationProjection(userID, projection)
	service := r.channelMessagesUpdatesWithPeerCache(ctx, userID, services, nil, false, nil, newViewerPeerCache(r))
	return mergeUpdates(state, service)
}

func mergeUpdates(a, b *tg.Updates) *tg.Updates {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	a.Updates = append(a.Updates, b.Updates...)
	a.Users = appendUniqueTGUsers(a.Users, b.Users...)
	a.Chats = appendUniqueTGChats(a.Chats, b.Chats...)
	if b.Date > a.Date {
		a.Date = b.Date
	}
	return a
}
