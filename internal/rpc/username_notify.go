package rpc

import (
	"context"
	"fmt"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

// UsernameAudienceDeliveryEffects projects every user snapshot frozen by the
// username aggregate into the standard durable, non-PTS updateUser effects.
// Channel-only mutations deliberately produce an empty snapshot: their
// updateChannel delivery remains owned by the channel aggregate.
func (r *Router) UsernameAudienceDeliveryEffects(snapshot store.UsernameAudienceDeliverySnapshot) ([]store.DeliveryEffect, error) {
	if r == nil {
		return nil, fmt.Errorf("username audience delivery router is not configured")
	}
	effects := make([]store.DeliveryEffect, 0)
	for _, user := range snapshot.Users {
		projected, err := r.UserAudienceDeliveryEffects(user)
		if err != nil {
			return nil, err
		}
		effects = append(effects, projected...)
	}
	return effects, nil
}

// NotifyPeerUsernamesChanged is now only the channel/cache hook invoked after a
// collectible username registry mutation commits. User delivery is a mandatory
// effect in the username store transaction; this hook must never recreate a
// post-commit user producer.
func (r *Router) NotifyPeerUsernamesChanged(ctx context.Context, peer domain.Peer) error {
	if r == nil {
		return nil
	}
	if peer.ID <= 0 {
		return fmt.Errorf("notify peer usernames changed: invalid peer id %d", peer.ID)
	}
	switch peer.Type {
	case domain.PeerTypeUser:
		return r.notifyUserUsernamesChanged(ctx, peer.ID)
	case domain.PeerTypeChannel:
		return r.notifyChannelUsernamesChanged(ctx, peer.ID)
	default:
		return fmt.Errorf("notify peer usernames changed: unsupported peer type %q for peer %d", peer.Type, peer.ID)
	}
}

func (r *Router) notifyUserUsernamesChanged(ctx context.Context, userID int64) error {
	r.invalidateRPCProjectionForUser(userID)
	return nil
}

func (r *Router) notifyChannelUsernamesChanged(ctx context.Context, channelID int64) error {
	r.invalidateRPCProjectionForChannel(channelID)
	if r.deps.Channels == nil {
		return nil
	}
	directory, ok := r.deps.Channels.(verificationChannelDirectory)
	if !ok {
		return fmt.Errorf("notify peer usernames changed: channel service does not expose GetChannelByID")
	}
	channel, err := directory.GetChannelByID(ctx, channelID)
	if err != nil {
		return fmt.Errorf("notify peer usernames changed: load channel %d: %w", channelID, err)
	}
	if channel.ID == 0 {
		return fmt.Errorf("notify peer usernames changed: channel %d not found", channelID)
	}
	return r.NotifyChannelChanged(ctx, channel)
}
