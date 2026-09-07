package egress

import (
	"context"
	"fmt"

	"telesrv/internal/edgecontrol"
	"telesrv/internal/store"
)

func (c *deliveryCoordinator) projectChannelWindow(ctx context.Context, window store.OutboxClaimWindow) ([]projectedOutboxItem, error) {
	if c.channelBuilder == nil {
		return nil, fmt.Errorf("egress: channel update builder is required")
	}
	out := make([]projectedOutboxItem, len(window.Items))
	requests := make([]ChannelUpdateRequest, len(window.Items))
	for i, item := range window.Items {
		payload, ok := item.Payload.(store.ChannelDeliveryPayload)
		if !ok || payload.MinPTS <= 0 || payload.MaxPTS < payload.MinPTS || item.Ref.Sequence != int64(payload.MaxPTS) {
			return nil, fmt.Errorf("egress: invalid channel delivery item=%d", item.Ref.ItemID)
		}
		audience, ok := edgeChannelAudience(payload.AudienceKind)
		if !ok {
			return nil, fmt.Errorf("egress: invalid channel audience kind %q", payload.AudienceKind)
		}
		requests[i] = ChannelUpdateRequest{ChannelID: window.StreamID, PTS: payload.MaxPTS}
		route := edgecontrol.ChannelDeliveryRoute{
			ChannelID:     window.StreamID,
			Audience:      audience,
			AudienceUsers: append([]int64(nil), payload.AudienceUserIDs...),
			AffectedUsers: append([]int64(nil), payload.AffectedUserIDs...),
		}
		if !route.ValidFor(edgecontrol.OrderingDomain{Kind: edgecontrol.QueueChannelPTS, StreamID: window.StreamID}) {
			return nil, fmt.Errorf("egress: non-canonical channel delivery route item=%d", item.Ref.ItemID)
		}
		out[i] = projectedOutboxItem{
			claimed:      item,
			channelRoute: route,
		}
	}
	built, err := c.channelBuilder(ctx, requests)
	if err != nil {
		return nil, fmt.Errorf("egress: build channel delivery window: %w", err)
	}
	if len(built) != len(out) {
		return nil, fmt.Errorf("egress: channel update builder returned %d items, want %d", len(built), len(out))
	}
	for i := range built {
		if len(built[i]) == 0 {
			return nil, fmt.Errorf("egress: channel update builder returned empty item=%d", window.Items[i].Ref.ItemID)
		}
		out[i].payload = built[i]
	}
	return out, nil
}

func edgeChannelAudience(kind store.ChannelDeliveryAudienceKind) (edgecontrol.ChannelDeliveryAudience, bool) {
	switch kind {
	case store.ChannelDeliveryAudienceMembers:
		return edgecontrol.ChannelAudienceMembers, true
	case store.ChannelDeliveryAudienceMessageBox:
		return edgecontrol.ChannelAudienceMessageBox, true
	case store.ChannelDeliveryAudienceMonoforumAdmins:
		return edgecontrol.ChannelAudienceMonoforumAdmins, true
	default:
		return 0, false
	}
}
