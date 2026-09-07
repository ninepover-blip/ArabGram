package store

import "telesrv/internal/domain"

// ChannelDeliveryLogicalShards is stable durable ownership space.  It does not
// change with the number of Egress processes or workers.
const (
	ChannelDeliveryLogicalShards    = 256
	MaxChannelDeliveryAffectedUsers = 1000
)

type ChannelDeliveryAudienceKind string

const (
	ChannelDeliveryAudienceMembers         ChannelDeliveryAudienceKind = "members"
	ChannelDeliveryAudienceMessageBox      ChannelDeliveryAudienceKind = "message_box"
	ChannelDeliveryAudienceMonoforumAdmins ChannelDeliveryAudienceKind = "monoforum_admins"
)

// ChannelDeliveryPayload is only the stable identity needed to reconstruct a
// projection from channel_update_events/channel_messages.  It never contains
// member rows or a process-local projection closure.
type ChannelDeliveryPayload struct {
	MinPTS          int
	MaxPTS          int
	ProjectionKind  domain.ChannelUpdateEventType
	AudienceKind    ChannelDeliveryAudienceKind
	AudienceUserIDs []int64
	AffectedUserIDs []int64
}

func (ChannelDeliveryPayload) outboxClaimPayload() {}

// ChannelDeliveryStore owns the channel_id/channel_pts state machine.  It
// shares typed fencing operations with account queues but never their tables,
// lanes, cursors, or capacity quota.
type ChannelDeliveryStore interface {
	DurableOutboxStateStore
}
