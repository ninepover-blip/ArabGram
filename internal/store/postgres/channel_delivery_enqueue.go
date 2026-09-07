package postgres

import (
	"context"
	"fmt"
	"sort"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

// insertChannelDeliverySignalTx appends one immutable accelerator signal and
// creates the channel lane only on the empty -> non-empty transition.  Appends
// to an active lane intentionally execute no UPDATE against the lane row.
//
// A monoforum signal freezes its complete authorized audience in the same
// transaction as the event. Message-box events target the saved peer plus the
// parent channel's active creator/direct-message managers; manager-only events
// target that manager set. Authorization changes after this point cannot alter
// an already-issued delivery command.
func insertChannelDeliverySignalTx(ctx context.Context, tx pgx.Tx, event domain.ChannelUpdateEvent) error {
	if event.ChannelID <= 0 {
		return fmt.Errorf("insert channel delivery signal: missing channel id")
	}
	if event.Pts <= 0 || event.PtsCount <= 0 || event.PtsCount > event.Pts {
		return fmt.Errorf("insert channel delivery signal: invalid pts range pts=%d pts_count=%d", event.Pts, event.PtsCount)
	}
	minPTS := event.Pts - event.PtsCount + 1
	affectedUserIDs, err := channelDeliveryAffectedUserIDs(event)
	if err != nil {
		return err
	}
	savedPeerUserID := int64(0)
	if event.Message.SavedPeer.Type == domain.PeerTypeUser && event.Message.SavedPeer.ID > 0 {
		savedPeerUserID = event.Message.SavedPeer.ID
	}
	if _, err := tx.Exec(ctx, `
WITH append_fence AS MATERIALIZED (
    SELECT pg_advisory_xact_lock_shared(
        outbox_lane_advisory_key('channel_pts', $1)
    ) AS held
), channel_scope AS MATERIALIZED (
    SELECT c.monoforum
    FROM channels c
    CROSS JOIN append_fence
    WHERE c.id = $1
), message_box_users AS MATERIALIZED (
    SELECT CASE
      WHEN $5::bigint > 0 THEN ARRAY[$5::bigint]
      ELSE COALESCE((
        SELECT array_agg(DISTINCT m.saved_peer_id ORDER BY m.saved_peer_id)
        FROM channel_messages m
        WHERE m.channel_id = $1
          AND m.id = ANY($6::int[])
          AND m.saved_peer_type = 'user'
          AND m.saved_peer_id > 0
      ), '{}'::bigint[])
    END AS user_ids
), monoforum_admin_users AS MATERIALIZED (
    SELECT COALESCE(array_agg(authorized.user_id ORDER BY authorized.user_id), '{}'::bigint[]) AS user_ids
    FROM (
        SELECT manager.user_id
        FROM channels mono
        JOIN channels parent
          ON parent.id = mono.linked_monoforum_id
         AND NOT parent.deleted
         AND parent.broadcast_messages_allowed
         AND parent.linked_monoforum_id = mono.id
        JOIN channel_members manager
          ON manager.channel_id = parent.id
         AND manager.status = 'active'
         AND (
              manager.role = 'creator'
              OR (
                  manager.role = 'admin'
                  AND COALESCE((manager.admin_rights->>'ManageDirectMessages')::boolean, false)
              )
         )
        WHERE mono.id = $1
          AND mono.monoforum
          AND NOT mono.deleted
        ORDER BY manager.user_id
        LIMIT 1001
    ) authorized
), frozen_audience AS MATERIALIZED (
    SELECT CASE
      WHEN cardinality(box.user_ids) = 0 THEN admins.user_ids
      ELSE COALESCE((
        SELECT array_agg(audience.user_id ORDER BY audience.user_id)
        FROM (
          SELECT unnest(box.user_ids) AS user_id
          UNION
          SELECT unnest(admins.user_ids) AS user_id
        ) audience
      ), '{}'::bigint[])
    END AS user_ids
    FROM message_box_users box
    CROSS JOIN monoforum_admin_users admins
), inserted AS (
    INSERT INTO channel_delivery_events (
        channel_id, min_pts, max_pts, projection_kind, audience_kind, audience_user_ids,
        affected_user_ids
    )
    SELECT $1, $2, $3, $4,
           CASE
             WHEN NOT c.monoforum THEN 'members'
             WHEN cardinality(box.user_ids) > 0 THEN 'message_box'
             ELSE 'monoforum_admins'
           END,
           CASE WHEN c.monoforum THEN audience.user_ids ELSE '{}'::bigint[] END,
           CASE WHEN c.monoforum THEN '{}'::bigint[] ELSE $7::bigint[] END
    FROM channel_scope c
    CROSS JOIN message_box_users box
    CROSS JOIN frozen_audience audience
    RETURNING id, channel_id, min_pts
)
INSERT INTO channel_delivery_lanes (
    channel_id, head_item_id, head_sequence, state, ready_at
)
SELECT channel_id, id, min_pts, 'ready', now()
FROM inserted
ON CONFLICT (channel_id) DO NOTHING`,
		event.ChannelID,
		minPTS,
		event.Pts,
		string(event.Type),
		savedPeerUserID,
		int32s(event.MessageIDs),
		affectedUserIDs,
	); err != nil {
		return fmt.Errorf("insert channel delivery signal: %w", err)
	}
	return nil
}

func channelDeliveryAffectedUserIDs(event domain.ChannelUpdateEvent) ([]int64, error) {
	ids := make([]int64, 0)
	switch event.Type {
	case domain.ChannelUpdateParticipant:
		ids = uniqueNonZeroInt64s(event.Previous.UserID, event.Participant.UserID)
	case domain.ChannelUpdateNewMessage:
		if event.Message.Action == nil {
			return ids, nil
		}
		switch event.Message.Action.Type {
		case domain.ChannelActionChatAddUser,
			domain.ChannelActionChatDelete,
			domain.ChannelActionChatJoined,
			domain.ChannelActionChatJoinedByLink:
			ids = uniqueNonZeroInt64s(event.Message.Action.UserIDs...)
		}
	}
	if ids == nil {
		ids = make([]int64, 0)
	}
	if len(ids) > store.MaxChannelDeliveryAffectedUsers {
		return nil, fmt.Errorf("insert channel delivery signal: affected audience %d exceeds %d", len(ids), store.MaxChannelDeliveryAffectedUsers)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}
