package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func pendingJoinDeliverySnapshotTx(ctx context.Context, tx pgx.Tx, channel domain.Channel) (store.ChannelPendingJoinDeliverySnapshot, error) {
	rows, err := tx.Query(ctx, `
WITH ranked_pending AS (
  SELECT user_id, date,
         row_number() OVER (ORDER BY date DESC, user_id DESC) AS ordinal,
         count(*) OVER ()::int AS total
  FROM channel_invite_importers
  WHERE channel_id = $1 AND requested
), pending AS (
  SELECT COALESCE(max(total), 0)::int AS total,
         COALESCE(array_agg(user_id ORDER BY date DESC, user_id DESC)
           FILTER (WHERE ordinal <= $2), ARRAY[]::bigint[]) AS recent
  FROM ranked_pending
)
SELECT m.user_id, pending.total, pending.recent
FROM channel_members m
CROSS JOIN pending
WHERE m.channel_id = $1
  AND m.status = 'active'
  AND (
    m.role = 'creator' OR
    (m.role = 'admin' AND (
      (m.admin_rights->>'InviteUsers')::boolean IS TRUE OR
      (m.admin_rights->>'ChangeInfo')::boolean IS TRUE
    ))
  )
ORDER BY m.user_id
LIMIT $3`, channel.ID, domain.MaxChannelPendingJoinRecentRequesters, domain.MaxChannelRealtimeFanout)
	if err != nil {
		return store.ChannelPendingJoinDeliverySnapshot{}, fmt.Errorf("snapshot pending join delivery: %w", err)
	}
	defer rows.Close()
	snapshot := store.ChannelPendingJoinDeliverySnapshot{}
	for rows.Next() {
		var targetUserID int64
		var count int
		var recent []int64
		if err := rows.Scan(&targetUserID, &count, &recent); err != nil {
			return store.ChannelPendingJoinDeliverySnapshot{}, err
		}
		snapshot.Targets = append(snapshot.Targets, store.ChannelPendingJoinDeliveryTarget{
			TargetUserID: targetUserID,
			Channel:      channel,
			Pending: domain.ChannelPendingJoinRequests{
				ChannelID: channel.ID, Count: count, RecentRequesters: append([]int64(nil), recent...),
			},
		})
	}
	if err := rows.Err(); err != nil {
		return store.ChannelPendingJoinDeliverySnapshot{}, err
	}
	rows.Close()
	if len(snapshot.Targets) != 0 {
		recentIDs := snapshot.Targets[0].Pending.RecentRequesters
		users, err := listUsersByIDs(ctx, tx, recentIDs)
		if err != nil {
			return store.ChannelPendingJoinDeliverySnapshot{}, fmt.Errorf("snapshot pending join requester users: %w", err)
		}
		byID := make(map[int64]domain.User, len(users))
		for _, user := range users {
			byID[user.ID] = user
		}
		recentUsers := make([]domain.User, 0, len(recentIDs))
		for _, userID := range recentIDs {
			user, ok := byID[userID]
			if !ok {
				return store.ChannelPendingJoinDeliverySnapshot{}, fmt.Errorf("snapshot pending join requester user %d: missing", userID)
			}
			recentUsers = append(recentUsers, user)
		}
		for i := range snapshot.Targets {
			snapshot.Targets[i].RecentUsers = append([]domain.User(nil), recentUsers...)
		}
	}
	return snapshot, nil
}

func applyPendingJoinDeliveryTx(ctx context.Context, tx pgx.Tx, snapshot store.ChannelPendingJoinDeliverySnapshot, build store.DeliveryEffectsBuilder[store.ChannelPendingJoinDeliverySnapshot]) error {
	if build == nil {
		return store.ErrDeliveryOutboxRequired
	}
	expected := make(map[int64]struct{}, len(snapshot.Targets))
	for _, target := range snapshot.Targets {
		if target.TargetUserID <= 0 || target.Channel.ID == 0 || target.Pending.ChannelID != target.Channel.ID {
			return errors.New("pending join delivery snapshot is invalid")
		}
		if len(target.RecentUsers) != len(target.Pending.RecentRequesters) {
			return errors.New("pending join delivery requester projection is incomplete")
		}
		for i, userID := range target.Pending.RecentRequesters {
			if userID <= 0 || target.RecentUsers[i].ID != userID {
				return errors.New("pending join delivery requester projection does not match summary")
			}
		}
		if _, duplicate := expected[target.TargetUserID]; duplicate {
			return errors.New("pending join delivery snapshot has duplicate target")
		}
		expected[target.TargetUserID] = struct{}{}
	}
	effects, err := build(snapshot)
	if err != nil {
		return err
	}
	if len(effects) != len(snapshot.Targets) {
		return errors.New("pending join delivery effect count does not match target count")
	}
	for _, effect := range effects {
		if effect.Kind != store.DeliveryEffectAbsolute {
			return errors.New("pending join delivery effect must be absolute")
		}
		if _, ok := expected[effect.TargetUserID]; !ok {
			return errors.New("pending join delivery effect has unexpected target")
		}
		delete(expected, effect.TargetUserID)
	}
	if len(expected) != 0 {
		return errors.New("pending join delivery effect omits target")
	}
	return applyAbsoluteDeliveryEffectsTx(ctx, tx, effects)
}

func applyAvailableMinDeliveryTx(ctx context.Context, tx pgx.Tx, snapshot store.ChannelAvailableMinDeliverySnapshot, build store.DeliveryEffectsBuilder[store.ChannelAvailableMinDeliverySnapshot]) error {
	if build == nil {
		return store.ErrDeliveryOutboxRequired
	}
	effects, err := build(snapshot)
	if err != nil {
		return err
	}
	if snapshot.TargetUserID == 0 {
		if len(effects) != 0 {
			return errors.New("no-op available-min snapshot produced delivery effects")
		}
		return nil
	}
	if snapshot.TargetUserID <= 0 || snapshot.Channel.ID == 0 || snapshot.AvailableMinID <= 0 {
		return errors.New("available-min delivery snapshot is invalid")
	}
	if len(effects) != 1 || effects[0].Kind != store.DeliveryEffectAbsolute || effects[0].TargetUserID != snapshot.TargetUserID {
		return errors.New("available-min delivery effect does not match target")
	}
	return applyAbsoluteDeliveryEffectsTx(ctx, tx, effects)
}
