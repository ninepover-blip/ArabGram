package postgres

import (
	"context"
	"fmt"

	"telesrv/internal/domain"
)

func (s *ChannelStore) SetChannelHistoryTTL(ctx context.Context, userID, channelID int64, period int, date int) (domain.Channel, []int64, error) {
	if userID == 0 || channelID == 0 || period < 0 {
		return domain.Channel{}, nil, domain.ErrChannelInvalid
	}
	channel, member, err := s.getChannelForMember(ctx, s.db, userID, channelID)
	if err != nil {
		return domain.Channel{}, nil, err
	}
	if !canChangeChannelInfo(member) {
		return domain.Channel{}, nil, domain.ErrChannelAdminRequired
	}
	if _, err := s.db.Exec(ctx, `
UPDATE channels
SET ttl_period = $2,
    updated_at = now()
WHERE id = $1`, channelID, period); err != nil {
		return domain.Channel{}, nil, fmt.Errorf("set channel history ttl: %w", err)
	}
	channel.TTLPeriod = period
	recipients, _ := s.ListActiveChannelMemberIDs(ctx, userID, channelID, 0)
	return channel, recipients, nil
}

func (s *ChannelStore) ClaimExpiredChannelMessages(ctx context.Context, now, limit int) ([]domain.DeleteChannelMessagesRequest, error) {
	if now <= 0 || limit <= 0 {
		return nil, nil
	}
	if limit > domain.MaxDeleteHistoryBatch {
		limit = domain.MaxDeleteHistoryBatch
	}
	rows, err := s.db.Query(ctx, `
SELECT c.creator_user_id, m.channel_id, m.id
FROM channel_messages m
JOIN channels c ON c.id = m.channel_id
WHERE m.expires_at > 0
  AND m.expires_at <= $1
  AND NOT m.deleted
  AND NOT c.deleted
ORDER BY m.expires_at ASC, m.channel_id ASC, m.id ASC
LIMIT $2`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("claim expired channel messages: %w", err)
	}
	defer rows.Close()
	type key struct {
		userID    int64
		channelID int64
	}
	index := make(map[key]int)
	out := make([]domain.DeleteChannelMessagesRequest, 0)
	for rows.Next() {
		var userID, channelID int64
		var id int
		if err := rows.Scan(&userID, &channelID, &id); err != nil {
			return nil, fmt.Errorf("scan expired channel message: %w", err)
		}
		k := key{userID: userID, channelID: channelID}
		pos, ok := index[k]
		if !ok {
			pos = len(out)
			index[k] = pos
			out = append(out, domain.DeleteChannelMessagesRequest{
				UserID:    userID,
				ChannelID: channelID,
				Date:      now,
				IDs:       make([]int, 0, 8),
			})
		}
		out[pos].IDs = append(out[pos].IDs, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate expired channel messages: %w", err)
	}
	return out, nil
}
