package postgres

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"sort"
	"telesrv/internal/domain"
)

func (s *ChannelStore) ListChannelUnreadReactions(ctx context.Context, viewerUserID int64, filter domain.ChannelUnreadReactionsFilter) (domain.ChannelHistory, error) {
	channel, member, err := s.getChannelForMember(ctx, s.db, viewerUserID, filter.ChannelID)
	if err != nil {
		return domain.ChannelHistory{}, err
	}
	limit := filter.Limit
	if limit <= 0 || limit > domain.MaxChannelUnreadReactionsLimit {
		limit = domain.MaxChannelUnreadReactionsLimit
	}
	filter.AddOffset = domain.ClampMessageHistoryAddOffset(filter.AddOffset)
	count, err := s.countChannelUnreadReactions(ctx, viewerUserID, filter, member.AvailableMinID)
	if err != nil {
		return domain.ChannelHistory{}, err
	}
	messages, err := s.queryChannelUnreadReactionsPage(ctx, viewerUserID, filter, member.AvailableMinID, limit)
	if err != nil {
		return domain.ChannelHistory{}, err
	}
	if err := s.populateChannelMessageReplies(ctx, s.db, viewerUserID, channel, messages); err != nil {
		return domain.ChannelHistory{}, err
	}
	if err := s.populateChannelMessagesReactions(ctx, s.db, viewerUserID, []domain.Channel{channel}, messages); err != nil {
		return domain.ChannelHistory{}, err
	}
	return domain.ChannelHistory{Channel: channel, Messages: messages, Count: count}, nil
}

func (s *ChannelStore) ReadChannelReactions(ctx context.Context, req domain.ReadChannelReactionsRequest) (domain.ReadChannelReactionsResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.ReadChannelReactionsResult{}, domain.ErrChannelInvalid
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.ReadChannelReactionsResult{}, fmt.Errorf("read channel reactions: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.ReadChannelReactionsResult{}, fmt.Errorf("begin read channel reactions: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, _, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID)
	if err != nil {
		return domain.ReadChannelReactionsResult{}, err
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelReadReactionsBatch {
		limit = domain.MaxChannelReadReactionsBatch
	}
	cleared, remaining, err := readChannelReactionsTx(ctx, tx, req.UserID, req.ChannelID, req.TopMsgID, limit)
	if err != nil {
		return domain.ReadChannelReactionsResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.ReadChannelReactionsResult{}, fmt.Errorf("commit read channel reactions: %w", err)
	}
	committed = true
	offset := 0
	if remaining > 0 {
		offset = 1
	}
	return domain.ReadChannelReactionsResult{
		Channel:    channel,
		Cleared:    cleared,
		Remaining:  remaining,
		Offset:     offset,
		ChannelPts: channel.Pts,
	}, nil
}

func (s *ChannelStore) countChannelUnreadReactionsForTop(ctx context.Context, userID, channelID int64, topMsgID, availableMinID int) int {
	var count int
	_ = s.db.QueryRow(ctx, `
SELECT COUNT(DISTINCT r.message_id)::int
FROM channel_message_reactions r
JOIN channel_messages cm ON cm.channel_id = r.channel_id AND cm.id = r.message_id
WHERE r.sender_user_id = $1
  AND r.channel_id = $2
  AND r.unread
  AND r.reacted_user_id <> $1
  AND cm.id > $4
  AND NOT cm.deleted
  AND (cm.id = $3 OR COALESCE(NULLIF(cm.reply_to_top_id, 0), NULLIF(cm.reply_to_msg_id, 0), 0) = $3)`, userID, channelID, topMsgID, availableMinID).Scan(&count)
	return count
}

func (s *ChannelStore) countChannelUnreadReactions(ctx context.Context, userID int64, filter domain.ChannelUnreadReactionsFilter, availableMinID int) (int, error) {
	where, args := channelUnreadReactionBaseWhere(userID, filter, availableMinID)
	var count int
	if err := s.db.QueryRow(ctx, `
SELECT COUNT(DISTINCT cm.id)::int
FROM channel_message_reactions r
JOIN channel_messages cm ON cm.channel_id = r.channel_id AND cm.id = r.message_id
WHERE `+where, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("count channel unread reactions: %w", err)
	}
	return count, nil
}

func (s *ChannelStore) queryChannelUnreadReactionsPage(ctx context.Context, userID int64, filter domain.ChannelUnreadReactionsFilter, availableMinID, limit int) ([]domain.ChannelMessage, error) {
	if limit <= 0 {
		return nil, nil
	}
	switch messageHistoryLoadType(filter.AddOffset, limit) {
	case messageHistoryLoadForward:
		return s.queryChannelUnreadReactionsForward(ctx, userID, filter, availableMinID, limit)
	case messageHistoryLoadAround:
		forwardLimit := -filter.AddOffset
		if forwardLimit > limit {
			forwardLimit = limit
		}
		backwardLimit := limit + filter.AddOffset
		if backwardLimit < 0 {
			backwardLimit = 0
		}
		forward, err := s.queryChannelUnreadReactionsForward(ctx, userID, filter, availableMinID, forwardLimit)
		if err != nil {
			return nil, err
		}
		backward, err := s.queryChannelUnreadReactionsBackward(ctx, userID, filter, availableMinID, backwardLimit, true)
		if err != nil {
			return nil, err
		}
		out := append(forward, backward...)
		sort.SliceStable(out, func(i, j int) bool { return channelMessageLess(out[i], out[j]) })
		return out, nil
	default:
		start := filter.AddOffset
		if start < 0 {
			start = 0
		}
		items, err := s.queryChannelUnreadReactionsBackward(ctx, userID, filter, availableMinID, limit+start, false)
		if err != nil || start >= len(items) {
			return nil, err
		}
		return items[start:], nil
	}
}

func (s *ChannelStore) queryChannelUnreadReactionsBackward(ctx context.Context, userID int64, filter domain.ChannelUnreadReactionsFilter, availableMinID, limit int, includeOffset bool) ([]domain.ChannelMessage, error) {
	if limit <= 0 {
		return nil, nil
	}
	where, args := channelUnreadReactionBaseWhere(userID, filter, availableMinID)
	where, args = appendChannelUnreadReactionBackwardOffset(where, args, filter, includeOffset)
	args = append(args, limit)
	return s.queryChannelUnreadReactions(ctx, filter.ChannelID, where, args, "DESC")
}

func (s *ChannelStore) queryChannelUnreadReactionsForward(ctx context.Context, userID int64, filter domain.ChannelUnreadReactionsFilter, availableMinID, limit int) ([]domain.ChannelMessage, error) {
	if limit <= 0 {
		return nil, nil
	}
	where, args := channelUnreadReactionBaseWhere(userID, filter, availableMinID)
	where, args = appendChannelUnreadReactionForwardOffset(where, args, filter)
	args = append(args, limit)
	out, err := s.queryChannelUnreadReactions(ctx, filter.ChannelID, where, args, "ASC")
	if err != nil {
		return nil, err
	}
	sort.SliceStable(out, func(i, j int) bool { return channelMessageLess(out[i], out[j]) })
	return out, nil
}

func (s *ChannelStore) queryChannelUnreadReactions(ctx context.Context, channelID int64, where string, args []any, order string) ([]domain.ChannelMessage, error) {
	rows, err := s.db.Query(ctx, `
SELECT `+channelMessageColumns+`
FROM channel_messages
WHERE channel_id = $2
  AND id = ANY(ARRAY(
      SELECT DISTINCT cm.id
      FROM channel_message_reactions r
      JOIN channel_messages cm ON cm.channel_id = r.channel_id AND cm.id = r.message_id
      WHERE `+where+`
      ORDER BY cm.id `+order+`
      LIMIT $`+fmt.Sprint(len(args))+`
  )::int[])
ORDER BY id `+order, args...)
	_ = channelID
	if err != nil {
		return nil, fmt.Errorf("list channel unread reactions: %w", err)
	}
	defer rows.Close()
	out := make([]domain.ChannelMessage, 0)
	for rows.Next() {
		msg, err := scanChannelMessage(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func channelUnreadReactionBaseWhere(userID int64, filter domain.ChannelUnreadReactionsFilter, availableMinID int) (string, []any) {
	args := []any{userID, filter.ChannelID}
	where := "r.sender_user_id = $1 AND r.channel_id = $2 AND r.unread AND r.reacted_user_id <> $1 AND NOT cm.deleted"
	if availableMinID > 0 {
		args = append(args, availableMinID)
		where += fmt.Sprintf(" AND cm.id > $%d", len(args))
	}
	if filter.TopMsgID > 0 {
		args = append(args, filter.TopMsgID)
		where += fmt.Sprintf(" AND (cm.id = $%d OR COALESCE(NULLIF(cm.reply_to_top_id, 0), NULLIF(cm.reply_to_msg_id, 0), 0) = $%d)", len(args), len(args))
	}
	if filter.MaxID > 0 {
		args = append(args, filter.MaxID)
		where += fmt.Sprintf(" AND cm.id < $%d", len(args))
	}
	if filter.MinID > 0 {
		args = append(args, filter.MinID)
		where += fmt.Sprintf(" AND cm.id > $%d", len(args))
	}
	return where, args
}

func appendChannelUnreadReactionBackwardOffset(where string, args []any, filter domain.ChannelUnreadReactionsFilter, include bool) (string, []any) {
	if filter.OffsetID > 0 {
		args = append(args, filter.OffsetID)
		if include {
			return where + fmt.Sprintf(" AND cm.id <= $%d", len(args)), args
		}
		return where + fmt.Sprintf(" AND cm.id < $%d", len(args)), args
	}
	return where, args
}

func appendChannelUnreadReactionForwardOffset(where string, args []any, filter domain.ChannelUnreadReactionsFilter) (string, []any) {
	if filter.OffsetID > 0 {
		args = append(args, filter.OffsetID)
		return where + fmt.Sprintf(" AND cm.id > $%d", len(args)), args
	}
	return where, args
}

func readChannelReactionsTx(ctx context.Context, tx pgx.Tx, userID, channelID int64, topMsgID, limit int) (int, int, error) {
	var cleared, remaining int
	if err := tx.QueryRow(ctx, `
WITH member_scope AS (
    SELECT available_min_id
    FROM channel_members
    WHERE user_id = $1 AND channel_id = $2
),
target_messages AS (
    SELECT DISTINCT r.message_id
    FROM channel_message_reactions r
    JOIN channel_messages cm ON cm.channel_id = r.channel_id AND cm.id = r.message_id
    JOIN member_scope ms ON true
    WHERE r.sender_user_id = $1
      AND r.channel_id = $2
      AND r.unread
      AND r.reacted_user_id <> $1
      AND cm.id > ms.available_min_id
      AND NOT cm.deleted
      AND ($3 = 0 OR cm.id = $3 OR COALESCE(NULLIF(cm.reply_to_top_id, 0), NULLIF(cm.reply_to_msg_id, 0), 0) = $3)
    ORDER BY r.message_id DESC
    LIMIT $4
),
updated AS (
    UPDATE channel_message_reactions r
    SET unread = false,
        updated_at = now()
    WHERE r.sender_user_id = $1
      AND r.channel_id = $2
      AND r.message_id IN (SELECT message_id FROM target_messages)
      AND r.unread
    RETURNING r.message_id
),
remaining_scoped AS (
    SELECT COUNT(DISTINCT r.message_id)::int AS count
    FROM channel_message_reactions r
    JOIN channel_messages cm ON cm.channel_id = r.channel_id AND cm.id = r.message_id
    JOIN member_scope ms ON true
    WHERE r.sender_user_id = $1
      AND r.channel_id = $2
      AND r.unread
      AND r.reacted_user_id <> $1
      AND cm.id > ms.available_min_id
      AND NOT cm.deleted
      AND ($3 = 0 OR cm.id = $3 OR COALESCE(NULLIF(cm.reply_to_top_id, 0), NULLIF(cm.reply_to_msg_id, 0), 0) = $3)
),
remaining_all AS (
    SELECT COUNT(DISTINCT r.message_id)::int AS count
    FROM channel_message_reactions r
    JOIN channel_messages cm ON cm.channel_id = r.channel_id AND cm.id = r.message_id
    JOIN member_scope ms ON true
    WHERE r.sender_user_id = $1
      AND r.channel_id = $2
      AND r.unread
      AND r.reacted_user_id <> $1
      AND cm.id > ms.available_min_id
      AND NOT cm.deleted
),
updated_dialog AS (
    UPDATE channel_dialogs
    SET unread_reactions_count = (SELECT count FROM remaining_all),
        updated_at = now()
    WHERE user_id = $1 AND channel_id = $2
)
SELECT (SELECT COUNT(DISTINCT message_id)::int FROM updated), (SELECT count FROM remaining_scoped)`, userID, channelID, topMsgID, limit).Scan(&cleared, &remaining); err != nil {
		return 0, 0, fmt.Errorf("read channel reactions: %w", err)
	}
	return cleared, remaining, nil
}

func clearChannelUnreadReactionsForMessageIDsTx(ctx context.Context, tx pgx.Tx, userID, channelID int64, ids []int32) ([]int, error) {
	if userID == 0 || channelID == 0 || len(ids) == 0 {
		return nil, nil
	}
	rows, err := tx.Query(ctx, `
UPDATE channel_message_reactions
SET unread = false,
    updated_at = now()
WHERE sender_user_id = $1
  AND channel_id = $2
  AND message_id = ANY($3::int[])
  AND unread
  AND reacted_user_id <> $1
RETURNING message_id`, userID, channelID, ids)
	if err != nil {
		return nil, fmt.Errorf("clear visible channel unread reactions: %w", err)
	}
	clearedSet := make(map[int]struct{})
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		clearedSet[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if len(clearedSet) == 0 {
		return nil, nil
	}
	cleared := make([]int, 0, len(clearedSet))
	for id := range clearedSet {
		cleared = append(cleared, id)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(cleared)))
	if err := refreshChannelUnreadReactionsCountTx(ctx, tx, userID, channelID); err != nil {
		return nil, err
	}
	return cleared, nil
}

func refreshChannelUnreadReactionsCountTx(ctx context.Context, tx pgx.Tx, userID, channelID int64) error {
	if userID == 0 || channelID == 0 {
		return nil
	}
	if _, err := tx.Exec(ctx, `
WITH active AS (
    SELECT m.available_min_id
    FROM channel_members m
    WHERE m.user_id = $1
      AND m.channel_id = $2
      AND m.status = 'active'
      AND NOT COALESCE((m.banned_rights->>'ViewMessages')::boolean, false)
),
counts AS (
    SELECT COUNT(DISTINCT r.message_id)::int AS count
    FROM channel_message_reactions r
    JOIN channel_messages cm ON cm.channel_id = r.channel_id AND cm.id = r.message_id
    JOIN active a ON true
    WHERE r.sender_user_id = $1
      AND r.channel_id = $2
      AND r.unread
      AND r.reacted_user_id <> $1
      AND cm.id > a.available_min_id
      AND NOT cm.deleted
)
INSERT INTO channel_dialogs (user_id, channel_id, unread_reactions_count)
SELECT $1, $2, counts.count
FROM active, counts
ON CONFLICT (user_id, channel_id) DO UPDATE SET
    unread_reactions_count = EXCLUDED.unread_reactions_count,
    updated_at = now()`, userID, channelID); err != nil {
		return fmt.Errorf("refresh channel unread reactions count: %w", err)
	}
	return nil
}

func refreshChannelUnreadReactionsCountsForMessagesTx(ctx context.Context, tx pgx.Tx, channelID int64, ids []int) error {
	if len(ids) == 0 {
		return nil
	}
	rows, err := tx.Query(ctx, `
SELECT DISTINCT sender_user_id
FROM channel_message_reactions
WHERE channel_id = $1
  AND message_id = ANY($2::int[])
  AND sender_user_id <> 0`, channelID, int32s(ids))
	if err != nil {
		return fmt.Errorf("list channel unread reaction owners: %w", err)
	}
	defer rows.Close()
	userIDs := make([]int64, 0)
	for rows.Next() {
		var userID int64
		if err := rows.Scan(&userID); err != nil {
			return err
		}
		userIDs = append(userIDs, userID)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, userID := range userIDs {
		if err := refreshChannelUnreadReactionsCountTx(ctx, tx, userID, channelID); err != nil {
			return err
		}
	}
	return nil
}
