package postgres

import (
	"context"
	"fmt"
	"sort"

	"go.uber.org/zap"

	"telesrv/internal/store/postgres/sqlcgen"
)

// reservePrivateSendPts advances every distinct account watermark once in one
// PostgreSQL statement. Callers already hold the same users' ordered advisory
// locks, so batching changes neither lock order nor per-account PTS semantics.
func (s *MessageStore) reservePrivateSendPts(ctx context.Context, db sqlcgen.DBTX, userIDs []int64) (map[int64]int, error) {
	unique, err := uniquePositiveUserIDs(userIDs)
	if err != nil {
		return nil, err
	}
	if len(unique) == 0 {
		return map[int64]int{}, nil
	}
	sort.Slice(unique, func(i, j int) bool { return unique[i] < unique[j] })
	rows, err := db.Query(ctx, `
WITH requested AS MATERIALIZED (
  SELECT user_id
  FROM unnest($1::bigint[]) AS input(user_id)
  ORDER BY user_id
), reserved AS (
  INSERT INTO user_update_watermarks (user_id, contiguous_pts)
  SELECT user_id, 1
  FROM requested
  ON CONFLICT (user_id) DO UPDATE
  SET contiguous_pts = user_update_watermarks.contiguous_pts + 1,
      updated_at = now()
  RETURNING user_id, contiguous_pts
)
SELECT user_id, contiguous_pts
FROM reserved`, unique)
	if err != nil {
		return nil, fmt.Errorf("reserve private send pts: %w", err)
	}
	defer rows.Close()
	out := make(map[int64]int, len(unique))
	for rows.Next() {
		var userID int64
		var pts int
		if err := rows.Scan(&userID, &pts); err != nil {
			return nil, fmt.Errorf("scan private send pts: %w", err)
		}
		out[userID] = pts
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("reserve private send pts: %w", err)
	}
	if len(out) != len(unique) {
		return nil, fmt.Errorf("reserve private send pts: returned %d users, want %d", len(out), len(unique))
	}
	caller := traceCaller(2)
	for _, userID := range unique {
		s.log.Debug("pts_reserve",
			zap.String("scope", "user"),
			zap.Int64("user_id", userID),
			zap.Int("pts", out[userID]),
			zap.Int("pts_count", 1),
			zap.String("caller", caller),
		)
	}
	return out, nil
}

func (s *MessageStore) reservePts(ctx context.Context, db sqlcgen.DBTX, userID int64) (int, error) {
	return s.reservePtsN(ctx, db, userID, 1)
}

func (s *MessageStore) reservePtsN(ctx context.Context, db sqlcgen.DBTX, userID int64, count int) (int, error) {
	count = normalizePtsCount(count)
	caller := traceCaller(2)
	pts, err := reserveUserPts(ctx, db, userID, count)
	if err != nil {
		s.log.Warn("pts_reserve_failed",
			zap.String("scope", "user"),
			zap.Int64("user_id", userID),
			zap.Int("pts_count", count),
			zap.String("caller", caller),
			zap.Error(err),
			zap.Error(ctx.Err()),
		)
		return 0, err
	}
	s.log.Debug("pts_reserve",
		zap.String("scope", "user"),
		zap.Int64("user_id", userID),
		zap.Int("pts", pts),
		zap.Int("pts_count", count),
		zap.String("caller", caller),
	)
	return pts, nil
}
