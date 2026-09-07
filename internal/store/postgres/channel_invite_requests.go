package postgres

import (
	"context"
	"fmt"
	"strings"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func (s *ChannelStore) ListInviteImporters(ctx context.Context, req domain.ChannelInviteImportersRequest) (domain.ChannelInviteImporterList, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.ChannelInviteImporterList{}, domain.ErrChannelInvalid
	}
	if _, member, err := s.getChannelForMember(ctx, s.db, req.UserID, req.ChannelID); err != nil {
		return domain.ChannelInviteImporterList{}, err
	} else if !canExportChannelInvite(member) {
		return domain.ChannelInviteImporterList{}, domain.ErrChannelAdminRequired
	}
	var inviteID int64
	if req.Hash != "" {
		invite, err := s.getInviteByChannelHash(ctx, s.db, req.ChannelID, req.Hash, false)
		if err != nil {
			return domain.ChannelInviteImporterList{}, err
		}
		inviteID = invite.InviteID
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelInviteListLimit {
		limit = domain.MaxChannelInviteListLimit
	}
	args := []any{req.ChannelID, req.Requested, inviteID, req.Query, req.OffsetDate, req.OffsetUserID, limit}
	where := []string{
		"i.channel_id = $1",
		"i.requested = $2",
		"($3::bigint = 0 OR i.invite_id = $3)",
		"($4::text = '' OR lower(trim(u.username || ' ' || u.first_name || ' ' || u.last_name)) LIKE '%' || lower($4) || '%')",
		"(($5::int = 0 AND $6::bigint = 0) OR (i.date, i.user_id) < ($5, $6))",
	}
	whereSQL := strings.Join(where, " AND ")
	var total int
	if err := s.db.QueryRow(ctx, `
SELECT COUNT(*)::int
FROM channel_invite_importers i
JOIN users u ON u.id = i.user_id
WHERE `+whereSQL, args[:6]...).Scan(&total); err != nil {
		return domain.ChannelInviteImporterList{}, err
	}
	rows, err := s.db.Query(ctx, `
SELECT i.channel_id, i.invite_id, i.user_id, i.date, i.requested, i.approved_by, i.via_chatlist, i.about
FROM channel_invite_importers i
JOIN users u ON u.id = i.user_id
WHERE `+whereSQL+`
ORDER BY i.date DESC, i.user_id DESC
LIMIT $7`, args...)
	if err != nil {
		return domain.ChannelInviteImporterList{}, err
	}
	defer rows.Close()
	importers := make([]domain.ChannelInviteImporter, 0, limit)
	for rows.Next() {
		var importer domain.ChannelInviteImporter
		if err := rows.Scan(&importer.ChannelID, &importer.InviteID, &importer.UserID, &importer.Date, &importer.Requested, &importer.ApprovedBy, &importer.ViaChatlist, &importer.About); err != nil {
			return domain.ChannelInviteImporterList{}, err
		}
		importers = append(importers, importer)
	}
	if err := rows.Err(); err != nil {
		return domain.ChannelInviteImporterList{}, err
	}
	return domain.ChannelInviteImporterList{Count: total, Importers: importers}, nil
}

func (s *ChannelStore) PendingJoinRequests(ctx context.Context, channelID int64, limit int) (domain.ChannelPendingJoinRequests, error) {
	if channelID == 0 {
		return domain.ChannelPendingJoinRequests{}, domain.ErrChannelInvalid
	}
	if limit <= 0 || limit > domain.MaxChannelPendingJoinRecentRequesters {
		limit = domain.MaxChannelPendingJoinRecentRequesters
	}
	rows, err := s.db.Query(ctx, `
SELECT user_id, COUNT(*) OVER()::int
FROM channel_invite_importers
WHERE channel_id = $1 AND requested
ORDER BY date DESC, user_id DESC
LIMIT $2`, channelID, limit)
	if err != nil {
		return domain.ChannelPendingJoinRequests{}, fmt.Errorf("list pending channel join requests: %w", err)
	}
	defer rows.Close()
	out := domain.ChannelPendingJoinRequests{
		ChannelID:        channelID,
		RecentRequesters: make([]int64, 0, limit),
	}
	for rows.Next() {
		var userID int64
		var count int
		if err := rows.Scan(&userID, &count); err != nil {
			return domain.ChannelPendingJoinRequests{}, err
		}
		out.Count = count
		out.RecentRequesters = append(out.RecentRequesters, userID)
	}
	if err := rows.Err(); err != nil {
		return domain.ChannelPendingJoinRequests{}, err
	}
	return out, nil
}

func (s *ChannelStore) HideAllChatJoinRequests(ctx context.Context, req domain.HideChannelJoinRequestsRequest, effects store.DeliveryEffectsBuilder[store.ChannelPendingJoinDeliverySnapshot]) (domain.CreateChannelResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 {
		return domain.CreateChannelResult{}, domain.ErrChannelInvalid
	}
	if effects == nil {
		return domain.CreateChannelResult{}, store.ErrDeliveryOutboxRequired
	}
	if req.Date == 0 {
		req.Date = nowUnix()
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.CreateChannelResult{}, fmt.Errorf("hide all channel join requests: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("begin hide all channel join requests: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	channel, member, err := s.getChannelForMember(ctx, tx, req.UserID, req.ChannelID)
	if err != nil {
		return domain.CreateChannelResult{}, err
	}
	if !canExportChannelInvite(member) {
		return domain.CreateChannelResult{}, domain.ErrChannelAdminRequired
	}
	var inviteID int64
	if req.Hash != "" {
		invite, err := s.getInviteByChannelHash(ctx, tx, req.ChannelID, req.Hash, true)
		if err != nil {
			return domain.CreateChannelResult{}, err
		}
		inviteID = invite.InviteID
	}
	limit := req.Limit
	if limit <= 0 || limit > domain.MaxChannelHideJoinRequests {
		limit = domain.MaxChannelHideJoinRequests
	}
	rows, err := tx.Query(ctx, `
SELECT user_id, invite_id
FROM channel_invite_importers
WHERE channel_id = $1 AND requested AND ($2::bigint = 0 OR invite_id = $2)
ORDER BY date ASC, user_id ASC
LIMIT $3
FOR UPDATE`, req.ChannelID, inviteID, limit)
	if err != nil {
		return domain.CreateChannelResult{}, err
	}
	targets := make([]domain.ChannelInviteImporter, 0, limit)
	for rows.Next() {
		var importer domain.ChannelInviteImporter
		if err := rows.Scan(&importer.UserID, &importer.InviteID); err != nil {
			rows.Close()
			return domain.CreateChannelResult{}, err
		}
		importer.ChannelID = req.ChannelID
		importer.Requested = true
		targets = append(targets, importer)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return domain.CreateChannelResult{}, err
	}
	rows.Close()
	result := domain.CreateChannelResult{Channel: channel}
	for _, importer := range targets {
		invite := domain.ChannelInvite{ChannelID: req.ChannelID, AdminUserID: req.UserID}
		if importer.InviteID != 0 {
			invite, err = s.getInviteByID(ctx, tx, req.ChannelID, importer.InviteID, true)
			if err != nil {
				return domain.CreateChannelResult{}, err
			}
		}
		if !req.Approved {
			if err := deletePendingInviteImporterTx(ctx, tx, invite, importer.UserID); err != nil {
				return domain.CreateChannelResult{}, err
			}
			continue
		}
		next, err := s.approveInviteImporterTx(ctx, tx, channel, invite, importer.UserID, req.UserID, req.Date)
		if err != nil {
			return domain.CreateChannelResult{}, err
		}
		result = next
		channel = next.Channel
	}
	result.Channel = channel
	snapshot := store.ChannelPendingJoinDeliverySnapshot{}
	if len(targets) != 0 {
		snapshot, err = pendingJoinDeliverySnapshotTx(ctx, tx, channel)
		if err != nil {
			return domain.CreateChannelResult{}, err
		}
	}
	if err := applyPendingJoinDeliveryTx(ctx, tx, snapshot, effects); err != nil {
		return domain.CreateChannelResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("commit hide all channel join requests: %w", err)
	}
	committed = true
	result.Recipients, _ = s.ListActiveChannelMemberIDs(ctx, req.UserID, req.ChannelID, 0)
	return result, nil
}

func (s *ChannelStore) ListChannelInviteAdminMemberIDs(ctx context.Context, channelID int64, limit int) ([]int64, error) {
	if channelID == 0 {
		return nil, domain.ErrChannelInvalid
	}
	if limit <= 0 || limit > domain.MaxChannelRealtimeFanout {
		limit = domain.MaxChannelRealtimeFanout
	}
	rows, err := s.db.Query(ctx, `
SELECT user_id
FROM channel_members
WHERE channel_id = $1
  AND status = 'active'
  AND (
    role = 'creator' OR
    (role = 'admin' AND (
      (admin_rights->>'InviteUsers')::boolean IS TRUE OR
      (admin_rights->>'ChangeInfo')::boolean IS TRUE
    ))
  )
ORDER BY user_id
LIMIT $2`, channelID, limit)
	if err != nil {
		return nil, fmt.Errorf("list channel invite admin members: %w", err)
	}
	defer rows.Close()
	out := make([]int64, 0, limit)
	for rows.Next() {
		var userID int64
		if err := rows.Scan(&userID); err != nil {
			return nil, err
		}
		out = append(out, userID)
	}
	return out, rows.Err()
}
