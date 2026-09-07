package postgres

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"strings"
	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func (s *ChannelStore) SetParticipantsHidden(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	var channel domain.Channel
	if err := withTx(ctx, s.db, "toggle channel participants hidden", func(tx pgx.Tx) error {
		var err error
		var member domain.ChannelMember
		channel, member, err = s.getChannelForMember(ctx, tx, userID, channelID)
		if err != nil {
			return err
		}
		if !channel.Megagroup || !canBanChannelUsers(member) {
			return domain.ErrChannelAdminRequired
		}
		if _, err := tx.Exec(ctx, `UPDATE channels SET participants_hidden = $2, updated_at = now() WHERE id = $1`, channelID, enabled); err != nil {
			return fmt.Errorf("update channel participants hidden: %w", err)
		}
		channel.ParticipantsHidden = enabled
		return nil
	}); err != nil {
		return domain.Channel{}, err
	}
	return channel, nil
}

func (s *ChannelStore) SetJoinToSend(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	var channel domain.Channel
	if err := withTx(ctx, s.db, "toggle channel join_to_send", func(tx pgx.Tx) error {
		var err error
		var member domain.ChannelMember
		channel, member, err = s.getChannelForMember(ctx, tx, userID, channelID)
		if err != nil {
			return err
		}
		if !channel.Megagroup || !canExportChannelInvite(member) {
			return domain.ErrChannelAdminRequired
		}
		if _, err := tx.Exec(ctx, `UPDATE channels SET join_to_send = $2, updated_at = now() WHERE id = $1`, channelID, enabled); err != nil {
			return fmt.Errorf("update channel join_to_send: %w", err)
		}
		channel.JoinToSend = enabled
		return nil
	}); err != nil {
		return domain.Channel{}, err
	}
	return channel, nil
}

func (s *ChannelStore) SetJoinRequest(ctx context.Context, userID, channelID int64, enabled bool) (domain.Channel, error) {
	if userID == 0 || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	var channel domain.Channel
	if err := withTx(ctx, s.db, "toggle channel join_request", func(tx pgx.Tx) error {
		var err error
		var member domain.ChannelMember
		channel, member, err = s.getChannelForMember(ctx, tx, userID, channelID)
		if err != nil {
			return err
		}
		if !channel.Megagroup || !canExportChannelInvite(member) {
			return domain.ErrChannelAdminRequired
		}
		if enabled && strings.TrimSpace(channel.Username) == "" {
			return domain.ErrChatPublicRequired
		}
		if _, err := tx.Exec(ctx, `UPDATE channels SET join_request = $2, updated_at = now() WHERE id = $1`, channelID, enabled); err != nil {
			return fmt.Errorf("update channel join_request: %w", err)
		}
		channel.JoinRequest = enabled
		return nil
	}); err != nil {
		return domain.Channel{}, err
	}
	return channel, nil
}

func (s *ChannelStore) recordPublicJoinRequestTx(ctx context.Context, tx pgx.Tx, channel domain.Channel, userID int64, date int) error {
	if existing, err := s.getChannelMember(ctx, tx, channel.ID, userID); err == nil {
		if existing.Status == domain.ChannelMemberActive {
			return domain.ErrUserAlreadyParticipant
		}
		if existing.Status == domain.ChannelMemberKicked || existing.Status == domain.ChannelMemberBanned || existing.BannedRights.ViewMessages {
			return domain.ErrInviteHashInvalid
		}
	} else if !errors.Is(err, domain.ErrChannelPrivate) {
		return err
	}
	tag, err := tx.Exec(ctx, `
INSERT INTO channel_invite_importers (channel_id, invite_id, user_id, date, requested)
VALUES ($1, 0, $2, $3, true)
ON CONFLICT (channel_id, user_id) DO UPDATE
SET invite_id = 0,
    date = EXCLUDED.date,
    requested = true,
    approved_by = 0,
    updated_at = now()
WHERE NOT channel_invite_importers.requested`, channel.ID, userID, date)
	if err != nil {
		return fmt.Errorf("insert public channel join request: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return domain.ErrInviteRequestSent
	}
	return nil
}

func (s *ChannelStore) HideChatJoinRequest(ctx context.Context, req domain.HideChannelJoinRequestRequest, effects store.DeliveryEffectsBuilder[store.ChannelPendingJoinDeliverySnapshot]) (domain.CreateChannelResult, error) {
	if req.UserID == 0 || req.ChannelID == 0 || req.TargetUserID == 0 {
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
		return domain.CreateChannelResult{}, fmt.Errorf("hide channel join request: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("begin hide channel join request: %w", err)
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
	importer, err := s.getPendingInviteImporterTx(ctx, tx, req.ChannelID, req.TargetUserID, true)
	if err != nil {
		return domain.CreateChannelResult{}, err
	}
	invite := domain.ChannelInvite{ChannelID: req.ChannelID, AdminUserID: req.UserID}
	if importer.InviteID != 0 {
		invite, err = s.getInviteByID(ctx, tx, req.ChannelID, importer.InviteID, true)
		if err != nil {
			return domain.CreateChannelResult{}, err
		}
	}
	var result domain.CreateChannelResult
	if req.Approved {
		result, err = s.approveInviteImporterTx(ctx, tx, channel, invite, req.TargetUserID, req.UserID, req.Date)
		if err != nil {
			return domain.CreateChannelResult{}, err
		}
	} else if err := deletePendingInviteImporterTx(ctx, tx, invite, req.TargetUserID); err != nil {
		return domain.CreateChannelResult{}, err
	} else {
		result = domain.CreateChannelResult{Channel: channel}
	}
	snapshot, err := pendingJoinDeliverySnapshotTx(ctx, tx, result.Channel)
	if err != nil {
		return domain.CreateChannelResult{}, err
	}
	if err := applyPendingJoinDeliveryTx(ctx, tx, snapshot, effects); err != nil {
		return domain.CreateChannelResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.CreateChannelResult{}, fmt.Errorf("commit hide channel join request: %w", err)
	}
	committed = true
	recipients, _ := s.ListActiveChannelMemberIDs(ctx, req.UserID, req.ChannelID, 0)
	result.Recipients = recipients
	return result, nil
}
