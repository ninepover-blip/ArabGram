package channels

import (
	"context"

	"telesrv/internal/domain"
)

// SetActiveCall 写入/清除频道行上的活跃群通话关联（groupcalls 模块专用）。
func (s *Service) SetActiveCall(ctx context.Context, channelID, callID, callAccessHash int64, notEmpty bool) (domain.Channel, error) {
	if s == nil || s.channels == nil || channelID == 0 {
		return domain.Channel{}, domain.ErrChannelInvalid
	}
	return s.channels.SetActiveCall(ctx, channelID, callID, callAccessHash, notEmpty)
}

// AppendCallServiceMessage 生成群通话服务消息（started/ended/invite，带频道 pts）。
func (s *Service) AppendCallServiceMessage(ctx context.Context, channelID, senderUserID int64, date int, action domain.ChannelMessageAction) (domain.SendChannelMessageResult, error) {
	if s == nil || s.channels == nil {
		return domain.SendChannelMessageResult{}, domain.ErrChannelInvalid
	}
	if err := s.ensureCanSend(ctx, senderUserID); err != nil {
		return domain.SendChannelMessageResult{}, err
	}
	return s.channels.AppendCallServiceMessage(ctx, channelID, senderUserID, date, action)
}

// AppendStarGiftAdminLog 记录频道 Star gift 的 Recent Actions 快照；它不是频道历史消息，
// 因此不产生 channel pts / updateNewChannelMessage / subscriber fanout。
func (s *Service) AppendStarGiftAdminLog(ctx context.Context, channelID, senderUserID int64, savedID int64, date int, action domain.ChannelMessageAction) error {
	if s == nil || s.channels == nil {
		return domain.ErrChannelInvalid
	}
	if err := s.ensureCanSend(ctx, senderUserID); err != nil {
		return err
	}
	return s.channels.AppendStarGiftAdminLog(ctx, channelID, senderUserID, savedID, date, action)
}
