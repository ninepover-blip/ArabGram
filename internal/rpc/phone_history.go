package rpc

import (
	"context"

	"go.uber.org/zap"

	"telesrv/internal/domain"
)

// sendPhoneCallServiceMessage 把已终结的通话落成 messageActionPhoneCall 历史
// （完整复刻 pin 服务消息模式，messages_pin.go）。要点：
//   - sender 恒为主叫 AdminID——无论谁发起 discard（官方语义：主叫侧渲染
//     「呼出」、被叫侧渲染「未接来电/通话 X 分钟」，missed 在被叫侧计未读）；
//   - Origin 置零：双方全部设备（含 discard 发起设备）都经 outbox 收到
//     updateNewMessage（带 pts）——发起设备的 RPC 响应里只有 updatePhoneCall，
//     服务消息必须从 update 流来；离线设备重连经 getDifference 补收，正好
//     补偿信令直推的不可靠（「未接来电」由此而来）；
//   - RandomID 由 callID 确定性派生：状态机保证一通通话只落一次历史，
//     极端重放也会被发送管道的 random_id 去重吸收；
//   - 失败仅 Warn 不回滚：通话状态是真值，历史是装饰（与 pin 同哲学）。
//
// 注意：本路径绕过 RPC 层的 checkSendRateLimit（限速挂在 handler 侧）——
// 这是有意为之，呼叫轰炸防护由通话并发上限与振铃超时承担。
func (r *Router) sendPhoneCallServiceMessage(ctx context.Context, call domain.PhoneCall) {
	if r.deps.Messages == nil || !call.Terminal() {
		return
	}
	recipientBlocked, err := r.peerBlocksUser(ctx, call.AdminID, call.ParticipantID)
	if err != nil {
		recipientBlocked = false
	}
	_, err = r.deps.Messages.SendPrivateText(ctx, call.AdminID, domain.SendPrivateTextRequest{
		SenderUserID:    call.AdminID,
		RecipientUserID: call.ParticipantID,
		RandomID:        phoneCallServiceMessageRandomID(call.ID),
		Media: &domain.MessageMedia{
			Kind: domain.MessageMediaKindService,
			ServiceAction: &domain.MessageServiceAction{
				Kind: domain.MessageServiceActionPhoneCall,
				Call: &domain.MessagePhoneCallAction{
					CallID:   call.ID,
					Reason:   string(call.DiscardReason),
					Duration: call.Duration,
					Video:    call.Video,
				},
			},
		},
		Date:             int(r.clock.Now().Unix()),
		RecipientBlocked: recipientBlocked,
	})
	if err != nil {
		r.log.Warn("phone call service message",
			zap.Int64("call_id", call.ID),
			zap.Int64("admin_id", call.AdminID),
			zap.Int64("participant_id", call.ParticipantID),
			zap.Error(err))
	}
}

func phoneCallServiceMessageRandomID(callID int64) int64 {
	id := callID ^ 0x70686f6e6563616c // "phonecal"
	if id == 0 {
		return 1
	}
	return id
}
