package rpc

import (
	"context"
	"errors"

	"github.com/iamxvbaba/td/proto"
	"github.com/iamxvbaba/td/tg"
	"go.uber.org/zap"

	appsecret "telesrv/internal/app/secretchat"
	"telesrv/internal/domain"
)

// 私聊端对端加密（Secret Chat / encrypted chat）握手 RPC handler。状态机与 DH 校验
// 归 app/secretchat；本文件只做鉴权、入参校验、TL 转换与在线推送编排。
//
// requestEncryption / acceptEncryption / discardEncryption 的握手闭环 +
// updateEncryption 在线设备定向、durable 离线 getDifference 补偿与输家设备收敛；
// qts 消息投递（sendEncrypted 等）见 encrypted_messages.go，
// 设计 docs/secret-chat-module.md。服务端是盲中继，永不接触共享密钥与明文。

// secretChatErr 把 app/secretchat + domain 业务错误映射为 RPC_ERROR。
func secretChatErr(err error) error {
	switch {
	case errors.Is(err, appsecret.ErrGAInvalid):
		return dhGAInvalidErr()
	case errors.Is(err, domain.ErrSecretChatAlreadyAccepted):
		return encryptionAlreadyAcceptedErr()
	case errors.Is(err, domain.ErrSecretChatAlreadyDeclined):
		return encryptionAlreadyDeclinedErr()
	case errors.Is(err, domain.ErrSecretChatRandomIDDuplicate):
		return secretChatRandomIDDuplicateErr()
	case errors.Is(err, domain.ErrSecretChatNotFound):
		return chatIDInvalidErr()
	default:
		return internalErr()
	}
}

// secretChatRequireUser 是密聊 RPC 的统一登录闸门。
func (r *Router) secretChatRequireUser(ctx context.Context) (int64, error) {
	userID, ok, err := r.currentUserID(ctx)
	if err != nil {
		return 0, internalErr()
	}
	if !ok {
		return 0, authKeyUnregisteredErr()
	}
	return userID, nil
}

// businessAuthKeyIDFrom 返回当前连接业务视角 auth_key_id 的 int64 绑定值。
func businessAuthKeyIDFrom(ctx context.Context) (int64, bool) {
	id, ok := AuthKeyIDFrom(ctx)
	if !ok {
		return 0, false
	}
	return businessAuthKeyInt64(id), true
}

// pushUpdateEncryption is only the online accelerator for the authoritative
// encrypted_state_events fact committed with the secret-chat mutation. It
// never enters an account PTS/non-PTS lane. targetAuthKeyID=0 is account-wide;
// an accepted chat must use its exact bound device.
func (r *Router) pushUpdateEncryption(ctx context.Context, targetUserID, targetAuthKeyID int64, chat domain.SecretChat, logMessage string) {
	now := int(r.clock.Now().Unix())
	upd := &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdateEncryption{
			Chat: tgEncryptedChatForViewer(chat, targetUserID),
			Date: now,
		}},
		Users: r.tgUsersForIDs(ctx, targetUserID, []int64{chat.AdminUserID, chat.ParticipantUserID}),
		Chats: []tg.ChatClass{},
		Date:  now,
		Seq:   0,
	}
	if targetAuthKeyID == 0 {
		r.pushUserMessageTransient(ctx, targetUserID, logMessage, upd)
		return
	}
	if targeted, ok := r.deps.Sessions.(AuthKeyTargetedSessionPusher); ok {
		_, _ = targeted.PushToUserAuthKeyTransient(ctx, targetUserID, deviceAuthKeyBytes(targetAuthKeyID), proto.MessageFromServer, upd, r.cfg.OutboundPushTimeout)
		return
	}
	r.log.Error("secret chat targeted session binder unavailable",
		zap.String("update", logMessage),
		zap.Int64("target_user_id", targetUserID),
		zap.Int64("target_auth_key_id", targetAuthKeyID))
}

// pushAcceptedLoserDiscarded 让 participant 账号中除获胜 business auth key 外的在线设备
// 删除 requested 幽灵。离线/未就绪设备由 accept 后新增的账号级 state event 收敛。
func (r *Router) pushAcceptedLoserDiscarded(ctx context.Context, chat domain.SecretChat, date int) {
	if chat.ParticipantUserID == 0 || chat.ParticipantAuthKeyID == 0 {
		return
	}
	upd := &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdateEncryption{
			Chat: &tg.EncryptedChatDiscarded{ID: chat.ID, HistoryDeleted: true},
			Date: date,
		}},
		Users: r.tgUsersForIDs(ctx, chat.ParticipantUserID, []int64{chat.AdminUserID, chat.ParticipantUserID}),
		Chats: []tg.ChatClass{},
		Date:  date,
		Seq:   0,
	}
	if targeted, ok := r.deps.Sessions.(AuthKeyTargetedSessionPusher); ok {
		_, _ = targeted.PushToUserExceptBusinessAuthKey(ctx, chat.ParticipantUserID,
			deviceAuthKeyBytes(chat.ParticipantAuthKeyID), proto.MessageFromServer, upd, r.cfg.OutboundPushTimeout)
		return
	}
	r.log.Error("secret chat targeted session binder unavailable",
		zap.String("update", "secret chat accept loser discarded"),
		zap.Int64("target_user_id", chat.ParticipantUserID),
		zap.Int64("exclude_auth_key_id", chat.ParticipantAuthKeyID))
}

// discardSecretChatsForAuthKey 在设备登出 / 授权撤销时级联 discard 该 perm auth_key 绑定的
// 全部活跃密聊，并向对端推送 encryptedChatDiscarded（在线加速）。durable state event 已由
// SecretChatService 与 state mutation 同边界写入；ownerUserID 是被销毁设备的所有者，用于定位对端。
// 修复 P1：此前 onAuthLogOut 等不级联 discard，对端继续往死 auth_key 投递成静默死链
// （消息 acked=f / qts 永久积压，对端永看不到 discarded）。
func (r *Router) discardSecretChatsForAuthKey(ctx context.Context, businessAuthKeyID, ownerUserID int64) {
	if r.deps.SecretChats == nil || businessAuthKeyID == 0 || ownerUserID == 0 {
		return
	}
	now := int(r.clock.Now().Unix())
	discarded, err := r.deps.SecretChats.DiscardForAuthKey(ctx, businessAuthKeyID, ownerUserID, now)
	if err != nil {
		// DiscardForAuthKey 出错也会返回已成功 discard 的部分，继续通知这部分对端。
		r.log.Debug("cascade discard secret chats for auth key", zap.Error(err))
	}
	for _, chat := range discarded {
		peer := chat.PeerOf(ownerUserID)
		if peer == 0 {
			continue
		}
		// 对端绑定设备已知则 device-level 定向，建链前（未绑定，0）则账号级。
		r.pushUpdateEncryption(ctx, peer, chat.PeerAuthKeyOf(ownerUserID), chat, "secret chat discarded on peer logout/revoke")
	}
}

func (r *Router) onMessagesRequestEncryption(ctx context.Context, req *tg.MessagesRequestEncryptionRequest) (tg.EncryptedChatClass, error) {
	if req == nil {
		return nil, inputRequestInvalidErr()
	}
	if req.RandomID == 0 || int(int32(req.RandomID)) != req.RandomID {
		return nil, secretChatRandomIDDuplicateErr()
	}
	if r.deps.SecretChats == nil || r.deps.Users == nil {
		return nil, notImplementedErr()
	}
	adminID, err := r.secretChatRequireUser(ctx)
	if err != nil {
		return nil, err
	}
	adminAuthKeyID, ok := businessAuthKeyIDFrom(ctx)
	if !ok {
		return nil, internalErr()
	}
	participant, found, err := r.userFromInput(ctx, adminID, req.UserID)
	if err != nil {
		return nil, internalErr()
	}
	// 自聊 / bot / 不存在统一 USER_ID_INVALID（bot 不支持密聊）。
	if !found || participant.ID == 0 || participant.ID == adminID || participant.Bot {
		return nil, userIDInvalidErr()
	}
	if blocked, err := r.peerBlocksUser(ctx, adminID, participant.ID); err != nil {
		return nil, err
	} else if blocked {
		return nil, userIsBlockedErr()
	}
	chat, err := r.deps.SecretChats.RequestEncryption(ctx, domain.SecretChatRequest{
		AdminUserID:       adminID,
		AdminAuthKeyID:    adminAuthKeyID,
		ParticipantUserID: participant.ID,
		RandomID:          int32(req.RandomID),
		GA:                req.GA,
		Date:              int(r.clock.Now().Unix()),
	})
	if err != nil {
		return nil, secretChatErr(err)
	}
	// 建链前邀请的账号级 state event 已与 secret_chats 写入同边界提交。
	// 推接受方全部在线设备 encryptedChatRequested（携 g_a）。离线设备经 getDifference 补回。
	r.pushUpdateEncryption(ctx, chat.ParticipantUserID, 0, chat, "secret chat requested")
	// 发起方同步收 encryptedChatWaiting（无 g_a）。
	return tgEncryptedChatForViewer(chat, adminID), nil
}

func (r *Router) onMessagesAcceptEncryption(ctx context.Context, req *tg.MessagesAcceptEncryptionRequest) (tg.EncryptedChatClass, error) {
	if req == nil {
		return nil, inputRequestInvalidErr()
	}
	if r.deps.SecretChats == nil {
		return nil, notImplementedErr()
	}
	userID, err := r.secretChatRequireUser(ctx)
	if err != nil {
		return nil, err
	}
	participantAuthKeyID, ok := businessAuthKeyIDFrom(ctx)
	if !ok {
		return nil, internalErr()
	}
	now := int(r.clock.Now().Unix())
	chat, err := r.deps.SecretChats.AcceptEncryption(ctx, req.Peer.ChatID, userID,
		participantAuthKeyID, req.Peer.AccessHash, req.GB, req.KeyFingerprint, now)
	if err != nil {
		return nil, secretChatErr(err)
	}
	// 建链完成事件与 participant loser 收敛事件已与 accept CAS 同边界提交。
	// 仅推发起方绑定设备 encryptedChat（GAOrB=g_b, key_fingerprint）。
	r.pushUpdateEncryption(ctx, chat.AdminUserID, chat.AdminAuthKeyID, chat, "secret chat accepted")
	r.pushAcceptedLoserDiscarded(ctx, chat, now)
	// 接受方同步收 encryptedChat（GAOrB=g_a）。
	return tgEncryptedChatForViewer(chat, userID), nil
}

func (r *Router) onMessagesDiscardEncryption(ctx context.Context, req *tg.MessagesDiscardEncryptionRequest) (bool, error) {
	if req == nil {
		return false, inputRequestInvalidErr()
	}
	if r.deps.SecretChats == nil {
		return false, notImplementedErr()
	}
	userID, err := r.secretChatRequireUser(ctx)
	if err != nil {
		return false, err
	}
	deviceAuthKeyID, ok := businessAuthKeyIDFrom(ctx)
	if !ok {
		return false, internalErr()
	}
	now := int(r.clock.Now().Unix())
	chat, already, err := r.deps.SecretChats.DiscardEncryption(ctx, req.ChatID, userID, deviceAuthKeyID, req.DeleteHistory, now)
	if err != nil {
		return false, secretChatErr(err)
	}
	if !already {
		// 推对端 encryptedChatDiscarded（history_deleted 决定对端是否删整个会话）。
		// durable state event 已与 discard mutation 同边界提交。
		if peer := chat.PeerOf(userID); peer != 0 {
			r.pushUpdateEncryption(ctx, peer, chat.PeerAuthKeyOf(userID), chat, "secret chat discarded")
		}
	}
	return true, nil
}
