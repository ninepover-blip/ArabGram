package rpc

import (
	"context"

	"github.com/iamxvbaba/td/tg"
	"go.uber.org/zap"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

// 本文件实现 app/bots 的 rpc 回调：token revoke 后的 session 失效闭环，
// bot commands / sticker-set 的 durable non-PTS effects，以及 @ChatBot
// 流式草稿 transient 推送。Router 创建后经
// botsService.SetRouterHooks / SetTextDraftPusher 装配（见 internal/node/core/runtime.go）。

// RevokeBotSessions 撤销 bot 的全部已登录 session：删除全部 authorization 行并
// 强制断开在线连接（与 account.resetAuthorization 被踢闭环同款顺序）。
func (r *Router) RevokeBotSessions(ctx context.Context, botUserID int64) error {
	if r.deps.Auth == nil || botUserID == 0 {
		return nil
	}
	deleted, err := r.deps.Auth.ResetAuthorizations(ctx, botUserID, [8]byte{})
	if err != nil {
		return err
	}
	for _, a := range deleted {
		r.revokeAuthKeySessions(a.AuthKeyID)
		if err := r.clearAuthKeyState(ctx, a.AuthKeyID); err != nil {
			r.log.Warn("revoke bot sessions: clear auth key state",
				zap.Int64("bot_user_id", botUserID), zap.Error(err))
		}
	}
	if len(deleted) > 0 {
		r.log.Info("revoked bot sessions", zap.Int64("bot_user_id", botUserID), zap.Int("count", len(deleted)))
	}
	return nil
}

// BotLifecycleDeliveryEffects projects the created/deleted bot user as an
// absolute updateUser for the frozen owner. The store validates that the
// builder emits exactly that one owner-targeted effect.
func (r *Router) BotLifecycleDeliveryEffects(_ context.Context) store.DeliveryEffectsBuilder[store.BotLifecycleDeliverySnapshot] {
	return func(snapshot store.BotLifecycleDeliverySnapshot) ([]store.DeliveryEffect, error) {
		payload, err := encodeDeliveryUpdate(&tg.Updates{
			Updates: []tg.UpdateClass{&tg.UpdateUser{UserID: snapshot.Bot.ID}},
			// The builder runs while the BotStore aggregate lock/transaction is
			// open. Use the pure domain->TL projector: consulting BotInfo here
			// would self-deadlock in memory and miss the uncommitted PG row.
			Users: []tg.UserClass{tgUser(snapshot.Bot)},
			Date:  int(r.clock.Now().Unix()),
		})
		if err != nil {
			return nil, err
		}
		return []store.DeliveryEffect{store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
			TargetUserID: snapshot.OwnerUserID, Payload: payload, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
		})}, nil
	}
}

// BotInfoDeliveryEffects is executed inside the bot-info aggregate after its
// owner/private-dialog viewer audience has been frozen.
func (r *Router) BotInfoDeliveryEffects(_ context.Context) store.DeliveryEffectsBuilder[store.UserAudienceDeliverySnapshot] {
	return r.UserAudienceDeliveryEffects
}

// BotCommandsDeliveryEffects projects one immutable updateBotCommands payload
// per viewer frozen by BotStore. BotFather/owner sessions are not necessarily
// viewers, so this absolute notification deliberately carries no session
// exclusion.
func (r *Router) BotCommandsDeliveryEffects(_ context.Context) store.DeliveryEffectsBuilder[store.BotCommandsDeliverySnapshot] {
	return func(snapshot store.BotCommandsDeliverySnapshot) ([]store.DeliveryEffect, error) {
		if len(snapshot.Audience) == 0 {
			return nil, nil
		}
		payload, err := encodeDeliveryUpdate(&tg.Updates{
			Updates: []tg.UpdateClass{&tg.UpdateBotCommands{
				Peer:     &tg.PeerUser{UserID: snapshot.BotUserID},
				BotID:    snapshot.BotUserID,
				Commands: tgBotCommands(snapshot.Commands),
			}},
			Date: int(r.clock.Now().Unix()),
		})
		if err != nil {
			return nil, err
		}
		effects := make([]store.DeliveryEffect, len(snapshot.Audience))
		for i, viewerUserID := range snapshot.Audience {
			// All effects share one immutable byte snapshot until the store's
			// set-based INSERT encodes its parameter arrays; avoid cloning the
			// same commands payload once per viewer in the transaction hot path.
			effects[i] = store.DeliveryEffect{
				Kind: store.DeliveryEffectAbsolute, TargetUserID: viewerUserID,
				Payload: payload, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
			}
		}
		return effects, nil
	}
}

// StickerSetDeliveryEffects is the TL projection boundary used by the built-in
// @Stickers service. The returned builder is executed by the media store while
// its mutation transaction is open; it never starts an asynchronous push.
func (r *Router) StickerSetDeliveryEffects(ctx context.Context, userID int64) store.DeliveryEffectsBuilder[store.StickerSetMutation] {
	if userID <= 0 || r.deps.DeliveryOutbox == nil {
		return nil
	}
	return r.stickerSetMutationDeliveryEffects(ctx, userID)
}

// InvalidateStickerSetCatalog runs synchronously after the media transaction
// commits. Keeping cache invalidation out of the effect builder prevents a
// concurrent read from repopulating the old catalog before commit.
func (r *Router) InvalidateStickerSetCatalog(kind domain.StickerSetKind) {
	r.invalidateStickerCatalog(kind)
}

// PushBotTextDraft 推送内置 service bot 的流式文本草稿。草稿是 TDesktop 专用的
// transient typing action，不写 message/dialog/pts/outbox；最终可恢复事实仍由随后
// 入库的普通 bot message 承担。
func (r *Router) PushBotTextDraft(ctx context.Context, botUserID, userID, randomID int64, text string) {
	if botUserID == 0 || userID == 0 || randomID == 0 || text == "" {
		return
	}
	r.pushUserMessageTransient(context.WithoutCancel(ctx), userID, "push bot text draft", &tg.UpdateShort{
		Update: &tg.UpdateUserTyping{
			UserID: botUserID,
			Action: &tg.SendMessageTextDraftAction{
				RandomID: randomID,
				Text:     tg.TextWithEntities{Text: text},
			},
		},
		Date: int(r.clock.Now().Unix()),
	})
}
