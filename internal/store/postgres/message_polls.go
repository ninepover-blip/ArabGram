package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	"telesrv/internal/store"
	"telesrv/internal/store/postgres/sqlcgen"
)

// 私聊消息 poll 投票/关闭：消息可见性在 message_boxes 校验（与 reaction 同款 SELECT FOR
// UPDATE 定位），poll 级语义委托 poll.go 的共享 SQL（polls 行 FOR UPDATE 防 quiz 并发双投）。

func (s *MessageStore) VoteMessagePoll(ctx context.Context, req domain.VotePrivateMessagePollRequest, effects store.DeliveryEffectsBuilder[domain.PrivateMessagePollResult]) (domain.PrivateMessagePollResult, error) {
	return s.mutateMessagePoll(ctx, req.UserID, req.Peer, req.MessageID, req.Date, effects, func(ctx context.Context, tx pgx.Tx, def domain.PollDefinition, date int) error {
		return applyPollVote(ctx, tx, def, req.UserID, req.Options, date)
	})
}

func (s *MessageStore) CloseMessagePoll(ctx context.Context, req domain.ClosePrivateMessagePollRequest, effects store.DeliveryEffectsBuilder[domain.PrivateMessagePollResult]) (domain.PrivateMessagePollResult, error) {
	return s.mutateMessagePoll(ctx, req.UserID, req.Peer, req.MessageID, req.Date, effects, func(ctx context.Context, tx pgx.Tx, def domain.PollDefinition, _ int) error {
		return closePollAsCreator(ctx, tx, def, req.UserID)
	})
}

func (s *MessageStore) mutateMessagePoll(
	ctx context.Context,
	userID int64,
	peer domain.Peer,
	messageID int,
	date int,
	effects store.DeliveryEffectsBuilder[domain.PrivateMessagePollResult],
	mutate func(ctx context.Context, tx pgx.Tx, def domain.PollDefinition, date int) error,
) (domain.PrivateMessagePollResult, error) {
	if userID == 0 || peer.Type != domain.PeerTypeUser || peer.ID == 0 || messageID <= 0 || messageID > domain.MaxMessageBoxID {
		return domain.PrivateMessagePollResult{}, domain.ErrMessageIDInvalid
	}
	if effects == nil {
		return domain.PrivateMessagePollResult{}, store.ErrDeliveryOutboxRequired
	}
	if date == 0 {
		date = int(time.Now().Unix())
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return domain.PrivateMessagePollResult{}, fmt.Errorf("mutate message poll: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return domain.PrivateMessagePollResult{}, fmt.Errorf("begin message poll tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()

	var target struct {
		privateMessageID int64
		messageSenderID  int64
		mediaJSON        string
	}
	if err := tx.QueryRow(ctx, `
SELECT private_message_id, message_sender_id, COALESCE(media::text, '{}')
FROM message_boxes
WHERE owner_user_id = $1
  AND box_id = $2
  AND peer_type = $3
  AND peer_id = $4
  AND NOT deleted
LIMIT 1`, userID, int32(messageID), string(peer.Type), peer.ID).Scan(&target.privateMessageID, &target.messageSenderID, &target.mediaJSON); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.PrivateMessagePollResult{}, domain.ErrMessageIDInvalid
		}
		return domain.PrivateMessagePollResult{}, fmt.Errorf("get message for poll: %w", err)
	}
	media, err := decodeMessageMedia(target.mediaJSON)
	if err != nil {
		return domain.PrivateMessagePollResult{}, fmt.Errorf("decode poll message media: %w", err)
	}
	if media == nil || media.Kind != domain.MessageMediaKindPoll || media.Poll == nil || media.Poll.ID == 0 {
		return domain.PrivateMessagePollResult{}, domain.ErrMessageIDInvalid
	}
	pollID := media.Poll.ID
	defs, err := loadPollDefinitions(ctx, tx, []int64{pollID}, true)
	if err != nil {
		return domain.PrivateMessagePollResult{}, err
	}
	def, ok := defs[pollID]
	if !ok {
		return domain.PrivateMessagePollResult{}, domain.ErrPollNotFound
	}
	if err := mutate(ctx, tx, def, date); err != nil {
		return domain.PrivateMessagePollResult{}, err
	}

	boxes, err := sqlcgen.New(tx).ListVisibleMessageBoxesByPrivateMessage(ctx, sqlcgen.ListVisibleMessageBoxesByPrivateMessageParams{
		OwnerUserIds:     privateMessageOwnerIDs(userID, peer.ID),
		MessageSenderID:  target.messageSenderID,
		PrivateMessageID: target.privateMessageID,
	})
	if err != nil {
		return domain.PrivateMessagePollResult{}, fmt.Errorf("list visible poll boxes: %w", err)
	}
	res := domain.PrivateMessagePollResult{PollID: pollID, Messages: make([]domain.Message, 0, len(boxes))}
	for _, box := range boxes {
		msg, err := messageFromVisibleBoxRow(box)
		if err != nil {
			return domain.PrivateMessagePollResult{}, err
		}
		res.Messages = append(res.Messages, msg)
	}
	// poll enrichment（按各 box owner 视角）由 enrichPrivateMessageReactions 统一挂载。
	if err := s.enrichPrivateMessageReactions(ctx, tx, userID, res.Messages); err != nil {
		return domain.PrivateMessagePollResult{}, err
	}
	intents, err := effects(res)
	if err != nil {
		return domain.PrivateMessagePollResult{}, fmt.Errorf("build message poll delivery effects: %w", err)
	}
	if len(intents) == 0 && len(res.Messages) > 0 {
		return domain.PrivateMessagePollResult{}, store.ErrDeliveryOutboxRequired
	}
	if _, err := applyDeliveryEffectsTx(ctx, tx, intents); err != nil {
		return domain.PrivateMessagePollResult{}, fmt.Errorf("apply message poll delivery effects: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.PrivateMessagePollResult{}, fmt.Errorf("commit message poll tx: %w", err)
	}
	committed = true
	return res, nil
}

// enrichPrivateMessagePolls 把页内全部 poll media 按各消息 owner 视角 enrich；
// 由 enrichPrivateMessageReactions 统一挂载（所有私聊读路径共用一个 choke point）。
func (s *MessageStore) enrichPrivateMessagePolls(ctx context.Context, db sqlcgen.DBTX, viewerUserID int64, messages []domain.Message) error {
	refs := make([]pollMediaRef, 0, 2)
	for i := range messages {
		viewer := messages[i].OwnerUserID
		if viewer == 0 {
			viewer = viewerUserID
		}
		refs = append(refs, pollMediaRef{media: messages[i].Media, viewer: viewer})
	}
	return enrichPollMediaRefs(ctx, db, refs)
}
