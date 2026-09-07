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

// PinPrivateMessage 翻转私聊消息的置顶状态。官方语义：
//   - pin 且非 pm_oneside：双方 box 行同步置位（对端 box 经
//     private_message_id 翻译），双方各收一条带账号 pts 的
//     updatePinnedMessages；
//   - pin 且 pm_oneside / Saved Messages：仅本侧；
//   - unpin：无 oneside 形态，双侧清除（对端行未置顶时自然跳过）；
//   - 状态已是目标值：幂等 no-op，不烧 pts、不发事件。
//
// 共享 pin 的服务消息、pin 状态、PTS 与 outbox 在同一事务提交。
func (s *MessageStore) PinPrivateMessage(ctx context.Context, req domain.PinPrivateMessageRequest) (res domain.PinPrivateMessageResult, err error) {
	res = domain.PinPrivateMessageResult{OwnerUserID: req.OwnerUserID}
	if req.OwnerUserID == 0 || req.Peer.Type != domain.PeerTypeUser || req.Peer.ID == 0 {
		return res, domain.ErrMessageIDInvalid
	}
	if req.MessageID <= 0 || req.MessageID > domain.MaxMessageBoxID {
		return res, domain.ErrMessageIDInvalid
	}
	if req.Date == 0 {
		req.Date = int(time.Now().Unix())
	}
	if req.Pinned && !req.PmOneside && req.Peer.ID != req.OwnerUserID {
		sendReq, err := store.PrivatePinServiceRequest(req)
		if err != nil {
			return res, err
		}
		sent, err := s.sendPrivateTextWithHooks(ctx, sendReq, privateSendTxHooks{
			before: func(ctx context.Context, tx pgx.Tx, _ *domain.SendPrivateTextRequest) error {
				var err error
				// The send path already holds the ordered user locks and append fences.
				res, err = s.pinPrivateMessageTx(ctx, tx, req)
				if err != nil {
					return err
				}
				if !res.Changed() {
					return errPrivatePinNoop
				}
				return nil
			},
		})
		if errors.Is(err, errPrivatePinNoop) {
			return domain.PinPrivateMessageResult{OwnerUserID: req.OwnerUserID}, nil
		}
		if errors.Is(err, domain.ErrReplyMessageIDInvalid) {
			err = domain.ErrMessageIDInvalid
		}
		if err != nil {
			return domain.PinPrivateMessageResult{OwnerUserID: req.OwnerUserID}, err
		}
		res.ServiceMessage = sent
		return res, nil
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return res, fmt.Errorf("pin private message: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return res, fmt.Errorf("begin pin message tx: %w", err)
	}
	committed := false
	defer func() {
		if committed {
			return
		}
		_ = tx.Rollback(ctx)
	}()

	if err := lockUsersForUpdate(ctx, tx, req.OwnerUserID, req.Peer.ID); err != nil {
		return res, fmt.Errorf("lock pin users: %w", err)
	}
	if err := lockDispatchOutboxAppendFences(ctx, tx, []int64{req.OwnerUserID, req.Peer.ID}); err != nil {
		return res, fmt.Errorf("lock pin dispatch append fences: %w", err)
	}
	res, err = s.pinPrivateMessageTx(ctx, tx, req)
	if err != nil {
		return domain.PinPrivateMessageResult{OwnerUserID: req.OwnerUserID}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return domain.PinPrivateMessageResult{OwnerUserID: req.OwnerUserID}, fmt.Errorf("commit pin message tx: %w", err)
	}
	committed = true
	return res, nil
}

var errPrivatePinNoop = errors.New("private pin no-op")

func (s *MessageStore) pinPrivateMessageTx(ctx context.Context, tx pgx.Tx, req domain.PinPrivateMessageRequest) (domain.PinPrivateMessageResult, error) {
	res := domain.PinPrivateMessageResult{OwnerUserID: req.OwnerUserID}
	qtx := sqlcgen.New(tx)
	owned, err := qtx.GetMessageBoxForPin(ctx, sqlcgen.GetMessageBoxForPinParams{
		OwnerUserID: req.OwnerUserID,
		PeerType:    string(req.Peer.Type),
		PeerID:      req.Peer.ID,
		BoxID:       int32(req.MessageID),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return res, domain.ErrMessageIDInvalid
		}
		return res, fmt.Errorf("get message for pin: %w", err)
	}
	media, mediaErr := decodeMessageMedia(owned.MediaJson)
	if mediaErr != nil {
		return res, fmt.Errorf("decode pin target media: %w", mediaErr)
	}
	if media != nil && media.Kind == domain.MessageMediaKindService {
		return res, domain.ErrMessageIDInvalid
	}

	type pinSide struct {
		userID int64
		peer   domain.Peer
		boxID  int
	}
	sides := []pinSide{{userID: req.OwnerUserID, peer: req.Peer, boxID: int(owned.BoxID)}}
	// 对端侧翻转：pin 仅在非 oneside 时传播；unpin 恒尝试清除（oneside
	// pin 的对端行本就未置顶，SetMessageBoxPinned 0 行自然跳过）。
	propagatePeer := req.Peer.ID != req.OwnerUserID && (!req.Pinned || !req.PmOneside)
	if propagatePeer {
		peerRow, peerErr := qtx.GetMessageBoxByPrivateMessage(ctx, sqlcgen.GetMessageBoxByPrivateMessageParams{
			OwnerUserID:      req.Peer.ID,
			PrivateMessageID: owned.PrivateMessageID,
		})
		if peerErr == nil {
			sides = append(sides, pinSide{
				userID: req.Peer.ID,
				peer:   domain.Peer{Type: domain.PeerTypeUser, ID: req.OwnerUserID},
				boxID:  int(peerRow.BoxID),
			})
		} else if !errors.Is(peerErr, pgx.ErrNoRows) {
			return res, fmt.Errorf("get peer message for pin: %w", peerErr)
		}
	}

	for _, side := range sides {
		affected, err := qtx.SetMessageBoxPinned(ctx, sqlcgen.SetMessageBoxPinnedParams{
			Pinned:      req.Pinned,
			OwnerUserID: side.userID,
			BoxID:       int32(side.boxID),
		})
		if err != nil {
			return res, fmt.Errorf("set message pinned: %w", err)
		}
		if affected == 0 {
			continue
		}
		pts, err := s.reservePts(ctx, tx, side.userID)
		if err != nil {
			return res, fmt.Errorf("allocate pin pts: %w", err)
		}
		event := domain.UpdateEvent{
			UserID:     side.userID,
			Type:       domain.UpdateEventPinnedMessages,
			Pts:        pts,
			PtsCount:   1,
			Date:       req.Date,
			Peer:       side.peer,
			Bool:       req.Pinned,
			MessageIDs: []int{side.boxID},
		}
		if err := appendUserUpdateEvent(ctx, tx, qtx, side.userID, event); err != nil {
			return res, fmt.Errorf("append pinned messages event: %w", err)
		}
		dispatchAuthKeyID := [8]byte{}
		dispatchSessionID := int64(0)
		if side.userID == req.OwnerUserID {
			dispatchAuthKeyID = req.OriginAuthKeyID
			dispatchSessionID = req.OriginSessionID
		}
		if err := enqueueDispatch(ctx, qtx, dispatchEnqueue{
			TargetUserID:     side.userID,
			Pts:              int32(pts),
			EventType:        string(domain.UpdateEventPinnedMessages),
			ExcludeAuthKeyID: dispatchAuthKeyID,
			ExcludeSessionID: dispatchSessionID,
		}); err != nil {
			return res, fmt.Errorf("enqueue pinned messages dispatch: %w", err)
		}
		res.Updated = append(res.Updated, domain.PinnedMessagesForUser{
			UserID:     side.userID,
			Peer:       side.peer,
			MessageIDs: []int{side.boxID},
			Pinned:     req.Pinned,
			Event:      event,
		})
	}
	return res, nil
}

// UnpinAllPrivateMessages 清空与某私聊 peer 的全部置顶。本侧整批清除，
// 共享置顶经 private_message_id 同步清除对端行；双方各收一条带账号 pts
// 的 updatePinnedMessages{pinned:false}，messages 为各自视角 box id。
func (s *MessageStore) UnpinAllPrivateMessages(ctx context.Context, req domain.UnpinAllPrivateMessagesRequest) (res domain.PinPrivateMessageResult, err error) {
	res = domain.PinPrivateMessageResult{OwnerUserID: req.OwnerUserID}
	if req.OwnerUserID == 0 || req.Peer.Type != domain.PeerTypeUser || req.Peer.ID == 0 {
		return res, domain.ErrMessageIDInvalid
	}
	if req.Date == 0 {
		req.Date = int(time.Now().Unix())
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return res, fmt.Errorf("unpin all private messages: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return res, fmt.Errorf("begin unpin all tx: %w", err)
	}
	qtx := sqlcgen.New(tx)
	committed := false
	defer func() {
		if committed {
			return
		}
		_ = tx.Rollback(ctx)
	}()

	if err := lockUsersForUpdate(ctx, tx, req.OwnerUserID, req.Peer.ID); err != nil {
		return res, fmt.Errorf("lock unpin users: %w", err)
	}
	if err := lockDispatchOutboxAppendFences(ctx, tx, []int64{req.OwnerUserID, req.Peer.ID}); err != nil {
		return res, fmt.Errorf("lock unpin dispatch append fences: %w", err)
	}
	ownRows, err := qtx.UnpinAllMessageBoxesByPeer(ctx, sqlcgen.UnpinAllMessageBoxesByPeerParams{
		OwnerUserID: req.OwnerUserID,
		PeerType:    string(req.Peer.Type),
		PeerID:      req.Peer.ID,
		LimitCount:  domain.MaxUnpinAllBatch,
	})
	if err != nil {
		return res, fmt.Errorf("unpin own messages: %w", err)
	}
	if len(ownRows) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return res, fmt.Errorf("commit unpin all tx: %w", err)
		}
		committed = true
		return res, nil
	}
	if len(ownRows) == domain.MaxUnpinAllBatch {
		more, moreErr := qtx.HasPinnedMessageBoxByPeer(ctx, sqlcgen.HasPinnedMessageBoxByPeerParams{
			OwnerUserID: req.OwnerUserID,
			PeerType:    string(req.Peer.Type),
			PeerID:      req.Peer.ID,
		})
		if moreErr != nil {
			return res, fmt.Errorf("check remaining pinned after unpin all: %w", moreErr)
		}
		if more {
			res.Offset = 1
		}
	}

	type unpinSide struct {
		userID int64
		peer   domain.Peer
		ids    []int
	}
	ownIDs := make([]int, 0, len(ownRows))
	senderIDs := make([]int64, 0, len(ownRows))
	pmIDs := make([]int64, 0, len(ownRows))
	for _, row := range ownRows {
		ownIDs = append(ownIDs, int(row.BoxID))
		senderIDs = append(senderIDs, row.MessageSenderID)
		pmIDs = append(pmIDs, row.PrivateMessageID)
	}
	sides := []unpinSide{{userID: req.OwnerUserID, peer: req.Peer, ids: ownIDs}}
	if req.Peer.ID != req.OwnerUserID {
		peerBoxIDs, err := qtx.UnpinMessageBoxesByPrivateMessages(ctx, sqlcgen.UnpinMessageBoxesByPrivateMessagesParams{
			MessageSenderIds:  senderIDs,
			PrivateMessageIds: pmIDs,
			OwnerUserID:       req.Peer.ID,
		})
		if err != nil {
			return res, fmt.Errorf("unpin peer messages: %w", err)
		}
		if len(peerBoxIDs) > 0 {
			peerIDs := make([]int, 0, len(peerBoxIDs))
			for _, id := range peerBoxIDs {
				peerIDs = append(peerIDs, int(id))
			}
			sides = append(sides, unpinSide{
				userID: req.Peer.ID,
				peer:   domain.Peer{Type: domain.PeerTypeUser, ID: req.OwnerUserID},
				ids:    peerIDs,
			})
		}
	}

	for _, side := range sides {
		pts, err := s.reservePts(ctx, tx, side.userID)
		if err != nil {
			return res, fmt.Errorf("allocate unpin all pts: %w", err)
		}
		event := domain.UpdateEvent{
			UserID:     side.userID,
			Type:       domain.UpdateEventPinnedMessages,
			Pts:        pts,
			PtsCount:   1,
			Date:       req.Date,
			Peer:       side.peer,
			Bool:       false,
			MessageIDs: side.ids,
		}
		if err := appendUserUpdateEvent(ctx, tx, qtx, side.userID, event); err != nil {
			return res, fmt.Errorf("append unpin all event: %w", err)
		}
		dispatchAuthKeyID := [8]byte{}
		dispatchSessionID := int64(0)
		if side.userID == req.OwnerUserID {
			dispatchAuthKeyID = req.OriginAuthKeyID
			dispatchSessionID = req.OriginSessionID
		}
		if err := enqueueDispatch(ctx, qtx, dispatchEnqueue{
			TargetUserID:     side.userID,
			Pts:              int32(pts),
			EventType:        string(domain.UpdateEventPinnedMessages),
			ExcludeAuthKeyID: dispatchAuthKeyID,
			ExcludeSessionID: dispatchSessionID,
		}); err != nil {
			return res, fmt.Errorf("enqueue unpin all dispatch: %w", err)
		}
		res.Updated = append(res.Updated, domain.PinnedMessagesForUser{
			UserID:     side.userID,
			Peer:       side.peer,
			MessageIDs: side.ids,
			Pinned:     false,
			Event:      event,
		})
	}
	if err := tx.Commit(ctx); err != nil {
		return res, fmt.Errorf("commit unpin all tx: %w", err)
	}
	committed = true
	return res, nil
}
