package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	"telesrv/internal/store/postgres/sqlcgen"
)

type starGiftViewerMessageRef struct {
	UserID int64
	MsgID  int
}

// registerUserStarGiftMessageRef records an owner-scoped service-message alias
// for a user-owned gift. Official clients may continue from a freshly emitted
// messageActionStarGiftUnique or a separate prepaid-upgrade notification and
// pass that message id to a lifecycle RPC, while payments.getSavedStarGifts may
// still expose the original received gift message as the aggregate's primary
// msg_id. expectedUniqueGiftID is zero for an ordinary gift and positive for a
// unique gift; the write boundary never aliases across lifecycle states.
func registerUserStarGiftMessageRef(
	ctx context.Context,
	tx pgx.Tx,
	ownerUserID int64,
	msgID int,
	savedGiftID int64,
	uniqueGiftID int64,
) error {
	return registerViewerStarGiftMessageRef(ctx, tx, ownerUserID, msgID, savedGiftID,
		domain.Peer{Type: domain.PeerTypeUser, ID: ownerUserID}, uniqueGiftID)
}

// registerViewerStarGiftMessageRef binds one viewer-local private message to
// the aggregate owner explicitly named by the action. The alias does not grant
// ownership: RPC callers resolve the real owner and authorize it again.
func registerViewerStarGiftMessageRef(
	ctx context.Context,
	tx pgx.Tx,
	viewerUserID int64,
	msgID int,
	savedGiftID int64,
	expectedOwner domain.Peer,
	uniqueGiftID int64,
) error {
	if viewerUserID <= 0 || msgID <= 0 || savedGiftID <= 0 || !validLifecyclePeer(expectedOwner) || uniqueGiftID < 0 {
		return fmt.Errorf("register star gift message ref: invalid identity")
	}
	tag, err := tx.Exec(ctx, `
INSERT INTO star_gift_user_message_refs(owner_user_id,msg_id,saved_gift_id)
SELECT $1,$2,p.id
FROM peer_star_gifts p
WHERE p.id=$3 AND p.owner_peer_type=$4 AND p.owner_peer_id=$5
  AND (($6::bigint=0 AND p.unique_gift_id IS NULL) OR ($6::bigint>0 AND p.unique_gift_id=$6::bigint))
  AND p.lifecycle_status='active'
ON CONFLICT(owner_user_id,msg_id) DO UPDATE
SET saved_gift_id=EXCLUDED.saved_gift_id
WHERE star_gift_user_message_refs.saved_gift_id=EXCLUDED.saved_gift_id`,
		viewerUserID, msgID, savedGiftID, string(expectedOwner.Type), expectedOwner.ID, uniqueGiftID)
	if err != nil {
		return fmt.Errorf("register star gift message ref: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("register star gift message ref: identity collision")
	}
	return nil
}

func registerChannelNotificationMessageRef(
	ctx context.Context,
	tx pgx.Tx,
	viewerUserID int64,
	msgID int,
	savedGiftID int64,
) error {
	if viewerUserID <= 0 || msgID <= 0 || savedGiftID <= 0 {
		return fmt.Errorf("register channel notification star gift message ref: invalid identity")
	}
	tag, err := tx.Exec(ctx, `
INSERT INTO star_gift_user_message_refs(owner_user_id,msg_id,saved_gift_id)
SELECT $1,$2,gift.id
FROM star_gift_channel_notification_jobs job
JOIN peer_star_gifts gift ON gift.id=job.saved_gift_id
WHERE job.saved_gift_id=$3 AND job.target_user_id=$1
  AND gift.owner_peer_type='channel' AND gift.lifecycle_status='active'
ON CONFLICT(owner_user_id,msg_id) DO UPDATE
SET saved_gift_id=EXCLUDED.saved_gift_id
WHERE star_gift_user_message_refs.saved_gift_id=EXCLUDED.saved_gift_id`,
		viewerUserID, msgID, savedGiftID)
	if err != nil {
		return fmt.Errorf("register channel notification star gift message ref: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("register channel notification star gift message ref: identity collision")
	}
	return nil
}

func userStarGiftMessageRefMatches(
	ctx context.Context,
	db interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	},
	ownerUserID int64,
	msgID int,
	savedGiftID int64,
) (bool, error) {
	var matches bool
	err := db.QueryRow(ctx, `SELECT EXISTS (
SELECT 1 FROM star_gift_user_message_refs
WHERE owner_user_id=$1 AND msg_id=$2 AND saved_gift_id=$3
)`, ownerUserID, msgID, savedGiftID).Scan(&matches)
	return matches, err
}

func listChannelStarGiftViewerMessageRefs(ctx context.Context, db interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, savedGiftID, channelID int64) ([]starGiftViewerMessageRef, error) {
	if savedGiftID <= 0 || channelID <= 0 {
		return nil, domain.ErrStarGiftCollectibleInvalid
	}
	rows, err := db.Query(ctx, `
SELECT ref.owner_user_id,ref.msg_id
FROM star_gift_user_message_refs ref
JOIN peer_star_gifts gift ON gift.id=ref.saved_gift_id
JOIN message_boxes box ON box.owner_user_id=ref.owner_user_id AND box.box_id=ref.msg_id
WHERE ref.saved_gift_id=$1
  AND gift.owner_peer_type='channel' AND gift.owner_peer_id=$2
  AND gift.lifecycle_status='active' AND NOT box.deleted
  AND box.media #>> '{service_action,kind}'='star_gift'
ORDER BY ref.owner_user_id,ref.msg_id`, savedGiftID, channelID)
	if err != nil {
		return nil, fmt.Errorf("list channel star gift viewer refs: %w", err)
	}
	defer rows.Close()
	refs := make([]starGiftViewerMessageRef, 0, maxChannelStarGiftNotificationRecipients)
	for rows.Next() {
		var ref starGiftViewerMessageRef
		if err := rows.Scan(&ref.UserID, &ref.MsgID); err != nil {
			return nil, fmt.Errorf("scan channel star gift viewer ref: %w", err)
		}
		if ref.UserID <= 0 || ref.MsgID <= 0 {
			return nil, fmt.Errorf("channel star gift viewer ref has invalid identity")
		}
		refs = append(refs, ref)
		if len(refs) > maxChannelStarGiftNotificationRecipients*2 {
			return nil, fmt.Errorf("channel star gift viewer refs exceed bound")
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate channel star gift viewer refs: %w", err)
	}
	return refs, nil
}

func listChannelStarGiftOwnershipMessageRefs(ctx context.Context, db interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, savedGiftID, channelID int64) ([]starGiftViewerMessageRef, error) {
	if savedGiftID <= 0 || channelID <= 0 {
		return nil, domain.ErrStarGiftCollectibleInvalid
	}
	rows, err := db.Query(ctx, `
SELECT ref.owner_user_id,ref.msg_id
FROM star_gift_user_message_refs ref
JOIN peer_star_gifts gift ON gift.id=ref.saved_gift_id
JOIN message_boxes box ON box.owner_user_id=ref.owner_user_id AND box.box_id=ref.msg_id
WHERE ref.saved_gift_id=$1
  AND gift.owner_peer_type='channel' AND gift.owner_peer_id=$2
  AND gift.lifecycle_status='active' AND NOT box.deleted
  AND (
      (
          box.media #>> '{service_action,kind}'='star_gift'
          AND box.media #>> '{service_action,star_gift,peer_channel_id}'=$2::text
          AND box.media #>> '{service_action,star_gift,saved_id}'=gift.saved_id::text
      )
      OR (
          box.media #>> '{service_action,kind}'='star_gift_unique'
          AND box.media #>> '{service_action,star_gift_unique,peer,Type}'='channel'
          AND box.media #>> '{service_action,star_gift_unique,peer,ID}'=$2::text
          AND box.media #>> '{service_action,star_gift_unique,saved_id}'=gift.saved_id::text
      )
  )
ORDER BY ref.owner_user_id,ref.msg_id`, savedGiftID, channelID)
	if err != nil {
		return nil, fmt.Errorf("list channel star gift ownership refs: %w", err)
	}
	defer rows.Close()
	refs := make([]starGiftViewerMessageRef, 0, maxChannelStarGiftNotificationRecipients)
	for rows.Next() {
		var ref starGiftViewerMessageRef
		if err := rows.Scan(&ref.UserID, &ref.MsgID); err != nil {
			return nil, fmt.Errorf("scan channel star gift ownership ref: %w", err)
		}
		if ref.UserID <= 0 || ref.MsgID <= 0 {
			return nil, fmt.Errorf("channel star gift ownership ref has invalid identity")
		}
		refs = append(refs, ref)
		if len(refs) > maxChannelStarGiftNotificationRecipients*2 {
			return nil, fmt.Errorf("channel star gift ownership refs exceed bound")
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate channel star gift ownership refs: %w", err)
	}
	return refs, nil
}

func listChannelStarGiftMutationUserIDs(ctx context.Context, db interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, savedGiftID, channelID int64) ([]int64, error) {
	if savedGiftID <= 0 || channelID <= 0 {
		return nil, domain.ErrStarGiftCollectibleInvalid
	}
	rows, err := db.Query(ctx, `
WITH gift AS (
    SELECT id
    FROM peer_star_gifts
    WHERE id=$1 AND owner_peer_type='channel' AND owner_peer_id=$2
), users AS (
    SELECT job.target_user_id AS user_id
    FROM star_gift_channel_notification_jobs job
    JOIN gift ON gift.id=job.saved_gift_id
    UNION
    SELECT ref.owner_user_id AS user_id
    FROM star_gift_user_message_refs ref
    JOIN gift ON gift.id=ref.saved_gift_id
)
SELECT user_id FROM users ORDER BY user_id`, savedGiftID, channelID)
	if err != nil {
		return nil, fmt.Errorf("list channel star gift mutation users: %w", err)
	}
	defer rows.Close()
	userIDs := make([]int64, 0, maxChannelStarGiftNotificationRecipients)
	for rows.Next() {
		var userID int64
		if err := rows.Scan(&userID); err != nil {
			return nil, fmt.Errorf("scan channel star gift mutation user: %w", err)
		}
		if userID <= 0 {
			return nil, fmt.Errorf("channel star gift mutation user has invalid identity")
		}
		userIDs = append(userIDs, userID)
		if len(userIDs) > maxChannelStarGiftNotificationRecipients*2 {
			return nil, fmt.Errorf("channel star gift mutation users exceed bound")
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate channel star gift mutation users: %w", err)
	}
	return userIDs, nil
}

func editChannelStarGiftViewerMessagesTx(
	ctx context.Context,
	tx pgx.Tx,
	messages *MessageStore,
	refs []starGiftViewerMessageRef,
	saved domain.SavedStarGift,
	date int,
	originUserID int64,
	originAuthKeyID [8]byte,
	originSessionID int64,
	mutate func(*domain.MessageStarGiftAction, int64) error,
) ([]domain.EditedMessageForUser, error) {
	if messages == nil || saved.ID <= 0 || saved.Owner.Type != domain.PeerTypeChannel || saved.Owner.ID <= 0 ||
		saved.SavedID <= 0 || date <= 0 || mutate == nil {
		return nil, domain.ErrStarGiftCollectibleInvalid
	}
	q := sqlcgen.New(tx)
	edits := make([]domain.EditedMessageForUser, 0, len(refs))
	for _, ref := range refs {
		var messageSenderID, privateMessageID int64
		err := tx.QueryRow(ctx, `
SELECT message_sender_id,private_message_id
FROM message_boxes
WHERE owner_user_id=$1 AND box_id=$2 AND peer_type='user' AND NOT deleted
FOR UPDATE`, ref.UserID, ref.MsgID).Scan(&messageSenderID, &privateMessageID)
		if err != nil {
			if err == pgx.ErrNoRows {
				continue
			}
			return nil, fmt.Errorf("lock channel star gift viewer ref: %w", err)
		}
		boxes, err := q.ListVisibleMessageBoxesByPrivateMessage(ctx, sqlcgen.ListVisibleMessageBoxesByPrivateMessageParams{
			OwnerUserIds: []int64{ref.UserID}, MessageSenderID: messageSenderID, PrivateMessageID: privateMessageID,
		})
		if err != nil {
			return nil, fmt.Errorf("load channel star gift viewer box: %w", err)
		}
		if len(boxes) != 1 || int(boxes[0].BoxID) != ref.MsgID {
			return nil, domain.ErrStarGiftCollectibleInvalid
		}
		box := boxes[0]
		media, err := decodeMessageMedia(box.MediaJson)
		if err != nil {
			return nil, fmt.Errorf("decode channel star gift viewer box: %w", err)
		}
		action := privateStarGiftAction(media)
		if action == nil || action.GiftID != saved.GiftID || action.PeerChannelID != saved.Owner.ID ||
			action.SavedID != saved.SavedID {
			return nil, fmt.Errorf("channel star gift viewer box has invalid projection")
		}
		if err := mutate(action, ref.UserID); err != nil {
			return nil, err
		}
		mediaJSON, err := encodeMessageMedia(media)
		if err != nil {
			return nil, fmt.Errorf("encode channel star gift viewer edit: %w", err)
		}
		pts, err := messages.reservePts(ctx, tx, ref.UserID)
		if err != nil {
			return nil, fmt.Errorf("allocate channel star gift viewer edit pts: %w", err)
		}
		tag, err := tx.Exec(ctx, `
UPDATE message_boxes SET media=$3,pts=$4
WHERE owner_user_id=$1 AND box_id=$2 AND NOT deleted`, ref.UserID, ref.MsgID, mediaJSON, int32(pts))
		if err != nil {
			return nil, fmt.Errorf("update channel star gift viewer box: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return nil, fmt.Errorf("update channel star gift viewer box lost row")
		}
		sharedMediaJSON, err := encodeSharedPrivateStarGiftMedia(media)
		if err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, `UPDATE private_messages SET media=$3
WHERE sender_user_id=$1 AND id=$2`, messageSenderID, privateMessageID, sharedMediaJSON); err != nil {
			return nil, fmt.Errorf("update channel star gift shared media: %w", err)
		}
		message, err := messageFromVisibleBoxRow(box)
		if err != nil {
			return nil, err
		}
		message.Media = media
		message.Pts = pts
		if err := replaceMessageBoxMediaIndexTx(ctx, tx, message.OwnerUserID, message.Peer.ID,
			message.ID, message.Date, message.Media, message.Entities); err != nil {
			return nil, err
		}
		event := domain.UpdateEvent{UserID: ref.UserID, Type: domain.UpdateEventEditMessage,
			Pts: pts, PtsCount: 1, Date: date, Message: message}
		if err := appendUserUpdateEvent(ctx, tx, q, ref.UserID, event); err != nil {
			return nil, fmt.Errorf("append channel star gift viewer edit event: %w", err)
		}
		excludeAuthKeyID, excludeSessionID := [8]byte{}, int64(0)
		if ref.UserID == originUserID {
			excludeAuthKeyID = originAuthKeyID
			excludeSessionID = originSessionID
		}
		if err := enqueueDispatch(ctx, q, dispatchEnqueue{
			TargetUserID: ref.UserID, Pts: int32(pts), EventType: string(domain.UpdateEventEditMessage),
			ExcludeAuthKeyID: excludeAuthKeyID, ExcludeSessionID: excludeSessionID,
		}); err != nil {
			return nil, fmt.Errorf("enqueue channel star gift viewer edit: %w", err)
		}
		edits = append(edits, domain.EditedMessageForUser{UserID: ref.UserID, Message: message, Event: event})
	}
	return edits, nil
}
