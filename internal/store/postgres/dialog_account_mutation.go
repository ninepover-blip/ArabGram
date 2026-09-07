package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

// MutateAccountDialogs is the single PostgreSQL owner for account-scoped
// dialog mutations. The transaction follows the established message lock
// order: ordered user advisory lock, business rows, watermark/event/dispatch.
func (s *DialogStore) MutateAccountDialogs(
	ctx context.Context,
	mutation store.DialogAccountMutation,
	build store.DeliveryEffectsBuilder[store.DialogAccountMutationSnapshot],
) (store.DialogAccountMutationSnapshot, error) {
	if build == nil {
		return store.DialogAccountMutationSnapshot{}, store.ErrDeliveryOutboxRequired
	}
	if err := mutation.Validate(); err != nil {
		return store.DialogAccountMutationSnapshot{}, err
	}
	beginner, ok := s.db.(txBeginner)
	if !ok {
		return store.DialogAccountMutationSnapshot{}, fmt.Errorf("dialog account mutation: db does not support transactions")
	}
	tx, err := beginner.Begin(ctx)
	if err != nil {
		return store.DialogAccountMutationSnapshot{}, fmt.Errorf("begin dialog account mutation: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(ctx)
		}
	}()
	if err := lockUsersForUpdate(ctx, tx, mutation.UserID); err != nil {
		return store.DialogAccountMutationSnapshot{}, fmt.Errorf("lock dialog account mutation owner: %w", err)
	}
	snapshot, err := mutateDialogAccountTx(ctx, tx, mutation)
	if err != nil {
		return store.DialogAccountMutationSnapshot{}, err
	}
	effects, err := build(cloneDialogAccountSnapshot(snapshot))
	if err != nil {
		return store.DialogAccountMutationSnapshot{}, fmt.Errorf("build dialog account delivery effects: %w", err)
	}
	if err := store.ValidateDialogAccountEffects(snapshot, effects); err != nil {
		return store.DialogAccountMutationSnapshot{}, err
	}
	applied, err := applyDeliveryEffectsTx(ctx, tx, effects)
	if err != nil {
		return store.DialogAccountMutationSnapshot{}, fmt.Errorf("apply dialog account delivery effects: %w", err)
	}
	snapshot.Effects = cloneDeliveryEffects(applied)
	if err := tx.Commit(ctx); err != nil {
		return store.DialogAccountMutationSnapshot{}, fmt.Errorf("commit dialog account mutation: %w", err)
	}
	committed = true
	return cloneDialogAccountSnapshot(snapshot), nil
}

func mutateDialogAccountTx(ctx context.Context, tx pgx.Tx, mutation store.DialogAccountMutation) (store.DialogAccountMutationSnapshot, error) {
	dialogs := NewDialogStore(tx)
	channels := NewChannelStore(tx)
	snapshot := store.DialogAccountMutationSnapshot{Mutation: cloneDialogAccountMutation(mutation)}
	switch mutation.Kind {
	case store.DialogAccountSaveDraft:
		current, found, err := dialogs.GetDraft(ctx, mutation.UserID, mutation.Draft.Peer, mutation.Draft.TopMessageID)
		if err != nil {
			return snapshot, err
		}
		if found && sameDraftContent(current, mutation.Draft) {
			break
		}
		if err := dialogs.SaveDraft(ctx, mutation.UserID, mutation.Draft); err != nil {
			return snapshot, err
		}
		snapshot.Changed = true
	case store.DialogAccountDeleteDraft:
		changed, err := dialogs.DeleteDraft(ctx, mutation.UserID, mutation.Peer, mutation.TopMessageID)
		if err != nil {
			return snapshot, err
		}
		snapshot.Changed = changed
	case store.DialogAccountClearDrafts:
		drafts, err := dialogs.ClearDrafts(ctx, mutation.UserID, mutation.Limit)
		if err != nil {
			return snapshot, err
		}
		snapshot.Drafts = cloneDialogDrafts(drafts)
		snapshot.Changed = len(drafts) > 0
	case store.DialogAccountSetPinned:
		changed, folderID, err := mutateDialogPinTx(ctx, tx, dialogs, channels, mutation)
		if err != nil {
			return snapshot, err
		}
		snapshot.Changed, snapshot.FolderID = changed, folderID
	case store.DialogAccountReorderPinned:
		before, err := loadPinnedDialogStateTx(ctx, tx, mutation.UserID, mutation.FolderID)
		if err != nil {
			return snapshot, err
		}
		if _, err := dialogs.ReorderPinned(ctx, mutation.UserID, mutation.FolderID, mutation.Peers, mutation.Force); err != nil {
			return snapshot, err
		}
		if _, err := channels.ReorderChannelPinnedDialogs(ctx, mutation.UserID, mutation.FolderID, mutation.Peers, mutation.Force); err != nil {
			return snapshot, err
		}
		if mutation.FolderID == domain.DialogMainFolderID {
			if _, err := NewCommunityStore(tx, nil, nil).ReorderCommunityPinned(ctx, mutation.UserID, mutation.Peers, mutation.Force); err != nil {
				return snapshot, err
			}
		}
		after, err := loadPinnedDialogStateTx(ctx, tx, mutation.UserID, mutation.FolderID)
		if err != nil {
			return snapshot, err
		}
		snapshot.Changed = !reflect.DeepEqual(before, after)
	case store.DialogAccountSetUnreadMark:
		var changed bool
		var err error
		if mutation.Peer.Type == domain.PeerTypeChannel {
			changed, err = channels.SetChannelDialogUnreadMark(ctx, mutation.UserID, mutation.Peer.ID, mutation.Value)
		} else {
			changed, err = dialogs.SetUnreadMark(ctx, mutation.UserID, mutation.Peer, mutation.Value)
		}
		if err != nil {
			return snapshot, err
		}
		snapshot.Changed = changed
	case store.DialogAccountHidePeerSettings:
		changed, err := dialogs.SetPeerSettingsBarHidden(ctx, mutation.UserID, mutation.Peer)
		if err != nil {
			return snapshot, err
		}
		snapshot.Changed = changed
	case store.DialogAccountUpsertFolder:
		current, found, err := dialogs.GetFolder(ctx, mutation.UserID, mutation.Folder.ID)
		if err != nil {
			return snapshot, err
		}
		if found && reflect.DeepEqual(current, mutation.Folder) {
			break
		}
		if err := dialogs.UpsertFolder(ctx, mutation.UserID, mutation.Folder); err != nil {
			return snapshot, err
		}
		snapshot.Changed = true
	case store.DialogAccountDeleteFolder:
		_, found, err := dialogs.GetFolder(ctx, mutation.UserID, mutation.FolderID)
		if err != nil {
			return snapshot, err
		}
		if !found {
			break
		}
		if err := dialogs.DeleteFolder(ctx, mutation.UserID, mutation.FolderID); err != nil {
			return snapshot, err
		}
		snapshot.Changed = true
	case store.DialogAccountReorderFolders:
		before, err := dialogFolderIDs(ctx, dialogs, mutation.UserID)
		if err != nil {
			return snapshot, err
		}
		if err := dialogs.ReorderFolders(ctx, mutation.UserID, mutation.FolderOrder); err != nil {
			return snapshot, err
		}
		after, err := dialogFolderIDs(ctx, dialogs, mutation.UserID)
		if err != nil {
			return snapshot, err
		}
		snapshot.Changed = !reflect.DeepEqual(before, after)
	case store.DialogAccountSetFolderTags:
		before, err := dialogs.ListFolders(ctx, mutation.UserID)
		if err != nil {
			return snapshot, err
		}
		if before.TagsEnabled == mutation.Value {
			break
		}
		if err := dialogs.SetFolderTagsEnabled(ctx, mutation.UserID, mutation.Value); err != nil {
			return snapshot, err
		}
		snapshot.Changed = true
	case store.DialogAccountEditPeerFolders:
		before, err := loadDialogFolderStateTx(ctx, tx, mutation.UserID, mutation.FolderPeers)
		if err != nil {
			return snapshot, err
		}
		private, channel := splitFolderPeerUpdates(mutation.FolderPeers)
		if err := dialogs.EditPeerFolders(ctx, mutation.UserID, private); err != nil {
			return snapshot, err
		}
		if err := channels.EditChannelPeerFolders(ctx, mutation.UserID, channel); err != nil {
			return snapshot, err
		}
		after, err := loadDialogFolderStateTx(ctx, tx, mutation.UserID, mutation.FolderPeers)
		if err != nil {
			return snapshot, err
		}
		snapshot.Changed = !reflect.DeepEqual(before, after)
	case store.DialogAccountSetChannelViewForum:
		changed, err := setChannelViewForumAsMessagesTx(ctx, tx, mutation.UserID, mutation.Peer.ID, mutation.Value)
		if err != nil {
			return snapshot, err
		}
		snapshot.Changed = changed
	default:
		return snapshot, fmt.Errorf("unsupported dialog account mutation %q", mutation.Kind)
	}
	return snapshot, nil
}

func setChannelViewForumAsMessagesTx(ctx context.Context, tx pgx.Tx, userID, channelID int64, enabled bool) (bool, error) {
	var changed bool
	if err := tx.QueryRow(ctx, `
WITH target AS (
    SELECT c.id AS channel_id, c.top_message_id, c.date AS top_message_date
    FROM channels c
    JOIN channel_members m ON m.channel_id = c.id
    WHERE c.id = $2 AND m.user_id = $1 AND m.status = 'active' AND NOT c.deleted
),
ensured AS (
    INSERT INTO channel_dialogs (user_id, channel_id, top_message_id, top_message_date)
    SELECT $1, channel_id, top_message_id, top_message_date FROM target
    ON CONFLICT (user_id, channel_id) DO NOTHING
),
updated_dialog AS (
    UPDATE channel_dialogs d
    SET view_forum_as_messages = $3, updated_at = now()
    WHERE d.user_id = $1 AND d.channel_id = $2
      AND EXISTS (SELECT 1 FROM target)
      AND d.view_forum_as_messages IS DISTINCT FROM $3::boolean
    RETURNING d.user_id
)
SELECT EXISTS (SELECT 1 FROM updated_dialog)::boolean`, userID, channelID, enabled).Scan(&changed); err != nil {
		return false, fmt.Errorf("set channel view forum as messages: %w", err)
	}
	return changed, nil
}

func mutateDialogPinTx(ctx context.Context, tx pgx.Tx, dialogs *DialogStore, channels *ChannelStore, mutation store.DialogAccountMutation) (bool, int, error) {
	if mutation.Peer.Type == domain.PeerTypeFolder {
		changed, err := dialogs.SetArchivePinned(ctx, mutation.UserID, mutation.Value)
		return changed, 0, err
	}
	folderID, pinned, err := loadOneDialogPinTx(ctx, tx, mutation.UserID, mutation.Peer)
	if err != nil {
		return false, 0, err
	}
	if pinned == mutation.Value {
		return false, folderID, nil
	}
	if mutation.Value {
		count, err := countPinnedDialogsTx(ctx, tx, mutation.UserID, folderID)
		if err != nil {
			return false, 0, err
		}
		if count >= mutation.PinnedLimit {
			return false, 0, domain.ErrPinnedDialogsTooMuch
		}
	}
	var changed bool
	switch mutation.Peer.Type {
	case domain.PeerTypeChannel:
		changed, folderID, err = channels.SetChannelDialogPinned(ctx, mutation.UserID, mutation.Peer.ID, mutation.Value)
	case domain.PeerTypeCommunity:
		changed, err = NewCommunityStore(tx, nil, nil).SetCommunityPinned(ctx, mutation.UserID, mutation.Peer.ID, mutation.Value)
		folderID = domain.DialogMainFolderID
	default:
		changed, folderID, err = dialogs.SetPinned(ctx, mutation.UserID, mutation.Peer, mutation.Value)
	}
	if err != nil || !changed || !mutation.Value {
		return changed, folderID, err
	}
	if err := promoteDialogPinTx(ctx, tx, mutation.UserID, mutation.Peer); err != nil {
		return false, 0, err
	}
	return true, folderID, nil
}

func loadOneDialogPinTx(ctx context.Context, tx pgx.Tx, userID int64, peer domain.Peer) (folderID int, pinned bool, err error) {
	switch peer.Type {
	case domain.PeerTypeChannel:
		err = tx.QueryRow(ctx, `SELECT COALESCE(folder_id,0)::int,pinned FROM channel_dialogs WHERE user_id=$1 AND channel_id=$2`, userID, peer.ID).Scan(&folderID, &pinned)
	case domain.PeerTypeCommunity:
		folderID = domain.DialogMainFolderID
		err = tx.QueryRow(ctx, `SELECT pinned FROM community_user_states WHERE user_id=$1 AND community_id=$2 AND collapsed`, userID, peer.ID).Scan(&pinned)
	default:
		err = tx.QueryRow(ctx, `SELECT folder_id::int,pinned FROM dialogs WHERE user_id=$1 AND peer_type=$2 AND peer_id=$3`, userID, string(peer.Type), peer.ID).Scan(&folderID, &pinned)
	}
	if err == pgx.ErrNoRows {
		return folderID, false, nil
	}
	return folderID, pinned, err
}

func countPinnedDialogsTx(ctx context.Context, tx pgx.Tx, userID int64, folderID int) (int, error) {
	var count int
	err := tx.QueryRow(ctx, `
SELECT (
  SELECT COUNT(*) FROM dialogs WHERE user_id=$1 AND pinned AND folder_id=$2
) + (
  SELECT COUNT(*) FROM channel_dialogs WHERE user_id=$1 AND pinned AND COALESCE(folder_id,0)=$2
) + CASE WHEN $2=0 THEN (
  SELECT COUNT(*) FROM community_user_states WHERE user_id=$1 AND pinned AND collapsed
) ELSE 0 END`, userID, folderID).Scan(&count)
	return count, err
}

func promoteDialogPinTx(ctx context.Context, tx pgx.Tx, userID int64, peer domain.Peer) error {
	var order int
	if err := tx.QueryRow(ctx, `
SELECT GREATEST(
  COALESCE((SELECT MAX(pinned_order) FROM dialogs WHERE user_id=$1 AND pinned),0),
  COALESCE((SELECT MAX(pinned_order) FROM channel_dialogs WHERE user_id=$1 AND pinned),0),
  COALESCE((SELECT MAX(pinned_order) FROM community_user_states WHERE user_id=$1 AND pinned),0)
)::int + 1`, userID).Scan(&order); err != nil {
		return err
	}
	var rowsAffected int64
	switch peer.Type {
	case domain.PeerTypeChannel:
		tag, err := tx.Exec(ctx, `UPDATE channel_dialogs SET pinned_order=$3,updated_at=now() WHERE user_id=$1 AND channel_id=$2 AND pinned`, userID, peer.ID, order)
		if err != nil {
			return err
		}
		rowsAffected = tag.RowsAffected()
	case domain.PeerTypeCommunity:
		tag, err := tx.Exec(ctx, `UPDATE community_user_states SET pinned_order=$3,updated_at=now() WHERE user_id=$1 AND community_id=$2 AND pinned`, userID, peer.ID, order)
		if err != nil {
			return err
		}
		rowsAffected = tag.RowsAffected()
	default:
		tag, err := tx.Exec(ctx, `UPDATE dialogs SET pinned_order=$4,updated_at=now() WHERE user_id=$1 AND peer_type=$2 AND peer_id=$3 AND pinned`, userID, string(peer.Type), peer.ID, order)
		if err != nil {
			return err
		}
		rowsAffected = tag.RowsAffected()
	}
	if rowsAffected != 1 {
		return fmt.Errorf("promote dialog pin: target row disappeared")
	}
	return nil
}

type pinnedDialogState struct {
	Peer  domain.Peer
	Order int
}

func loadPinnedDialogStateTx(ctx context.Context, tx pgx.Tx, userID int64, folderID int) ([]pinnedDialogState, error) {
	rows, err := tx.Query(ctx, `
SELECT peer_type,peer_id,pinned_order FROM (
  SELECT peer_type,peer_id,pinned_order FROM dialogs WHERE user_id=$1 AND pinned AND folder_id=$2
  UNION ALL
  SELECT 'channel',channel_id,pinned_order FROM channel_dialogs WHERE user_id=$1 AND pinned AND COALESCE(folder_id,0)=$2
  UNION ALL
  SELECT 'community',community_id,pinned_order FROM community_user_states WHERE $2=0 AND user_id=$1 AND pinned AND collapsed
) pinned
ORDER BY peer_type,peer_id`, userID, folderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []pinnedDialogState{}
	for rows.Next() {
		var peerType string
		var item pinnedDialogState
		if err := rows.Scan(&peerType, &item.Peer.ID, &item.Order); err != nil {
			return nil, err
		}
		item.Peer.Type = domain.PeerType(peerType)
		out = append(out, item)
	}
	return out, rows.Err()
}

func loadDialogFolderStateTx(ctx context.Context, tx pgx.Tx, userID int64, requested []domain.FolderPeerUpdate) (map[domain.Peer]int, error) {
	out := make(map[domain.Peer]int, len(requested))
	for _, item := range requested {
		folderID := -1
		var err error
		if item.Peer.Type == domain.PeerTypeChannel {
			err = tx.QueryRow(ctx, `SELECT COALESCE(folder_id,0)::int FROM channel_dialogs WHERE user_id=$1 AND channel_id=$2`, userID, item.Peer.ID).Scan(&folderID)
		} else {
			err = tx.QueryRow(ctx, `SELECT folder_id::int FROM dialogs WHERE user_id=$1 AND peer_type=$2 AND peer_id=$3`, userID, string(item.Peer.Type), item.Peer.ID).Scan(&folderID)
		}
		if err != nil && err != pgx.ErrNoRows {
			return nil, err
		}
		out[item.Peer] = folderID
	}
	return out, nil
}

func splitFolderPeerUpdates(items []domain.FolderPeerUpdate) (private, channel []domain.FolderPeerUpdate) {
	for _, item := range items {
		if item.Peer.Type == domain.PeerTypeChannel {
			channel = append(channel, item)
		} else {
			private = append(private, item)
		}
	}
	return private, channel
}

func dialogFolderIDs(ctx context.Context, dialogs *DialogStore, userID int64) ([]int, error) {
	list, err := dialogs.ListFolders(ctx, userID)
	if err != nil {
		return nil, err
	}
	out := make([]int, 0, len(list.Folders))
	for _, folder := range list.Folders {
		out = append(out, folder.ID)
	}
	return out, nil
}

func sameDraftContent(a, b domain.DialogDraft) bool {
	a.Date, b.Date = 0, 0
	return reflect.DeepEqual(a, b)
}

func cloneDialogAccountMutation(in store.DialogAccountMutation) store.DialogAccountMutation {
	in.Draft = cloneDialogDraftValue(in.Draft)
	in.Peers = append([]domain.Peer(nil), in.Peers...)
	in.Folder = cloneDialogFolderValue(in.Folder)
	in.FolderOrder = append([]int(nil), in.FolderOrder...)
	in.FolderPeers = append([]domain.FolderPeerUpdate(nil), in.FolderPeers...)
	return in
}

func cloneDialogAccountSnapshot(in store.DialogAccountMutationSnapshot) store.DialogAccountMutationSnapshot {
	in.Mutation = cloneDialogAccountMutation(in.Mutation)
	in.Drafts = cloneDialogDrafts(in.Drafts)
	in.Effects = cloneDeliveryEffects(in.Effects)
	return in
}

func cloneDialogDrafts(in []domain.DialogDraft) []domain.DialogDraft {
	out := make([]domain.DialogDraft, len(in))
	for i := range in {
		out[i] = cloneDialogDraftValue(in[i])
	}
	return out
}

func cloneDialogDraftValue(in domain.DialogDraft) domain.DialogDraft {
	data, _ := jsonMarshal(in)
	var out domain.DialogDraft
	_ = jsonUnmarshal(data, &out)
	return out
}

func cloneDialogFolderValue(in domain.DialogFolder) domain.DialogFolder {
	data, _ := jsonMarshal(in)
	var out domain.DialogFolder
	_ = jsonUnmarshal(data, &out)
	return out
}

func cloneDeliveryEffects(in []store.DeliveryEffect) []store.DeliveryEffect {
	out := append([]store.DeliveryEffect(nil), in...)
	for i := range out {
		out[i].Payload = append([]byte(nil), out[i].Payload...)
		out[i].Event.Peers = append([]domain.Peer(nil), out[i].Event.Peers...)
		out[i].Event.FilterOrder = append([]int(nil), out[i].Event.FilterOrder...)
		out[i].Event.FolderPeers = append([]domain.FolderPeerUpdate(nil), out[i].Event.FolderPeers...)
	}
	return out
}

// Keep encoding helpers local so snapshots never alias caller-owned slices.
func jsonMarshal(v any) ([]byte, error)      { return json.Marshal(v) }
func jsonUnmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
