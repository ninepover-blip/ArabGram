package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
)

// 共享媒体索引维护(迁移 0118)。分类真值唯一来自 domain.ClassifyMediaCategories;
// 写路径只在「创建」和「编辑改媒体」两处维护,删除靠读查询 JOIN 过滤 deleted、不在此维护。

// insertChannelMediaIndexTx 为一条频道消息按其媒体类别写索引行(无类别则 no-op)。
func insertChannelMediaIndexTx(ctx context.Context, tx pgx.Tx, channelID int64, id, date int, media *domain.MessageMedia, entities []domain.MessageEntity) error {
	for _, c := range domain.ClassifyMediaCategories(media, entities) {
		if _, err := tx.Exec(ctx, `
INSERT INTO channel_message_media (channel_id, id, category, message_date)
VALUES ($1,$2,$3,$4)
ON CONFLICT (channel_id, id, category) DO NOTHING`, channelID, id, int16(c), date); err != nil {
			return fmt.Errorf("insert channel media index: %w", err)
		}
	}
	return nil
}

// deleteChannelMediaIndexTx 清掉一条频道消息的全部索引行(编辑改媒体前先清后插)。
func deleteChannelMediaIndexTx(ctx context.Context, tx pgx.Tx, channelID int64, id int) error {
	if _, err := tx.Exec(ctx, `DELETE FROM channel_message_media WHERE channel_id = $1 AND id = $2`, channelID, id); err != nil {
		return fmt.Errorf("delete channel media index: %w", err)
	}
	return nil
}

// replaceChannelMediaIndexTx 在编辑替换媒体后重建索引行(类别可能变化)。
func replaceChannelMediaIndexTx(ctx context.Context, tx pgx.Tx, channelID int64, id, date int, media *domain.MessageMedia, entities []domain.MessageEntity) error {
	if err := deleteChannelMediaIndexTx(ctx, tx, channelID, id); err != nil {
		return err
	}
	return insertChannelMediaIndexTx(ctx, tx, channelID, id, date, media, entities)
}

// insertMessageBoxMediaIndexTx 为一条私聊 owner box 按其媒体类别写索引行。
func insertMessageBoxMediaIndexTx(ctx context.Context, tx pgx.Tx, ownerUserID, peerID int64, boxID, date int, media *domain.MessageMedia, entities []domain.MessageEntity) error {
	for _, c := range domain.ClassifyMediaCategories(media, entities) {
		if _, err := tx.Exec(ctx, `
INSERT INTO message_box_media (owner_user_id, box_id, peer_id, category, message_date)
VALUES ($1,$2,$3,$4,$5)
ON CONFLICT (owner_user_id, box_id, category) DO NOTHING`, ownerUserID, boxID, peerID, int16(c), date); err != nil {
			return fmt.Errorf("insert message box media index: %w", err)
		}
	}
	return nil
}

// deleteMessageBoxMediaIndexTx 清掉一条私聊 owner box 的全部索引行。
func deleteMessageBoxMediaIndexTx(ctx context.Context, tx pgx.Tx, ownerUserID int64, boxID int) error {
	if _, err := tx.Exec(ctx, `DELETE FROM message_box_media WHERE owner_user_id = $1 AND box_id = $2`, ownerUserID, boxID); err != nil {
		return fmt.Errorf("delete message box media index: %w", err)
	}
	return nil
}

// replaceMessageBoxMediaIndexTx 在编辑替换媒体后重建索引行。
func replaceMessageBoxMediaIndexTx(ctx context.Context, tx pgx.Tx, ownerUserID, peerID int64, boxID, date int, media *domain.MessageMedia, entities []domain.MessageEntity) error {
	if err := deleteMessageBoxMediaIndexTx(ctx, tx, ownerUserID, boxID); err != nil {
		return err
	}
	return insertMessageBoxMediaIndexTx(ctx, tx, ownerUserID, peerID, boxID, date, media, entities)
}
