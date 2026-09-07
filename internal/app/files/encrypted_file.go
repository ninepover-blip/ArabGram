package files

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"telesrv/internal/domain"
)

// CreateEncryptedFileFromUpload 把已上传分片组装成密聊文件 blob 并铸造 EncryptedFile 快照。
// 盲中继：内容是客户端加密的 bytes，不解析、不分类、不缩略图；blob 落 location_key
// "enc:<id>"（复用 BlobBackend，下载经 inputEncryptedFileLocation → 同 key）。
// access_hash 不强校验（沿用现有媒体 dev 姿态，依赖不可枚举 id）。元数据持久化由调用方
// （rpc 层经 SecretChats.PutEncryptedFile）负责。
func (s *Service) CreateEncryptedFileFromUpload(ctx context.Context, file domain.UploadedFileRef, keyFingerprint int) (domain.EncryptedFileRef, error) {
	body, err := s.assembleUploadBlob(ctx, file.OwnerUserID, file.FileID, file.Parts)
	if err != nil {
		return domain.EncryptedFileRef{}, err
	}
	if body.Size == 0 {
		return domain.EncryptedFileRef{}, domain.ErrDocumentInvalid
	}
	id := randomID()
	if err := s.media.PutFileBlob(ctx, domain.FileBlob{
		LocationKey: fmt.Sprintf("enc:%d", id),
		Backend:     domain.MediaBackend(s.blobs.Name()),
		ObjectKey:   body.ObjectKey,
		Size:        body.Size,
		SHA256:      body.SHA256,
		MimeType:    "application/octet-stream",
	}); err != nil {
		return domain.EncryptedFileRef{}, err
	}
	if err := s.cleanupUploadParts(ctx, file.OwnerUserID, file.FileID); err != nil {
		s.log.Warn("cleanup encrypted file upload parts failed",
			zap.Int64("owner_user_id", file.OwnerUserID),
			zap.Int64("file_id", file.FileID),
			zap.Int64("encrypted_file_id", id),
			zap.Error(err))
	}
	return domain.EncryptedFileRef{
		ID:             id,
		AccessHash:     randomID(),
		Size:           body.Size,
		DCID:           s.dc,
		KeyFingerprint: keyFingerprint,
	}, nil
}
