package filedata

import (
	"errors"
	"strings"

	"telesrv/internal/domain"
	"telesrv/internal/filedata/filedatapb"
)

func errorKind(err error) filedatapb.ErrorKind {
	switch {
	case err == nil:
		return filedatapb.ErrorKind_ERROR_KIND_NONE
	case errors.Is(err, domain.ErrFilePartInvalid):
		return filedatapb.ErrorKind_ERROR_KIND_FILE_PART_INVALID
	case errors.Is(err, domain.ErrFilePartsInvalid):
		return filedatapb.ErrorKind_ERROR_KIND_FILE_PARTS_INVALID
	case errors.Is(err, domain.ErrFilePartTooBig):
		return filedatapb.ErrorKind_ERROR_KIND_FILE_PART_TOO_BIG
	case errors.Is(err, domain.ErrUploadQuotaExceeded):
		return filedatapb.ErrorKind_ERROR_KIND_UPLOAD_QUOTA_EXCEEDED
	case errors.Is(err, domain.ErrStorageFull):
		return filedatapb.ErrorKind_ERROR_KIND_STORAGE_FULL
	case errors.Is(err, domain.ErrPhotoInvalid):
		return filedatapb.ErrorKind_ERROR_KIND_PHOTO_INVALID
	case errors.Is(err, domain.ErrDocumentInvalid):
		return filedatapb.ErrorKind_ERROR_KIND_DOCUMENT_INVALID
	default:
		return filedatapb.ErrorKind_ERROR_KIND_NONE
	}
}

func errorFromPB(kind filedatapb.ErrorKind, msg string) error {
	if kind == filedatapb.ErrorKind_ERROR_KIND_NONE && strings.TrimSpace(msg) == "" {
		return nil
	}
	switch kind {
	case filedatapb.ErrorKind_ERROR_KIND_FILE_PART_INVALID:
		return domain.ErrFilePartInvalid
	case filedatapb.ErrorKind_ERROR_KIND_FILE_PARTS_INVALID:
		return domain.ErrFilePartsInvalid
	case filedatapb.ErrorKind_ERROR_KIND_FILE_PART_TOO_BIG:
		return domain.ErrFilePartTooBig
	case filedatapb.ErrorKind_ERROR_KIND_UPLOAD_QUOTA_EXCEEDED:
		return domain.ErrUploadQuotaExceeded
	case filedatapb.ErrorKind_ERROR_KIND_STORAGE_FULL:
		return domain.ErrStorageFull
	case filedatapb.ErrorKind_ERROR_KIND_PHOTO_INVALID:
		return domain.ErrPhotoInvalid
	case filedatapb.ErrorKind_ERROR_KIND_DOCUMENT_INVALID:
		return domain.ErrDocumentInvalid
	default:
		if strings.TrimSpace(msg) == "" {
			return nil
		}
		return errors.New(msg)
	}
}
