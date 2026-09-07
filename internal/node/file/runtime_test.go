package file

import (
	"testing"

	"telesrv/internal/config"
)

func TestBlobStorageConfigPreservesFileSettings(t *testing.T) {
	got := blobStorageConfig(config.FileConfig{
		BlobBackendKind:            "s3",
		BlobDir:                    "local-blobs",
		BlobStagingDir:             "s3-staging",
		S3Endpoint:                 "minio.example.test:9000",
		S3Region:                   "ap-east-1",
		S3Bucket:                   "media",
		S3AccessKeyID:              "access",
		S3SecretAccessKey:          "secret",
		S3UseSSL:                   false,
		S3PathStyle:                true,
		S3CreateBucket:             true,
		StorageLowSpaceGuardEnable: true,
		StorageMinFreeBytes:        4096,
		StorageMaxTotalBytes:       8192,
	})
	if got.BlobBackendKind != "s3" || got.BlobDir != "local-blobs" || got.BlobStagingDir != "s3-staging" ||
		got.S3Endpoint != "minio.example.test:9000" || got.S3Region != "ap-east-1" || got.S3Bucket != "media" ||
		got.S3AccessKeyID != "access" || got.S3SecretAccessKey != "secret" || got.S3UseSSL || !got.S3PathStyle || !got.S3CreateBucket ||
		!got.StorageLowSpaceGuardEnable || got.StorageMinFreeBytes != 4096 || got.StorageMaxTotalBytes != 8192 {
		t.Fatal("File runtime did not preserve blob storage configuration")
	}
}
