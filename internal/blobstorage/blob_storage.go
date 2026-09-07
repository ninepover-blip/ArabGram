package blobstorage

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	filesapp "telesrv/internal/app/files"
	"telesrv/internal/domain"
)

type BackendCounter interface {
	FileBlobBackendCounts(ctx context.Context) (map[domain.MediaBackend]int64, error)
}

func RequireConfiguredBackend(ctx context.Context, counter BackendCounter, configured string) error {
	counts, err := counter.FileBlobBackendCounts(ctx)
	if err != nil {
		return err
	}
	var mismatches []string
	for backend, count := range counts {
		if count > 0 && string(backend) != configured {
			mismatches = append(mismatches, fmt.Sprintf("%s=%d", backend, count))
		}
	}
	if len(mismatches) == 0 {
		return nil
	}
	sort.Strings(mismatches)
	return fmt.Errorf(
		"file_blobs contains permanent backend rows incompatible with TELESRV_BLOB_BACKEND=%s (%s); run the explicit blob migration before changing the configured backend",
		configured, strings.Join(mismatches, ", "),
	)
}

type Runtime struct {
	Permanent  filesapp.BlobBackend
	UploadPart filesapp.UploadPartBackend
}

type UniqueBytesStore interface {
	UniqueFileBlobBytes(ctx context.Context, backend domain.MediaBackend) (int64, error)
}

type Config struct {
	BlobBackendKind string
	BlobDir         string
	BlobStagingDir  string

	S3Endpoint        string
	S3Region          string
	S3Bucket          string
	S3AccessKeyID     string
	S3SecretAccessKey string
	S3UseSSL          bool
	S3PathStyle       bool
	S3CreateBucket    bool

	StorageLowSpaceGuardEnable bool
	StorageMinFreeBytes        int64
	StorageMaxTotalBytes       int64
}

type CapacityRuntime struct {
	PermanentWrite filesapp.SpaceGuard
	StagingWrite   filesapp.SpaceGuard
	Workers        []filesapp.SpaceGuard
}

func NewCapacityRuntime(ctx context.Context, cfg Config, store UniqueBytesStore) (CapacityRuntime, error) {
	noop := filesapp.NoopSpaceGuard{}
	result := CapacityRuntime{PermanentWrite: noop, StagingWrite: noop}
	if !cfg.StorageLowSpaceGuardEnable {
		return result, nil
	}

	localRoot := cfg.BlobDir
	if cfg.BlobBackendKind == string(domain.MediaBackendS3) {
		localRoot = cfg.BlobStagingDir
	}
	if cfg.StorageMinFreeBytes > 0 {
		local := filesapp.NewLocalDiskSpaceGuard(localRoot, cfg.StorageMinFreeBytes)
		if err := local.Refresh(ctx); err != nil {
			return CapacityRuntime{}, fmt.Errorf("initialize local storage capacity snapshot: %w", err)
		}
		result.StagingWrite = local
		result.PermanentWrite = local
		result.Workers = append(result.Workers, local)
	}

	if cfg.BlobBackendKind == string(domain.MediaBackendS3) && cfg.StorageMaxTotalBytes > 0 {
		s3 := filesapp.NewS3BudgetSpaceGuard(store, cfg.StorageMaxTotalBytes)
		if err := s3.Refresh(ctx); err != nil {
			return CapacityRuntime{}, fmt.Errorf("initialize s3 storage capacity snapshot: %w", err)
		}
		result.Workers = append(result.Workers, s3)
		result.PermanentWrite = filesapp.MultiSpaceGuard{result.StagingWrite, s3}
	}
	return result, nil
}

func NewRuntime(ctx context.Context, cfg Config) (Runtime, error) {
	switch cfg.BlobBackendKind {
	case string(domain.MediaBackendLocalFS):
		local, err := filesapp.NewLocalFS(cfg.BlobDir)
		if err != nil {
			return Runtime{}, fmt.Errorf("init localfs blob backend: %w", err)
		}
		return Runtime{Permanent: local, UploadPart: local}, nil

	case string(domain.MediaBackendS3):
		staging, err := filesapp.NewLocalFS(cfg.BlobStagingDir)
		if err != nil {
			return Runtime{}, fmt.Errorf("init local upload staging backend: %w", err)
		}
		s3, err := filesapp.NewS3FS(ctx, filesapp.S3Config{
			Endpoint:        cfg.S3Endpoint,
			Region:          cfg.S3Region,
			Bucket:          cfg.S3Bucket,
			AccessKeyID:     cfg.S3AccessKeyID,
			SecretAccessKey: cfg.S3SecretAccessKey,
			UseSSL:          cfg.S3UseSSL,
			PathStyle:       cfg.S3PathStyle,
			CreateBucket:    cfg.S3CreateBucket,
			StagingDir:      filepath.Join(cfg.BlobStagingDir, "_s3_spool"),
		})
		if err != nil {
			return Runtime{}, fmt.Errorf("init s3 blob backend: %w", err)
		}
		return Runtime{Permanent: s3, UploadPart: staging}, nil

	default:
		return Runtime{}, fmt.Errorf("unsupported blob backend %q", cfg.BlobBackendKind)
	}
}
