package blobstorage

import (
	"context"
	"strings"
	"testing"

	"telesrv/internal/domain"
)

type fixedBlobBackendCounts map[domain.MediaBackend]int64

func (f fixedBlobBackendCounts) FileBlobBackendCounts(context.Context) (map[domain.MediaBackend]int64, error) {
	return f, nil
}

func TestRequireConfiguredBackend(t *testing.T) {
	ctx := context.Background()
	if err := RequireConfiguredBackend(ctx, fixedBlobBackendCounts{
		domain.MediaBackendLocalFS: 3,
	}, string(domain.MediaBackendLocalFS)); err != nil {
		t.Fatalf("matching backend rejected: %v", err)
	}
	if err := RequireConfiguredBackend(ctx, fixedBlobBackendCounts{}, string(domain.MediaBackendS3)); err != nil {
		t.Fatalf("empty database rejected: %v", err)
	}
	err := RequireConfiguredBackend(ctx, fixedBlobBackendCounts{
		domain.MediaBackendLocalFS: 2,
		domain.MediaBackendS3:      4,
	}, string(domain.MediaBackendS3))
	if err == nil || !strings.Contains(err.Error(), "localfs=2") {
		t.Fatalf("mismatched backend error = %v", err)
	}
}

func TestNewRuntimeDefaultsToOneLocalFS(t *testing.T) {
	runtime, err := NewRuntime(context.Background(), Config{
		BlobBackendKind: string(domain.MediaBackendLocalFS),
		BlobDir:         t.TempDir(),
	})
	if err != nil {
		t.Fatalf("new localfs runtime: %v", err)
	}
	if runtime.Permanent.Name() != string(domain.MediaBackendLocalFS) {
		t.Fatalf("permanent backend = %q", runtime.Permanent.Name())
	}
	if runtime.UploadPart == nil {
		t.Fatal("localfs upload-part backend is nil")
	}
}
