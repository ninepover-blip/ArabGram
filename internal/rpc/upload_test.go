package rpc

import (
	"testing"

	"github.com/iamxvbaba/td/clock"
	"github.com/iamxvbaba/td/tg"
	"github.com/iamxvbaba/td/tlprofile"
	"go.uber.org/zap"
)

func TestCoreDispatcherDoesNotRegisterFileDataRPCs(t *testing.T) {
	router := New(Config{}, Deps{}, zap.NewNop(), clock.System)
	for _, method := range []tlprofile.SemanticID{
		tlprofile.SemanticMethodUploadSaveFilePart,
		tlprofile.SemanticMethodUploadSaveBigFilePart,
		tlprofile.SemanticMethodUploadGetFile,
		tlprofile.SemanticMethodUploadGetFileHashes,
	} {
		if router.dispatcher.Has(method) {
			t.Fatalf("Core dispatcher registered Edge-only FileData method %#016x", uint64(method))
		}
	}
	if !router.dispatcher.Has(tlprofile.SemanticMethodUploadGetWebFile) {
		t.Fatal("Core dispatcher lost upload.getWebFile while removing FileData methods")
	}
}

func TestStorageFileTypePrefersMagicOverMime(t *testing.T) {
	webp := []byte{'R', 'I', 'F', 'F', 0, 0, 0, 0, 'W', 'E', 'B', 'P'}
	if _, ok := storageFileType("image/jpeg", webp).(*tg.StorageFileWebp); !ok {
		t.Fatalf("webp bytes mislabeled as jpeg should return StorageFileWebp")
	}
}

func TestStorageFileTypeFallsBackToMime(t *testing.T) {
	if _, ok := storageFileType("image/png", nil).(*tg.StorageFilePng); !ok {
		t.Fatalf("png mime without bytes should return StorageFilePng")
	}
}
