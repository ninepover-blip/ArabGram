package filedata

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"runtime"
	"testing"
	"time"

	"github.com/iamxvbaba/td/tg"

	filesapp "telesrv/internal/app/files"
	"telesrv/internal/domain"
)

func BenchmarkGRPCPayloadFlow(b *testing.B) {
	for _, size := range []int{64 << 10, 1 << 20} {
		payload := benchmarkPayload(size)
		name := payloadSizeName(size)
		b.Run("GetFile/"+name, func(b *testing.B) {
			store := &benchmarkPayloadStore{payload: payload}
			remote, cleanup := startBufconnFileData(b, "benchmark-token", store, store, store)
			defer cleanup()
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				chunk, found, err := remote.GetFile(context.Background(), domain.FileDownloadRequest{
					LocationKey: "doc:1",
					Limit:       size,
				})
				if err != nil || !found || len(chunk.Bytes) != size {
					b.Fatalf("GetFile: size=%d found=%v err=%v", len(chunk.Bytes), found, err)
				}
				// This is the Edge tail: FileData's request-owned storage is published
				// read-only and moves directly into tg.UploadFile. TL encoding is the
				// next and only required wire copy.
				uploadFile := tg.UploadFile{Bytes: chunk.Bytes}
				if !sameBacking(uploadFile.Bytes, chunk.Bytes) {
					b.Fatal("tg.UploadFile payload was copied")
				}
				runtime.KeepAlive(uploadFile)
			}
		})
		b.Run("GetBlobRange/"+name, func(b *testing.B) {
			store := &benchmarkPayloadStore{payload: payload}
			remote, cleanup := startBufconnFileData(b, "benchmark-token", store, store, store)
			defer cleanup()
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				data, total, err := remote.GetRange(context.Background(), "blob", 0, int64(size))
				if err != nil || total != int64(size) || len(data) != size {
					b.Fatalf("GetRange: size=%d total=%d err=%v", len(data), total, err)
				}
			}
		})
		b.Run("GetUploadPart/"+name, func(b *testing.B) {
			store := &benchmarkPayloadStore{payload: payload}
			remote, cleanup := startBufconnFileData(b, "benchmark-token", store, store, store)
			defer cleanup()
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				data, err := remote.GetUploadPart(context.Background(), "part")
				if err != nil || len(data) != size {
					b.Fatalf("GetUploadPart: size=%d err=%v", len(data), err)
				}
			}
		})
		b.Run("PutUploadPart/"+name, func(b *testing.B) {
			store := &benchmarkPayloadStore{payload: payload}
			remote, cleanup := startBufconnFileData(b, "benchmark-token", store, store, store)
			defer cleanup()
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				obj, err := remote.PutUploadPart(context.Background(), 1, 2, i, payload)
				if err != nil || obj.Size != int64(size) {
					b.Fatalf("PutUploadPart: size=%d err=%v", obj.Size, err)
				}
			}
		})
		b.Run("PutBlobBytes/"+name, func(b *testing.B) {
			store := &benchmarkPayloadStore{payload: payload}
			remote, cleanup := startBufconnFileData(b, "benchmark-token", store, store, store)
			defer cleanup()
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := remote.Put(context.Background(), payload); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("PutBlobReader/"+name, func(b *testing.B) {
			store := &benchmarkPayloadStore{payload: payload}
			remote, cleanup := startBufconnFileData(b, "benchmark-token", store, store, store)
			defer cleanup()
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, _, err := remote.PutReader(context.Background(), bytes.NewReader(payload)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

type benchmarkPayloadStore struct {
	payload []byte
}

func (s *benchmarkPayloadStore) Name() string { return "benchmark" }

func (s *benchmarkPayloadStore) Put(ctx context.Context, data []byte) (string, error) {
	key, _, _, err := s.PutReader(ctx, bytes.NewReader(data))
	return key, err
}

func (s *benchmarkPayloadStore) PutReader(_ context.Context, src io.Reader) (string, int64, []byte, error) {
	size, err := io.Copy(io.Discard, src)
	if err != nil {
		return "", 0, nil, err
	}
	return "blob", size, make([]byte, sha256.Size), nil
}

func (s *benchmarkPayloadStore) Get(context.Context, string) ([]byte, error) {
	return bytes.Clone(s.payload), nil
}

func (s *benchmarkPayloadStore) GetRange(_ context.Context, _ string, offset, limit int64) ([]byte, int64, error) {
	start := min(max(offset, 0), int64(len(s.payload)))
	end := int64(len(s.payload))
	if limit > 0 && start+limit < end {
		end = start + limit
	}
	return bytes.Clone(s.payload[start:end]), int64(len(s.payload)), nil
}

func (s *benchmarkPayloadStore) PutUploadPart(_ context.Context, _ int64, _ int64, _ int, data []byte) (filesapp.UploadPartObject, error) {
	return filesapp.UploadPartObject{
		Backend:   domain.MediaBackend(s.Name()),
		ObjectKey: "part",
		Size:      int64(len(data)),
		SHA256:    make([]byte, sha256.Size),
	}, nil
}

func (s *benchmarkPayloadStore) GetUploadPart(context.Context, string) ([]byte, error) {
	return bytes.Clone(s.payload), nil
}

func (s *benchmarkPayloadStore) OpenUploadPart(ctx context.Context, key string) (io.ReadCloser, error) {
	data, err := s.GetUploadPart(ctx, key)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (*benchmarkPayloadStore) DeleteUploadPart(context.Context, string) error { return nil }

func (*benchmarkPayloadStore) DeleteExpiredUploadParts(context.Context, time.Time, int) (int64, error) {
	return 0, nil
}

func (*benchmarkPayloadStore) SaveFilePart(context.Context, int64, int64, int, []byte) (bool, error) {
	return true, nil
}

func (*benchmarkPayloadStore) SaveBigFilePart(context.Context, int64, int64, int, int, []byte) (bool, error) {
	return true, nil
}

func (s *benchmarkPayloadStore) GetFile(context.Context, domain.FileDownloadRequest) (domain.FileChunk, bool, error) {
	return domain.FileChunk{Bytes: bytes.Clone(s.payload), MimeType: "application/octet-stream", Total: int64(len(s.payload))}, true, nil
}

func (*benchmarkPayloadStore) GetFileHashes(context.Context, domain.FileHashRequest) ([]domain.FileHash, bool, error) {
	return nil, true, nil
}

func (*benchmarkPayloadStore) AssembleUploadBlob(context.Context, int64, int64, int) (filesapp.AssembledUploadBlob, error) {
	return filesapp.AssembledUploadBlob{ObjectKey: "blob", SHA256: make([]byte, sha256.Size)}, nil
}

func benchmarkPayload(size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i*31 + 7)
	}
	return payload
}

func payloadSizeName(size int) string {
	if size == 64<<10 {
		return "64KiB"
	}
	return "1MiB"
}
