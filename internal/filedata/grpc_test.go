package filedata

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"

	filesapp "telesrv/internal/app/files"
	"telesrv/internal/domain"
	"telesrv/internal/filedata/filedatapb"
)

func TestGRPCRemoteUploadPartMaterializeAndRange(t *testing.T) {
	ctx := context.Background()
	backend, err := filesapp.NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalFS: %v", err)
	}
	service := &grpcTestDataService{
		blobs:       backend,
		uploadParts: backend,
		parts:       make(map[string]filesapp.UploadPartObject),
	}
	remote, cleanup := startBufconnFileData(t, "test-token", service, backend, backend)
	defer cleanup()

	if got := remote.Name(); got != "localfs" {
		t.Fatalf("remote backend = %q, want localfs", got)
	}
	for part, data := range [][]byte{[]byte("hello "), []byte("world")} {
		saved, err := remote.SaveFilePart(ctx, 42, 99, part, data)
		if err != nil {
			t.Fatalf("SaveFilePart(%d): %v", part, err)
		}
		if !saved {
			t.Fatalf("SaveFilePart(%d) saved = false", part)
		}
	}

	blob, err := remote.AssembleUploadBlob(ctx, 42, 99, 2)
	if err != nil {
		t.Fatalf("AssembleUploadBlob: %v", err)
	}
	if blob.Size != int64(len("hello world")) {
		t.Fatalf("assembled size = %d, want %d", blob.Size, len("hello world"))
	}
	got, total, err := remote.GetRange(ctx, blob.ObjectKey, 6, 5)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	if string(got) != "world" || total != int64(len("hello world")) {
		t.Fatalf("range = %q total=%d, want world total=%d", got, total, len("hello world"))
	}
	service.hashes = map[string][]domain.FileHash{
		"doc:99": {{Offset: 0, Limit: 11, Hash: []byte("hello-world-sha")}},
	}
	hashes, found, err := remote.GetFileHashes(ctx, domain.FileHashRequest{LocationKey: "doc:99"})
	if err != nil || !found {
		t.Fatalf("GetFileHashes found=%v err=%v", found, err)
	}
	if len(hashes) != 1 || hashes[0].Limit != 11 || string(hashes[0].Hash) != "hello-world-sha" {
		t.Fatalf("hashes = %+v", hashes)
	}
}

func TestGRPCRemoteTransfersAndReadsImmutableFileRange(t *testing.T) {
	ctx := context.Background()
	backend, err := filesapp.NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalFS: %v", err)
	}
	payload := []byte("immutable-filedata-grpc-range")
	objectKey, err := backend.Put(ctx, payload)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	source := &domain.ImmutableFileRange{
		Backend:     domain.MediaBackendLocalFS,
		ObjectKey:   objectKey,
		Offset:      0,
		Length:      len(payload),
		Total:       int64(len(payload)),
		MimeType:    "application/octet-stream",
		RangeSHA256: sha256.Sum256(payload),
	}
	service := &grpcTestDataService{
		blobs:       backend,
		uploadParts: backend,
		parts:       make(map[string]filesapp.UploadPartObject),
		fileChunk: domain.FileChunk{
			Bytes: append([]byte(nil), payload...), MimeType: source.MimeType,
			Total: source.Total, ImmutableRange: source,
		},
	}
	remote, cleanup := startBufconnFileData(t, "test-token", service, backend, backend)
	defer cleanup()

	chunk, found, err := remote.GetFile(ctx, domain.FileDownloadRequest{LocationKey: "doc:7", Limit: len(payload)})
	if err != nil || !found {
		t.Fatalf("GetFile found=%v err=%v", found, err)
	}
	if chunk.ImmutableRange == nil || *chunk.ImmutableRange != *source || !bytes.Equal(chunk.Bytes, payload) {
		t.Fatalf("GetFile chunk = %+v", chunk)
	}
	replayed, err := remote.ReadImmutableFileRange(ctx, *chunk.ImmutableRange)
	if err != nil {
		t.Fatalf("ReadImmutableFileRange: %v", err)
	}
	if !bytes.Equal(replayed, payload) {
		t.Fatalf("replayed = %q, want %q", replayed, payload)
	}
}

func TestGRPCRemoteRejectsWrongBearerToken(t *testing.T) {
	backend, err := filesapp.NewLocalFS(t.TempDir())
	if err != nil {
		t.Fatalf("NewLocalFS: %v", err)
	}
	service := &grpcTestDataService{
		blobs:       backend,
		uploadParts: backend,
		parts:       make(map[string]filesapp.UploadPartObject),
	}
	listener, stop := startBufconnFileDataServer(t, "good-token", service, backend, backend)
	defer stop()

	_, conn, err := DialGRPCRemote(context.Background(), GRPCClientConfig{
		Resolver:       bufconnResolver{listener: listener},
		Token:          "bad-token",
		RequestTimeout: time.Second,
	})
	if conn != nil {
		_ = conn.Close()
	}
	if err == nil {
		t.Fatal("DialGRPCRemote with wrong token succeeded")
	}
}

func startBufconnFileData(t testing.TB, token string, service DataService, blobs filesapp.BlobBackend, uploadParts filesapp.UploadPartBackend) (*GRPCRemote, func()) {
	t.Helper()
	listener, stopServer := startBufconnFileDataServer(t, token, service, blobs, uploadParts)
	remote, conn, err := DialGRPCRemote(context.Background(), GRPCClientConfig{
		Resolver:       bufconnResolver{listener: listener},
		Token:          token,
		RequestTimeout: 10 * time.Second,
	})
	if err != nil {
		stopServer()
		t.Fatalf("DialGRPCRemote: %v", err)
	}
	return remote, func() {
		_ = conn.Close()
		stopServer()
	}
}

func startBufconnFileDataServer(t testing.TB, token string, service DataService, blobs filesapp.BlobBackend, uploadParts filesapp.UploadPartBackend) (*bufconn.Listener, func()) {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(
		grpc.ChainUnaryInterceptor(bearerUnaryServerInterceptor(token)),
		grpc.ChainStreamInterceptor(bearerStreamServerInterceptor(token)),
	)
	filedatapb.RegisterFileDataServiceServer(srv, &grpcServer{
		service:     service,
		blobs:       blobs,
		uploadParts: uploadParts,
		instanceID:  "test-filedata",
	})
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus(filedatapb.FileDataService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, healthSrv)
	go func() {
		_ = srv.Serve(listener)
	}()
	return listener, func() {
		healthSrv.Shutdown()
		srv.Stop()
		_ = listener.Close()
	}
}

type bufconnResolver struct {
	listener *bufconn.Listener
}

func (r bufconnResolver) Target() string { return "passthrough:///bufnet" }

func (r bufconnResolver) DialOptions() []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return r.listener.Dial()
		}),
	}
}

type grpcTestDataService struct {
	blobs       filesapp.BlobBackend
	uploadParts filesapp.UploadPartBackend

	mu        sync.Mutex
	parts     map[string]filesapp.UploadPartObject
	hashes    map[string][]domain.FileHash
	fileChunk domain.FileChunk
}

func (s *grpcTestDataService) SaveFilePart(ctx context.Context, ownerUserID, fileID int64, part int, data []byte) (bool, error) {
	return s.save(ctx, ownerUserID, fileID, part, data)
}

func (s *grpcTestDataService) SaveBigFilePart(ctx context.Context, ownerUserID, fileID int64, part, totalParts int, data []byte) (bool, error) {
	if totalParts <= 0 {
		return false, domain.ErrFilePartsInvalid
	}
	return s.save(ctx, ownerUserID, fileID, part, data)
}

func (s *grpcTestDataService) GetFile(context.Context, domain.FileDownloadRequest) (domain.FileChunk, bool, error) {
	if s.fileChunk.Bytes != nil {
		return s.fileChunk, true, nil
	}
	return domain.FileChunk{}, false, nil
}

func (s *grpcTestDataService) GetFileHashes(_ context.Context, req domain.FileHashRequest) ([]domain.FileHash, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	hashes, ok := s.hashes[req.LocationKey]
	if !ok {
		return nil, false, nil
	}
	return append([]domain.FileHash(nil), hashes...), true, nil
}

func (s *grpcTestDataService) AssembleUploadBlob(ctx context.Context, ownerUserID, fileID int64, expectedParts int) (filesapp.AssembledUploadBlob, error) {
	if expectedParts <= 0 {
		return filesapp.AssembledUploadBlob{}, domain.ErrFilePartsInvalid
	}
	var data bytes.Buffer
	for part := 0; part < expectedParts; part++ {
		s.mu.Lock()
		obj, ok := s.parts[partKey(ownerUserID, fileID, part)]
		s.mu.Unlock()
		if !ok {
			return filesapp.AssembledUploadBlob{}, domain.ErrFilePartsInvalid
		}
		partData, err := s.uploadParts.GetUploadPart(ctx, obj.ObjectKey)
		if err != nil {
			return filesapp.AssembledUploadBlob{}, err
		}
		_, _ = data.Write(partData)
	}
	key, size, sum, err := s.blobs.PutReader(ctx, bytes.NewReader(data.Bytes()))
	if err != nil {
		return filesapp.AssembledUploadBlob{}, err
	}
	return filesapp.AssembledUploadBlob{ObjectKey: key, Size: size, SHA256: sum}, nil
}

func (s *grpcTestDataService) DeleteExpiredUploadParts(context.Context, time.Time, int) (int64, error) {
	return 0, nil
}

func (s *grpcTestDataService) save(ctx context.Context, ownerUserID, fileID int64, part int, data []byte) (bool, error) {
	obj, err := s.uploadParts.PutUploadPart(ctx, ownerUserID, fileID, part, data)
	if err != nil {
		return false, err
	}
	s.mu.Lock()
	s.parts[partKey(ownerUserID, fileID, part)] = obj
	s.mu.Unlock()
	return true, nil
}

func partKey(ownerUserID, fileID int64, part int) string {
	return fmt.Sprintf("%d/%d/%d", ownerUserID, fileID, part)
}
