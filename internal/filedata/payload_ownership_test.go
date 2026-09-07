package filedata

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	filesapp "telesrv/internal/app/files"
	"telesrv/internal/domain"
	"telesrv/internal/filedata/filedatapb"
)

func TestGRPCServerBorrowsReadOnlyPayloads(t *testing.T) {
	for _, size := range []int{64 << 10, 1 << 20} {
		t.Run(payloadSizeName(size), func(t *testing.T) {
			store := &serverTransferStore{benchmarkPayloadStore: &benchmarkPayloadStore{payload: benchmarkPayload(size)}}
			server := &grpcServer{service: store, blobs: store, uploadParts: store}

			fileBytes := benchmarkPayload(size)
			store.next = fileBytes
			file, err := server.GetFile(context.Background(), &filedatapb.GetFileRequest{LocationKey: "doc:1", Limit: int32(size)})
			if err != nil || !sameBacking(file.Data, fileBytes) || cap(file.Data) != len(file.Data) {
				t.Fatalf("GetFile did not borrow a capacity-clipped service payload: err=%v", err)
			}

			rangeBytes := benchmarkPayload(size)
			store.next = rangeBytes
			blobRange, err := server.GetBlobRange(context.Background(), &filedatapb.GetBlobRangeRequest{ObjectKey: "blob", Limit: int64(size)})
			if err != nil || !sameBacking(blobRange.Data, rangeBytes) || cap(blobRange.Data) != len(blobRange.Data) {
				t.Fatalf("GetBlobRange did not borrow a capacity-clipped backend payload: err=%v", err)
			}

			partBytes := benchmarkPayload(size)
			store.next = partBytes
			part, err := server.GetUploadPart(context.Background(), &filedatapb.GetUploadPartRequest{ObjectKey: "part"})
			if err != nil || !sameBacking(part.Data, partBytes) || cap(part.Data) != len(part.Data) {
				t.Fatalf("GetUploadPart did not borrow a capacity-clipped backend payload: err=%v", err)
			}
		})
	}
}

func TestGRPCRemoteTransfersResponsesAndBorrowsRequests(t *testing.T) {
	for _, size := range []int{64 << 10, 1 << 20} {
		t.Run(payloadSizeName(size), func(t *testing.T) {
			payload := benchmarkPayload(size)
			fileResponse := &filedatapb.GetFileResponse{Found: true, Data: payload, Total: int64(size)}
			client := &captureFileDataClient{getFileResponse: fileResponse}
			remote := testGRPCRemote(client)
			chunk, found, err := remote.GetFile(context.Background(), domain.FileDownloadRequest{LocationKey: "doc:1", Limit: size})
			if err != nil || !found || !sameBacking(chunk.Bytes, payload) || cap(chunk.Bytes) != len(chunk.Bytes) || fileResponse.Data != nil {
				t.Fatalf("GetFile ownership: found=%v err=%v response_retained=%v", found, err, fileResponse.Data != nil)
			}

			rangePayload := benchmarkPayload(size)
			rangeResponse := &filedatapb.GetBlobRangeResponse{Data: rangePayload, Total: int64(size)}
			client.getRangeResponse = rangeResponse
			gotRange, _, err := remote.GetRange(context.Background(), "blob", 0, int64(size))
			if err != nil || !sameBacking(gotRange, rangePayload) || cap(gotRange) != len(gotRange) || rangeResponse.Data != nil {
				t.Fatalf("GetRange ownership: err=%v response_retained=%v", err, rangeResponse.Data != nil)
			}

			partPayload := benchmarkPayload(size)
			partResponse := &filedatapb.GetUploadPartResponse{Data: partPayload}
			client.getUploadPartResponse = partResponse
			gotPart, err := remote.GetUploadPart(context.Background(), "part")
			if err != nil || !sameBacking(gotPart, partPayload) || cap(gotPart) != len(gotPart) || partResponse.Data != nil {
				t.Fatalf("GetUploadPart ownership: err=%v response_retained=%v", err, partResponse.Data != nil)
			}

			uploadSum := benchmarkPayload(sha256.Size)
			client.putUploadPartResponse = &filedatapb.UploadPartObjectResponse{Size: int64(size), Sha256: uploadSum}
			obj, err := remote.PutUploadPart(context.Background(), 1, 2, 3, payload)
			if err != nil || client.putUploadPartRequest == nil || !sameBacking(client.putUploadPartRequest.Data, payload) {
				t.Fatalf("PutUploadPart request was copied: err=%v", err)
			}
			if !sameBacking(obj.SHA256, uploadSum) || client.putUploadPartResponse.Sha256 != nil {
				t.Fatal("PutUploadPart response hash was not transferred")
			}

			client.saveFilePartResponse = &filedatapb.SaveFilePartResponse{Saved: true}
			if saved, err := remote.SaveFilePart(context.Background(), 1, 2, 3, payload); err != nil || !saved {
				t.Fatalf("SaveFilePart: saved=%v err=%v", saved, err)
			}
			if client.saveFilePartRequest == nil || !sameBacking(client.saveFilePartRequest.Data, payload) {
				t.Fatal("SaveFilePart request was copied")
			}

			client.putBlobResponse = &filedatapb.BlobObjectResponse{ObjectKey: "blob", Size: int64(size), Sha256: benchmarkPayload(sha256.Size)}
			if _, err := remote.Put(context.Background(), payload); err != nil {
				t.Fatal(err)
			}
			directStream := client.putBlobStreams[len(client.putBlobStreams)-1]
			assertDirectPutChunks(t, directStream.chunks, payload)

			client.putBlobResponse = &filedatapb.BlobObjectResponse{ObjectKey: "blob", Size: int64(size), Sha256: benchmarkPayload(sha256.Size)}
			if _, _, _, err := remote.PutReader(context.Background(), bytes.NewReader(payload)); err != nil {
				t.Fatal(err)
			}
			readerStream := client.putBlobStreams[len(client.putBlobStreams)-1]
			assertReaderPutChunks(t, readerStream.chunks, payload)
		})
	}
}

func TestBufconnPayloadMutationIsolation(t *testing.T) {
	for _, size := range []int{64 << 10, 1 << 20} {
		t.Run(payloadSizeName(size), func(t *testing.T) {
			payload := benchmarkPayload(size)
			original := bytes.Clone(payload)
			store := &recordingPayloadStore{benchmarkPayloadStore: &benchmarkPayloadStore{payload: original}}
			remote, cleanup := startBufconnFileData(t, "ownership-token", store, store, store)
			defer cleanup()

			chunk, found, err := remote.GetFile(context.Background(), domain.FileDownloadRequest{LocationKey: "doc:1", Limit: size})
			if err != nil || !found {
				t.Fatalf("GetFile: found=%v err=%v", found, err)
			}
			if sameBacking(chunk.Bytes, store.payload) {
				t.Fatal("gRPC GetFile response still aliases File backend storage")
			}

			blobRange, _, err := remote.GetRange(context.Background(), "blob", 0, int64(size))
			if err != nil {
				t.Fatal(err)
			}
			if sameBacking(blobRange, store.payload) {
				t.Fatal("gRPC GetRange response still aliases File backend storage")
			}

			part, err := remote.GetUploadPart(context.Background(), "part")
			if err != nil {
				t.Fatal(err)
			}
			if sameBacking(part, store.payload) {
				t.Fatal("gRPC GetUploadPart response still aliases File backend storage")
			}

			if _, err := remote.Put(context.Background(), payload); err != nil {
				t.Fatal(err)
			}
			payload[0] ^= 0xff
			if got := store.lastBlob(); !bytes.Equal(got, original) {
				t.Fatal("PutBlob did not finish consuming bytes before return")
			}
			payload[0] ^= 0xff

			if _, _, _, err := remote.PutReader(context.Background(), bytes.NewReader(payload)); err != nil {
				t.Fatal(err)
			}
			if got := store.lastBlob(); !bytes.Equal(got, original) {
				t.Fatal("PutReader stream reused or mutated a sent buffer")
			}

			if _, err := remote.PutUploadPart(context.Background(), 1, 2, 3, payload); err != nil {
				t.Fatal(err)
			}
			payload[0] ^= 0xff
			if got := store.lastPart(); !bytes.Equal(got, original) {
				t.Fatal("PutUploadPart did not consume request before return")
			}
		})
	}
}

func sameBacking(a, b []byte) bool {
	return len(a) > 0 && len(b) > 0 && &a[0] == &b[0]
}

func testGRPCRemote(client filedatapb.FileDataServiceClient) *GRPCRemote {
	return &GRPCRemote{client: client, requestTimeout: time.Second}
}

func assertDirectPutChunks(t *testing.T, chunks []*filedatapb.PutBlobChunk, payload []byte) {
	t.Helper()
	offset := 0
	for _, chunk := range chunks {
		if len(chunk.Data) == 0 || &chunk.Data[0] != &payload[offset] {
			t.Fatalf("direct Put chunk at %d was copied", offset)
		}
		offset += len(chunk.Data)
	}
	if offset != len(payload) {
		t.Fatalf("direct Put sent %d bytes, want %d", offset, len(payload))
	}
}

func assertReaderPutChunks(t *testing.T, chunks []*filedatapb.PutBlobChunk, payload []byte) {
	t.Helper()
	var joined []byte
	for i, chunk := range chunks {
		joined = append(joined, chunk.Data...)
		if i > 0 && sameBacking(chunks[i-1].Data, chunk.Data) {
			t.Fatalf("PutReader chunks %d and %d reuse backing memory", i-1, i)
		}
	}
	if !bytes.Equal(joined, payload) {
		t.Fatal("PutReader chunks changed after Send")
	}
}

type captureFileDataClient struct {
	filedatapb.FileDataServiceClient

	getFileResponse       *filedatapb.GetFileResponse
	getRangeResponse      *filedatapb.GetBlobRangeResponse
	getUploadPartResponse *filedatapb.GetUploadPartResponse
	putUploadPartRequest  *filedatapb.PutUploadPartRequest
	putUploadPartResponse *filedatapb.UploadPartObjectResponse
	saveFilePartRequest   *filedatapb.SaveFilePartRequest
	saveFilePartResponse  *filedatapb.SaveFilePartResponse
	putBlobResponse       *filedatapb.BlobObjectResponse
	putBlobStreams        []*capturePutBlobStream
}

func (c *captureFileDataClient) GetFile(context.Context, *filedatapb.GetFileRequest, ...grpc.CallOption) (*filedatapb.GetFileResponse, error) {
	return c.getFileResponse, nil
}

func (c *captureFileDataClient) GetBlobRange(context.Context, *filedatapb.GetBlobRangeRequest, ...grpc.CallOption) (*filedatapb.GetBlobRangeResponse, error) {
	return c.getRangeResponse, nil
}

func (c *captureFileDataClient) GetUploadPart(context.Context, *filedatapb.GetUploadPartRequest, ...grpc.CallOption) (*filedatapb.GetUploadPartResponse, error) {
	return c.getUploadPartResponse, nil
}

func (c *captureFileDataClient) PutUploadPart(_ context.Context, req *filedatapb.PutUploadPartRequest, _ ...grpc.CallOption) (*filedatapb.UploadPartObjectResponse, error) {
	c.putUploadPartRequest = req
	return c.putUploadPartResponse, nil
}

func (c *captureFileDataClient) SaveFilePart(_ context.Context, req *filedatapb.SaveFilePartRequest, _ ...grpc.CallOption) (*filedatapb.SaveFilePartResponse, error) {
	c.saveFilePartRequest = req
	return c.saveFilePartResponse, nil
}

func (c *captureFileDataClient) PutBlob(ctx context.Context, _ ...grpc.CallOption) (grpc.ClientStreamingClient[filedatapb.PutBlobChunk, filedatapb.BlobObjectResponse], error) {
	stream := &capturePutBlobStream{ctx: ctx, response: c.putBlobResponse}
	c.putBlobStreams = append(c.putBlobStreams, stream)
	return stream, nil
}

type capturePutBlobStream struct {
	ctx      context.Context
	chunks   []*filedatapb.PutBlobChunk
	response *filedatapb.BlobObjectResponse
}

func (s *capturePutBlobStream) Send(chunk *filedatapb.PutBlobChunk) error {
	s.chunks = append(s.chunks, chunk)
	return nil
}

func (s *capturePutBlobStream) CloseAndRecv() (*filedatapb.BlobObjectResponse, error) {
	return s.response, nil
}

func (*capturePutBlobStream) Header() (metadata.MD, error) { return nil, nil }
func (*capturePutBlobStream) Trailer() metadata.MD         { return nil }
func (*capturePutBlobStream) CloseSend() error             { return nil }
func (s *capturePutBlobStream) Context() context.Context   { return s.ctx }
func (s *capturePutBlobStream) SendMsg(msg any) error      { return s.Send(msg.(*filedatapb.PutBlobChunk)) }
func (*capturePutBlobStream) RecvMsg(any) error            { return io.EOF }

type serverTransferStore struct {
	*benchmarkPayloadStore
	next []byte
}

func (s *serverTransferStore) GetFile(context.Context, domain.FileDownloadRequest) (domain.FileChunk, bool, error) {
	data := s.next
	s.next = nil
	return domain.FileChunk{Bytes: data, Total: int64(len(data))}, true, nil
}

func (s *serverTransferStore) GetRange(context.Context, string, int64, int64) ([]byte, int64, error) {
	data := s.next
	s.next = nil
	return data, int64(len(data)), nil
}

func (s *serverTransferStore) GetUploadPart(context.Context, string) ([]byte, error) {
	data := s.next
	s.next = nil
	return data, nil
}

type recordingPayloadStore struct {
	*benchmarkPayloadStore

	mu      sync.Mutex
	putBlob []byte
	putPart []byte
}

func (s *recordingPayloadStore) PutReader(_ context.Context, src io.Reader) (string, int64, []byte, error) {
	data, err := io.ReadAll(src)
	if err != nil {
		return "", 0, nil, err
	}
	s.mu.Lock()
	s.putBlob = data
	s.mu.Unlock()
	return "blob", int64(len(data)), make([]byte, sha256.Size), nil
}

func (s *recordingPayloadStore) PutUploadPart(_ context.Context, _ int64, _ int64, _ int, data []byte) (filesapp.UploadPartObject, error) {
	s.mu.Lock()
	s.putPart = bytes.Clone(data)
	s.mu.Unlock()
	return filesapp.UploadPartObject{Backend: domain.MediaBackend(s.Name()), ObjectKey: "part", Size: int64(len(data)), SHA256: make([]byte, sha256.Size)}, nil
}

func (s *recordingPayloadStore) lastBlob() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return bytes.Clone(s.putBlob)
}

func (s *recordingPayloadStore) lastPart() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return bytes.Clone(s.putPart)
}
