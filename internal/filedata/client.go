package filedata

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	filesapp "telesrv/internal/app/files"
	"telesrv/internal/domain"
	"telesrv/internal/filedata/filedatapb"
)

func DialGRPCRemote(ctx context.Context, cfg GRPCClientConfig) (*GRPCRemote, *grpc.ClientConn, error) {
	if strings.TrimSpace(cfg.Token) == "" {
		return nil, nil, ErrGRPCTokenMissing
	}
	resolverProvider, err := grpcResolverProvider(cfg)
	if err != nil {
		return nil, nil, err
	}
	target := strings.TrimSpace(resolverProvider.Target())
	if target == "" {
		return nil, nil, ErrGRPCTargetsMissing
	}
	opts, err := clientDialOptions(cfg, resolverProvider)
	if err != nil {
		return nil, nil, err
	}
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("filedata grpc client: %w", err)
	}
	remote := &GRPCRemote{
		client:         filedatapb.NewFileDataServiceClient(conn),
		health:         healthpb.NewHealthClient(conn),
		requestTimeout: clientRequestTimeout(cfg.RequestTimeout),
		log:            cfg.Logger,
		token:          strings.TrimSpace(cfg.Token),
	}
	if remote.log == nil {
		remote.log = zap.NewNop()
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := remote.Check(ctx); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	return remote, conn, nil
}

func (r *GRPCRemote) Check(ctx context.Context) error {
	if r == nil || r.client == nil {
		return ErrGRPCUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	callCtx = r.withAuth(callCtx)
	if r.health != nil {
		res, err := r.health.Check(callCtx, &healthpb.HealthCheckRequest{Service: filedatapb.FileDataService_ServiceDesc.ServiceName})
		if err != nil {
			return fmt.Errorf("filedata grpc health: %w", err)
		}
		if res.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			return fmt.Errorf("filedata grpc health: status %s", res.GetStatus().String())
		}
	}
	info, err := r.client.GetInfo(callCtx, &filedatapb.FileDataInfoRequest{
		ProtocolVersion:             grpcProtocolVersion,
		MinSupportedProtocolVersion: grpcMinSupportedVersion,
		Capabilities:                append([]string(nil), grpcCapabilities...),
	})
	if err != nil {
		return fmt.Errorf("filedata grpc info: %w", err)
	}
	if info.GetError() != "" {
		return fmt.Errorf("%w: %s", ErrGRPCProtocolMismatch, info.GetError())
	}
	if !protocolRangesOverlap(grpcMinSupportedVersion, grpcProtocolVersion, info.GetMinSupportedProtocolVersion(), info.GetProtocolVersion()) {
		return fmt.Errorf("%w: client=%d..%d file=%d..%d",
			ErrGRPCProtocolMismatch,
			grpcMinSupportedVersion,
			grpcProtocolVersion,
			info.GetMinSupportedProtocolVersion(),
			info.GetProtocolVersion(),
		)
	}
	if strings.TrimSpace(info.GetBackend()) == "" {
		return ErrBackendMissing
	}
	r.backend = strings.TrimSpace(info.GetBackend())
	return nil
}

func (r *GRPCRemote) Name() string {
	if r == nil {
		return ""
	}
	return r.backend
}

func (r *GRPCRemote) Put(ctx context.Context, data []byte) (string, error) {
	key, _, _, err := r.putBlob(ctx, func(stream grpc.ClientStreamingClient[filedatapb.PutBlobChunk, filedatapb.BlobObjectResponse]) error {
		for offset := 0; offset < len(data); {
			end := min(offset+defaultGRPCPutBlobChunk, len(data))
			if err := stream.Send(&filedatapb.PutBlobChunk{Data: data[offset:end]}); err != nil {
				return fmt.Errorf("filedata grpc put_blob send: %w", err)
			}
			offset = end
		}
		return nil
	})
	return key, err
}

func (r *GRPCRemote) PutReader(ctx context.Context, src io.Reader) (string, int64, []byte, error) {
	if r == nil || r.client == nil {
		return "", 0, nil, ErrGRPCUnavailable
	}
	if src == nil {
		return "", 0, nil, fmt.Errorf("filedata put blob reader is nil")
	}
	return r.putBlob(ctx, func(stream grpc.ClientStreamingClient[filedatapb.PutBlobChunk, filedatapb.BlobObjectResponse]) error {
		for {
			bufferSize := putBlobReadBufferSize(src)
			if bufferSize == 0 {
				return nil
			}
			// SendMsg may retain the message for tracing or retry. Each read therefore
			// gets an immutable buffer whose ownership moves to that message.
			buf := make([]byte, bufferSize)
			n, readErr := io.ReadFull(src, buf)
			if n > 0 {
				if err := stream.Send(&filedatapb.PutBlobChunk{Data: buf[:n]}); err != nil {
					return fmt.Errorf("filedata grpc put_blob send: %w", err)
				}
			}
			switch readErr {
			case nil:
				continue
			case io.EOF, io.ErrUnexpectedEOF:
				return nil
			default:
				return readErr
			}
		}
	})
}

func (r *GRPCRemote) putBlob(ctx context.Context, send func(grpc.ClientStreamingClient[filedatapb.PutBlobChunk, filedatapb.BlobObjectResponse]) error) (string, int64, []byte, error) {
	if r == nil || r.client == nil {
		return "", 0, nil, ErrGRPCUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	stream, err := r.client.PutBlob(r.withAuth(callCtx))
	if err != nil {
		return "", 0, nil, fmt.Errorf("filedata grpc put_blob: %w", err)
	}
	if err := send(stream); err != nil {
		return "", 0, nil, err
	}
	res, err := stream.CloseAndRecv()
	if err != nil {
		return "", 0, nil, fmt.Errorf("filedata grpc put_blob close: %w", err)
	}
	if err := errorFromPB(res.GetErrorKind(), res.GetError()); err != nil {
		return "", 0, nil, err
	}
	return res.GetObjectKey(), res.GetSize(), takePBBytes(&res.Sha256), nil
}

func putBlobReadBufferSize(src io.Reader) int {
	remaining := -1
	switch src := src.(type) {
	case *bytes.Buffer:
		remaining = src.Len()
	case *bytes.Reader:
		remaining = src.Len()
	case *strings.Reader:
		remaining = src.Len()
	case *io.LimitedReader:
		if src.N <= 0 {
			remaining = 0
		} else if src.N < int64(defaultGRPCPutBlobChunk) {
			remaining = int(src.N)
		}
	}
	if remaining == 0 {
		return 0
	}
	if remaining > 0 {
		return min(remaining, defaultGRPCPutBlobChunk)
	}
	return defaultGRPCPutBlobChunk
}

func (r *GRPCRemote) Get(ctx context.Context, objectKey string) ([]byte, error) {
	data, _, err := r.GetRange(ctx, objectKey, 0, 0)
	return data, err
}

func (r *GRPCRemote) GetRange(ctx context.Context, objectKey string, offset, limit int64) ([]byte, int64, error) {
	if r == nil || r.client == nil {
		return nil, 0, ErrGRPCUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	res, err := r.client.GetBlobRange(r.withAuth(callCtx), &filedatapb.GetBlobRangeRequest{
		ObjectKey: objectKey,
		Offset:    offset,
		Limit:     limit,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("filedata grpc get_blob_range: %w", err)
	}
	if err := errorFromPB(res.GetErrorKind(), res.GetError()); err != nil {
		return nil, 0, err
	}
	return takePBBytes(&res.Data), res.GetTotal(), nil
}

func (r *GRPCRemote) PutUploadPart(ctx context.Context, ownerUserID, fileID int64, part int, data []byte) (filesapp.UploadPartObject, error) {
	if r == nil || r.client == nil {
		return filesapp.UploadPartObject{}, ErrGRPCUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	res, err := r.client.PutUploadPart(r.withAuth(callCtx), &filedatapb.PutUploadPartRequest{
		OwnerUserId: ownerUserID,
		FileId:      fileID,
		FilePart:    int32(part),
		Data:        data,
	})
	if err != nil {
		return filesapp.UploadPartObject{}, fmt.Errorf("filedata grpc put_upload_part: %w", err)
	}
	if err := errorFromPB(res.GetErrorKind(), res.GetError()); err != nil {
		return filesapp.UploadPartObject{}, err
	}
	return filesapp.UploadPartObject{
		Backend:   domain.MediaBackend(res.GetBackend()),
		ObjectKey: res.GetObjectKey(),
		Size:      res.GetSize(),
		SHA256:    takePBBytes(&res.Sha256),
	}, nil
}

func (r *GRPCRemote) GetUploadPart(ctx context.Context, objectKey string) ([]byte, error) {
	if r == nil || r.client == nil {
		return nil, ErrGRPCUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	res, err := r.client.GetUploadPart(r.withAuth(callCtx), &filedatapb.GetUploadPartRequest{ObjectKey: objectKey})
	if err != nil {
		return nil, fmt.Errorf("filedata grpc get_upload_part: %w", err)
	}
	if err := errorFromPB(res.GetErrorKind(), res.GetError()); err != nil {
		return nil, err
	}
	return takePBBytes(&res.Data), nil
}

func (r *GRPCRemote) OpenUploadPart(ctx context.Context, objectKey string) (io.ReadCloser, error) {
	data, err := r.GetUploadPart(ctx, objectKey)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (r *GRPCRemote) DeleteUploadPart(ctx context.Context, objectKey string) error {
	if r == nil || r.client == nil {
		return ErrGRPCUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	res, err := r.client.DeleteUploadPart(r.withAuth(callCtx), &filedatapb.DeleteUploadPartRequest{ObjectKey: objectKey})
	if err != nil {
		return fmt.Errorf("filedata grpc delete_upload_part: %w", err)
	}
	return errorFromPB(res.GetErrorKind(), res.GetError())
}

func (r *GRPCRemote) DeleteExpiredUploadParts(ctx context.Context, before time.Time, limit int) (int64, error) {
	if r == nil || r.client == nil {
		return 0, ErrGRPCUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	res, err := r.client.DeleteExpiredUploadParts(r.withAuth(callCtx), &filedatapb.DeleteExpiredUploadPartsRequest{
		BeforeUnixNano: before.UnixNano(),
		Limit:          int32(limit),
	})
	if err != nil {
		return 0, fmt.Errorf("filedata grpc delete_expired_upload_parts: %w", err)
	}
	if err := errorFromPB(res.GetErrorKind(), res.GetError()); err != nil {
		return res.GetDeleted(), err
	}
	return res.GetDeleted(), nil
}

func (r *GRPCRemote) AssembleUploadBlob(ctx context.Context, ownerUserID, fileID int64, expectedParts int) (filesapp.AssembledUploadBlob, error) {
	if r == nil || r.client == nil {
		return filesapp.AssembledUploadBlob{}, ErrGRPCUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	res, err := r.client.MaterializeUploadBlob(r.withAuth(callCtx), &filedatapb.MaterializeUploadBlobRequest{
		OwnerUserId:   ownerUserID,
		FileId:        fileID,
		ExpectedParts: int32(expectedParts),
	})
	if err != nil {
		return filesapp.AssembledUploadBlob{}, fmt.Errorf("filedata grpc materialize_upload_blob: %w", err)
	}
	if err := errorFromPB(res.GetErrorKind(), res.GetError()); err != nil {
		return filesapp.AssembledUploadBlob{}, err
	}
	return filesapp.AssembledUploadBlob{
		ObjectKey: res.GetObjectKey(),
		Size:      res.GetSize(),
		SHA256:    takePBBytes(&res.Sha256),
	}, nil
}

func (r *GRPCRemote) SaveFilePart(ctx context.Context, ownerUserID, fileID int64, part int, data []byte) (bool, error) {
	return r.saveFilePart(ctx, ownerUserID, fileID, part, 0, false, data)
}

func (r *GRPCRemote) SaveBigFilePart(ctx context.Context, ownerUserID, fileID int64, part, totalParts int, data []byte) (bool, error) {
	return r.saveFilePart(ctx, ownerUserID, fileID, part, totalParts, true, data)
}

func (r *GRPCRemote) saveFilePart(ctx context.Context, ownerUserID, fileID int64, part, totalParts int, big bool, data []byte) (bool, error) {
	if r == nil || r.client == nil {
		return false, ErrGRPCUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	res, err := r.client.SaveFilePart(r.withAuth(callCtx), &filedatapb.SaveFilePartRequest{
		OwnerUserId:    ownerUserID,
		FileId:         fileID,
		FilePart:       int32(part),
		FileTotalParts: int32(totalParts),
		Big:            big,
		Data:           data,
	})
	if err != nil {
		return false, fmt.Errorf("filedata grpc save_file_part: %w", err)
	}
	if err := errorFromPB(res.GetErrorKind(), res.GetError()); err != nil {
		return false, err
	}
	return res.GetSaved(), nil
}

func (r *GRPCRemote) GetFile(ctx context.Context, req domain.FileDownloadRequest) (domain.FileChunk, bool, error) {
	if r == nil || r.client == nil {
		return domain.FileChunk{}, false, ErrGRPCUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	res, err := r.client.GetFile(r.withAuth(callCtx), &filedatapb.GetFileRequest{
		LocationKey: req.LocationKey,
		Offset:      req.Offset,
		Limit:       int32(req.Limit),
	})
	if err != nil {
		return domain.FileChunk{}, false, fmt.Errorf("filedata grpc get_file: %w", err)
	}
	if err := errorFromPB(res.GetErrorKind(), res.GetError()); err != nil {
		return domain.FileChunk{}, false, err
	}
	chunk := domain.FileChunk{
		Bytes:    takePBBytes(&res.Data),
		MimeType: res.GetMimeType(),
		Total:    res.GetTotal(),
	}
	immutablePresent := res.GetImmutableBackend() != "" || res.GetImmutableObjectKey() != "" ||
		res.GetImmutableOffset() != 0 || res.GetImmutableLength() != 0 || len(res.GetImmutableRangeSha256()) != 0
	if immutablePresent {
		digest := res.GetImmutableRangeSha256()
		if res.GetImmutableBackend() == "" || res.GetImmutableObjectKey() == "" ||
			res.GetImmutableOffset() < 0 || res.GetImmutableLength() < 0 || len(digest) != sha256.Size {
			return domain.FileChunk{}, false, fmt.Errorf("filedata grpc get_file: invalid immutable range descriptor")
		}
		source := &domain.ImmutableFileRange{
			Backend:   domain.MediaBackend(res.GetImmutableBackend()),
			ObjectKey: res.GetImmutableObjectKey(),
			Offset:    res.GetImmutableOffset(),
			Length:    int(res.GetImmutableLength()),
			Total:     res.GetTotal(),
			MimeType:  res.GetMimeType(),
		}
		copy(source.RangeSHA256[:], digest)
		chunk.ImmutableRange = source
	}
	return chunk, res.GetFound(), nil
}

// ReadImmutableFileRange is the Edge replay path. It bypasses mutable location
// metadata and reads the already-authorized content-addressed object directly
// through the File service's range API.
func (r *GRPCRemote) ReadImmutableFileRange(ctx context.Context, source domain.ImmutableFileRange) ([]byte, error) {
	if r == nil || source.ObjectKey == "" || source.Offset < 0 || source.Length < 0 || source.Total < 0 {
		return nil, fmt.Errorf("filedata grpc immutable range: invalid descriptor")
	}
	if source.Backend != domain.MediaBackend(r.Name()) {
		return nil, fmt.Errorf("filedata grpc immutable range: backend mismatch source=%q configured=%q", source.Backend, r.Name())
	}
	data, total, err := r.GetRange(ctx, source.ObjectKey, source.Offset, int64(source.Length))
	if err != nil {
		return nil, err
	}
	if total != source.Total || len(data) != source.Length {
		return nil, fmt.Errorf("filedata grpc immutable range changed: total=%d/%d length=%d/%d", total, source.Total, len(data), source.Length)
	}
	if digest := sha256.Sum256(data); digest != source.RangeSHA256 {
		return nil, fmt.Errorf("filedata grpc immutable range digest mismatch")
	}
	return data, nil
}

func (r *GRPCRemote) GetFileHashes(ctx context.Context, req domain.FileHashRequest) ([]domain.FileHash, bool, error) {
	if r == nil || r.client == nil {
		return nil, false, ErrGRPCUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	res, err := r.client.GetFileHashes(r.withAuth(callCtx), &filedatapb.GetFileHashesRequest{
		LocationKey: req.LocationKey,
		Offset:      req.Offset,
	})
	if err != nil {
		return nil, false, fmt.Errorf("filedata grpc get_file_hashes: %w", err)
	}
	if err := errorFromPB(res.GetErrorKind(), res.GetError()); err != nil {
		return nil, false, err
	}
	hashes := make([]domain.FileHash, 0, len(res.GetHashes()))
	for _, hash := range res.GetHashes() {
		hashes = append(hashes, domain.FileHash{
			Offset: hash.GetOffset(),
			Limit:  int(hash.GetLimit()),
			Hash:   takePBBytes(&hash.Hash),
		})
	}
	return hashes, res.GetFound(), nil
}

// takePBBytes moves a protobuf byte field into the domain result. Unary/client-
// stream completion gives this client exclusive storage ownership of the
// decoded response; the domain publishes that storage as immutable. Clipping
// capacity prevents an append from writing into an unexposed protobuf tail.
func takePBBytes(field *[]byte) []byte {
	data := *field
	*field = nil
	return data[:len(data):len(data)]
}

func (r *GRPCRemote) withRequestTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, r.requestTimeout)
}

func (r *GRPCRemote) withAuth(ctx context.Context) context.Context {
	if r == nil {
		return ctx
	}
	return appendBearerToken(ctx, r.token)
}

func clientRequestTimeout(d time.Duration) time.Duration {
	if d <= 0 {
		return defaultGRPCClientTimeout
	}
	return d
}
