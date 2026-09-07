package filedata

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"telesrv/internal/domain"
	"telesrv/internal/filedata/filedatapb"
)

func StartGRPC(ctx context.Context, cfg GRPCServerConfig) (*grpc.Server, error) {
	if strings.TrimSpace(cfg.Addr) == "" {
		return nil, ErrGRPCAddrMissing
	}
	if strings.TrimSpace(cfg.Token) == "" {
		return nil, ErrGRPCTokenMissing
	}
	if cfg.Service == nil || cfg.BlobBackend == nil || cfg.UploadParts == nil {
		return nil, ErrMissingDependency
	}
	if strings.TrimSpace(cfg.BlobBackend.Name()) == "" {
		return nil, ErrBackendMissing
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	transportCreds, tlsEnabled, err := serverTransportCredentials(cfg)
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("listen filedata grpc %s: %w", cfg.Addr, err)
	}
	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(bearerUnaryServerInterceptor(cfg.Token)),
		grpc.ChainStreamInterceptor(bearerStreamServerInterceptor(cfg.Token)),
		grpc.MaxRecvMsgSize(messageSize(cfg.MaxRecvMsgBytes)),
		grpc.MaxSendMsgSize(messageSize(cfg.MaxSendMsgBytes)),
	}
	if tlsEnabled {
		opts = append(opts, grpc.Creds(transportCreds))
	}
	srv := grpc.NewServer(opts...)
	filedatapb.RegisterFileDataServiceServer(srv, &grpcServer{
		service:     cfg.Service,
		blobs:       cfg.BlobBackend,
		uploadParts: cfg.UploadParts,
		instanceID:  strings.TrimSpace(cfg.InstanceID),
	})
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus(filedatapb.FileDataService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, healthSrv)
	go func() {
		<-ctx.Done()
		healthSrv.Shutdown()
		stopped := make(chan struct{})
		go func() {
			srv.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(2 * time.Second):
			srv.Stop()
		}
	}()
	go func() {
		cfg.Logger.Info("filedata grpc listening", zap.String("addr", ln.Addr().String()), zap.String("backend", cfg.BlobBackend.Name()))
		if err := srv.Serve(ln); err != nil {
			cfg.Logger.Warn("filedata grpc exited", zap.Error(err))
		}
	}()
	return srv, nil
}

func (s *grpcServer) GetInfo(_ context.Context, req *filedatapb.FileDataInfoRequest) (*filedatapb.FileDataInfoResponse, error) {
	res := &filedatapb.FileDataInfoResponse{
		ProtocolVersion:             grpcProtocolVersion,
		MinSupportedProtocolVersion: grpcMinSupportedVersion,
		Capabilities:                append([]string(nil), grpcCapabilities...),
		Implementation:              "telesrv-filedata-grpc",
		InstanceId:                  s.instanceID,
	}
	if s != nil && s.blobs != nil {
		res.Backend = s.blobs.Name()
	}
	if !protocolRangesOverlap(req.GetMinSupportedProtocolVersion(), req.GetProtocolVersion(), grpcMinSupportedVersion, grpcProtocolVersion) {
		res.Error = fmt.Sprintf("incompatible protocol client=%d..%d file=%d..%d",
			req.GetMinSupportedProtocolVersion(),
			req.GetProtocolVersion(),
			grpcMinSupportedVersion,
			grpcProtocolVersion,
		)
	}
	return res, nil
}

func (s *grpcServer) SaveFilePart(ctx context.Context, req *filedatapb.SaveFilePartRequest) (*filedatapb.SaveFilePartResponse, error) {
	if s == nil || s.service == nil {
		return nil, status.Error(codes.Internal, ErrMissingDependency.Error())
	}
	var (
		saved bool
		err   error
	)
	if req.GetBig() {
		saved, err = s.service.SaveBigFilePart(ctx, req.GetOwnerUserId(), req.GetFileId(), int(req.GetFilePart()), int(req.GetFileTotalParts()), req.GetData())
	} else {
		saved, err = s.service.SaveFilePart(ctx, req.GetOwnerUserId(), req.GetFileId(), int(req.GetFilePart()), req.GetData())
	}
	if err != nil {
		return &filedatapb.SaveFilePartResponse{Error: err.Error(), ErrorKind: errorKind(err)}, nil
	}
	return &filedatapb.SaveFilePartResponse{Saved: saved}, nil
}

func (s *grpcServer) GetFile(ctx context.Context, req *filedatapb.GetFileRequest) (*filedatapb.GetFileResponse, error) {
	if s == nil || s.service == nil {
		return nil, status.Error(codes.Internal, ErrMissingDependency.Error())
	}
	chunk, found, err := s.service.GetFile(ctx, domain.FileDownloadRequest{
		LocationKey: req.GetLocationKey(),
		Offset:      req.GetOffset(),
		Limit:       int(req.GetLimit()),
	})
	if err != nil {
		return &filedatapb.GetFileResponse{Error: err.Error(), ErrorKind: errorKind(err)}, nil
	}
	res := &filedatapb.GetFileResponse{
		Found:    found,
		Data:     readOnlyPBBytes(chunk.Bytes),
		MimeType: chunk.MimeType,
		Total:    chunk.Total,
	}
	if source := chunk.ImmutableRange; source != nil {
		res.ImmutableBackend = string(source.Backend)
		res.ImmutableObjectKey = source.ObjectKey
		res.ImmutableOffset = source.Offset
		res.ImmutableLength = int32(source.Length)
		res.ImmutableRangeSha256 = append([]byte(nil), source.RangeSHA256[:]...)
	}
	return res, nil
}

func (s *grpcServer) GetFileHashes(ctx context.Context, req *filedatapb.GetFileHashesRequest) (*filedatapb.GetFileHashesResponse, error) {
	if s == nil || s.service == nil {
		return nil, status.Error(codes.Internal, ErrMissingDependency.Error())
	}
	hashes, found, err := s.service.GetFileHashes(ctx, domain.FileHashRequest{
		LocationKey: req.GetLocationKey(),
		Offset:      req.GetOffset(),
	})
	if err != nil {
		return &filedatapb.GetFileHashesResponse{Error: err.Error(), ErrorKind: errorKind(err)}, nil
	}
	res := &filedatapb.GetFileHashesResponse{Found: found, Hashes: make([]*filedatapb.FileHash, 0, len(hashes))}
	for _, hash := range hashes {
		res.Hashes = append(res.Hashes, &filedatapb.FileHash{
			Offset: hash.Offset,
			Limit:  int32(hash.Limit),
			Hash:   hash.Hash,
		})
	}
	return res, nil
}

func (s *grpcServer) MaterializeUploadBlob(ctx context.Context, req *filedatapb.MaterializeUploadBlobRequest) (*filedatapb.MaterializeUploadBlobResponse, error) {
	if s == nil || s.service == nil {
		return nil, status.Error(codes.Internal, ErrMissingDependency.Error())
	}
	blob, err := s.service.AssembleUploadBlob(ctx, req.GetOwnerUserId(), req.GetFileId(), int(req.GetExpectedParts()))
	if err != nil {
		return &filedatapb.MaterializeUploadBlobResponse{Error: err.Error(), ErrorKind: errorKind(err)}, nil
	}
	return &filedatapb.MaterializeUploadBlobResponse{
		ObjectKey: blob.ObjectKey,
		Size:      blob.Size,
		Sha256:    blob.SHA256,
	}, nil
}

func (s *grpcServer) PutBlob(stream grpc.ClientStreamingServer[filedatapb.PutBlobChunk, filedatapb.BlobObjectResponse]) error {
	if s == nil || s.blobs == nil {
		return status.Error(codes.Internal, ErrMissingDependency.Error())
	}
	key, size, sum, err := s.blobs.PutReader(stream.Context(), &putBlobStreamReader{stream: stream})
	if err != nil {
		return stream.SendAndClose(&filedatapb.BlobObjectResponse{Error: err.Error(), ErrorKind: errorKind(err)})
	}
	return stream.SendAndClose(&filedatapb.BlobObjectResponse{ObjectKey: key, Size: size, Sha256: sum})
}

type putBlobStreamReader struct {
	stream  grpc.ClientStreamingServer[filedatapb.PutBlobChunk, filedatapb.BlobObjectResponse]
	pending []byte
}

func (r *putBlobStreamReader) Read(dst []byte) (int, error) {
	if len(dst) == 0 {
		return 0, nil
	}
	for len(r.pending) == 0 {
		chunk, err := r.stream.Recv()
		if err != nil {
			return 0, err
		}
		r.pending = chunk.Data
		chunk.Data = nil
	}
	n := copy(dst, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

func (s *grpcServer) GetBlobRange(ctx context.Context, req *filedatapb.GetBlobRangeRequest) (*filedatapb.GetBlobRangeResponse, error) {
	if s == nil || s.blobs == nil {
		return nil, status.Error(codes.Internal, ErrMissingDependency.Error())
	}
	data, total, err := s.blobs.GetRange(ctx, req.GetObjectKey(), req.GetOffset(), req.GetLimit())
	if err != nil {
		return &filedatapb.GetBlobRangeResponse{Error: err.Error(), ErrorKind: errorKind(err)}, nil
	}
	return &filedatapb.GetBlobRangeResponse{Data: readOnlyPBBytes(data), Total: total}, nil
}

func (s *grpcServer) PutUploadPart(ctx context.Context, req *filedatapb.PutUploadPartRequest) (*filedatapb.UploadPartObjectResponse, error) {
	if s == nil || s.uploadParts == nil {
		return nil, status.Error(codes.Internal, ErrMissingDependency.Error())
	}
	obj, err := s.uploadParts.PutUploadPart(ctx, req.GetOwnerUserId(), req.GetFileId(), int(req.GetFilePart()), req.GetData())
	if err != nil {
		return &filedatapb.UploadPartObjectResponse{Error: err.Error(), ErrorKind: errorKind(err)}, nil
	}
	return &filedatapb.UploadPartObjectResponse{
		Backend:   string(obj.Backend),
		ObjectKey: obj.ObjectKey,
		Size:      obj.Size,
		Sha256:    obj.SHA256,
	}, nil
}

func (s *grpcServer) GetUploadPart(ctx context.Context, req *filedatapb.GetUploadPartRequest) (*filedatapb.GetUploadPartResponse, error) {
	if s == nil || s.uploadParts == nil {
		return nil, status.Error(codes.Internal, ErrMissingDependency.Error())
	}
	data, err := s.uploadParts.GetUploadPart(ctx, req.GetObjectKey())
	if err != nil {
		return &filedatapb.GetUploadPartResponse{Error: err.Error(), ErrorKind: errorKind(err)}, nil
	}
	return &filedatapb.GetUploadPartResponse{Data: readOnlyPBBytes(data)}, nil
}

// readOnlyPBBytes lets the gRPC marshaler borrow immutable service/backend
// storage without a full payload copy. The response must never mutate it, and
// capacity clipping prevents accidental append into an unexposed shared tail.
func readOnlyPBBytes(data []byte) []byte {
	return data[:len(data):len(data)]
}

func (s *grpcServer) DeleteUploadPart(ctx context.Context, req *filedatapb.DeleteUploadPartRequest) (*filedatapb.ErrorResponse, error) {
	if s == nil || s.uploadParts == nil {
		return nil, status.Error(codes.Internal, ErrMissingDependency.Error())
	}
	if err := s.uploadParts.DeleteUploadPart(ctx, req.GetObjectKey()); err != nil {
		return &filedatapb.ErrorResponse{Error: err.Error(), ErrorKind: errorKind(err)}, nil
	}
	return &filedatapb.ErrorResponse{}, nil
}

func (s *grpcServer) DeleteExpiredUploadParts(ctx context.Context, req *filedatapb.DeleteExpiredUploadPartsRequest) (*filedatapb.DeleteExpiredUploadPartsResponse, error) {
	if s == nil || s.service == nil {
		return nil, status.Error(codes.Internal, ErrMissingDependency.Error())
	}
	deleted, err := s.service.DeleteExpiredUploadParts(ctx, time.Unix(0, req.GetBeforeUnixNano()), int(req.GetLimit()))
	if err != nil {
		return &filedatapb.DeleteExpiredUploadPartsResponse{Deleted: deleted, Error: err.Error(), ErrorKind: errorKind(err)}, nil
	}
	return &filedatapb.DeleteExpiredUploadPartsResponse{Deleted: deleted}, nil
}
