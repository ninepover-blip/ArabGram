package filedata

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	filesapp "telesrv/internal/app/files"
	"telesrv/internal/domain"
	"telesrv/internal/filedata/filedatapb"
)

const (
	defaultGRPCClientTimeout = 10 * time.Second
	defaultGRPCMessageSize   = 4 << 20
	defaultGRPCPutBlobChunk  = 256 << 10
	grpcProtocolVersion      = 1
	grpcMinSupportedVersion  = 1
	grpcResolverStatic       = "static"
	grpcResolverDNS          = "dns"
	staticResolverScheme     = "filedata-static"
	staticResolverTarget     = staticResolverScheme + ":///filedata"
	dnsResolverScheme        = "dns"
)

var (
	ErrGRPCAddrMissing      = errors.New("filedata grpc: addr is empty")
	ErrGRPCTargetsMissing   = errors.New("filedata grpc: targets are empty")
	ErrGRPCTokenMissing     = errors.New("filedata grpc: bearer token is required")
	ErrGRPCUnavailable      = errors.New("filedata grpc: unavailable")
	ErrGRPCProtocolMismatch = errors.New("filedata grpc: protocol version mismatch")
	ErrMissingDependency    = errors.New("filedata grpc: missing dependency")
	ErrBackendMissing       = errors.New("filedata grpc: backend is empty")
	grpcCapabilities        = []string{"blob-range", "upload-parts", "materialize-upload-blob", "file-hashes", "immutable-file-range"}
)

type DataService interface {
	// SaveFilePart and SaveBigFilePart borrow data as read-only for the duration of the call.
	SaveFilePart(ctx context.Context, ownerUserID, fileID int64, part int, data []byte) (bool, error)
	SaveBigFilePart(ctx context.Context, ownerUserID, fileID int64, part, totalParts int, data []byte) (bool, error)
	// GetFile transfers ownership of FileChunk.Bytes to the caller.
	GetFile(ctx context.Context, req domain.FileDownloadRequest) (domain.FileChunk, bool, error)
	// GetFileHashes transfers ownership of every FileHash.Hash to the caller.
	GetFileHashes(ctx context.Context, req domain.FileHashRequest) ([]domain.FileHash, bool, error)
	// AssembleUploadBlob transfers ownership of AssembledUploadBlob.SHA256.
	AssembleUploadBlob(ctx context.Context, ownerUserID, fileID int64, expectedParts int) (filesapp.AssembledUploadBlob, error)
	DeleteExpiredUploadParts(ctx context.Context, before time.Time, limit int) (int64, error)
}

type GRPCServerConfig struct {
	Addr            string
	InstanceID      string
	Token           string
	TLSCertFile     string
	TLSKeyFile      string
	TLSClientCAFile string
	Service         DataService
	BlobBackend     filesapp.BlobBackend
	UploadParts     filesapp.UploadPartBackend
	Logger          *zap.Logger
	MaxRecvMsgBytes int
	MaxSendMsgBytes int
}

type GRPCClientConfig struct {
	Targets         []string
	Resolver        GRPCResolverProvider
	ResolverKind    string
	Token           string
	Logger          *zap.Logger
	RequestTimeout  time.Duration
	TLSCAFile       string
	TLSServerName   string
	TLSCertFile     string
	TLSKeyFile      string
	MaxRecvMsgBytes int
	MaxSendMsgBytes int
	DialOptions     []grpc.DialOption
}

type GRPCRemote struct {
	client         filedatapb.FileDataServiceClient
	health         healthpb.HealthClient
	requestTimeout time.Duration
	log            *zap.Logger
	backend        string
	token          string
}

type grpcServer struct {
	filedatapb.UnimplementedFileDataServiceServer

	service     DataService
	blobs       filesapp.BlobBackend
	uploadParts filesapp.UploadPartBackend
	instanceID  string
}

func messageSize(n int) int {
	if n <= 0 {
		return defaultGRPCMessageSize
	}
	return n
}

func protocolRangesOverlap(aMin, aMax, bMin, bMax uint32) bool {
	return aMin <= bMax && bMin <= aMax
}
