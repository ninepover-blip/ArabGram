package egress

import (
	"context"
	"errors"
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

	"telesrv/internal/edgecontrol"
	"telesrv/internal/egress/egresspb"
	"telesrv/internal/store"
)

const (
	defaultGRPCDeliveryClientTimeout  = 5 * time.Second
	defaultMaxGRPCDeliveryMessageSize = 1 << 20
	maxGRPCDeliveryBatchSize          = 256
	grpcDeliveryProtocolVersion       = 3
	grpcDeliveryMinSupportedVersion   = 3
)

var (
	ErrGRPCDeliveryAddrMissing      = errors.New("egress delivery grpc: addr is empty")
	ErrGRPCDeliveryTargetsMissing   = errors.New("egress delivery grpc: targets are empty")
	ErrGRPCDeliveryTokenMissing     = errors.New("egress delivery grpc: bearer token is required")
	ErrGRPCDeliveryUnavailable      = errors.New("egress delivery grpc: unavailable")
	ErrGRPCDeliveryProtocolMismatch = errors.New("egress delivery grpc: protocol version mismatch")
	ErrGRPCDeliveryInvalidDelivery  = errors.New("egress delivery grpc: invalid evidence")
	grpcDeliveryCapabilities        = []string{
		"physical-receipt-batch",
		"client-ack-observation",
		"explicit-ordering-domain",
		"fenced-delivery-ref",
	}
)

// GRPCDeliveryServerConfig exposes only the v3 physical-write and diagnostic
// client-ACK evidence service. No legacy RPC is registered on the listener.
type GRPCDeliveryServerConfig struct {
	Addr            string
	InstanceID      string
	Token           string
	TLSCertFile     string
	TLSKeyFile      string
	TLSClientCAFile string
	Store           store.DispatchOutboxStore
	DeliveryStore   store.DeliveryOutboxStore
	ChannelStore    store.ChannelDeliveryStore
	Mutations       AttemptMutationWriter
	ClientAcks      ClientAckObservationSink
	Logger          *zap.Logger
	MaxRecvMsgBytes int
	MaxSendMsgBytes int
}

type GRPCDeliveryClientConfig struct {
	Targets         []string
	Resolver        GRPCDeliveryResolverProvider
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

type GRPCDeliveryRemote struct {
	client         egresspb.EgressDeliveryServiceClient
	health         healthpb.HealthClient
	requestTimeout time.Duration
	log            *zap.Logger
}

type egressDeliveryGRPCServer struct {
	egresspb.UnimplementedEgressDeliveryServiceServer
	stores     deliveryEvidenceStores
	instanceID string
}

func StartGRPCDelivery(ctx context.Context, cfg GRPCDeliveryServerConfig) (*grpc.Server, error) {
	if strings.TrimSpace(cfg.Addr) == "" {
		return nil, ErrGRPCDeliveryAddrMissing
	}
	if strings.TrimSpace(cfg.Token) == "" {
		return nil, ErrGRPCDeliveryTokenMissing
	}
	if cfg.Store == nil || cfg.DeliveryStore == nil || cfg.ChannelStore == nil || cfg.Mutations == nil || cfg.ClientAcks == nil {
		return nil, ErrMissingDependency
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	transportCreds, tlsEnabled, err := grpcDeliveryServerTransportCredentials(cfg)
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("listen egress delivery grpc %s: %w", cfg.Addr, err)
	}
	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(grpcDeliveryBearerUnaryServerInterceptor(cfg.Token)),
		grpc.MaxRecvMsgSize(grpcDeliveryMessageSize(cfg.MaxRecvMsgBytes)),
		grpc.MaxSendMsgSize(grpcDeliveryMessageSize(cfg.MaxSendMsgBytes)),
	}
	if tlsEnabled {
		opts = append(opts, grpc.Creds(transportCreds))
	}
	srv := grpc.NewServer(opts...)
	egresspb.RegisterEgressDeliveryServiceServer(srv, newEgressDeliveryGRPCServer(
		cfg.Store, cfg.DeliveryStore, cfg.ChannelStore, cfg.Mutations, cfg.ClientAcks, cfg.InstanceID,
	))
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus(egresspb.EgressDeliveryService_ServiceDesc.ServiceName, healthpb.HealthCheckResponse_SERVING)
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
		cfg.Logger.Info("egress delivery grpc listening", zap.String("addr", ln.Addr().String()))
		if err := srv.Serve(ln); err != nil {
			cfg.Logger.Warn("egress delivery grpc exited", zap.Error(err))
		}
	}()
	return srv, nil
}

func DialGRPCDeliveryRemote(ctx context.Context, cfg GRPCDeliveryClientConfig) (*GRPCDeliveryRemote, *grpc.ClientConn, error) {
	if strings.TrimSpace(cfg.Token) == "" {
		return nil, nil, ErrGRPCDeliveryTokenMissing
	}
	provider, err := grpcDeliveryResolverProvider(cfg)
	if err != nil {
		return nil, nil, err
	}
	target := strings.TrimSpace(provider.Target())
	if target == "" {
		return nil, nil, ErrGRPCDeliveryTargetsMissing
	}
	opts, err := grpcDeliveryClientDialOptions(cfg, provider)
	if err != nil {
		return nil, nil, err
	}
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("egress delivery grpc client: %w", err)
	}
	remote := &GRPCDeliveryRemote{
		client:         egresspb.NewEgressDeliveryServiceClient(conn),
		health:         healthpb.NewHealthClient(conn),
		requestTimeout: grpcDeliveryClientRequestTimeout(cfg.RequestTimeout),
		log:            cfg.Logger,
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

func (r *GRPCDeliveryRemote) Check(ctx context.Context) error {
	if r == nil || r.client == nil {
		return ErrGRPCDeliveryUnavailable
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	if r.health != nil {
		res, err := r.health.Check(callCtx, &healthpb.HealthCheckRequest{Service: egresspb.EgressDeliveryService_ServiceDesc.ServiceName})
		if err != nil {
			return fmt.Errorf("egress delivery grpc health: %w", err)
		}
		if res.GetStatus() != healthpb.HealthCheckResponse_SERVING {
			return fmt.Errorf("egress delivery grpc health: status %s", res.GetStatus().String())
		}
	}
	info, err := r.client.GetInfo(callCtx, &egresspb.EgressInfoRequest{
		ProtocolVersion: grpcDeliveryProtocolVersion, MinSupportedProtocolVersion: grpcDeliveryMinSupportedVersion,
		Capabilities: append([]string(nil), grpcDeliveryCapabilities...),
	})
	if err != nil {
		return fmt.Errorf("egress delivery grpc info: %w", err)
	}
	if info.GetError() != "" || !grpcDeliveryProtocolRangesOverlap(
		grpcDeliveryMinSupportedVersion, grpcDeliveryProtocolVersion,
		info.GetMinSupportedProtocolVersion(), info.GetProtocolVersion(),
	) || !grpcDeliveryHasCapabilities(info.GetCapabilities(), grpcDeliveryCapabilities) {
		return fmt.Errorf("%w: remote=%d..%d capabilities=%v error=%q",
			ErrGRPCDeliveryProtocolMismatch, info.GetMinSupportedProtocolVersion(), info.GetProtocolVersion(), info.GetCapabilities(), info.GetError())
	}
	return nil
}

func (r *GRPCDeliveryRemote) ReportPhysicalReceipts(ctx context.Context, receipts []edgecontrol.PhysicalReceipt) ([]edgecontrol.PhysicalReceiptResult, error) {
	if r == nil || r.client == nil {
		return nil, ErrGRPCDeliveryUnavailable
	}
	req, err := physicalReceiptBatchRequest(receipts)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	res, err := r.client.ReportPhysicalReceipts(callCtx, req)
	if err != nil {
		return nil, fmt.Errorf("egress delivery grpc report physical receipts: %w", err)
	}
	return physicalReceiptResultsFromPB(res, len(receipts))
}

func (r *GRPCDeliveryRemote) ReportClientAcks(ctx context.Context, observations []edgecontrol.ClientAckObservation) ([]edgecontrol.ClientAckObservationResult, error) {
	if r == nil || r.client == nil {
		return nil, ErrGRPCDeliveryUnavailable
	}
	req, err := clientAckBatchRequest(observations)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := r.withRequestTimeout(ctx)
	defer cancel()
	res, err := r.client.ReportClientAcks(callCtx, req)
	if err != nil {
		return nil, fmt.Errorf("egress delivery grpc report client acks: %w", err)
	}
	return clientAckResultsFromPB(res, len(observations))
}

func newEgressDeliveryGRPCServer(
	dispatch store.DispatchOutboxStore,
	absolute store.DeliveryOutboxStore,
	channel store.ChannelDeliveryStore,
	mutations AttemptMutationWriter,
	clientAcks ClientAckObservationSink,
	instanceID string,
) *egressDeliveryGRPCServer {
	return &egressDeliveryGRPCServer{
		stores:     deliveryEvidenceStores{dispatch: dispatch, absolute: absolute, channel: channel, mutations: mutations, clientAcks: clientAcks},
		instanceID: strings.TrimSpace(instanceID),
	}
}

func (s *egressDeliveryGRPCServer) GetInfo(_ context.Context, req *egresspb.EgressInfoRequest) (*egresspb.EgressInfoResponse, error) {
	res := &egresspb.EgressInfoResponse{
		ProtocolVersion: grpcDeliveryProtocolVersion, MinSupportedProtocolVersion: grpcDeliveryMinSupportedVersion,
		Capabilities:   append([]string(nil), grpcDeliveryCapabilities...),
		Implementation: "telesrv-egress-delivery-v3", InstanceId: s.instanceID,
	}
	if req == nil || !grpcDeliveryProtocolRangesOverlap(
		req.GetMinSupportedProtocolVersion(), req.GetProtocolVersion(),
		grpcDeliveryMinSupportedVersion, grpcDeliveryProtocolVersion,
	) {
		res.Error = fmt.Sprintf("incompatible protocol peer=%d..%d egress=%d..%d",
			req.GetMinSupportedProtocolVersion(), req.GetProtocolVersion(), grpcDeliveryMinSupportedVersion, grpcDeliveryProtocolVersion)
	} else if !grpcDeliveryHasCapabilities(req.GetCapabilities(), grpcDeliveryCapabilities) {
		res.Error = fmt.Sprintf("missing required capabilities peer=%v egress=%v", req.GetCapabilities(), grpcDeliveryCapabilities)
	}
	return res, nil
}

func (s *egressDeliveryGRPCServer) ReportPhysicalReceipts(ctx context.Context, req *egresspb.PhysicalReceiptBatchRequest) (*egresspb.DeliveryEvidenceBatchResponse, error) {
	if s == nil || s.stores.dispatch == nil || s.stores.absolute == nil || s.stores.channel == nil {
		return nil, status.Error(codes.Internal, ErrMissingDependency.Error())
	}
	count, err := physicalReceiptBatchSize(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	response := newEvidenceBatchResponse(count)
	indices := make([]int, 0, count)
	receipts := make([]edgecontrol.PhysicalReceipt, 0, count)
	for i := 0; i < count; i++ {
		receipt, itemErr := physicalReceiptFromPB(req, i)
		if itemErr != nil {
			setEvidenceResult(response, i, edgecontrol.PhysicalReceiptResult{Outcome: edgecontrol.PhysicalReceiptRejected}, itemErr.Error())
			continue
		}
		indices = append(indices, i)
		receipts = append(receipts, receipt)
	}
	results := applyPhysicalReceiptBatch(ctx, s.stores, receipts)
	for i, result := range results {
		setEvidenceResult(response, indices[i], result, "")
	}
	return response, nil
}

func (s *egressDeliveryGRPCServer) ReportClientAcks(ctx context.Context, req *egresspb.ClientAckBatchRequest) (*egresspb.DeliveryEvidenceBatchResponse, error) {
	if s == nil || s.stores.dispatch == nil || s.stores.absolute == nil || s.stores.channel == nil {
		return nil, status.Error(codes.Internal, ErrMissingDependency.Error())
	}
	count, err := clientAckBatchSize(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	response := newEvidenceBatchResponse(count)
	indices := make([]int, 0, count)
	observations := make([]edgecontrol.ClientAckObservation, 0, count)
	for i := 0; i < count; i++ {
		observation, itemErr := clientAckFromPB(req, i)
		if itemErr != nil {
			setClientAckEvidenceResult(response, i, edgecontrol.ClientAckObservationResult{Outcome: edgecontrol.ClientAckObservationRejected}, itemErr.Error())
			continue
		}
		indices = append(indices, i)
		observations = append(observations, observation)
	}
	results := applyClientAckBatch(ctx, s.stores, observations)
	for i, result := range results {
		setClientAckEvidenceResult(response, indices[i], result, "")
	}
	return response, nil
}

func newEvidenceBatchResponse(count int) *egresspb.DeliveryEvidenceBatchResponse {
	return &egresspb.DeliveryEvidenceBatchResponse{Outcomes: make([]egresspb.DeliveryEvidenceOutcome, count)}
}

func setEvidenceResult(response *egresspb.DeliveryEvidenceBatchResponse, index int, result edgecontrol.PhysicalReceiptResult, detail string) {
	switch result.Outcome {
	case edgecontrol.PhysicalReceiptApplied:
		response.Outcomes[index] = egresspb.DeliveryEvidenceOutcome_DELIVERY_EVIDENCE_OUTCOME_RECORDED
	case edgecontrol.PhysicalReceiptStale:
		response.Outcomes[index] = egresspb.DeliveryEvidenceOutcome_DELIVERY_EVIDENCE_OUTCOME_FENCED
	case edgecontrol.PhysicalReceiptRejected:
		response.Outcomes[index] = egresspb.DeliveryEvidenceOutcome_DELIVERY_EVIDENCE_OUTCOME_REJECTED
	default:
		response.Outcomes[index] = egresspb.DeliveryEvidenceOutcome_DELIVERY_EVIDENCE_OUTCOME_RETRYABLE
	}
	if detail != "" {
		if response.Details == nil {
			response.Details = make(map[uint32]string)
		}
		response.Details[uint32(index)] = detail
	}
}

func setClientAckEvidenceResult(response *egresspb.DeliveryEvidenceBatchResponse, index int, result edgecontrol.ClientAckObservationResult, detail string) {
	switch result.Outcome {
	case edgecontrol.ClientAckObservationApplied:
		response.Outcomes[index] = egresspb.DeliveryEvidenceOutcome_DELIVERY_EVIDENCE_OUTCOME_RECORDED
	case edgecontrol.ClientAckObservationStale:
		response.Outcomes[index] = egresspb.DeliveryEvidenceOutcome_DELIVERY_EVIDENCE_OUTCOME_FENCED
	case edgecontrol.ClientAckObservationRejected:
		response.Outcomes[index] = egresspb.DeliveryEvidenceOutcome_DELIVERY_EVIDENCE_OUTCOME_REJECTED
	default:
		response.Outcomes[index] = egresspb.DeliveryEvidenceOutcome_DELIVERY_EVIDENCE_OUTCOME_RETRYABLE
	}
	if detail != "" {
		if response.Details == nil {
			response.Details = make(map[uint32]string)
		}
		response.Details[uint32(index)] = detail
	}
}

func grpcDeliveryProtocolRangesOverlap(aMin, aMax, bMin, bMax uint32) bool {
	return aMin != 0 && aMax != 0 && bMin != 0 && bMax != 0 && aMin <= bMax && bMin <= aMax
}

func grpcDeliveryHasCapabilities(got, required []string) bool {
	available := make(map[string]struct{}, len(got))
	for _, capability := range got {
		available[capability] = struct{}{}
	}
	for _, capability := range required {
		if _, ok := available[capability]; !ok {
			return false
		}
	}
	return true
}

func grpcDeliveryClientRequestTimeout(value time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	return defaultGRPCDeliveryClientTimeout
}

func (r *GRPCDeliveryRemote) withRequestTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, grpcDeliveryClientRequestTimeout(r.requestTimeout))
}
