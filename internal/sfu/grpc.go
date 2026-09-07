package sfu

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"telesrv/internal/sfu/sfupb"
)

const (
	defaultGRPCControlClientTimeout  = 5 * time.Second
	defaultMaxGRPCControlMessageSize = 4 << 20
	sfuGRPCControlProtocolVersion    = 1
	sfuGRPCControlMinVersion         = 1
)

var (
	ErrGRPCControlAddrMissing  = errors.New("sfu grpc control: addr is empty")
	ErrGRPCControlTokenMissing = errors.New("sfu grpc control: bearer token is required")
	ErrGRPCControlUnavailable  = errors.New("sfu grpc control: unavailable")
)

var sfuGRPCControlCapabilities = []string{
	"sfu-control",
	"owner-health",
	"instance-info",
}

type GRPCControlServerConfig struct {
	Addr            string
	InstanceID      string
	Token           string
	TLSCertFile     string
	TLSKeyFile      string
	TLSClientCAFile string
	Service         Service
	Health          func(context.Context) error
	Logger          *zap.Logger
	MaxRecvMsgBytes int
	MaxSendMsgBytes int
}

type GRPCRemoteConfig struct {
	Token           string
	RequestTimeout  time.Duration
	TLSCAFile       string
	TLSServerName   string
	TLSCertFile     string
	TLSKeyFile      string
	MaxRecvMsgBytes int
	MaxSendMsgBytes int
	DialOptions     []grpc.DialOption
}

type GRPCRemoteService struct {
	token          string
	requestTimeout time.Duration
	dialOptions    []grpc.DialOption

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

type sfuGRPCControlServer struct {
	sfupb.UnimplementedSFUControlServiceServer

	service    Service
	instanceID string
}

func StartGRPCControl(ctx context.Context, cfg GRPCControlServerConfig) (*grpc.Server, error) {
	if strings.TrimSpace(cfg.Addr) == "" {
		return nil, ErrGRPCControlAddrMissing
	}
	if strings.TrimSpace(cfg.Token) == "" {
		return nil, ErrGRPCControlTokenMissing
	}
	if cfg.Service == nil {
		return nil, errors.New("sfu grpc control: service is required")
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	transportCreds, tlsEnabled, err := sfuGRPCServerTransportCredentials(cfg)
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("listen sfu grpc control %s: %w", cfg.Addr, err)
	}
	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(sfuGRPCBearerUnaryServerInterceptor(cfg.Token)),
		grpc.MaxRecvMsgSize(sfuGRPCMessageSize(cfg.MaxRecvMsgBytes)),
		grpc.MaxSendMsgSize(sfuGRPCMessageSize(cfg.MaxSendMsgBytes)),
	}
	if tlsEnabled {
		opts = append(opts, grpc.Creds(transportCreds))
	}
	srv := grpc.NewServer(opts...)
	sfupb.RegisterSFUControlServiceServer(srv, &sfuGRPCControlServer{
		service:    cfg.Service,
		instanceID: strings.TrimSpace(cfg.InstanceID),
	})
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus(sfupb.SFUControlService_ServiceDesc.ServiceName, sfuGRPCHealthStatus(ctx, cfg.Health))
	healthpb.RegisterHealthServer(srv, healthSrv)
	if cfg.Health != nil {
		go sfuGRPCWatchHealth(ctx, cfg.Health, healthSrv)
	}
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
		cfg.Logger.Info("sfu grpc control listening", zap.String("addr", ln.Addr().String()))
		if err := srv.Serve(ln); err != nil {
			cfg.Logger.Warn("sfu grpc control exited", zap.Error(err))
		}
	}()
	return srv, nil
}

func NewGRPCRemoteService(cfg GRPCRemoteConfig) (*GRPCRemoteService, error) {
	if strings.TrimSpace(cfg.Token) == "" {
		return nil, ErrGRPCControlTokenMissing
	}
	timeout := cfg.RequestTimeout
	if timeout <= 0 {
		timeout = defaultGRPCControlClientTimeout
	}
	transportCreds, err := sfuGRPCClientTransportCredentials(cfg)
	if err != nil {
		return nil, err
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(sfuGRPCMessageSize(cfg.MaxRecvMsgBytes)),
			grpc.MaxCallSendMsgSize(sfuGRPCMessageSize(cfg.MaxSendMsgBytes)),
		),
	}
	opts = append(opts, cfg.DialOptions...)
	return &GRPCRemoteService{
		token:          strings.TrimSpace(cfg.Token),
		requestTimeout: timeout,
		dialOptions:    opts,
		conns:          make(map[string]*grpc.ClientConn),
	}, nil
}

func (s *GRPCRemoteService) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var first error
	for target, conn := range s.conns {
		if err := conn.Close(); err != nil && first == nil {
			first = fmt.Errorf("close sfu grpc control %s: %w", target, err)
		}
		delete(s.conns, target)
	}
	return first
}

func (s *GRPCRemoteService) Join(ctx context.Context, owner OwnerRecord, callID, userID int64, kind EndpointKind, offer ClientOffer) (ServerAnswer, error) {
	client, _, err := s.clientFor(owner.ControlAddr)
	if err != nil {
		return ServerAnswer{}, err
	}
	callCtx, cancel := s.withRequestTimeout(ctx)
	defer cancel()
	res, err := client.Join(s.withAuth(callCtx), &sfupb.JoinRequest{
		CallId: callID,
		UserId: userID,
		Kind:   endpointKindToPB(kind),
		Offer:  clientOfferToPB(offer),
	})
	if err != nil {
		return ServerAnswer{}, fmt.Errorf("sfu grpc join: %w", err)
	}
	if err := sfuGRPCError(res.GetError()); err != nil {
		return ServerAnswer{}, err
	}
	return serverAnswerFromPB(res.GetAnswer()), nil
}

func (s *GRPCRemoteService) Leave(ctx context.Context, owner OwnerRecord, callID, userID int64, kind EndpointKind) error {
	client, _, err := s.clientFor(owner.ControlAddr)
	if err != nil {
		return err
	}
	callCtx, cancel := s.withRequestTimeout(ctx)
	defer cancel()
	res, err := client.Leave(s.withAuth(callCtx), &sfupb.LeaveRequest{
		CallId: callID,
		UserId: userID,
		Kind:   endpointKindToPB(kind),
	})
	if err != nil {
		return fmt.Errorf("sfu grpc leave: %w", err)
	}
	return sfuGRPCError(res.GetError())
}

func (s *GRPCRemoteService) CloseRoom(ctx context.Context, owner OwnerRecord, callID int64) error {
	client, _, err := s.clientFor(owner.ControlAddr)
	if err != nil {
		return err
	}
	callCtx, cancel := s.withRequestTimeout(ctx)
	defer cancel()
	res, err := client.CloseRoom(s.withAuth(callCtx), &sfupb.CloseRoomRequest{CallId: callID})
	if err != nil {
		return fmt.Errorf("sfu grpc close_room: %w", err)
	}
	return sfuGRPCError(res.GetError())
}

func (s *GRPCRemoteService) AliveUserIDs(ctx context.Context, owner OwnerRecord, callID int64) ([]int64, error) {
	client, _, err := s.clientFor(owner.ControlAddr)
	if err != nil {
		return nil, err
	}
	callCtx, cancel := s.withRequestTimeout(ctx)
	defer cancel()
	res, err := client.Alive(s.withAuth(callCtx), &sfupb.AliveRequest{CallId: callID})
	if err != nil {
		return nil, fmt.Errorf("sfu grpc alive: %w", err)
	}
	if err := sfuGRPCError(res.GetError()); err != nil {
		return nil, err
	}
	return append([]int64(nil), res.GetUserIds()...), nil
}

func (s *GRPCRemoteService) CheckInstance(ctx context.Context, record InstanceRecord) error {
	client, conn, err := s.clientFor(record.ControlAddr)
	if err != nil {
		return err
	}
	callCtx, cancel := s.withRequestTimeout(ctx)
	defer cancel()
	healthClient := healthpb.NewHealthClient(conn)
	healthRes, err := healthClient.Check(s.withAuth(callCtx), &healthpb.HealthCheckRequest{
		Service: sfupb.SFUControlService_ServiceDesc.ServiceName,
	})
	if err != nil {
		return fmt.Errorf("sfu grpc health: %w", err)
	}
	if healthRes.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("sfu grpc health: status %s", healthRes.GetStatus().String())
	}
	info, err := client.GetInfo(s.withAuth(callCtx), &sfupb.SFUInfoRequest{
		ProtocolVersion:             sfuGRPCControlProtocolVersion,
		MinSupportedProtocolVersion: sfuGRPCControlMinVersion,
		Capabilities:                append([]string(nil), sfuGRPCControlCapabilities...),
	})
	if err != nil {
		return fmt.Errorf("sfu grpc info: %w", err)
	}
	if err := sfuGRPCError(info.GetError()); err != nil {
		return err
	}
	if info.GetProtocolVersion() < sfuGRPCControlMinVersion || info.GetMinSupportedProtocolVersion() > sfuGRPCControlProtocolVersion {
		return fmt.Errorf("sfu grpc info: incompatible protocol version server=%d min=%d", info.GetProtocolVersion(), info.GetMinSupportedProtocolVersion())
	}
	if record.InstanceID != "" && info.GetInstanceId() != "" && info.GetInstanceId() != record.InstanceID {
		return fmt.Errorf("sfu grpc info: instance_id mismatch record=%s server=%s", record.InstanceID, info.GetInstanceId())
	}
	return nil
}

func (s *GRPCRemoteService) clientFor(controlAddr string) (sfupb.SFUControlServiceClient, *grpc.ClientConn, error) {
	if s == nil {
		return nil, nil, ErrGRPCControlUnavailable
	}
	target, err := grpcControlTarget(controlAddr)
	if err != nil {
		return nil, nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	conn := s.conns[target]
	if conn == nil {
		conn, err = grpc.NewClient(target, s.dialOptions...)
		if err != nil {
			return nil, nil, fmt.Errorf("sfu grpc client %s: %w", target, err)
		}
		s.conns[target] = conn
	}
	return sfupb.NewSFUControlServiceClient(conn), conn, nil
}

func (s *GRPCRemoteService) withRequestTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok || s == nil || s.requestTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, s.requestTimeout)
}

func (s *GRPCRemoteService) withAuth(ctx context.Context) context.Context {
	if s == nil {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+s.token)
}

func (s *sfuGRPCControlServer) GetInfo(context.Context, *sfupb.SFUInfoRequest) (*sfupb.SFUInfoResponse, error) {
	return &sfupb.SFUInfoResponse{
		ProtocolVersion:             sfuGRPCControlProtocolVersion,
		MinSupportedProtocolVersion: sfuGRPCControlMinVersion,
		Capabilities:                append([]string(nil), sfuGRPCControlCapabilities...),
		Implementation:              "telesrv-sfu",
		InstanceId:                  s.instanceID,
	}, nil
}

func (s *sfuGRPCControlServer) Join(ctx context.Context, req *sfupb.JoinRequest) (*sfupb.JoinResponse, error) {
	answer, err := s.service.Join(ctx, req.GetCallId(), req.GetUserId(), endpointKindFromPB(req.GetKind()), clientOfferFromPB(req.GetOffer()))
	if err != nil {
		return &sfupb.JoinResponse{Error: err.Error()}, nil
	}
	return &sfupb.JoinResponse{Answer: serverAnswerToPB(answer)}, nil
}

func (s *sfuGRPCControlServer) Leave(ctx context.Context, req *sfupb.LeaveRequest) (*sfupb.ErrorResponse, error) {
	if err := s.service.Leave(ctx, req.GetCallId(), req.GetUserId(), endpointKindFromPB(req.GetKind())); err != nil {
		return &sfupb.ErrorResponse{Error: err.Error()}, nil
	}
	return &sfupb.ErrorResponse{}, nil
}

func (s *sfuGRPCControlServer) CloseRoom(ctx context.Context, req *sfupb.CloseRoomRequest) (*sfupb.ErrorResponse, error) {
	if err := s.service.CloseRoom(ctx, req.GetCallId()); err != nil {
		return &sfupb.ErrorResponse{Error: err.Error()}, nil
	}
	return &sfupb.ErrorResponse{}, nil
}

func (s *sfuGRPCControlServer) Alive(_ context.Context, req *sfupb.AliveRequest) (*sfupb.AliveResponse, error) {
	return &sfupb.AliveResponse{UserIds: append([]int64(nil), s.service.AliveUserIDs(req.GetCallId())...)}, nil
}

func sfuGRPCBearerUnaryServerInterceptor(token string) grpc.UnaryServerInterceptor {
	token = strings.TrimSpace(token)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if token == "" {
			return nil, status.Error(codes.Unauthenticated, "bearer token is not configured")
		}
		md, ok := metadata.FromIncomingContext(ctx)
		if !ok {
			return nil, status.Error(codes.Unauthenticated, "missing authorization")
		}
		values := md.Get("authorization")
		expected := "Bearer " + token
		if len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte(expected)) != 1 {
			return nil, status.Error(codes.Unauthenticated, "invalid authorization")
		}
		return handler(ctx, req)
	}
}

func sfuGRPCWatchHealth(ctx context.Context, check func(context.Context) error, healthSrv *health.Server) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	update := func() {
		healthSrv.SetServingStatus(sfupb.SFUControlService_ServiceDesc.ServiceName, sfuGRPCHealthStatus(ctx, check))
	}
	update()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			update()
		}
	}
}

func sfuGRPCHealthStatus(ctx context.Context, check func(context.Context) error) healthpb.HealthCheckResponse_ServingStatus {
	if check == nil {
		return healthpb.HealthCheckResponse_SERVING
	}
	if ctx == nil {
		ctx = context.Background()
	}
	checkCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if err := check(checkCtx); err != nil {
		return healthpb.HealthCheckResponse_NOT_SERVING
	}
	return healthpb.HealthCheckResponse_SERVING
}

func sfuGRPCMessageSize(configured int) int {
	if configured > 0 {
		return configured
	}
	return defaultMaxGRPCControlMessageSize
}

func sfuGRPCServerTransportCredentials(cfg GRPCControlServerConfig) (credentials.TransportCredentials, bool, error) {
	certFile := strings.TrimSpace(cfg.TLSCertFile)
	keyFile := strings.TrimSpace(cfg.TLSKeyFile)
	clientCAFile := strings.TrimSpace(cfg.TLSClientCAFile)
	if certFile == "" && keyFile == "" && clientCAFile == "" {
		return nil, false, nil
	}
	if (certFile == "") != (keyFile == "") {
		return nil, false, fmt.Errorf("sfu grpc control tls: server cert and key must be configured together")
	}
	if certFile == "" {
		return nil, false, fmt.Errorf("sfu grpc control tls: server cert and key are required when client CA is configured")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, false, fmt.Errorf("sfu grpc control tls: load server certificate: %w", err)
	}
	tlsCfg := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
	}
	if clientCAFile != "" {
		pool, err := sfuGRPCLoadCertPool(clientCAFile)
		if err != nil {
			return nil, false, fmt.Errorf("sfu grpc control tls: load client CA: %w", err)
		}
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return credentials.NewTLS(tlsCfg), true, nil
}

func sfuGRPCClientTransportCredentials(cfg GRPCRemoteConfig) (credentials.TransportCredentials, error) {
	caFile := strings.TrimSpace(cfg.TLSCAFile)
	serverName := strings.TrimSpace(cfg.TLSServerName)
	certFile := strings.TrimSpace(cfg.TLSCertFile)
	keyFile := strings.TrimSpace(cfg.TLSKeyFile)
	if caFile == "" && serverName == "" && certFile == "" && keyFile == "" {
		return insecure.NewCredentials(), nil
	}
	if (certFile == "") != (keyFile == "") {
		return nil, fmt.Errorf("sfu grpc control tls: client cert and key must be configured together")
	}
	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		ServerName: serverName,
	}
	if caFile != "" {
		pool, err := sfuGRPCLoadCertPool(caFile)
		if err != nil {
			return nil, fmt.Errorf("sfu grpc control tls: load root CA: %w", err)
		}
		tlsCfg.RootCAs = pool
	}
	if certFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("sfu grpc control tls: load client certificate: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return credentials.NewTLS(tlsCfg), nil
}

func sfuGRPCLoadCertPool(path string) (*x509.CertPool, error) {
	pemBytes, err := os.ReadFile(strings.TrimSpace(path))
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		return nil, fmt.Errorf("no PEM certificates found in %s", path)
	}
	return pool, nil
}

func grpcControlTarget(addr string) (string, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", ErrGRPCControlAddrMissing
	}
	if strings.Contains(addr, "://") {
		u, err := url.Parse(addr)
		if err != nil || u.User != nil {
			return "", fmt.Errorf("sfu grpc control: invalid control addr %q", addr)
		}
		switch u.Scheme {
		case "dns":
			return addr, nil
		case "grpc":
			if u.Host == "" {
				return "", fmt.Errorf("sfu grpc control: invalid control addr %q", addr)
			}
			return u.Host, nil
		default:
			return "", fmt.Errorf("sfu grpc control: unsupported control addr scheme %q", u.Scheme)
		}
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return "", fmt.Errorf("sfu grpc control target must be host:port %q: %w", addr, err)
	}
	return addr, nil
}

func endpointKindToPB(kind EndpointKind) sfupb.EndpointKind {
	switch kind {
	case EndpointPresentation:
		return sfupb.EndpointKind_ENDPOINT_KIND_PRESENTATION
	default:
		return sfupb.EndpointKind_ENDPOINT_KIND_MAIN
	}
}

func endpointKindFromPB(kind sfupb.EndpointKind) EndpointKind {
	switch kind {
	case sfupb.EndpointKind_ENDPOINT_KIND_PRESENTATION:
		return EndpointPresentation
	default:
		return EndpointMain
	}
}

func clientOfferToPB(in ClientOffer) *sfupb.ClientOffer {
	groups := make([]*sfupb.SsrcGroup, 0, len(in.SsrcGroups))
	for _, group := range in.SsrcGroups {
		groups = append(groups, &sfupb.SsrcGroup{
			Semantics: group.Semantics,
			Sources:   append([]uint32(nil), group.Sources...),
		})
	}
	return &sfupb.ClientOffer{
		AudioSsrc:         in.AudioSSRC,
		Ufrag:             in.Ufrag,
		Pwd:               in.Pwd,
		FingerprintSha256: in.FingerprintSHA256,
		SsrcGroups:        groups,
	}
}

func clientOfferFromPB(in *sfupb.ClientOffer) ClientOffer {
	if in == nil {
		return ClientOffer{}
	}
	groups := make([]SsrcGroup, 0, len(in.GetSsrcGroups()))
	for _, group := range in.GetSsrcGroups() {
		groups = append(groups, SsrcGroup{
			Semantics: group.GetSemantics(),
			Sources:   append([]uint32(nil), group.GetSources()...),
		})
	}
	return ClientOffer{
		AudioSSRC:         in.GetAudioSsrc(),
		Ufrag:             in.GetUfrag(),
		Pwd:               in.GetPwd(),
		FingerprintSHA256: in.GetFingerprintSha256(),
		SsrcGroups:        groups,
	}
}

func serverAnswerToPB(in ServerAnswer) *sfupb.ServerAnswer {
	candidates := make([]*sfupb.Candidate, 0, len(in.Candidates))
	for _, candidate := range in.Candidates {
		candidates = append(candidates, &sfupb.Candidate{
			Ip:       candidate.IP,
			Port:     int32(candidate.Port),
			Protocol: candidate.Protocol,
			Type:     candidate.Type,
		})
	}
	return &sfupb.ServerAnswer{
		Ufrag:             in.Ufrag,
		Pwd:               in.Pwd,
		FingerprintSha256: in.FingerprintSHA256,
		Candidates:        candidates,
	}
}

func serverAnswerFromPB(in *sfupb.ServerAnswer) ServerAnswer {
	if in == nil {
		return ServerAnswer{}
	}
	candidates := make([]Candidate, 0, len(in.GetCandidates()))
	for _, candidate := range in.GetCandidates() {
		candidates = append(candidates, Candidate{
			IP:       candidate.GetIp(),
			Port:     int(candidate.GetPort()),
			Protocol: candidate.GetProtocol(),
			Type:     candidate.GetType(),
		})
	}
	return ServerAnswer{
		Ufrag:             in.GetUfrag(),
		Pwd:               in.GetPwd(),
		FingerprintSHA256: in.GetFingerprintSha256(),
		Candidates:        candidates,
	}
}

func sfuGRPCError(message string) error {
	if message == "" {
		return nil
	}
	return fmt.Errorf("sfu grpc control: %s", message)
}
