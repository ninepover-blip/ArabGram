package egress

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"

	"telesrv/internal/egress/egresspb"
)

const (
	grpcDeliveryResolverStatic       = "static"
	grpcDeliveryResolverDNS          = "dns"
	staticGRPCDeliveryResolverScheme = "egress-delivery-static"
	staticGRPCDeliveryResolverTarget = staticGRPCDeliveryResolverScheme + ":///egress-delivery"
	dnsGRPCDeliveryResolverScheme    = "dns"
)

func grpcDeliveryMessageSize(configured int) int {
	if configured > 0 {
		return configured
	}
	return defaultMaxGRPCDeliveryMessageSize
}

func grpcDeliveryBearerUnaryServerInterceptor(token string) grpc.UnaryServerInterceptor {
	token = strings.TrimSpace(token)
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
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

func grpcDeliveryBearerUnaryClientInterceptor(token string) grpc.UnaryClientInterceptor {
	token = strings.TrimSpace(token)
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token), method, req, reply, cc, opts...)
	}
}

func grpcDeliveryClientDialOptions(cfg GRPCDeliveryClientConfig, provider GRPCDeliveryResolverProvider) ([]grpc.DialOption, error) {
	transportCreds, err := grpcDeliveryClientTransportCredentials(cfg)
	if err != nil {
		return nil, err
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCreds),
		grpc.WithChainUnaryInterceptor(grpcDeliveryBearerUnaryClientInterceptor(cfg.Token)),
		grpc.WithDisableRetry(),
		grpc.WithDefaultServiceConfig(grpcDeliveryDefaultServiceConfig()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(grpcDeliveryMessageSize(cfg.MaxRecvMsgBytes)),
			grpc.MaxCallSendMsgSize(grpcDeliveryMessageSize(cfg.MaxSendMsgBytes)),
		),
	}
	if provider != nil {
		opts = append(opts, provider.DialOptions()...)
	}
	return append(opts, cfg.DialOptions...), nil
}

func grpcDeliveryDefaultServiceConfig() string {
	return fmt.Sprintf(`{"loadBalancingConfig":[{"round_robin":{}}],"healthCheckConfig":{"serviceName":%q}}`,
		egresspb.EgressDeliveryService_ServiceDesc.ServiceName)
}

func grpcDeliveryServerTransportCredentials(cfg GRPCDeliveryServerConfig) (credentials.TransportCredentials, bool, error) {
	certFile := strings.TrimSpace(cfg.TLSCertFile)
	keyFile := strings.TrimSpace(cfg.TLSKeyFile)
	clientCAFile := strings.TrimSpace(cfg.TLSClientCAFile)
	if certFile == "" && keyFile == "" && clientCAFile == "" {
		return nil, false, nil
	}
	if certFile == "" || keyFile == "" {
		return nil, false, fmt.Errorf("egress delivery grpc tls: server cert and key are required")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, false, fmt.Errorf("egress delivery grpc tls: load server certificate: %w", err)
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}
	if clientCAFile != "" {
		pool, err := grpcDeliveryLoadCertPool(clientCAFile)
		if err != nil {
			return nil, false, fmt.Errorf("egress delivery grpc tls: load client CA: %w", err)
		}
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return credentials.NewTLS(tlsCfg), true, nil
}

func grpcDeliveryClientTransportCredentials(cfg GRPCDeliveryClientConfig) (credentials.TransportCredentials, error) {
	caFile := strings.TrimSpace(cfg.TLSCAFile)
	serverName := strings.TrimSpace(cfg.TLSServerName)
	certFile := strings.TrimSpace(cfg.TLSCertFile)
	keyFile := strings.TrimSpace(cfg.TLSKeyFile)
	if caFile == "" && serverName == "" && certFile == "" && keyFile == "" {
		return insecure.NewCredentials(), nil
	}
	if (certFile == "") != (keyFile == "") {
		return nil, fmt.Errorf("egress delivery grpc tls: client cert and key must be configured together")
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName}
	if caFile != "" {
		pool, err := grpcDeliveryLoadCertPool(caFile)
		if err != nil {
			return nil, fmt.Errorf("egress delivery grpc tls: load root CA: %w", err)
		}
		tlsCfg.RootCAs = pool
	}
	if certFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("egress delivery grpc tls: load client certificate: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return credentials.NewTLS(tlsCfg), nil
}

func grpcDeliveryLoadCertPool(path string) (*x509.CertPool, error) {
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

type GRPCDeliveryResolverProvider interface {
	Target() string
	DialOptions() []grpc.DialOption
}

type StaticGRPCDeliveryResolverProvider struct {
	mu        sync.RWMutex
	targets   []string
	resolvers map[*manual.Resolver]struct{}
}

type DNSGRPCDeliveryResolverProvider struct{ target string }

func ParseGRPCDeliveryTargetsForResolver(raw, resolverKind string) ([]string, error) {
	switch strings.ToLower(strings.TrimSpace(resolverKind)) {
	case "", grpcDeliveryResolverStatic:
		return normalizeGRPCDeliveryTargets(strings.Split(raw, ","))
	case grpcDeliveryResolverDNS:
		return parseDNSGRPCDeliveryTargets(raw)
	default:
		return nil, fmt.Errorf("egress delivery grpc: unsupported resolver provider %q", resolverKind)
	}
}

func parseDNSGRPCDeliveryTargets(raw string) ([]string, error) {
	parts := strings.Split(raw, ",")
	targets := make([]string, 0, len(parts))
	for _, part := range parts {
		if target := strings.TrimSpace(part); target != "" {
			targets = append(targets, target)
		}
	}
	if len(targets) == 0 {
		return nil, ErrGRPCDeliveryTargetsMissing
	}
	if len(targets) != 1 {
		return nil, fmt.Errorf("egress delivery grpc dns resolver requires exactly one target, got %d", len(targets))
	}
	if err := validateGRPCDeliveryDNSTarget(targets[0]); err != nil {
		return nil, err
	}
	return targets, nil
}

func NewStaticGRPCDeliveryResolverProvider(targets []string) (*StaticGRPCDeliveryResolverProvider, error) {
	normalized, err := normalizeGRPCDeliveryTargets(targets)
	if err != nil {
		return nil, err
	}
	return &StaticGRPCDeliveryResolverProvider{targets: normalized, resolvers: make(map[*manual.Resolver]struct{})}, nil
}

func (p *StaticGRPCDeliveryResolverProvider) Targets() []string {
	if p == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return append([]string(nil), p.targets...)
}

func (p *StaticGRPCDeliveryResolverProvider) UpdateTargets(targets []string) error {
	if p == nil {
		return ErrGRPCDeliveryTargetsMissing
	}
	normalized, err := normalizeGRPCDeliveryTargets(targets)
	if err != nil {
		return err
	}
	state := grpcDeliveryResolverState(normalized)
	p.mu.Lock()
	p.targets = normalized
	active := make([]*manual.Resolver, 0, len(p.resolvers))
	for r := range p.resolvers {
		active = append(active, r)
	}
	p.mu.Unlock()
	for _, r := range active {
		r.UpdateState(state)
	}
	return nil
}

func (p *StaticGRPCDeliveryResolverProvider) Target() string {
	if len(p.Targets()) == 0 {
		return ""
	}
	return staticGRPCDeliveryResolverTarget
}

func (p *StaticGRPCDeliveryResolverProvider) DialOptions() []grpc.DialOption {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.targets) == 0 {
		return nil
	}
	r := manual.NewBuilderWithScheme(staticGRPCDeliveryResolverScheme)
	r.InitialState(grpcDeliveryResolverState(p.targets))
	r.CloseCallback = func() {
		p.mu.Lock()
		delete(p.resolvers, r)
		p.mu.Unlock()
	}
	p.resolvers[r] = struct{}{}
	return []grpc.DialOption{grpc.WithResolvers(r)}
}

func NewDNSGRPCDeliveryResolverProviderFromTargets(targets []string) (*DNSGRPCDeliveryResolverProvider, error) {
	normalized, err := parseDNSGRPCDeliveryTargets(strings.Join(targets, ","))
	if err != nil {
		return nil, err
	}
	return &DNSGRPCDeliveryResolverProvider{target: normalized[0]}, nil
}

func (p *DNSGRPCDeliveryResolverProvider) Target() string {
	if p == nil || p.target == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(p.target)), dnsGRPCDeliveryResolverScheme+"://") {
		return p.target
	}
	return dnsGRPCDeliveryResolverScheme + ":///" + p.target
}

func (p *DNSGRPCDeliveryResolverProvider) DialOptions() []grpc.DialOption { return nil }

func grpcDeliveryResolverProvider(cfg GRPCDeliveryClientConfig) (GRPCDeliveryResolverProvider, error) {
	if cfg.Resolver != nil {
		return cfg.Resolver, nil
	}
	switch strings.ToLower(strings.TrimSpace(cfg.ResolverKind)) {
	case "", grpcDeliveryResolverStatic:
		return NewStaticGRPCDeliveryResolverProvider(cfg.Targets)
	case grpcDeliveryResolverDNS:
		return NewDNSGRPCDeliveryResolverProviderFromTargets(cfg.Targets)
	default:
		return nil, fmt.Errorf("egress delivery grpc: unsupported resolver provider %q", cfg.ResolverKind)
	}
}

func normalizeGRPCDeliveryTargets(targets []string) ([]string, error) {
	if len(targets) == 0 {
		return nil, ErrGRPCDeliveryTargetsMissing
	}
	out := make([]string, 0, len(targets))
	seen := make(map[string]struct{}, len(targets))
	for _, raw := range targets {
		target := strings.TrimSpace(raw)
		if target == "" {
			continue
		}
		if err := validateGRPCDeliveryTarget(target); err != nil {
			return nil, err
		}
		if _, exists := seen[target]; exists {
			continue
		}
		seen[target] = struct{}{}
		out = append(out, target)
	}
	if len(out) == 0 {
		return nil, ErrGRPCDeliveryTargetsMissing
	}
	return out, nil
}

func validateGRPCDeliveryTarget(target string) error {
	if err := validateGRPCDeliveryHostPort(target); err != nil {
		return fmt.Errorf("egress delivery grpc target must be host:port %q: %w", target, err)
	}
	return nil
}

func validateGRPCDeliveryDNSTarget(target string) error {
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(target)), dnsGRPCDeliveryResolverScheme+"://") {
		return validateGRPCDeliveryDNSTargetURI(target)
	}
	return validateGRPCDeliveryTarget(target)
}

func validateGRPCDeliveryDNSTargetURI(target string) error {
	u, err := url.Parse(target)
	if err != nil || !strings.EqualFold(u.Scheme, dnsGRPCDeliveryResolverScheme) {
		return fmt.Errorf("invalid egress delivery grpc dns target URI %q", target)
	}
	if u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.Fragment != "" || strings.ContainsAny(target, " \t\r\n") {
		return fmt.Errorf("invalid egress delivery grpc dns target URI %q", target)
	}
	if u.Host != "" {
		if err := validateGRPCDeliveryHostPort(u.Host); err != nil {
			return fmt.Errorf("invalid egress delivery grpc dns authority %q: %w", target, err)
		}
	}
	endpoint := strings.TrimPrefix(u.Path, "/")
	if endpoint == "" || strings.Contains(endpoint, "/") {
		return fmt.Errorf("egress delivery grpc dns target must contain one endpoint %q", target)
	}
	return validateGRPCDeliveryTarget(endpoint)
}

func validateGRPCDeliveryHostPort(target string) error {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return err
	}
	if strings.TrimSpace(host) == "" || strings.TrimSpace(port) == "" || strings.ContainsAny(host+port, " \t\r\n") {
		return fmt.Errorf("invalid host:port target %q", target)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber <= 0 || portNumber > 65535 {
		return fmt.Errorf("invalid port in target %q", target)
	}
	return nil
}

func grpcDeliveryResolverState(targets []string) resolver.State {
	addresses := make([]resolver.Address, 0, len(targets))
	for _, target := range targets {
		addresses = append(addresses, resolver.Address{Addr: target})
	}
	return resolver.State{Addresses: addresses}
}
