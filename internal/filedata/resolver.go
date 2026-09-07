package filedata

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"google.golang.org/grpc"
	_ "google.golang.org/grpc/balancer/roundrobin"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
)

type GRPCResolverProvider interface {
	Target() string
	DialOptions() []grpc.DialOption
}

type StaticGRPCResolverProvider struct {
	mu        sync.RWMutex
	targets   []string
	resolvers map[*manual.Resolver]struct{}
}

type DNSGRPCResolverProvider struct {
	target string
}

func ParseGRPCTargets(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, ErrGRPCTargetsMissing
	}
	parts := strings.Split(raw, ",")
	targets := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		target := strings.TrimSpace(part)
		if target == "" {
			continue
		}
		if err := validateTarget(target); err != nil {
			return nil, err
		}
		if _, ok := seen[target]; ok {
			continue
		}
		seen[target] = struct{}{}
		targets = append(targets, target)
	}
	if len(targets) == 0 {
		return nil, ErrGRPCTargetsMissing
	}
	return targets, nil
}

func ParseGRPCTargetsForResolver(raw, resolverKind string) ([]string, error) {
	switch strings.ToLower(strings.TrimSpace(resolverKind)) {
	case "", grpcResolverStatic:
		return ParseGRPCTargets(raw)
	case grpcResolverDNS:
		return parseDNSGRPCTargets(raw)
	default:
		return nil, fmt.Errorf("filedata grpc: unsupported resolver provider %q", resolverKind)
	}
}

func NewStaticGRPCResolverProvider(targets []string) (*StaticGRPCResolverProvider, error) {
	targets, err := normalizeTargets(targets)
	if err != nil {
		return nil, err
	}
	return &StaticGRPCResolverProvider{targets: targets, resolvers: make(map[*manual.Resolver]struct{})}, nil
}

func (p *StaticGRPCResolverProvider) Target() string {
	if p == nil {
		return ""
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.targets) == 0 {
		return ""
	}
	return staticResolverTarget
}

func (p *StaticGRPCResolverProvider) DialOptions() []grpc.DialOption {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.targets) == 0 {
		return nil
	}
	r := manual.NewBuilderWithScheme(staticResolverScheme)
	r.InitialState(resolverState(p.targets))
	p.resolvers[r] = struct{}{}
	return []grpc.DialOption{grpc.WithResolvers(r)}
}

func (p *StaticGRPCResolverProvider) UpdateTargets(targets []string) error {
	if p == nil {
		return ErrGRPCTargetsMissing
	}
	targets, err := normalizeTargets(targets)
	if err != nil {
		return err
	}
	state := resolverState(targets)
	p.mu.Lock()
	p.targets = targets
	resolvers := make([]*manual.Resolver, 0, len(p.resolvers))
	for r := range p.resolvers {
		resolvers = append(resolvers, r)
	}
	p.mu.Unlock()
	for _, r := range resolvers {
		r.UpdateState(state)
	}
	return nil
}

func NewDNSGRPCResolverProviderFromTargets(targets []string) (*DNSGRPCResolverProvider, error) {
	targets, err := normalizeDNSTargets(targets)
	if err != nil {
		return nil, err
	}
	return &DNSGRPCResolverProvider{target: targets[0]}, nil
}

func (p *DNSGRPCResolverProvider) Target() string {
	if p == nil || p.target == "" {
		return ""
	}
	if isDNSTargetURI(p.target) {
		return p.target
	}
	return dnsResolverScheme + ":///" + p.target
}

func (p *DNSGRPCResolverProvider) DialOptions() []grpc.DialOption { return nil }

func grpcResolverProvider(cfg GRPCClientConfig) (GRPCResolverProvider, error) {
	if cfg.Resolver != nil {
		return cfg.Resolver, nil
	}
	switch strings.ToLower(strings.TrimSpace(cfg.ResolverKind)) {
	case "", grpcResolverStatic:
		return NewStaticGRPCResolverProvider(cfg.Targets)
	case grpcResolverDNS:
		return NewDNSGRPCResolverProviderFromTargets(cfg.Targets)
	default:
		return nil, fmt.Errorf("filedata grpc: unsupported resolver provider %q", cfg.ResolverKind)
	}
}

func parseDNSGRPCTargets(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, ErrGRPCTargetsMissing
	}
	parts := strings.Split(raw, ",")
	targets := make([]string, 0, len(parts))
	for _, part := range parts {
		target := strings.TrimSpace(part)
		if target != "" {
			targets = append(targets, target)
		}
	}
	return normalizeDNSTargets(targets)
}

func normalizeTargets(targets []string) ([]string, error) {
	if len(targets) == 0 {
		return nil, ErrGRPCTargetsMissing
	}
	out := make([]string, 0, len(targets))
	seen := make(map[string]struct{}, len(targets))
	for _, raw := range targets {
		target := strings.TrimSpace(raw)
		if target == "" {
			continue
		}
		if err := validateTarget(target); err != nil {
			return nil, err
		}
		if _, ok := seen[target]; ok {
			continue
		}
		seen[target] = struct{}{}
		out = append(out, target)
	}
	if len(out) == 0 {
		return nil, ErrGRPCTargetsMissing
	}
	return out, nil
}

func normalizeDNSTargets(targets []string) ([]string, error) {
	if len(targets) == 0 {
		return nil, ErrGRPCTargetsMissing
	}
	out := make([]string, 0, len(targets))
	for _, raw := range targets {
		target := strings.TrimSpace(raw)
		if target == "" {
			continue
		}
		out = append(out, target)
	}
	if len(out) == 0 {
		return nil, ErrGRPCTargetsMissing
	}
	if len(out) != 1 {
		return nil, fmt.Errorf("filedata grpc dns resolver requires exactly one target, got %d", len(out))
	}
	if !isDNSTargetURI(out[0]) {
		if host, port, err := net.SplitHostPort(out[0]); err != nil || strings.TrimSpace(host) == "" || strings.TrimSpace(port) == "" {
			return nil, fmt.Errorf("filedata grpc dns target %q must be host:port or dns:///host:port", out[0])
		}
	}
	return out, nil
}

func resolverState(targets []string) resolver.State {
	addrs := make([]resolver.Address, 0, len(targets))
	for _, target := range targets {
		addrs = append(addrs, resolver.Address{Addr: target})
	}
	return resolver.State{Addresses: addrs}
}

func validateTarget(target string) error {
	if strings.TrimSpace(target) == "" {
		return ErrGRPCTargetsMissing
	}
	if strings.Contains(target, "://") {
		u, err := url.Parse(target)
		if err != nil {
			return fmt.Errorf("filedata grpc target %q: %w", target, err)
		}
		if u.Scheme != "passthrough" && u.Scheme != "dns" {
			return fmt.Errorf("filedata grpc target %q uses unsupported scheme %q", target, u.Scheme)
		}
		return nil
	}
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return fmt.Errorf("filedata grpc target %q must be host:port: %w", target, err)
	}
	if strings.TrimSpace(host) == "" {
		return fmt.Errorf("filedata grpc target %q has empty host", target)
	}
	p, err := strconv.Atoi(port)
	if err != nil || p <= 0 || p > 65535 {
		return fmt.Errorf("filedata grpc target %q has invalid port", target)
	}
	return nil
}

func isDNSTargetURI(target string) bool {
	u, err := url.Parse(target)
	return err == nil && u.Scheme == dnsResolverScheme
}

func clientDialOptions(cfg GRPCClientConfig, resolverProvider GRPCResolverProvider) ([]grpc.DialOption, error) {
	transport, err := clientTransportCredentials(cfg)
	if err != nil {
		return nil, err
	}
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(transport),
		grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(messageSize(cfg.MaxRecvMsgBytes)),
			grpc.MaxCallSendMsgSize(messageSize(cfg.MaxSendMsgBytes)),
		),
	}
	if resolverProvider != nil {
		opts = append(opts, resolverProvider.DialOptions()...)
	}
	opts = append(opts, cfg.DialOptions...)
	return opts, nil
}
