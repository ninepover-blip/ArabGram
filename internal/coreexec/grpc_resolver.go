package coreexec

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

const (
	staticCoreExecResolverScheme = "coreexec-static"
	staticCoreExecResolverTarget = staticCoreExecResolverScheme + ":///coreexec"
	dnsCoreExecResolverScheme    = "dns"
	GRPCResolverStatic           = "static"
	GRPCResolverDNS              = "dns"
)

// GRPCResolverProvider is the replaceable CoreExec service-discovery boundary.
// The first provider is a static endpoint list; future Kubernetes, xDS, or
// Consul providers should plug in here without changing Edge/Core contracts.
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
		if err := validateCoreExecTarget(target); err != nil {
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
	case "", GRPCResolverStatic:
		return ParseGRPCTargets(raw)
	case GRPCResolverDNS:
		return parseDNSGRPCTargets(raw)
	default:
		return nil, fmt.Errorf("coreexec grpc: unsupported resolver provider %q", resolverKind)
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
		if target == "" {
			continue
		}
		targets = append(targets, target)
	}
	return normalizeDNSGRPCTargets(targets)
}

func NewDNSGRPCResolverProvider(target string) (*DNSGRPCResolverProvider, error) {
	return NewDNSGRPCResolverProviderFromTargets([]string{target})
}

func NewDNSGRPCResolverProviderFromTargets(targets []string) (*DNSGRPCResolverProvider, error) {
	targets, err := normalizeDNSGRPCTargets(targets)
	if err != nil {
		return nil, err
	}
	return &DNSGRPCResolverProvider{target: targets[0]}, nil
}

func (p *DNSGRPCResolverProvider) Target() string {
	if p == nil || p.target == "" {
		return ""
	}
	if isGRPCDNSTargetURI(p.target) {
		return p.target
	}
	return dnsCoreExecResolverScheme + ":///" + p.target
}

func (p *DNSGRPCResolverProvider) DialOptions() []grpc.DialOption {
	return nil
}

func (p *DNSGRPCResolverProvider) Targets() []string {
	if p == nil || p.target == "" {
		return nil
	}
	return []string{p.target}
}

func NewStaticGRPCResolverProvider(targets []string) (*StaticGRPCResolverProvider, error) {
	targets, err := normalizeGRPCTargets(targets)
	if err != nil {
		return nil, err
	}
	return &StaticGRPCResolverProvider{
		targets:   targets,
		resolvers: make(map[*manual.Resolver]struct{}),
	}, nil
}

func (p *StaticGRPCResolverProvider) Targets() []string {
	if p == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]string, len(p.targets))
	copy(out, p.targets)
	return out
}

func (p *StaticGRPCResolverProvider) UpdateTargets(targets []string) error {
	if p == nil {
		return ErrGRPCTargetsMissing
	}
	targets, err := normalizeGRPCTargets(targets)
	if err != nil {
		return err
	}
	state := grpcResolverState(targets)
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

func (p *StaticGRPCResolverProvider) Target() string {
	if p == nil {
		return ""
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.targets) == 0 {
		return ""
	}
	return staticCoreExecResolverTarget
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
	r := p.newManualResolverLocked()
	if p.resolvers == nil {
		p.resolvers = make(map[*manual.Resolver]struct{})
	}
	p.resolvers[r] = struct{}{}
	return []grpc.DialOption{grpc.WithResolvers(r)}
}

func grpcResolverProvider(cfg GRPCClientConfig) (GRPCResolverProvider, error) {
	if cfg.Resolver != nil {
		return cfg.Resolver, nil
	}
	switch strings.ToLower(strings.TrimSpace(cfg.ResolverKind)) {
	case "", GRPCResolverStatic:
		return NewStaticGRPCResolverProvider(cfg.Targets)
	case GRPCResolverDNS:
		return NewDNSGRPCResolverProviderFromTargets(cfg.Targets)
	default:
		return nil, fmt.Errorf("coreexec grpc: unsupported resolver provider %q", cfg.ResolverKind)
	}
}

func normalizeGRPCTargets(targets []string) ([]string, error) {
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
		if err := validateCoreExecTarget(target); err != nil {
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

func normalizeDNSGRPCTargets(targets []string) ([]string, error) {
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
		return nil, fmt.Errorf("coreexec grpc dns resolver requires exactly one target, got %d", len(out))
	}
	if err := validateCoreExecDNSTarget(out[0]); err != nil {
		return nil, err
	}
	return out, nil
}

func validateCoreExecDNSTarget(target string) error {
	if isGRPCDNSTargetURI(target) {
		return validateGRPCDNSTargetURI(target)
	}
	return validateCoreExecTarget(target)
}

func isGRPCDNSTargetURI(target string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(target)), dnsCoreExecResolverScheme+"://")
}

func validateGRPCDNSTargetURI(target string) error {
	u, err := url.Parse(target)
	if err != nil {
		return fmt.Errorf("invalid coreexec grpc dns target URI %q: %w", target, err)
	}
	if !strings.EqualFold(u.Scheme, dnsCoreExecResolverScheme) {
		return fmt.Errorf("coreexec grpc dns target URI must use dns scheme %q", target)
	}
	if u.User != nil || u.Opaque != "" || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("coreexec grpc dns target URI must not include userinfo, opaque data, query, or fragment %q", target)
	}
	if strings.ContainsAny(target, " \t\r\n") {
		return fmt.Errorf("invalid whitespace in coreexec grpc dns target URI %q", target)
	}
	if strings.TrimSpace(u.Host) != "" {
		if err := validateHostPort(u.Host); err != nil {
			return fmt.Errorf("coreexec grpc dns target authority must be host:port %q: %w", target, err)
		}
	}
	endpoint := strings.TrimPrefix(u.Path, "/")
	if endpoint == "" || strings.Contains(endpoint, "/") {
		return fmt.Errorf("coreexec grpc dns target URI must contain exactly one endpoint path %q", target)
	}
	if err := validateCoreExecTarget(endpoint); err != nil {
		return fmt.Errorf("coreexec grpc dns target endpoint invalid %q: %w", target, err)
	}
	return nil
}

func validateHostPort(target string) error {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		return err
	}
	if strings.TrimSpace(host) == "" || strings.TrimSpace(port) == "" {
		return fmt.Errorf("invalid host:port target %q", target)
	}
	if strings.ContainsAny(host+port, " \t\r\n") {
		return fmt.Errorf("invalid whitespace in host:port target %q", target)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil {
		return fmt.Errorf("invalid numeric port in target %q: %w", target, err)
	}
	if portNumber <= 0 || portNumber > 65535 {
		return fmt.Errorf("port out of range in target %q", target)
	}
	return nil
}

func (p *StaticGRPCResolverProvider) newManualResolverLocked() *manual.Resolver {
	r := manual.NewBuilderWithScheme(staticCoreExecResolverScheme)
	r.InitialState(grpcResolverState(p.targets))
	r.CloseCallback = func() {
		p.removeManualResolver(r)
	}
	return r
}

func (p *StaticGRPCResolverProvider) removeManualResolver(r *manual.Resolver) {
	if p == nil || r == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.resolvers, r)
}

func grpcResolverState(targets []string) resolver.State {
	addresses := make([]resolver.Address, 0, len(targets))
	for _, target := range targets {
		addresses = append(addresses, resolver.Address{Addr: target})
	}
	return resolver.State{Addresses: addresses}
}
