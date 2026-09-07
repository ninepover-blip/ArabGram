package sfu

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"
)

func TestGRPCRemoteServiceRoundTrip(t *testing.T) {
	local := &grpcFakeSFU{alive: []int64{55, 66}}
	addr := startTestSFUGRPC(t, local, "remote-sfu", "secret", nil)
	client, err := NewGRPCRemoteService(GRPCRemoteConfig{Token: "secret"})
	if err != nil {
		t.Fatalf("NewGRPCRemoteService: %v", err)
	}
	defer func() { _ = client.Close() }()

	owner := OwnerRecord{CallID: 9401, InstanceID: "remote-sfu", ControlAddr: addr}
	if err := client.CheckInstance(context.Background(), InstanceRecord{InstanceID: "remote-sfu", ControlAddr: addr}); err != nil {
		t.Fatalf("CheckInstance: %v", err)
	}
	answer, err := client.Join(context.Background(), owner, 9401, 55, EndpointPresentation, ClientOffer{
		AudioSSRC:         777,
		Ufrag:             "u",
		Pwd:               "p",
		FingerprintSHA256: "AA:BB",
		SsrcGroups:        []SsrcGroup{{Semantics: "SIM", Sources: []uint32{1, 2}}},
	})
	if err != nil {
		t.Fatalf("join: %v", err)
	}
	if answer.Ufrag != "grpc" || len(answer.Candidates) != 1 || answer.Candidates[0].Port != 12399 {
		t.Fatalf("answer = %+v, want grpc answer with candidate", answer)
	}
	offer := local.lastOffer()
	if offer.AudioSSRC != 777 || len(offer.SsrcGroups) != 1 || len(offer.SsrcGroups[0].Sources) != 2 {
		t.Fatalf("offer = %+v, want full protobuf round trip", offer)
	}
	if err := client.Leave(context.Background(), owner, 9401, 55, EndpointPresentation); err != nil {
		t.Fatalf("leave: %v", err)
	}
	if err := client.CloseRoom(context.Background(), owner, 9401); err != nil {
		t.Fatalf("close room: %v", err)
	}
	alive, err := client.AliveUserIDs(context.Background(), owner, 9401)
	if err != nil {
		t.Fatalf("alive: %v", err)
	}
	if len(alive) != 2 || alive[0] != 55 || alive[1] != 66 {
		t.Fatalf("alive = %v, want [55 66]", alive)
	}
	if local.joinCount() != 1 || local.leaveCount() != 1 || local.closeCount() != 1 {
		t.Fatalf("counts join/leave/close = %d/%d/%d, want 1/1/1", local.joinCount(), local.leaveCount(), local.closeCount())
	}
}

func TestGRPCRemoteServiceRequiresToken(t *testing.T) {
	local := &grpcFakeSFU{}
	addr := startTestSFUGRPC(t, local, "remote-sfu", "secret", nil)
	client, err := NewGRPCRemoteService(GRPCRemoteConfig{Token: "wrong"})
	if err != nil {
		t.Fatalf("NewGRPCRemoteService: %v", err)
	}
	defer func() { _ = client.Close() }()

	record := InstanceRecord{InstanceID: "remote-sfu", ControlAddr: addr}
	if err := client.CheckInstance(context.Background(), record); err == nil {
		t.Fatal("CheckInstance with wrong token unexpectedly succeeded")
	}
	owner := OwnerRecord{CallID: 9402, InstanceID: "remote-sfu", ControlAddr: addr}
	if _, err := client.Join(context.Background(), owner, 9402, 55, EndpointMain, ClientOffer{Ufrag: "u", Pwd: "p"}); err == nil {
		t.Fatal("join with wrong token unexpectedly succeeded")
	}
	if local.joinCount() != 0 {
		t.Fatalf("unauthorized join reached service")
	}
}

func TestGRPCControlRequiresConfiguredBearerToken(t *testing.T) {
	ctx := context.Background()
	if _, err := StartGRPCControl(ctx, GRPCControlServerConfig{Addr: "127.0.0.1:0", Service: &grpcFakeSFU{}}); !errors.Is(err, ErrGRPCControlTokenMissing) {
		t.Fatalf("StartGRPCControl missing token err = %v, want ErrGRPCControlTokenMissing", err)
	}
	if _, err := NewGRPCRemoteService(GRPCRemoteConfig{}); !errors.Is(err, ErrGRPCControlTokenMissing) {
		t.Fatalf("NewGRPCRemoteService missing token err = %v, want ErrGRPCControlTokenMissing", err)
	}
}

func TestGRPCRemoteServiceCheckInstanceRejectsMismatchedInstanceID(t *testing.T) {
	addr := startTestSFUGRPC(t, &grpcFakeSFU{}, "new-sfu", "secret", nil)
	client, err := NewGRPCRemoteService(GRPCRemoteConfig{Token: "secret"})
	if err != nil {
		t.Fatalf("NewGRPCRemoteService: %v", err)
	}
	defer func() { _ = client.Close() }()

	if err := client.CheckInstance(context.Background(), InstanceRecord{InstanceID: "old-sfu", ControlAddr: addr}); err == nil {
		t.Fatal("CheckInstance accepted stale instance record with reused control addr")
	}
}

func TestGRPCRemoteServiceCheckInstanceReportsNotServing(t *testing.T) {
	addr := startTestSFUGRPC(t, &grpcFakeSFU{}, "remote-sfu", "secret", func(context.Context) error {
		return errors.New("heartbeat stale")
	})
	client, err := NewGRPCRemoteService(GRPCRemoteConfig{Token: "secret"})
	if err != nil {
		t.Fatalf("NewGRPCRemoteService: %v", err)
	}
	defer func() { _ = client.Close() }()

	if err := client.CheckInstance(context.Background(), InstanceRecord{InstanceID: "remote-sfu", ControlAddr: addr}); err == nil {
		t.Fatal("CheckInstance accepted NOT_SERVING remote SFU")
	}
}

func TestOwnerServiceClaimsSelectedRemoteInstanceThroughGRPC(t *testing.T) {
	remoteLocal := &grpcFakeSFU{}
	addr := startTestSFUGRPC(t, remoteLocal, "remote-sfu", "secret", nil)
	remote, err := NewGRPCRemoteService(GRPCRemoteConfig{Token: "secret"})
	if err != nil {
		t.Fatalf("NewGRPCRemoteService: %v", err)
	}
	defer func() { _ = remote.Close() }()

	ctx := context.Background()
	instances := NewMemoryInstanceRegistry()
	if err := instances.Register(ctx, InstanceRecord{InstanceID: "remote-sfu", ControlAddr: addr}, time.Minute); err != nil {
		t.Fatalf("register remote sfu: %v", err)
	}
	owners := NewMemoryOwnerRegistry()
	core, err := NewRemoteOwnerService(owners, "core-a", time.Minute,
		WithOwnerSelector(NewRegistryOwnerSelector(instances, WithInstanceHealthChecker(remote))),
		WithRemoteService(remote))
	if err != nil {
		t.Fatalf("owner service: %v", err)
	}
	if _, err := core.Join(ctx, 9403, 12, EndpointMain, ClientOffer{Ufrag: "u", Pwd: "p"}); err != nil {
		t.Fatalf("join selected remote owner: %v", err)
	}
	record, found, err := owners.Get(ctx, 9403)
	if err != nil || !found {
		t.Fatalf("owner record = %+v found=%v err=%v", record, found, err)
	}
	if record.InstanceID != "remote-sfu" || record.ControlAddr != addr {
		t.Fatalf("owner record = %+v, want selected gRPC remote sfu", record)
	}
	if remoteLocal.joinCount() != 1 {
		t.Fatalf("remote join count = %d, want 1", remoteLocal.joinCount())
	}
}

func TestGRPCRemoteServiceRoundTripWithMTLS(t *testing.T) {
	files := writeSFUGRPCTLSTestFiles(t)
	local := &grpcFakeSFU{alive: []int64{77}}
	addr := startTestSFUGRPCWithConfig(t, local, "secure-sfu", "secret", nil, func(cfg *GRPCControlServerConfig) {
		cfg.TLSCertFile = files.serverCert
		cfg.TLSKeyFile = files.serverKey
		cfg.TLSClientCAFile = files.caCert
	})
	client, err := NewGRPCRemoteService(GRPCRemoteConfig{
		Token:         "secret",
		TLSCAFile:     files.caCert,
		TLSServerName: "sfu.test",
		TLSCertFile:   files.clientCert,
		TLSKeyFile:    files.clientKey,
	})
	if err != nil {
		t.Fatalf("NewGRPCRemoteService: %v", err)
	}
	defer func() { _ = client.Close() }()

	record := InstanceRecord{InstanceID: "secure-sfu", ControlAddr: addr}
	if err := client.CheckInstance(context.Background(), record); err != nil {
		t.Fatalf("mTLS CheckInstance: %v", err)
	}
	answer, err := client.Join(context.Background(), OwnerRecord{CallID: 9404, InstanceID: "secure-sfu", ControlAddr: addr}, 9404, 77, EndpointMain, ClientOffer{Ufrag: "u", Pwd: "p"})
	if err != nil {
		t.Fatalf("mTLS join: %v", err)
	}
	if answer.Ufrag != "grpc" {
		t.Fatalf("answer = %+v, want grpc answer", answer)
	}
}

func TestGRPCRemoteServiceMTLSRejectsMissingClientCertificate(t *testing.T) {
	files := writeSFUGRPCTLSTestFiles(t)
	addr := startTestSFUGRPCWithConfig(t, &grpcFakeSFU{}, "secure-sfu", "secret", nil, func(cfg *GRPCControlServerConfig) {
		cfg.TLSCertFile = files.serverCert
		cfg.TLSKeyFile = files.serverKey
		cfg.TLSClientCAFile = files.caCert
	})
	client, err := NewGRPCRemoteService(GRPCRemoteConfig{
		Token:         "secret",
		TLSCAFile:     files.caCert,
		TLSServerName: "sfu.test",
	})
	if err != nil {
		t.Fatalf("NewGRPCRemoteService: %v", err)
	}
	defer func() { _ = client.Close() }()

	if err := client.CheckInstance(context.Background(), InstanceRecord{InstanceID: "secure-sfu", ControlAddr: addr}); err == nil {
		t.Fatal("CheckInstance without mTLS client certificate unexpectedly succeeded")
	}
}

func startTestSFUGRPC(t *testing.T, service Service, instanceID, token string, health func(context.Context) error) string {
	return startTestSFUGRPCWithConfig(t, service, instanceID, token, health, nil)
}

func startTestSFUGRPCWithConfig(t *testing.T, service Service, instanceID, token string, health func(context.Context) error, mutate func(*GRPCControlServerConfig)) string {
	t.Helper()
	addr := unusedSFUTCPAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cfg := GRPCControlServerConfig{
		Addr:       addr,
		InstanceID: instanceID,
		Token:      token,
		Service:    service,
		Health:     health,
		Logger:     zaptest.NewLogger(t),
	}
	if mutate != nil {
		mutate(&cfg)
	}
	srv, err := StartGRPCControl(ctx, cfg)
	if err != nil {
		t.Fatalf("StartGRPCControl: %v", err)
	}
	t.Cleanup(srv.Stop)
	return addr
}

func unusedSFUTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen unused tcp addr: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close unused tcp addr: %v", err)
	}
	return addr
}

type grpcFakeSFU struct {
	mu          sync.Mutex
	joins       []int64
	leaves      []int64
	closedRooms []int64
	offer       ClientOffer
	alive       []int64
}

func (f *grpcFakeSFU) Enabled() bool { return true }

func (f *grpcFakeSFU) Join(_ context.Context, callID, _ int64, _ EndpointKind, offer ClientOffer) (ServerAnswer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.joins = append(f.joins, callID)
	f.offer = offer
	return ServerAnswer{
		Ufrag:             "grpc",
		Pwd:               "pwd",
		FingerprintSHA256: "AA",
		Candidates:        []Candidate{{IP: "127.0.0.1", Port: 12399, Protocol: "udp", Type: "host"}},
	}, nil
}

func (f *grpcFakeSFU) Leave(_ context.Context, callID, _ int64, _ EndpointKind) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leaves = append(f.leaves, callID)
	return nil
}

func (f *grpcFakeSFU) CloseRoom(_ context.Context, callID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closedRooms = append(f.closedRooms, callID)
	return nil
}

func (f *grpcFakeSFU) AliveUserIDs(int64) []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.alive...)
}

func (f *grpcFakeSFU) lastOffer() ClientOffer {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.offer
}

func (f *grpcFakeSFU) joinCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.joins)
}

func (f *grpcFakeSFU) leaveCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.leaves)
}

func (f *grpcFakeSFU) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.closedRooms)
}

type sfuGRPCTLSTestFiles struct {
	caCert     string
	serverCert string
	serverKey  string
	clientCert string
	clientKey  string
}

func writeSFUGRPCTLSTestFiles(t testing.TB) sfuGRPCTLSTestFiles {
	t.Helper()
	dir := t.TempDir()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber:          sfuTLSRandomSerial(t),
		Subject:               pkix.Name{CommonName: "sfu grpc test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	files := sfuGRPCTLSTestFiles{
		caCert:     filepath.Join(dir, "ca.pem"),
		serverCert: filepath.Join(dir, "server.pem"),
		serverKey:  filepath.Join(dir, "server-key.pem"),
		clientCert: filepath.Join(dir, "client.pem"),
		clientKey:  filepath.Join(dir, "client-key.pem"),
	}
	writeSFUTLSCertPEM(t, files.caCert, caDER)
	writeSFUTLSSignedCertPair(t, files.serverCert, files.serverKey, caTemplate, caKey, &x509.Certificate{
		SerialNumber: sfuTLSRandomSerial(t),
		Subject:      pkix.Name{CommonName: "sfu.test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"sfu.test"},
	})
	writeSFUTLSSignedCertPair(t, files.clientCert, files.clientKey, caTemplate, caKey, &x509.Certificate{
		SerialNumber: sfuTLSRandomSerial(t),
		Subject:      pkix.Name{CommonName: "core.test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	return files
}

func writeSFUTLSSignedCertPair(t testing.TB, certPath, keyPath string, caTemplate *x509.Certificate, caKey *rsa.PrivateKey, template *x509.Certificate) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, caTemplate, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	writeSFUTLSCertPEM(t, certPath, certDER)
	writeSFUTLSKeyPEM(t, keyPath, key)
}

func sfuTLSRandomSerial(t testing.TB) *big.Int {
	t.Helper()
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}
	return serial
}

func writeSFUTLSCertPEM(t testing.TB, path string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeSFUTLSKeyPEM(t testing.TB, path string, key *rsa.PrivateKey) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0o600); err != nil {
		t.Fatal(err)
	}
}
