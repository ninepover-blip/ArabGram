package filedata

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

func serverTransportCredentials(cfg GRPCServerConfig) (credentials.TransportCredentials, bool, error) {
	certFile := strings.TrimSpace(cfg.TLSCertFile)
	keyFile := strings.TrimSpace(cfg.TLSKeyFile)
	if certFile == "" && keyFile == "" && strings.TrimSpace(cfg.TLSClientCAFile) == "" {
		return nil, false, nil
	}
	if certFile == "" || keyFile == "" {
		return nil, false, fmt.Errorf("filedata grpc TLS cert and key must be configured together")
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, false, fmt.Errorf("load filedata grpc TLS cert: %w", err)
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}
	if caFile := strings.TrimSpace(cfg.TLSClientCAFile); caFile != "" {
		pool, err := loadCertPool(caFile)
		if err != nil {
			return nil, false, err
		}
		tlsCfg.ClientCAs = pool
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
	}
	return credentials.NewTLS(tlsCfg), true, nil
}

func clientTransportCredentials(cfg GRPCClientConfig) (credentials.TransportCredentials, error) {
	caFile := strings.TrimSpace(cfg.TLSCAFile)
	certFile := strings.TrimSpace(cfg.TLSCertFile)
	keyFile := strings.TrimSpace(cfg.TLSKeyFile)
	serverName := strings.TrimSpace(cfg.TLSServerName)
	if caFile == "" && certFile == "" && keyFile == "" && serverName == "" {
		return insecure.NewCredentials(), nil
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: serverName}
	if caFile != "" {
		pool, err := loadCertPool(caFile)
		if err != nil {
			return nil, err
		}
		tlsCfg.RootCAs = pool
	}
	if (certFile == "") != (keyFile == "") {
		return nil, fmt.Errorf("filedata grpc client TLS cert and key must be configured together")
	}
	if certFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load filedata grpc client TLS cert: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return credentials.NewTLS(tlsCfg), nil
}

func loadCertPool(path string) (*x509.CertPool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read filedata grpc CA file %q: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return nil, fmt.Errorf("filedata grpc CA file %q contains no certificates", path)
	}
	return pool, nil
}
