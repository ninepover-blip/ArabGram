package main

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

func main() {
	outDir := flag.String("out", "", "directory for generated PEM files")
	serverName := flag.String("server-name", "core.internal", "CoreExec server certificate name")
	flag.Parse()
	if *outDir == "" {
		fatalf("-out is required")
	}
	if *serverName == "" {
		fatalf("-server-name is required")
	}
	if err := os.MkdirAll(*outDir, 0o700); err != nil {
		fatalf("create output dir: %v", err)
	}
	caKey := mustRSA("ca")
	caTemplate := &x509.Certificate{
		SerialNumber:          mustSerial(),
		Subject:               pkix.Name{CommonName: "telesrv CoreExec smoke CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		fatalf("create ca cert: %v", err)
	}
	writeCert(filepath.Join(*outDir, "ca.pem"), caDER)
	writeSignedPair(*outDir, "server", caTemplate, caKey, serverTemplate(*serverName))
	writeSignedPair(*outDir, "client", caTemplate, caKey, &x509.Certificate{
		SerialNumber: mustSerial(),
		Subject:      pkix.Name{CommonName: "telesrv CoreExec smoke edge"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	fmt.Printf("ca=%s\nserver=%s\nclient=%s\n", filepath.Join(*outDir, "ca.pem"), filepath.Join(*outDir, "server.pem"), filepath.Join(*outDir, "client.pem"))
}

func serverTemplate(name string) *x509.Certificate {
	t := &x509.Certificate{
		SerialNumber: mustSerial(),
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if ip := net.ParseIP(name); ip != nil {
		t.IPAddresses = []net.IP{ip}
	} else {
		t.DNSNames = []string{name}
	}
	return t
}

func writeSignedPair(outDir, name string, ca *x509.Certificate, caKey *rsa.PrivateKey, template *x509.Certificate) {
	key := mustRSA(name)
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	if err != nil {
		fatalf("create %s cert: %v", name, err)
	}
	writeCert(filepath.Join(outDir, name+".pem"), der)
	writeKey(filepath.Join(outDir, name+"-key.pem"), key)
}

func mustRSA(name string) *rsa.PrivateKey {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		fatalf("generate %s key: %v", name, err)
	}
	return key
}

func mustSerial() *big.Int {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		fatalf("generate serial: %v", err)
	}
	return serial
}

func writeCert(path string, der []byte) {
	writePEM(path, &pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func writeKey(path string, key *rsa.PrivateKey) {
	writePEM(path, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}

func writePEM(path string, block *pem.Block) {
	if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
		fatalf("write %s: %v", path, err)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
