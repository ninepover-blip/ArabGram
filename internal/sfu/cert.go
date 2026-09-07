package sfu

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/pion/dtls/v3/pkg/crypto/selfsign"
)

// newDTLSCertificate 生成进程级自签证书（DTLS 身份不靠 CA，靠信令面下发的指纹）。
func newDTLSCertificate() (tls.Certificate, string, error) {
	cert, err := selfsign.GenerateSelfSigned()
	if err != nil {
		return tls.Certificate{}, "", fmt.Errorf("sfu: self-signed cert: %w", err)
	}
	fp, err := certificateFingerprint(cert.Certificate[0])
	if err != nil {
		return tls.Certificate{}, "", err
	}
	return cert, fp, nil
}

// certificateFingerprint 计算 RFC 4572 风格 sha-256 指纹（"AA:BB:..."）。
func certificateFingerprint(der []byte) (string, error) {
	if _, err := x509.ParseCertificate(der); err != nil {
		return "", fmt.Errorf("sfu: parse cert: %w", err)
	}
	sum := sha256.Sum256(der)
	parts := make([]string, len(sum))
	for i, b := range sum {
		parts[i] = strings.ToUpper(hex.EncodeToString([]byte{b}))
	}
	return strings.Join(parts, ":"), nil
}

// normalizeFingerprint 去除大小写/分隔差异后比较用。
func normalizeFingerprint(fp string) string {
	return strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(fp), ":", ""))
}
