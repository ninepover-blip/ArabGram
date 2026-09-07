package mtprotoedge

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// rsaKeyBits 是 server RSA 私钥位数。MTProto 要求 2048-bit。
const rsaKeyBits = 2048

// LoadOrGenerateRSAKey 从 path 加载 PEM 编码的 server RSA 私钥；
// 不存在则生成 2048-bit 新密钥并持久化（含父目录）。
//
// server RSA 私钥用于 MTProto 密钥交换；其公钥 fingerprint 需 patch 进 TDesktop
// （记录于 docs/tdesktop-patch-notes.md）。
func LoadOrGenerateRSAKey(path string) (*rsa.PrivateKey, error) {
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		key, perr := parseRSAKeyPEM(data)
		if perr != nil {
			return nil, fmt.Errorf("parse %q: %w", path, perr)
		}
		return key, nil
	case errors.Is(err, os.ErrNotExist):
		// 继续生成。
	default:
		return nil, fmt.Errorf("read %q: %w", path, err)
	}

	key, err := rsa.GenerateKey(rand.Reader, rsaKeyBits)
	if err != nil {
		return nil, fmt.Errorf("generate rsa key: %w", err)
	}

	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("create key dir %q: %w", dir, err)
		}
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(key),
	})
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		return nil, fmt.Errorf("write key %q: %w", path, err)
	}
	return key, nil
}

func parseRSAKeyPEM(data []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, errors.New("no PEM block found")
	}
	return x509.ParsePKCS1PrivateKey(block.Bytes)
}
