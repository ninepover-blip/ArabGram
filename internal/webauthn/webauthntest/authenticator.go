// Package webauthntest 提供一个纯 Go 软件 authenticator,模拟平台 passkey 设备
// 生成符合 WebAuthn 规范的 attestation(注册)与 assertion(登录)。
//
// 它与被测的 internal/webauthn 是「同一规范的两份独立实现」:authenticator 按规范
// 构造字节并用真私钥签名,服务端按规范验证。e2e/单测通过即说明双方对规范的理解一致。
// 仅供测试使用(被 _test.go 导入)。
package webauthntest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"

	"github.com/fxamacker/cbor/v2"
)

const (
	flagUserPresent      = 0x01
	flagUserVerified     = 0x04
	flagAttestedCredData = 0x40
)

var b64 = base64.RawURLEncoding

// Authenticator 是一个 ES256(ECDSA P-256)软件 authenticator,持有一对密钥与一个
// 凭据 id,可重复用于注册与多次登录(签名计数器递增)。
type Authenticator struct {
	priv      *ecdsa.PrivateKey
	credID    []byte
	aaguid    []byte
	signCount uint32
}

// New 生成一个新的软件 authenticator(随机 ES256 密钥 + 随机 credential id)。
func New() (*Authenticator, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	credID := make([]byte, 32)
	if _, err := rand.Read(credID); err != nil {
		return nil, err
	}
	aaguid := make([]byte, 16) // 全 0 aaguid(与多数 platform passkey 一致)
	return &Authenticator{priv: priv, credID: credID, aaguid: aaguid}, nil
}

// CredentialID 返回 credential id 原始字节。
func (a *Authenticator) CredentialID() []byte { return append([]byte(nil), a.credID...) }

// CredentialIDB64 返回 base64url(无填充) 的 credential id(对应 TL 的 ID/RawID string)。
func (a *Authenticator) CredentialIDB64() string { return b64.EncodeToString(a.credID) }

type clientData struct {
	Type        string `json:"type"`
	Challenge   string `json:"challenge"`
	Origin      string `json:"origin"`
	CrossOrigin bool   `json:"crossOrigin"`
}

func (a *Authenticator) clientDataJSON(typ, origin string, challenge []byte) []byte {
	out, _ := json.Marshal(clientData{Type: typ, Challenge: b64.EncodeToString(challenge), Origin: origin})
	return out
}

func (a *Authenticator) cosePublicKey() []byte {
	x := make([]byte, 32)
	y := make([]byte, 32)
	a.priv.PublicKey.X.FillBytes(x)
	a.priv.PublicKey.Y.FillBytes(y)
	key := struct {
		Kty int    `cbor:"1,keyasint"`
		Alg int    `cbor:"3,keyasint"`
		Crv int    `cbor:"-1,keyasint"`
		X   []byte `cbor:"-2,keyasint"`
		Y   []byte `cbor:"-3,keyasint"`
	}{Kty: 2, Alg: -7, Crv: 1, X: x, Y: y}
	out, _ := cbor.Marshal(key)
	return out
}

func (a *Authenticator) authData(rpID string, flags byte, includeCred bool) []byte {
	h := sha256.Sum256([]byte(rpID))
	buf := make([]byte, 0, 128)
	buf = append(buf, h[:]...)
	buf = append(buf, flags)
	var counter [4]byte
	binary.BigEndian.PutUint32(counter[:], a.signCount)
	buf = append(buf, counter[:]...)
	if includeCred {
		buf = append(buf, a.aaguid...)
		var l [2]byte
		binary.BigEndian.PutUint16(l[:], uint16(len(a.credID)))
		buf = append(buf, l[:]...)
		buf = append(buf, a.credID...)
		buf = append(buf, a.cosePublicKey()...)
	}
	return buf
}

// Register 模拟 createCredential,返回 clientDataJSON 与 attestationObject(fmt=none)。
func (a *Authenticator) Register(rpID, origin string, challenge []byte) (clientDataJSON, attestationObject []byte, err error) {
	cd := a.clientDataJSON("webauthn.create", origin, challenge)
	ad := a.authData(rpID, flagUserPresent|flagUserVerified|flagAttestedCredData, true)
	att := struct {
		Fmt      string         `cbor:"fmt"`
		AttStmt  map[string]any `cbor:"attStmt"`
		AuthData []byte         `cbor:"authData"`
	}{Fmt: "none", AttStmt: map[string]any{}, AuthData: ad}
	obj, err := cbor.Marshal(att)
	if err != nil {
		return nil, nil, err
	}
	return cd, obj, nil
}

// Assert 模拟 getCredential,返回 clientDataJSON、authenticatorData 与 signature。
// 每次调用签名计数器 +1。
func (a *Authenticator) Assert(rpID, origin string, challenge []byte) (clientDataJSON, authenticatorData, signature []byte, err error) {
	a.signCount++
	cd := a.clientDataJSON("webauthn.get", origin, challenge)
	ad := a.authData(rpID, flagUserPresent|flagUserVerified, false)
	cdHash := sha256.Sum256(cd)
	signed := append(append([]byte(nil), ad...), cdHash[:]...)
	digest := sha256.Sum256(signed)
	sig, err := ecdsa.SignASN1(rand.Reader, a.priv, digest[:])
	if err != nil {
		return nil, nil, nil, err
	}
	return cd, ad, sig, nil
}
