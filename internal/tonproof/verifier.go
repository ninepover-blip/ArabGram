// Package tonproof verifies TON Connect proof-of-wallet ownership without
// querying an untrusted chain account. A complete state_init is mandatory.
package tonproof

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strings"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton/wallet"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"telesrv/internal/domain"
	"telesrv/internal/tonaddr"
)

type Config struct {
	Domain  string
	Network domain.TONNetwork
	MaxSkew time.Duration
}

type Request struct {
	Address         string
	StateInit       []byte
	ExpectedPayload string
	Proof           wallet.TonConnectProof
}

type Result struct {
	Address       string
	StateInitHash []byte
	PublicKey     []byte
	ProofHash     []byte
}

type Verifier struct {
	domain  string
	network domain.TONNetwork
	inner   *wallet.TonConnectVerifier
}

func New(cfg Config) (*Verifier, error) {
	domainName := strings.TrimSpace(strings.ToLower(cfg.Domain))
	if domainName == "" || len(domainName) > 253 || strings.ContainsAny(domainName, "/:\\") ||
		!cfg.Network.Valid() || cfg.MaxSkew < time.Second || cfg.MaxSkew > 10*time.Minute {
		return nil, fmt.Errorf("invalid TON proof verifier config")
	}
	return &Verifier{
		domain: domainName, network: cfg.Network,
		inner: wallet.NewTonConnectVerifier(domainName, cfg.MaxSkew, nil),
	}, nil
}

func (v *Verifier) Verify(ctx context.Context, req Request) (Result, error) {
	if v == nil || v.inner == nil || len(req.StateInit) == 0 || len(req.StateInit) > 16384 ||
		strings.TrimSpace(req.ExpectedPayload) == "" || len(req.ExpectedPayload) > 512 {
		return Result{}, domain.ErrStarGiftTONExportInvalid
	}
	addr, err := tonaddr.Parse(req.Address, v.network)
	if err != nil {
		return Result{}, domain.ErrStarGiftTONExportInvalid
	}
	if req.Proof.Domain.LengthBytes != uint32(len([]byte(req.Proof.Domain.Value))) ||
		!strings.EqualFold(req.Proof.Domain.Value, v.domain) || req.Proof.Payload != req.ExpectedPayload {
		return Result{}, domain.ErrStarGiftTONExportInvalid
	}
	stateCell, err := cell.FromBOC(req.StateInit)
	if err != nil || !equalBytes(stateCell.Hash(), addr.Data()) {
		return Result{}, domain.ErrStarGiftTONExportInvalid
	}
	var stateInit tlb.StateInit
	if err := tlb.Parse(&stateInit, stateCell); err != nil || stateInit.Code == nil || stateInit.Data == nil {
		return Result{}, domain.ErrStarGiftTONExportInvalid
	}
	if err := v.inner.VerifyProof(ctx, addr, req.Proof, req.ExpectedPayload, req.StateInit); err != nil {
		return Result{}, fmt.Errorf("verify TON proof: %w", err)
	}
	account := &tlb.Account{
		IsActive: true, Code: stateInit.Code, Data: stateInit.Data,
		State: &tlb.AccountState{AccountStorage: tlb.AccountStorage{Status: tlb.AccountStatusActive}},
	}
	version := wallet.GetWalletVersion(account)
	if version == wallet.Unknown {
		return Result{}, domain.ErrStarGiftTONExportInvalid
	}
	publicKey, err := wallet.ParsePubKeyFromData(version, stateInit.Data)
	if err != nil || len(publicKey) != 32 {
		return Result{}, domain.ErrStarGiftTONExportInvalid
	}
	proofHash := hashProof(addr, stateCell.Hash(), req.Proof)
	return Result{
		Address: addr.StringRaw(), StateInitHash: append([]byte(nil), stateCell.Hash()...),
		PublicKey: append([]byte(nil), publicKey...), ProofHash: proofHash,
	}, nil
}

func hashProof(addr *address.Address, stateInitHash []byte, proof wallet.TonConnectProof) []byte {
	h := sha256.New()
	h.Write([]byte("telesrv-ton-proof-v1\x00"))
	var number [8]byte
	binary.BigEndian.PutUint32(number[:4], uint32(addr.Workchain()))
	h.Write(number[:4])
	h.Write(addr.Data())
	h.Write(stateInitHash)
	binary.BigEndian.PutUint64(number[:], uint64(proof.Timestamp))
	h.Write(number[:])
	binary.BigEndian.PutUint32(number[:4], proof.Domain.LengthBytes)
	h.Write(number[:4])
	h.Write([]byte(strings.ToLower(proof.Domain.Value)))
	h.Write([]byte(proof.Payload))
	h.Write(proof.Signature)
	return h.Sum(nil)
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := range a {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}
