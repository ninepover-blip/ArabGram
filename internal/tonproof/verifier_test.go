package tonproof

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton/wallet"

	"telesrv/internal/domain"
)

func TestVerifierAcceptsBoundV2ProofForFriendlyAndRawAddresses(t *testing.T) {
	verifier, err := New(Config{Domain: "gifts.example", Network: domain.TONNetworkMainnet, MaxSkew: time.Minute})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	request, publicKey := signedProofRequest(t, "gifts.example", "export-challenge")
	result, err := verifier.Verify(context.Background(), request)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if result.Address == "" || !bytes.Equal(result.PublicKey, publicKey) || len(result.StateInitHash) != 32 || len(result.ProofHash) != 32 {
		t.Fatalf("unexpected proof result: %+v", result)
	}
	request.Address = result.Address
	rawResult, err := verifier.Verify(context.Background(), request)
	if err != nil {
		t.Fatalf("Verify raw address: %v", err)
	}
	if rawResult.Address != result.Address || !bytes.Equal(rawResult.ProofHash, result.ProofHash) {
		t.Fatalf("raw and friendly addresses produced different evidence: friendly=%+v raw=%+v", result, rawResult)
	}

	request.Proof.Domain.LengthBytes++
	if _, err := verifier.Verify(context.Background(), request); err == nil {
		t.Fatal("domain byte-length mismatch accepted")
	}
}

func TestVerifierRejectsTestnetFriendlyAddressOnMainnet(t *testing.T) {
	verifier, err := New(Config{Domain: "gifts.example", Network: domain.TONNetworkMainnet, MaxSkew: time.Minute})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	request, _ := signedProofRequest(t, "gifts.example", "export-challenge")
	addr, err := address.ParseAddr(request.Address)
	if err != nil {
		t.Fatalf("ParseAddr: %v", err)
	}
	addr.SetTestnetOnly(true)
	request.Address = addr.String()
	if _, err := verifier.Verify(context.Background(), request); err == nil {
		t.Fatal("testnet-only friendly address accepted on mainnet")
	}
}

func TestVerifierRejectsChallengeReplay(t *testing.T) {
	verifier, err := New(Config{Domain: "gifts.example", Network: domain.TONNetworkMainnet, MaxSkew: time.Minute})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	request, _ := signedProofRequest(t, "gifts.example", "challenge-a")
	request.ExpectedPayload = "challenge-b"
	if _, err := verifier.Verify(context.Background(), request); err == nil {
		t.Fatal("proof replayed against another challenge")
	}
}

func signedProofRequest(t *testing.T, proofDomain, payload string) (Request, ed25519.PublicKey) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	stateInit, err := wallet.GetStateInit(publicKey, wallet.V4R2, wallet.DefaultSubwallet)
	if err != nil {
		t.Fatalf("GetStateInit: %v", err)
	}
	stateCell, err := tlb.ToCell(stateInit)
	if err != nil {
		t.Fatalf("state init cell: %v", err)
	}
	addr := address.NewAddress(0, 0, stateCell.Hash())
	proof := wallet.TonConnectProof{Timestamp: time.Now().Unix(), Payload: payload}
	proof.Domain.Value = proofDomain
	proof.Domain.LengthBytes = uint32(len([]byte(proofDomain)))
	proof.Signature = ed25519.Sign(privateKey, proofSigningHash(addr, proof))
	return Request{
		Address: addr.String(), StateInit: stateCell.ToBOC(), ExpectedPayload: payload, Proof: proof,
	}, publicKey
}

func proofSigningHash(addr *address.Address, proof wallet.TonConnectProof) []byte {
	var message bytes.Buffer
	message.WriteString("ton-proof-item-v2/")
	_ = binary.Write(&message, binary.BigEndian, addr.Workchain())
	message.Write(addr.Data())
	_ = binary.Write(&message, binary.LittleEndian, proof.Domain.LengthBytes)
	message.WriteString(proof.Domain.Value)
	_ = binary.Write(&message, binary.LittleEndian, proof.Timestamp)
	message.WriteString(proof.Payload)
	messageHash := sha256.Sum256(message.Bytes())
	full := append([]byte{0xff, 0xff}, []byte("ton-connect")...)
	full = append(full, messageHash[:]...)
	hash := sha256.Sum256(full)
	return hash[:]
}
