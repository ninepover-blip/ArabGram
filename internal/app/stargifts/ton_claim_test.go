package stargifts

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton/wallet"

	"telesrv/internal/domain"
)

const tonClaimTestBotToken = "900001:claim-test-secret"

func TestTONClaimAuthenticatesMiniAppAndBindsFreshOwnerProof(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	store := &fakeTONClaimStore{
		export: domain.StarGiftTONExport{ID: 7, UniqueGiftID: 17, Network: domain.TONNetworkMainnet, Status: domain.StarGiftTONExportFinalized},
		asset:  domain.StarGiftTONAsset{ExportID: 7, OwnerAddress: "0:owner", ItemAddress: "0:item"},
		gift:   domain.UniqueStarGift{ID: 17, Title: "Crystal", Slug: "crystal-1", Num: 1},
	}
	service, err := NewTONClaimService(store, TONClaimConfig{
		Network: domain.TONNetworkMainnet, ProofDomain: "gifts.example", BotToken: tonClaimTestBotToken,
		ChallengeTTL: 2 * time.Minute, InitDataTTL: 15 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewTONClaimService: %v", err)
	}
	initData := signedTONClaimInitData(t, tonClaimTestBotToken, 44, now, "query-a")
	challenge, err := service.CreateChallenge(context.Background(), initData, "https://gifts.example/nft/crystal-1", int(now.Unix()))
	if err != nil {
		t.Fatalf("CreateChallenge: %v", err)
	}
	if challenge.Payload == "" || challenge.NFTAddress != store.asset.ItemAddress || store.challenge.UserID != 44 ||
		store.challenge.ExportID != store.export.ID || store.scheduledExportID != store.export.ID {
		t.Fatalf("challenge=%+v stored=%+v schedule=%d", challenge, store.challenge, store.scheduledExportID)
	}
	wantAuth := sha256.Sum256([]byte(initData))
	if !bytes.Equal(store.challenge.AuthContextHash, wantAuth[:]) {
		t.Fatal("claim challenge is not bound to signed Mini App initData")
	}

	proofRequest := signedTONClaimProof(t, "gifts.example", challenge.Payload, now)
	result, err := service.Verify(context.Background(), TONClaimSubmission{
		InitData: initData, Payload: challenge.Payload, Chain: "-239", Address: proofRequest.Address,
		StateInit: proofRequest.StateInit, Proof: proofRequest.Proof,
	}, int(now.Unix()))
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if store.commit.UserID != 44 || store.commit.WalletAddress == "" || len(store.commit.StateInitHash) != 32 ||
		len(store.commit.WalletPublicKey) != 32 || len(store.commit.ProofHash) != 32 || result.Gift.Host.ID != 44 {
		t.Fatalf("commit=%+v result=%+v", store.commit, result)
	}
}

func TestTONClaimRejectsStaleOrReboundMiniAppData(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	store := &fakeTONClaimStore{
		export: domain.StarGiftTONExport{ID: 7, Network: domain.TONNetworkMainnet, Status: domain.StarGiftTONExportFinalized},
		asset:  domain.StarGiftTONAsset{ExportID: 7, OwnerAddress: "0:owner", ItemAddress: "0:item"},
		gift:   domain.UniqueStarGift{ID: 17, Slug: "gift-1"},
	}
	service, err := NewTONClaimService(store, TONClaimConfig{
		Network: domain.TONNetworkMainnet, ProofDomain: "gifts.example", BotToken: tonClaimTestBotToken,
		ChallengeTTL: time.Minute, InitDataTTL: 5 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	stale := signedTONClaimInitData(t, tonClaimTestBotToken, 44, now.Add(-6*time.Minute), "stale")
	if _, err := service.CreateChallenge(context.Background(), stale, "gift-1", int(now.Unix())); !errors.Is(err, ErrTONClaimUnauthorized) {
		t.Fatalf("stale initData err=%v", err)
	}
	valid := signedTONClaimInitData(t, tonClaimTestBotToken, 44, now, "valid")
	if _, err := service.CreateChallenge(context.Background(), valid+"x", "gift-1", int(now.Unix())); !errors.Is(err, ErrTONClaimUnauthorized) {
		t.Fatalf("rebound initData err=%v", err)
	}
}

type fakeTONClaimStore struct {
	export            domain.StarGiftTONExport
	asset             domain.StarGiftTONAsset
	gift              domain.UniqueStarGift
	challenge         domain.StarGiftTONChallengeRequest
	commit            domain.StarGiftTONClaimCommit
	scheduledExportID int64
}

func (s *fakeTONClaimStore) TONExportByGiftRef(context.Context, string) (domain.StarGiftTONExport, domain.StarGiftTONAsset, domain.UniqueStarGift, bool, error) {
	return s.export, s.asset, s.gift, s.export.ID > 0, nil
}

func (s *fakeTONClaimStore) CreateTONClaimChallenge(_ context.Context, challenge domain.StarGiftTONChallengeRequest) (domain.StarGiftTONChallenge, error) {
	s.challenge = challenge
	return domain.StarGiftTONChallenge{ID: 8, ExportID: challenge.ExportID, CreatedAt: challenge.Date, ExpiresAt: challenge.ExpiresAt}, nil
}

func (s *fakeTONClaimStore) ScheduleTONOwnerCheck(_ context.Context, exportID int64, _ int) error {
	s.scheduledExportID = exportID
	return nil
}

func (s *fakeTONClaimStore) CommitTONClaim(_ context.Context, commit domain.StarGiftTONClaimCommit) (domain.StarGiftTONFinalizationResult, error) {
	s.commit = commit
	gift := s.gift
	gift.Host = domain.Peer{Type: domain.PeerTypeUser, ID: commit.UserID}
	return domain.StarGiftTONFinalizationResult{Export: s.export, Asset: s.asset, Gift: gift}, nil
}

func signedTONClaimInitData(t *testing.T, token string, userID int64, authTime time.Time, queryID string) string {
	t.Helper()
	values := url.Values{
		"auth_date": {strconv.FormatInt(authTime.Unix(), 10)},
		"query_id":  {queryID},
		"user":      {`{"id":` + strconv.FormatInt(userID, 10) + `,"first_name":"Test"}`},
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+values.Get(key))
	}
	secret := hmac.New(sha256.New, []byte("WebAppData"))
	_, _ = secret.Write([]byte(token))
	signature := hmac.New(sha256.New, secret.Sum(nil))
	_, _ = signature.Write([]byte(strings.Join(parts, "\n")))
	values.Set("hash", hex.EncodeToString(signature.Sum(nil)))
	return values.Encode()
}

type tonClaimProofFixture struct {
	Address   string
	StateInit []byte
	Proof     wallet.TonConnectProof
}

func signedTONClaimProof(t *testing.T, proofDomain, payload string, now time.Time) tonClaimProofFixture {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	stateInit, err := wallet.GetStateInit(publicKey, wallet.V4R2, wallet.DefaultSubwallet)
	if err != nil {
		t.Fatal(err)
	}
	stateCell, err := tlb.ToCell(stateInit)
	if err != nil {
		t.Fatal(err)
	}
	addr := address.NewAddress(0, 0, stateCell.Hash())
	proof := wallet.TonConnectProof{Timestamp: now.Unix(), Payload: payload}
	proof.Domain.Value, proof.Domain.LengthBytes = proofDomain, uint32(len([]byte(proofDomain)))
	proof.Signature = ed25519.Sign(privateKey, tonClaimProofSigningHash(addr, proof))
	return tonClaimProofFixture{Address: addr.String(), StateInit: stateCell.ToBOC(), Proof: proof}
}

func tonClaimProofSigningHash(addr *address.Address, proof wallet.TonConnectProof) []byte {
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
