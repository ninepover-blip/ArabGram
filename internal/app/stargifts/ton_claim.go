package stargifts

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/xssnick/tonutils-go/ton/wallet"

	"telesrv/internal/domain"
	"telesrv/internal/store"
	"telesrv/internal/tonproof"
)

var (
	ErrTONClaimUnauthorized = errors.New("TON gift claim authentication failed")
	ErrTONClaimInvalid      = errors.New("TON gift claim is invalid")
	ErrTONClaimNotOwner     = errors.New("wallet is not the current NFT owner")
)

type TONClaimConfig struct {
	Network      domain.TONNetwork
	ProofDomain  string
	BotToken     string
	ChallengeTTL time.Duration
	InitDataTTL  time.Duration
}

type TONClaimService struct {
	store store.StarGiftTONClaimStore
	proof *tonproof.Verifier
	cfg   TONClaimConfig
	now   func() time.Time
}

type TONClaimChallenge struct {
	Payload       string
	ExpiresAt     int
	Gift          domain.UniqueStarGift
	WalletAddress string
	NFTAddress    string
}

type TONClaimSubmission struct {
	InitData  string
	Payload   string
	Chain     string
	Address   string
	StateInit []byte
	Proof     wallet.TonConnectProof
}

func NewTONClaimService(claimStore store.StarGiftTONClaimStore, cfg TONClaimConfig) (*TONClaimService, error) {
	cfg.BotToken = strings.TrimSpace(cfg.BotToken)
	cfg.ProofDomain = strings.ToLower(strings.TrimSpace(cfg.ProofDomain))
	if claimStore == nil || !cfg.Network.Valid() || cfg.ProofDomain == "" || cfg.BotToken == "" ||
		cfg.ChallengeTTL < time.Minute || cfg.ChallengeTTL > 10*time.Minute ||
		cfg.InitDataTTL < time.Minute || cfg.InitDataTTL > 24*time.Hour {
		return nil, fmt.Errorf("invalid TON gift claim config")
	}
	if _, _, ok := domain.ParseBotToken(cfg.BotToken); !ok {
		return nil, fmt.Errorf("invalid TON gift claim bot token")
	}
	verifier, err := tonproof.New(tonproof.Config{Domain: cfg.ProofDomain, Network: cfg.Network, MaxSkew: cfg.ChallengeTTL})
	if err != nil {
		return nil, err
	}
	return &TONClaimService{store: claimStore, proof: verifier, cfg: cfg, now: time.Now}, nil
}

func (s *TONClaimService) Resolve(ctx context.Context, ref string) (TONExportView, domain.StarGiftTONAsset, bool, error) {
	if s == nil {
		return TONExportView{}, domain.StarGiftTONAsset{}, false, nil
	}
	export, asset, gift, found, err := s.store.TONExportByGiftRef(ctx, normalizeTONGiftRef(ref))
	return TONExportView{Export: export, Gift: gift}, asset, found, err
}

func (s *TONClaimService) CreateChallenge(ctx context.Context, initData, ref string, now int) (TONClaimChallenge, error) {
	userID, err := verifyTONClaimInitData(initData, s.cfg.BotToken, s.cfg.InitDataTTL, time.Unix(int64(now), 0))
	if err != nil {
		return TONClaimChallenge{}, ErrTONClaimUnauthorized
	}
	view, asset, found, err := s.Resolve(ctx, ref)
	if err != nil || !found || view.Export.Status != domain.StarGiftTONExportFinalized || asset.OwnerAddress == "" {
		return TONClaimChallenge{}, ErrTONClaimInvalid
	}
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		return TONClaimChallenge{}, fmt.Errorf("generate TON claim challenge: %w", err)
	}
	payload := base64.RawURLEncoding.EncodeToString(random)
	digest := sha256.Sum256([]byte(payload))
	authContext := sha256.Sum256([]byte(strings.TrimSpace(initData)))
	expiresAt := int(time.Unix(int64(now), 0).Add(s.cfg.ChallengeTTL).Unix())
	if _, err := s.store.CreateTONClaimChallenge(ctx, domain.StarGiftTONChallengeRequest{
		ChallengeDigest: digest[:], Purpose: domain.StarGiftTONChallengeClaim, ExportID: view.Export.ID,
		UserID: userID, AuthContextHash: authContext[:], Network: s.cfg.Network, ProofDomain: s.cfg.ProofDomain,
		PayloadHash: digest[:], Date: now, ExpiresAt: expiresAt,
	}); err != nil {
		return TONClaimChallenge{}, err
	}
	if err := s.store.ScheduleTONOwnerCheck(ctx, view.Export.ID, now); err != nil {
		return TONClaimChallenge{}, err
	}
	return TONClaimChallenge{Payload: payload, ExpiresAt: expiresAt, Gift: view.Gift,
		WalletAddress: asset.OwnerAddress, NFTAddress: asset.ItemAddress}, nil
}

func (s *TONClaimService) Verify(ctx context.Context, submission TONClaimSubmission, now int) (domain.StarGiftTONFinalizationResult, error) {
	if s == nil || now <= 0 || submission.Chain != tonConnectChain(s.cfg.Network) || strings.TrimSpace(submission.Payload) == "" {
		return domain.StarGiftTONFinalizationResult{}, ErrTONClaimInvalid
	}
	userID, err := verifyTONClaimInitData(submission.InitData, s.cfg.BotToken, s.cfg.InitDataTTL, time.Unix(int64(now), 0))
	if err != nil {
		return domain.StarGiftTONFinalizationResult{}, ErrTONClaimUnauthorized
	}
	verified, err := s.proof.Verify(ctx, tonproof.Request{
		Address: submission.Address, StateInit: submission.StateInit, ExpectedPayload: submission.Payload, Proof: submission.Proof,
	})
	if err != nil {
		return domain.StarGiftTONFinalizationResult{}, ErrTONClaimUnauthorized
	}
	digest := sha256.Sum256([]byte(submission.Payload))
	authContext := sha256.Sum256([]byte(strings.TrimSpace(submission.InitData)))
	result, err := s.store.CommitTONClaim(ctx, domain.StarGiftTONClaimCommit{
		ChallengeDigest: digest[:], UserID: userID, AuthContextHash: authContext[:], WalletAddress: verified.Address,
		StateInitHash: verified.StateInitHash, WalletPublicKey: verified.PublicKey, ProofHash: verified.ProofHash, ClaimedAt: now,
	})
	if errors.Is(err, domain.ErrStarGiftTONExportStateConflict) {
		return domain.StarGiftTONFinalizationResult{}, ErrTONClaimNotOwner
	}
	return result, err
}

func verifyTONClaimInitData(raw, botToken string, maxAge time.Duration, now time.Time) (int64, error) {
	values, err := url.ParseQuery(strings.TrimSpace(raw))
	if err != nil || values.Get("hash") == "" {
		return 0, ErrTONClaimUnauthorized
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		if key != "hash" {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+values.Get(key))
	}
	secretMAC := hmac.New(sha256.New, []byte("WebAppData"))
	_, _ = secretMAC.Write([]byte(botToken))
	signature := hmac.New(sha256.New, secretMAC.Sum(nil))
	_, _ = signature.Write([]byte(strings.Join(parts, "\n")))
	want, err := hex.DecodeString(values.Get("hash"))
	if err != nil || !hmac.Equal(want, signature.Sum(nil)) {
		return 0, ErrTONClaimUnauthorized
	}
	authDate, err := strconv.ParseInt(values.Get("auth_date"), 10, 64)
	if err != nil || authDate <= 0 {
		return 0, ErrTONClaimUnauthorized
	}
	authTime := time.Unix(authDate, 0)
	if authTime.After(now.Add(time.Minute)) || now.Sub(authTime) > maxAge {
		return 0, ErrTONClaimUnauthorized
	}
	var user struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal([]byte(values.Get("user")), &user); err != nil || user.ID <= 0 {
		return 0, ErrTONClaimUnauthorized
	}
	return user.ID, nil
}

func normalizeTONGiftRef(raw string) string {
	raw = strings.TrimSpace(raw)
	if parsed, err := url.Parse(raw); err == nil && parsed.Host != "" {
		raw = strings.Trim(strings.TrimSpace(parsed.Path), "/")
	}
	raw = strings.TrimPrefix(raw, "nft/")
	return strings.TrimSpace(raw)
}
