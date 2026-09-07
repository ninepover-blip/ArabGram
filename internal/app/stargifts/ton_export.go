package stargifts

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/xssnick/tonutils-go/ton/wallet"

	"telesrv/internal/domain"
	"telesrv/internal/tonaddr"
	"telesrv/internal/tonproof"
)

const maxTONCapabilityTokenBytes = 128

type TONExportConfig struct {
	PublicBaseURL      string
	Network            domain.TONNetwork
	Collection         string
	CollectionCodeHash string
	MintABI            string
	InitialItemIndex   string
	ProofDomain        string
	ExportTTL          time.Duration
	ChallengeTTL       time.Duration
	CapabilitySecret   []byte
	AllowUserIDs       []int64
}

type TONExportService struct {
	store              tonExportStore
	gifts              uniqueGiftStore
	proof              *tonproof.Verifier
	cfg                TONExportConfig
	allowUserIDs       map[int64]struct{}
	publicBaseURL      string
	collectionCodeHash []byte
	now                func() time.Time
}

type tonExportStore interface {
	CreateTONExport(ctx context.Context, req domain.StarGiftTONExportRequest) (domain.StarGiftTONExport, error)
	TONExportByTokenDigest(ctx context.Context, tokenDigest []byte) (domain.StarGiftTONExport, bool, error)
	TONExportByUniqueGiftID(ctx context.Context, uniqueGiftID int64) (domain.StarGiftTONExport, bool, error)
	CancelTONExport(ctx context.Context, exportID, userID int64, now int, reason string) (domain.StarGiftTONExport, error)
	CreateTONProofChallenge(ctx context.Context, req domain.StarGiftTONChallengeRequest) (domain.StarGiftTONChallenge, error)
	TONProofChallengeByDigest(ctx context.Context, challengeDigest []byte) (domain.StarGiftTONChallenge, bool, error)
	VerifyTONExportProof(ctx context.Context, proof domain.StarGiftTONProofResult) (domain.StarGiftTONExport, error)
}

type uniqueGiftStore interface {
	UniqueByID(ctx context.Context, id int64) (domain.UniqueStarGift, bool, error)
}

type TONExportView struct {
	Export domain.StarGiftTONExport
	Gift   domain.UniqueStarGift
}

type TONProofChallenge struct {
	Export    domain.StarGiftTONExport
	Payload   string
	Domain    string
	ExpiresAt int
}

type TONProofSubmission struct {
	Payload   string
	Chain     string
	Address   string
	StateInit []byte
	Proof     wallet.TonConnectProof
}

func NewTONExportService(tonStore tonExportStore, gifts uniqueGiftStore, cfg TONExportConfig) (*TONExportService, error) {
	base := strings.TrimRight(strings.TrimSpace(cfg.PublicBaseURL), "/")
	parsed, err := url.Parse(base)
	if tonStore == nil || gifts == nil || err != nil || parsed.Scheme != "https" || parsed.Host == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || !cfg.Network.Valid() ||
		strings.TrimSpace(cfg.Collection) == "" || strings.TrimSpace(cfg.CollectionCodeHash) == "" ||
		!domain.ValidStarGiftTONMintABI(strings.TrimSpace(cfg.MintABI)) || !validTONIndex(cfg.InitialItemIndex) ||
		cfg.ExportTTL < 5*time.Minute || cfg.ExportTTL > 24*time.Hour ||
		cfg.ChallengeTTL < time.Minute || cfg.ChallengeTTL > 10*time.Minute || len(cfg.CapabilitySecret) < 32 {
		return nil, fmt.Errorf("invalid TON Star Gift export config")
	}
	collection, err := tonaddr.Parse(cfg.Collection, cfg.Network)
	if err != nil {
		return nil, fmt.Errorf("invalid TON Star Gift collection address")
	}
	collectionCodeHash, err := domain.DecodeTONHash(cfg.CollectionCodeHash)
	if err != nil {
		return nil, fmt.Errorf("invalid TON Star Gift collection code hash: %w", err)
	}
	initialItemIndex, err := strconv.ParseUint(strings.TrimSpace(cfg.InitialItemIndex), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid TON Star Gift initial item index")
	}
	proofVerifier, err := tonproof.New(tonproof.Config{Domain: cfg.ProofDomain, Network: cfg.Network, MaxSkew: cfg.ChallengeTTL})
	if err != nil {
		return nil, err
	}
	cfg.Collection = collection.StringRaw()
	cfg.MintABI = strings.TrimSpace(cfg.MintABI)
	cfg.InitialItemIndex = strconv.FormatUint(initialItemIndex, 10)
	cfg.ProofDomain = strings.ToLower(strings.TrimSpace(cfg.ProofDomain))
	allowUserIDs := make(map[int64]struct{}, len(cfg.AllowUserIDs))
	for _, userID := range cfg.AllowUserIDs {
		if userID <= 0 {
			return nil, fmt.Errorf("invalid TON Star Gift export allow user ID")
		}
		if _, exists := allowUserIDs[userID]; exists {
			return nil, fmt.Errorf("duplicate TON Star Gift export allow user ID %d", userID)
		}
		allowUserIDs[userID] = struct{}{}
	}
	return &TONExportService{
		store: tonStore, gifts: gifts, proof: proofVerifier, cfg: cfg, allowUserIDs: allowUserIDs, publicBaseURL: base,
		collectionCodeHash: collectionCodeHash, now: time.Now,
	}, nil
}

func (s *TONExportService) Name() string { return "telesrv-ton" }

func (s *TONExportService) CreateWithdrawal(ctx context.Context, req StarGiftWithdrawalProviderRequest) (StarGiftWithdrawalProviderResult, error) {
	if s == nil || req.UserID <= 0 || !req.Ref.Valid() || req.Date <= 0 || req.Gift.ID <= 0 || req.Gift.Owner != req.Ref.Owner {
		return StarGiftWithdrawalProviderResult{}, domain.ErrStarGiftWithdrawalUnavailable
	}
	if req.Ref.Owner != (domain.Peer{Type: domain.PeerTypeUser, ID: req.UserID}) {
		return StarGiftWithdrawalProviderResult{}, domain.ErrStarGiftWithdrawalUnavailable
	}
	if len(s.allowUserIDs) > 0 {
		if _, allowed := s.allowUserIDs[req.UserID]; !allowed {
			return StarGiftWithdrawalProviderResult{}, domain.ErrStarGiftWithdrawalUnavailable
		}
	}
	expiresAt := int(time.Unix(int64(req.Date), 0).Add(s.cfg.ExportTTL).Unix())
	existing, found, err := s.store.TONExportByUniqueGiftID(ctx, req.Gift.ID)
	if err != nil {
		return StarGiftWithdrawalProviderResult{}, err
	}
	if found && existing.Status != domain.StarGiftTONExportFailed &&
		!(existing.Status == domain.StarGiftTONExportAwaitingWallet && existing.ExpiresAt <= req.Date) {
		expiresAt = existing.ExpiresAt
	}
	token := s.capabilityToken(req.UserID, req.Gift.ID, expiresAt)
	tokenDigest := sha256.Sum256([]byte(token))
	metadataURI := s.publicBaseURL + "/ton-gift/nft/" + strconv.FormatInt(req.Gift.ID, 10) + ".json"
	metadataJSON, err := buildTONMetadata(s.publicBaseURL, req.Gift)
	if err != nil {
		return StarGiftWithdrawalProviderResult{}, err
	}
	metadataHash := sha256.Sum256(metadataJSON)
	export, err := s.store.CreateTONExport(ctx, domain.StarGiftTONExportRequest{
		UserID: req.UserID, Ref: req.Ref, TokenDigest: tokenDigest[:], Network: s.cfg.Network,
		CollectionAddress: s.cfg.Collection, CollectionCodeHash: s.collectionCodeHash, InitialItemIndex: s.cfg.InitialItemIndex,
		MintABI:     s.cfg.MintABI,
		MetadataURI: metadataURI, MetadataHash: metadataHash[:], MetadataJSON: metadataJSON,
		Date: req.Date, ExpiresAt: expiresAt,
	})
	if err != nil {
		if errors.Is(err, domain.ErrStarGiftTONAdmissionUnavailable) {
			return StarGiftWithdrawalProviderResult{}, domain.ErrStarGiftWithdrawalUnavailable
		}
		return StarGiftWithdrawalProviderResult{}, err
	}
	exportURL := s.publicBaseURL + "/ton-gift/export#" + token
	withdrawal := domain.StarGiftWithdrawal{
		ProviderRequestID: token, URL: exportURL, ExpiresAt: export.ExpiresAt,
		Status: string(export.Status), Gift: req.Gift,
	}
	return StarGiftWithdrawalProviderResult{
		RequestID: token, URL: exportURL, ExpiresAt: export.ExpiresAt, Durable: true, Withdrawal: withdrawal,
	}, nil
}

func (s *TONExportService) Resolve(ctx context.Context, token string) (TONExportView, bool, error) {
	token = strings.TrimSpace(token)
	if s == nil || token == "" || len(token) > maxTONCapabilityTokenBytes {
		return TONExportView{}, false, nil
	}
	digest := sha256.Sum256([]byte(token))
	export, found, err := s.store.TONExportByTokenDigest(ctx, digest[:])
	if err != nil || !found {
		return TONExportView{}, false, err
	}
	now := int(s.now().Unix())
	if export.Status == domain.StarGiftTONExportAwaitingWallet && export.ExpiresAt <= now {
		export, err = s.store.CancelTONExport(ctx, export.ID, export.OwnerUserID, now, "TON wallet proof window expired")
		if err != nil && !errors.Is(err, domain.ErrStarGiftTONExportStateConflict) {
			return TONExportView{}, false, err
		}
		if errors.Is(err, domain.ErrStarGiftTONExportStateConflict) {
			export, found, err = s.store.TONExportByTokenDigest(ctx, digest[:])
			if err != nil || !found {
				return TONExportView{}, false, err
			}
		}
	}
	gift, found, err := s.gifts.UniqueByID(ctx, export.UniqueGiftID)
	if err != nil || !found {
		return TONExportView{}, false, err
	}
	return TONExportView{Export: export, Gift: gift}, true, nil
}

func (s *TONExportService) Metadata(ctx context.Context, uniqueGiftID int64) ([]byte, bool, error) {
	export, found, err := s.store.TONExportByUniqueGiftID(ctx, uniqueGiftID)
	if err != nil || !found {
		return nil, false, err
	}
	return append([]byte(nil), export.MetadataJSON...), true, nil
}

func (s *TONExportService) Media(ctx context.Context, uniqueGiftID int64) (domain.Document, bool, error) {
	if _, found, err := s.store.TONExportByUniqueGiftID(ctx, uniqueGiftID); err != nil || !found {
		return domain.Document{}, false, err
	}
	gift, found, err := s.gifts.UniqueByID(ctx, uniqueGiftID)
	if err != nil || !found {
		return domain.Document{}, false, err
	}
	if gift.Model.Document != nil {
		return *gift.Model.Document, true, nil
	}
	if gift.Pattern.Document != nil {
		return *gift.Pattern.Document, true, nil
	}
	return domain.Document{}, false, nil
}

func (s *TONExportService) CreateProofChallenge(ctx context.Context, token string, now int) (TONProofChallenge, error) {
	view, found, err := s.Resolve(ctx, token)
	if err != nil || !found || now <= 0 || view.Export.Status != domain.StarGiftTONExportAwaitingWallet || view.Export.ExpiresAt <= now {
		return TONProofChallenge{}, domain.ErrStarGiftTONExportStateConflict
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return TONProofChallenge{}, fmt.Errorf("generate TON proof challenge: %w", err)
	}
	payload := base64.RawURLEncoding.EncodeToString(raw)
	challengeDigest := sha256.Sum256([]byte(payload))
	authContext := sha256.Sum256([]byte(strings.TrimSpace(token)))
	expiresAt := int(time.Unix(int64(now), 0).Add(s.cfg.ChallengeTTL).Unix())
	_, err = s.store.CreateTONProofChallenge(ctx, domain.StarGiftTONChallengeRequest{
		ChallengeDigest: challengeDigest[:], Purpose: domain.StarGiftTONChallengeExport,
		ExportID: view.Export.ID, UserID: view.Export.OwnerUserID, AuthContextHash: authContext[:],
		Network: view.Export.Network, ProofDomain: s.cfg.ProofDomain, PayloadHash: challengeDigest[:],
		Date: now, ExpiresAt: expiresAt,
	})
	if err != nil {
		return TONProofChallenge{}, err
	}
	return TONProofChallenge{Export: view.Export, Payload: payload, Domain: s.cfg.ProofDomain, ExpiresAt: expiresAt}, nil
}

func (s *TONExportService) VerifyProof(ctx context.Context, token string, submission TONProofSubmission, now int) (domain.StarGiftTONExport, error) {
	view, found, err := s.Resolve(ctx, token)
	if err != nil || !found || now <= 0 {
		return domain.StarGiftTONExport{}, domain.ErrStarGiftTONExportInvalid
	}
	if submission.Chain != tonConnectChain(view.Export.Network) {
		return domain.StarGiftTONExport{}, domain.ErrStarGiftTONExportInvalid
	}
	challengeDigest := sha256.Sum256([]byte(submission.Payload))
	authContext := sha256.Sum256([]byte(strings.TrimSpace(token)))
	challenge, found, err := s.store.TONProofChallengeByDigest(ctx, challengeDigest[:])
	if err != nil || !found || challenge.ExportID != view.Export.ID || challenge.UserID != view.Export.OwnerUserID ||
		challenge.Purpose != domain.StarGiftTONChallengeExport || challenge.Network != view.Export.Network ||
		!strings.EqualFold(challenge.ProofDomain, s.cfg.ProofDomain) || challenge.Status != "pending" ||
		challenge.ExpiresAt <= now || !hmac.Equal(challenge.PayloadHash, challengeDigest[:]) ||
		!hmac.Equal(challenge.AuthContextHash, authContext[:]) {
		return domain.StarGiftTONExport{}, domain.ErrStarGiftTONExportStateConflict
	}
	proofResult, err := s.proof.Verify(ctx, tonproof.Request{
		Address: submission.Address, StateInit: submission.StateInit, ExpectedPayload: submission.Payload, Proof: submission.Proof,
	})
	if err != nil {
		return domain.StarGiftTONExport{}, domain.ErrStarGiftTONExportInvalid
	}
	return s.store.VerifyTONExportProof(ctx, domain.StarGiftTONProofResult{
		ExportID: view.Export.ID, ChallengeDigest: challengeDigest[:], WalletAddress: proofResult.Address,
		WalletStateInitHash: proofResult.StateInitHash, WalletPublicKey: proofResult.PublicKey,
		ProofHash: proofResult.ProofHash, VerifiedAt: now,
	})
}

func tonConnectChain(network domain.TONNetwork) string {
	if network == domain.TONNetworkTestnet {
		return "-3"
	}
	return "-239"
}

func (s *TONExportService) capabilityToken(userID, uniqueGiftID int64, expiresAt int) string {
	mac := hmac.New(sha256.New, s.cfg.CapabilitySecret)
	mac.Write([]byte("telesrv-ton-export-v1\x00"))
	var ids [24]byte
	binary.BigEndian.PutUint64(ids[:8], uint64(userID))
	binary.BigEndian.PutUint64(ids[8:], uint64(uniqueGiftID))
	binary.BigEndian.PutUint64(ids[16:], uint64(expiresAt))
	mac.Write(ids[:])
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

type tonMetadata struct {
	Name         string                 `json:"name"`
	Description  string                 `json:"description"`
	Image        string                 `json:"image"`
	AnimationURL string                 `json:"animation_url,omitempty"`
	Attributes   []tonMetadataAttribute `json:"attributes"`
}

type tonMetadataAttribute struct {
	TraitType string `json:"trait_type"`
	Value     any    `json:"value"`
}

func buildTONMetadata(baseURL string, gift domain.UniqueStarGift) ([]byte, error) {
	mediaURL := baseURL + "/ton-gift/nft/" + strconv.FormatInt(gift.ID, 10) + "/media"
	metadata := tonMetadata{
		Name:        gift.Title + " #" + strconv.Itoa(gift.Num),
		Description: "A Telegram collectible gift exported by telesrv.",
		Image:       mediaURL,
		Attributes: []tonMetadataAttribute{
			{TraitType: "Model", Value: gift.Model.Name},
			{TraitType: "Pattern", Value: gift.Pattern.Name},
			{TraitType: "Backdrop", Value: gift.Backdrop.Name},
			{TraitType: "Number", Value: gift.Num},
		},
	}
	if gift.Model.Document != nil && strings.Contains(strings.ToLower(gift.Model.Document.MimeType), "tgsticker") {
		metadata.AnimationURL = mediaURL
	}
	return json.Marshal(metadata)
}

func validTONIndex(value string) bool {
	value = strings.TrimSpace(value)
	if len(value) == 0 || len(value) > 20 {
		return false
	}
	for _, ch := range value {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

var _ StarGiftWithdrawalProvider = (*TONExportService)(nil)
