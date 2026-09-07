package stargifts

import (
	"bytes"
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"

	"telesrv/internal/domain"
)

const testTONCollection = "0:2222222222222222222222222222222222222222222222222222222222222222"

var testTONCollectionCodeHash = strings.Repeat("33", 32)

func TestTONExportProviderPersistsImmutableMetadataAndReturnsDurableURL(t *testing.T) {
	gift := domain.UniqueStarGift{
		ID: 17, Title: "Crystal Star", Slug: "crystal-star-7", Num: 7,
		Owner:    domain.Peer{Type: domain.PeerTypeUser, ID: 3},
		Model:    domain.StarGiftCollectibleAttribute{Name: "Aurora"},
		Pattern:  domain.StarGiftCollectibleAttribute{Name: "Rays"},
		Backdrop: domain.StarGiftCollectibleAttribute{Name: "Night"},
	}
	store := newFakeTONExportStore(gift)
	service, err := NewTONExportService(store, store, TONExportConfig{
		PublicBaseURL: "https://gifts.example", Network: domain.TONNetworkMainnet,
		Collection: address.MustParseRawAddr(testTONCollection).String(), CollectionCodeHash: testTONCollectionCodeHash,
		MintABI: domain.StarGiftTONMintABIBasicCollectionV1, InitialItemIndex: "010", ProofDomain: "gifts.example",
		ExportTTL: time.Hour, ChallengeTTL: 2 * time.Minute, CapabilitySecret: bytes.Repeat([]byte{4}, 32),
	})
	if err != nil {
		t.Fatalf("NewTONExportService: %v", err)
	}
	result, err := service.CreateWithdrawal(context.Background(), StarGiftWithdrawalProviderRequest{
		UserID: 3, Ref: domain.SavedStarGiftRef{Owner: gift.Owner, MsgID: 9}, Date: 100, Gift: gift,
	})
	if err != nil {
		t.Fatalf("CreateWithdrawal: %v", err)
	}
	if !result.Durable || result.Withdrawal.Gift.ID != gift.ID || result.URL != "https://gifts.example/ton-gift/export#"+result.RequestID {
		t.Fatalf("withdrawal result = %+v", result)
	}
	if len(store.created.MetadataJSON) == 0 || !bytes.Contains(store.created.MetadataJSON, []byte(`"Aurora"`)) {
		t.Fatalf("metadata JSON = %s", store.created.MetadataJSON)
	}
	if store.created.CollectionAddress != testTONCollection || store.created.InitialItemIndex != "10" || len(store.created.CollectionCodeHash) != 32 {
		t.Fatalf("canonical collection admission = address:%q index:%q hash:%x", store.created.CollectionAddress,
			store.created.InitialItemIndex, store.created.CollectionCodeHash)
	}
	digest := sha256.Sum256(store.created.MetadataJSON)
	if !bytes.Equal(store.created.MetadataHash, digest[:]) || store.created.MetadataURI != "https://gifts.example/ton-gift/nft/17.json" {
		t.Fatalf("metadata evidence = uri:%q hash:%x", store.created.MetadataURI, store.created.MetadataHash)
	}
	replay, err := service.CreateWithdrawal(context.Background(), StarGiftWithdrawalProviderRequest{
		UserID: 3, Ref: domain.SavedStarGiftRef{Owner: gift.Owner, MsgID: 9}, Date: 101, Gift: gift,
	})
	if err != nil || replay.RequestID != result.RequestID {
		t.Fatalf("deterministic capability replay = %+v err=%v", replay, err)
	}
}

func TestTONExportProviderAdmissionAllowlistOnlyBlocksNewIntents(t *testing.T) {
	gift := domain.UniqueStarGift{ID: 17, Title: "Gift", Num: 1, Owner: domain.Peer{Type: domain.PeerTypeUser, ID: 3}}
	store := newFakeTONExportStore(gift)
	baseConfig := TONExportConfig{
		PublicBaseURL: "https://gifts.example", Network: domain.TONNetworkMainnet,
		Collection: testTONCollection, CollectionCodeHash: testTONCollectionCodeHash,
		MintABI: domain.StarGiftTONMintABIBasicCollectionV1, InitialItemIndex: "10", ProofDomain: "gifts.example",
		ExportTTL: time.Hour, ChallengeTTL: 2 * time.Minute, CapabilitySecret: bytes.Repeat([]byte{4}, 32),
	}
	baseConfig.AllowUserIDs = []int64{7}
	service, err := NewTONExportService(store, store, baseConfig)
	if err != nil {
		t.Fatalf("NewTONExportService denied: %v", err)
	}
	request := StarGiftWithdrawalProviderRequest{
		UserID: 3, Ref: domain.SavedStarGiftRef{Owner: gift.Owner, MsgID: 9}, Date: 100, Gift: gift,
	}
	if _, err := service.CreateWithdrawal(context.Background(), request); err != domain.ErrStarGiftWithdrawalUnavailable {
		t.Fatalf("CreateWithdrawal denied err = %v, want ErrStarGiftWithdrawalUnavailable", err)
	}

	baseConfig.AllowUserIDs = []int64{3}
	service, err = NewTONExportService(store, store, baseConfig)
	if err != nil {
		t.Fatalf("NewTONExportService allowed: %v", err)
	}
	if _, err := service.CreateWithdrawal(context.Background(), request); err != nil {
		t.Fatalf("CreateWithdrawal allowed: %v", err)
	}
}

func TestTONExportProviderRejectsChannelOwner(t *testing.T) {
	gift := domain.UniqueStarGift{
		ID: 17, Title: "Gift", Num: 1,
		Owner: domain.Peer{Type: domain.PeerTypeChannel, ID: 30},
	}
	store := newFakeTONExportStore(gift)
	service, err := NewTONExportService(store, store, TONExportConfig{
		PublicBaseURL: "https://gifts.example", Network: domain.TONNetworkMainnet,
		Collection: testTONCollection, CollectionCodeHash: testTONCollectionCodeHash,
		MintABI: domain.StarGiftTONMintABIBasicCollectionV1, InitialItemIndex: "10", ProofDomain: "gifts.example",
		ExportTTL: time.Hour, ChallengeTTL: 2 * time.Minute, CapabilitySecret: bytes.Repeat([]byte{4}, 32),
	})
	if err != nil {
		t.Fatalf("NewTONExportService: %v", err)
	}
	if _, err := service.CreateWithdrawal(context.Background(), StarGiftWithdrawalProviderRequest{
		UserID: 3, Ref: domain.SavedStarGiftRef{Owner: gift.Owner, SavedID: 9}, Date: 100, Gift: gift,
	}); err != domain.ErrStarGiftWithdrawalUnavailable {
		t.Fatalf("channel withdrawal err = %v, want ErrStarGiftWithdrawalUnavailable", err)
	}
	if store.export.ID != 0 {
		t.Fatalf("channel withdrawal created TON export %+v", store.export)
	}
}

func TestTONExportChallengeBindsBearerAndExport(t *testing.T) {
	gift := domain.UniqueStarGift{ID: 17, Title: "Gift", Num: 1, Owner: domain.Peer{Type: domain.PeerTypeUser, ID: 3}}
	store := newFakeTONExportStore(gift)
	service, err := NewTONExportService(store, store, TONExportConfig{
		PublicBaseURL: "https://gifts.example", Network: domain.TONNetworkMainnet,
		Collection: testTONCollection, CollectionCodeHash: testTONCollectionCodeHash,
		MintABI: domain.StarGiftTONMintABIBasicCollectionV1, InitialItemIndex: "10", ProofDomain: "gifts.example",
		ExportTTL: time.Hour, ChallengeTTL: 2 * time.Minute, CapabilitySecret: bytes.Repeat([]byte{4}, 32),
	})
	if err != nil {
		t.Fatalf("NewTONExportService: %v", err)
	}
	service.now = func() time.Time { return time.Unix(109, 0) }
	created, err := service.CreateWithdrawal(context.Background(), StarGiftWithdrawalProviderRequest{
		UserID: 3, Ref: domain.SavedStarGiftRef{Owner: gift.Owner, MsgID: 9}, Date: 100, Gift: gift,
	})
	if err != nil {
		t.Fatalf("CreateWithdrawal: %v", err)
	}
	store.export.Status = domain.StarGiftTONExportAwaitingWallet
	challenge, err := service.CreateProofChallenge(context.Background(), created.RequestID, 110)
	if err != nil {
		t.Fatalf("CreateProofChallenge: %v", err)
	}
	if challenge.Domain != "gifts.example" || challenge.Payload == "" || store.challenge.ExportID != store.export.ID {
		t.Fatalf("challenge = %+v stored=%+v", challenge, store.challenge)
	}
	wantContext := sha256.Sum256([]byte(created.RequestID))
	if !bytes.Equal(store.challenge.AuthContextHash, wantContext[:]) {
		t.Fatal("challenge is not bound to bearer digest")
	}
}

func TestTONExportResolveExpiresAwaitingWalletIntent(t *testing.T) {
	gift := domain.UniqueStarGift{ID: 17, Title: "Gift", Num: 1, Owner: domain.Peer{Type: domain.PeerTypeUser, ID: 3}}
	store := newFakeTONExportStore(gift)
	service, err := NewTONExportService(store, store, TONExportConfig{
		PublicBaseURL: "https://gifts.example", Network: domain.TONNetworkMainnet,
		Collection: testTONCollection, CollectionCodeHash: testTONCollectionCodeHash,
		MintABI: domain.StarGiftTONMintABIBasicCollectionV1, InitialItemIndex: "10", ProofDomain: "gifts.example",
		ExportTTL: time.Hour, ChallengeTTL: 2 * time.Minute, CapabilitySecret: bytes.Repeat([]byte{4}, 32),
	})
	if err != nil {
		t.Fatalf("NewTONExportService: %v", err)
	}
	created, err := service.CreateWithdrawal(context.Background(), StarGiftWithdrawalProviderRequest{
		UserID: 3, Ref: domain.SavedStarGiftRef{Owner: gift.Owner, MsgID: 9}, Date: 100, Gift: gift,
	})
	if err != nil {
		t.Fatalf("CreateWithdrawal: %v", err)
	}
	store.export.Status = domain.StarGiftTONExportAwaitingWallet
	service.now = func() time.Time { return time.Unix(int64(store.export.ExpiresAt+1), 0) }
	view, found, err := service.Resolve(context.Background(), created.RequestID)
	if err != nil || !found || view.Export.Status != domain.StarGiftTONExportFailed || view.Export.LastError == "" {
		t.Fatalf("Resolve expired export = %+v found=%v err=%v", view, found, err)
	}
	restarted, err := service.CreateWithdrawal(context.Background(), StarGiftWithdrawalProviderRequest{
		UserID: 3, Ref: domain.SavedStarGiftRef{Owner: gift.Owner, MsgID: 9}, Date: store.export.ExpiresAt + 2, Gift: gift,
	})
	if err != nil || restarted.RequestID == created.RequestID {
		t.Fatalf("restart expired export = %+v err=%v", restarted, err)
	}
	if _, found, err := service.Resolve(context.Background(), created.RequestID); err != nil || found {
		t.Fatalf("old capability after restart found=%v err=%v", found, err)
	}
}

type fakeTONExportStore struct {
	gift      domain.UniqueStarGift
	created   domain.StarGiftTONExportRequest
	export    domain.StarGiftTONExport
	challenge domain.StarGiftTONChallengeRequest
}

func newFakeTONExportStore(gift domain.UniqueStarGift) *fakeTONExportStore {
	return &fakeTONExportStore{gift: gift}
}

func (s *fakeTONExportStore) CreateTONExport(_ context.Context, req domain.StarGiftTONExportRequest) (domain.StarGiftTONExport, error) {
	if s.export.ID == 0 {
		s.created = req
		s.export = domain.StarGiftTONExport{
			ID: 5, UniqueGiftID: s.gift.ID, OwnerUserID: req.UserID, Network: req.Network,
			CollectionAddress: req.CollectionAddress, MintABI: req.MintABI,
			MetadataURI: req.MetadataURI, MetadataHash: req.MetadataHash, MetadataJSON: req.MetadataJSON,
			Status: domain.StarGiftTONExportAwaitingWallet, CreatedAt: req.Date, ExpiresAt: req.ExpiresAt,
		}
	} else if s.export.Status == domain.StarGiftTONExportFailed && !bytes.Equal(s.created.TokenDigest, req.TokenDigest) {
		s.created = req
		s.export.Status = domain.StarGiftTONExportAwaitingWallet
		s.export.ExpiresAt = req.ExpiresAt
		s.export.LastError = ""
	}
	return s.export, nil
}

func (s *fakeTONExportStore) TONExportByTokenDigest(_ context.Context, digest []byte) (domain.StarGiftTONExport, bool, error) {
	return s.export, s.export.ID != 0 && bytes.Equal(digest, s.created.TokenDigest), nil
}

func (s *fakeTONExportStore) TONExportByUniqueGiftID(_ context.Context, uniqueGiftID int64) (domain.StarGiftTONExport, bool, error) {
	return s.export, s.export.UniqueGiftID == uniqueGiftID, nil
}

func (s *fakeTONExportStore) CancelTONExport(_ context.Context, exportID, userID int64, now int, reason string) (domain.StarGiftTONExport, error) {
	if s.export.ID != exportID || s.export.OwnerUserID != userID || s.export.Status != domain.StarGiftTONExportAwaitingWallet {
		return domain.StarGiftTONExport{}, domain.ErrStarGiftTONExportStateConflict
	}
	s.export.Status = domain.StarGiftTONExportFailed
	s.export.LastError = reason
	return s.export, nil
}

func (s *fakeTONExportStore) CreateTONProofChallenge(_ context.Context, req domain.StarGiftTONChallengeRequest) (domain.StarGiftTONChallenge, error) {
	s.challenge = req
	return domain.StarGiftTONChallenge{ID: 8, ExportID: req.ExportID, Status: "pending"}, nil
}

func (s *fakeTONExportStore) TONProofChallengeByDigest(context.Context, []byte) (domain.StarGiftTONChallenge, bool, error) {
	return domain.StarGiftTONChallenge{}, false, nil
}

func (s *fakeTONExportStore) VerifyTONExportProof(context.Context, domain.StarGiftTONProofResult) (domain.StarGiftTONExport, error) {
	return s.export, nil
}

func (s *fakeTONExportStore) UniqueByID(_ context.Context, id int64) (domain.UniqueStarGift, bool, error) {
	return s.gift, s.gift.ID == id, nil
}
