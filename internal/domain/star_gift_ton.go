package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
)

type TONNetwork string

const (
	TONNetworkMainnet TONNetwork = "mainnet"
	TONNetworkTestnet TONNetwork = "testnet"

	StarGiftTONMintABIBasicCollectionV1 = "basic-collection-v1"
)

func ValidStarGiftTONMintABI(value string) bool {
	return value == StarGiftTONMintABIBasicCollectionV1
}

func (n TONNetwork) Valid() bool {
	return n == TONNetworkMainnet || n == TONNetworkTestnet
}

type StarGiftTONExportStatus string

const (
	StarGiftTONExportResolvingItem  StarGiftTONExportStatus = "resolving_item"
	StarGiftTONExportAwaitingWallet StarGiftTONExportStatus = "awaiting_wallet"
	StarGiftTONExportProofVerified  StarGiftTONExportStatus = "proof_verified"
	StarGiftTONExportMintQueued     StarGiftTONExportStatus = "mint_queued"
	StarGiftTONExportPrepared       StarGiftTONExportStatus = "prepared"
	StarGiftTONExportSubmitted      StarGiftTONExportStatus = "submitted"
	StarGiftTONExportConfirmed      StarGiftTONExportStatus = "confirmed"
	StarGiftTONExportFinalized      StarGiftTONExportStatus = "finalized"
	StarGiftTONExportFailed         StarGiftTONExportStatus = "failed"
	StarGiftTONExportQuarantined    StarGiftTONExportStatus = "quarantined"
)

func (s StarGiftTONExportStatus) Valid() bool {
	switch s {
	case StarGiftTONExportResolvingItem, StarGiftTONExportAwaitingWallet, StarGiftTONExportProofVerified, StarGiftTONExportMintQueued, StarGiftTONExportPrepared,
		StarGiftTONExportSubmitted, StarGiftTONExportConfirmed, StarGiftTONExportFinalized,
		StarGiftTONExportFailed, StarGiftTONExportQuarantined:
		return true
	default:
		return false
	}
}

func (s StarGiftTONExportStatus) PossiblySubmitted() bool {
	return s == StarGiftTONExportPrepared || s == StarGiftTONExportSubmitted || s == StarGiftTONExportConfirmed ||
		s == StarGiftTONExportFinalized || s == StarGiftTONExportQuarantined
}

type StarGiftTONExportRequest struct {
	UserID             int64
	Ref                SavedStarGiftRef
	TokenDigest        []byte
	Network            TONNetwork
	CollectionAddress  string
	CollectionCodeHash []byte
	MintABI            string
	InitialItemIndex   string
	MetadataURI        string
	MetadataHash       []byte
	MetadataJSON       []byte
	Date               int
	ExpiresAt          int
}

func (r StarGiftTONExportRequest) Valid() bool {
	metadataDigest := sha256.Sum256(r.MetadataJSON)
	return r.UserID > 0 && r.Ref.Valid() && r.Ref.Owner == (Peer{Type: PeerTypeUser, ID: r.UserID}) &&
		len(r.TokenDigest) == 32 && r.Network.Valid() && strings.TrimSpace(r.CollectionAddress) != "" &&
		len(r.CollectionCodeHash) == 32 && ValidStarGiftTONMintABI(r.MintABI) &&
		validTONLogicalTime(r.InitialItemIndex) && strings.TrimSpace(r.MetadataURI) != "" &&
		len(r.MetadataHash) == 32 && len(r.MetadataJSON) >= 2 && len(r.MetadataJSON) <= 65536 &&
		bytes.Equal(r.MetadataHash, metadataDigest[:]) && r.Date > 0 && r.ExpiresAt > r.Date
}

type StarGiftTONChainState struct {
	Network               TONNetwork
	CollectionAddress     string
	CollectionCodeHash    []byte
	ObservedNextItemIndex string
}

func (s StarGiftTONChainState) Valid() bool {
	return s.Network.Valid() && strings.TrimSpace(s.CollectionAddress) != "" && len(s.CollectionCodeHash) == 32 &&
		validTONLogicalTime(s.ObservedNextItemIndex)
}

type StarGiftTONAdmissionRequest struct {
	StarGiftTONChainState
	InitialItemIndex string
	MintABI          string
	WorkerInstanceID string
	MintEnabled      bool
	ObservedAt       int
	LeaseExpiresAt   int
}

func (r StarGiftTONAdmissionRequest) Valid() bool {
	return r.StarGiftTONChainState.Valid() && validTONLogicalTime(r.InitialItemIndex) && ValidStarGiftTONMintABI(r.MintABI) &&
		strings.TrimSpace(r.WorkerInstanceID) != "" && len(strings.TrimSpace(r.WorkerInstanceID)) <= 128 &&
		r.ObservedAt > 0 && r.LeaseExpiresAt > r.ObservedAt
}

type StarGiftTONExport struct {
	ID                  int64
	UniqueGiftID        int64
	OwnerUserID         int64
	Network             TONNetwork
	CollectionAddress   string
	MintABI             string
	ItemIndex           string
	ExpectedItemAddress string
	MetadataURI         string
	MetadataHash        []byte
	MetadataJSON        []byte
	WalletAddress       string
	WalletStateInitHash []byte
	WalletPublicKey     []byte
	ProofHash           []byte
	Status              StarGiftTONExportStatus
	CreatedAt           int
	ExpiresAt           int
	ProofVerifiedAt     int
	SubmittedAt         int
	ConfirmedAt         int
	FinalizedAt         int
	Version             int64
	LastError           string
}

type StarGiftTONProofResult struct {
	ExportID            int64
	ChallengeDigest     []byte
	WalletAddress       string
	WalletStateInitHash []byte
	WalletPublicKey     []byte
	ProofHash           []byte
	VerifiedAt          int
}

func (r StarGiftTONProofResult) Valid() bool {
	return r.ExportID > 0 && len(r.ChallengeDigest) == 32 && strings.TrimSpace(r.WalletAddress) != "" && len(r.WalletStateInitHash) == 32 &&
		len(r.WalletPublicKey) == 32 && len(r.ProofHash) == 32 && r.VerifiedAt > 0
}

type StarGiftTONChallengePurpose string

const (
	StarGiftTONChallengeExport StarGiftTONChallengePurpose = "export"
	StarGiftTONChallengeClaim  StarGiftTONChallengePurpose = "claim"
)

type StarGiftTONChallengeRequest struct {
	ChallengeDigest []byte
	Purpose         StarGiftTONChallengePurpose
	ExportID        int64
	UserID          int64
	AuthContextHash []byte
	Network         TONNetwork
	ProofDomain     string
	PayloadHash     []byte
	Date            int
	ExpiresAt       int
}

func (r StarGiftTONChallengeRequest) Valid() bool {
	purposeShape := (r.Purpose == StarGiftTONChallengeExport || r.Purpose == StarGiftTONChallengeClaim) && r.ExportID > 0
	return len(r.ChallengeDigest) == 32 && purposeShape && r.UserID > 0 && len(r.AuthContextHash) == 32 &&
		r.Network.Valid() && strings.TrimSpace(r.ProofDomain) != "" && len(r.PayloadHash) == 32 &&
		r.Date > 0 && r.ExpiresAt > r.Date
}

type StarGiftTONChallenge struct {
	ID              int64
	ChallengeDigest []byte
	Purpose         StarGiftTONChallengePurpose
	ExportID        int64
	UserID          int64
	AuthContextHash []byte
	Network         TONNetwork
	ProofDomain     string
	PayloadHash     []byte
	WalletAddress   string
	StateInitHash   []byte
	WalletPublicKey []byte
	ProofHash       []byte
	Status          string
	CreatedAt       int
	ExpiresAt       int
	VerifiedAt      int
	ConsumedAt      int
}

type StarGiftTONAsset struct {
	ExportID          int64
	Network           TONNetwork
	CollectionAddress string
	ItemAddress       string
	OwnerAddress      string
	MetadataHash      []byte
	SubmissionHash    []byte
	SubmissionBOC     []byte
	MintTxHash        []byte
	MintTxLT          string
	MintBlockSeqno    int64
	FinalityDepth     int
	PreparedAt        int
	ValidUntil        int
	SubmittedAt       int
	ConfirmedAt       int
	OwnerObservedLT   string
	OwnerObservedAt   int
}

type StarGiftTONJobKind string

const (
	StarGiftTONJobResolveItem    StarGiftTONJobKind = "resolve_item"
	StarGiftTONJobMint           StarGiftTONJobKind = "mint"
	StarGiftTONJobConfirm        StarGiftTONJobKind = "confirm"
	StarGiftTONJobReconcileOwner StarGiftTONJobKind = "reconcile_owner"
	StarGiftTONJobFinalize       StarGiftTONJobKind = "finalize"
	StarGiftTONJobSyncOwner      StarGiftTONJobKind = "sync_owner"
)

type StarGiftTONItemResolution struct {
	JobID        int64
	WorkerID     string
	FencingToken int64
	ItemAddress  string
	ResolvedAt   int
}

func (r StarGiftTONItemResolution) Valid() bool {
	return r.JobID > 0 && strings.TrimSpace(r.WorkerID) != "" && r.FencingToken > 0 &&
		strings.TrimSpace(r.ItemAddress) != "" && r.ResolvedAt > 0
}

type StarGiftTONJobStatus string

const (
	StarGiftTONJobPending   StarGiftTONJobStatus = "pending"
	StarGiftTONJobLeased    StarGiftTONJobStatus = "leased"
	StarGiftTONJobCompleted StarGiftTONJobStatus = "completed"
	StarGiftTONJobFailed    StarGiftTONJobStatus = "failed"
)

type StarGiftTONJob struct {
	ID             int64
	ExportID       int64
	Key            string
	Kind           StarGiftTONJobKind
	Status         StarGiftTONJobStatus
	AvailableAt    int
	LeaseOwner     string
	LeaseExpiresAt int
	FencingToken   int64
	Attempts       int
	MaxAttempts    int
	LastError      string
	CreatedAt      int
	CompletedAt    int
	Export         StarGiftTONExport
	Asset          *StarGiftTONAsset
}

type StarGiftTONMintPreparation struct {
	JobID          int64
	WorkerID       string
	FencingToken   int64
	SubmissionHash []byte
	SubmissionBOC  []byte
	PreparedAt     int
	ValidUntil     int
}

func (p StarGiftTONMintPreparation) Valid() bool {
	return p.JobID > 0 && strings.TrimSpace(p.WorkerID) != "" && p.FencingToken > 0 && len(p.SubmissionHash) == 32 &&
		len(p.SubmissionBOC) > 0 && len(p.SubmissionBOC) <= 65536 && p.PreparedAt > 0 && p.ValidUntil > p.PreparedAt
}

type StarGiftTONMintSubmission struct {
	JobID        int64
	WorkerID     string
	FencingToken int64
	TxHash       []byte
	TxLT         string
	BlockSeqno   int64
	SubmittedAt  int
}

func (s StarGiftTONMintSubmission) Valid() bool {
	return s.JobID > 0 && strings.TrimSpace(s.WorkerID) != "" && s.FencingToken > 0 &&
		((len(s.TxHash) == 0 && s.TxLT == "" && s.BlockSeqno == 0) ||
			(len(s.TxHash) == 32 && validTONLogicalTime(s.TxLT) && s.BlockSeqno >= 0)) && s.SubmittedAt > 0
}

type StarGiftTONMintObservation struct {
	JobID        int64
	WorkerID     string
	FencingToken int64
	TxHash       []byte
	TxLT         string
	BlockSeqno   int64
	ObservedAt   int
}

func (o StarGiftTONMintObservation) Valid() bool {
	return o.JobID > 0 && strings.TrimSpace(o.WorkerID) != "" && o.FencingToken > 0 &&
		len(o.TxHash) == 32 && validTONLogicalTime(o.TxLT) && o.BlockSeqno > 0 && o.ObservedAt > 0
}

type StarGiftTONMintConfirmation struct {
	JobID         int64
	WorkerID      string
	FencingToken  int64
	OwnerAddress  string
	TxHash        []byte
	TxLT          string
	BlockSeqno    int64
	FinalityDepth int
	ConfirmedAt   int
}

type StarGiftTONOwnerObservation struct {
	JobID        int64
	WorkerID     string
	FencingToken int64
	OwnerAddress string
	ObservedLT   string
	ObservedAt   int
	NextCheckAt  int
}

func (o StarGiftTONOwnerObservation) Valid() bool {
	return o.JobID > 0 && strings.TrimSpace(o.WorkerID) != "" && o.FencingToken > 0 &&
		strings.TrimSpace(o.OwnerAddress) != "" && validTONLogicalTime(o.ObservedLT) &&
		o.ObservedAt > 0 && o.NextCheckAt > o.ObservedAt
}

func (c StarGiftTONMintConfirmation) Valid() bool {
	return c.JobID > 0 && strings.TrimSpace(c.WorkerID) != "" && c.FencingToken > 0 &&
		strings.TrimSpace(c.OwnerAddress) != "" && len(c.TxHash) == 32 && validTONLogicalTime(c.TxLT) &&
		c.BlockSeqno >= 0 && c.FinalityDepth > 0 && c.ConfirmedAt > 0
}

type StarGiftTONJobFailure struct {
	JobID        int64
	WorkerID     string
	FencingToken int64
	Error        string
	FailedAt     int
	RetryAt      int
	Terminal     bool
}

type StarGiftTONRecoveryRequest struct {
	ExportID           int64
	Network            TONNetwork
	CollectionAddress  string
	CollectionCodeHash []byte
	MintABI            string
	InitialItemIndex   string
	Actor              string
	Reason             string
	RecoveredAt        int
}

func (r StarGiftTONRecoveryRequest) Valid() bool {
	actor := strings.TrimSpace(r.Actor)
	reason := strings.TrimSpace(r.Reason)
	return r.ExportID > 0 && r.Network.Valid() && strings.TrimSpace(r.CollectionAddress) != "" &&
		len(r.CollectionCodeHash) == 32 && ValidStarGiftTONMintABI(r.MintABI) && validTONLogicalTime(r.InitialItemIndex) &&
		len(actor) >= 1 && len(actor) <= 128 && len(reason) >= 1 && len(reason) <= 500 && r.RecoveredAt > 0
}

type StarGiftTONRecoveryResult struct {
	RecoveryID int64
	Export     StarGiftTONExport
	Job        StarGiftTONJob
}

type StarGiftTONFinalization struct {
	JobID        int64
	WorkerID     string
	FencingToken int64
	FinalizedAt  int
}

func (f StarGiftTONFinalization) Valid() bool {
	return f.JobID > 0 && strings.TrimSpace(f.WorkerID) != "" && f.FencingToken > 0 && f.FinalizedAt > 0
}

type StarGiftTONFinalizationResult struct {
	Export       StarGiftTONExport
	Asset        StarGiftTONAsset
	Gift         UniqueStarGift
	PreviousHost Peer
}

type StarGiftTONClaimCommit struct {
	ChallengeDigest []byte
	UserID          int64
	AuthContextHash []byte
	WalletAddress   string
	StateInitHash   []byte
	WalletPublicKey []byte
	ProofHash       []byte
	ClaimedAt       int
}

func (c StarGiftTONClaimCommit) Valid() bool {
	return len(c.ChallengeDigest) == 32 && c.UserID > 0 && len(c.AuthContextHash) == 32 && strings.TrimSpace(c.WalletAddress) != "" &&
		len(c.StateInitHash) == 32 && len(c.WalletPublicKey) == 32 && len(c.ProofHash) == 32 && c.ClaimedAt > 0
}

func validTONLogicalTime(value string) bool {
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

func DecodeTONHash(value string) ([]byte, error) {
	value = strings.TrimSpace(strings.TrimPrefix(value, "0x"))
	decoders := []func(string) ([]byte, error){
		hex.DecodeString, base64.StdEncoding.DecodeString, base64.RawStdEncoding.DecodeString,
		base64.URLEncoding.DecodeString, base64.RawURLEncoding.DecodeString,
	}
	for _, decode := range decoders {
		if data, err := decode(value); err == nil && len(data) == 32 {
			return data, nil
		}
	}
	return nil, fmt.Errorf("expected a 32-byte hex or base64 hash")
}
