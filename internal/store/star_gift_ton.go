package store

import (
	"context"

	"telesrv/internal/domain"
)

// StarGiftTONStore separates Core-owned externalization locks from worker-owned
// chain facts. Worker methods may update only export/asset/job rows; gift
// lifecycle and PTS finalization remain Core responsibilities.
type StarGiftTONStore interface {
	CreateTONExport(ctx context.Context, req domain.StarGiftTONExportRequest) (domain.StarGiftTONExport, error)
	TONExportByID(ctx context.Context, exportID int64) (domain.StarGiftTONExport, bool, error)
	TONExportByTokenDigest(ctx context.Context, tokenDigest []byte) (domain.StarGiftTONExport, bool, error)
	TONExportByUniqueGiftID(ctx context.Context, uniqueGiftID int64) (domain.StarGiftTONExport, bool, error)
	CreateTONProofChallenge(ctx context.Context, req domain.StarGiftTONChallengeRequest) (domain.StarGiftTONChallenge, error)
	TONProofChallengeByDigest(ctx context.Context, challengeDigest []byte) (domain.StarGiftTONChallenge, bool, error)
	VerifyTONExportProof(ctx context.Context, proof domain.StarGiftTONProofResult) (domain.StarGiftTONExport, error)
	CancelTONExport(ctx context.Context, exportID, userID int64, now int, reason string) (domain.StarGiftTONExport, error)

	RegisterTONAdmission(ctx context.Context, req domain.StarGiftTONAdmissionRequest) error
	ClaimTONJobs(ctx context.Context, workerID string, kinds []domain.StarGiftTONJobKind, now, leaseSeconds, limit int) ([]domain.StarGiftTONJob, error)
	RenewTONJob(ctx context.Context, jobID int64, workerID string, fencingToken int64, now, leaseSeconds int) error
	RecordTONItemResolved(ctx context.Context, resolution domain.StarGiftTONItemResolution) (domain.StarGiftTONExport, error)
	RecordTONMintPrepared(ctx context.Context, preparation domain.StarGiftTONMintPreparation) (domain.StarGiftTONAsset, error)
	RecordTONMintSubmitted(ctx context.Context, submission domain.StarGiftTONMintSubmission) (domain.StarGiftTONAsset, error)
	RecordTONMintObserved(ctx context.Context, observation domain.StarGiftTONMintObservation) (domain.StarGiftTONAsset, error)
	RecordTONMintConfirmed(ctx context.Context, confirmation domain.StarGiftTONMintConfirmation) (domain.StarGiftTONAsset, error)
	RecordTONOwnerObserved(ctx context.Context, observation domain.StarGiftTONOwnerObservation) (domain.StarGiftTONAsset, error)
	FailTONJob(ctx context.Context, failure domain.StarGiftTONJobFailure) error
}

// StarGiftTONFinalizerStore is Core-only. It is intentionally absent from the
// chain worker interface so a relayer process cannot mutate gift lifecycle,
// account PTS, update events, or dispatch outbox state.
type StarGiftTONFinalizerStore interface {
	ClaimTONCoreJobs(ctx context.Context, workerID string, now, leaseSeconds, limit int) ([]domain.StarGiftTONJob, error)
	RenewTONCoreJob(ctx context.Context, jobID int64, workerID string, fencingToken int64, now, leaseSeconds int) error
	FinalizeTONExport(ctx context.Context, finalization domain.StarGiftTONFinalization) (domain.StarGiftTONFinalizationResult, error)
	SyncTONOwner(ctx context.Context, finalization domain.StarGiftTONFinalization) (domain.StarGiftTONFinalizationResult, error)
	FailTONCoreJob(ctx context.Context, failure domain.StarGiftTONJobFailure) error
}

type StarGiftTONClaimStore interface {
	TONExportByGiftRef(ctx context.Context, ref string) (domain.StarGiftTONExport, domain.StarGiftTONAsset, domain.UniqueStarGift, bool, error)
	CreateTONClaimChallenge(ctx context.Context, req domain.StarGiftTONChallengeRequest) (domain.StarGiftTONChallenge, error)
	ScheduleTONOwnerCheck(ctx context.Context, exportID int64, now int) error
	CommitTONClaim(ctx context.Context, claim domain.StarGiftTONClaimCommit) (domain.StarGiftTONFinalizationResult, error)
}

// StarGiftTONRecoveryStore is an operator-only boundary. It can only requeue
// the original failed chain job for an already quarantined export.
type StarGiftTONRecoveryStore interface {
	ResumeTONExport(ctx context.Context, req domain.StarGiftTONRecoveryRequest) (domain.StarGiftTONRecoveryResult, error)
}
