package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
	storepkg "telesrv/internal/store"
	"telesrv/internal/store/postgres/sqlcgen"
)

type StarGiftTONStore struct {
	db sqlcgen.DBTX
}

var _ storepkg.StarGiftTONStore = (*StarGiftTONStore)(nil)
var _ storepkg.StarGiftTONRecoveryStore = (*StarGiftTONStore)(nil)

func NewStarGiftTONStore(db sqlcgen.DBTX) *StarGiftTONStore {
	return &StarGiftTONStore{db: db}
}

func (s *StarGiftTONStore) CreateTONExport(ctx context.Context, req domain.StarGiftTONExportRequest) (domain.StarGiftTONExport, error) {
	if s == nil || s.db == nil || !req.Valid() {
		return domain.StarGiftTONExport{}, domain.ErrStarGiftTONExportInvalid
	}
	var out domain.StarGiftTONExport
	err := withTx(ctx, s.db, "create TON star gift export", func(tx pgx.Tx) error {
		if err := lockUsersForUpdate(ctx, tx, req.UserID); err != nil {
			return err
		}
		if err := lockStarGiftProfiles(ctx, tx, req.Ref.Owner); err != nil {
			return err
		}
		saved, err := lockSavedStarGiftForUpgrade(ctx, tx, req.Ref)
		if err != nil {
			return err
		}
		if saved.Owner != req.Ref.Owner || !saved.LifecycleStatus.Live() || saved.UniqueGiftID == 0 || saved.CanExportAt > req.Date {
			return domain.ErrStarGiftTransferUnavailable
		}
		if _, err := tx.Exec(ctx, `SELECT id FROM unique_star_gifts WHERE id=$1 FOR UPDATE`, saved.UniqueGiftID); err != nil {
			return err
		}
		unique, found, err := NewStarGiftStore(tx).UniqueByID(ctx, saved.UniqueGiftID)
		if err != nil {
			return err
		}
		if !found || unique.Burned || unique.OwnerAddress != "" || unique.Owner != saved.Owner {
			return domain.ErrStarGiftExternalizationPending
		}

		existing, existingFound, err := tonExportByPredicateForUpdate(ctx, tx, "unique_gift_id=$1", unique.ID)
		if err != nil {
			return err
		}
		var existingDigest []byte
		if existingFound {
			if err := tx.QueryRow(ctx, `SELECT token_digest FROM star_gift_ton_exports WHERE id=$1`, existing.ID).Scan(&existingDigest); err != nil {
				return err
			}
		}
		if unique.ExternalizationPending {
			if !existingFound {
				return domain.ErrStarGiftTONExportStateConflict
			}
			expiredAwaitingWallet := existing.Status == domain.StarGiftTONExportAwaitingWallet && existing.ExpiresAt <= req.Date
			if !expiredAwaitingWallet {
				if !bytes.Equal(existingDigest, req.TokenDigest) {
					return domain.ErrStarGiftExternalizationPending
				}
				out = existing
				return nil
			}
			if existing.ItemIndex != "" {
				return domain.ErrStarGiftTONExportStateConflict
			}
			if bytes.Equal(existingDigest, req.TokenDigest) {
				return domain.ErrStarGiftTONExportStateConflict
			}
		}
		if err := ensureNoStarGiftMarketConflict(ctx, tx, unique.ID); err != nil {
			if errors.Is(err, domain.ErrStarGiftTransferUnavailable) {
				return domain.ErrStarGiftTONExportStateConflict
			}
			return err
		}
		var localWithdrawalPending bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(
		 SELECT 1 FROM star_gift_withdrawal_requests
		 WHERE unique_gift_id=$1 AND status='pending' AND expires_at>$2
		)`, unique.ID, req.Date).Scan(&localWithdrawalPending); err != nil {
			return err
		}
		if localWithdrawalPending {
			return domain.ErrStarGiftTONExportStateConflict
		}
		if err := requireTONAdmission(ctx, tx, req.Network, strings.TrimSpace(req.CollectionAddress), req.CollectionCodeHash,
			req.MintABI, req.InitialItemIndex, req.Date, false); err != nil {
			return err
		}

		if existingFound {
			if existing.Status != domain.StarGiftTONExportFailed &&
				!(existing.Status == domain.StarGiftTONExportAwaitingWallet && existing.ExpiresAt <= req.Date) {
				return domain.ErrStarGiftTONExportStateConflict
			}
			if bytes.Equal(existingDigest, req.TokenDigest) {
				return domain.ErrStarGiftTONExportStateConflict
			}
			var hasAsset bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM star_gift_ton_assets WHERE export_id=$1)`, existing.ID).Scan(&hasAsset); err != nil {
				return err
			}
			if hasAsset || existing.ItemIndex != "" || existing.ExpectedItemAddress != "" || !sameTONExportIntent(existing, req) {
				return domain.ErrStarGiftTONExportStateConflict
			}
			if _, err := tx.Exec(ctx, `UPDATE star_gift_ton_exports SET token_digest=$2,owner_user_id=$3,wallet_address='',wallet_state_init_hash=NULL,
			 wallet_public_key=NULL,proof_hash=NULL,status='awaiting_wallet',expires_at=$4,proof_verified_at=0,submitted_at=0,
			 confirmed_at=0,finalized_at=0,version=version+1,last_error='',updated_at=now() WHERE id=$1`,
				existing.ID, req.TokenDigest, req.UserID, req.ExpiresAt); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE star_gift_ton_claims SET status='expired',updated_at=now()
			 WHERE export_id=$1 AND purpose='export' AND status='pending'`, existing.ID); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE unique_star_gifts SET externalization_pending=true,updated_at=now()
			 WHERE id=$1`, unique.ID); err != nil {
				return err
			}
			loaded, found, err := tonExportByPredicate(ctx, tx, "id=$1", existing.ID)
			if err != nil || !found {
				return err
			}
			out = loaded
			return nil
		}

		collectionAddress := strings.TrimSpace(req.CollectionAddress)
		if err := tx.QueryRow(ctx, `INSERT INTO star_gift_ton_exports(
		 unique_gift_id,owner_user_id,token_digest,network,collection_address,mint_abi,item_index,
		 metadata_uri,metadata_hash,metadata_json,status,created_at,expires_at)
		 VALUES($1,$2,$3,$4,$5,$6,'',$7,$8,$9,'awaiting_wallet',$10,$11) RETURNING id`, unique.ID, req.UserID, req.TokenDigest,
			string(req.Network), collectionAddress, req.MintABI, strings.TrimSpace(req.MetadataURI),
			req.MetadataHash, req.MetadataJSON, req.Date, req.ExpiresAt).Scan(&out.ID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE unique_star_gifts SET externalization_pending=true,updated_at=now() WHERE id=$1`, unique.ID); err != nil {
			return err
		}
		loaded, found, err := tonExportByPredicate(ctx, tx, "id=$1", out.ID)
		if err != nil || !found {
			return err
		}
		out = loaded
		return nil
	})
	if err != nil {
		return domain.StarGiftTONExport{}, err
	}
	return out, nil
}

func sameTONExportIntent(existing domain.StarGiftTONExport, req domain.StarGiftTONExportRequest) bool {
	return existing.UniqueGiftID > 0 && existing.Network == req.Network &&
		existing.CollectionAddress == strings.TrimSpace(req.CollectionAddress) && existing.MintABI == req.MintABI &&
		existing.MetadataURI == strings.TrimSpace(req.MetadataURI) &&
		bytes.Equal(existing.MetadataHash, req.MetadataHash) && bytes.Equal(existing.MetadataJSON, req.MetadataJSON)
}

func (s *StarGiftTONStore) TONExportByID(ctx context.Context, exportID int64) (domain.StarGiftTONExport, bool, error) {
	if s == nil || s.db == nil || exportID <= 0 {
		return domain.StarGiftTONExport{}, false, nil
	}
	return tonExportByPredicate(ctx, s.db, "id=$1", exportID)
}

func (s *StarGiftTONStore) TONExportByTokenDigest(ctx context.Context, tokenDigest []byte) (domain.StarGiftTONExport, bool, error) {
	if s == nil || s.db == nil || len(tokenDigest) != 32 {
		return domain.StarGiftTONExport{}, false, nil
	}
	return tonExportByPredicate(ctx, s.db, "token_digest=$1", tokenDigest)
}

func (s *StarGiftTONStore) TONExportByUniqueGiftID(ctx context.Context, uniqueGiftID int64) (domain.StarGiftTONExport, bool, error) {
	if s == nil || s.db == nil || uniqueGiftID <= 0 {
		return domain.StarGiftTONExport{}, false, nil
	}
	return tonExportByPredicate(ctx, s.db, "unique_gift_id=$1", uniqueGiftID)
}

func (s *StarGiftTONStore) CreateTONProofChallenge(ctx context.Context, req domain.StarGiftTONChallengeRequest) (domain.StarGiftTONChallenge, error) {
	if s == nil || s.db == nil || !req.Valid() {
		return domain.StarGiftTONChallenge{}, domain.ErrStarGiftTONExportInvalid
	}
	var out domain.StarGiftTONChallenge
	err := withTx(ctx, s.db, "create TON proof challenge", func(tx pgx.Tx) error {
		if req.Purpose == domain.StarGiftTONChallengeExport {
			export, found, err := tonExportByPredicateForUpdate(ctx, tx, "id=$1", req.ExportID)
			if err != nil || !found || export.OwnerUserID != req.UserID || export.Network != req.Network ||
				export.Status != domain.StarGiftTONExportAwaitingWallet || export.ExpiresAt <= req.Date {
				return domain.ErrStarGiftTONExportStateConflict
			}
			var tokenDigest []byte
			if err := tx.QueryRow(ctx, `SELECT token_digest FROM star_gift_ton_exports WHERE id=$1`, export.ID).Scan(&tokenDigest); err != nil {
				return err
			}
			if !bytes.Equal(tokenDigest, req.AuthContextHash) {
				return domain.ErrStarGiftTONExportInvalid
			}
		} else {
			export, found, err := tonExportByPredicateForUpdate(ctx, tx, "id=$1", req.ExportID)
			if err != nil || !found || export.Network != req.Network || export.Status != domain.StarGiftTONExportFinalized {
				return domain.ErrStarGiftTONExportStateConflict
			}
		}
		if err := tx.QueryRow(ctx, `INSERT INTO star_gift_ton_claims(
		 challenge_digest,purpose,export_id,user_id,auth_context_hash,network,proof_domain,
		 proof_payload_hash,created_at,expires_at)
		 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id`, req.ChallengeDigest, string(req.Purpose),
			req.ExportID, req.UserID, req.AuthContextHash, string(req.Network), strings.ToLower(strings.TrimSpace(req.ProofDomain)),
			req.PayloadHash, req.Date, req.ExpiresAt).Scan(&out.ID); err != nil {
			return err
		}
		loaded, found, err := tonProofChallengeByPredicate(ctx, tx, "id=$1", out.ID, false)
		if err != nil || !found {
			return err
		}
		out = loaded
		return nil
	})
	if err != nil {
		return domain.StarGiftTONChallenge{}, err
	}
	return out, nil
}

func (s *StarGiftTONStore) TONProofChallengeByDigest(ctx context.Context, challengeDigest []byte) (domain.StarGiftTONChallenge, bool, error) {
	if s == nil || s.db == nil || len(challengeDigest) != 32 {
		return domain.StarGiftTONChallenge{}, false, nil
	}
	return tonProofChallengeByPredicate(ctx, s.db, "challenge_digest=$1", challengeDigest, false)
}

func (s *StarGiftTONStore) VerifyTONExportProof(ctx context.Context, proof domain.StarGiftTONProofResult) (domain.StarGiftTONExport, error) {
	if s == nil || s.db == nil || !proof.Valid() {
		return domain.StarGiftTONExport{}, domain.ErrStarGiftTONExportInvalid
	}
	var out domain.StarGiftTONExport
	err := withTx(ctx, s.db, "verify TON star gift export proof", func(tx pgx.Tx) error {
		challenge, found, err := tonProofChallengeByPredicate(ctx, tx, "challenge_digest=$1", proof.ChallengeDigest, true)
		if err != nil || !found || challenge.Purpose != domain.StarGiftTONChallengeExport ||
			challenge.ExportID != proof.ExportID || (challenge.Status != "pending" && challenge.Status != "consumed") ||
			(challenge.Status == "pending" && challenge.ExpiresAt <= proof.VerifiedAt) {
			return domain.ErrStarGiftTONExportStateConflict
		}
		current, found, err := tonExportByPredicateForUpdate(ctx, tx, "id=$1", proof.ExportID)
		if err != nil || !found {
			return domain.ErrStarGiftTONExportInvalid
		}
		if challenge.Status == "consumed" {
			if (current.Status == domain.StarGiftTONExportProofVerified || current.Status == domain.StarGiftTONExportMintQueued || current.Status.PossiblySubmitted()) &&
				current.WalletAddress == strings.TrimSpace(proof.WalletAddress) && bytes.Equal(current.ProofHash, proof.ProofHash) &&
				challenge.WalletAddress == current.WalletAddress && bytes.Equal(challenge.ProofHash, proof.ProofHash) {
				out = current
				return nil
			}
			return domain.ErrStarGiftTONExportStateConflict
		}
		if current.Status == domain.StarGiftTONExportProofVerified || current.Status == domain.StarGiftTONExportMintQueued || current.Status.PossiblySubmitted() {
			return domain.ErrStarGiftTONExportStateConflict
		}
		if current.Status != domain.StarGiftTONExportAwaitingWallet || current.ItemIndex != "" || current.ExpectedItemAddress != "" {
			return domain.ErrStarGiftTONExportStateConflict
		}
		if current.ExpiresAt <= proof.VerifiedAt {
			return domain.ErrStarGiftTONExportExpired
		}
		if challenge.UserID != current.OwnerUserID || challenge.Network != current.Network {
			return domain.ErrStarGiftTONExportStateConflict
		}
		var admissionInitial, observedNext, admissionMintABI string
		var admitted bool
		if err := tx.QueryRow(ctx, `SELECT initial_item_index::text,observed_next_item_index::text,
		 mint_abi,mint_enabled AND lease_expires_at>$3 FROM star_gift_ton_admissions
		 WHERE network=$1 AND collection_address=$2 FOR UPDATE`, string(current.Network), current.CollectionAddress,
			proof.VerifiedAt).Scan(&admissionInitial, &observedNext, &admissionMintABI, &admitted); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return domain.ErrStarGiftTONAdmissionUnavailable
			}
			return err
		}
		if !admitted || admissionMintABI != current.MintABI {
			return domain.ErrStarGiftTONAdmissionUnavailable
		}
		var laneBusy bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM star_gift_ton_exports
		 WHERE network=$1 AND collection_address=$2 AND item_index<>'' AND status<>'finalized' AND id<>$3)`,
			string(current.Network), current.CollectionAddress, current.ID).Scan(&laneBusy); err != nil {
			return err
		}
		if laneBusy {
			return domain.ErrStarGiftTONAdmissionUnavailable
		}
		if _, err := tx.Exec(ctx, `INSERT INTO star_gift_ton_collection_cursors(network,collection_address,next_item_index)
		 VALUES($1,$2,$3::numeric) ON CONFLICT(network,collection_address) DO NOTHING`,
			string(current.Network), current.CollectionAddress, admissionInitial); err != nil {
			return err
		}
		var itemIndex string
		if err := tx.QueryRow(ctx, `SELECT next_item_index::text FROM star_gift_ton_collection_cursors
		 WHERE network=$1 AND collection_address=$2 FOR UPDATE`, string(current.Network), current.CollectionAddress).Scan(&itemIndex); err != nil {
			return err
		}
		if compareTONLogicalTime(itemIndex, observedNext) != 0 {
			return domain.ErrStarGiftTONAdmissionUnavailable
		}
		tag, err := tx.Exec(ctx, `UPDATE star_gift_ton_collection_cursors SET next_item_index=next_item_index+1,updated_at=now()
		 WHERE network=$1 AND collection_address=$2 AND next_item_index<18446744073709551615`,
			string(current.Network), current.CollectionAddress)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 1 {
			return domain.ErrStarGiftTONAdmissionUnavailable
		}
		if _, err := tx.Exec(ctx, `UPDATE star_gift_ton_exports SET
		 item_index=$2,wallet_address=$3,wallet_state_init_hash=$4,wallet_public_key=$5,proof_hash=$6,
		 status='proof_verified',proof_verified_at=$7,version=version+1,last_error='',updated_at=now()
		 WHERE id=$1`, current.ID, itemIndex, strings.TrimSpace(proof.WalletAddress), proof.WalletStateInitHash,
			proof.WalletPublicKey, proof.ProofHash, proof.VerifiedAt); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO star_gift_ton_jobs(export_id,job_key,kind,available_at,created_at)
		 VALUES($1,$2,'resolve_item',$3,$3) ON CONFLICT(job_key) DO UPDATE SET status='pending',available_at=EXCLUDED.available_at,
		 lease_owner='',lease_expires_at=0,attempts=0,last_error='',completed_at=0,updated_at=now()
		 WHERE star_gift_ton_jobs.status='failed'`, current.ID,
			fmt.Sprintf("resolve-item:%d", current.ID), proof.VerifiedAt); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE star_gift_ton_claims SET status='consumed',wallet_address=$2,
		 wallet_state_init_hash=$3,wallet_public_key=$4,proof_hash=$5,verified_at=$6,consumed_at=$6,updated_at=now()
		 WHERE id=$1`, challenge.ID, strings.TrimSpace(proof.WalletAddress), proof.WalletStateInitHash,
			proof.WalletPublicKey, proof.ProofHash, proof.VerifiedAt); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE star_gift_ton_claims SET status='expired',updated_at=now()
		 WHERE purpose='export' AND export_id=$1 AND id<>$2 AND status='pending'`, current.ID, challenge.ID); err != nil {
			return err
		}
		loaded, found, err := tonExportByPredicate(ctx, tx, "id=$1", current.ID)
		if err != nil || !found {
			return err
		}
		out = loaded
		return nil
	})
	if err != nil {
		return domain.StarGiftTONExport{}, err
	}
	return out, nil
}

func (s *StarGiftTONStore) CancelTONExport(ctx context.Context, exportID, userID int64, now int, reason string) (domain.StarGiftTONExport, error) {
	if s == nil || s.db == nil || exportID <= 0 || userID <= 0 || now <= 0 {
		return domain.StarGiftTONExport{}, domain.ErrStarGiftTONExportInvalid
	}
	var out domain.StarGiftTONExport
	err := withTx(ctx, s.db, "cancel TON star gift export", func(tx pgx.Tx) error {
		current, found, err := tonExportByPredicateForUpdate(ctx, tx, "id=$1", exportID)
		if err != nil || !found || current.OwnerUserID != userID {
			return domain.ErrStarGiftTONExportInvalid
		}
		if current.Status == domain.StarGiftTONExportFailed {
			out = current
			return nil
		}
		if current.Status != domain.StarGiftTONExportAwaitingWallet {
			return domain.ErrStarGiftTONExportStateConflict
		}
		if _, err := tx.Exec(ctx, `UPDATE star_gift_ton_exports SET status='failed',last_error=$2,
		 version=version+1,updated_at=now() WHERE id=$1`, current.ID, boundedTONError(reason)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE unique_star_gifts SET externalization_pending=false,updated_at=now()
		 WHERE id=$1 AND externalization_pending`, current.UniqueGiftID); err != nil {
			return err
		}
		loaded, found, err := tonExportByPredicate(ctx, tx, "id=$1", current.ID)
		if err != nil || !found {
			return err
		}
		out = loaded
		return nil
	})
	if err != nil {
		return domain.StarGiftTONExport{}, err
	}
	return out, nil
}

func (s *StarGiftTONStore) RegisterTONAdmission(ctx context.Context, req domain.StarGiftTONAdmissionRequest) error {
	if s == nil || s.db == nil || !req.Valid() {
		return domain.ErrStarGiftTONAdmissionUnavailable
	}
	req.CollectionAddress = strings.TrimSpace(req.CollectionAddress)
	req.WorkerInstanceID = strings.TrimSpace(req.WorkerInstanceID)
	return withTx(ctx, s.db, "register TON worker admission", func(tx pgx.Tx) error {
		var existingCodeHash []byte
		var existingInitial, existingMintABI, existingWorker string
		var existingLease int
		err := tx.QueryRow(ctx, `SELECT collection_code_hash,initial_item_index::text,mint_abi,worker_instance_id,lease_expires_at
		 FROM star_gift_ton_admissions WHERE network=$1 AND collection_address=$2 FOR UPDATE`,
			string(req.Network), req.CollectionAddress).Scan(&existingCodeHash, &existingInitial, &existingMintABI, &existingWorker, &existingLease)
		switch {
		case err == nil:
			if !bytes.Equal(existingCodeHash, req.CollectionCodeHash) || existingInitial != req.InitialItemIndex || existingMintABI != req.MintABI ||
				(existingLease > req.ObservedAt && existingWorker != req.WorkerInstanceID) {
				return domain.ErrStarGiftTONAdmissionUnavailable
			}
		case !errors.Is(err, pgx.ErrNoRows):
			return err
		}

		var cursor string
		cursorFound := true
		if err := tx.QueryRow(ctx, `SELECT next_item_index::text FROM star_gift_ton_collection_cursors
		 WHERE network=$1 AND collection_address=$2 FOR UPDATE`, string(req.Network), req.CollectionAddress).Scan(&cursor); err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				return err
			}
			cursorFound = false
		}
		var activeItemIndex string
		err = tx.QueryRow(ctx, `SELECT item_index FROM star_gift_ton_exports
		 WHERE network=$1 AND collection_address=$2 AND item_index<>'' AND status<>'finalized'`,
			string(req.Network), req.CollectionAddress).Scan(&activeItemIndex)
		activeFound := err == nil
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}

		if !cursorFound {
			if activeFound || compareTONLogicalTime(req.ObservedNextItemIndex, req.InitialItemIndex) != 0 {
				return domain.ErrStarGiftTONAdmissionUnavailable
			}
		} else if activeFound {
			if incrementTONDecimal(activeItemIndex) != cursor ||
				(compareTONLogicalTime(req.ObservedNextItemIndex, activeItemIndex) != 0 &&
					compareTONLogicalTime(req.ObservedNextItemIndex, cursor) != 0) {
				return domain.ErrStarGiftTONAdmissionUnavailable
			}
		} else if compareTONLogicalTime(req.ObservedNextItemIndex, cursor) != 0 {
			return domain.ErrStarGiftTONAdmissionUnavailable
		}

		_, err = tx.Exec(ctx, `INSERT INTO star_gift_ton_admissions(
		 network,collection_address,collection_code_hash,mint_abi,initial_item_index,observed_next_item_index,
		 worker_instance_id,mint_enabled,observed_at,lease_expires_at)
		 VALUES($1,$2,$3,$4,$5::numeric,$6::numeric,$7,$8,$9,$10)
		 ON CONFLICT(network,collection_address) DO UPDATE SET
		 observed_next_item_index=EXCLUDED.observed_next_item_index,worker_instance_id=EXCLUDED.worker_instance_id,
		 mint_enabled=EXCLUDED.mint_enabled,observed_at=EXCLUDED.observed_at,
		 lease_expires_at=EXCLUDED.lease_expires_at,updated_at=now()`,
			string(req.Network), req.CollectionAddress, req.CollectionCodeHash, req.MintABI, req.InitialItemIndex,
			req.ObservedNextItemIndex, req.WorkerInstanceID, req.MintEnabled, req.ObservedAt, req.LeaseExpiresAt)
		return err
	})
}

func requireTONAdmission(ctx context.Context, tx pgx.Tx, network domain.TONNetwork, collectionAddress string,
	collectionCodeHash []byte, mintABI, initialItemIndex string, now int, forUpdate bool,
) error {
	query := `SELECT EXISTS(SELECT 1 FROM star_gift_ton_admissions
	 WHERE network=$1 AND collection_address=$2 AND collection_code_hash=$3 AND mint_abi=$4 AND initial_item_index=$5::numeric
	 AND mint_enabled AND lease_expires_at>$6)`
	if forUpdate {
		query = `SELECT mint_enabled AND lease_expires_at>$6 AND collection_code_hash=$3 AND mint_abi=$4 AND initial_item_index=$5::numeric
		 FROM star_gift_ton_admissions WHERE network=$1 AND collection_address=$2 FOR UPDATE`
	}
	var admitted bool
	if err := tx.QueryRow(ctx, query, string(network), strings.TrimSpace(collectionAddress), collectionCodeHash, mintABI, initialItemIndex, now).Scan(&admitted); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrStarGiftTONAdmissionUnavailable
		}
		return err
	}
	if !admitted {
		return domain.ErrStarGiftTONAdmissionUnavailable
	}
	return nil
}

func (s *StarGiftTONStore) ClaimTONJobs(ctx context.Context, workerID string, kinds []domain.StarGiftTONJobKind, now, leaseSeconds, limit int) ([]domain.StarGiftTONJob, error) {
	workerID = strings.TrimSpace(workerID)
	kindNames := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		switch kind {
		case domain.StarGiftTONJobResolveItem, domain.StarGiftTONJobMint, domain.StarGiftTONJobConfirm, domain.StarGiftTONJobReconcileOwner:
			kindNames = append(kindNames, string(kind))
		default:
			return nil, domain.ErrStarGiftTONExportInvalid
		}
	}
	if s == nil || s.db == nil || workerID == "" || len(workerID) > 128 || len(kindNames) == 0 || now <= 0 || leaseSeconds <= 0 || leaseSeconds > 3600 || limit <= 0 || limit > 100 {
		return nil, domain.ErrStarGiftTONExportInvalid
	}
	rows, err := s.db.Query(ctx, `WITH candidates AS (
	 SELECT j.id FROM star_gift_ton_jobs j
	 JOIN star_gift_ton_exports e ON e.id=j.export_id
	 WHERE j.attempts<j.max_attempts
	   AND ((j.status='pending' AND j.available_at<=$1) OR (j.status='leased' AND j.lease_expires_at<=$1))
	   AND j.kind=ANY($5::text[])
	   AND ((j.kind='resolve_item' AND e.status='proof_verified') OR
	        (j.kind='mint' AND e.status IN ('mint_queued','prepared')) OR
	        (j.kind='confirm' AND e.status IN ('submitted','quarantined')) OR
	        (j.kind='reconcile_owner' AND e.status IN ('confirmed','finalized')))
	 ORDER BY j.available_at,j.id
	 FOR UPDATE OF j SKIP LOCKED LIMIT $2
	)
	UPDATE star_gift_ton_jobs j SET status='leased',lease_owner=$3,lease_expires_at=$1+$4,
	 fencing_token=j.fencing_token+1,attempts=j.attempts+1,updated_at=now()
	FROM candidates c WHERE j.id=c.id RETURNING j.id`, now, limit, workerID, leaseSeconds, kindNames)
	if err != nil {
		return nil, fmt.Errorf("claim TON jobs: %w", err)
	}
	ids := make([]int64, 0, limit)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	jobs := make([]domain.StarGiftTONJob, 0, len(ids))
	for _, id := range ids {
		job, found, err := s.tonJobByID(ctx, id)
		if err != nil {
			return nil, err
		}
		if found {
			jobs = append(jobs, job)
		}
	}
	return jobs, nil
}

func (s *StarGiftTONStore) RenewTONJob(ctx context.Context, jobID int64, workerID string, fencingToken int64, now, leaseSeconds int) error {
	if s == nil || s.db == nil || jobID <= 0 || strings.TrimSpace(workerID) == "" || fencingToken <= 0 || now <= 0 || leaseSeconds <= 0 || leaseSeconds > 3600 {
		return domain.ErrStarGiftTONLeaseLost
	}
	tag, err := s.db.Exec(ctx, `UPDATE star_gift_ton_jobs SET lease_expires_at=$5,updated_at=now()
	 WHERE id=$1 AND status='leased' AND lease_owner=$2 AND fencing_token=$3 AND lease_expires_at>$4`,
		jobID, strings.TrimSpace(workerID), fencingToken, now, now+leaseSeconds)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return domain.ErrStarGiftTONLeaseLost
	}
	return nil
}

func (s *StarGiftTONStore) RecordTONItemResolved(ctx context.Context, resolution domain.StarGiftTONItemResolution) (domain.StarGiftTONExport, error) {
	if s == nil || s.db == nil || !resolution.Valid() {
		return domain.StarGiftTONExport{}, domain.ErrStarGiftTONExportInvalid
	}
	var out domain.StarGiftTONExport
	err := withTx(ctx, s.db, "record TON NFT item address", func(tx pgx.Tx) error {
		job, export, err := lockTONJobAndExport(ctx, tx, resolution.JobID)
		if err != nil {
			return err
		}
		if job.Kind != domain.StarGiftTONJobResolveItem || job.Status != domain.StarGiftTONJobLeased ||
			job.LeaseOwner != strings.TrimSpace(resolution.WorkerID) || job.FencingToken != resolution.FencingToken ||
			job.LeaseExpiresAt <= resolution.ResolvedAt {
			return domain.ErrStarGiftTONLeaseLost
		}
		if export.Status != domain.StarGiftTONExportProofVerified || export.ItemIndex == "" || export.ExpectedItemAddress != "" {
			return domain.ErrStarGiftTONExportStateConflict
		}
		if _, err := tx.Exec(ctx, `UPDATE star_gift_ton_exports SET expected_item_address=$2,status='mint_queued',
		 version=version+1,last_error='',updated_at=now() WHERE id=$1`, export.ID, strings.TrimSpace(resolution.ItemAddress)); err != nil {
			return err
		}
		if err := completeTONJob(ctx, tx, job.ID, resolution.ResolvedAt); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO star_gift_ton_jobs(export_id,job_key,kind,available_at,created_at)
		 VALUES($1,$2,'mint',$3,$3) ON CONFLICT(job_key) DO NOTHING`, export.ID,
			fmt.Sprintf("mint:%d", export.ID), resolution.ResolvedAt); err != nil {
			return err
		}
		loaded, found, err := tonExportByPredicate(ctx, tx, "id=$1", export.ID)
		if err != nil || !found {
			return err
		}
		out = loaded
		return nil
	})
	if err != nil {
		return domain.StarGiftTONExport{}, err
	}
	return out, nil
}

func (s *StarGiftTONStore) RecordTONMintPrepared(ctx context.Context, preparation domain.StarGiftTONMintPreparation) (domain.StarGiftTONAsset, error) {
	if s == nil || s.db == nil || !preparation.Valid() {
		return domain.StarGiftTONAsset{}, domain.ErrStarGiftTONExportInvalid
	}
	var out domain.StarGiftTONAsset
	err := withTx(ctx, s.db, "record prepared TON mint", func(tx pgx.Tx) error {
		job, export, err := lockTONJobAndExport(ctx, tx, preparation.JobID)
		if err != nil {
			return err
		}
		if job.Kind != domain.StarGiftTONJobMint || job.Status != domain.StarGiftTONJobLeased ||
			job.LeaseOwner != strings.TrimSpace(preparation.WorkerID) || job.FencingToken != preparation.FencingToken ||
			job.LeaseExpiresAt <= preparation.PreparedAt {
			return domain.ErrStarGiftTONLeaseLost
		}
		if export.Status != domain.StarGiftTONExportMintQueued {
			return domain.ErrStarGiftTONExportStateConflict
		}
		if err := tx.QueryRow(ctx, `INSERT INTO star_gift_ton_assets(export_id,network,collection_address,item_address,
		 metadata_hash,submission_hash,submission_boc,prepared_at,valid_until)
		 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING export_id`, export.ID, string(export.Network),
			export.CollectionAddress, export.ExpectedItemAddress, export.MetadataHash, preparation.SubmissionHash,
			preparation.SubmissionBOC, preparation.PreparedAt, preparation.ValidUntil).Scan(&out.ExportID); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE star_gift_ton_exports SET status='prepared',
		 version=version+1,last_error='',updated_at=now() WHERE id=$1`, export.ID); err != nil {
			return err
		}
		out, _, err = tonAssetByExportID(ctx, tx, export.ID)
		return err
	})
	if err != nil {
		return domain.StarGiftTONAsset{}, err
	}
	return out, nil
}

func (s *StarGiftTONStore) RecordTONMintSubmitted(ctx context.Context, submission domain.StarGiftTONMintSubmission) (domain.StarGiftTONAsset, error) {
	if s == nil || s.db == nil || !submission.Valid() {
		return domain.StarGiftTONAsset{}, domain.ErrStarGiftTONExportInvalid
	}
	var out domain.StarGiftTONAsset
	err := withTx(ctx, s.db, "record TON mint submission", func(tx pgx.Tx) error {
		job, export, err := lockTONJobAndExport(ctx, tx, submission.JobID)
		if err != nil {
			return err
		}
		if job.Kind != domain.StarGiftTONJobMint || job.Status != domain.StarGiftTONJobLeased ||
			job.LeaseOwner != strings.TrimSpace(submission.WorkerID) || job.FencingToken != submission.FencingToken ||
			job.LeaseExpiresAt <= submission.SubmittedAt {
			return domain.ErrStarGiftTONLeaseLost
		}
		if export.Status != domain.StarGiftTONExportPrepared {
			return domain.ErrStarGiftTONExportStateConflict
		}
		asset, found, err := tonAssetByExportIDForUpdate(ctx, tx, export.ID)
		if err != nil || !found || submission.SubmittedAt < asset.PreparedAt {
			return domain.ErrStarGiftTONExportStateConflict
		}
		if _, err := tx.Exec(ctx, `UPDATE star_gift_ton_assets SET mint_tx_hash=$2,mint_tx_lt=$3,
		 mint_block_seqno=$4,submitted_at=$5,updated_at=now() WHERE export_id=$1`, export.ID,
			nullableTONHash(submission.TxHash), submission.TxLT, submission.BlockSeqno, submission.SubmittedAt); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE star_gift_ton_exports SET status='submitted',submitted_at=$2,
		 version=version+1,last_error='',updated_at=now() WHERE id=$1`, export.ID, submission.SubmittedAt); err != nil {
			return err
		}
		if err := completeTONJob(ctx, tx, job.ID, submission.SubmittedAt); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO star_gift_ton_jobs(export_id,job_key,kind,available_at,created_at)
		 VALUES($1,$2,'confirm',$3,$3) ON CONFLICT(job_key) DO NOTHING`, export.ID,
			fmt.Sprintf("confirm:%d", export.ID), submission.SubmittedAt); err != nil {
			return err
		}
		asset, found, err = tonAssetByExportID(ctx, tx, export.ID)
		if err != nil || !found {
			return err
		}
		out = asset
		return nil
	})
	if err != nil {
		return domain.StarGiftTONAsset{}, err
	}
	return out, nil
}

func (s *StarGiftTONStore) RecordTONMintObserved(ctx context.Context, observation domain.StarGiftTONMintObservation) (domain.StarGiftTONAsset, error) {
	if s == nil || s.db == nil || !observation.Valid() {
		return domain.StarGiftTONAsset{}, domain.ErrStarGiftTONExportInvalid
	}
	var out domain.StarGiftTONAsset
	err := withTx(ctx, s.db, "record TON mint finality anchor", func(tx pgx.Tx) error {
		job, export, err := lockTONJobAndExport(ctx, tx, observation.JobID)
		if err != nil {
			return err
		}
		if job.Kind != domain.StarGiftTONJobConfirm || job.Status != domain.StarGiftTONJobLeased ||
			job.LeaseOwner != strings.TrimSpace(observation.WorkerID) || job.FencingToken != observation.FencingToken ||
			job.LeaseExpiresAt <= observation.ObservedAt {
			return domain.ErrStarGiftTONLeaseLost
		}
		if export.Status != domain.StarGiftTONExportSubmitted && export.Status != domain.StarGiftTONExportQuarantined {
			return domain.ErrStarGiftTONExportStateConflict
		}
		asset, found, err := tonAssetByExportIDForUpdate(ctx, tx, export.ID)
		if err != nil || !found || asset.SubmittedAt == 0 || observation.ObservedAt < asset.SubmittedAt {
			return domain.ErrStarGiftTONExportStateConflict
		}
		if (len(asset.MintTxHash) != 0 && !bytes.Equal(asset.MintTxHash, observation.TxHash)) ||
			(asset.MintTxLT != "" && asset.MintTxLT != observation.TxLT) ||
			(asset.MintBlockSeqno != 0 && asset.MintBlockSeqno != observation.BlockSeqno) {
			return domain.ErrStarGiftTONExportStateConflict
		}
		if _, err := tx.Exec(ctx, `UPDATE star_gift_ton_assets SET mint_tx_hash=$2,mint_tx_lt=$3,
			mint_block_seqno=$4,updated_at=now() WHERE export_id=$1 AND mint_block_seqno=0`, export.ID,
			observation.TxHash, observation.TxLT, observation.BlockSeqno); err != nil {
			return err
		}
		out, found, err = tonAssetByExportID(ctx, tx, export.ID)
		if err != nil || !found {
			return err
		}
		return nil
	})
	if err != nil {
		return domain.StarGiftTONAsset{}, err
	}
	return out, nil
}

func (s *StarGiftTONStore) RecordTONMintConfirmed(ctx context.Context, confirmation domain.StarGiftTONMintConfirmation) (domain.StarGiftTONAsset, error) {
	if s == nil || s.db == nil || !confirmation.Valid() {
		return domain.StarGiftTONAsset{}, domain.ErrStarGiftTONExportInvalid
	}
	var out domain.StarGiftTONAsset
	err := withTx(ctx, s.db, "record TON mint confirmation", func(tx pgx.Tx) error {
		job, export, err := lockTONJobAndExport(ctx, tx, confirmation.JobID)
		if err != nil {
			return err
		}
		if job.Kind != domain.StarGiftTONJobConfirm || job.Status != domain.StarGiftTONJobLeased ||
			job.LeaseOwner != strings.TrimSpace(confirmation.WorkerID) || job.FencingToken != confirmation.FencingToken ||
			job.LeaseExpiresAt <= confirmation.ConfirmedAt {
			return domain.ErrStarGiftTONLeaseLost
		}
		if export.Status != domain.StarGiftTONExportSubmitted && export.Status != domain.StarGiftTONExportQuarantined {
			return domain.ErrStarGiftTONExportStateConflict
		}
		if strings.TrimSpace(confirmation.OwnerAddress) != export.WalletAddress {
			return domain.ErrStarGiftTONExportStateConflict
		}
		asset, found, err := tonAssetByExportIDForUpdate(ctx, tx, export.ID)
		if err != nil || !found || confirmation.ConfirmedAt < asset.SubmittedAt {
			return domain.ErrStarGiftTONExportStateConflict
		}
		if asset.MintBlockSeqno <= 0 || !bytes.Equal(asset.MintTxHash, confirmation.TxHash) ||
			asset.MintTxLT != confirmation.TxLT || asset.MintBlockSeqno != confirmation.BlockSeqno {
			return domain.ErrStarGiftTONExportStateConflict
		}
		if _, err := tx.Exec(ctx, `UPDATE star_gift_ton_assets SET owner_address=$2,mint_tx_hash=$3,mint_tx_lt=$4,
		 mint_block_seqno=$5,finality_depth=$6,confirmed_at=$7,owner_observed_lt=$4,owner_observed_at=$7,updated_at=now()
		 WHERE export_id=$1`, export.ID, strings.TrimSpace(confirmation.OwnerAddress), confirmation.TxHash,
			confirmation.TxLT, confirmation.BlockSeqno, confirmation.FinalityDepth, confirmation.ConfirmedAt); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE star_gift_ton_exports SET status='confirmed',confirmed_at=$2,
		 version=version+1,last_error='',updated_at=now() WHERE id=$1`, export.ID, confirmation.ConfirmedAt); err != nil {
			return err
		}
		if err := completeTONJob(ctx, tx, job.ID, confirmation.ConfirmedAt); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO star_gift_ton_jobs(export_id,job_key,kind,available_at,created_at)
		 VALUES($1,$2,'finalize',$3,$3) ON CONFLICT(job_key) DO NOTHING`, export.ID,
			fmt.Sprintf("finalize:%d", export.ID), confirmation.ConfirmedAt); err != nil {
			return err
		}
		out, found, err = tonAssetByExportID(ctx, tx, export.ID)
		if err != nil || !found {
			return err
		}
		return nil
	})
	if err != nil {
		return domain.StarGiftTONAsset{}, err
	}
	return out, nil
}

func (s *StarGiftTONStore) RecordTONOwnerObserved(ctx context.Context, observation domain.StarGiftTONOwnerObservation) (domain.StarGiftTONAsset, error) {
	if s == nil || s.db == nil || !observation.Valid() {
		return domain.StarGiftTONAsset{}, domain.ErrStarGiftTONExportInvalid
	}
	var out domain.StarGiftTONAsset
	err := withTx(ctx, s.db, "record TON star gift owner", func(tx pgx.Tx) error {
		job, export, err := lockTONJobAndExport(ctx, tx, observation.JobID)
		if err != nil {
			return err
		}
		if job.Kind != domain.StarGiftTONJobReconcileOwner || job.Status != domain.StarGiftTONJobLeased ||
			job.LeaseOwner != strings.TrimSpace(observation.WorkerID) || job.FencingToken != observation.FencingToken ||
			job.LeaseExpiresAt <= observation.ObservedAt {
			return domain.ErrStarGiftTONLeaseLost
		}
		if export.Status != domain.StarGiftTONExportConfirmed && export.Status != domain.StarGiftTONExportFinalized {
			return domain.ErrStarGiftTONExportStateConflict
		}
		asset, found, err := tonAssetByExportIDForUpdate(ctx, tx, export.ID)
		if err != nil || !found || asset.ConfirmedAt == 0 {
			return domain.ErrStarGiftTONExportStateConflict
		}
		if asset.OwnerObservedLT != "" && compareTONLogicalTime(observation.ObservedLT, asset.OwnerObservedLT) < 0 {
			return domain.ErrStarGiftTONExportStateConflict
		}
		ownerChanged := asset.OwnerAddress != strings.TrimSpace(observation.OwnerAddress)
		if _, err := tx.Exec(ctx, `UPDATE star_gift_ton_assets SET owner_address=$2,owner_observed_lt=$3,
		 owner_observed_at=$4,updated_at=now() WHERE export_id=$1`, export.ID,
			strings.TrimSpace(observation.OwnerAddress), observation.ObservedLT, observation.ObservedAt); err != nil {
			return err
		}
		nextCheckAt := observation.NextCheckAt
		var claimNeedsFreshObservation bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM star_gift_ton_claims
WHERE export_id=$1 AND purpose='claim' AND status='pending' AND expires_at>$2 AND created_at>=$2)`,
			export.ID, observation.ObservedAt).Scan(&claimNeedsFreshObservation); err != nil {
			return err
		}
		if claimNeedsFreshObservation {
			nextCheckAt = observation.ObservedAt + 1
		}
		if _, err := tx.Exec(ctx, `UPDATE star_gift_ton_jobs SET status='pending',available_at=$2,attempts=0,
		 lease_owner='',lease_expires_at=0,last_error='',updated_at=now() WHERE id=$1`, job.ID, nextCheckAt); err != nil {
			return err
		}
		if ownerChanged && export.Status == domain.StarGiftTONExportFinalized {
			if _, err := tx.Exec(ctx, `INSERT INTO star_gift_ton_jobs(export_id,job_key,kind,available_at,created_at)
			 VALUES($1,$2,'sync_owner',$3,$3) ON CONFLICT(job_key) DO NOTHING`, export.ID,
				fmt.Sprintf("sync-owner:%d:%s", export.ID, observation.ObservedLT), observation.ObservedAt); err != nil {
				return err
			}
		}
		out, found, err = tonAssetByExportID(ctx, tx, export.ID)
		if err != nil || !found {
			return err
		}
		return nil
	})
	if err != nil {
		return domain.StarGiftTONAsset{}, err
	}
	return out, nil
}

func (s *StarGiftTONStore) FailTONJob(ctx context.Context, failure domain.StarGiftTONJobFailure) error {
	if s == nil || s.db == nil || failure.JobID <= 0 || strings.TrimSpace(failure.WorkerID) == "" ||
		failure.FencingToken <= 0 || failure.FailedAt <= 0 || failure.RetryAt <= failure.FailedAt {
		return domain.ErrStarGiftTONLeaseLost
	}
	return withTx(ctx, s.db, "fail TON job", func(tx pgx.Tx) error {
		job, export, err := lockTONJobAndExport(ctx, tx, failure.JobID)
		if err != nil {
			return err
		}
		if job.Status != domain.StarGiftTONJobLeased || job.LeaseOwner != strings.TrimSpace(failure.WorkerID) ||
			job.FencingToken != failure.FencingToken {
			return domain.ErrStarGiftTONLeaseLost
		}
		_, hasAsset, err := tonAssetByExportIDForUpdate(ctx, tx, export.ID)
		if err != nil {
			return err
		}
		terminal := failure.Terminal || job.Attempts >= job.MaxAttempts
		jobStatus := "pending"
		attempts := job.Attempts
		exportStatus := export.Status
		unlockGift := false
		ensureConfirm := false
		if terminal {
			switch job.Kind {
			case domain.StarGiftTONJobResolveItem, domain.StarGiftTONJobMint:
				jobStatus = "failed"
				exportStatus = domain.StarGiftTONExportQuarantined
				if hasAsset {
					ensureConfirm = true
				}
			case domain.StarGiftTONJobConfirm:
				exportStatus = domain.StarGiftTONExportQuarantined
				if failure.Terminal {
					jobStatus = "failed"
				} else {
					attempts = 0
				}
			case domain.StarGiftTONJobReconcileOwner:
				if failure.Terminal {
					jobStatus = "failed"
				} else {
					attempts = 0
				}
			default:
				return domain.ErrStarGiftTONLeaseLost
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE star_gift_ton_jobs SET status=$2,available_at=$3,lease_owner='',
		 lease_expires_at=0,attempts=$4,last_error=$5,updated_at=now() WHERE id=$1`, job.ID, jobStatus,
			failure.RetryAt, attempts, boundedTONError(failure.Error)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE star_gift_ton_exports SET status=$2,submitted_at=$3,last_error=$4,
		 version=version+1,updated_at=now() WHERE id=$1`, export.ID, string(exportStatus), export.SubmittedAt,
			boundedTONError(failure.Error)); err != nil {
			return err
		}
		if unlockGift {
			if _, err := tx.Exec(ctx, `UPDATE unique_star_gifts SET externalization_pending=false,updated_at=now()
			 WHERE id=$1 AND externalization_pending`, export.UniqueGiftID); err != nil {
				return err
			}
			if _, err := tx.Exec(ctx, `UPDATE star_gift_ton_claims SET status='expired',updated_at=now()
			 WHERE export_id=$1 AND purpose='export' AND status='pending'`, export.ID); err != nil {
				return err
			}
		}
		if ensureConfirm {
			if _, err := tx.Exec(ctx, `INSERT INTO star_gift_ton_jobs(export_id,job_key,kind,status,available_at,created_at)
			 VALUES($1,$2,'confirm','pending',$3,$4)
			 ON CONFLICT(job_key) DO UPDATE SET status='pending',available_at=EXCLUDED.available_at,lease_owner='',
			 lease_expires_at=0,attempts=0,last_error='',completed_at=0,updated_at=now()
			 WHERE star_gift_ton_jobs.status='failed'`, export.ID, fmt.Sprintf("confirm:%d", export.ID),
				failure.RetryAt, failure.FailedAt); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *StarGiftTONStore) ResumeTONExport(ctx context.Context, req domain.StarGiftTONRecoveryRequest) (domain.StarGiftTONRecoveryResult, error) {
	if s == nil || s.db == nil || !req.Valid() {
		return domain.StarGiftTONRecoveryResult{}, domain.ErrStarGiftTONExportInvalid
	}
	req.Actor = strings.TrimSpace(req.Actor)
	req.Reason = strings.TrimSpace(req.Reason)
	var out domain.StarGiftTONRecoveryResult
	err := withTx(ctx, s.db, "resume quarantined TON export", func(tx pgx.Tx) error {
		export, found, err := tonExportByPredicateForUpdate(ctx, tx, "id=$1", req.ExportID)
		if err != nil || !found {
			return domain.ErrStarGiftTONExportInvalid
		}
		if export.Status != domain.StarGiftTONExportQuarantined || export.ItemIndex == "" || export.ProofVerifiedAt <= 0 ||
			export.Network != req.Network || export.CollectionAddress != strings.TrimSpace(req.CollectionAddress) || export.MintABI != req.MintABI {
			return domain.ErrStarGiftTONExportStateConflict
		}
		if err := requireTONAdmission(ctx, tx, req.Network, req.CollectionAddress, req.CollectionCodeHash,
			req.MintABI, req.InitialItemIndex, req.RecoveredAt, true); err != nil {
			return err
		}

		asset, hasAsset, err := tonAssetByExportIDForUpdate(ctx, tx, export.ID)
		if err != nil {
			return err
		}
		var kind domain.StarGiftTONJobKind
		nextStatus := export.Status
		switch {
		case export.ExpectedItemAddress == "" && !hasAsset:
			kind = domain.StarGiftTONJobResolveItem
			nextStatus = domain.StarGiftTONExportProofVerified
		case export.ExpectedItemAddress != "" && !hasAsset:
			kind = domain.StarGiftTONJobMint
			nextStatus = domain.StarGiftTONExportMintQueued
		case export.ExpectedItemAddress != "" && hasAsset && asset.PreparedAt > 0 && len(asset.SubmissionBOC) > 0:
			kind = domain.StarGiftTONJobConfirm
		default:
			return domain.ErrStarGiftTONExportStateConflict
		}

		job, found, err := scanTONJobRow(tx.QueryRow(ctx, `SELECT id,export_id,job_key,kind,status,available_at,
		 lease_owner,lease_expires_at,fencing_token,attempts,max_attempts,last_error,created_at,completed_at
		 FROM star_gift_ton_jobs WHERE export_id=$1 AND kind=$2 FOR UPDATE`, export.ID, string(kind)))
		if err != nil || !found || job.Status != domain.StarGiftTONJobFailed {
			return domain.ErrStarGiftTONExportStateConflict
		}
		previousError := job.LastError
		if previousError == "" {
			previousError = export.LastError
		}
		if _, err := tx.Exec(ctx, `UPDATE star_gift_ton_exports SET status=$2,last_error='',version=version+1,updated_at=now()
		 WHERE id=$1`, export.ID, string(nextStatus)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `UPDATE star_gift_ton_jobs SET status='pending',available_at=$2,lease_owner='',
		 lease_expires_at=0,attempts=0,last_error='',completed_at=0,updated_at=now() WHERE id=$1`, job.ID, req.RecoveredAt); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `INSERT INTO star_gift_ton_recoveries(
		 export_id,job_id,actor,reason,previous_export_status,previous_job_status,previous_job_kind,previous_error,recovered_at)
		 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id`, export.ID, job.ID, req.Actor, req.Reason,
			string(export.Status), string(job.Status), string(job.Kind), boundedTONError(previousError), req.RecoveredAt).Scan(&out.RecoveryID); err != nil {
			return err
		}
		loadedExport, found, err := tonExportByPredicate(ctx, tx, "id=$1", export.ID)
		if err != nil || !found {
			return err
		}
		loadedJob, found, err := scanTONJobRow(tx.QueryRow(ctx, `SELECT id,export_id,job_key,kind,status,available_at,
		 lease_owner,lease_expires_at,fencing_token,attempts,max_attempts,last_error,created_at,completed_at
		 FROM star_gift_ton_jobs WHERE id=$1`, job.ID))
		if err != nil || !found {
			return err
		}
		loadedJob.Export = loadedExport
		if hasAsset {
			loadedJob.Asset = &asset
		}
		out.Export = loadedExport
		out.Job = loadedJob
		return nil
	})
	if err != nil {
		return domain.StarGiftTONRecoveryResult{}, err
	}
	return out, nil
}

func tonExportByPredicate(ctx context.Context, db sqlcgen.DBTX, predicate string, value any) (domain.StarGiftTONExport, bool, error) {
	return scanTONExportRow(db.QueryRow(ctx, tonExportQuery(predicate), value))
}

func tonExportByPredicateForUpdate(ctx context.Context, tx pgx.Tx, predicate string, value any) (domain.StarGiftTONExport, bool, error) {
	return scanTONExportRow(tx.QueryRow(ctx, tonExportQuery(predicate)+" FOR UPDATE", value))
}

func tonExportQuery(predicate string) string {
	return `SELECT id,unique_gift_id,owner_user_id,network,collection_address,mint_abi,item_index,expected_item_address,
	 metadata_uri,metadata_hash,metadata_json,wallet_address,COALESCE(wallet_state_init_hash,decode('','hex')),
	 COALESCE(wallet_public_key,decode('','hex')),COALESCE(proof_hash,decode('','hex')),status,created_at,expires_at,
	 proof_verified_at,submitted_at,confirmed_at,finalized_at,version,last_error
	 FROM star_gift_ton_exports WHERE ` + predicate
}

func scanTONExportRow(row pgx.Row) (domain.StarGiftTONExport, bool, error) {
	var out domain.StarGiftTONExport
	var network, status string
	err := row.Scan(&out.ID, &out.UniqueGiftID, &out.OwnerUserID, &network, &out.CollectionAddress,
		&out.MintABI, &out.ItemIndex, &out.ExpectedItemAddress, &out.MetadataURI, &out.MetadataHash, &out.MetadataJSON, &out.WalletAddress,
		&out.WalletStateInitHash, &out.WalletPublicKey, &out.ProofHash, &status, &out.CreatedAt, &out.ExpiresAt,
		&out.ProofVerifiedAt, &out.SubmittedAt, &out.ConfirmedAt, &out.FinalizedAt, &out.Version, &out.LastError)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.StarGiftTONExport{}, false, nil
	}
	if err != nil {
		return domain.StarGiftTONExport{}, false, fmt.Errorf("scan TON export: %w", err)
	}
	out.Network = domain.TONNetwork(network)
	out.Status = domain.StarGiftTONExportStatus(status)
	return out, true, nil
}

func tonProofChallengeByPredicate(ctx context.Context, db sqlcgen.DBTX, predicate string, value any, forUpdate bool) (domain.StarGiftTONChallenge, bool, error) {
	query := `SELECT id,challenge_digest,purpose,COALESCE(export_id,0),user_id,auth_context_hash,network,
	 proof_domain,proof_payload_hash,wallet_address,COALESCE(wallet_state_init_hash,decode('','hex')),
	 COALESCE(wallet_public_key,decode('','hex')),COALESCE(proof_hash,decode('','hex')),status,
	 created_at,expires_at,verified_at,consumed_at FROM star_gift_ton_claims WHERE ` + predicate
	if forUpdate {
		query += " FOR UPDATE"
	}
	var out domain.StarGiftTONChallenge
	var purpose, network string
	err := db.QueryRow(ctx, query, value).Scan(&out.ID, &out.ChallengeDigest, &purpose, &out.ExportID, &out.UserID,
		&out.AuthContextHash, &network, &out.ProofDomain, &out.PayloadHash, &out.WalletAddress,
		&out.StateInitHash, &out.WalletPublicKey, &out.ProofHash, &out.Status,
		&out.CreatedAt, &out.ExpiresAt, &out.VerifiedAt, &out.ConsumedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.StarGiftTONChallenge{}, false, nil
	}
	if err != nil {
		return domain.StarGiftTONChallenge{}, false, err
	}
	out.Purpose = domain.StarGiftTONChallengePurpose(purpose)
	out.Network = domain.TONNetwork(network)
	return out, true, nil
}

func (s *StarGiftTONStore) tonJobByID(ctx context.Context, jobID int64) (domain.StarGiftTONJob, bool, error) {
	job, found, err := scanTONJobRow(s.db.QueryRow(ctx, `SELECT id,export_id,job_key,kind,status,available_at,
	 lease_owner,lease_expires_at,fencing_token,attempts,max_attempts,last_error,created_at,completed_at
	 FROM star_gift_ton_jobs WHERE id=$1`, jobID))
	if err != nil || !found {
		return job, found, err
	}
	export, found, err := s.TONExportByID(ctx, job.ExportID)
	if err != nil || !found {
		return domain.StarGiftTONJob{}, false, err
	}
	job.Export = export
	if asset, found, err := tonAssetByExportID(ctx, s.db, job.ExportID); err != nil {
		return domain.StarGiftTONJob{}, false, err
	} else if found {
		job.Asset = &asset
	}
	return job, true, nil
}

func scanTONJobRow(row pgx.Row) (domain.StarGiftTONJob, bool, error) {
	var out domain.StarGiftTONJob
	var kind, status string
	err := row.Scan(&out.ID, &out.ExportID, &out.Key, &kind, &status, &out.AvailableAt, &out.LeaseOwner,
		&out.LeaseExpiresAt, &out.FencingToken, &out.Attempts, &out.MaxAttempts, &out.LastError,
		&out.CreatedAt, &out.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.StarGiftTONJob{}, false, nil
	}
	if err != nil {
		return domain.StarGiftTONJob{}, false, err
	}
	out.Kind = domain.StarGiftTONJobKind(kind)
	out.Status = domain.StarGiftTONJobStatus(status)
	return out, true, nil
}

func lockTONJobAndExport(ctx context.Context, tx pgx.Tx, jobID int64) (domain.StarGiftTONJob, domain.StarGiftTONExport, error) {
	job, found, err := scanTONJobRow(tx.QueryRow(ctx, `SELECT id,export_id,job_key,kind,status,available_at,
	 lease_owner,lease_expires_at,fencing_token,attempts,max_attempts,last_error,created_at,completed_at
	 FROM star_gift_ton_jobs WHERE id=$1 FOR UPDATE`, jobID))
	if err != nil || !found {
		return domain.StarGiftTONJob{}, domain.StarGiftTONExport{}, domain.ErrStarGiftTONLeaseLost
	}
	export, found, err := tonExportByPredicateForUpdate(ctx, tx, "id=$1", job.ExportID)
	if err != nil || !found {
		return domain.StarGiftTONJob{}, domain.StarGiftTONExport{}, domain.ErrStarGiftTONExportInvalid
	}
	return job, export, nil
}

func completeTONJob(ctx context.Context, tx pgx.Tx, jobID int64, completedAt int) error {
	_, err := tx.Exec(ctx, `UPDATE star_gift_ton_jobs SET status='completed',lease_owner='',lease_expires_at=0,
	 completed_at=$2,last_error='',updated_at=now() WHERE id=$1`, jobID, completedAt)
	return err
}

func tonAssetByExportID(ctx context.Context, db sqlcgen.DBTX, exportID int64) (domain.StarGiftTONAsset, bool, error) {
	return scanTONAssetRow(db.QueryRow(ctx, tonAssetQuery(false), exportID))
}

func tonAssetByExportIDForUpdate(ctx context.Context, tx pgx.Tx, exportID int64) (domain.StarGiftTONAsset, bool, error) {
	return scanTONAssetRow(tx.QueryRow(ctx, tonAssetQuery(true), exportID))
}

func tonAssetQuery(forUpdate bool) string {
	query := `SELECT export_id,network,collection_address,item_address,owner_address,metadata_hash,submission_hash,submission_boc,
	 COALESCE(mint_tx_hash,decode('','hex')),
	 mint_tx_lt,mint_block_seqno,finality_depth,prepared_at,valid_until,submitted_at,confirmed_at,owner_observed_lt,owner_observed_at
	 FROM star_gift_ton_assets WHERE export_id=$1`
	if forUpdate {
		query += " FOR UPDATE"
	}
	return query
}

func scanTONAssetRow(row pgx.Row) (domain.StarGiftTONAsset, bool, error) {
	var out domain.StarGiftTONAsset
	var network string
	err := row.Scan(&out.ExportID, &network, &out.CollectionAddress, &out.ItemAddress, &out.OwnerAddress,
		&out.MetadataHash, &out.SubmissionHash, &out.SubmissionBOC, &out.MintTxHash, &out.MintTxLT, &out.MintBlockSeqno, &out.FinalityDepth,
		&out.PreparedAt, &out.ValidUntil, &out.SubmittedAt, &out.ConfirmedAt, &out.OwnerObservedLT, &out.OwnerObservedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.StarGiftTONAsset{}, false, nil
	}
	if err != nil {
		return domain.StarGiftTONAsset{}, false, err
	}
	out.Network = domain.TONNetwork(network)
	return out, true, nil
}

func boundedTONError(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 1000 {
		return value[:1000]
	}
	return value
}

func nullableTONHash(value []byte) any {
	if len(value) == 0 {
		return nil
	}
	return value
}

func compareTONLogicalTime(a, b string) int {
	a = strings.TrimLeft(a, "0")
	b = strings.TrimLeft(b, "0")
	if a == "" {
		a = "0"
	}
	if b == "" {
		b = "0"
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return strings.Compare(a, b)
}

func incrementTONDecimal(value string) string {
	n, ok := new(big.Int).SetString(value, 10)
	if !ok || n.Sign() < 0 {
		return ""
	}
	return n.Add(n, big.NewInt(1)).String()
}
