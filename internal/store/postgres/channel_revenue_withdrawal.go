package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
)

// IssueChannelRevenueWithdrawal persists a short-lived claim command without
// moving funds. Amount zero binds the command to the full balance observed in
// this transaction. The creator check is repeated against the authoritative
// channel row so an RPC-layer permission check cannot race ownership transfer.
func (s *StarGiftLifecycleStore) IssueChannelRevenueWithdrawal(ctx context.Context, req domain.ChannelRevenueWithdrawalRequest) (domain.ChannelRevenueWithdrawal, error) {
	if s == nil || s.db == nil || req.ChannelID <= 0 || req.CreatorUserID <= 0 || !req.Currency.Valid() ||
		req.PasswordChangedAt.IsZero() || req.AuthKeyID == ([8]byte{}) || req.AuthorizationCreatedAt.IsZero() ||
		len(req.TokenDigest) != 32 || req.Amount < 0 || req.Date <= 0 || req.ExpiresAt <= req.Date {
		return domain.ChannelRevenueWithdrawal{}, domain.ErrChannelRevenueWithdrawalInvalid
	}
	var out domain.ChannelRevenueWithdrawal
	err := withTx(ctx, s.db, "issue channel revenue withdrawal", func(tx pgx.Tx) error {
		// One live capability per channel/currency bounds authenticated token
		// churn. The advisory lock closes the empty-slot race; locking an existing
		// command before user/channel rows also matches Complete's command-first
		// order, so replacement cannot deadlock with a confirmation POST.
		advisoryKey := fmt.Sprintf("channel-revenue-withdrawal:%d:%s", req.ChannelID, req.Currency)
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, advisoryKey); err != nil {
			return err
		}
		var replacedPendingID int64
		err := tx.QueryRow(ctx, `SELECT id FROM channel_revenue_withdrawals
WHERE channel_id=$1 AND currency=$2 AND status='pending' FOR UPDATE`, req.ChannelID, string(req.Currency)).
			Scan(&replacedPendingID)
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if err := lockActiveRevenueCreatorUserTx(ctx, tx, req.CreatorUserID); err != nil {
			return err
		}
		if err := requireRevenueAuthorizationAdmissionTx(ctx, tx, req.CreatorUserID, req.AuthKeyID, req.AuthorizationCreatedAt); err != nil {
			return err
		}
		if err := requireChannelCreatorMembershipTx(ctx, tx, req.ChannelID, req.CreatorUserID); err != nil {
			return err
		}
		if err := requireRevenuePasswordAdmissionTx(ctx, tx, req.CreatorUserID, req.PasswordChangedAt); err != nil {
			return err
		}
		balance, err := lockChannelRevenueBalanceTx(ctx, tx, req.ChannelID, req.Currency)
		if err != nil {
			return err
		}
		amount := req.Amount
		if amount == 0 {
			amount = balance
		}
		if amount <= 0 || amount > balance {
			return domain.ErrChannelRevenueInsufficient
		}
		if replacedPendingID != 0 {
			if _, err := tx.Exec(ctx, `DELETE FROM channel_revenue_withdrawals WHERE id=$1 AND status='pending'`, replacedPendingID); err != nil {
				return err
			}
		}
		return tx.QueryRow(ctx, `INSERT INTO channel_revenue_withdrawals
(token_digest,channel_id,creator_user_id,currency,amount,status,expires_at,created_at)
VALUES($1,$2,$3,$4,$5,'pending',$6,$7)
RETURNING id,channel_id,creator_user_id,currency,amount,status,expires_at,created_at`,
			req.TokenDigest, req.ChannelID, req.CreatorUserID, string(req.Currency), amount, req.ExpiresAt, req.Date).
			Scan(&out.ID, &out.ChannelID, &out.CreatorUserID, &out.Currency, &out.Amount, &out.Status, &out.ExpiresAt, &out.CreatedAt)
	})
	if err != nil {
		if errors.Is(err, domain.ErrChannelRevenueWithdrawalExpired) || errors.Is(err, domain.ErrChannelRevenueInsufficient) {
			return out, err
		}
		return domain.ChannelRevenueWithdrawal{}, err
	}
	return out, nil
}

func (s *StarGiftLifecycleStore) ResolveChannelRevenueWithdrawal(ctx context.Context, tokenDigest []byte) (domain.ChannelRevenueWithdrawal, bool, error) {
	if s == nil || s.db == nil || len(tokenDigest) != 32 {
		return domain.ChannelRevenueWithdrawal{}, false, nil
	}
	out, err := scanChannelRevenueWithdrawal(s.db.QueryRow(ctx, `SELECT
id,channel_id,creator_user_id,currency,amount,status,expires_at,created_at,
COALESCE(completed_at,0),COALESCE(channel_transaction_id,0),COALESCE(user_transaction_id,0),
COALESCE(channel_balance_after,0),COALESCE(user_balance_after,0)
FROM channel_revenue_withdrawals WHERE token_digest=$1`, tokenDigest))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ChannelRevenueWithdrawal{}, false, nil
	}
	if err != nil {
		return domain.ChannelRevenueWithdrawal{}, false, fmt.Errorf("resolve channel revenue withdrawal: %w", err)
	}
	return out, true, nil
}

// CompleteChannelRevenueWithdrawal is the only money-moving boundary. It
// serializes on the command and channel ledger, rechecks creator ownership,
// debits the channel, credits the creator, writes both signed ledger entries,
// and seals the durable receipt in one transaction. A completed token is an
// exact replay and never creates another movement.
func (s *StarGiftLifecycleStore) CompleteChannelRevenueWithdrawal(ctx context.Context, tokenDigest []byte, date int) (domain.ChannelRevenueWithdrawal, error) {
	if s == nil || s.db == nil || len(tokenDigest) != 32 || date <= 0 {
		return domain.ChannelRevenueWithdrawal{}, domain.ErrChannelRevenueWithdrawalInvalid
	}
	var out domain.ChannelRevenueWithdrawal
	err := withTx(ctx, s.db, "complete channel revenue withdrawal", func(tx pgx.Tx) error {
		var err error
		out, err = scanChannelRevenueWithdrawal(tx.QueryRow(ctx, `SELECT
id,channel_id,creator_user_id,currency,amount,status,expires_at,created_at,
COALESCE(completed_at,0),COALESCE(channel_transaction_id,0),COALESCE(user_transaction_id,0),
COALESCE(channel_balance_after,0),COALESCE(user_balance_after,0)
FROM channel_revenue_withdrawals WHERE token_digest=$1 FOR UPDATE`, tokenDigest))
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrChannelRevenueWithdrawalInvalid
		}
		if err != nil {
			return err
		}
		if out.Status == "completed" {
			return nil
		}
		if out.Status != "pending" {
			return domain.ErrChannelRevenueWithdrawalInvalid
		}
		if date >= out.ExpiresAt {
			return domain.ErrChannelRevenueWithdrawalExpired
		}
		if err := lockActiveRevenueCreatorUserTx(ctx, tx, out.CreatorUserID); err != nil {
			return err
		}
		if err := requireChannelCreatorMembershipTx(ctx, tx, out.ChannelID, out.CreatorUserID); err != nil {
			return err
		}
		// The lifecycle/ownership helper locks creator user -> channel -> member.
		// Paid reaction/message settlement does not lock the user row, so the
		// shared economy subsequence stays channel -> personal -> channel ledger.
		// Account deletion starts with the creator user row and therefore cannot
		// cross this path while it still appears active.
		personalBalance, err := lockPersonalRevenueBalanceTx(ctx, tx, out.CreatorUserID, out.Currency)
		if err != nil {
			return err
		}
		if personalBalance > math.MaxInt64-out.Amount {
			return domain.ErrChannelRevenueWithdrawalInvalid
		}
		balance, err := lockChannelRevenueBalanceTx(ctx, tx, out.ChannelID, out.Currency)
		if err != nil {
			return err
		}
		if balance < out.Amount {
			return domain.ErrChannelRevenueInsufficient
		}
		out.ChannelBalanceAfter = balance - out.Amount
		if out.Currency == domain.ChannelRevenueTON {
			if err := tx.QueryRow(ctx, `UPDATE channel_ton_balances SET balance_nanoton=balance_nanoton-$2,updated_at=now()
WHERE channel_id=$1 RETURNING balance_nanoton`, out.ChannelID, out.Amount).Scan(&out.ChannelBalanceAfter); err != nil {
				return err
			}
			if err := tx.QueryRow(ctx, `INSERT INTO channel_ton_transactions
(channel_id,actor_user_id,amount_nanoton,reason,peer_type,peer_id,date)
VALUES($1,$2,$3,$4,'user',$2,$5) RETURNING id`, out.ChannelID, out.CreatorUserID, -out.Amount,
				string(domain.StarsReasonWithdrawal), date).Scan(&out.ChannelTransactionID); err != nil {
				return err
			}
			if err := creditRevenueTONToUserTx(ctx, tx, &out, date); err != nil {
				return err
			}
		} else {
			if err := tx.QueryRow(ctx, `UPDATE channel_stars_balances SET balance=balance-$2,updated_at=now()
WHERE channel_id=$1 RETURNING balance`, out.ChannelID, out.Amount).Scan(&out.ChannelBalanceAfter); err != nil {
				return err
			}
			if err := tx.QueryRow(ctx, `INSERT INTO channel_stars_transactions
(channel_id,actor_user_id,amount,reason,peer_type,peer_id,date)
VALUES($1,$2,$3,$4,'user',$2,$5) RETURNING id`, out.ChannelID, out.CreatorUserID, -out.Amount,
				string(domain.StarsReasonWithdrawal), date).Scan(&out.ChannelTransactionID); err != nil {
				return err
			}
			if err := creditRevenueStarsToUserTx(ctx, tx, &out, date); err != nil {
				return err
			}
		}
		out.Status = "completed"
		out.CompletedAt = date
		_, err = tx.Exec(ctx, `UPDATE channel_revenue_withdrawals SET
status='completed',completed_at=$2,channel_transaction_id=$3,user_transaction_id=$4,
channel_balance_after=$5,user_balance_after=$6 WHERE id=$1`, out.ID, date, out.ChannelTransactionID,
			out.UserTransactionID, out.ChannelBalanceAfter, out.UserBalanceAfter)
		return err
	})
	if err != nil {
		if errors.Is(err, domain.ErrChannelRevenueWithdrawalExpired) || errors.Is(err, domain.ErrChannelRevenueInsufficient) {
			return out, err
		}
		return domain.ChannelRevenueWithdrawal{}, err
	}
	return out, nil
}

type channelRevenueRowScanner interface {
	Scan(dest ...any) error
}

func scanChannelRevenueWithdrawal(row channelRevenueRowScanner) (domain.ChannelRevenueWithdrawal, error) {
	var out domain.ChannelRevenueWithdrawal
	err := row.Scan(&out.ID, &out.ChannelID, &out.CreatorUserID, &out.Currency, &out.Amount, &out.Status,
		&out.ExpiresAt, &out.CreatedAt, &out.CompletedAt, &out.ChannelTransactionID, &out.UserTransactionID,
		&out.ChannelBalanceAfter, &out.UserBalanceAfter)
	return out, err
}

// lockActiveRevenueCreatorUserTx serializes claims with account tombstoning.
// The user row must be first: logical account deletion takes it FOR UPDATE
// before changing any account state. Holding it FOR SHARE prevents a token from
// being completed into an unreachable deleted account while preserving the
// paid-spend channel -> personal balance order (paid spend never locks users).
func lockActiveRevenueCreatorUserTx(ctx context.Context, tx pgx.Tx, creatorUserID int64) error {
	var activeUser bool
	if err := tx.QueryRow(ctx, `SELECT deleted_at IS NULL FROM users WHERE id=$1 FOR SHARE`, creatorUserID).Scan(&activeUser); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrChannelRevenueWithdrawalInvalid
		}
		return err
	}
	if !activeUser {
		return domain.ErrChannelRevenueWithdrawalInvalid
	}
	return nil
}

// requireRevenuePasswordAdmissionTx closes the gap between the RPC's SRP proof
// check and durable command issuance. The exact password_changed_at observed
// before CheckPassword is a version token; locking and comparing it here means
// a concurrent password change either commits before issuance (and rejects the
// stale proof) or waits until the command has already linearized.
func requireRevenuePasswordAdmissionTx(ctx context.Context, tx pgx.Tx, creatorUserID int64, expectedChangedAt time.Time) error {
	var hasPassword bool
	var changedAt *time.Time
	var passwordMature bool
	err := tx.QueryRow(ctx, `SELECT has_password,password_changed_at,
COALESCE(password_changed_at <= now()-interval '24 hours',false)
FROM account_passwords WHERE user_id=$1 FOR SHARE`, creatorUserID).Scan(&hasPassword, &changedAt, &passwordMature)
	if errors.Is(err, pgx.ErrNoRows) {
		return &domain.ChannelRevenuePasswordStateChangedError{}
	}
	if err != nil {
		return err
	}
	actualChangedAt := time.Time{}
	if changedAt != nil {
		actualChangedAt = *changedAt
	}
	if !hasPassword || actualChangedAt.IsZero() || !actualChangedAt.Equal(expectedChangedAt) || !passwordMature {
		return &domain.ChannelRevenuePasswordStateChangedError{
			HasPassword: hasPassword, PasswordChangedAt: actualChangedAt,
		}
	}
	return nil
}

// requireRevenueAuthorizationAdmissionTx binds the checked 24-hour session age
// to the durable command transaction. Bind changes authorization ownership only
// after locking the target user and updates this row; the user -> authorization
// lock order below therefore gives either an issue-before-rebind command or a
// fail-closed observation of the new session, never a stale age check.
func requireRevenueAuthorizationAdmissionTx(ctx context.Context, tx pgx.Tx, creatorUserID int64, authKeyID [8]byte, expectedCreatedAt time.Time) error {
	var (
		actualUserID    int64
		createdAt       time.Time
		passwordPending bool
		sessionMature   bool
	)
	err := tx.QueryRow(ctx, `SELECT user_id,created_at,password_pending,
created_at <= now()-interval '24 hours'
FROM authorizations WHERE auth_key_id=$1 FOR SHARE`, authKeyIDToInt64(authKeyID)).
		Scan(&actualUserID, &createdAt, &passwordPending, &sessionMature)
	if errors.Is(err, pgx.ErrNoRows) {
		return &domain.ChannelRevenueAuthorizationStateChangedError{}
	}
	if err != nil {
		return err
	}
	ownerMatches := actualUserID == creatorUserID
	if !ownerMatches || passwordPending || !createdAt.Equal(expectedCreatedAt) || !sessionMature {
		return &domain.ChannelRevenueAuthorizationStateChangedError{
			HasAuthorization: true,
			OwnerMatches:     ownerMatches,
			PasswordPending:  passwordPending,
			CreatedAt:        createdAt,
		}
	}
	return nil
}

func requireChannelCreatorMembershipTx(ctx context.Context, tx pgx.Tx, channelID, creatorUserID int64) error {
	var currentCreator int64
	var channelDeleted bool
	if err := tx.QueryRow(ctx, `SELECT creator_user_id,deleted FROM channels WHERE id=$1 FOR SHARE`, channelID).
		Scan(&currentCreator, &channelDeleted); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrChannelRevenueWithdrawalInvalid
		}
		return err
	}
	if channelDeleted || currentCreator != creatorUserID {
		return domain.ErrChannelRevenueWithdrawalInvalid
	}

	var role, status string
	if err := tx.QueryRow(ctx, `SELECT role,status FROM channel_members
WHERE channel_id=$1 AND user_id=$2 FOR SHARE`, channelID, creatorUserID).Scan(&role, &status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrChannelRevenueWithdrawalInvalid
		}
		return err
	}
	if role != string(domain.ChannelRoleCreator) || status != string(domain.ChannelMemberActive) {
		return domain.ErrChannelRevenueWithdrawalInvalid
	}
	return nil
}

func lockChannelRevenueBalanceTx(ctx context.Context, tx pgx.Tx, channelID int64, currency domain.ChannelRevenueCurrency) (int64, error) {
	var balance int64
	var err error
	if currency == domain.ChannelRevenueTON {
		err = tx.QueryRow(ctx, `SELECT balance_nanoton FROM channel_ton_balances WHERE channel_id=$1 FOR UPDATE`, channelID).Scan(&balance)
	} else {
		err = tx.QueryRow(ctx, `SELECT balance FROM channel_stars_balances WHERE channel_id=$1 FOR UPDATE`, channelID).Scan(&balance)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return balance, err
}

func lockPersonalRevenueBalanceTx(ctx context.Context, tx pgx.Tx, userID int64, currency domain.ChannelRevenueCurrency) (int64, error) {
	var balance int64
	if currency == domain.ChannelRevenueTON {
		if _, err := tx.Exec(ctx, `INSERT INTO ton_balances(user_id,balance_nanoton,granted,updated_at)
VALUES($1,0,false,now()) ON CONFLICT(user_id) DO NOTHING`, userID); err != nil {
			return 0, err
		}
		err := tx.QueryRow(ctx, `SELECT balance_nanoton FROM ton_balances WHERE user_id=$1 FOR UPDATE`, userID).Scan(&balance)
		return balance, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO stars_balances(user_id,balance,granted,updated_at)
VALUES($1,0,false,now()) ON CONFLICT(user_id) DO NOTHING`, userID); err != nil {
		return 0, err
	}
	err := tx.QueryRow(ctx, `SELECT balance FROM stars_balances WHERE user_id=$1 FOR UPDATE`, userID).Scan(&balance)
	return balance, err
}

func creditRevenueStarsToUserTx(ctx context.Context, tx pgx.Tx, out *domain.ChannelRevenueWithdrawal, date int) error {
	if err := tx.QueryRow(ctx, `UPDATE stars_balances SET balance=balance+$2,updated_at=now()
WHERE user_id=$1 RETURNING balance`, out.CreatorUserID, out.Amount).Scan(&out.UserBalanceAfter); err != nil {
		return err
	}
	return tx.QueryRow(ctx, `INSERT INTO stars_transactions
(user_id,peer_type,peer_id,amount,reason,title,description,date)
VALUES($1,'channel',$2,$3,$4,'Channel revenue claim','',$5) RETURNING id`, out.CreatorUserID, out.ChannelID,
		out.Amount, string(domain.StarsReasonWithdrawal), date).Scan(&out.UserTransactionID)
}

func creditRevenueTONToUserTx(ctx context.Context, tx pgx.Tx, out *domain.ChannelRevenueWithdrawal, date int) error {
	if err := tx.QueryRow(ctx, `UPDATE ton_balances SET balance_nanoton=balance_nanoton+$2,updated_at=now()
WHERE user_id=$1 RETURNING balance_nanoton`, out.CreatorUserID, out.Amount).Scan(&out.UserBalanceAfter); err != nil {
		return err
	}
	return tx.QueryRow(ctx, `INSERT INTO ton_transactions
(user_id,amount_nanoton,reason,peer_type,peer_id,date)
VALUES($1,$2,$3,'channel',$4,$5) RETURNING id`, out.CreatorUserID, out.Amount,
		string(domain.StarsReasonWithdrawal), out.ChannelID, date).Scan(&out.UserTransactionID)
}
