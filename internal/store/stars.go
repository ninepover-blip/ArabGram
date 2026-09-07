package store

import (
	"context"
	"fmt"

	"telesrv/internal/domain"
)

// StarsStore 持久化 Stars 本地账本：per-user 余额 + 交易流水。
// 借记/贷记/授予必须各自在单个事务内原子完成（余额与流水不漂移）。
type StarsStore interface {
	// GetBalance 返回账号当前余额；无行时返回零值（Balance 0, Granted false）。
	GetBalance(ctx context.Context, userID int64) (domain.StarsBalance, error)
	// EnsureGrant 幂等地应用一次起始授予：仅当从未授予时贷记 amount 并置 granted=true、
	// 写一条 grant 流水，全在单事务内完成。返回最新余额 + 本次是否实际授予。
	EnsureGrant(ctx context.Context, userID, amount int64, date int) (domain.StarsBalance, bool, error)
	// Credit 在单事务内为账号入账（amount>0）并写流水（amount=+x）；余额行不存在则创建。
	Credit(ctx context.Context, userID, amount int64, reason domain.StarsTransactionReason, peer domain.Peer, date int, title, desc string) (domain.StarsBalance, error)
	// Debit 在单事务内做 SELECT ... FOR UPDATE 充足性检查后扣款（amount>0），写流水（amount=-x）。
	// 余额不足返回 domain.ErrStarsInsufficient。
	Debit(ctx context.Context, userID, amount int64, reason domain.StarsTransactionReason, peer domain.Peer, date int, title, desc string) (domain.StarsBalance, error)
	// ListTransactions 按方向与顺序做 keyset 分页，返回一页流水 + 当前余额。
	ListTransactions(ctx context.Context, userID int64, query domain.StarsTransactionQuery) (domain.StarsTransactionPage, error)
	// Client-visible ledger mutations use these mandatory aggregate methods so
	// the balance, transaction row and absolute delivery fact commit together.
	CreditWithDelivery(ctx context.Context, userID, amount int64, reason domain.StarsTransactionReason, peer domain.Peer, date int, title, desc string, effects DeliveryEffectsBuilder[domain.StarsBalance]) (domain.StarsBalance, error)
	DebitWithDelivery(ctx context.Context, userID, amount, startingGrant int64, reason domain.StarsTransactionReason, peer domain.Peer, date int, title, desc string, effects DeliveryEffectsBuilder[domain.StarsBalance]) (domain.StarsBalance, error)
}

func ValidateStarsBalanceDeliveryEffects(userID int64, effects []DeliveryEffect) error {
	if userID <= 0 || len(effects) != 1 {
		return fmt.Errorf("stars balance delivery requires exactly one target effect")
	}
	effect := effects[0]
	if effect.Kind != DeliveryEffectAbsolute || effect.TargetUserID != userID {
		return fmt.Errorf("stars balance delivery target does not match ledger owner")
	}
	return effect.Validate()
}

// StarsPurchaseStore owns fiat self-topup, friend-gift and giveaway-launch
// aggregates. Successful settlement commits the affected ledger/message box
// atomically; exact form retries return the original receipt.
type StarsPurchaseStore interface {
	IssueStarsPurchaseForm(context.Context, domain.StarsPurchaseForm) (domain.StarsPurchaseForm, error)
	PurchaseStarsWithDelivery(context.Context, domain.StarsPurchaseRequest, DeliveryEffectsBuilder[domain.StarsPurchaseResult]) (domain.StarsPurchaseResult, error)
	GetStarsGiveawayInfo(context.Context, int64, int64, int, int) (domain.StarsGiveawayInfo, error)
}

// ValidateStarsPurchaseDeliveryEffects enforces the exact non-PTS balance
// owner affected by a checkout. Giveaways do not mutate a user ledger and
// therefore must not manufacture a balance delivery.
func ValidateStarsPurchaseDeliveryEffects(result domain.StarsPurchaseResult, effects []DeliveryEffect) error {
	if result.Balance.UserID == 0 {
		if len(effects) != 0 {
			return fmt.Errorf("stars purchase without user ledger mutation requires no delivery effects")
		}
		return nil
	}
	return ValidateStarsBalanceDeliveryEffects(result.Balance.UserID, effects)
}
