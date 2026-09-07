package store

import (
	"context"
	"time"

	"telesrv/internal/domain"
)

// PasskeyStore 持久化已注册的 passkey 凭据(durable)。
type PasskeyStore interface {
	InsertPasskey(ctx context.Context, cred domain.PasskeyCredential) error
	GetPasskeyByCredentialID(ctx context.Context, credentialID []byte) (domain.PasskeyCredential, bool, error)
	ListPasskeysByUser(ctx context.Context, userID int64) ([]domain.PasskeyCredential, error)
	UpdatePasskeyUsage(ctx context.Context, credentialID []byte, signCount uint32, lastUsedAt int64) error
	DeletePasskey(ctx context.Context, userID int64, credentialID []byte) (bool, error)
}

// PasskeyChallengeStore 暂存一次性 WebAuthn 挑战(短 TTL、用后即焚)。
type PasskeyChallengeStore interface {
	SavePasskeyChallenge(ctx context.Context, challenge []byte, c domain.PasskeyChallenge, ttl time.Duration) error
	// ConsumePasskeyChallenge 取出并删除挑战;过期或不存在返回 found=false。
	ConsumePasskeyChallenge(ctx context.Context, challenge []byte) (domain.PasskeyChallenge, bool, error)
}
