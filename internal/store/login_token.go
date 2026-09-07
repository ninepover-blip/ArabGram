package store

import (
	"context"
	"time"

	"telesrv/internal/domain"
)

// LoginTokenTarget identifies the QR-login target connection. It is protocol
// state, not a user identity: the token later binds this auth key/session to the
// scanner's user.
type LoginTokenTarget struct {
	RawAuthKeyID [8]byte `json:"raw_auth_key_id"`
	AuthKeyID    [8]byte `json:"auth_key_id"`
	SessionID    int64   `json:"session_id"`
}

// LoginTokenRecord is the shared short-lived QR-login state. Redis stores this
// as JSON under a token key and indexes the target connection to that token key.
type LoginTokenRecord struct {
	Token         []byte               `json:"token"`
	Target        LoginTokenTarget     `json:"target"`
	Authorization domain.Authorization `json:"authorization"`
	ExceptIDs     []int64              `json:"except_ids"`

	CreatedAtUnixMilli int64 `json:"created_at_unix_milli"`
	ExpiresAtUnixMilli int64 `json:"expires_at_unix_milli"`

	Accepting bool `json:"accepting"`
	Accepted  bool `json:"accepted"`

	AcceptedUserID        int64                `json:"accepted_user_id"`
	AcceptedAuthorization domain.Authorization `json:"accepted_auth"`
	AcceptedAtUnixMilli   int64                `json:"accepted_at_unix_milli"`
}

func (r LoginTokenRecord) ExpiresAt() time.Time {
	return time.UnixMilli(r.ExpiresAtUnixMilli)
}

type LoginTokenAcceptStatus string

const (
	LoginTokenAcceptStarted         LoginTokenAcceptStatus = "started"
	LoginTokenAcceptMissing         LoginTokenAcceptStatus = "missing"
	LoginTokenAcceptExpired         LoginTokenAcceptStatus = "expired"
	LoginTokenAcceptAlreadyAccepted LoginTokenAcceptStatus = "already_accepted"
)

// LoginTokenRegistryStore coordinates auth.exportLoginToken/auth.acceptLoginToken
// across Core instances. Implementations must make Begin/Finish atomic for a
// token, so at most one scanner can bind the target auth key.
type LoginTokenRegistryStore interface {
	GetLoginTokenByTarget(ctx context.Context, target LoginTokenTarget) (LoginTokenRecord, bool, error)
	PutLoginToken(ctx context.Context, record LoginTokenRecord, ttl time.Duration) (bool, error)
	GetLoginToken(ctx context.Context, token []byte) (LoginTokenRecord, bool, error)
	BeginLoginTokenAccept(ctx context.Context, token []byte, userID int64, now time.Time) (LoginTokenRecord, LoginTokenAcceptStatus, error)
	FinishLoginTokenAccept(ctx context.Context, token []byte, userID int64, accepted domain.Authorization, now time.Time) (bool, error)
	FailLoginTokenAccept(ctx context.Context, token []byte) error
	DeleteLoginToken(ctx context.Context, token []byte, target LoginTokenTarget) error
}
