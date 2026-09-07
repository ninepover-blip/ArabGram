package store

import "context"

type AuthInvalidationKind string

const (
	AuthInvalidationKindAuthorization          AuthInvalidationKind = "authorization"
	AuthInvalidationKindProtocolAuthKeyDeleted AuthInvalidationKind = "protocol_auth_key_deleted"
)

func (k AuthInvalidationKind) Normalized() AuthInvalidationKind {
	switch k {
	case "":
		return AuthInvalidationKindAuthorization
	case AuthInvalidationKindAuthorization, AuthInvalidationKindProtocolAuthKeyDeleted:
		return k
	default:
		return AuthInvalidationKind("")
	}
}

func (k AuthInvalidationKind) Valid() bool {
	return k.Normalized() != ""
}

// AuthInvalidationEvent is a process-to-process cache invalidation signal for
// authorization membership changes. PostgreSQL remains authoritative; this
// broker only closes the window where another Core process still has a positive
// in-memory auth-key -> user cache entry after logout/revoke/rebind.
type AuthInvalidationEvent struct {
	Kind            AuthInvalidationKind
	SourceID        string
	AuthKeyIDs      [][8]byte
	DateUnix        int64
	OriginSourceID  string
	OriginSessionID int64
}

type AuthInvalidationBroker interface {
	PublishAuthInvalidation(ctx context.Context, event AuthInvalidationEvent) error
	SubscribeAuthInvalidations(ctx context.Context, handle func(context.Context, AuthInvalidationEvent)) error
}
