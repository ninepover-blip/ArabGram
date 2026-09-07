package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"telesrv/internal/store/postgres/sqlcgen"
)

var errInvalidDispatchOutboxExclusionPair = errors.New("dispatch outbox exclusion requires both raw auth key and session id")

type dispatchEnqueue struct {
	TargetUserID     int64
	Pts              int32
	EventType        string
	ExcludeAuthKeyID [8]byte
	ExcludeSessionID int64
}

// enqueueDispatch is the sole PTS producer boundary. The item INSERT trigger
// installs an absent lane marker with ON CONFLICT DO NOTHING; it never updates
// an active lane.
func enqueueDispatch(ctx context.Context, q *sqlcgen.Queries, arg dispatchEnqueue) error {
	hasAuthKey := arg.ExcludeAuthKeyID != ([8]byte{})
	hasSession := arg.ExcludeSessionID != 0
	if hasAuthKey != hasSession {
		return errInvalidDispatchOutboxExclusionPair
	}
	if q == nil {
		return fmt.Errorf("enqueue dispatch: queries are required")
	}
	return q.EnqueueDispatch(ctx, sqlcgen.EnqueueDispatchParams{
		TargetUserID: arg.TargetUserID, Pts: arg.Pts, EventType: arg.EventType,
		ExcludeAuthKeyID: arg.ExcludeAuthKeyID[:], ExcludeSessionID: arg.ExcludeSessionID,
	})
}

type DispatchOutboxStore struct {
	durableOutboxState
}

type DispatchOutboxOption func(*DispatchOutboxStore)

func WithLeaseTimeout(d time.Duration) DispatchOutboxOption {
	return func(s *DispatchOutboxStore) {
		if d > 0 {
			s.defaultLease = d
		}
	}
}

func NewDispatchOutboxStore(db sqlcgen.DBTX, opts ...DispatchOutboxOption) *DispatchOutboxStore {
	s := &DispatchOutboxStore{
		durableOutboxState: newDurableOutboxState(db, dispatchOutboxStateConfig, defaultOutboxLease),
	}
	for _, option := range opts {
		if option != nil {
			option(s)
		}
	}
	return s
}
