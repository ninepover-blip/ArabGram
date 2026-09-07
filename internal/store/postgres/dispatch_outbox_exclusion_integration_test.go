package postgres

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"telesrv/internal/domain"
)

func TestDispatchOutboxExclusionPairInvariantPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	owner := createTestUser(t, ctx, NewUserStore(pool), "+1887"+suffix+"01", "OutboxPair", "")
	t.Cleanup(func() { _, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", owner.ID) })

	event := domain.UpdateEvent{
		Type:     domain.UpdateEventDialogPinned,
		PtsCount: 1,
		Date:     1700002300,
		Peer:     domain.Peer{Type: domain.PeerTypeUser, ID: owner.ID},
		Bool:     true,
	}
	for _, test := range []struct {
		name      string
		authKeyID [8]byte
		sessionID int64
	}{
		{name: "auth key only", authKeyID: [8]byte{1}},
		{name: "session only", sessionID: 77},
	} {
		t.Run("write boundary "+test.name, func(t *testing.T) {
			_, err := NewUpdateEventStore(pool).AppendAllocatedWithDispatch(ctx, owner.ID, event, test.authKeyID, test.sessionID)
			if !errors.Is(err, errInvalidDispatchOutboxExclusionPair) {
				t.Fatalf("AppendAllocatedWithDispatch error = %v, want %v", err, errInvalidDispatchOutboxExclusionPair)
			}
		})
	}

	var eventCount int
	if err := pool.QueryRow(ctx, "SELECT count(*)::int FROM user_update_events WHERE user_id = $1", owner.ID).Scan(&eventCount); err != nil {
		t.Fatalf("count events after rejected writes: %v", err)
	}
	if eventCount != 0 {
		t.Fatalf("events after rejected writes = %d, want 0 (transaction rollback)", eventCount)
	}

	stored, err := NewUpdateEventStore(pool).AppendAllocated(ctx, owner.ID, event)
	if err != nil {
		t.Fatalf("append durable event for constraint test: %v", err)
	}
	for _, test := range []struct {
		name      string
		authKeyID [8]byte
		sessionID int64
	}{
		{name: "auth key only", authKeyID: [8]byte{1}},
		{name: "session only", sessionID: 77},
	} {
		t.Run("database constraint "+test.name, func(t *testing.T) {
			_, err := pool.Exec(ctx, `
INSERT INTO dispatch_outbox (
  target_user_id, pts, event_type, exclude_auth_key_id, exclude_session_id
) VALUES ($1, $2, $3, $4, $5)`, owner.ID, stored.Pts, string(stored.Type), test.authKeyID[:], test.sessionID)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "23514" || pgErr.ConstraintName != "dispatch_outbox_exclusion_pair_check" {
				t.Fatalf("direct insert error = %v, want check violation from dispatch_outbox_exclusion_pair_check", err)
			}
		})
	}

	var outboxCount int
	if err := pool.QueryRow(ctx, "SELECT count(*)::int FROM dispatch_outbox WHERE target_user_id = $1", owner.ID).Scan(&outboxCount); err != nil {
		t.Fatalf("count outbox after rejected inserts: %v", err)
	}
	if outboxCount != 0 {
		t.Fatalf("outbox rows after rejected inserts = %d, want 0", outboxCount)
	}
}

func TestDispatchOutboxPreservesRawHighBitAuthKeyIDPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	owner := createTestUser(t, ctx, NewUserStore(pool), "+1887"+randomSuffix(t)+"03", "OutboxRawAuthKey", "")
	t.Cleanup(func() { _, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", owner.ID) })

	// The high protocol bit would make this pattern a negative signed bigint.
	// The durable boundary must carry the bytes unchanged and never reinterpret it.
	authKeyID := [8]byte{0xef, 0xcd, 0xab, 0x89, 0x67, 0x45, 0x23, 0x80}
	if _, err := NewUpdateEventStore(pool).AppendAllocatedWithDispatch(ctx, owner.ID, domain.UpdateEvent{
		Type: domain.UpdateEventDialogPinned, PtsCount: 1, Date: 1700002301,
		Peer: domain.Peer{Type: domain.PeerTypeUser, ID: owner.ID}, Bool: true,
	}, authKeyID, 88); err != nil {
		t.Fatalf("append raw high-bit auth key dispatch: %v", err)
	}
	var raw []byte
	if err := pool.QueryRow(ctx, `SELECT exclude_auth_key_id FROM dispatch_outbox WHERE target_user_id=$1`, owner.ID).Scan(&raw); err != nil {
		t.Fatalf("load raw auth key: %v", err)
	}
	if !bytes.Equal(raw, authKeyID[:]) {
		t.Fatalf("stored auth key = %x, want exact %x", raw, authKeyID)
	}
	var compatibilityHelperExists bool
	if err := pool.QueryRow(ctx, `SELECT to_regprocedure('outbox_auth_key_id_bytes(bigint)') IS NOT NULL`).Scan(&compatibilityHelperExists); err != nil {
		t.Fatalf("inspect removed helper: %v", err)
	}
	if compatibilityHelperExists {
		t.Fatal("runtime bigint compatibility helper still exists")
	}
}

func TestDispatchOutboxZeroExclusionCoversAllSessionsAndOfflineDifference(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	suffix := randomSuffix(t)
	owner := createTestUser(t, ctx, NewUserStore(pool), "+1887"+suffix+"02", "OutboxAllSessions", "")
	t.Cleanup(func() { _, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", owner.ID) })

	events := NewUpdateEventStore(pool)
	for i, eventType := range []domain.UpdateEventType{domain.UpdateEventDialogPinned, domain.UpdateEventDialogUnreadMark} {
		if _, err := events.AppendAllocatedWithDispatch(ctx, owner.ID, domain.UpdateEvent{
			Type: eventType, PtsCount: 1, Date: 1_700_002_400 + i,
			Peer: domain.Peer{Type: domain.PeerTypeUser, ID: owner.ID}, Bool: true,
		}, [8]byte{}, 0); err != nil {
			t.Fatalf("append event %d: %v", i, err)
		}
	}

	rows, err := pool.Query(ctx, `
SELECT exclude_auth_key_id, exclude_session_id
FROM dispatch_outbox
WHERE target_user_id = $1
ORDER BY pts`, owner.ID)
	if err != nil {
		t.Fatalf("query dispatch rows: %v", err)
	}
	defer rows.Close()
	rowCount := 0
	for rows.Next() {
		var raw []byte
		var sessionID int64
		if err := rows.Scan(&raw, &sessionID); err != nil {
			t.Fatalf("scan dispatch row: %v", err)
		}
		authKeyID, err := outboxAuthKeyID(raw)
		if err != nil || authKeyID != ([8]byte{}) || sessionID != 0 {
			t.Fatalf("dispatch exclusion = %x/%d err=%v, want none", raw, sessionID, err)
		}
		rowCount++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate dispatch rows: %v", err)
	}
	if rowCount != 2 {
		t.Fatalf("dispatch rows = %d, want 2", rowCount)
	}

	difference, err := events.ListAfter(ctx, owner.ID, 0, 10)
	if err != nil {
		t.Fatalf("list offline difference events: %v", err)
	}
	if len(difference) != 2 || difference[0].Pts != 1 || difference[1].Pts != 2 || difference[0].PtsCount != 1 || difference[1].PtsCount != 1 {
		t.Fatalf("difference = %+v, want contiguous pts 1,2", difference)
	}
}
