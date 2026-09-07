package postgres

import (
	"context"
	"testing"

	"telesrv/internal/domain"
)

func TestReserveUserPtsRejectsZeroBeforeQuery(t *testing.T) {
	if _, err := reserveUserPts(context.Background(), nil, 0, 1); err == nil {
		t.Fatal("reserveUserPts user=0 succeeded, want fail-fast before DB access")
	}
}

func TestAppendAllocatedFirstPtsRangeAndRollback(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	owner, err := NewUserStore(pool).Create(ctx, domain.User{
		AccessHash: 4, Phone: "+1555" + randomSuffix(t) + "01", FirstName: "FirstPtsRange",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", owner.ID) })
	event := domain.UpdateEvent{
		Type: domain.UpdateEventDeleteMessages, PtsCount: 3, Date: 1700000003,
		MessageIDs: []int{101, 102, 103},
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	allocated, err := NewUpdateEventStore(tx).AppendAllocated(ctx, owner.ID, event)
	if err != nil {
		_ = tx.Rollback(ctx)
		t.Fatal(err)
	}
	if allocated.Pts != 3 || allocated.PtsCount != 3 {
		_ = tx.Rollback(ctx)
		t.Fatalf("allocated=%+v want pts/count 3/3", allocated)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM user_update_watermarks WHERE user_id=$1`, owner.ID).Scan(&rows); err != nil || rows != 0 {
		t.Fatalf("watermark after rollback rows=%d err=%v", rows, err)
	}
	allocated, err = NewUpdateEventStore(pool).AppendAllocated(ctx, owner.ID, event)
	if err != nil || allocated.Pts != 3 || allocated.PtsCount != 3 {
		t.Fatalf("committed allocation=%+v err=%v", allocated, err)
	}
	if pts, err := NewUpdateEventStore(pool).MaxContiguousPts(ctx, owner.ID); err != nil || pts != 3 {
		t.Fatalf("MaxContiguousPts=%d err=%v", pts, err)
	}
}

func TestAppendRejectsPtsHole(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	owner, err := NewUserStore(pool).Create(ctx, domain.User{
		AccessHash: 1, Phone: "+1556" + randomSuffix(t) + "01", FirstName: "Contig",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", owner.ID) })
	events := NewUpdateEventStore(pool)
	for _, pts := range []int{1, 2, 3} {
		if err := events.Append(ctx, owner.ID, domain.UpdateEvent{Type: domain.UpdateEventNoop, Pts: pts, PtsCount: 1, Date: 1700000000 + pts}); err != nil {
			t.Fatalf("append pts=%d: %v", pts, err)
		}
	}
	if err := events.Append(ctx, owner.ID, domain.UpdateEvent{Type: domain.UpdateEventNoop, Pts: 5, PtsCount: 1, Date: 1700000005}); err == nil {
		t.Fatal("append pts=5 succeeded across a gap")
	}
	if got, err := events.MaxContiguousPts(ctx, owner.ID); err != nil || got != 3 {
		t.Fatalf("contiguous=%d err=%v want=3", got, err)
	}
}

func TestAppendWithDispatchWritesImmutableItemAndLaneAtomically(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	owner, err := NewUserStore(pool).Create(ctx, domain.User{
		AccessHash: 2, Phone: "+1557" + randomSuffix(t) + "01", FirstName: "Dispatch",
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanupDispatchUser(t, pool, owner.ID)
	event := domain.UpdateEvent{
		UserID: owner.ID, Type: domain.UpdateEventDialogPinned, PtsCount: 1, Date: 1700000001,
		Peer: domain.Peer{Type: domain.PeerTypeUser, ID: 1000000002}, Bool: true,
	}
	excludeAuthKeyID := [8]byte{0xff, 0x80, 0x01, 0x02, 0x03, 0x04, 0x05, 0xf0}
	if _, err := NewUpdateEventStore(pool).AppendAllocatedWithDispatch(ctx, owner.ID, event, excludeAuthKeyID, 77); err != nil {
		t.Fatalf("AppendAllocatedWithDispatch: %v", err)
	}
	var pts, headPTS int
	var eventType, laneState string
	var excludeAuth []byte
	var excludeSession int64
	if err := pool.QueryRow(ctx, `
SELECT d.pts, d.event_type, d.exclude_auth_key_id, d.exclude_session_id,
       l.head_sequence, l.state
FROM dispatch_outbox d
JOIN dispatch_outbox_lanes l ON l.stream_id = d.target_user_id
WHERE d.target_user_id = $1`, owner.ID).Scan(
		&pts, &eventType, &excludeAuth, &excludeSession, &headPTS, &laneState,
	); err != nil {
		t.Fatal(err)
	}
	storedAuthKeyID, authErr := outboxAuthKeyID(excludeAuth)
	if pts != 1 || headPTS != 1 || eventType != string(domain.UpdateEventDialogPinned) || authErr != nil ||
		storedAuthKeyID != excludeAuthKeyID || excludeSession != 77 || laneState != "ready" {
		t.Fatalf("item/lane=%d/%d/%s/%x/%d/%s authErr=%v", pts, headPTS, eventType, excludeAuth, excludeSession, laneState, authErr)
	}
	if _, err := pool.Exec(ctx, `UPDATE dispatch_outbox SET event_type = 'noop' WHERE target_user_id = $1`, owner.ID); err == nil {
		t.Fatal("immutable dispatch item UPDATE succeeded")
	}
}
