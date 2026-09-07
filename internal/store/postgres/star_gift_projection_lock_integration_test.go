package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"telesrv/internal/domain"
)

func TestUserStarGiftProjectionLockScopeSerializesReverseAliasesPostgres(t *testing.T) {
	pool := testPool(t)
	baseCtx := context.Background()
	ctx, cancel := context.WithTimeout(baseCtx, 10*time.Second)
	defer cancel()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	low := createTestUser(t, baseCtx, users, "+1887"+suffix+"01", "ProjectionLow", "")
	owner := createTestUser(t, baseCtx, users, "+1887"+suffix+"02", "ProjectionOwner", "")
	high := createTestUser(t, baseCtx, users, "+1887"+suffix+"03", "ProjectionHigh", "")
	userIDs := []int64{low.ID, owner.ID, high.ID}
	t.Cleanup(func() {
		_, _ = pool.Exec(baseCtx, "DELETE FROM dispatch_outbox WHERE target_user_id = ANY($1::bigint[])", userIDs)
		_, _ = pool.Exec(baseCtx, "DELETE FROM user_update_events WHERE user_id = ANY($1::bigint[])", userIDs)
		_, _ = pool.Exec(baseCtx, "DELETE FROM user_update_watermarks WHERE user_id = ANY($1::bigint[])", userIDs)
		_, _ = pool.Exec(baseCtx, "DELETE FROM message_boxes WHERE owner_user_id = ANY($1::bigint[])", userIDs)
		_, _ = pool.Exec(baseCtx, "DELETE FROM private_messages WHERE sender_user_id = ANY($1::bigint[])", userIDs)
		_, _ = pool.Exec(baseCtx, "DELETE FROM dialogs WHERE user_id = ANY($1::bigint[])", userIDs)
		_, _ = pool.Exec(baseCtx, "DELETE FROM users WHERE id = ANY($1::bigint[])", userIDs)
	})

	if !(low.ID < owner.ID && owner.ID < high.ID) {
		t.Fatalf("test users are not monotonic: low=%d owner=%d high=%d", low.ID, owner.ID, high.ID)
	}
	messages := newTestMessageStore(pool, WithMessageAllocators(&perUserCounterAllocator{}))
	now := int(time.Now().Unix())
	ownerToHigh, err := messages.SendPrivateText(baseCtx, domain.SendPrivateTextRequest{
		SenderUserID: owner.ID, RecipientUserID: high.ID, RandomID: time.Now().UnixNano(),
		Message: "gift projection high", Date: now,
	})
	if err != nil {
		t.Fatalf("seed owner-to-high projection: %v", err)
	}
	ownerToLow, err := messages.SendPrivateText(baseCtx, domain.SendPrivateTextRequest{
		SenderUserID: owner.ID, RecipientUserID: low.ID, RandomID: time.Now().UnixNano() + 1,
		Message: "gift projection low", Date: now + 1,
	})
	if err != nil {
		t.Fatalf("seed owner-to-low projection: %v", err)
	}

	// Scope one models a multi-alias gift owned by the middle user: its first
	// projection points high and its second projection points low. Scope two is
	// the reverse concurrent ownership path low->middle. Without precollecting
	// both aliases before locking, the two paths can acquire middle/high before
	// discovering low and form an AB-BA cycle in message/watermark rows.
	wideScope, err := loadUserStarGiftProjectionLockScope(baseCtx, pool, owner.ID, 1,
		ownerToHigh.SenderMessage.ID, ownerToLow.SenderMessage.ID)
	if err != nil {
		t.Fatalf("load wide projection scope: %v", err)
	}
	reverseScope, err := loadUserStarGiftProjectionLockScope(baseCtx, pool, low.ID, 2,
		ownerToLow.RecipientMessage.ID, 0)
	if err != nil {
		t.Fatalf("load reverse projection scope: %v", err)
	}
	if !equalUserIDSets(wideScope.UserIDs, []int64{low.ID, owner.ID, high.ID}) {
		t.Fatalf("wide projection users = %v, want [%d %d %d]", wideScope.UserIDs, low.ID, owner.ID, high.ID)
	}
	if !equalUserIDSets(reverseScope.UserIDs, []int64{low.ID, owner.ID}) {
		t.Fatalf("reverse projection users = %v, want [%d %d]", reverseScope.UserIDs, low.ID, owner.ID)
	}

	gate, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin low-user gate: %v", err)
	}
	defer func() { _ = gate.Rollback(baseCtx) }()
	if err := lockUsersForUpdate(ctx, gate, low.ID); err != nil {
		t.Fatalf("lock low-user gate: %v", err)
	}

	wideTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin wide-scope tx: %v", err)
	}
	defer func() { _ = wideTx.Rollback(baseCtx) }()
	reverseTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin reverse-scope tx: %v", err)
	}
	defer func() { _ = reverseTx.Rollback(baseCtx) }()

	type lockResult struct {
		name string
		err  error
	}
	started := make(chan struct{}, 2)
	results := make(chan lockResult, 2)
	startLock := func(name string, tx pgx.Tx, scope userStarGiftProjectionLockScope) {
		go func() {
			started <- struct{}{}
			results <- lockResult{name: name, err: lockUserStarGiftProjectionUsers(ctx, tx, scope)}
		}()
	}
	startLock("wide", wideTx, wideScope)
	startLock("reverse", reverseTx, reverseScope)
	<-started
	<-started

	select {
	case result := <-results:
		t.Fatalf("%s scope bypassed the pre-held lowest user lock: %v", result.name, result.err)
	case <-time.After(150 * time.Millisecond):
	}

	// Both production helpers must still be waiting on low. If either acquired
	// owner/high first, the try-lock exposes that partial reverse acquisition.
	probe, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin high-lock probe: %v", err)
	}
	var ownerFree, highFree bool
	if err := probe.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1),pg_try_advisory_xact_lock($2)`,
		owner.ID, high.ID).Scan(&ownerFree, &highFree); err != nil {
		_ = probe.Rollback(baseCtx)
		t.Fatalf("probe higher user locks: %v", err)
	}
	if !ownerFree || !highFree {
		_ = probe.Rollback(baseCtx)
		t.Fatalf("projection helper acquired higher lock before low: owner_free=%v high_free=%v", ownerFree, highFree)
	}
	if err := probe.Rollback(ctx); err != nil {
		t.Fatalf("release high-lock probe: %v", err)
	}
	if err := gate.Commit(ctx); err != nil {
		t.Fatalf("release low-user gate: %v", err)
	}

	commitResult := func(result lockResult) {
		t.Helper()
		if result.err != nil {
			t.Fatalf("%s projection lock failed: %v", result.name, result.err)
		}
		var commitErr error
		switch result.name {
		case "wide":
			commitErr = wideTx.Commit(ctx)
		case "reverse":
			commitErr = reverseTx.Commit(ctx)
		default:
			t.Fatalf("unknown lock result %q", result.name)
		}
		if commitErr != nil {
			t.Fatalf("commit %s projection lock: %v", result.name, commitErr)
		}
	}
	for completed := 0; completed < 2; completed++ {
		select {
		case result := <-results:
			commitResult(result)
		case <-ctx.Done():
			t.Fatalf("reverse projection locks did not serialize: %v", ctx.Err())
		}
	}
}
