package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/store"
	"telesrv/internal/store/postgres/sqlcgen"
)

func TestBotCommandsDeliveryAtomicPostgres(t *testing.T) {
	pool := testPool(t)
	ctx := context.Background()
	users := NewUserStore(pool)
	bots := NewBotStore(pool)
	suffix := time.Now().UnixNano()
	owner, err := users.Create(ctx, domain.User{AccessHash: suffix, Phone: testPhone(suffix, 1), FirstName: "owner"})
	if err != nil {
		t.Fatal(err)
	}
	viewer2, err := users.Create(ctx, domain.User{AccessHash: suffix + 1, Phone: testPhone(suffix, 2), FirstName: "viewer2"})
	if err != nil {
		t.Fatal(err)
	}
	bot, _, err := bots.CreateBotAccountWithDelivery(ctx, domain.User{AccessHash: suffix + 2, FirstName: "bot"}, domain.BotProfile{OwnerUserID: owner.ID}, testBotLifecycleEffects)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM edge_delivery_outbox WHERE target_user_id = $1 AND payload = $2`, owner.ID, testBotLifecyclePayload); err != nil {
		t.Fatal(err)
	}
	// Seed both projections, including a duplicate logical viewer. The frozen
	// audience query must union and dedupe them inside the command transaction.
	if _, err := pool.Exec(ctx, `
INSERT INTO dialogs (user_id, peer_type, peer_id, top_message_id, top_message_date)
VALUES ($1, 'user', $3, 0, 0), ($3, 'user', $1, 0, 0), ($3, 'user', $2, 0, 0)
ON CONFLICT (user_id, peer_type, peer_id) DO NOTHING`, owner.ID, viewer2.ID, bot.ID); err != nil {
		t.Fatal(err)
	}

	var builds atomic.Int32
	effects := func(snapshot store.BotCommandsDeliverySnapshot) ([]store.DeliveryEffect, error) {
		builds.Add(1)
		if len(snapshot.Audience) != 2 || snapshot.Audience[0] != owner.ID || snapshot.Audience[1] != viewer2.ID {
			return nil, errors.New("unexpected frozen audience")
		}
		out := make([]store.DeliveryEffect, len(snapshot.Audience))
		for i, viewerID := range snapshot.Audience {
			out[i] = store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
				TargetUserID: viewerID, Payload: []byte{byte(snapshot.Version)}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
			})
		}
		return out, nil
	}
	before, _, _ := users.ByID(ctx, bot.ID)
	first := []domain.BotCommand{{Command: "start", Description: "begin"}}
	version, changed, err := bots.UpdateBotCommandsWithDelivery(ctx, bot.ID, first, effects)
	if err != nil || !changed || version != before.BotInfoVersion+1 {
		t.Fatalf("first update = version:%d changed:%v err:%v", version, changed, err)
	}
	assertBotCommandDeliveryRows(t, ctx, pool, []int64{owner.ID, viewer2.ID}, 2)

	version, changed, err = bots.UpdateBotCommandsWithDelivery(ctx, bot.ID, first, func(store.BotCommandsDeliverySnapshot) ([]store.DeliveryEffect, error) {
		return nil, errors.New("no-op projected")
	})
	if err != nil || changed || version != before.BotInfoVersion+1 {
		t.Fatalf("no-op = version:%d changed:%v err:%v", version, changed, err)
	}
	assertBotCommandDeliveryRows(t, ctx, pool, []int64{owner.ID, viewer2.ID}, 2)

	projectionErr := errors.New("projection failed")
	if _, _, err := bots.UpdateBotCommandsWithDelivery(ctx, bot.ID, []domain.BotCommand{{Command: "failed", Description: "rollback"}}, func(store.BotCommandsDeliverySnapshot) ([]store.DeliveryEffect, error) {
		return nil, projectionErr
	}); !errors.Is(err, projectionErr) {
		t.Fatalf("projection error = %v", err)
	}
	afterFailure, _, _ := users.ByID(ctx, bot.ID)
	profile, _, _ := bots.GetBot(ctx, bot.ID)
	if afterFailure.BotInfoVersion != version || !botCommandSlicesEqual(profile.Commands, first) {
		t.Fatalf("projection failure leaked: version=%d commands=%v", afterFailure.BotInfoVersion, profile.Commands)
	}
	assertBotCommandDeliveryRows(t, ctx, pool, []int64{owner.ID, viewer2.ID}, 2)

	concurrent := []domain.BotCommand{{Command: "status", Description: "show"}}
	const callers = 12
	start := make(chan struct{})
	errCh := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, err := bots.UpdateBotCommandsWithDelivery(ctx, bot.ID, concurrent, effects)
			errCh <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent update: %v", err)
		}
	}
	if builds.Load() != 2 {
		t.Fatalf("effect builds = %d, want first mutation + one concurrent winner", builds.Load())
	}
	assertBotCommandDeliveryRows(t, ctx, pool, []int64{owner.ID, viewer2.ID}, 4)
	finalUser, _, _ := users.ByID(ctx, bot.ID)
	if finalUser.BotInfoVersion != before.BotInfoVersion+2 {
		t.Fatalf("final version = %d, want %d", finalUser.BotInfoVersion, before.BotInfoVersion+2)
	}

	if _, _, err := bots.UpdateBotCommandsWithDelivery(ctx, bot.ID, []domain.BotCommand{{Command: "missing", Description: "outbox"}}, nil); !errors.Is(err, store.ErrDeliveryOutboxRequired) {
		t.Fatalf("missing builder error = %v", err)
	}
	unchanged, _, _ := users.ByID(ctx, bot.ID)
	profile, _, _ = bots.GetBot(ctx, bot.ID)
	if unchanged.BotInfoVersion != finalUser.BotInfoVersion || !botCommandSlicesEqual(profile.Commands, concurrent) {
		t.Fatalf("missing builder leaked: version=%d commands=%v", unchanged.BotInfoVersion, profile.Commands)
	}
}

func assertBotCommandDeliveryRows(t *testing.T, ctx context.Context, pool sqlcgen.DBTX, targetIDs []int64, want int) {
	t.Helper()
	var got int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM edge_delivery_outbox WHERE target_user_id = ANY($1::bigint[])`, targetIDs).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("delivery rows = %d, want %d", got, want)
	}
}

func testPhone(seed int64, ordinal int) string {
	if seed < 0 {
		seed = -seed
	}
	return fmt.Sprintf("+88%d%d", seed, ordinal)
}
