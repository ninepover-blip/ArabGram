package memory

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func TestBotCommandsDeliveryAtomicNoopAndConcurrentReplay(t *testing.T) {
	ctx := context.Background()
	users := NewUserStore()
	dialogs := NewDialogStore()
	outbox := NewDeliveryOutboxStore()
	bots := NewBotStore(users)
	bots.AttachDeliveryDependencies(dialogs, outbox)
	owner, err := users.Create(ctx, domain.User{Phone: "+81001", FirstName: "owner"})
	if err != nil {
		t.Fatal(err)
	}
	bot, _, err := bots.CreateBotAccountWithDelivery(ctx, domain.User{FirstName: "bot", Username: "atomic_bot"}, domain.BotProfile{OwnerUserID: owner.ID}, testBotLifecycleEffects)
	if err != nil {
		t.Fatal(err)
	}
	outbox = NewDeliveryOutboxStore()
	bots.AttachDeliveryDependencies(dialogs, outbox)
	viewer2, err := users.Create(ctx, domain.User{Phone: "+81002", FirstName: "viewer2"})
	if err != nil {
		t.Fatal(err)
	}
	// Freeze from both read-model projections and prove the union is deduped.
	if err := dialogs.SaveList(ctx, owner.ID, domain.DialogList{Dialogs: []domain.Dialog{{Peer: domain.Peer{Type: domain.PeerTypeUser, ID: bot.ID}}}}); err != nil {
		t.Fatal(err)
	}
	if err := dialogs.SaveList(ctx, bot.ID, domain.DialogList{Dialogs: []domain.Dialog{
		{Peer: domain.Peer{Type: domain.PeerTypeUser, ID: owner.ID}},
		{Peer: domain.Peer{Type: domain.PeerTypeUser, ID: viewer2.ID}},
	}}); err != nil {
		t.Fatal(err)
	}

	var builds atomic.Int32
	effects := func(snapshot store.BotCommandsDeliverySnapshot) ([]store.DeliveryEffect, error) {
		builds.Add(1)
		if len(snapshot.Audience) != 2 || snapshot.Audience[0] != owner.ID || snapshot.Audience[1] != viewer2.ID {
			t.Fatalf("frozen audience = %v", snapshot.Audience)
		}
		out := make([]store.DeliveryEffect, len(snapshot.Audience))
		for i, viewerID := range snapshot.Audience {
			out[i] = store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
				TargetUserID: viewerID, Payload: []byte{byte(snapshot.Version)}, RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
			})
		}
		return out, nil
	}
	commands := []domain.BotCommand{{Command: "start", Description: "begin"}}
	before, _, _ := users.ByID(ctx, bot.ID)
	version, changed, err := bots.UpdateBotCommandsWithDelivery(ctx, bot.ID, commands, effects)
	if err != nil || !changed || version != before.BotInfoVersion+1 {
		t.Fatalf("first update = version:%d changed:%v err:%v", version, changed, err)
	}
	if got := len(outbox.Snapshot()); got != 2 {
		t.Fatalf("first outbox rows = %d, want 2", got)
	}
	version, changed, err = bots.UpdateBotCommandsWithDelivery(ctx, bot.ID, commands, func(store.BotCommandsDeliverySnapshot) ([]store.DeliveryEffect, error) {
		return nil, errors.New("no-op must not project")
	})
	if err != nil || changed || version != before.BotInfoVersion+1 || len(outbox.Snapshot()) != 2 {
		t.Fatalf("no-op = version:%d changed:%v rows:%d err:%v", version, changed, len(outbox.Snapshot()), err)
	}

	projectionErr := errors.New("projection failed")
	failedCommands := []domain.BotCommand{{Command: "failed", Description: "rollback"}}
	if _, _, err := bots.UpdateBotCommandsWithDelivery(ctx, bot.ID, failedCommands, func(store.BotCommandsDeliverySnapshot) ([]store.DeliveryEffect, error) {
		return nil, projectionErr
	}); !errors.Is(err, projectionErr) {
		t.Fatalf("projection error = %v", err)
	}
	profile, _, _ := bots.GetBot(ctx, bot.ID)
	afterFailure, _, _ := users.ByID(ctx, bot.ID)
	if !botCommandSlicesEqual(profile.Commands, commands) || afterFailure.BotInfoVersion != version || len(outbox.Snapshot()) != 2 {
		t.Fatalf("failed mutation leaked: commands=%v version=%d rows=%d", profile.Commands, afterFailure.BotInfoVersion, len(outbox.Snapshot()))
	}

	concurrentCommands := []domain.BotCommand{{Command: "status", Description: "show"}}
	const callers = 16
	start := make(chan struct{})
	errCh := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, _, err := bots.UpdateBotCommandsWithDelivery(ctx, bot.ID, concurrentCommands, effects)
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
	if got := len(outbox.Snapshot()); got != 4 {
		t.Fatalf("outbox rows after concurrent replay = %d, want 4", got)
	}
	finalUser, _, _ := users.ByID(ctx, bot.ID)
	if finalUser.BotInfoVersion != before.BotInfoVersion+2 {
		t.Fatalf("final version = %d, want %d", finalUser.BotInfoVersion, before.BotInfoVersion+2)
	}
}

func TestBotCommandsDeliveryMissingDependencyAndAudienceOverflowRollback(t *testing.T) {
	ctx := context.Background()
	users := NewUserStore()
	dialogs := NewDialogStore()
	bots := NewBotStore(users)
	bots.AttachDeliveryDependencies(dialogs, NewDeliveryOutboxStore())
	owner, _ := users.Create(ctx, domain.User{Phone: "+82001"})
	bot, _, err := bots.CreateBotAccountWithDelivery(ctx, domain.User{FirstName: "bot", Username: "bounded_bot"}, domain.BotProfile{OwnerUserID: owner.ID}, testBotLifecycleEffects)
	if err != nil {
		t.Fatal(err)
	}
	bots.AttachDeliveryDependencies(dialogs, nil)
	commands := []domain.BotCommand{{Command: "start", Description: "begin"}}
	if _, _, err := bots.UpdateBotCommandsWithDelivery(ctx, bot.ID, commands, func(store.BotCommandsDeliverySnapshot) ([]store.DeliveryEffect, error) { return nil, nil }); !errors.Is(err, store.ErrDeliveryOutboxRequired) {
		t.Fatalf("missing outbox error = %v", err)
	}
	before, _, _ := users.ByID(ctx, bot.ID)
	profile, _, _ := bots.GetBot(ctx, bot.ID)
	if len(profile.Commands) != 0 {
		t.Fatalf("commands changed without outbox: %v", profile.Commands)
	}

	outbox := NewDeliveryOutboxStore()
	bots.AttachDeliveryDependencies(dialogs, outbox)
	// Seed the authoritative read model just over the hard boundary. Direct map
	// setup keeps this limit test fast while exercising the exact locked scan.
	users.mu.Lock()
	for i := 0; i <= store.MaxBotCommandsDeliveryAudience; i++ {
		id := int64(900000 + i)
		users.byID[id] = domain.User{ID: id, FirstName: "viewer"}
		dialogs.m[id] = domain.DialogList{Dialogs: []domain.Dialog{{Peer: domain.Peer{Type: domain.PeerTypeUser, ID: bot.ID}}}}
	}
	users.mu.Unlock()
	called := false
	if _, _, err := bots.UpdateBotCommandsWithDelivery(ctx, bot.ID, commands, func(store.BotCommandsDeliverySnapshot) ([]store.DeliveryEffect, error) {
		called = true
		return nil, nil
	}); err == nil {
		t.Fatal("audience overflow unexpectedly succeeded")
	}
	if called {
		t.Fatal("builder called after audience overflow")
	}
	after, _, _ := users.ByID(ctx, bot.ID)
	profile, _, _ = bots.GetBot(ctx, bot.ID)
	if after.BotInfoVersion != before.BotInfoVersion || len(profile.Commands) != 0 || len(outbox.Snapshot()) != 0 {
		t.Fatalf("overflow leaked mutation: version=%d commands=%v rows=%d", after.BotInfoVersion, profile.Commands, len(outbox.Snapshot()))
	}
}
