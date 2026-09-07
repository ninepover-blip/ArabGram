package memory

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

func collectibleUsernameMemoryDeliveryEffects(snapshot store.UsernameAudienceDeliverySnapshot) ([]store.DeliveryEffect, error) {
	effects := make([]store.DeliveryEffect, 0)
	for _, changedUser := range snapshot.Users {
		for _, viewerID := range changedUser.Audience {
			effects = append(effects, store.AbsoluteDeliveryEffect(store.DeliveryOutboxEnqueue{
				TargetUserID:   viewerID,
				Payload:        []byte(fmt.Sprintf("username:%d", changedUser.User.ID)),
				RecoveryPolicy: store.OutboxRecoveryAbsoluteReload,
			}))
		}
	}
	return effects, nil
}

func newCollectibleUsernameDeliveryFixture(t *testing.T, count int) (*CollectibleUsernameStore, *DeliveryOutboxStore, []domain.User) {
	t.Helper()
	ctx := context.Background()
	users := NewUserStore()
	outbox := NewDeliveryOutboxStore()
	registry := NewCollectibleUsernameStore()
	registry.AttachDelivery(users, outbox)
	created := make([]domain.User, 0, count)
	for index := 0; index < count; index++ {
		user, err := users.Create(ctx, domain.User{
			AccessHash: int64(91_000 + index),
			Phone:      fmt.Sprintf("1555900%04d", index),
			FirstName:  fmt.Sprintf("Username%d", index),
		})
		if err != nil {
			t.Fatalf("create delivery user %d: %v", index, err)
		}
		created = append(created, user)
	}
	return registry, outbox, created
}

func TestUsernameRegistryDeliveryAtomicRollbackAndNoopMemory(t *testing.T) {
	ctx := context.Background()
	registry, outbox, users := newCollectibleUsernameDeliveryFixture(t, 1)
	owner := domain.Peer{Type: domain.PeerTypeUser, ID: users[0].ID}
	mustMintCollectible(t, registry, "atomictoggle", owner)

	wantErr := errors.New("encode username update failed")
	changed, err := registry.SetUsernameActiveWithDelivery(ctx, owner, "atomictoggle", false,
		func(store.UsernameAudienceDeliverySnapshot) ([]store.DeliveryEffect, error) {
			return nil, wantErr
		})
	if !errors.Is(err, wantErr) || changed {
		t.Fatalf("failed toggle = changed:%v err:%v, want rollback", changed, err)
	}
	rows, err := registry.PeerUsernames(ctx, owner)
	if err != nil || len(rows) != 1 || !rows[0].Active {
		t.Fatalf("registry after failed toggle = %+v err=%v", rows, err)
	}
	if items := outbox.Snapshot(); len(items) != 0 {
		t.Fatalf("outbox after failed toggle = %+v, want empty", items)
	}

	buildCalls := 0
	changed, err = registry.SetUsernameActiveWithDelivery(ctx, owner, "atomictoggle", false,
		func(snapshot store.UsernameAudienceDeliverySnapshot) ([]store.DeliveryEffect, error) {
			buildCalls++
			if len(snapshot.Users) != 1 || snapshot.Users[0].User.ID != owner.ID ||
				len(snapshot.Users[0].Audience) != 1 || snapshot.Users[0].Audience[0] != owner.ID {
				return nil, fmt.Errorf("unexpected frozen snapshot: %+v", snapshot)
			}
			return collectibleUsernameMemoryDeliveryEffects(snapshot)
		})
	if err != nil || !changed || buildCalls != 1 {
		t.Fatalf("successful toggle = changed:%v buildCalls:%d err:%v", changed, buildCalls, err)
	}
	rows, err = registry.PeerUsernames(ctx, owner)
	if err != nil || len(rows) != 1 || rows[0].Active {
		t.Fatalf("registry after successful toggle = %+v err=%v", rows, err)
	}
	if items := outbox.Snapshot(); len(items) != 1 || items[0].TargetUserID != owner.ID {
		t.Fatalf("outbox after successful toggle = %+v", items)
	}

	changed, err = registry.SetUsernameActiveWithDelivery(ctx, owner, "atomictoggle", false,
		func(store.UsernameAudienceDeliverySnapshot) ([]store.DeliveryEffect, error) {
			t.Fatal("no-op toggle invoked delivery builder")
			return nil, nil
		})
	if err != nil || changed {
		t.Fatalf("no-op toggle = changed:%v err:%v", changed, err)
	}
	if items := outbox.Snapshot(); len(items) != 1 {
		t.Fatalf("outbox after no-op toggle = %+v, want one item", items)
	}
}

func TestCollectibleUsernameTransferDeliveryAtomicRollbackAndReplayMemory(t *testing.T) {
	ctx := context.Background()
	registry, outbox, users := newCollectibleUsernameDeliveryFixture(t, 2)
	from := domain.Peer{Type: domain.PeerTypeUser, ID: users[0].ID}
	to := domain.Peer{Type: domain.PeerTypeUser, ID: users[1].ID}
	seed := mustMintCollectible(t, registry, "atomictransfer", from)
	req := domain.TransferCollectibleUsernameRequest{
		Username: "atomictransfer", To: to, Actor: "operator",
		Reason: "atomic delivery test", CommandKey: "atomic-transfer-command",
	}

	wantErr := errors.New("project transfer failed")
	asset, changed, err := registry.TransferCollectibleUsernameWithDelivery(ctx, req,
		func(store.UsernameAudienceDeliverySnapshot) ([]store.DeliveryEffect, error) {
			return nil, wantErr
		})
	if !errors.Is(err, wantErr) || changed || asset.ID != 0 {
		t.Fatalf("failed transfer = asset:%+v changed:%v err:%v", asset, changed, err)
	}
	current, err := registry.CollectibleUsername(ctx, req.Username)
	if err != nil || current.ID != seed.ID || current.Owner != from || current.Version != seed.Version {
		t.Fatalf("asset after failed transfer = %+v err=%v", current, err)
	}
	fromRows, err := registry.PeerUsernames(ctx, from)
	if err != nil || len(fromRows) != 1 || fromRows[0].CollectibleID != seed.ID {
		t.Fatalf("old owner registry after rollback = %+v err=%v", fromRows, err)
	}
	toRows, err := registry.PeerUsernames(ctx, to)
	if err != nil || len(toRows) != 0 {
		t.Fatalf("new owner registry after rollback = %+v err=%v", toRows, err)
	}
	if items := outbox.Snapshot(); len(items) != 0 {
		t.Fatalf("outbox after failed transfer = %+v, want empty", items)
	}

	buildCalls := 0
	asset, changed, err = registry.TransferCollectibleUsernameWithDelivery(ctx, req,
		func(snapshot store.UsernameAudienceDeliverySnapshot) ([]store.DeliveryEffect, error) {
			buildCalls++
			if len(snapshot.Users) != 2 {
				return nil, fmt.Errorf("transfer snapshot users = %+v", snapshot.Users)
			}
			return collectibleUsernameMemoryDeliveryEffects(snapshot)
		})
	if err != nil || !changed || asset.ID != seed.ID || asset.Owner != to || buildCalls != 1 {
		t.Fatalf("successful transfer = asset:%+v changed:%v buildCalls:%d err:%v", asset, changed, buildCalls, err)
	}
	if items := outbox.Snapshot(); len(items) != 2 {
		t.Fatalf("outbox after successful transfer = %+v, want two audience effects", items)
	}

	replayed, changed, err := registry.TransferCollectibleUsernameWithDelivery(ctx, req,
		func(store.UsernameAudienceDeliverySnapshot) ([]store.DeliveryEffect, error) {
			t.Fatal("command replay invoked delivery builder")
			return nil, nil
		})
	if err != nil || changed || replayed.ID != seed.ID || replayed.Owner != to {
		t.Fatalf("transfer replay = asset:%+v changed:%v err:%v", replayed, changed, err)
	}
	if items := outbox.Snapshot(); len(items) != 2 {
		t.Fatalf("outbox after transfer replay = %+v, want two items", items)
	}
	log, err := registry.CollectibleUsernameTransfers(ctx, seed.ID, 10)
	if err != nil || len(log) != 2 {
		t.Fatalf("provenance after replay = %+v err=%v, want mint plus transfer", log, err)
	}
}

func TestCollectibleUsernameChannelDeliveryUsesEmptyUserSnapshotMemory(t *testing.T) {
	ctx := context.Background()
	registry := NewCollectibleUsernameStore()
	outbox := NewDeliveryOutboxStore()
	registry.AttachDelivery(nil, outbox)
	channel := domain.Peer{Type: domain.PeerTypeChannel, ID: 44001}
	buildCalls := 0

	asset, created, err := registry.MintCollectibleUsernameWithDelivery(ctx,
		collectibleMintRequest("channelatomic", channel, "channel-atomic-command"),
		func(snapshot store.UsernameAudienceDeliverySnapshot) ([]store.DeliveryEffect, error) {
			buildCalls++
			if len(snapshot.Users) != 0 {
				return nil, fmt.Errorf("channel mutation leaked into user snapshot: %+v", snapshot)
			}
			return nil, nil
		})
	if err != nil || !created || asset.Owner != channel || buildCalls != 1 {
		t.Fatalf("channel mint = asset:%+v created:%v buildCalls:%d err:%v", asset, created, buildCalls, err)
	}
	rows, err := registry.PeerUsernames(ctx, channel)
	if err != nil || len(rows) != 1 || rows[0].CollectibleID != asset.ID || !rows[0].Active {
		t.Fatalf("channel registry = %+v err=%v", rows, err)
	}
	if items := outbox.Snapshot(); len(items) != 0 {
		t.Fatalf("channel mutation wrote user outbox = %+v", items)
	}
}
