package sfu

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type ownerFakeSFU struct {
	mu          sync.Mutex
	joins       []int64
	leaves      []int64
	closedRooms []int64
	activeCalls []int64
}

func (f *ownerFakeSFU) Enabled() bool { return true }

func (f *ownerFakeSFU) Join(_ context.Context, callID, _ int64, _ EndpointKind, _ ClientOffer) (ServerAnswer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.joins = append(f.joins, callID)
	f.activeCalls = append(f.activeCalls, callID)
	return ServerAnswer{Ufrag: "local", Pwd: "pwd", FingerprintSHA256: "AA"}, nil
}

func (f *ownerFakeSFU) Leave(_ context.Context, callID, _ int64, _ EndpointKind) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leaves = append(f.leaves, callID)
	return nil
}

func (f *ownerFakeSFU) CloseRoom(_ context.Context, callID int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closedRooms = append(f.closedRooms, callID)
	return nil
}

func (f *ownerFakeSFU) AliveUserIDs(int64) []int64 { return nil }

func (f *ownerFakeSFU) ActiveCallIDs() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.activeCalls...)
}

func (f *ownerFakeSFU) joinCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.joins)
}

type ownerFakeRemoteService struct {
	mu          sync.Mutex
	joins       []OwnerRecord
	leaves      []OwnerRecord
	closedRooms []OwnerRecord
	alive       []int64
}

func (f *ownerFakeRemoteService) Join(_ context.Context, owner OwnerRecord, _, _ int64, _ EndpointKind, _ ClientOffer) (ServerAnswer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.joins = append(f.joins, owner)
	return ServerAnswer{Ufrag: "remote", Pwd: "pwd", FingerprintSHA256: "BB"}, nil
}

func (f *ownerFakeRemoteService) Leave(_ context.Context, owner OwnerRecord, _, _ int64, _ EndpointKind) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leaves = append(f.leaves, owner)
	return nil
}

func (f *ownerFakeRemoteService) CloseRoom(_ context.Context, owner OwnerRecord, _ int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closedRooms = append(f.closedRooms, owner)
	return nil
}

func (f *ownerFakeRemoteService) AliveUserIDs(_ context.Context, _ OwnerRecord, _ int64) ([]int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.alive...), nil
}

func (f *ownerFakeRemoteService) joinCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.joins)
}

func TestOwnerServiceDoesNotSplitRemoteOwnedRoom(t *testing.T) {
	ctx := context.Background()
	registry := NewMemoryOwnerRegistry()
	localA := &ownerFakeSFU{}
	localB := &ownerFakeSFU{}
	ownerA, err := NewOwnerService(localA, registry, "sfu-a", time.Minute)
	if err != nil {
		t.Fatalf("owner A: %v", err)
	}
	ownerB, err := NewOwnerService(localB, registry, "sfu-b", time.Minute)
	if err != nil {
		t.Fatalf("owner B: %v", err)
	}
	if _, err := ownerA.Join(ctx, 9001, 1, EndpointMain, ClientOffer{Ufrag: "u", Pwd: "p"}); err != nil {
		t.Fatalf("owner A join: %v", err)
	}
	if _, err := ownerB.Join(ctx, 9001, 2, EndpointMain, ClientOffer{Ufrag: "u", Pwd: "p"}); !errors.Is(err, ErrRemoteOwnerUnavailable) {
		t.Fatalf("owner B remote-owned join err = %v, want ErrRemoteOwnerUnavailable", err)
	}
	if localB.joinCount() != 0 {
		t.Fatalf("remote-owned call was opened locally on owner B")
	}
	if err := ownerA.CloseRoom(ctx, 9001); err != nil {
		t.Fatalf("owner A close: %v", err)
	}
	if _, err := ownerB.Join(ctx, 9001, 2, EndpointMain, ClientOffer{Ufrag: "u", Pwd: "p"}); err != nil {
		t.Fatalf("owner B join after release: %v", err)
	}
	if localB.joinCount() != 1 {
		t.Fatalf("owner B local joins = %d, want 1 after release", localB.joinCount())
	}
}

func TestRemoteOwnerServiceRequiresExplicitRemoteDependencies(t *testing.T) {
	ctx := context.Background()
	owners := NewMemoryOwnerRegistry()
	instances := NewMemoryInstanceRegistry()
	remote := &ownerFakeRemoteService{}

	if _, err := NewRemoteOwnerService(owners, "core-a", time.Minute,
		WithRemoteService(remote)); !errors.Is(err, ErrRemoteOwnerDependencyRequired) {
		t.Fatalf("remote owner without selector err = %v, want ErrRemoteOwnerDependencyRequired", err)
	}
	if _, err := NewRemoteOwnerService(owners, "core-a", time.Minute,
		WithOwnerSelector(NewRegistryOwnerSelector(instances))); !errors.Is(err, ErrRemoteOwnerDependencyRequired) {
		t.Fatalf("remote owner without remote err = %v, want ErrRemoteOwnerDependencyRequired", err)
	}
	if _, err := NewRemoteOwnerService(nil, "core-a", time.Minute,
		WithOwnerSelector(NewRegistryOwnerSelector(instances)), WithRemoteService(remote)); !errors.Is(err, ErrInvalidOwnerRecord) {
		t.Fatalf("remote owner without registry err = %v, want ErrInvalidOwnerRecord", err)
	}

	if err := instances.Register(ctx, InstanceRecord{
		InstanceID:  "remote-sfu",
		ControlAddr: "grpc://remote-sfu:2450",
		ActiveCalls: 0,
		MaxCalls:    8,
	}, time.Minute); err != nil {
		t.Fatalf("register remote instance: %v", err)
	}
	core, err := NewRemoteOwnerService(owners, "core-a", time.Minute,
		WithOwnerSelector(NewRegistryOwnerSelector(instances)), WithRemoteService(remote))
	if err != nil {
		t.Fatalf("remote owner: %v", err)
	}
	if !core.Enabled() {
		t.Fatalf("remote owner is not enabled")
	}
	answer, err := core.Join(ctx, 92001, 7, EndpointMain, ClientOffer{Ufrag: "u", Pwd: "p"})
	if err != nil {
		t.Fatalf("join remote owner: %v", err)
	}
	if answer.Ufrag != "remote" || remote.joinCount() != 1 {
		t.Fatalf("answer=%+v remote joins=%d, want remote path", answer, remote.joinCount())
	}
	owner, found, err := owners.Get(ctx, 92001)
	if err != nil || !found || owner.InstanceID != "remote-sfu" {
		t.Fatalf("owner record = %+v found=%v err=%v, want remote-sfu", owner, found, err)
	}
}

func TestRemoteOwnerServiceRejectsCoreLocalOwnerRecord(t *testing.T) {
	ctx := context.Background()
	owners := NewMemoryOwnerRegistry()
	instances := NewMemoryInstanceRegistry()
	if err := instances.Register(ctx, InstanceRecord{
		InstanceID:  "remote-sfu",
		ControlAddr: "grpc://remote-sfu:2450",
		ActiveCalls: 0,
		MaxCalls:    8,
	}, time.Minute); err != nil {
		t.Fatalf("register remote instance: %v", err)
	}
	if _, _, err := owners.Claim(ctx, OwnerRecord{
		CallID:      92002,
		InstanceID:  "core-a",
		ControlAddr: "core-local",
	}, time.Minute); err != nil {
		t.Fatalf("seed core-local owner: %v", err)
	}
	core, err := NewRemoteOwnerService(owners, "core-a", time.Minute,
		WithOwnerSelector(NewRegistryOwnerSelector(instances)), WithRemoteService(&ownerFakeRemoteService{}))
	if err != nil {
		t.Fatalf("remote owner: %v", err)
	}
	if _, err := core.Join(ctx, 92002, 7, EndpointMain, ClientOffer{Ufrag: "u", Pwd: "p"}); !errors.Is(err, ErrRemoteOwnerUnavailable) {
		t.Fatalf("join with core-local owner err = %v, want ErrRemoteOwnerUnavailable", err)
	}
	if err := core.Leave(ctx, 92002, 7, EndpointMain); !errors.Is(err, ErrRemoteOwnerUnavailable) {
		t.Fatalf("leave with core-local owner err = %v, want ErrRemoteOwnerUnavailable", err)
	}
	if err := core.CloseRoom(ctx, 92002); !errors.Is(err, ErrRemoteOwnerUnavailable) {
		t.Fatalf("close with core-local owner err = %v, want ErrRemoteOwnerUnavailable", err)
	}
}

func TestOwnerServiceHeartbeatRefreshesActiveCalls(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := NewMemoryOwnerRegistry()
	local := &ownerFakeSFU{activeCalls: []int64{9010}}
	owner, err := NewOwnerService(local, registry, "sfu-a", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("owner: %v", err)
	}
	go owner.RunOwnerHeartbeat(ctx, 10*time.Millisecond)
	deadline := time.After(time.Second)
	for {
		record, found, err := registry.Get(ctx, 9010)
		if err != nil {
			t.Fatalf("get owner: %v", err)
		}
		if found && record.InstanceID == "sfu-a" {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("owner heartbeat did not publish active call")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestRefreshRemoteOwnersRefreshesFullHealthyInstanceOwners(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(1000, 0).UTC()
	instances := NewMemoryInstanceRegistry()
	instances.now = func() time.Time { return now }
	owners := NewMemoryOwnerRegistry()
	owners.now = func() time.Time { return now }

	if err := instances.Register(ctx, InstanceRecord{
		InstanceID:  "sfu-full",
		ControlAddr: "grpc://127.0.0.1:2450",
		AdvertiseIP: "127.0.0.1",
		UDPPort:     12398,
		ActiveCalls: 1,
		MaxCalls:    1,
	}, time.Hour); err != nil {
		t.Fatalf("register instance: %v", err)
	}
	owner, local, err := owners.Claim(ctx, OwnerRecord{
		CallID:      91001,
		InstanceID:  "sfu-full",
		ControlAddr: "grpc://127.0.0.1:2450",
	}, time.Minute)
	if err != nil || !local {
		t.Fatalf("claim owner = %+v local=%v err=%v", owner, local, err)
	}
	originalExpiry := owner.ExpiresAtUnix

	now = now.Add(50 * time.Second)
	refreshed, err := RefreshRemoteOwners(ctx, owners, instances, instanceHealthFunc(func(context.Context, InstanceRecord) error {
		return nil
	}), time.Minute, time.Second)
	if err != nil {
		t.Fatalf("refresh remote owners: %v", err)
	}
	if refreshed != 1 {
		t.Fatalf("refreshed = %d, want 1", refreshed)
	}
	got, found, err := owners.Get(ctx, 91001)
	if err != nil || !found {
		t.Fatalf("owner after refresh found=%v err=%v", found, err)
	}
	if got.ExpiresAtUnix <= originalExpiry {
		t.Fatalf("owner expiry = %d, want > original %d", got.ExpiresAtUnix, originalExpiry)
	}
}

func TestRefreshRemoteOwnersSkipsUnhealthyInstanceOwners(t *testing.T) {
	ctx := context.Background()
	now := time.Unix(2000, 0).UTC()
	instances := NewMemoryInstanceRegistry()
	instances.now = func() time.Time { return now }
	owners := NewMemoryOwnerRegistry()
	owners.now = func() time.Time { return now }

	if err := instances.Register(ctx, InstanceRecord{
		InstanceID:  "sfu-unhealthy",
		ControlAddr: "grpc://127.0.0.1:2450",
		ActiveCalls: 1,
		MaxCalls:    8,
	}, time.Hour); err != nil {
		t.Fatalf("register instance: %v", err)
	}
	if _, _, err := owners.Claim(ctx, OwnerRecord{
		CallID:      91002,
		InstanceID:  "sfu-unhealthy",
		ControlAddr: "grpc://127.0.0.1:2450",
	}, time.Minute); err != nil {
		t.Fatalf("claim owner: %v", err)
	}

	now = now.Add(50 * time.Second)
	refreshed, err := RefreshRemoteOwners(ctx, owners, instances, instanceHealthFunc(func(context.Context, InstanceRecord) error {
		return errors.New("unhealthy")
	}), time.Minute, time.Second)
	if err != nil {
		t.Fatalf("refresh remote owners: %v", err)
	}
	if refreshed != 0 {
		t.Fatalf("refreshed = %d, want 0", refreshed)
	}
	now = now.Add(11 * time.Second)
	if _, found, err := owners.Get(ctx, 91002); err != nil || found {
		t.Fatalf("owner after unhealthy skip found=%v err=%v, want expired", found, err)
	}
}
