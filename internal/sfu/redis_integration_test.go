package sfu

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisSFUInstanceRegistrySelectorAndOwnerClaim(t *testing.T) {
	clientA, clientB, prefix := openSFURedisIntegration(t)
	defer clientA.Close()
	defer clientB.Close()
	defer cleanupSFURedisPrefix(t, clientA, prefix)

	ctx := context.Background()
	instances := NewRedisInstanceRegistry(clientA, WithRedisInstancePrefix(prefix+":instance"))
	ownersA := NewRedisOwnerRegistry(clientA, WithRedisOwnerPrefix(prefix+":owner"))
	ownersB := NewRedisOwnerRegistry(clientB, WithRedisOwnerPrefix(prefix+":owner"))
	for _, record := range []InstanceRecord{
		{InstanceID: "sfu-full", ControlAddr: "grpc://full:2450", AdvertiseIP: "10.0.0.1", UDPPort: 12300, ActiveCalls: 2, MaxCalls: 2},
		{InstanceID: "sfu-busy", ControlAddr: "grpc://busy:2450", AdvertiseIP: "10.0.0.2", UDPPort: 12301, ActiveCalls: 4},
		{InstanceID: "sfu-idle", ControlAddr: "grpc://idle:2450", AdvertiseIP: "10.0.0.3", UDPPort: 12302, ActiveCalls: 1, MaxCalls: 3},
	} {
		if err := instances.Register(ctx, record, time.Minute); err != nil {
			t.Fatalf("register %s: %v", record.InstanceID, err)
		}
	}

	selector := NewRegistryOwnerSelector(instances)
	const callID = int64(91001)
	selected, err := selector.PickOwner(ctx, callID)
	if err != nil {
		t.Fatalf("PickOwner: %v", err)
	}
	if selected.InstanceID != "sfu-idle" || selected.ControlAddr != "grpc://idle:2450" || selected.UDPPort != 12302 {
		t.Fatalf("selected owner = %+v, want least-loaded non-full sfu-idle", selected)
	}

	localIdle := &ownerFakeSFU{}
	ownerIdle, err := NewOwnerService(localIdle, ownersA, "sfu-idle", time.Minute,
		WithOwnerSelector(selector),
		WithOwnerControlAddr("grpc://idle:2450"),
		WithOwnerAdvertise("10.0.0.3", 12302))
	if err != nil {
		t.Fatalf("owner idle: %v", err)
	}
	localBusy := &ownerFakeSFU{}
	ownerBusy, err := NewOwnerService(localBusy, ownersB, "sfu-busy", time.Minute,
		WithOwnerSelector(selector),
		WithOwnerControlAddr("grpc://busy:2450"),
		WithOwnerAdvertise("10.0.0.2", 12301))
	if err != nil {
		t.Fatalf("owner busy: %v", err)
	}

	if _, err := ownerIdle.Join(ctx, callID, 1001, EndpointMain, ClientOffer{Ufrag: "u", Pwd: "p"}); err != nil {
		t.Fatalf("idle join: %v", err)
	}
	if localIdle.joinCount() != 1 {
		t.Fatalf("idle local joins = %d, want 1", localIdle.joinCount())
	}
	if _, err := ownerBusy.Join(ctx, callID, 1002, EndpointMain, ClientOffer{Ufrag: "u", Pwd: "p"}); !errors.Is(err, ErrRemoteOwnerUnavailable) {
		t.Fatalf("busy join err = %v, want remote owner unavailable", err)
	}
	if localBusy.joinCount() != 0 {
		t.Fatalf("remote-owned call opened on busy SFU: joins=%d", localBusy.joinCount())
	}
	owner, found, err := ownersB.Get(ctx, callID)
	if err != nil || !found || owner.InstanceID != "sfu-idle" || owner.ControlAddr != "grpc://idle:2450" {
		t.Fatalf("redis owner = %+v found=%v err=%v, want sfu-idle", owner, found, err)
	}
	listed, err := ownersB.ListInstance(ctx, "sfu-idle")
	if err != nil || len(listed) != 1 || listed[0].CallID != callID {
		t.Fatalf("owner index = %+v err=%v, want call %d", listed, err, callID)
	}
	if err := ownersB.Release(ctx, callID, "sfu-busy"); err != nil {
		t.Fatalf("wrong-owner release: %v", err)
	}
	if _, found, err := ownersA.Get(ctx, callID); err != nil || !found {
		t.Fatalf("owner after wrong release found=%v err=%v, want retained", found, err)
	}
	if err := ownersA.Release(ctx, callID, "sfu-idle"); err != nil {
		t.Fatalf("owner release: %v", err)
	}
	if _, found, err := ownersB.Get(ctx, callID); err != nil || found {
		t.Fatalf("owner after release found=%v err=%v, want deleted", found, err)
	}
}

func TestRedisSFUOwnerServiceRoutesSelectedRemoteInstanceThroughGRPC(t *testing.T) {
	clientA, clientB, prefix := openSFURedisIntegration(t)
	defer clientA.Close()
	defer clientB.Close()
	defer cleanupSFURedisPrefix(t, clientA, prefix)

	remoteLocal := &redisRemoteSFU{alive: map[int64][]int64{93001: []int64{2001, 2002}}}
	addr := startTestSFUGRPC(t, remoteLocal, "remote-sfu", "secret", nil)

	ctx := context.Background()
	instances := NewRedisInstanceRegistry(clientA, WithRedisInstancePrefix(prefix+":instance-grpc"))
	owners := NewRedisOwnerRegistry(clientB, WithRedisOwnerPrefix(prefix+":owner-grpc"))
	if err := instances.Register(ctx, InstanceRecord{
		InstanceID:  "remote-sfu",
		ControlAddr: addr,
		ActiveCalls: 0,
		MaxCalls:    8,
	}, time.Minute); err != nil {
		t.Fatalf("register remote sfu: %v", err)
	}
	remote, err := NewGRPCRemoteService(GRPCRemoteConfig{Token: "secret"})
	if err != nil {
		t.Fatalf("NewGRPCRemoteService: %v", err)
	}
	defer func() { _ = remote.Close() }()
	core, err := NewRemoteOwnerService(owners, "core-a", time.Minute,
		WithOwnerSelector(NewRegistryOwnerSelector(instances, WithInstanceHealthChecker(remote))),
		WithRemoteService(remote))
	if err != nil {
		t.Fatalf("owner service: %v", err)
	}

	answer, err := core.Join(ctx, 93001, 2001, EndpointMain, ClientOffer{Ufrag: "u", Pwd: "p"})
	if err != nil {
		t.Fatalf("join selected remote owner: %v", err)
	}
	if answer.Ufrag != "remote" || remoteLocal.joinCount() != 1 {
		t.Fatalf("answer=%+v remote joins=%d, want remote join", answer, remoteLocal.joinCount())
	}
	owner, found, err := owners.Get(ctx, 93001)
	if err != nil || !found || owner.InstanceID != "remote-sfu" || owner.ControlAddr != addr {
		t.Fatalf("owner record = %+v found=%v err=%v, want remote-sfu", owner, found, err)
	}
	alive := core.AliveUserIDs(93001)
	if len(alive) != 2 || alive[0] != 2001 || alive[1] != 2002 {
		t.Fatalf("alive users = %+v, want remote users", alive)
	}
	if err := core.Leave(ctx, 93001, 2001, EndpointMain); err != nil {
		t.Fatalf("leave selected remote owner: %v", err)
	}
	if remoteLocal.leaveCount() != 1 {
		t.Fatalf("remote leaves = %d, want 1", remoteLocal.leaveCount())
	}
	if err := core.CloseRoom(ctx, 93001); err != nil {
		t.Fatalf("close selected remote owner: %v", err)
	}
	if remoteLocal.closeCount() != 1 {
		t.Fatalf("remote closes = %d, want 1", remoteLocal.closeCount())
	}
	if _, found, err := owners.Get(ctx, 93001); err != nil || found {
		t.Fatalf("owner after close found=%v err=%v, want released", found, err)
	}
}

func TestRedisSFUInstanceRegistryReplacesSameControlAddr(t *testing.T) {
	client, _, prefix := openSFURedisIntegration(t)
	defer client.Close()
	defer cleanupSFURedisPrefix(t, client, prefix)

	ctx := context.Background()
	instances := NewRedisInstanceRegistry(client, WithRedisInstancePrefix(prefix+":instance-replace"))
	if err := instances.Register(ctx, InstanceRecord{
		InstanceID:  "sfu-old",
		ControlAddr: "grpc://127.0.0.1:2450",
		ActiveCalls: 1,
		MaxCalls:    8,
	}, time.Minute); err != nil {
		t.Fatalf("register old: %v", err)
	}
	if err := instances.Register(ctx, InstanceRecord{
		InstanceID:  "sfu-new",
		ControlAddr: "grpc://127.0.0.1:2450",
		ActiveCalls: 0,
		MaxCalls:    8,
	}, time.Minute); err != nil {
		t.Fatalf("register new: %v", err)
	}
	records, err := instances.List(ctx)
	if err != nil {
		t.Fatalf("list instances: %v", err)
	}
	if len(records) != 1 || records[0].InstanceID != "sfu-new" {
		t.Fatalf("records = %+v, want only sfu-new", records)
	}
}

func TestRedisSFUOwnerServiceSkipsUnhealthyAndFullRemoteInstances(t *testing.T) {
	clientA, clientB, prefix := openSFURedisIntegration(t)
	defer clientA.Close()
	defer clientB.Close()
	defer cleanupSFURedisPrefix(t, clientA, prefix)

	unhealthyLocal := &redisRemoteSFU{}
	unhealthyAddr := startTestSFUGRPC(t, unhealthyLocal, "sfu-unhealthy-idle", "secret", func(context.Context) error {
		return errors.New("heartbeat failed")
	})
	fullLocal := &redisRemoteSFU{}
	fullAddr := startTestSFUGRPC(t, fullLocal, "sfu-full", "secret", nil)
	healthyLocal := &redisRemoteSFU{}
	healthyAddr := startTestSFUGRPC(t, healthyLocal, "sfu-healthy", "secret", nil)

	ctx := context.Background()
	instances := NewRedisInstanceRegistry(clientA, WithRedisInstancePrefix(prefix+":instance-health"))
	owners := NewRedisOwnerRegistry(clientB, WithRedisOwnerPrefix(prefix+":owner-health"))
	for _, record := range []InstanceRecord{
		{InstanceID: "sfu-unhealthy-idle", ControlAddr: unhealthyAddr, ActiveCalls: 0, MaxCalls: 8},
		{InstanceID: "sfu-full", ControlAddr: fullAddr, ActiveCalls: 2, MaxCalls: 2},
		{InstanceID: "sfu-healthy", ControlAddr: healthyAddr, ActiveCalls: 5, MaxCalls: 8},
	} {
		if err := instances.Register(ctx, record, time.Minute); err != nil {
			t.Fatalf("register %s: %v", record.InstanceID, err)
		}
	}

	remote, err := NewGRPCRemoteService(GRPCRemoteConfig{Token: "secret"})
	if err != nil {
		t.Fatalf("NewGRPCRemoteService: %v", err)
	}
	defer func() { _ = remote.Close() }()
	core, err := NewRemoteOwnerService(owners, "core-a", time.Minute,
		WithOwnerSelector(NewRegistryOwnerSelector(instances, WithInstanceHealthChecker(remote))),
		WithRemoteService(remote))
	if err != nil {
		t.Fatalf("owner service: %v", err)
	}

	answer, err := core.Join(ctx, 94001, 3001, EndpointMain, ClientOffer{Ufrag: "u", Pwd: "p"})
	if err != nil {
		t.Fatalf("join selected healthy remote owner: %v", err)
	}
	if answer.Ufrag != "remote" || healthyLocal.joinCount() != 1 {
		t.Fatalf("answer=%+v healthy joins=%d, want healthy remote join", answer, healthyLocal.joinCount())
	}
	if unhealthyLocal.joinCount() != 0 || fullLocal.joinCount() != 0 {
		t.Fatalf("unavailable candidates received joins: unhealthy=%d full=%d", unhealthyLocal.joinCount(), fullLocal.joinCount())
	}
	owner, found, err := owners.Get(ctx, 94001)
	if err != nil || !found || owner.InstanceID != "sfu-healthy" || owner.ControlAddr != healthyAddr {
		t.Fatalf("owner record = %+v found=%v err=%v, want healthy remote", owner, found, err)
	}
}

func TestRedisSFURegistriesExpireByRecordTimestamp(t *testing.T) {
	client, _, prefix := openSFURedisIntegration(t)
	defer client.Close()
	defer cleanupSFURedisPrefix(t, client, prefix)

	ctx := context.Background()
	now := time.Date(2026, 8, 17, 12, 0, 0, 0, time.UTC)
	instanceRegistry := NewRedisInstanceRegistry(client,
		WithRedisInstancePrefix(prefix+":instance-expire"),
		WithRedisInstanceNow(func() time.Time { return now }))
	if err := instanceRegistry.Register(ctx, InstanceRecord{InstanceID: "sfu-stale", ControlAddr: "grpc://stale:2450"}, 30*time.Second); err != nil {
		t.Fatalf("register stale instance: %v", err)
	}
	if records, err := instanceRegistry.List(ctx); err != nil || len(records) != 1 {
		t.Fatalf("fresh instance records = %+v err=%v, want one", records, err)
	}
	now = now.Add(31 * time.Second)
	if records, err := instanceRegistry.List(ctx); err != nil || len(records) != 0 {
		t.Fatalf("expired instance records = %+v err=%v, want empty", records, err)
	}

	now = time.Date(2026, 8, 17, 13, 0, 0, 0, time.UTC)
	ownerRegistry := NewRedisOwnerRegistry(client,
		WithRedisOwnerPrefix(prefix+":owner-expire"),
		WithRedisOwnerNow(func() time.Time { return now }))
	owner, local, err := ownerRegistry.Claim(ctx, OwnerRecord{CallID: 92001, InstanceID: "sfu-a"}, 30*time.Second)
	if err != nil || !local || owner.InstanceID != "sfu-a" {
		t.Fatalf("claim owner = %+v local=%v err=%v", owner, local, err)
	}
	now = now.Add(31 * time.Second)
	if owner, found, err := ownerRegistry.Get(ctx, 92001); err != nil || found {
		t.Fatalf("expired owner = %+v found=%v err=%v, want empty", owner, found, err)
	}
}

type redisRemoteSFU struct {
	mu     sync.Mutex
	joins  int
	leaves int
	closes int
	alive  map[int64][]int64
}

func (f *redisRemoteSFU) Enabled() bool { return true }

func (f *redisRemoteSFU) Join(context.Context, int64, int64, EndpointKind, ClientOffer) (ServerAnswer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.joins++
	return ServerAnswer{Ufrag: "remote", Pwd: "pwd", FingerprintSHA256: "AA"}, nil
}

func (f *redisRemoteSFU) Leave(context.Context, int64, int64, EndpointKind) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leaves++
	return nil
}

func (f *redisRemoteSFU) CloseRoom(context.Context, int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closes++
	return nil
}

func (f *redisRemoteSFU) AliveUserIDs(callID int64) []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.alive[callID]...)
}

func (f *redisRemoteSFU) joinCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.joins
}

func (f *redisRemoteSFU) leaveCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.leaves
}

func (f *redisRemoteSFU) closeCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closes
}

func openSFURedisIntegration(t *testing.T) (*redis.Client, *redis.Client, string) {
	t.Helper()
	addr := os.Getenv("TELESRV_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TELESRV_TEST_REDIS_ADDR to run redis integration test")
	}
	password := os.Getenv("TELESRV_TEST_REDIS_PASSWORD")
	db := 0
	if raw := os.Getenv("TELESRV_TEST_REDIS_DB"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("parse TELESRV_TEST_REDIS_DB: %v", err)
		}
		db = parsed
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	a := redis.NewClient(&redis.Options{Addr: addr, Password: password, DB: db})
	if err := a.Ping(ctx).Err(); err != nil {
		_ = a.Close()
		t.Fatalf("redis ping: %v", err)
	}
	b := redis.NewClient(&redis.Options{Addr: addr, Password: password, DB: db})
	if err := b.Ping(ctx).Err(); err != nil {
		_ = a.Close()
		_ = b.Close()
		t.Fatalf("redis ping second client: %v", err)
	}
	return a, b, fmt.Sprintf("test:sfu:%d", time.Now().UnixNano())
}

func cleanupSFURedisPrefix(t *testing.T, client *redis.Client, prefix string) {
	t.Helper()
	keys, err := client.Keys(context.Background(), prefix+"*").Result()
	if err != nil {
		t.Logf("list redis cleanup keys: %v", err)
		return
	}
	if len(keys) == 0 {
		return
	}
	if err := client.Del(context.Background(), keys...).Err(); err != nil {
		t.Logf("cleanup redis keys: %v", err)
	}
}
