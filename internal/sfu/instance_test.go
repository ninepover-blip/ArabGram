package sfu

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRegistryOwnerSelectorPicksStableInstance(t *testing.T) {
	registry := NewMemoryInstanceRegistry()
	ctx := context.Background()
	if err := registry.Register(ctx, InstanceRecord{InstanceID: "sfu-b", ControlAddr: "grpc://sfu-b:2450"}, time.Minute); err != nil {
		t.Fatalf("register b: %v", err)
	}
	if err := registry.Register(ctx, InstanceRecord{InstanceID: "sfu-a", ControlAddr: "grpc://sfu-a:2450"}, time.Minute); err != nil {
		t.Fatalf("register a: %v", err)
	}
	selector := NewRegistryOwnerSelector(registry)
	owner, err := selector.PickOwner(ctx, 2)
	if err != nil {
		t.Fatalf("pick owner: %v", err)
	}
	if owner.InstanceID != "sfu-a" || owner.ControlAddr != "grpc://sfu-a:2450" || owner.CallID != 2 {
		t.Fatalf("owner = %+v, want sfu-a for call 2", owner)
	}
	owner, err = selector.PickOwner(ctx, 3)
	if err != nil {
		t.Fatalf("pick owner 3: %v", err)
	}
	if owner.InstanceID != "sfu-b" {
		t.Fatalf("owner for call 3 = %+v, want sfu-b", owner)
	}
}

func TestRegistryOwnerSelectorSkipsFullAndPicksLeastLoaded(t *testing.T) {
	registry := NewMemoryInstanceRegistry()
	ctx := context.Background()
	records := []InstanceRecord{
		{InstanceID: "sfu-full", ControlAddr: "grpc://sfu-full:2450", ActiveCalls: 2, MaxCalls: 2},
		{InstanceID: "sfu-busy", ControlAddr: "grpc://sfu-busy:2450", ActiveCalls: 4, MaxCalls: 0},
		{InstanceID: "sfu-idle", ControlAddr: "grpc://sfu-idle:2450", ActiveCalls: 1, MaxCalls: 3},
	}
	for _, record := range records {
		if err := registry.Register(ctx, record, time.Minute); err != nil {
			t.Fatalf("register %s: %v", record.InstanceID, err)
		}
	}
	owner, err := NewRegistryOwnerSelector(registry).PickOwner(ctx, 99)
	if err != nil {
		t.Fatalf("PickOwner: %v", err)
	}
	if owner.InstanceID != "sfu-idle" {
		t.Fatalf("owner = %+v, want least-loaded non-full sfu-idle", owner)
	}
}

func TestRegistryOwnerSelectorPrefersUnboundedCapacityAtSameLoad(t *testing.T) {
	registry := NewMemoryInstanceRegistry()
	ctx := context.Background()
	if err := registry.Register(ctx, InstanceRecord{InstanceID: "sfu-limited", ControlAddr: "grpc://limited:2450", ActiveCalls: 1, MaxCalls: 4}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(ctx, InstanceRecord{InstanceID: "sfu-unlimited", ControlAddr: "grpc://unlimited:2450", ActiveCalls: 1}, time.Minute); err != nil {
		t.Fatal(err)
	}
	owner, err := NewRegistryOwnerSelector(registry).PickOwner(ctx, 2)
	if err != nil {
		t.Fatalf("PickOwner: %v", err)
	}
	if owner.InstanceID != "sfu-unlimited" {
		t.Fatalf("owner = %+v, want unlimited instance at same load", owner)
	}
}

func TestInstanceRegistryReplacesStaleRecordForSameControlAddr(t *testing.T) {
	registry := NewMemoryInstanceRegistry()
	ctx := context.Background()
	if err := registry.Register(ctx, InstanceRecord{
		InstanceID:  "sfu-old",
		ControlAddr: "grpc://127.0.0.1:2450",
		ActiveCalls: 1,
		MaxCalls:    8,
	}, time.Minute); err != nil {
		t.Fatalf("register old: %v", err)
	}
	if err := registry.Register(ctx, InstanceRecord{
		InstanceID:  "sfu-new",
		ControlAddr: "grpc://127.0.0.1:2450",
		ActiveCalls: 0,
		MaxCalls:    8,
	}, time.Minute); err != nil {
		t.Fatalf("register new: %v", err)
	}
	records, err := registry.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(records) != 1 || records[0].InstanceID != "sfu-new" {
		t.Fatalf("records = %+v, want only sfu-new", records)
	}
}

func TestRegistryOwnerSelectorSkipsUnhealthyInstance(t *testing.T) {
	registry := NewMemoryInstanceRegistry()
	ctx := context.Background()
	if err := registry.Register(ctx, InstanceRecord{InstanceID: "sfu-unhealthy", ControlAddr: "grpc://bad:2450", ActiveCalls: 0}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(ctx, InstanceRecord{InstanceID: "sfu-healthy", ControlAddr: "grpc://good:2450", ActiveCalls: 2}, time.Minute); err != nil {
		t.Fatal(err)
	}
	selector := NewRegistryOwnerSelector(registry, WithInstanceHealthChecker(instanceHealthFunc(func(_ context.Context, record InstanceRecord) error {
		if record.InstanceID == "sfu-unhealthy" {
			return errors.New("connection refused")
		}
		return nil
	})))
	owner, err := selector.PickOwner(ctx, 99)
	if err != nil {
		t.Fatalf("PickOwner: %v", err)
	}
	if owner.InstanceID != "sfu-healthy" {
		t.Fatalf("owner = %+v, want healthy instance despite higher active call count", owner)
	}
}

func TestRegistryOwnerSelectorHealthCheckTimeoutSkipsSlowInstance(t *testing.T) {
	registry := NewMemoryInstanceRegistry()
	ctx := context.Background()
	if err := registry.Register(ctx, InstanceRecord{InstanceID: "sfu-a-slow", ControlAddr: "grpc://slow:2450", ActiveCalls: 0}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(ctx, InstanceRecord{InstanceID: "sfu-b-healthy", ControlAddr: "grpc://healthy:2450", ActiveCalls: 3}, time.Minute); err != nil {
		t.Fatal(err)
	}
	slowStarted := make(chan struct{}, 1)
	selector := NewRegistryOwnerSelector(registry,
		WithInstanceHealthTimeout(20*time.Millisecond),
		WithInstanceHealthChecker(instanceHealthFunc(func(ctx context.Context, record InstanceRecord) error {
			if record.InstanceID != "sfu-a-slow" {
				return nil
			}
			select {
			case slowStarted <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		})))
	start := time.Now()
	owner, err := selector.PickOwner(ctx, 101)
	if err != nil {
		t.Fatalf("PickOwner: %v", err)
	}
	if owner.InstanceID != "sfu-b-healthy" {
		t.Fatalf("owner = %+v, want healthy instance after slow health timeout", owner)
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("PickOwner took %v, want per-instance health timeout to avoid long stall", elapsed)
	}
	select {
	case <-slowStarted:
	default:
		t.Fatal("slow instance health check was not exercised")
	}
}

func TestRegistryOwnerSelectorNoHealthyInstance(t *testing.T) {
	registry := NewMemoryInstanceRegistry()
	ctx := context.Background()
	if err := registry.Register(ctx, InstanceRecord{InstanceID: "sfu-a", ControlAddr: "grpc://a:2450"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	selector := NewRegistryOwnerSelector(registry, WithInstanceHealthChecker(instanceHealthFunc(func(context.Context, InstanceRecord) error {
		return errors.New("health failed")
	})))
	_, err := selector.PickOwner(ctx, 1)
	if !errors.Is(err, ErrNoAvailableInstance) {
		t.Fatalf("PickOwner err = %v, want ErrNoAvailableInstance", err)
	}
}

func TestInstanceHeartbeatPublishesActiveCallCount(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registry := NewMemoryInstanceRegistry()
	active := activeCallProviderFunc(func() []int64 { return []int64{10, 10, 20, 0} })
	go RunInstanceHeartbeatWithActiveCalls(ctx, registry, InstanceRecord{
		InstanceID:  "sfu-a",
		ControlAddr: "grpc://sfu-a:2450",
		MaxCalls:    5,
	}, time.Second, 50*time.Millisecond, active)
	deadline := time.After(time.Second)
	for {
		records, err := registry.List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(records) == 1 && records[0].ActiveCalls == 2 && records[0].MaxCalls == 5 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("heartbeat records = %+v, want active_calls=2 max_calls=5", records)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func TestInstanceHeartbeatStatusReadiness(t *testing.T) {
	now := time.Date(2026, 8, 17, 10, 0, 0, 0, time.UTC)
	status := NewInstanceHeartbeatStatus(time.Minute)
	status.now = func() time.Time { return now }

	if err := status.Ready(context.Background()); !errors.Is(err, ErrInstanceHeartbeatStale) {
		t.Fatalf("initial Ready err = %v, want ErrInstanceHeartbeatStale", err)
	}

	status.ObserveInstanceHeartbeat(nil)
	if err := status.Ready(context.Background()); err != nil {
		t.Fatalf("Ready after success: %v", err)
	}

	heartbeatErr := errors.New("redis unavailable")
	status.ObserveInstanceHeartbeat(heartbeatErr)
	if err := status.Ready(context.Background()); !errors.Is(err, heartbeatErr) {
		t.Fatalf("Ready after heartbeat error = %v, want %v", err, heartbeatErr)
	}

	status.ObserveInstanceHeartbeat(nil)
	now = now.Add(time.Minute)
	if err := status.Ready(context.Background()); !errors.Is(err, ErrInstanceHeartbeatStale) {
		t.Fatalf("Ready after stale heartbeat = %v, want ErrInstanceHeartbeatStale", err)
	}
}

func TestRegistryOwnerSelectorNoAvailableInstance(t *testing.T) {
	selector := NewRegistryOwnerSelector(NewMemoryInstanceRegistry())
	_, err := selector.PickOwner(context.Background(), 1)
	if !errors.Is(err, ErrNoAvailableInstance) {
		t.Fatalf("PickOwner err = %v, want ErrNoAvailableInstance", err)
	}
}

type activeCallProviderFunc func() []int64

func (f activeCallProviderFunc) ActiveCallIDs() []int64 { return f() }

type instanceHealthFunc func(context.Context, InstanceRecord) error

func (f instanceHealthFunc) CheckInstance(ctx context.Context, record InstanceRecord) error {
	return f(ctx, record)
}
