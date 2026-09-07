package egress

import (
	"context"
	"strings"
	"testing"
	"time"

	"telesrv/internal/edgecontrol"
	"telesrv/internal/store"
)

type serviceLimitEvents struct{ store.DispatchUpdateEventStore }
type serviceLimitDispatch struct{ store.DispatchOutboxStore }
type serviceLimitAbsolute struct{ store.DeliveryOutboxStore }
type serviceLimitChannel struct{ store.ChannelDeliveryStore }
type serviceLimitPlanner struct{ edgecontrol.DeliveryPlanner }
type serviceLimitMutations struct{}

type serviceLimitReadModels struct{}

func (serviceLimitReadModels) PublishReadModelInvalidations(context.Context, []store.ReadModelInvalidation) error {
	return nil
}

func (*serviceLimitMutations) BindAttemptTargets(context.Context, store.OutboxQueueKind, []store.OutboxAttemptTargetSet) ([]store.OutboxBindTargetResult, error) {
	return nil, nil
}

func (*serviceLimitMutations) RecordAttemptEvidenceBatch(context.Context, store.OutboxQueueKind, []store.OutboxAttemptEvidence) ([]store.OutboxEvidenceResult, error) {
	return nil, nil
}

func (*serviceLimitMutations) ResolveTargetAttemptBatch(context.Context, store.OutboxQueueKind, []store.OutboxTargetAttemptResolution) ([]store.OutboxResolutionResult, error) {
	return nil, nil
}

func (*serviceLimitMutations) ResolveOwnedAttemptBatch(context.Context, store.OutboxQueueKind, []store.OutboxOwnedAttemptResolution) ([]store.OutboxResolutionResult, error) {
	return nil, nil
}

func newServiceForLimitTest(cfg Config) error {
	_, err := NewService(
		&serviceLimitEvents{}, &serviceLimitDispatch{}, &serviceLimitAbsolute{}, &serviceLimitChannel{},
		&serviceLimitMutations{}, &serviceLimitPlanner{},
		func(context.Context, []OutboxUpdateRequest) ([][]byte, error) { return nil, nil },
		serviceLimitReadModels{},
		func(context.Context, []ChannelUpdateRequest) ([][]byte, error) { return nil, nil },
		nil, nil,
		cfg,
	)
	return err
}

func newServiceForIdentityLimitTest(instanceID string, workers int) error {
	return newServiceForLimitTest(Config{
		InstanceID: instanceID, Workers: workers, LeaseDuration: 5 * time.Second,
		DeliveryAttemptTimeout: time.Second, DeliveryClockSkewAllowance: time.Second,
	})
}

func TestServiceRejectsInstanceIdentityThatCannotFormDurableOwner(t *testing.T) {
	lastWorker := normalizedOutboxWorkers(store.DispatchOutboxLogicalShards) - 1
	suffixBytes := len(deliveryWorkerOwner("", store.OutboxQueueChannelPTS, lastWorker))
	maxInstanceBytes := store.MaxDeliveryLeaseOwnerBytes - suffixBytes
	if maxInstanceBytes <= 0 || maxInstanceBytes >= edgecontrol.MaxDeliveryInstanceIDBytes {
		t.Fatalf("invalid delivery identity limits: instance=%d owner=%d suffix=%d",
			edgecontrol.MaxDeliveryInstanceIDBytes, store.MaxDeliveryLeaseOwnerBytes, suffixBytes)
	}
	if err := newServiceForIdentityLimitTest(strings.Repeat("i", maxInstanceBytes), store.DispatchOutboxLogicalShards); err != nil {
		t.Fatalf("maximum durable Egress instance identity rejected: %v", err)
	}
	if err := newServiceForIdentityLimitTest(strings.Repeat("i", maxInstanceBytes+1), store.DispatchOutboxLogicalShards); err == nil {
		t.Fatal("Egress instance identity that overflows the durable owner was accepted")
	}
	if err := newServiceForIdentityLimitTest(" egress-a", 1); err == nil {
		t.Fatal("non-canonical Egress instance identity was accepted")
	}
}

func TestServiceRejectsUnboundedRuntimeConfiguration(t *testing.T) {
	base := Config{
		InstanceID: "egress-a", Workers: 4, Batch: 64, WindowSize: 32,
		WindowByteLimit: 2 << 20, LeaseDuration: 5 * time.Second,
		DeliveryAttemptTimeout: time.Second, DeliveryClockSkewAllowance: time.Second,
	}
	tests := []struct {
		name string
		edit func(*Config)
	}{
		{"workers", func(c *Config) { c.Workers = outboxLogicalShards + 1 }},
		{"claim batch", func(c *Config) { c.Batch = edgecontrol.MaxDeliveryBatchItems + 1 }},
		{"window items", func(c *Config) { c.WindowSize = edgecontrol.MaxDeliveryBatchItems + 1 }},
		{"window bytes", func(c *Config) { c.WindowByteLimit = edgecontrol.MaxDeliveryBatchBytes + 1 }},
		{"actor partitions", func(c *Config) { c.ActorPartitions = maxDomainActorPartitions + 1 }},
		{"actor mailbox", func(c *Config) { c.ActorMailbox = maxDomainActorMailbox + 1 }},
		{"actor tasks", func(c *Config) { c.ActorPartitions = 1024; c.ActorMailbox = 1024 }},
		{"actor bytes", func(c *Config) { c.ActorMailboxBytes = maxDomainActorBytes + 1 }},
		{"wire horizon", func(c *Config) {
			c.DeliveryAttemptTimeout = time.Minute
			c.DeliveryClockSkewAllowance = 11 * time.Second
			c.LeaseDuration = 2 * time.Minute
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cfg := base
			test.edit(&cfg)
			if err := newServiceForLimitTest(cfg); err == nil {
				t.Fatal("unbounded Egress runtime configuration was accepted")
			}
		})
	}
}
