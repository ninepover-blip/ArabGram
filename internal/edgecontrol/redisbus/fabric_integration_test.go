package redisbus_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/iamxvbaba/td/proto"
	"github.com/iamxvbaba/td/tg"
	"github.com/redis/go-redis/v9"

	"telesrv/internal/edgecontrol"
	"telesrv/internal/edgecontrol/redisbus"
	"telesrv/internal/edgecontrol/redisregistry"
)

func TestRedisFabricRoutesV3DeliveryAdmissionAndSessionControl(t *testing.T) {
	addr := os.Getenv("TELESRV_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TELESRV_TEST_REDIS_ADDR to run redis fabric integration test")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = client.Close() }()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	prefix := fmt.Sprintf("test:edge:fabric:v3:%d", time.Now().UnixNano())
	defer cleanupRedisPrefix(t, client, prefix)
	registry := redisregistry.New(client, redisregistry.WithPrefix(prefix+":location"))
	bus := redisbus.New(client, redisbus.WithPrefix(prefix+":control"))
	rawAuthKeyID := [8]byte{2}
	record := edgecontrol.LocationRecord{InstanceID: "edge-b", UserID: 42, RawAuthKeyID: rawAuthKeyID, BusinessAuthKeyID: [8]byte{8}, SessionID: 22, ReceivesUpdates: true, Layer: 228}
	const leaseID = "edge-b-test-lease"
	if err := registry.AcquireInstanceLease(ctx, record.InstanceID, leaseID, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := registry.ApplyLocationMutations(ctx, record.InstanceID, leaseID, []edgecontrol.LocationMutation{{Record: record}}); err != nil {
		t.Fatal(err)
	}

	subCtx, cancelSub := context.WithCancel(ctx)
	defer cancelSub()
	errCh := make(chan error, 2)
	deliveryCapture := &redisFabricDeliveryCapture{}
	go func() { errCh <- bus.SubscribeDeliveryBatches(subCtx, "edge-b", deliveryCapture.admit) }()
	controller := &redisFabricFullController{seedRawAffected: 1}
	go func() {
		errCh <- bus.SubscribeSessionControls(subCtx, "edge-b", func(_ context.Context, cmd edgecontrol.SessionControlCommand) edgecontrol.SessionControlAck {
			return edgecontrol.HandleSessionControlCommand(controller, cmd)
		})
	}()
	time.Sleep(100 * time.Millisecond)

	payload, err := edgecontrol.EncodeDeliveryUpdate(&tg.Updates{Date: 123})
	if err != nil {
		t.Fatal(err)
	}
	fabric := edgecontrol.NewDeliveryFabric(edgecontrol.DeliveryFabricConfig{InstanceID: "egress-a", Registry: registry, Bus: bus, CommandTimeout: 2 * time.Second})
	defer fabric.Close()
	request := edgecontrol.DeliveryRequest{TargetUserID: 42, NotAfter: time.Now().Add(time.Minute), Items: []edgecontrol.DeliveryItem{{
		Ref:         edgecontrol.DeliveryRef{Domain: edgecontrol.OrderingDomain{Kind: edgecontrol.QueueAccountPTS, StreamID: 42}, OutboxID: 9901, TargetUserID: 42, PTS: 19, LeaseFence: 7, Attempt: 2},
		MessageType: proto.MessageFromServer, PayloadHash: edgecontrol.DeliveryPayloadHash(payload), UpdateBytes: payload,
	}}}
	request.EvidenceDeadline = request.NotAfter.Add(time.Second)
	plan, err := fabric.PrepareDelivery(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Targets()) != 1 || deliveryCapture.count() != 0 {
		t.Fatalf("prepare targets=%+v deliveries=%d", plan.Targets(), deliveryCapture.count())
	}
	result, err := fabric.AdmitPreparedDelivery(ctx, plan)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Targets) != 1 || result.Targets[0].Admission.Outcome != edgecontrol.AdmissionAccepted || deliveryCapture.count() != 1 {
		t.Fatalf("result=%+v deliveries=%d", result, deliveryCapture.count())
	}

	control := edgecontrol.NewSessionControlFabric(edgecontrol.SessionControlFabricConfig{InstanceID: "edge-a", Registry: registry, Bus: bus, CommandTimeout: 2 * time.Second})
	control.BindUserForAuthKey(rawAuthKeyID, 22, 42)
	if controller.boundUserRaw != rawAuthKeyID || controller.boundUserSession != 22 || controller.boundUserID != 42 {
		t.Fatalf("bound user mismatch")
	}
	if affected := control.SeedInheritedLayerForRawAuthKey(rawAuthKeyID, 228); affected != 1 {
		t.Fatalf("affected=%d", affected)
	}

	cancelSub()
	waitRedisFabricSubscribers(t, errCh, 2)
}

func TestRedisSessionControlConcurrentCommandsShareOneAckSubscription(t *testing.T) {
	addr := os.Getenv("TELESRV_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TELESRV_TEST_REDIS_ADDR to run redis fabric integration test")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = client.Close() }()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	prefix := fmt.Sprintf("test:edge:session-ack-mux:%d", time.Now().UnixNano())
	defer cleanupRedisPrefix(t, client, prefix)
	bus := redisbus.New(client, redisbus.WithPrefix(prefix+":control"))

	subCtx, cancelSub := context.WithCancel(ctx)
	errCh := make(chan error, 1)
	go func() {
		errCh <- bus.SubscribeSessionControls(subCtx, "edge-b", func(_ context.Context, cmd edgecontrol.SessionControlCommand) edgecontrol.SessionControlAck {
			return edgecontrol.SessionControlAck{CommandID: cmd.CommandID, Affected: 1}
		})
	}()
	time.Sleep(100 * time.Millisecond)

	const commandCount = 256
	start := make(chan struct{})
	results := make(chan error, commandCount)
	var wait sync.WaitGroup
	wait.Add(commandCount)
	for i := 0; i < commandCount; i++ {
		i := i
		go func() {
			defer wait.Done()
			<-start
			commandCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			commandID := fmt.Sprintf("core-a/%04d", i)
			ack, err := bus.SendSessionControl(commandCtx, "edge-b", edgecontrol.SessionControlCommand{
				CommandID: commandID, SourceInstanceID: "core-a", TargetInstanceID: "edge-b",
				Kind: edgecontrol.SessionControlAddUserChannelMembership, UserID: int64(i + 1), ChannelID: 99,
			})
			if err == nil && (ack.CommandID != commandID || ack.SourceInstanceID != "core-a" || ack.TargetInstanceID != "edge-b" || ack.Affected != 1) {
				err = fmt.Errorf("ack identity mismatch: %+v", ack)
			}
			results <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	ackChannel := prefix + ":control:session:ack:core-a"
	subscribers, err := client.PubSubNumSub(ctx, ackChannel).Result()
	if err != nil {
		t.Fatal(err)
	}
	if got := subscribers[ackChannel]; got != 1 {
		t.Fatalf("shared ACK subscribers=%d want=1 for %d concurrent commands", got, commandCount)
	}

	cancelSub()
	waitRedisFabricSubscribers(t, errCh, 1)
}

type redisFabricDeliveryCapture struct {
	mu      sync.Mutex
	batches []edgecontrol.DeliveryBatch
}

func (c *redisFabricDeliveryCapture) admit(_ context.Context, batch edgecontrol.DeliveryBatch) edgecontrol.DeliveryAdmission {
	c.mu.Lock()
	c.batches = append(c.batches, batch)
	c.mu.Unlock()
	return edgecontrol.DeliveryAdmission{BatchID: batch.BatchID, CommandID: batch.CommandID, SourceInstanceID: batch.SourceInstanceID, TargetInstanceID: batch.TargetInstanceID, Outcome: edgecontrol.AdmissionAccepted}
}
func (c *redisFabricDeliveryCapture) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.batches)
}

func cleanupRedisPrefix(t *testing.T, client *redis.Client, prefix string) {
	t.Helper()
	keys, err := client.Keys(context.Background(), prefix+"*").Result()
	if err != nil {
		t.Logf("list cleanup keys: %v", err)
		return
	}
	if len(keys) > 0 {
		_ = client.Del(context.Background(), keys...).Err()
	}
}
func waitRedisFabricSubscribers(t *testing.T, errCh <-chan error, want int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for i := 0; i < want; i++ {
		select {
		case err := <-errCh:
			if err != nil && err != context.Canceled {
				t.Fatalf("subscriber err=%v", err)
			}
		case <-deadline:
			t.Fatalf("timeout subscriber %d/%d", i+1, want)
		}
	}
}

type redisFabricFullController struct {
	edgecontrol.FullController
	boundUserRaw     [8]byte
	boundUserSession int64
	boundUserID      int64
	seedRawKey       [8]byte
	seedRawLayer     int
	seedRawAffected  int
}

func (c *redisFabricFullController) BindUserForAuthKey(raw [8]byte, session, user int64) {
	c.boundUserRaw = raw
	c.boundUserSession = session
	c.boundUserID = user
}
func (c *redisFabricFullController) SeedInheritedLayerForRawAuthKey(raw [8]byte, layer int) int {
	c.seedRawKey = raw
	c.seedRawLayer = layer
	return c.seedRawAffected
}
