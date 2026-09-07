package phone

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

	"telesrv/internal/domain"
)

func TestRedisActiveCallStoreCrossCoreLifecycle(t *testing.T) {
	clientA, clientB, prefix := openPhoneRedisIntegration(t)
	defer clientA.Close()
	defer clientB.Close()
	defer cleanupPhoneRedisPrefix(t, clientA, prefix)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clk := newTestClock()
	svcA := newRedisPhoneTestService(t, clientA, prefix, clk)
	svcB := newRedisPhoneTestService(t, clientB, prefix, clk)
	ga, gaHash := testGA()
	gb := testGB()
	req := domain.PhoneCallRequest{
		CalleeID:     2,
		RandomID:     9001,
		GAHash:       gaHash,
		Protocol:     testProtocol(),
		CallerDevice: domain.SessionRef{RawAuthKeyID: [8]byte{0xa1}, SessionID: 101},
		PrivacyP2P:   true,
	}

	call, err := svcA.RequestCall(ctx, 1, req)
	if err != nil {
		t.Fatalf("core A request call: %v", err)
	}
	dup, err := svcB.RequestCall(ctx, 1, req)
	if err != nil {
		t.Fatalf("core B duplicate request call: %v", err)
	}
	if dup.ID != call.ID || dup.AccessHash != call.AccessHash {
		t.Fatalf("duplicate call = %+v, want id/access hash from %+v", dup, call)
	}
	if ringing, transitioned, err := svcB.ReceivedCall(ctx, 2, call.ID, call.AccessHash); err != nil || !transitioned || ringing.State != domain.PhoneCallStateRinging {
		t.Fatalf("core B received call = %+v transitioned=%v err=%v", ringing, transitioned, err)
	}
	accepted, err := svcB.AcceptCall(ctx, 2, call.ID, call.AccessHash, gb, testProtocol(), domain.SessionRef{RawAuthKeyID: [8]byte{0xa2}, SessionID: 202})
	if err != nil || accepted.State != domain.PhoneCallStateAccepted {
		t.Fatalf("core B accept call = %+v err=%v", accepted, err)
	}
	confirmed, forced, err := svcA.ConfirmCall(ctx, 1, call.ID, call.AccessHash, ga, 0x1234, testProtocol())
	if err != nil || forced || confirmed.State != domain.PhoneCallStateConfirmed {
		t.Fatalf("core A confirm call = %+v forced=%v err=%v", confirmed, forced, err)
	}
	var forwardedPeer int64
	var forwardedDevice domain.SessionRef
	drop, err := svcB.Signal(ctx, 2, call.ID, call.AccessHash, func(peerUserID int64, peerDevice domain.SessionRef) {
		forwardedPeer = peerUserID
		forwardedDevice = peerDevice
	})
	if err != nil || drop || forwardedPeer != 1 || forwardedDevice.SessionID != 101 {
		t.Fatalf("cross-core signal drop=%v err=%v peer=%d device=%+v", drop, err, forwardedPeer, forwardedDevice)
	}
	discarded, already, err := svcB.DiscardCall(ctx, 2, call.ID, call.AccessHash, domain.PhoneCallDiscardReasonHangup, 9)
	if err != nil || already || discarded.State != domain.PhoneCallStateDiscarded {
		t.Fatalf("core B discard call = %+v already=%v err=%v", discarded, already, err)
	}
	snap, found := svcA.Lookup(ctx, call.ID, call.AccessHash)
	if !found || snap.State != domain.PhoneCallStateDiscarded || snap.Duration != 9 {
		t.Fatalf("core A terminal lookup = %+v found=%v", snap, found)
	}
}

func TestRedisActiveCallStoreCrossCoreAcceptRace(t *testing.T) {
	clientA, clientB, prefix := openPhoneRedisIntegration(t)
	defer clientA.Close()
	defer clientB.Close()
	defer cleanupPhoneRedisPrefix(t, clientA, prefix)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	clk := newTestClock()
	svcA := newRedisPhoneTestService(t, clientA, prefix, clk)
	svcB := newRedisPhoneTestService(t, clientB, prefix, clk)
	_, gaHash := testGA()
	call, err := svcA.RequestCall(ctx, 1, domain.PhoneCallRequest{
		CalleeID:   2,
		RandomID:   9002,
		GAHash:     gaHash,
		Protocol:   testProtocol(),
		PrivacyP2P: true,
	})
	if err != nil {
		t.Fatalf("request call: %v", err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, tc := range []struct {
		svc       *Service
		sessionID int64
	}{
		{svc: svcA, sessionID: 201},
		{svc: svcB, sessionID: 202},
	} {
		wg.Add(1)
		go func(svc *Service, sessionID int64) {
			defer wg.Done()
			_, err := svc.AcceptCall(ctx, 2, call.ID, call.AccessHash, testGB(), testProtocol(), domain.SessionRef{SessionID: sessionID})
			errs <- err
		}(tc.svc, tc.sessionID)
	}
	wg.Wait()
	close(errs)

	successes, alreadyAccepted := 0, 0
	for err := range errs {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrAlreadyAccepted):
			alreadyAccepted++
		default:
			t.Fatalf("accept race err = %v", err)
		}
	}
	if successes != 1 || alreadyAccepted != 1 {
		t.Fatalf("accept race successes=%d alreadyAccepted=%d, want 1/1", successes, alreadyAccepted)
	}
}

func newRedisPhoneTestService(t *testing.T, client redis.UniversalClient, prefix string, clk *testClock) *Service {
	t.Helper()
	svc, err := NewService(Config{}, NewRedisActiveCallStore(client, WithRedisActiveCallPrefix(prefix)), WithClock(clk))
	if err != nil {
		t.Fatalf("new redis phone service: %v", err)
	}
	return svc
}

func openPhoneRedisIntegration(t *testing.T) (*redis.Client, *redis.Client, string) {
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
	return a, b, fmt.Sprintf("test:phone:active:%d", time.Now().UnixNano())
}

func cleanupPhoneRedisPrefix(t *testing.T, client *redis.Client, prefix string) {
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
