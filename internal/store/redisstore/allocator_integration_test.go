package redisstore

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"
)

type staticCounterSource struct {
	value int
}

func (s staticCounterSource) Current(context.Context, int64) (int, error) {
	return s.value, nil
}

func (s staticCounterSource) CurrentBatch(_ context.Context, userIDs []int64) (map[int64]int, error) {
	out := make(map[int64]int, len(userIDs))
	for _, userID := range userIDs {
		out[userID] = s.value
	}
	return out, nil
}

func TestRedisAllocatorsRejectNegativeOwnerBeforeBackendAccess(t *testing.T) {
	boxes := NewBoxIDAllocator(nil, staticCounterSource{})
	if _, err := boxes.NextBoxID(context.Background(), -1); err == nil {
		t.Fatal("NextBoxID accepted a negative owner")
	}
	if _, err := boxes.CurrentBoxID(context.Background(), -1); err == nil {
		t.Fatal("CurrentBoxID accepted a negative owner")
	}
	channelMessages := NewChannelMessageIDAllocator(nil, staticCounterSource{})
	if _, err := channelMessages.NextChannelMessageID(context.Background(), -1); err == nil {
		t.Fatal("NextChannelMessageID accepted a negative channel")
	}
}

func TestRedisBoxAllocatorRecoverFromCounterSource(t *testing.T) {
	addr := os.Getenv("TELESRV_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TELESRV_TEST_REDIS_ADDR to run redis integration test")
	}
	ctx := context.Background()
	c, err := Open(ctx, addr, "", 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	userID := time.Now().UnixNano()
	t.Cleanup(func() { _ = c.Del(ctx, boxIDKey(userID)).Err() })

	boxes := NewBoxIDAllocator(c, staticCounterSource{value: 100})
	currentBox, err := boxes.CurrentBoxID(ctx, userID)
	if err != nil {
		t.Fatalf("CurrentBoxID: %v", err)
	}
	if currentBox != 100 {
		t.Fatalf("current box = %d, want recovered 100", currentBox)
	}
	nextBox, err := boxes.NextBoxID(ctx, userID)
	if err != nil {
		t.Fatalf("NextBoxID: %v", err)
	}
	if nextBox != 101 {
		t.Fatalf("next box = %d, want 101", nextBox)
	}
}

func TestRedisBoxAllocatorConcurrentFirstUse(t *testing.T) {
	addr := os.Getenv("TELESRV_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TELESRV_TEST_REDIS_ADDR to run redis integration test")
	}
	ctx := context.Background()
	c, err := Open(ctx, addr, "", 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	userID := time.Now().UnixNano()
	t.Cleanup(func() { _ = c.Del(ctx, boxIDKey(userID)).Err() })

	const workers = 32
	boxes := NewBoxIDAllocator(c, staticCounterSource{value: 1000})
	values := make(chan int, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := boxes.NextBoxID(ctx, userID)
			if err != nil {
				errs <- err
				return
			}
			values <- v
		}()
	}
	wg.Wait()
	close(values)
	close(errs)

	for err := range errs {
		t.Fatalf("NextBoxID: %v", err)
	}
	seen := make(map[int]bool, workers)
	for v := range values {
		if v < 1001 || v > 1000+workers {
			t.Fatalf("box id = %d, want recovered contiguous range", v)
		}
		if seen[v] {
			t.Fatalf("duplicate box id %d", v)
		}
		seen[v] = true
	}
	for want := 1001; want <= 1000+workers; want++ {
		if !seen[want] {
			t.Fatalf("missing box id %d", want)
		}
	}
	current, err := boxes.CurrentBoxID(ctx, userID)
	if err != nil {
		t.Fatalf("CurrentBoxID: %v", err)
	}
	if current != 1000+workers {
		t.Fatalf("current box id = %d, want %d", current, 1000+workers)
	}
}

func TestRedisBoxAllocatorBatchAllocatesDistinctUsersOnce(t *testing.T) {
	addr := os.Getenv("TELESRV_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TELESRV_TEST_REDIS_ADDR to run redis integration test")
	}
	ctx := context.Background()
	c, err := Open(ctx, addr, "", 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	firstUser := time.Now().UnixNano()
	secondUser := firstUser + 1
	keys := []string{boxIDKey(firstUser), boxIDKey(secondUser)}
	t.Cleanup(func() { _ = c.Del(ctx, keys...).Err() })
	if err := c.MSet(ctx, keys[0], 40, keys[1], 90).Err(); err != nil {
		t.Fatalf("seed counters: %v", err)
	}

	boxes := NewBoxIDAllocator(c, staticCounterSource{value: 1000})
	got, err := boxes.NextBoxIDs(ctx, []int64{firstUser, secondUser, firstUser})
	if err != nil {
		t.Fatalf("NextBoxIDs: %v", err)
	}
	if len(got) != 2 || got[firstUser] != 41 || got[secondUser] != 91 {
		t.Fatalf("NextBoxIDs = %+v, want first=41 second=91", got)
	}
	if current, err := c.MGet(ctx, keys...).Result(); err != nil {
		t.Fatalf("MGet: %v", err)
	} else if current[0] != "41" || current[1] != "91" {
		t.Fatalf("counters = %#v, want each incremented exactly once", current)
	}
}

func TestRedisBoxAllocatorBatchRecoversMissingCounters(t *testing.T) {
	addr := os.Getenv("TELESRV_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TELESRV_TEST_REDIS_ADDR to run redis integration test")
	}
	ctx := context.Background()
	c, err := Open(ctx, addr, "", 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	firstUser := time.Now().UnixNano()
	secondUser := firstUser + 1
	keys := []string{boxIDKey(firstUser), boxIDKey(secondUser)}
	t.Cleanup(func() { _ = c.Del(ctx, keys...).Err() })

	got, err := NewBoxIDAllocator(c, staticCounterSource{value: 700}).NextBoxIDs(ctx, []int64{firstUser, secondUser})
	if err != nil {
		t.Fatalf("NextBoxIDs: %v", err)
	}
	if got[firstUser] != 701 || got[secondUser] != 701 {
		t.Fatalf("NextBoxIDs = %+v, want both recovered to 701", got)
	}
}

func TestRedisBoxAllocatorBatchRejectsInvalidUserBeforeAllocation(t *testing.T) {
	addr := os.Getenv("TELESRV_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TELESRV_TEST_REDIS_ADDR to run redis integration test")
	}
	ctx := context.Background()
	c, err := Open(ctx, addr, "", 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	userID := time.Now().UnixNano()
	key := boxIDKey(userID)
	t.Cleanup(func() { _ = c.Del(ctx, key).Err() })
	if err := c.Set(ctx, key, 10, 0).Err(); err != nil {
		t.Fatalf("seed counter: %v", err)
	}
	if _, err := NewBoxIDAllocator(c, nil).NextBoxIDs(ctx, []int64{userID, 0}); err == nil {
		t.Fatal("NextBoxIDs accepted zero user id")
	}
	if got, err := c.Get(ctx, key).Int64(); err != nil || got != 10 {
		t.Fatalf("counter after rejected batch = %d err=%v, want unchanged 10", got, err)
	}
}

func TestRedisBoxAllocatorRejectsMissingDurableSourceBeforeAllocation(t *testing.T) {
	addr := os.Getenv("TELESRV_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TELESRV_TEST_REDIS_ADDR to run redis integration test")
	}
	ctx := context.Background()
	c, err := Open(ctx, addr, "", 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	userID := time.Now().UnixNano()
	key := boxIDKey(userID)
	t.Cleanup(func() { _ = c.Del(ctx, key).Err() })
	if err := c.Set(ctx, key, 10, 0).Err(); err != nil {
		t.Fatalf("seed counter: %v", err)
	}
	if _, err := NewBoxIDAllocator(c, nil).NextBoxID(ctx, userID); err == nil {
		t.Fatal("NextBoxID accepted a missing durable source")
	}
	if _, err := NewBoxIDAllocator(c, nil).NextBoxIDs(ctx, []int64{userID}); err == nil {
		t.Fatal("NextBoxIDs accepted a missing durable source")
	}
	if got, err := c.Get(ctx, key).Int64(); err != nil || got != 10 {
		t.Fatalf("counter after rejected allocation = %d err=%v, want unchanged 10", got, err)
	}
}

func TestRedisRateLimiterWindow(t *testing.T) {
	addr := os.Getenv("TELESRV_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("set TELESRV_TEST_REDIS_ADDR to run redis integration test")
	}
	ctx := context.Background()
	c, err := Open(ctx, addr, "", 0)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	key := "test:" + time.Now().Format("150405.000000000")
	t.Cleanup(func() { _ = c.Del(ctx, rateLimitKey(key)).Err() })

	limiter := NewRateLimiter(c)
	allowed, retry, err := limiter.Allow(ctx, key, 1, time.Minute)
	if err != nil {
		t.Fatalf("Allow first: %v", err)
	}
	if !allowed || retry != 0 {
		t.Fatalf("first allowed=%v retry=%d, want allowed", allowed, retry)
	}
	allowed, retry, err = limiter.Allow(ctx, key, 1, time.Minute)
	if err != nil {
		t.Fatalf("Allow second: %v", err)
	}
	if allowed || retry <= 0 {
		t.Fatalf("second allowed=%v retry=%d, want limited with retry", allowed, retry)
	}

	batchKey := key + ":batch"
	t.Cleanup(func() { _ = c.Del(ctx, rateLimitKey(batchKey)).Err() })
	allowed, retry, err = limiter.AllowN(ctx, batchKey, 2, 3, time.Minute)
	if err != nil {
		t.Fatalf("AllowN first: %v", err)
	}
	if !allowed || retry != 0 {
		t.Fatalf("AllowN first allowed=%v retry=%d, want allowed", allowed, retry)
	}
	allowed, retry, err = limiter.AllowN(ctx, batchKey, 2, 3, time.Minute)
	if err != nil {
		t.Fatalf("AllowN second: %v", err)
	}
	if allowed || retry <= 0 {
		t.Fatalf("AllowN second allowed=%v retry=%d, want limited with retry", allowed, retry)
	}

	// Heal a counter left without TTL by an older split INCRBY→EXPIRE writer.
	// Without this branch one transient crash could permanently deny login-code
	// issuance for the affected phone/auth-key limiter dimension.
	orphanKey := key + ":orphan-no-ttl"
	redisOrphanKey := rateLimitKey(orphanKey)
	t.Cleanup(func() { _ = c.Del(ctx, redisOrphanKey).Err() })
	if err := c.Set(ctx, redisOrphanKey, 100, 0).Err(); err != nil {
		t.Fatalf("seed no-TTL counter: %v", err)
	}
	allowed, retry, err = limiter.Allow(ctx, orphanKey, 1, 5*time.Second)
	if err != nil || allowed || retry <= 0 || retry > 5 {
		t.Fatalf("heal no-TTL counter allowed=%v retry=%d err=%v", allowed, retry, err)
	}
	if ttl, err := c.PTTL(ctx, redisOrphanKey).Result(); err != nil || ttl <= 0 || ttl > 5*time.Second {
		t.Fatalf("healed counter TTL=%v err=%v, want (0,5s]", ttl, err)
	}
}
