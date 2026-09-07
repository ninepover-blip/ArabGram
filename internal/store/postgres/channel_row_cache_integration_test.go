package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"telesrv/internal/domain"
)

// TestChannelRowCacheInvalidatesOnNotify 端到端验证：channels 触发器(0117) → LISTEN/NOTIFY →
// ChannelChangeListener → ChannelRowCache 失效闭环。门控于 TELESRV_TEST_POSTGRES_DSN。
//
// 用「listener 连接时的 flush」作为就绪信号(轮询缓存被清空即确认 listener 已 LISTEN)，避免靠
// sleep 猜时序导致竞态。
func TestChannelRowCacheInvalidatesOnNotify(t *testing.T) {
	pool := testPool(t) // 未设 DSN 会 t.Skip
	dsn := os.Getenv("TELESRV_TEST_POSTGRES_DSN")
	ctx := context.Background()
	suffix := randomSuffix(t)

	users := NewUserStore(pool)
	owner, err := users.Create(ctx, domain.User{
		AccessHash: 451,
		Phone:      "+1889" + suffix + "01",
		FirstName:  "CacheOwner",
	})
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}

	cache := NewChannelRowCache(1000)
	channels := newTestChannelStore(pool, WithChannelRowCache(cache))

	var channelID int64
	t.Cleanup(func() {
		if channelID != 0 {
			_, _ = pool.Exec(ctx, "DELETE FROM channels WHERE id = $1", channelID)
		}
		_, _ = pool.Exec(ctx, "DELETE FROM users WHERE id = $1", owner.ID)
	})

	created, err := channels.CreateChannel(ctx, domain.CreateChannelRequest{
		CreatorUserID: owner.ID,
		Title:         "CacheChan " + suffix,
		Megagroup:     true,
		Date:          1700000700,
	})
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	channelID = created.Channel.ID

	// 先填充缓存(此时 listener 未起)。
	if _, err := channels.GetChannel(ctx, owner.ID, channelID); err != nil {
		t.Fatalf("warm GetChannel: %v", err)
	}
	if _, ok := cache.get(channelID); !ok {
		t.Fatalf("GetChannel 后频道行应已缓存")
	}

	// 起 listener；它连接后会 flush 整表——用缓存被清空作为「listener 已就绪」的信号。
	lctx, cancel := context.WithCancel(ctx)
	defer cancel()
	listener := NewChannelChangeListener(dsn, cache, nil)
	go listener.Run(lctx)
	if !waitUntil(2*time.Second, func() bool { _, ok := cache.get(channelID); return !ok }) {
		t.Fatalf("listener 未在预期内连接并 flush 缓存")
	}

	// listener 就绪后重新填充缓存。
	if _, err := channels.GetChannel(ctx, owner.ID, channelID); err != nil {
		t.Fatalf("re-warm GetChannel: %v", err)
	}
	if _, ok := cache.get(channelID); !ok {
		t.Fatalf("re-warm 后频道行应已缓存")
	}

	// 模拟任意写点直接改 channels 表(触发器会 NOTIFY)。
	const newTitle = "Renamed By Trigger"
	if _, err := pool.Exec(ctx, "UPDATE channels SET title = $2, updated_at = now() WHERE id = $1", channelID, newTitle); err != nil {
		t.Fatalf("update title: %v", err)
	}

	// 触发器 → NOTIFY → listener 应实时失效该频道。
	if !waitUntil(3*time.Second, func() bool { _, ok := cache.get(channelID); return !ok }) {
		t.Fatalf("UPDATE 后缓存未在预期内被 NOTIFY 失效")
	}

	// 失效后再读应拿到新值(并回填缓存)。
	view, err := channels.GetChannel(ctx, owner.ID, channelID)
	if err != nil {
		t.Fatalf("post-invalidation GetChannel: %v", err)
	}
	if view.Channel.Title != newTitle {
		t.Fatalf("失效后标题 = %q, want %q", view.Channel.Title, newTitle)
	}
}

func waitUntil(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}
