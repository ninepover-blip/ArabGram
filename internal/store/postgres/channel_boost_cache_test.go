package postgres

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"telesrv/internal/domain"
)

// boostClock 是 boost 缓存 TTL 测试用的可推进单调时钟(注入 readmodelcache.Config.Now)。
type boostClock struct {
	mu sync.Mutex
	t  int64
}

func newBoostClock(unix int64) *boostClock { return &boostClock{t: unix} }

func (b *boostClock) now() time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	return time.Unix(b.t, 0)
}

func (b *boostClock) set(unix int64) {
	b.mu.Lock()
	b.t = unix
	b.mu.Unlock()
}

func TestChannelBoostCacheNilDisabled(t *testing.T) {
	if c := NewChannelBoostCache(0, time.Second); c != nil {
		t.Fatalf("max<=0 应返回 nil(禁用)，got %v", c)
	}
	if c := NewChannelBoostCache(16, 0); c != nil {
		t.Fatalf("ttl<=0 应返回 nil(禁用)，got %v", c)
	}
	var nilCache *ChannelBoostCache
	peer := domain.Peer{Type: domain.PeerTypeChannel, ID: 10}
	nilCache.put(1, peer, 2)
	nilCache.putPeerTotal(peer, 2)
	nilCache.delete(1, peer)
	nilCache.deletePeerTotal(peer)
	nilCache.deleteUser(1)
	nilCache.flush()
	if _, ok := nilCache.get(1, peer); ok {
		t.Fatalf("nil 缓存 get 必须返回 ok=false")
	}
	if _, ok := nilCache.getPeerTotal(peer); ok {
		t.Fatalf("nil 缓存 getPeerTotal 必须返回 ok=false")
	}
}

func TestChannelBoostCachePutGetExpireDelete(t *testing.T) {
	clk := newBoostClock(1000)
	c := newChannelBoostCache(16, 10*time.Second, clk.now)
	peer := domain.Peer{Type: domain.PeerTypeChannel, ID: 10}
	otherPeer := domain.Peer{Type: domain.PeerTypeChannel, ID: 11}

	c.put(100, peer, 3)
	c.put(100, otherPeer, 1)

	clk.set(1005) // < 1000+10:仍有效
	if got, ok := c.get(100, peer); !ok || got != 3 {
		t.Fatalf("get before ttl = %d ok=%v, want 3/true", got, ok)
	}
	if got, ok := c.get(100, otherPeer); !ok || got != 1 {
		t.Fatalf("other before ttl = %d ok=%v, want 1/true", got, ok)
	}

	clk.set(1010) // == 1000+10:expiresAt<=now 视为过期
	if _, ok := c.get(100, peer); ok {
		t.Fatalf("expiresAt==now 应视为过期")
	}

	// delete 精确失效(在新鲜窗口内重写两个 peer,使其 expireAt=1020)。
	c.put(100, peer, 4)
	c.put(100, otherPeer, 1)
	c.delete(100, peer)
	clk.set(1011)
	if _, ok := c.get(100, peer); ok {
		t.Fatalf("delete 后不应命中")
	}
	if got, ok := c.get(100, otherPeer); !ok || got != 1 {
		t.Fatalf("delete exact 不应影响其它频道: %d ok=%v", got, ok)
	}

	c.deleteUser(100)
	if _, ok := c.get(100, otherPeer); ok {
		t.Fatalf("deleteUser 后同用户其它频道也应失效")
	}
}

func TestChannelBoostCachePeerTotalPutGetExpireDelete(t *testing.T) {
	clk := newBoostClock(1000)
	c := newChannelBoostCache(16, 10*time.Second, clk.now)
	peer := domain.Peer{Type: domain.PeerTypeChannel, ID: 10}
	otherPeer := domain.Peer{Type: domain.PeerTypeChannel, ID: 11}

	c.putPeerTotal(peer, 30)
	c.putPeerTotal(otherPeer, 11)

	clk.set(1005)
	if got, ok := c.getPeerTotal(peer); !ok || got != 30 {
		t.Fatalf("peer total before ttl = %d ok=%v, want 30/true", got, ok)
	}
	if got, ok := c.getPeerTotal(otherPeer); !ok || got != 11 {
		t.Fatalf("other peer total before ttl = %d ok=%v, want 11/true", got, ok)
	}

	clk.set(1010)
	if _, ok := c.getPeerTotal(peer); ok {
		t.Fatalf("peer total expiresAt==now 应视为过期")
	}

	c.putPeerTotal(peer, 31)
	c.putPeerTotal(otherPeer, 12)
	c.deletePeerTotal(peer)
	clk.set(1011)
	if _, ok := c.getPeerTotal(peer); ok {
		t.Fatalf("deletePeerTotal 后不应命中")
	}
	if got, ok := c.getPeerTotal(otherPeer); !ok || got != 12 {
		t.Fatalf("deletePeerTotal 不应影响其它频道: %d ok=%v", got, ok)
	}
}

func TestChannelBoostCacheCapFlush(t *testing.T) {
	c := NewChannelBoostCache(2, time.Minute)
	ch := func(id int64) domain.Peer { return domain.Peer{Type: domain.PeerTypeChannel, ID: id} }
	c.put(1, ch(1), 1)
	c.put(1, ch(2), 1)
	c.put(1, ch(3), 1) // 超 cap=2:LRU 单条驱逐最旧的 ch1,而非整表清空
	if _, ok := c.get(1, ch(1)); ok {
		t.Fatalf("超限后最旧条目(ch1)应被驱逐")
	}
	if _, ok := c.get(1, ch(2)); !ok {
		t.Fatalf("LRU 单条驱逐应保留次新条目(ch2),证明非整表 flush")
	}
	if got, ok := c.get(1, ch(3)); !ok || got != 1 {
		t.Fatalf("最新条目(ch3)应在: %d ok=%v", got, ok)
	}
	c.flush()
	if _, ok := c.get(1, ch(3)); ok {
		t.Fatalf("flush 后不应命中")
	}
}

func TestChannelBoostCacheSingleflightsColdLoad(t *testing.T) {
	ctx := context.Background()
	c := NewChannelBoostCache(16, time.Minute)
	peer := domain.Peer{Type: domain.PeerTypeChannel, ID: 42}
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	load := func() (int, error) {
		calls.Add(1)
		once.Do(func() { close(started) })
		<-release
		return 7, nil
	}

	const goroutines = 16
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			boosts, err := c.getOrLoad(ctx, 100, peer, load)
			if err != nil {
				errs <- err
				return
			}
			if boosts != 7 {
				errs <- fmt.Errorf("boosts = %d, want 7", boosts)
			}
		}()
	}
	<-started
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("load calls = %d, want 1", got)
	}
	if boosts, err := c.getOrLoad(ctx, 100, peer, load); err != nil || boosts != 7 {
		t.Fatalf("cached getOrLoad = %d/%v, want 7/nil", boosts, err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("cache hit called load again: calls=%d", got)
	}
}

func TestChannelBoostCachePeerTotalSingleflightsColdLoad(t *testing.T) {
	ctx := context.Background()
	c := NewChannelBoostCache(16, time.Minute)
	peer := domain.Peer{Type: domain.PeerTypeChannel, ID: 42}
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	load := func() (int, error) {
		calls.Add(1)
		once.Do(func() { close(started) })
		<-release
		return 70, nil
	}

	const goroutines = 16
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			boosts, err := c.getPeerTotalOrLoad(ctx, peer, load)
			if err != nil {
				errs <- err
				return
			}
			if boosts != 70 {
				errs <- fmt.Errorf("peer boosts = %d, want 70", boosts)
			}
		}()
	}
	<-started
	time.Sleep(20 * time.Millisecond)
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("peer total load calls = %d, want 1", got)
	}
	if boosts, err := c.getPeerTotalOrLoad(ctx, peer, load); err != nil || boosts != 70 {
		t.Fatalf("cached peer total = %d/%v, want 70/nil", boosts, err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("peer total cache hit called load again: calls=%d", got)
	}
}

func TestReadModelChangeListenerInvalidatesChannelBoostCache(t *testing.T) {
	boosts := NewChannelBoostCache(16, time.Minute)
	peer := domain.Peer{Type: domain.PeerTypeChannel, ID: 7}
	otherPeer := domain.Peer{Type: domain.PeerTypeChannel, ID: 8}
	boosts.put(100, peer, 2)
	boosts.put(100, otherPeer, 3)
	boosts.put(200, peer, 4)
	boosts.putPeerTotal(peer, 10)
	boosts.putPeerTotal(otherPeer, 11)

	listener := NewReadModelChangeListener("", ReadModelCacheSet{ChannelBoosts: boosts}, nil)
	listener.handlePayload(`{"model":"channel_self_boosts","owner_user_id":100,"peer_type":"channel","peer_id":7}`)

	if _, ok := boosts.get(100, peer); ok {
		t.Fatalf("channel_self_boosts 事件应失效精确 user/channel")
	}
	if _, ok := boosts.getPeerTotal(peer); ok {
		t.Fatalf("channel_self_boosts 事件应失效 channel total")
	}
	if got, ok := boosts.get(100, otherPeer); !ok || got != 3 {
		t.Fatalf("其它频道不应失效: %d ok=%v", got, ok)
	}
	if got, ok := boosts.get(200, peer); !ok || got != 4 {
		t.Fatalf("其它用户不应失效: %d ok=%v", got, ok)
	}
	if got, ok := boosts.getPeerTotal(otherPeer); !ok || got != 11 {
		t.Fatalf("其它频道 total 不应失效: %d ok=%v", got, ok)
	}
}

func TestChannelStoreInvalidateBoostCacheForUserAndPeers(t *testing.T) {
	boosts := NewChannelBoostCache(16, time.Minute)
	store := &ChannelStore{boostCache: boosts}
	oldPeer := domain.Peer{Type: domain.PeerTypeChannel, ID: 7}
	newPeer := domain.Peer{Type: domain.PeerTypeChannel, ID: 8}
	otherPeer := domain.Peer{Type: domain.PeerTypeChannel, ID: 9}

	boosts.put(100, oldPeer, 1)
	boosts.put(100, newPeer, 1)
	boosts.put(200, oldPeer, 1)
	boosts.putPeerTotal(oldPeer, 10)
	boosts.putPeerTotal(newPeer, 11)
	boosts.putPeerTotal(otherPeer, 12)

	store.invalidateBoostCacheForUserAndPeers(100, map[domain.Peer]struct{}{
		oldPeer: {},
		newPeer: {},
	})

	if _, ok := boosts.get(100, oldPeer); ok {
		t.Fatalf("当前用户 old peer self boost cache 应失效")
	}
	if _, ok := boosts.get(100, newPeer); ok {
		t.Fatalf("当前用户 new peer self boost cache 应失效")
	}
	if got, ok := boosts.get(200, oldPeer); !ok || got != 1 {
		t.Fatalf("其它用户 self boost cache 不应失效: %d ok=%v", got, ok)
	}
	if _, ok := boosts.getPeerTotal(oldPeer); ok {
		t.Fatalf("old peer total cache 应失效")
	}
	if _, ok := boosts.getPeerTotal(newPeer); ok {
		t.Fatalf("new peer total cache 应失效")
	}
	if got, ok := boosts.getPeerTotal(otherPeer); !ok || got != 12 {
		t.Fatalf("其它频道 total cache 不应失效: %d ok=%v", got, ok)
	}
}
