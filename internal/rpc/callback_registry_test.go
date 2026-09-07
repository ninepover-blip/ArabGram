package rpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/store/storetest"
)

func TestCallbackRegistryResolveDelivers(t *testing.T) {
	reg := newCallbackRegistry(storetest.NewBotCallbackRegistryStore())
	queryID, p := reg.register(100, 200)
	if queryID == 0 {
		t.Fatal("query id must be non-zero")
	}
	want := domain.BotCallbackAnswer{Alert: true, Message: "hi"}
	if !reg.resolve(100, queryID, want) {
		t.Fatal("resolve by owner bot must succeed")
	}
	select {
	case got := <-p.ch:
		if got != want {
			t.Fatalf("delivered %+v, want %+v", got, want)
		}
	default:
		t.Fatal("answer not delivered to channel")
	}
	// 解挂后条目已删除：再次 resolve 失败、registry 计数归零。
	if reg.resolve(100, queryID, want) {
		t.Fatal("second resolve must fail (already deregistered)")
	}
	if n := reg.size(); n != 0 {
		t.Fatalf("registry size = %d, want 0 after resolve", n)
	}
}

func TestCallbackRegistryNonOwnerRejected(t *testing.T) {
	reg := newCallbackRegistry(storetest.NewBotCallbackRegistryStore())
	queryID, p := reg.register(100, 200)
	// 非属主 bot（999）不得解挂（I6）。
	if reg.resolve(999, queryID, domain.BotCallbackAnswer{Message: "evil"}) {
		t.Fatal("non-owner resolve must be rejected")
	}
	select {
	case <-p.ch:
		t.Fatal("non-owner answer must not be delivered")
	default:
	}
	if n := reg.size(); n != 1 {
		t.Fatalf("registry size = %d, want 1 (entry retained after rejected resolve)", n)
	}
	reg.deregister(queryID)
	if n := reg.size(); n != 0 {
		t.Fatalf("registry size = %d, want 0 after deregister", n)
	}
}

func TestCallbackRegistryUnknownQuery(t *testing.T) {
	reg := newCallbackRegistry(storetest.NewBotCallbackRegistryStore())
	if reg.resolve(100, 12345, domain.BotCallbackAnswer{}) {
		t.Fatal("resolve of unregistered query must fail")
	}
}

func TestCallbackRegistryUniqueQueryIDs(t *testing.T) {
	reg := newCallbackRegistry(storetest.NewBotCallbackRegistryStore())
	seen := make(map[int64]struct{})
	for i := 0; i < 1000; i++ {
		q, _ := reg.register(1, 2)
		if _, dup := seen[q]; dup {
			t.Fatalf("duplicate query id %d", q)
		}
		seen[q] = struct{}{}
	}
	if n := reg.size(); n != 1000 {
		t.Fatalf("registry size = %d, want 1000", n)
	}
}

func TestCallbackRegistryRequiresSharedStore(t *testing.T) {
	reg := newCallbackRegistry()
	if queryID, pending, err := reg.registerContext(context.Background(), time.Now(), 100, 200, time.Second); queryID != 0 || pending != nil || !errors.Is(err, errBotCallbackRegistryStoreRequired) {
		t.Fatalf("register without shared store = query %d pending %v err %v, want required", queryID, pending, err)
	}
	if ok, err := reg.resolveContext(context.Background(), 100, 12345, domain.BotCallbackAnswer{}); ok || !errors.Is(err, errBotCallbackRegistryStoreRequired) {
		t.Fatalf("resolve without shared store = ok %v err %v, want required", ok, err)
	}
}
