package rpc

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"
)

// TestBotAPIEnqueueDispatcherCallerPathBeforeRun 锁定未启动 worker 时的 caller-path
// durable insert：bot_api_updates 是投递真值，不能因为后台 worker 未开始而丢行。
func TestBotAPIEnqueueDispatcherCallerPathBeforeRun(t *testing.T) {
	d := newBotAPIEnqueueDispatcher(zaptest.NewLogger(t), 4)
	ran := false
	d.Enqueue(context.Background(), func(context.Context) { ran = true })
	if !ran {
		t.Fatal("job must run on caller path before Run is called")
	}
}

// TestBotAPIEnqueueDispatcherFIFOOrder 锁定启动后单 worker FIFO：同一 bot 的
// update_id 顺序必须与 enqueue 顺序一致（乱序会让 bot 侧 getUpdates 看到错序消息）。
func TestBotAPIEnqueueDispatcherFIFOOrder(t *testing.T) {
	d := newBotAPIEnqueueDispatcher(zaptest.NewLogger(t), 16)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.Run(ctx)
	for !d.started.Load() {
		time.Sleep(time.Millisecond)
	}

	var mu sync.Mutex
	var order []int
	done := make(chan struct{})
	for i := 0; i < 5; i++ {
		i := i
		d.Enqueue(context.Background(), func(context.Context) {
			mu.Lock()
			order = append(order, i)
			if len(order) == 5 {
				close(done)
			}
			mu.Unlock()
		})
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("jobs did not complete")
	}
	mu.Lock()
	defer mu.Unlock()
	for i, got := range order {
		if got != i {
			t.Fatalf("order = %v, want FIFO", order)
		}
	}
}

// TestBotAPIEnqueueDispatcherUsesCallerPathWhenFull 锁定队列满时的 caller-path durable
// insert：Bot API 队列行是投递真值，满时发送者多等一次 INSERT，绝不丢。
func TestBotAPIEnqueueDispatcherUsesCallerPathWhenFull(t *testing.T) {
	d := newBotAPIEnqueueDispatcher(zaptest.NewLogger(t), 1)
	d.started.Store(true) // 模拟已启动但 worker 不消费（阻塞场景）

	// 塞满容量 1 的队列。
	d.Enqueue(context.Background(), func(context.Context) {})

	ran := false
	d.Enqueue(context.Background(), func(context.Context) { ran = true })
	if !ran {
		t.Fatal("job must execute on caller path when queue is full")
	}
}
