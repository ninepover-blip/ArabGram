package rpc

import (
	"context"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// Bot API 私聊 enqueue 异步化（性能审计 H2）：user→bot 私聊发送/编辑时，把
// bot_api_updates 的 INSERT（以及可能打 PG 的 bot 判定）移出发送者 RPC 同步路径，
// 发送者不再为 Bot API 队列写入多等一次 PG 往返。
//
// bot_api_updates 行本身就是投递真值
// （getUpdates 只读该表，没有 getDifference 类收敛面），因此队列满时在调用方路径完成
// durable insert，而不是丢弃——发送者多等一次 INSERT，换 update 不丢。
//
// 单 worker FIFO：保证同一 bot 的 update_id 顺序与发送顺序一致（并发 goroutine 池
// 会让 bot 侧 getUpdates 看到乱序消息）。

const (
	defaultBotAPIEnqueueBuffer    = 4096
	botAPIEnqueueJobTimeout       = 10 * time.Second
	botAPIEnqueueCallerPathReason = "bot api enqueue queue full, executing durable insert on caller path"
)

type botAPIEnqueueDispatcher struct {
	log     *zap.Logger
	jobs    chan func(context.Context)
	started atomic.Bool
}

func newBotAPIEnqueueDispatcher(log *zap.Logger, buffer int) *botAPIEnqueueDispatcher {
	if buffer <= 0 {
		buffer = defaultBotAPIEnqueueBuffer
	}
	return &botAPIEnqueueDispatcher{
		log:  log.Named("botapi-enqueue"),
		jobs: make(chan func(context.Context), buffer),
	}
}

// RunBotAPIEnqueue 启动 Bot API enqueue 后台 worker，由 main 与其它 dispatcher 一同 go 起。
// 阻塞到 ctx 取消；未调用前 enqueue 在调用方路径完成 durable insert。
func (r *Router) RunBotAPIEnqueue(ctx context.Context) {
	r.botAPIEnqueueQueue.Run(ctx)
}

func (d *botAPIEnqueueDispatcher) Run(ctx context.Context) {
	if d == nil || !d.started.CompareAndSwap(false, true) {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case job := <-d.jobs:
			jobCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), botAPIEnqueueJobTimeout)
			job(jobCtx)
			cancel()
		}
	}
}

// Enqueue 投递一个 Bot API 队列写入任务。dispatcher 未启动时在调用方路径执行；
// 已启动时投入 FIFO，满则回到调用方路径完成 durable insert——绝不丢弃（队列行是投递真值）。
func (d *botAPIEnqueueDispatcher) Enqueue(reqCtx context.Context, job func(context.Context)) {
	if d == nil || job == nil {
		return
	}
	if !d.started.Load() {
		job(reqCtx)
		return
	}
	select {
	case d.jobs <- job:
	default:
		d.log.Warn(botAPIEnqueueCallerPathReason)
		job(reqCtx)
	}
}
