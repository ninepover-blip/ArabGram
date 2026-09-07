package redisbus

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"

	"telesrv/internal/edgecontrol"
)

type sessionControlTask struct {
	cmd   edgecontrol.SessionControlCommand
	bytes int
}

type sessionControlExecutor struct {
	ctx      context.Context
	bus      *Bus
	handle   edgecontrol.SessionControlCommandHandler
	queues   []chan sessionControlTask
	maxBytes int64
	used     atomic.Int64
	stop     chan struct{}
	done     sync.WaitGroup
	once     sync.Once
}

func newSessionControlExecutor(ctx context.Context, bus *Bus, handle edgecontrol.SessionControlCommandHandler) *sessionControlExecutor {
	e := &sessionControlExecutor{
		ctx: ctx, bus: bus, handle: handle, maxBytes: defaultSubscriberBytes, stop: make(chan struct{}),
		queues: make([]chan sessionControlTask, defaultSubscriberShards),
	}
	for i := range e.queues {
		e.queues[i] = make(chan sessionControlTask, defaultSubscriberMailbox)
		e.done.Add(1)
		go e.run(e.queues[i])
	}
	return e
}

func (e *sessionControlExecutor) submit(cmd edgecontrol.SessionControlCommand, bytes int) error {
	if !reserveAtomicBytes(&e.used, e.maxBytes, bytes) {
		return edgecontrol.ErrDeliveryOverloaded
	}
	queue := e.queues[sessionControlHash(cmd)&uint64(len(e.queues)-1)]
	select {
	case queue <- sessionControlTask{cmd: cmd, bytes: bytes}:
		return nil
	case <-e.stop:
		e.used.Add(-int64(bytes))
		return edgecontrol.ErrDeliveryIndeterminate
	default:
		e.used.Add(-int64(bytes))
		return edgecontrol.ErrDeliveryOverloaded
	}
}

func (e *sessionControlExecutor) run(queue <-chan sessionControlTask) {
	defer e.done.Done()
	for {
		select {
		case <-e.stop:
			return
		case task := <-queue:
			timeout := task.cmd.DeliveryTimeout
			if timeout <= 0 || timeout > defaultSubscriberCommandTime {
				timeout = defaultSubscriberCommandTime
			}
			handleCtx, cancel := context.WithTimeout(e.ctx, timeout)
			ack := e.handle(handleCtx, task.cmd)
			cancel()
			e.used.Add(-int64(task.bytes))
			e.bus.publishSessionControlAck(e.ctx, task.cmd, ack)
		}
	}
}

func (e *sessionControlExecutor) close() {
	e.once.Do(func() { close(e.stop) })
	e.done.Wait()
}

func sessionControlHash(cmd edgecontrol.SessionControlCommand) uint64 {
	if cmd.TargetUserID != 0 {
		return uint64(cmd.TargetUserID)
	}
	if cmd.UserID != 0 {
		return uint64(cmd.UserID)
	}
	if cmd.SessionID != 0 {
		return uint64(cmd.SessionID)
	}
	return binary.LittleEndian.Uint64(cmd.RawAuthKeyID[:]) ^ binary.LittleEndian.Uint64(cmd.AuthKeyID[:])
}

func reserveAtomicBytes(used *atomic.Int64, max int64, bytes int) bool {
	if used == nil || bytes <= 0 || int64(bytes) > max {
		return false
	}
	for {
		current := used.Load()
		if current > max-int64(bytes) {
			return false
		}
		if used.CompareAndSwap(current, current+int64(bytes)) {
			return true
		}
	}
}
