package redisbus

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"

	"telesrv/internal/edgecontrol"
)

const maxSessionAckMuxPending = 8192

type sessionAckResult struct {
	ack edgecontrol.SessionControlAck
	err error
}

// sessionAckMux owns one Redis subscription for every concurrent control
// command emitted by a source service instance. Command IDs demultiplex ACKs
// in bounded process memory; Redis no longer opens one TCP Pub/Sub connection
// per request.
type sessionAckMux struct {
	bus      *Bus
	sourceID string
	ctx      context.Context
	cancel   context.CancelFunc
	pubsub   *redis.PubSub

	mu      sync.Mutex
	pending map[string]chan sessionAckResult
	closed  bool
}

func (b *Bus) ensureSessionAckMux(ctx context.Context, sourceID string) (*sessionAckMux, error) {
	sourceID = strings.TrimSpace(sourceID)
	if sourceID == "" {
		return nil, fmt.Errorf("edge redis bus: session control source instance id is required")
	}
	b.sessionAckMu.Lock()
	defer b.sessionAckMu.Unlock()
	if mux := b.sessionAckMuxes[sourceID]; mux != nil {
		return mux, nil
	}
	muxCtx, cancel := context.WithCancel(context.Background())
	pubsub := b.c.Subscribe(muxCtx, b.sessionControlAckChannel(sourceID))
	if _, err := pubsub.Receive(ctx); err != nil {
		cancel()
		_ = pubsub.Close()
		return nil, fmt.Errorf("edge redis bus subscribe shared session control ack: %w", err)
	}
	mux := &sessionAckMux{
		bus: b, sourceID: sourceID, ctx: muxCtx, cancel: cancel, pubsub: pubsub,
		pending: make(map[string]chan sessionAckResult),
	}
	b.sessionAckMuxes[sourceID] = mux
	go mux.run()
	return mux, nil
}

func (m *sessionAckMux) register(commandID string) (<-chan sessionAckResult, func(), error) {
	commandID = strings.TrimSpace(commandID)
	if commandID == "" {
		return nil, nil, fmt.Errorf("edge redis bus: session control command id is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, nil, fmt.Errorf("edge redis bus: session control ack mux is closed")
	}
	if _, ok := m.pending[commandID]; ok {
		return nil, nil, fmt.Errorf("edge redis bus: duplicate pending session control command")
	}
	if len(m.pending) >= maxSessionAckMuxPending {
		return nil, nil, fmt.Errorf("edge redis bus: session control ack capacity exhausted")
	}
	waiter := make(chan sessionAckResult, 1)
	m.pending[commandID] = waiter
	return waiter, func() {
		m.mu.Lock()
		if m.pending[commandID] == waiter {
			delete(m.pending, commandID)
		}
		m.mu.Unlock()
	}, nil
}

func (m *sessionAckMux) run() {
	messages := m.pubsub.Channel()
	for {
		select {
		case <-m.ctx.Done():
			m.close(m.ctx.Err())
			return
		case msg, ok := <-messages:
			if !ok {
				m.close(fmt.Errorf("edge redis bus: shared session control ack subscription closed"))
				return
			}
			if len(msg.Payload) == 0 || len(msg.Payload) > maxSessionControlWireBytes {
				continue
			}
			var ack edgecontrol.SessionControlAck
			if err := json.Unmarshal([]byte(msg.Payload), &ack); err != nil || strings.TrimSpace(ack.CommandID) == "" {
				continue
			}
			m.mu.Lock()
			waiter := m.pending[ack.CommandID]
			m.mu.Unlock()
			if waiter != nil {
				select {
				case waiter <- sessionAckResult{ack: ack}:
				default:
				}
			}
		}
	}
}

func (m *sessionAckMux) close(err error) {
	if err == nil {
		err = fmt.Errorf("edge redis bus: session control ack mux stopped")
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	for _, waiter := range m.pending {
		select {
		case waiter <- sessionAckResult{err: err}:
		default:
		}
	}
	m.pending = nil
	m.mu.Unlock()
	m.cancel()
	_ = m.pubsub.Close()
	m.bus.sessionAckMu.Lock()
	if m.bus.sessionAckMuxes[m.sourceID] == m {
		delete(m.bus.sessionAckMuxes, m.sourceID)
	}
	m.bus.sessionAckMu.Unlock()
}
