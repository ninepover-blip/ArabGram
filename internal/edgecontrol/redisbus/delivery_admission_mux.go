package redisbus

import (
	"context"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"

	"telesrv/internal/edgecontrol"
)

type admissionResult struct {
	admission edgecontrol.DeliveryAdmission
	err       error
}

const maxAdmissionMuxPending = 8192

// admissionMux owns the single Redis subscription for one source Edge. Redis
// is strictly the command-admission plane; physical receipts never enter it.
type admissionMux struct {
	bus      *Bus
	sourceID string
	ctx      context.Context
	cancel   context.CancelFunc
	pubsub   *redis.PubSub

	mu      sync.Mutex
	pending map[edgecontrol.CommandID]chan admissionResult
	closed  bool
}

func (b *Bus) ensureAdmissionMux(ctx context.Context, sourceID string) (*admissionMux, error) {
	if sourceID == "" {
		return nil, fmt.Errorf("edge redis bus: source instance id is required")
	}
	b.admissionMu.Lock()
	defer b.admissionMu.Unlock()
	if mux := b.admissionMuxes[sourceID]; mux != nil {
		return mux, nil
	}
	muxCtx, cancel := context.WithCancel(context.Background())
	pubsub := b.c.Subscribe(muxCtx, b.deliveryAdmissionChannel(sourceID))
	if _, err := pubsub.Receive(ctx); err != nil {
		cancel()
		_ = pubsub.Close()
		return nil, fmt.Errorf("edge redis bus subscribe admission mux: %w", err)
	}
	mux := &admissionMux{
		bus: b, sourceID: sourceID, ctx: muxCtx, cancel: cancel, pubsub: pubsub,
		pending: make(map[edgecontrol.CommandID]chan admissionResult),
	}
	b.admissionMuxes[sourceID] = mux
	go mux.run()
	return mux, nil
}

func (m *admissionMux) register(commandID edgecontrol.CommandID) (<-chan admissionResult, func(), error) {
	if commandID.Empty() {
		return nil, nil, fmt.Errorf("edge redis bus: command id is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, nil, fmt.Errorf("edge redis bus: admission mux is closed")
	}
	if _, ok := m.pending[commandID]; ok {
		return nil, nil, fmt.Errorf("edge redis bus: duplicate pending delivery command")
	}
	if len(m.pending) >= maxAdmissionMuxPending {
		return nil, nil, edgecontrol.ErrDeliveryOverloaded
	}
	ch := make(chan admissionResult, 1)
	m.pending[commandID] = ch
	return ch, func() {
		m.mu.Lock()
		if m.pending[commandID] == ch {
			delete(m.pending, commandID)
		}
		m.mu.Unlock()
	}, nil
}

func (m *admissionMux) run() {
	messages := m.pubsub.Channel()
	for {
		select {
		case <-m.ctx.Done():
			m.close(m.ctx.Err())
			return
		case msg, ok := <-messages:
			if !ok {
				m.close(fmt.Errorf("edge redis bus: admission subscription closed"))
				return
			}
			admission, err := decodeDeliveryAdmissionWire(msg.Payload)
			if err != nil || admission.CommandID.Empty() {
				continue
			}
			m.mu.Lock()
			waiter := m.pending[admission.CommandID]
			m.mu.Unlock()
			if waiter != nil {
				select {
				case waiter <- admissionResult{admission: admission}:
				default:
				}
			}
		}
	}
}

func (m *admissionMux) close(err error) {
	if err == nil {
		err = fmt.Errorf("edge redis bus: admission mux stopped")
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	for _, waiter := range m.pending {
		select {
		case waiter <- admissionResult{err: err}:
		default:
		}
	}
	m.pending = nil
	m.mu.Unlock()
	m.cancel()
	_ = m.pubsub.Close()
	m.bus.admissionMu.Lock()
	if m.bus.admissionMuxes[m.sourceID] == m {
		delete(m.bus.admissionMuxes, m.sourceID)
	}
	m.bus.admissionMu.Unlock()
}
