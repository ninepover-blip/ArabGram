package redisbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"telesrv/internal/edgecontrol"
)

const (
	defaultPrefix                = "edge:control"
	defaultSubscriberCommandTime = 2 * time.Second
	defaultSubscriberShards      = 32
	defaultSubscriberMailbox     = 256
	defaultSubscriberBytes       = int64(128 << 20)
	maxSessionControlWireBytes   = 16 << 20
)

var ErrNilClient = errors.New("edge redis bus: nil redis client")

type Bus struct {
	c      redis.UniversalClient
	prefix string

	admissionMu     sync.Mutex
	admissionMuxes  map[string]*admissionMux
	sessionAckMu    sync.Mutex
	sessionAckMuxes map[string]*sessionAckMux
}

type Option func(*Bus)

func WithPrefix(prefix string) Option {
	return func(b *Bus) {
		prefix = strings.TrimSpace(prefix)
		if prefix != "" {
			b.prefix = strings.TrimRight(prefix, ":")
		}
	}
}

func New(c redis.UniversalClient, opts ...Option) *Bus {
	b := &Bus{
		c: c, prefix: defaultPrefix,
		admissionMuxes:  make(map[string]*admissionMux),
		sessionAckMuxes: make(map[string]*sessionAckMux),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(b)
		}
	}
	return b
}

func (b *Bus) SendSessionControl(ctx context.Context, targetInstanceID string, cmd edgecontrol.SessionControlCommand) (edgecontrol.SessionControlAck, error) {
	if b == nil || b.c == nil {
		return edgecontrol.SessionControlAck{}, ErrNilClient
	}
	if strings.TrimSpace(targetInstanceID) == "" || strings.TrimSpace(cmd.CommandID) == "" || strings.TrimSpace(cmd.SourceInstanceID) == "" {
		return edgecontrol.SessionControlAck{}, fmt.Errorf("edge redis bus: source instance, target instance and command id are required")
	}
	cmd.TargetInstanceID = targetInstanceID
	mux, err := b.ensureSessionAckMux(ctx, cmd.SourceInstanceID)
	if err != nil {
		return edgecontrol.SessionControlAck{}, err
	}
	waiter, unregister, err := mux.register(cmd.CommandID)
	if err != nil {
		return edgecontrol.SessionControlAck{}, err
	}
	defer unregister()
	raw, err := json.Marshal(cmd)
	if err != nil {
		return edgecontrol.SessionControlAck{}, fmt.Errorf("edge redis bus encode session control command: %w", err)
	}
	if err := b.c.Publish(ctx, b.sessionControlCommandChannel(targetInstanceID), raw).Err(); err != nil {
		return edgecontrol.SessionControlAck{}, fmt.Errorf("edge redis bus publish session control command: %w", err)
	}
	select {
	case <-ctx.Done():
		return edgecontrol.SessionControlAck{}, ctx.Err()
	case got := <-waiter:
		if got.err != nil {
			return edgecontrol.SessionControlAck{}, got.err
		}
		ack := got.ack
		if ack.CommandID != cmd.CommandID || ack.SourceInstanceID != cmd.SourceInstanceID || ack.TargetInstanceID != targetInstanceID {
			return edgecontrol.SessionControlAck{}, fmt.Errorf("edge redis bus: session control ack identity mismatch")
		}
		return ack, nil
	}
}

func (b *Bus) SubscribeSessionControls(ctx context.Context, instanceID string, handle edgecontrol.SessionControlCommandHandler) error {
	if b == nil || b.c == nil {
		return ErrNilClient
	}
	if strings.TrimSpace(instanceID) == "" {
		return fmt.Errorf("edge redis bus: instance id is required")
	}
	if handle == nil {
		return fmt.Errorf("edge redis bus: session control handler is required")
	}
	pubsub := b.c.Subscribe(ctx, b.sessionControlCommandChannel(instanceID))
	defer func() { _ = pubsub.Close() }()
	if _, err := pubsub.Receive(ctx); err != nil {
		return fmt.Errorf("edge redis bus subscribe session control commands: %w", err)
	}
	executor := newSessionControlExecutor(ctx, b, handle)
	defer executor.close()
	messages := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-messages:
			if !ok {
				return fmt.Errorf("edge redis bus session control command subscription closed")
			}
			if len(msg.Payload) == 0 || len(msg.Payload) > maxSessionControlWireBytes {
				continue
			}
			var cmd edgecontrol.SessionControlCommand
			if err := json.Unmarshal([]byte(msg.Payload), &cmd); err != nil {
				continue
			}
			if err := executor.submit(cmd, len(msg.Payload)); err != nil {
				b.publishSessionControlAck(ctx, cmd, edgecontrol.SessionControlAck{Error: err.Error()})
			}
		}
	}
}

func (b *Bus) publishSessionControlAck(ctx context.Context, cmd edgecontrol.SessionControlCommand, ack edgecontrol.SessionControlAck) {
	if ack.CommandID == "" {
		ack.CommandID = cmd.CommandID
	}
	if ack.SourceInstanceID == "" {
		ack.SourceInstanceID = cmd.SourceInstanceID
	}
	if ack.TargetInstanceID == "" {
		ack.TargetInstanceID = cmd.TargetInstanceID
	}
	raw, err := json.Marshal(ack)
	if err == nil {
		_ = b.c.Publish(ctx, b.sessionControlAckChannel(cmd.SourceInstanceID), raw).Err()
	}
}

func (b *Bus) deliveryCommandChannel(instanceID string) string {
	return fmt.Sprintf("%s:delivery:v3:command:%s", b.prefix, instanceID)
}

func (b *Bus) deliveryAdmissionChannel(sourceInstanceID string) string {
	return fmt.Sprintf("%s:delivery:v3:admission:%s", b.prefix, sourceInstanceID)
}

func (b *Bus) sessionControlCommandChannel(instanceID string) string {
	return fmt.Sprintf("%s:session:control:%s", b.prefix, instanceID)
}

func (b *Bus) sessionControlAckChannel(sourceInstanceID string) string {
	return fmt.Sprintf("%s:session:ack:%s", b.prefix, sourceInstanceID)
}
