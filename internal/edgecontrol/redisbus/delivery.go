package redisbus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"telesrv/internal/edgecontrol"
)

func (b *Bus) SendDeliveryBatch(ctx context.Context, target string, batch edgecontrol.DeliveryBatch) (edgecontrol.DeliveryAdmission, error) {
	if b == nil || b.c == nil {
		return edgecontrol.DeliveryAdmission{}, ErrNilClient
	}
	if target == "" {
		return edgecontrol.DeliveryAdmission{}, fmt.Errorf("edge redis bus: target instance is required")
	}
	batch.TargetInstanceID = target
	if err := edgecontrol.ValidateDeliveryBatchEnvelope(batch); err != nil {
		return edgecontrol.DeliveryAdmission{}, err
	}
	if !time.Now().Before(batch.EvidenceDeadline) {
		return edgecontrol.DeliveryAdmission{}, fmt.Errorf("edge redis bus: delivery evidence deadline elapsed")
	}
	mux, err := b.ensureAdmissionMux(ctx, batch.SourceInstanceID)
	if err != nil {
		return edgecontrol.DeliveryAdmission{}, err
	}
	waiter, unregister, err := mux.register(batch.CommandID)
	if err != nil {
		return edgecontrol.DeliveryAdmission{}, err
	}
	defer unregister()
	raw, err := encodeDeliveryBatchWire(batch)
	if err != nil {
		return edgecontrol.DeliveryAdmission{}, err
	}
	receivers, err := b.c.Publish(ctx, b.deliveryCommandChannel(target), raw).Result()
	if err != nil {
		return edgecontrol.DeliveryAdmission{}, fmt.Errorf("edge redis bus publish delivery command: %w", err)
	}
	if receivers == 0 {
		return edgecontrol.DeliveryAdmission{}, edgecontrol.ErrDeliveryTargetUnavailable
	}
	select {
	case <-ctx.Done():
		return edgecontrol.DeliveryAdmission{}, ctx.Err()
	case got := <-waiter:
		if got.err != nil {
			return edgecontrol.DeliveryAdmission{}, got.err
		}
		if got.admission.BatchID != batch.BatchID || got.admission.CommandID != batch.CommandID ||
			got.admission.SourceInstanceID != batch.SourceInstanceID || got.admission.TargetInstanceID != target {
			return edgecontrol.DeliveryAdmission{}, fmt.Errorf("edge redis bus: admission identity mismatch")
		}
		return got.admission, nil
	}
}

func (b *Bus) SubscribeDeliveryBatches(ctx context.Context, instanceID string, handle edgecontrol.DeliveryBatchHandler) error {
	if b == nil || b.c == nil {
		return ErrNilClient
	}
	if instanceID == "" || handle == nil {
		return fmt.Errorf("edge redis bus: instance and delivery handler are required")
	}
	pubsub := b.c.Subscribe(ctx, b.deliveryCommandChannel(instanceID))
	defer func() { _ = pubsub.Close() }()
	if _, err := pubsub.Receive(ctx); err != nil {
		return fmt.Errorf("edge redis bus subscribe delivery commands: %w", err)
	}
	executor := newDeliverySubscriberExecutor(ctx, b, handle)
	defer executor.close()
	messages := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-messages:
			if !ok {
				return fmt.Errorf("edge redis bus: delivery command subscription closed")
			}
			batch, err := decodeDeliveryBatchWire(msg.Payload)
			if err != nil || batch.TargetInstanceID != instanceID {
				continue
			}
			if !time.Now().Before(batch.EvidenceDeadline) {
				b.publishAdmission(ctx, batch, edgecontrol.DeliveryAdmission{Outcome: edgecontrol.AdmissionRejected, Detail: edgecontrol.DetailDeadline})
				continue
			}
			if err := executor.submit(batch, len(msg.Payload)); err != nil {
				admission := edgecontrol.DeliveryAdmission{Outcome: edgecontrol.AdmissionOverloaded, Detail: edgecontrol.DetailCapacity}
				if errors.Is(err, context.DeadlineExceeded) {
					admission = edgecontrol.DeliveryAdmission{Outcome: edgecontrol.AdmissionRejected, Detail: edgecontrol.DetailDeadline}
				}
				b.publishAdmission(ctx, batch, admission)
			}
		}
	}
}

func (b *Bus) publishAdmission(ctx context.Context, batch edgecontrol.DeliveryBatch, admission edgecontrol.DeliveryAdmission) {
	admission.BatchID = batch.BatchID
	admission.CommandID = batch.CommandID
	admission.SourceInstanceID = batch.SourceInstanceID
	admission.TargetInstanceID = batch.TargetInstanceID
	raw, err := encodeDeliveryAdmissionWire(admission)
	if err != nil {
		return
	}
	_ = b.c.Publish(ctx, b.deliveryAdmissionChannel(batch.SourceInstanceID), raw).Err()
}

type deliverySubscriberTask struct {
	batch edgecontrol.DeliveryBatch
	bytes int
}
type deliverySubscriberExecutor struct {
	ctx      context.Context
	bus      *Bus
	handle   edgecontrol.DeliveryBatchHandler
	queues   []chan deliverySubscriberTask
	maxBytes int64
	used     atomic.Int64
	stop     chan struct{}
	done     sync.WaitGroup
	once     sync.Once
}

func newDeliverySubscriberExecutor(ctx context.Context, bus *Bus, handle edgecontrol.DeliveryBatchHandler) *deliverySubscriberExecutor {
	e := &deliverySubscriberExecutor{ctx: ctx, bus: bus, handle: handle, maxBytes: defaultSubscriberBytes, stop: make(chan struct{}), queues: make([]chan deliverySubscriberTask, defaultSubscriberShards)}
	for i := range e.queues {
		e.queues[i] = make(chan deliverySubscriberTask, defaultSubscriberMailbox)
		e.done.Add(1)
		go e.run(e.queues[i])
	}
	return e
}
func (e *deliverySubscriberExecutor) submit(batch edgecontrol.DeliveryBatch, bytes int) error {
	if !time.Now().Before(batch.EvidenceDeadline) {
		return context.DeadlineExceeded
	}
	if !reserveAtomicBytes(&e.used, e.maxBytes, bytes) {
		return edgecontrol.ErrDeliveryOverloaded
	}
	q := e.queues[edgecontrol.OrderingDomainHash(batch.Items[0].Ref.Domain)&uint64(len(e.queues)-1)]
	select {
	case q <- deliverySubscriberTask{batch: batch, bytes: bytes}:
		return nil
	case <-e.stop:
		e.used.Add(-int64(bytes))
		return edgecontrol.ErrDeliveryIndeterminate
	default:
		e.used.Add(-int64(bytes))
		return edgecontrol.ErrDeliveryOverloaded
	}
}
func (e *deliverySubscriberExecutor) run(q <-chan deliverySubscriberTask) {
	defer e.done.Done()
	for {
		select {
		case <-e.stop:
			return
		case task := <-q:
			now := time.Now()
			deadline := now.Add(defaultSubscriberCommandTime)
			capDeadline := task.batch.NotAfter
			if !now.Before(task.batch.NotAfter) {
				capDeadline = task.batch.EvidenceDeadline
			}
			if capDeadline.Before(deadline) {
				deadline = capDeadline
			}
			if !time.Now().Before(deadline) {
				e.used.Add(-int64(task.bytes))
				e.bus.publishAdmission(e.ctx, task.batch, edgecontrol.DeliveryAdmission{Outcome: edgecontrol.AdmissionRejected, Detail: edgecontrol.DetailDeadline})
				continue
			}
			ctx, cancel := context.WithDeadline(e.ctx, deadline)
			admission := e.handle(ctx, task.batch)
			cancel()
			e.used.Add(-int64(task.bytes))
			e.bus.publishAdmission(e.ctx, task.batch, admission)
		}
	}
}
func (e *deliverySubscriberExecutor) close() { e.once.Do(func() { close(e.stop) }); e.done.Wait() }
