package edge

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/edgecontrol"
	"telesrv/internal/mtprotoedge"
)

const (
	clientAckReporterShards = 16
	clientAckShardCapacity  = 512
	clientAckBatchSize      = 128
	clientAckFlushInterval  = 2 * time.Millisecond
	clientAckRetryInterval  = 10 * time.Millisecond
)

// deliveryClientAckReporter is a fixed actor set. Conn outbound actors never
// wait for this late-observation plane: a saturated shard drops the observation
// explicitly while physical receipts remain the completion authority.
type deliveryClientAckReporter struct {
	remote           edgecontrol.ClientAckObservationReporter
	targetInstanceID string
	timeout          time.Duration
	log              *zap.Logger
	shards           []chan edgecontrol.ClientAckObservation
	accepting        atomic.Bool
	dropped          atomic.Uint64
	stop             chan struct{}
	started          chan struct{}
	done             sync.WaitGroup
	runOnce          sync.Once
	once             sync.Once
}

func newDeliveryClientAckReporter(remote edgecontrol.ClientAckObservationReporter, targetInstanceID string, timeout time.Duration, log *zap.Logger) *deliveryClientAckReporter {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	if log == nil {
		log = zap.NewNop()
	}
	r := &deliveryClientAckReporter{
		remote: remote, targetInstanceID: targetInstanceID, timeout: timeout, log: log,
		stop: make(chan struct{}), started: make(chan struct{}), shards: make([]chan edgecontrol.ClientAckObservation, clientAckReporterShards),
	}
	for i := range r.shards {
		r.shards[i] = make(chan edgecontrol.ClientAckObservation, clientAckShardCapacity)
	}
	r.accepting.Store(true)
	return r
}

func (r *deliveryClientAckReporter) TrySubmit(ack mtprotoedge.DeliveryClientAck) bool {
	if r == nil || r.remote == nil || !r.accepting.Load() {
		return false
	}
	observation := edgecontrol.ClientAckObservation{
		Tracking: ack.Tracking, TargetInstanceID: r.targetInstanceID, AuthKeyID: ack.AuthKeyID,
		SessionID: ack.SessionID, ServerMsgID: ack.ServerMsgID, ObservedAt: ack.AckedAt,
	}
	if !observation.Valid() {
		return false
	}
	queue := r.shards[edgecontrol.OrderingDomainHash(observation.Tracking.Ref.Domain)&uint64(len(r.shards)-1)]
	select {
	case queue <- observation:
		return true
	default:
		return false
	}
}

func (r *deliveryClientAckReporter) Observe(ack mtprotoedge.DeliveryClientAck) {
	if r != nil && !r.TrySubmit(ack) {
		r.dropped.Add(1)
	}
}

func (r *deliveryClientAckReporter) Run(ctx context.Context) {
	if r == nil || r.remote == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.runOnce.Do(func() {
		for _, queue := range r.shards {
			r.done.Add(1)
			go r.runShard(ctx, queue)
		}
		close(r.started)
	})
	select {
	case <-ctx.Done():
	case <-r.stop:
	}
	r.Close()
	r.done.Wait()
}

func (r *deliveryClientAckReporter) Close() {
	if r != nil {
		r.once.Do(func() {
			r.accepting.Store(false)
			close(r.stop)
			r.runOnce.Do(func() { close(r.started) })
		})
	}
}

func (r *deliveryClientAckReporter) Wait() {
	if r != nil {
		<-r.started
		r.done.Wait()
	}
}

func (r *deliveryClientAckReporter) runShard(ctx context.Context, queue <-chan edgecontrol.ClientAckObservation) {
	defer r.done.Done()
	batch := make([]edgecontrol.ClientAckObservation, 0, clientAckBatchSize)
	for {
		select {
		case <-ctx.Done():
			r.shutdownDrain(queue, batch)
			return
		case <-r.stop:
			r.shutdownDrain(queue, batch)
			return
		case first := <-queue:
			batch = append(batch[:0], first)
			timer := time.NewTimer(clientAckFlushInterval)
		collect:
			for len(batch) < clientAckBatchSize {
				select {
				case item := <-queue:
					batch = append(batch, item)
				case <-timer.C:
					break collect
				case <-ctx.Done():
					stopClientAckTimer(timer)
					r.shutdownDrain(queue, batch)
					return
				case <-r.stop:
					stopClientAckTimer(timer)
					r.shutdownDrain(queue, batch)
					return
				}
			}
			stopClientAckTimer(timer)
			batch = r.reportUntilTerminal(ctx, batch)
		}
	}
}

func (r *deliveryClientAckReporter) reportUntilTerminal(ctx context.Context, pending []edgecontrol.ClientAckObservation) []edgecontrol.ClientAckObservation {
	for len(pending) > 0 {
		writeCtx, cancel := context.WithTimeout(ctx, r.timeout)
		results, err := r.remote.ReportClientAcks(writeCtx, pending)
		cancel()
		if err == nil && len(results) == len(pending) {
			retry := pending[:0]
			for i, result := range results {
				switch result.Outcome {
				case edgecontrol.ClientAckObservationApplied, edgecontrol.ClientAckObservationStale, edgecontrol.ClientAckObservationRejected:
				case edgecontrol.ClientAckObservationRetryable:
					retry = append(retry, pending[i])
				default:
					retry = append(retry, pending[i])
				}
			}
			pending = retry
			if len(pending) == 0 {
				return nil
			}
		}
		timer := time.NewTimer(clientAckRetryInterval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			stopClientAckTimer(timer)
			return pending
		case <-r.stop:
			stopClientAckTimer(timer)
			return pending
		}
	}
	return nil
}

func (r *deliveryClientAckReporter) shutdownDrain(queue <-chan edgecontrol.ClientAckObservation, initial []edgecontrol.ClientAckObservation) {
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()
	pending := append([]edgecontrol.ClientAckObservation(nil), initial...)
	for {
		for len(pending) < clientAckBatchSize {
			select {
			case observation := <-queue:
				pending = append(pending, observation)
			default:
				goto report
			}
		}
	report:
		if len(pending) == 0 {
			return
		}
		pending = r.reportShutdownUntilTerminal(ctx, pending)
		if len(pending) != 0 {
			r.dropped.Add(uint64(len(pending)))
			r.log.Warn("client ACK reporter shutdown left unresolved observations", zap.Int("count", len(pending)))
			return
		}
		pending = pending[:0]
	}
}

func (r *deliveryClientAckReporter) reportShutdownUntilTerminal(ctx context.Context, pending []edgecontrol.ClientAckObservation) []edgecontrol.ClientAckObservation {
	for len(pending) > 0 {
		writeCtx, cancel := context.WithTimeout(ctx, r.timeout)
		results, err := r.remote.ReportClientAcks(writeCtx, pending)
		cancel()
		if err == nil && len(results) == len(pending) {
			retry := pending[:0]
			for i, result := range results {
				switch result.Outcome {
				case edgecontrol.ClientAckObservationApplied, edgecontrol.ClientAckObservationStale, edgecontrol.ClientAckObservationRejected:
				default:
					retry = append(retry, pending[i])
				}
			}
			pending = retry
			if len(pending) == 0 {
				return nil
			}
		}
		timer := time.NewTimer(clientAckRetryInterval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			stopClientAckTimer(timer)
			return pending
		}
	}
	return nil
}

func stopClientAckTimer(timer *time.Timer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}
