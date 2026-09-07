package edge

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/edgecontrol"
)

const (
	physicalReceiptReporterShards = 16
	physicalReceiptShardCapacity  = 2048
	physicalReceiptBatchSize      = 128
	physicalReceiptFlushInterval  = 2 * time.Millisecond
	physicalReceiptRetryInterval  = 10 * time.Millisecond
)

type physicalReceiptReporter struct {
	remote  edgecontrol.PhysicalReceiptReporter
	timeout time.Duration
	log     *zap.Logger
	shards  []physicalReceiptReporterShard
	stop    chan struct{}
	started chan struct{}
	done    sync.WaitGroup
	runOnce sync.Once
	once    sync.Once
}

type physicalReceiptReporterShard struct {
	owner    *physicalReceiptReporter
	queue    chan edgecontrol.PhysicalReceipt
	reserved atomic.Int64
}

func newPhysicalReceiptReporter(remote edgecontrol.PhysicalReceiptReporter, timeout time.Duration, log *zap.Logger) *physicalReceiptReporter {
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	if log == nil {
		log = zap.NewNop()
	}
	r := &physicalReceiptReporter{remote: remote, timeout: timeout, log: log, stop: make(chan struct{}), started: make(chan struct{}), shards: make([]physicalReceiptReporterShard, physicalReceiptReporterShards)}
	for i := range r.shards {
		r.shards[i] = physicalReceiptReporterShard{owner: r, queue: make(chan edgecontrol.PhysicalReceipt, physicalReceiptShardCapacity)}
	}
	return r
}

func (r *physicalReceiptReporter) Run(ctx context.Context) {
	if r == nil || r.remote == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.runOnce.Do(func() {
		for i := range r.shards {
			r.done.Add(1)
			go r.shards[i].run(ctx)
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
func (r *physicalReceiptReporter) Close() {
	if r != nil {
		r.once.Do(func() {
			close(r.stop)
			r.runOnce.Do(func() { close(r.started) })
		})
	}
}
func (r *physicalReceiptReporter) Wait() {
	if r != nil {
		<-r.started
		r.done.Wait()
	}
}

func (r *physicalReceiptReporter) ReservePhysicalReceipts(batch edgecontrol.BatchID, source, target string, command edgecontrol.CommandID, notAfter time.Time, refs []edgecontrol.DeliveryRef) (edgecontrol.PhysicalReceiptReservation, error) {
	if r == nil || r.remote == nil || batch.Empty() || source == "" || target == "" || command.Empty() || len(refs) == 0 || !notAfter.After(time.Now()) {
		return nil, errors.New("edge physical receipt reporter: invalid reservation")
	}
	for _, ref := range refs {
		if !ref.Valid() {
			return nil, errors.New("edge physical receipt reporter: invalid delivery ref")
		}
	}
	shard := &r.shards[physicalReporterHash(source, command)&uint64(len(r.shards)-1)]
	count := int64(len(refs))
	for {
		used := shard.reserved.Load()
		if used > physicalReceiptShardCapacity-count {
			return nil, edgecontrol.ErrDeliveryOverloaded
		}
		if shard.reserved.CompareAndSwap(used, used+count) {
			break
		}
	}
	select {
	case <-r.stop:
		shard.reserved.Add(-count)
		return nil, edgecontrol.ErrDeliveryIndeterminate
	default:
	}
	return &physicalReceiptReservation{shard: shard, batch: batch, source: source, target: target, command: command, refs: append([]edgecontrol.DeliveryRef(nil), refs...), states: make([]atomic.Uint32, len(refs))}, nil
}

type physicalReceiptReservation struct {
	shard   *physicalReceiptReporterShard
	batch   edgecontrol.BatchID
	source  string
	target  string
	command edgecontrol.CommandID
	refs    []edgecontrol.DeliveryRef
	states  []atomic.Uint32
}

func (r *physicalReceiptReservation) Commit(index int, receipt edgecontrol.PhysicalReceipt) {
	if r == nil || r.shard == nil || index < 0 || index >= len(r.states) || receipt.BatchID != r.batch || receipt.SourceInstanceID != r.source || receipt.TargetInstanceID != r.target || receipt.CommandID != r.command || receipt.Ref != r.refs[index] || !validReporterPhysicalReceipt(receipt) || !r.states[index].CompareAndSwap(0, 1) {
		return
	}
	select {
	case r.shard.queue <- receipt:
		return
	default:
		// Every queued or not-yet-committed slot remains charged to reserved,
		// whose hard cap equals queue capacity. Reaching this branch means the
		// O(1) reservation invariant was corrupted; silently dropping durable
		// physical evidence would be worse than failing the process closed.
		panic("edge physical receipt reporter: reserved queue invariant violated")
	}
}

func validReporterPhysicalReceipt(receipt edgecontrol.PhysicalReceipt) bool {
	if receipt.BatchID.Empty() || receipt.CommandID.Empty() || !edgecontrol.ValidDeliveryInstanceID(receipt.SourceInstanceID) ||
		!edgecontrol.ValidDeliveryInstanceID(receipt.TargetInstanceID) || !receipt.Ref.Valid() || receipt.ObservedAt.IsZero() ||
		!receipt.Detail.Valid() || receipt.EligibleSessions < 0 || receipt.WrittenSessions < 0 || receipt.WrittenSessions > receipt.EligibleSessions {
		return false
	}
	switch receipt.Outcome {
	case edgecontrol.PhysicalWritten:
		return receipt.EligibleSessions > 0 && receipt.WrittenSessions == receipt.EligibleSessions && receipt.FirstServerMsgID > 0
	case edgecontrol.PhysicalNoEligibleSessions:
		return receipt.EligibleSessions == 0 && receipt.WrittenSessions == 0 && receipt.FirstServerMsgID == 0
	case edgecontrol.PhysicalIndeterminate, edgecontrol.PhysicalRejected:
		return receipt.WrittenSessions == 0 && receipt.FirstServerMsgID == 0
	default:
		return false
	}
}
func (r *physicalReceiptReservation) Release(index int) {
	if r != nil && r.shard != nil && index >= 0 && index < len(r.states) && r.states[index].CompareAndSwap(0, 2) {
		r.shard.reserved.Add(-1)
	}
}

func (s *physicalReceiptReporterShard) run(ctx context.Context) {
	defer s.owner.done.Done()
	batch := make([]edgecontrol.PhysicalReceipt, 0, physicalReceiptBatchSize)
	for {
		select {
		case <-ctx.Done():
			s.shutdownDrain(batch)
			return
		case <-s.owner.stop:
			s.shutdownDrain(batch)
			return
		case first := <-s.queue:
			batch = append(batch[:0], first)
			timer := time.NewTimer(physicalReceiptFlushInterval)
		collect:
			for len(batch) < physicalReceiptBatchSize {
				select {
				case item := <-s.queue:
					batch = append(batch, item)
				case <-timer.C:
					break collect
				case <-ctx.Done():
					stopPhysicalTimer(timer)
					s.shutdownDrain(batch)
					return
				case <-s.owner.stop:
					stopPhysicalTimer(timer)
					s.shutdownDrain(batch)
					return
				}
			}
			stopPhysicalTimer(timer)
			batch = s.reportUntilTerminal(ctx, batch)
		}
	}
}
func (s *physicalReceiptReporterShard) reportUntilTerminal(ctx context.Context, pending []edgecontrol.PhysicalReceipt) []edgecontrol.PhysicalReceipt {
	for len(pending) > 0 {
		writeCtx, cancel := context.WithTimeout(ctx, s.owner.timeout)
		results, err := s.owner.remote.ReportPhysicalReceipts(writeCtx, pending)
		cancel()
		if err == nil && len(results) == len(pending) {
			retry := pending[:0]
			for i, result := range results {
				switch result.Outcome {
				case edgecontrol.PhysicalReceiptApplied, edgecontrol.PhysicalReceiptStale, edgecontrol.PhysicalReceiptRejected:
					s.reserved.Add(-1)
				case edgecontrol.PhysicalReceiptRetryable:
					retry = append(retry, pending[i])
				default:
					retry = append(retry, pending[i])
				}
			}
			pending = retry
			if len(pending) == 0 {
				return pending
			}
		}
		select {
		case <-ctx.Done():
			// The process parent context only transfers ownership to the bounded
			// shutdown drain. Dropping here would erase already-committed physical
			// evidence before the independent shutdown context can report it.
			return pending
		case <-s.owner.stop:
			return pending
		case <-time.After(physicalReceiptRetryInterval):
		}
	}
	return pending
}
func (s *physicalReceiptReporterShard) shutdownDrain(initial []edgecontrol.PhysicalReceipt) {
	ctx, cancel := context.WithTimeout(context.Background(), s.owner.timeout)
	defer cancel()
	pending := append([]edgecontrol.PhysicalReceipt(nil), initial...)
	for {
		for len(pending) < physicalReceiptBatchSize {
			select {
			case receipt := <-s.queue:
				pending = append(pending, receipt)
			default:
				goto report
			}
		}
	report:
		if len(pending) == 0 {
			return
		}
		pending = s.reportShutdownUntilTerminal(ctx, pending)
		if len(pending) != 0 {
			s.reserved.Add(-int64(len(pending)))
			s.owner.log.Warn("physical receipt reporter shutdown left unresolved evidence", zap.Int("count", len(pending)))
			return
		}
		pending = pending[:0]
	}
}

func (s *physicalReceiptReporterShard) reportShutdownUntilTerminal(ctx context.Context, pending []edgecontrol.PhysicalReceipt) []edgecontrol.PhysicalReceipt {
	for len(pending) > 0 {
		writeCtx, cancel := context.WithTimeout(ctx, s.owner.timeout)
		results, err := s.owner.remote.ReportPhysicalReceipts(writeCtx, pending)
		cancel()
		if err == nil && len(results) == len(pending) {
			retry := pending[:0]
			for i, result := range results {
				switch result.Outcome {
				case edgecontrol.PhysicalReceiptApplied, edgecontrol.PhysicalReceiptStale, edgecontrol.PhysicalReceiptRejected:
					s.reserved.Add(-1)
				default:
					retry = append(retry, pending[i])
				}
			}
			pending = retry
			if len(pending) == 0 {
				return nil
			}
		}
		timer := time.NewTimer(physicalReceiptRetryInterval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			stopPhysicalTimer(timer)
			return pending
		}
	}
	return nil
}
func stopPhysicalTimer(timer *time.Timer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.C:
	default:
	}
}
func physicalReporterHash(source string, command edgecontrol.CommandID) uint64 {
	h := uint64(1469598103934665603)
	for i := 0; i < len(source); i++ {
		h ^= uint64(source[i])
		h *= 1099511628211
	}
	for _, b := range command {
		h ^= uint64(b)
		h *= 1099511628211
	}
	return h
}

var _ edgecontrol.PhysicalReceiptSink = (*physicalReceiptReporter)(nil)
