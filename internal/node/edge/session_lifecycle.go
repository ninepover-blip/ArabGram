package edge

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/coreexec"
	"telesrv/internal/mtprotoedge"
)

const (
	edgeSessionOfflineMaxPending    = 65536
	edgeSessionOfflineFlushInterval = 50 * time.Millisecond
	edgeSessionOfflineRetryMin      = 50 * time.Millisecond
	edgeSessionOfflineRetryMax      = 2 * time.Second
	edgeSessionOfflineDrainTimeout  = 5 * time.Second
)

var (
	errEdgeSessionLifecycleStopped  = errors.New("edge session lifecycle reporter stopped")
	errEdgeSessionLifecycleCapacity = errors.New("edge session lifecycle reporter capacity exceeded")
)

type sessionOfflineBatchSender interface {
	ReportSessionOfflineBatch(context.Context, []coreexec.SessionOfflineEvent) error
}

type sessionOfflineKey struct {
	rawAuthKeyID [8]byte
	sessionID    int64
}

// edgeSessionLifecycleReporter coalesces repeated physical generations of the
// same logical session and sends identity-only lifecycle batches to Core. The
// pending map is bounded above the process active-session admission ceiling;
// it never creates one goroutine or one synchronous gRPC call per disconnect.
type edgeSessionLifecycleReporter struct {
	sender sessionOfflineBatchSender
	log    *zap.Logger

	mu        sync.Mutex
	pending   map[sessionOfflineKey]coreexec.SessionOfflineEvent
	inFlight  int
	accepting bool
	wake      chan struct{}
	done      chan struct{}
}

func newEdgeSessionLifecycleReporter(sender sessionOfflineBatchSender, log *zap.Logger) (*edgeSessionLifecycleReporter, error) {
	if sender == nil {
		return nil, coreexec.ErrRemoteRPCUnavailable
	}
	if log == nil {
		log = zap.NewNop()
	}
	return &edgeSessionLifecycleReporter{
		sender:    sender,
		log:       log,
		pending:   make(map[sessionOfflineKey]coreexec.SessionOfflineEvent),
		accepting: true,
		wake:      make(chan struct{}, 1),
		done:      make(chan struct{}),
	}, nil
}

func (r *edgeSessionLifecycleReporter) Submit(event coreexec.SessionOfflineEvent) error {
	if r == nil || event.RawAuthKeyID == ([8]byte{}) || event.SessionID == 0 || event.UserID <= 0 || event.DisconnectedAt <= 0 {
		return coreexec.ErrSessionOfflineEventInvalid
	}
	key := sessionOfflineKey{rawAuthKeyID: event.RawAuthKeyID, sessionID: event.SessionID}
	r.mu.Lock()
	if !r.accepting {
		r.mu.Unlock()
		return errEdgeSessionLifecycleStopped
	}
	if current, ok := r.pending[key]; ok {
		if current.DisconnectedAt > event.DisconnectedAt {
			event = current
		}
	} else if len(r.pending)+r.inFlight >= edgeSessionOfflineMaxPending {
		r.mu.Unlock()
		return errEdgeSessionLifecycleCapacity
	}
	r.pending[key] = event
	r.mu.Unlock()
	r.signal()
	return nil
}

func (r *edgeSessionLifecycleReporter) Pending() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.pending) + r.inFlight
}

func (r *edgeSessionLifecycleReporter) Run(ctx context.Context) {
	if r == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	defer close(r.done)
	retry := edgeSessionOfflineRetryMin
	for {
		if err := ctx.Err(); err != nil {
			r.stopAccepting()
			drainCtx, cancel := context.WithTimeout(context.Background(), edgeSessionOfflineDrainTimeout)
			err := r.drain(drainCtx)
			cancel()
			if err != nil {
				r.log.Error("session offline lifecycle drain failed",
					zap.Int("pending", r.Pending()), zap.Error(err))
			}
			return
		}
		if r.Pending() == 0 {
			select {
			case <-ctx.Done():
				continue
			case <-r.wake:
			}
		}
		if r.Pending() < coreexec.MaxSessionOfflineBatchEvents {
			timer := time.NewTimer(edgeSessionOfflineFlushInterval)
			coalescing := true
			for coalescing && r.Pending() < coreexec.MaxSessionOfflineBatchEvents {
				select {
				case <-ctx.Done():
					coalescing = false
				case <-timer.C:
					coalescing = false
				case <-r.wake:
					// A new event wakes the empty queue, but it must not end the
					// coalescing window unless the fixed batch limit is reached.
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			if ctx.Err() != nil {
				continue
			}
		}
		batch := r.takeBatch(coreexec.MaxSessionOfflineBatchEvents)
		if len(batch) == 0 {
			continue
		}
		if err := r.sender.ReportSessionOfflineBatch(ctx, batch); err != nil {
			r.requeue(batch)
			r.log.Warn("session offline lifecycle batch failed; retained for retry",
				zap.Int("events", len(batch)), zap.Int("pending", r.Pending()), zap.Error(err))
			if !waitContext(ctx, retry) {
				continue
			}
			retry = min(retry*2, edgeSessionOfflineRetryMax)
			continue
		}
		r.completeBatch(len(batch))
		retry = edgeSessionOfflineRetryMin
	}
}

func (r *edgeSessionLifecycleReporter) Wait(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *edgeSessionLifecycleReporter) drain(ctx context.Context) error {
	retry := edgeSessionOfflineRetryMin
	for r.Pending() != 0 {
		batch := r.takeBatch(coreexec.MaxSessionOfflineBatchEvents)
		if len(batch) == 0 {
			return nil
		}
		if err := r.sender.ReportSessionOfflineBatch(ctx, batch); err != nil {
			r.requeue(batch)
			if !waitContext(ctx, retry) {
				return fmt.Errorf("drain %d lifecycle events: %w", r.Pending(), err)
			}
			retry = min(retry*2, edgeSessionOfflineRetryMax)
			continue
		}
		r.completeBatch(len(batch))
		retry = edgeSessionOfflineRetryMin
	}
	return nil
}

func (r *edgeSessionLifecycleReporter) stopAccepting() {
	r.mu.Lock()
	r.accepting = false
	r.mu.Unlock()
}

func (r *edgeSessionLifecycleReporter) takeBatch(limit int) []coreexec.SessionOfflineEvent {
	if limit <= 0 {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.pending) == 0 {
		return nil
	}
	batch := make([]coreexec.SessionOfflineEvent, 0, min(limit, len(r.pending)))
	for key, event := range r.pending {
		batch = append(batch, event)
		delete(r.pending, key)
		if len(batch) == limit {
			break
		}
	}
	r.inFlight += len(batch)
	return batch
}

func (r *edgeSessionLifecycleReporter) requeue(batch []coreexec.SessionOfflineEvent) {
	r.mu.Lock()
	r.inFlight -= len(batch)
	if r.inFlight < 0 {
		r.inFlight = 0
	}
	for _, event := range batch {
		key := sessionOfflineKey{rawAuthKeyID: event.RawAuthKeyID, sessionID: event.SessionID}
		if current, ok := r.pending[key]; ok && current.DisconnectedAt >= event.DisconnectedAt {
			continue
		}
		r.pending[key] = event
	}
	r.mu.Unlock()
	r.signal()
}

func (r *edgeSessionLifecycleReporter) completeBatch(size int) {
	r.mu.Lock()
	r.inFlight -= size
	if r.inFlight < 0 {
		r.inFlight = 0
	}
	r.mu.Unlock()
}

func (r *edgeSessionLifecycleReporter) signal() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func waitContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

type edgeSessionLifecycleObserver struct {
	local    mtprotoedge.SessionLifecycleObserver
	reporter *edgeSessionLifecycleReporter
	log      *zap.Logger
	now      func() time.Time
}

func newEdgeSessionLifecycleObserver(
	local mtprotoedge.SessionLifecycleObserver,
	reporter *edgeSessionLifecycleReporter,
	log *zap.Logger,
) (*edgeSessionLifecycleObserver, error) {
	if local == nil || reporter == nil {
		return nil, errors.New("edge session lifecycle observer requires local codec and Core reporter")
	}
	if log == nil {
		log = zap.NewNop()
	}
	return &edgeSessionLifecycleObserver{local: local, reporter: reporter, log: log, now: time.Now}, nil
}

func (o *edgeSessionLifecycleObserver) SessionOffline(rawAuthKeyID [8]byte, sessionID, userID int64, lastForUser bool) {
	if o == nil {
		return
	}
	// The Edge shell owns only codec/profile accelerators. Passing userID=0
	// prevents it from starting a fake local business-presence state machine.
	o.local.SessionOffline(rawAuthKeyID, sessionID, 0, false)
	if userID <= 0 {
		return
	}
	err := o.reporter.Submit(coreexec.SessionOfflineEvent{
		RawAuthKeyID:   rawAuthKeyID,
		SessionID:      sessionID,
		UserID:         userID,
		LastForUser:    lastForUser,
		DisconnectedAt: int(o.now().Unix()),
	})
	if err != nil {
		o.log.Error("session offline lifecycle admission failed", zap.Error(err))
	}
}

func (o *edgeSessionLifecycleObserver) SessionDestroyed(rawAuthKeyID [8]byte, sessionID int64) {
	if o == nil || o.local == nil {
		return
	}
	if destroyed, ok := o.local.(mtprotoedge.SessionDestructionObserver); ok {
		destroyed.SessionDestroyed(rawAuthKeyID, sessionID)
	}
}

var (
	_ mtprotoedge.SessionLifecycleObserver   = (*edgeSessionLifecycleObserver)(nil)
	_ mtprotoedge.SessionDestructionObserver = (*edgeSessionLifecycleObserver)(nil)
)
