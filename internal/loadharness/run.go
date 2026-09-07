package loadharness

import (
	"context"
	"crypto/md5"
	cryptorand "crypto/rand"
	"crypto/rsa"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/iamxvbaba/td/mtproto"
	"github.com/iamxvbaba/td/pool"
	tdrpc "github.com/iamxvbaba/td/rpc"
	"github.com/iamxvbaba/td/telegram"
	"github.com/iamxvbaba/td/tg"
)

const (
	workerConnecting int32 = iota
	workerReady
	workerDisconnected
	workerOffline
	workerStopped
)

type RunConfig struct {
	// Package-private seams used only by explicit real-wire fault probes.
	wrapInvoker            func(SessionRecord, tg.Invoker) tg.Invoker
	wrapPreparationInvoker func(SessionRecord, tg.Invoker) tg.Invoker
	safety                 *loadSafety
	events                 *eventWriter
	wrapUpdates            func(SessionRecord, telegram.UpdateHandler) telegram.UpdateHandler
	observeEvents          func(*eventWriter) // Explicit wire fault probes only.
	observeLifecycle       func(*runLifecycle)
	observeWorkers         func([]*loadWorker)
	configureClient        func(SessionRecord, *clientHooks) // Real socket/PFS evidence in explicit wire probes.
	ManifestPath           string
	SessionKeyPath         string
	RSAKeyOverride         string
	ReportPath             string
	EventsPath             string
	EventLimits            EventLimits
	ResourceLimits         ResourceLimits
	EnvironmentPath        string
	MaxMessageIntents      uint64
	FileFixturePath        string
	ServerMetricsTargets   MetricsTargets
	StartOrder             string
	StartOrderSeed         int64
	SessionLimit           int
	Duration               time.Duration
	RecoveryDuration       time.Duration
	RampDuration           time.Duration
	RPCInterval            time.Duration
	MessageInterval        time.Duration
	MessageRate            float64
	MessageQueueDepth      int
	DeliverySettle         time.Duration
	DeliveryCacheRecords   int
	DeliveryLedgerBytes    int64
	FileInterval           time.Duration
	FileSizeBytes          int
	FileChunkBytes         int
	SetupTimeout           time.Duration
	OperationTimeout       time.Duration
	SampleInterval         time.Duration
	OfflineFraction        float64
	OfflineAt              time.Duration
	OfflineFor             time.Duration
	MinimumReadyRatio      float64
	ExpectServerRestart    bool
}

const RunReportVersion = 18
const defaultMessageIntentLimit = uint64(100000000)

func (c RunConfig) messageIntentLimit() uint64 {
	if c.MaxMessageIntents == 0 {
		return defaultMessageIntentLimit
	}
	return c.MaxMessageIntents
}

func (c RunConfig) validate() error {
	if err := c.ResourceLimits.validate(); err != nil {
		return err
	}
	if c.MaxMessageIntents > 1000000000 {
		return errors.New("message intent limit exceeds one billion")
	}
	if err := c.EventLimits.validate(); err != nil {
		return err
	}
	if err := (deliveryLedgerOptions{CacheRecords: c.DeliveryCacheRecords, MaxBytes: c.DeliveryLedgerBytes}).validate(); err != nil {
		return err
	}
	if err := c.ServerMetricsTargets.validate(); err != nil {
		return err
	}
	if c.ManifestPath == "" || c.SessionKeyPath == "" || c.ReportPath == "" || c.EventsPath == "" {
		return errors.New("manifest, session-key, report and events paths are required")
	}
	if err := validateEvidencePaths(c.ReportPath, c.EventsPath); err != nil {
		return err
	}
	if c.Duration <= 0 || c.RecoveryDuration < 0 || c.RampDuration < 0 || c.RPCInterval <= 0 || c.OperationTimeout <= 0 || c.SampleInterval <= 0 {
		return errors.New("run durations and intervals are invalid")
	}
	if math.IsNaN(c.MessageRate) || math.IsInf(c.MessageRate, 0) || c.MessageRate < 0 || c.MessageRate > 100000 || c.MessageQueueDepth < 0 || c.MessageQueueDepth > 1024 || c.DeliverySettle < 0 {
		return errors.New("message rate, queue depth or delivery settle is invalid")
	}
	if c.MessageRate > 0 && c.MessageInterval > 0 {
		return errors.New("message-rate and message-interval workloads are mutually exclusive")
	}
	if c.MessageRate > 0 && (c.MessageQueueDepth == 0 || c.RampDuration >= c.Duration) {
		return errors.New("fixed-rate workload requires a queue depth and load duration beyond the connection ramp")
	}
	if c.FileSizeBytes < 0 || c.FileChunkBytes < 0 || c.FileChunkBytes > 1<<20 || c.FileSizeBytes > 64<<20 {
		return errors.New("file size must be <=64MiB and chunk size must be <=1MiB")
	}
	if c.FileSizeBytes > 0 && (c.FileInterval <= 0 || c.FileChunkBytes <= 0) {
		return errors.New("enabled file workload requires positive interval and chunk size")
	}
	if c.FileSizeBytes > 0 && c.SetupTimeout <= 0 {
		return errors.New("enabled file workload requires a positive setup timeout")
	}
	if c.FileSizeBytes > 0 {
		path := resolveFileFixturePath(c.ManifestPath, c.FileFixturePath)
		for _, other := range []string{c.ReportPath, c.EventsPath, c.ManifestPath, c.SessionKeyPath} {
			if err := validateEvidencePaths(other, path); err != nil {
				return errors.New("file fixture overlaps evidence or identity input")
			}
		}
	}
	if c.OfflineFraction < 0 || c.OfflineFraction > 1 || c.MinimumReadyRatio <= 0 || c.MinimumReadyRatio > 1 {
		return errors.New("offline fraction and minimum ready ratio must be within [0,1]")
	}
	if c.OfflineFraction > 0 && (c.OfflineAt <= 0 || c.OfflineFor <= 0 || c.OfflineAt+c.OfflineFor >= c.Duration) {
		return errors.New("offline window must be positive and fit inside load duration")
	}
	if c.StartOrder != "" && c.StartOrder != StartupOrderShuffled && c.StartOrder != StartupOrderAccountIndex {
		return fmt.Errorf("unknown run start order %q", c.StartOrder)
	}
	return nil
}

type updateState struct {
	mu    sync.Mutex
	value tg.UpdatesState
	valid bool
}

func (s *updateState) load() (tg.UpdatesState, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value, s.valid
}

func (s *updateState) store(value tg.UpdatesState) {
	s.mu.Lock()
	s.value = value
	s.valid = true
	s.mu.Unlock()
}

type harnessCounters struct {
	connectionAttempts      atomic.Uint64
	reconnects              atomic.Uint64
	disconnects             atomic.Uint64
	updates                 atomic.Uint64
	fatalErrors             atomic.Uint64
	unjoinedClients         atomic.Uint64
	downloadBytes           atomic.Uint64
	messageScheduled        atomic.Uint64
	messageEnqueued         atomic.Uint64
	messageCompleted        atomic.Uint64
	messageQueueFull        atomic.Uint64
	messageNotReady         atomic.Uint64
	messageReadiness        operationMetrics
	messageIntentAdmissions atomic.Uint64
	messageIntentLimit      uint64
	safety                  *loadSafety
	messageReadyAt          time.Time // Written by scheduler; read only after its WaitGroup completes.
	schedulerLag            operationMetrics
	senderQueueWait         operationMetrics
	plannedToSend           operationMetrics
}

func (c *harnessCounters) admitMessageIntent() bool {
	limit := c.messageIntentLimit
	if limit == 0 {
		limit = defaultMessageIntentLimit
	}
	for {
		n := c.messageIntentAdmissions.Load()
		if n >= limit {
			if c.safety != nil {
				c.safety.trip("workload", "message_intent_budget", time.Now().UTC())
			}
			return false
		}
		if c.messageIntentAdmissions.CompareAndSwap(n, n+1) {
			return true
		}
	}
}

// messageArrival keeps the monotonic deadline even when the scheduler or
// sender falls behind. Queueing is part of the offered load's latency.
type messageArrival struct {
	plannedAt  time.Time
	enqueuedAt time.Time
}

var debugConnectionErrors atomic.Uint64
var debugOperationErrors atomic.Uint64

func debugConnectionError(sessionIndex int, err error) {
	if os.Getenv("TELESRV_LOAD_DEBUG_ERRORS") != "1" || err == nil || debugConnectionErrors.Add(1) > 20 {
		return
	}
	// Explicit opt-in diagnostics go only to stderr and are bounded. They are
	// never copied into the attachable report or event stream.
	fmt.Fprintf(os.Stderr, "load debug: session=%d error_type=%T error=%v\n", sessionIndex, err, err)
}

func debugOperationError(operation string, err error) {
	// Connection lifecycle errors have their own bounded diagnostic path. Do not
	// let an expected reconnect storm consume the operation-error budget that is
	// needed to diagnose actual RPC failures.
	if operation == "connection.dead" || os.Getenv("TELESRV_LOAD_DEBUG_ERRORS") != "1" || err == nil || debugOperationErrors.Add(1) > 20 {
		return
	}
	// Operation names are a code-owned finite vocabulary. Raw errors are
	// opt-in, stderr-only and bounded; reports/events retain only classifications.
	fmt.Fprintf(os.Stderr, "load debug: operation=%s error_type=%T error=%v\n", operation, err, err)
}

type downloadFixture struct {
	location *tg.InputDocumentFileLocation
	size     int
	chunk    int
}

type loadWorker struct {
	runClientOverride   func(context.Context) error // Explicit lifecycle test dependency only.
	wrapInvoker         func(SessionRecord, tg.Invoker) tg.Invoker
	wrapUpdates         func(SessionRecord, telegram.UpdateHandler) telegram.UpdateHandler
	configureClient     func(SessionRecord, *clientHooks)
	record              SessionRecord
	target              SessionRecord
	endpoint            Endpoint
	publicKey           *rsa.PublicKey
	storage             *EncryptedFileStorage
	metrics             *metricSet
	counters            *harnessCounters
	events              *eventWriter
	rpcInterval         time.Duration
	msgInterval         time.Duration
	fileInterval        time.Duration
	operationTimeout    time.Duration
	fileFixture         *downloadFixture
	delivery            *deliveryTracker
	allowPlannedOffline bool
	producerDeadline    time.Time // Immutable after worker launch; zero only in focused tests.

	lifecycleMu                 sync.Mutex
	workMu                      sync.Mutex
	producerStopOnce            sync.Once
	producerDrainOnce           sync.Once
	workStopOnce                sync.Once
	workDrainOnce               sync.Once
	gracefulStopOnce            sync.Once
	desired                     atomic.Bool
	state                       atomic.Int32
	everReady                   atomic.Bool
	businessReady               atomic.Bool
	clientGeneration            atomic.Uint64
	plannedDisconnectGeneration atomic.Uint64
	primaryAttempt              uint64 // Protected by lifecycleMu; not a socket or MTProto session ID.
	signal                      chan struct{}
	producerStop                chan struct{}
	producerDrained             chan struct{}
	workStop                    chan struct{}
	workDrained                 chan struct{}
	inflightProducers           int // Protected by workMu; additions are fenced by producerStop.
	inflightWork                int // Protected by workMu; additions are fenced by workStop.
	gracefulStop                chan struct{}
	clientCloseTimeout          time.Duration // Explicit lifecycle-test override only when non-zero.
	clientJoinTimeout           time.Duration // Explicit lifecycle-test override only when non-zero.
	lastUpdate                  updateState
	deliveryState               updateState
	messageSeq                  atomic.Uint64
	fileSeq                     atomic.Uint64
	sendQueue                   chan messageArrival
	reconcile                   chan chan struct{}
}

func newLoadWorker(record, target SessionRecord, endpoint Endpoint, publicKey *rsa.PublicKey, storage *EncryptedFileStorage, metrics *metricSet, counters *harnessCounters, events *eventWriter, rpcInterval, messageInterval, fileInterval, operationTimeout time.Duration, fixture *downloadFixture, delivery *deliveryTracker, messageQueueDepth int) *loadWorker {
	w := &loadWorker{
		record: record, target: target, endpoint: endpoint, publicKey: publicKey, storage: storage,
		metrics: metrics, counters: counters, events: events, rpcInterval: rpcInterval,
		msgInterval: messageInterval, fileInterval: fileInterval, operationTimeout: operationTimeout, fileFixture: fixture,
		delivery: delivery, signal: make(chan struct{}, 1), producerStop: make(chan struct{}),
		producerDrained: make(chan struct{}), workStop: make(chan struct{}),
		workDrained: make(chan struct{}), gracefulStop: make(chan struct{}),
		sendQueue: make(chan messageArrival, messageQueueDepth),
		reconcile: make(chan chan struct{}),
	}
	w.state.Store(workerStopped)
	return w
}

func (w *loadWorker) requestProducerStop() {
	w.producerStopOnce.Do(func() {
		w.workMu.Lock()
		close(w.producerStop)
		if w.inflightProducers == 0 {
			w.producerDrainOnce.Do(func() { close(w.producerDrained) })
		}
		w.workMu.Unlock()
	})
}

func (w *loadWorker) producerStopRequested() bool {
	select {
	case <-w.producerStop:
		return true
	default:
		return false
	}
}

func (w *loadWorker) requestWorkStop() {
	// The final observation fence also closes producer admission when a caller
	// did not already close it at the scheduled workload boundary.
	w.requestProducerStop()
	w.workStopOnce.Do(func() {
		w.workMu.Lock()
		close(w.workStop)
		if w.inflightWork == 0 {
			w.workDrainOnce.Do(func() { close(w.workDrained) })
		}
		w.workMu.Unlock()
	})
}

func (w *loadWorker) workStopRequested() bool {
	select {
	case <-w.workStop:
		return true
	default:
		return false
	}
}

// beginProducerWork admits traffic-generating work. Its counter is also part
// of the broader work drain, while producerDrained lets the owner stop load
// generation before it keeps observing updates and runs final reconciliation.
func (w *loadWorker) beginProducerWork() bool {
	w.workMu.Lock()
	defer w.workMu.Unlock()
	if w.producerStopRequested() || w.workStopRequested() || (!w.producerDeadline.IsZero() && !time.Now().Before(w.producerDeadline)) {
		return false
	}
	w.inflightProducers++
	w.inflightWork++
	return true
}

func (w *loadWorker) endProducerWork() {
	w.workMu.Lock()
	w.inflightProducers--
	w.inflightWork--
	if w.inflightProducers < 0 || w.inflightWork < 0 {
		w.workMu.Unlock()
		panic("loadharness: negative in-flight producer work")
	}
	if w.inflightProducers == 0 && w.producerStopRequested() {
		w.producerDrainOnce.Do(func() { close(w.producerDrained) })
	}
	if w.inflightWork == 0 && w.workStopRequested() {
		w.workDrainOnce.Do(func() { close(w.workDrained) })
	}
	w.workMu.Unlock()
}

// beginWork is the linearization point for client-issued RPC work and update
// accounting. requestWorkStop and beginWork share workMu, so no operation can
// be admitted after requestWorkStop returns. Work admitted before the fence is
// allowed to finish and is included in the authoritative workload-end cut.
func (w *loadWorker) beginWork() bool {
	w.workMu.Lock()
	defer w.workMu.Unlock()
	if w.workStopRequested() {
		return false
	}
	w.inflightWork++
	return true
}

func (w *loadWorker) endWork() {
	w.workMu.Lock()
	w.inflightWork--
	if w.inflightWork < 0 {
		w.workMu.Unlock()
		panic("loadharness: negative in-flight work")
	}
	if w.inflightWork == 0 && w.workStopRequested() {
		w.workDrainOnce.Do(func() { close(w.workDrained) })
	}
	w.workMu.Unlock()
}

func (w *loadWorker) requestGracefulStop() {
	w.gracefulStopOnce.Do(func() { close(w.gracefulStop) })
}

func (w *loadWorker) gracefulStopRequested() bool {
	select {
	case <-w.gracefulStop:
		return true
	default:
		return false
	}
}

func (w *loadWorker) plannedGracefulDisconnect(generation uint64) error {
	w.plannedDisconnectGeneration.Store(generation)
	return nil
}

func (w *loadWorker) closeTimeout() time.Duration {
	if w.clientCloseTimeout > 0 {
		return w.clientCloseTimeout
	}
	return max(5*time.Second, w.operationTimeout)
}

func (w *loadWorker) joinTimeout() time.Duration {
	if w.clientJoinTimeout > 0 {
		return w.clientJoinTimeout
	}
	return max(5*time.Second, w.operationTimeout)
}

type clientBoundaryResult uint8

const (
	clientBoundaryJoined clientBoundaryResult = iota
	clientBoundaryContext
	clientBoundaryTimeout
)

// waitClientBoundary gives a completed client precedence over parent
// cancellation, and parent cancellation precedence over a timeout, when more
// than one boundary becomes ready in the same scheduling turn. Without the
// final non-blocking checks, Go's randomized select can manufacture a false
// lifecycle timeout or hide a completed client result.
func waitClientBoundary(ctxDone <-chan struct{}, done <-chan error, timer <-chan time.Time) (error, clientBoundaryResult) {
	select {
	case err := <-done:
		return err, clientBoundaryJoined
	case <-ctxDone:
		select {
		case err := <-done:
			return err, clientBoundaryJoined
		default:
			return nil, clientBoundaryContext
		}
	case <-timer:
		select {
		case err := <-done:
			return err, clientBoundaryJoined
		default:
		}
		select {
		case <-ctxDone:
			return nil, clientBoundaryContext
		default:
			return nil, clientBoundaryTimeout
		}
	}
}

func (w *loadWorker) operationContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, w.operationTimeout)
}

func (w *loadWorker) setOnline(online bool) {
	w.lifecycleMu.Lock()
	w.desired.Store(online)
	if !online {
		w.businessReady.Store(false)
		w.delivery.deviceTransition(w.record.Index, w.clientGeneration.Load(), "offline")
	}
	w.lifecycleMu.Unlock()
	w.events.write(map[string]any{"type": "session_desired", "at": time.Now().UTC(), "session_index": w.record.Index, "online": online, "client_generation": w.clientGeneration.Load()})
	select {
	case w.signal <- struct{}{}:
	default:
	}
}

func (w *loadWorker) supervise(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	defer w.state.Store(workerStopped)
	for {
		if w.workStopRequested() {
			select {
			case <-ctx.Done():
			case <-w.gracefulStop:
			}
			return
		}
		if w.producerStopRequested() {
			// The scheduled workload has ended. Do not create a replacement
			// generation during settle/reconcile; an already-running generation
			// stays alive in the waitClient block below to observe updates.
			select {
			case <-ctx.Done():
			case <-w.gracefulStop:
			}
			return
		}
		if w.gracefulStopRequested() {
			return
		}
		if err := ctx.Err(); err != nil {
			w.state.Store(workerStopped)
			return
		}
		if !w.desired.Load() {
			w.state.Store(workerOffline)
			select {
			case <-ctx.Done():
				w.state.Store(workerStopped)
				return
			case <-w.workStop:
				continue
			case <-w.gracefulStop:
				return
			case <-w.signal:
				continue
			}
		}

		// Fence generation creation with the producer gate. A generation admitted
		// before quiesce may start, but its runClient entry must independently
		// acquire the startup token; no generation can be created at or after the
		// scheduled deadline or after requestProducerStop returns.
		if !w.beginProducerWork() {
			select {
			case <-ctx.Done():
			case <-w.gracefulStop:
			}
			return
		}
		clientCtx, cancel := context.WithCancel(ctx)
		w.lifecycleMu.Lock()
		generation := w.clientGeneration.Add(1)
		w.plannedDisconnectGeneration.Store(0)
		w.delivery.deviceTransition(w.record.Index, generation, "start")
		w.lifecycleMu.Unlock()
		w.events.write(map[string]any{"type": "client_generation_start", "at": time.Now().UTC(), "session_index": w.record.Index, "generation": generation})
		done := make(chan error, 1)
		go func() {
			if clientCtx.Err() != nil {
				done <- clientCtx.Err()
				return
			}
			if w.runClientOverride != nil {
				done <- w.runClientOverride(clientCtx)
			} else {
				done <- w.runClient(clientCtx)
			}
		}()
		w.endProducerWork()
		const (
			clientExitUnexpected = iota
			clientExitOffline
			clientExitContext
			clientExitGraceful
		)
		var clientErr error
		joined := false
		exitReason := clientExitUnexpected
		workStopC := w.workStop
	waitClient:
		for {
			if !w.desired.Load() {
				exitReason = clientExitOffline
				break waitClient
			}
			select {
			case <-ctx.Done():
				exitReason = clientExitContext
				break waitClient
			case <-w.gracefulStop:
				exitReason = clientExitGraceful
				break waitClient
			case <-workStopC:
				// The client callback parks after its currently admitted logical
				// operation. Keep the transport alive until the workload-end cut.
				workStopC = nil
			case <-w.signal:
				if !w.desired.Load() {
					exitReason = clientExitOffline
					break waitClient
				}
			case err := <-done:
				clientErr, joined = err, true
				exitReason = clientExitUnexpected
				break waitClient
			}
		}
		if exitReason == clientExitGraceful && !joined {
			timeout := w.closeTimeout()
			timer := time.NewTimer(timeout)
			var boundary clientBoundaryResult
			clientErr, boundary = waitClientBoundary(ctx.Done(), done, timer.C)
			switch boundary {
			case clientBoundaryJoined:
				joined = true
			case clientBoundaryContext:
				exitReason = clientExitContext
			case clientBoundaryTimeout:
				w.events.write(map[string]any{"type": "client_graceful_close_timeout", "at": time.Now().UTC(), "session_index": w.record.Index, "timeout": timeout.String()})
				if w.counters.safety != nil {
					w.counters.safety.trip("lifecycle", "client_graceful_close_timeout", time.Now().UTC())
				}
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		if !joined {
			cancel()
			timeout := w.joinTimeout()
			timer := time.NewTimer(timeout)
			var boundary clientBoundaryResult
			clientErr, boundary = waitClientBoundary(nil, done, timer.C)
			if boundary == clientBoundaryJoined {
				joined = true
			} else {
				unjoined := w.counters.unjoinedClients.Add(1)
				w.events.write(map[string]any{"type": "client_cancel_join_timeout", "at": time.Now().UTC(), "session_index": w.record.Index, "generation": generation, "timeout": timeout.String(), "unjoined_clients": unjoined})
				if w.counters.safety != nil {
					w.counters.safety.trip("lifecycle", "client_cancel_join_timeout", time.Now().UTC())
				}
				// A Go goroutine cannot be force-killed safely. Keep the supervisor
				// attached until the real client exits, so no device-stop marker,
				// WorkersStoppedAt, recovery, or evidence finalization can overtake
				// callbacks from an unjoined generation. The standalone load process
				// remains the outer hard-stop boundary for this pathological failure.
				clientErr = <-done
				joined = true
			}
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		} else {
			cancel()
		}
		w.markBusinessUnavailable()
		w.delivery.deviceTransition(w.record.Index, generation, "stop")
		stopClass := classifyError(clientErr)
		w.events.write(map[string]any{"type": "client_generation_stop", "at": time.Now().UTC(), "session_index": w.record.Index, "generation": generation, "class": stopClass})
		if exitReason == clientExitGraceful || w.gracefulStopRequested() {
			if clientErr != nil && ctx.Err() == nil {
				w.counters.fatalErrors.Add(1)
				w.events.write(map[string]any{"type": "worker_error", "at": time.Now().UTC(), "session_index": w.record.Index, "class": classifyError(clientErr)})
			}
			return
		}
		if exitReason == clientExitContext || ctx.Err() != nil {
			return
		}
		if exitReason == clientExitOffline || !w.desired.Load() {
			w.state.Store(workerOffline)
			continue
		}
		w.counters.fatalErrors.Add(1)
		w.events.write(map[string]any{"type": "worker_error", "at": time.Now().UTC(), "session_index": w.record.Index, "class": classifyError(clientErr)})
		if w.workStopRequested() {
			return
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-w.workStop:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (w *loadWorker) runClient(ctx context.Context) error {
	w.markBusinessUnavailable()
	defer w.markBusinessUnavailable()
	generation := w.clientGeneration.Load()
	if !w.beginProducerWork() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-w.gracefulStop:
			return nil
		}
	}
	var startupWorkOnce sync.Once
	finishStartupWork := func() { startupWorkOnce.Do(w.endProducerWork) }
	defer finishStartupWork()
	reconnectSignal := make(chan struct{}, 1)
	var handler telegram.UpdateHandler = telegram.UpdateHandlerFunc(func(_ context.Context, updates tg.UpdatesClass) error {
		if !w.beginWork() {
			return nil
		}
		defer w.endWork()
		w.counters.updates.Add(1)
		observeUpdatesClass(w.delivery, w.record.Index, updates, deliveryLive, generation)
		return nil
	})
	if w.wrapUpdates != nil {
		handler = w.wrapUpdates(w.record, handler)
	}
	hooks := clientHooks{
		Update: handler,
		ConnectionState: func(state telegram.ConnectionState) {
			w.primaryState(generation, state)
			if state == telegram.ConnectionStateReady {
				select {
				case reconnectSignal <- struct{}{}:
				default:
				}
			}
		},
		Dead: func(err error) {
			debugConnectionError(w.record.Index, err)
			w.metrics.observe("connection.dead", time.Now(), err)
			w.events.write(map[string]any{
				"type": "connection_dead", "at": time.Now().UTC(), "session_index": w.record.Index,
				"class": classifyError(err), "reason": classifyErrorReason(err),
			})
		},
	}
	if w.configureClient != nil {
		w.configureClient(w.record, &hooks)
	}
	client, err := newClient(w.endpoint, w.publicKey, w.storage, hooks)
	if err != nil {
		return err
	}
	return client.Run(ctx, func(ctx context.Context) error {
		statusStart := time.Now()
		operationCtx, cancelOperation := w.operationContext(ctx)
		status, err := client.Auth().Status(operationCtx)
		cancelOperation()
		w.metrics.observe("auth.status", statusStart, err)
		if err != nil {
			return err
		}
		if !status.Authorized || status.User == nil || status.User.ID != w.record.UserID {
			if w.counters.safety != nil {
				w.counters.safety.trip("identity", "manifest_authorization_mismatch", time.Now().UTC())
			}
			return errors.New("session is not authorized for its manifest user")
		}
		raw := tg.NewClient(workloadInvoker(w.record, client, w.wrapInvoker))
		if err := w.activateBusiness(ctx, raw, reconnectSignal); err != nil {
			return err
		}
		finishStartupWork()

		rpcTicker := time.NewTicker(w.rpcInterval)
		defer rpcTicker.Stop()
		rpcC := rpcTicker.C
		var messageTicker *time.Ticker
		var messageC <-chan time.Time
		if w.msgInterval > 0 && w.record.DeviceIndex == 0 && w.target.UserID > 0 {
			messageTicker = time.NewTicker(w.msgInterval)
			messageC = messageTicker.C
			defer messageTicker.Stop()
		}
		var fileTicker *time.Ticker
		var fileC <-chan time.Time
		if w.fileFixture != nil && w.fileInterval > 0 {
			fileTicker = time.NewTicker(w.fileInterval)
			fileC = fileTicker.C
			defer fileTicker.Stop()
		}
		producerStopC := w.producerStop
		cycle := 0
		for {
			if w.workStopRequested() {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-w.gracefulStop:
					return w.plannedGracefulDisconnect(generation)
				}
			}
			if w.gracefulStopRequested() {
				return w.plannedGracefulDisconnect(generation)
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-w.gracefulStop:
				return w.plannedGracefulDisconnect(generation)
			case <-w.workStop:
				continue
			case <-producerStopC:
				// A closed channel must be removed from the select. Disabling the
				// ticker cases here plus beginProducerWork's mutex-protected check
				// prevents a timer selected concurrently with the stop fence from
				// admitting post-duration traffic.
				producerStopC = nil
				rpcC, messageC, fileC = nil, nil, nil
			case <-reconnectSignal:
				if !w.beginWork() {
					continue
				}
				err := w.activateBusiness(ctx, raw, reconnectSignal)
				w.endWork()
				if err != nil {
					return err
				}
			case <-rpcC:
				if !w.beginProducerWork() {
					continue
				}
				w.runRPC(ctx, client, raw, cycle)
				w.endProducerWork()
				cycle++
			case <-messageC:
				if !w.beginProducerWork() {
					continue
				}
				w.sendMessage(ctx, raw, messageArrival{})
				w.endProducerWork()
			case arrival := <-w.sendQueue:
				if !w.beginWork() {
					continue
				}
				w.sendMessage(ctx, raw, arrival)
				w.endWork()
			case done := <-w.reconcile:
				if !w.beginWork() {
					close(done)
					continue
				}
				err := w.catchUpDelivery(ctx, raw)
				w.endWork()
				if err == nil {
					cursor, _ := w.deliveryState.load()
					w.delivery.markFinalReconciled(w.record.Index, w.clientGeneration.Load(), cursor)
				}
				close(done)
				if err != nil {
					return err
				}
			case <-fileC:
				if !w.beginProducerWork() {
					continue
				}
				w.downloadFileChunk(ctx, raw)
				w.endProducerWork()
			}
		}
	})
}

func (w *loadWorker) markBusinessUnavailable() {
	w.lifecycleMu.Lock()
	defer w.lifecycleMu.Unlock()
	w.businessReady.Store(false)
	w.delivery.deviceTransition(w.record.Index, w.clientGeneration.Load(), "unavailable")
}

func (w *loadWorker) runRPC(ctx context.Context, client *telegram.Client, raw *tg.Client, cycle int) {
	if ctx.Err() != nil {
		return
	}
	start := time.Now()
	operationCtx, cancel := w.operationContext(ctx)
	defer cancel()
	var err error
	switch cycle % 4 {
	case 0:
		err = client.Ping(operationCtx)
		w.metrics.observe("ping", start, err)
	case 1:
		var state *tg.UpdatesState
		state, err = raw.UpdatesGetState(operationCtx)
		if err == nil {
			w.lastUpdate.store(*state)
		}
		w.metrics.observe("updates.getState", start, err)
	case 2:
		_, err = raw.MessagesGetDialogs(operationCtx, &tg.MessagesGetDialogsRequest{OffsetPeer: &tg.InputPeerEmpty{}, Limit: 20})
		w.metrics.observe("messages.getDialogs", start, err)
	case 3:
		_, err = raw.HelpGetConfig(operationCtx)
		w.metrics.observe("help.getConfig", start, err)
	}
}

func (w *loadWorker) refreshUpdateState(ctx context.Context, raw *tg.Client) error {
	start := time.Now()
	operationCtx, cancel := w.operationContext(ctx)
	state, err := raw.UpdatesGetState(operationCtx)
	cancel()
	w.metrics.observe("updates.getState", start, err)
	if err == nil {
		w.lastUpdate.store(*state)
		if _, valid := w.deliveryState.load(); !valid {
			w.deliveryState.store(*state)
		}
	}
	return err
}

func (w *loadWorker) sendMessage(ctx context.Context, raw *tg.Client, arrival messageArrival) {
	defer w.counters.messageCompleted.Add(1)
	if ctx.Err() != nil {
		return
	}
	if !w.counters.admitMessageIntent() {
		return
	}
	sequence := w.messageSeq.Add(1)
	var randomBytes [8]byte
	if _, err := cryptorand.Read(randomBytes[:]); err != nil {
		return
	}
	randomID := int64(binary.LittleEndian.Uint64(randomBytes[:]))
	if randomID == 0 {
		randomID = int64(sequence)
	}
	start := time.Now()
	if !arrival.plannedAt.IsZero() {
		w.counters.senderQueueWait.observeDuration(start.Sub(arrival.enqueuedAt), nil)
		w.counters.plannedToSend.observeDuration(start.Sub(arrival.plannedAt), nil)
	}
	marker := w.delivery.marker(w.record.Index, sequence)
	if err := w.delivery.begin(marker, w.record.Index, w.target.UserID, randomID, start, arrival.plannedAt); err != nil {
		w.metrics.observe("messages.sendMessage", start, err)
		return
	}
	// Each attempt receives its own TL object; Encode mutates flags. The
	// immutable marker/random-id/peer and original latency origin never change.
	for attempt := 0; attempt < 2; attempt++ {
		if ctx.Err() != nil {
			break
		}
		attemptStart := time.Now()
		operationCtx, cancel := w.operationContext(ctx)
		result, err := raw.MessagesSendMessage(operationCtx, &tg.MessagesSendMessageRequest{
			Peer: &tg.InputPeerUser{UserID: w.target.UserID, AccessHash: w.target.AccessHash}, Message: marker, RandomID: randomID,
		})
		cancel()
		var messageID int
		if err == nil {
			messageID, err = validateSendConfirmation(result, marker, randomID, w.record.UserID, w.target.UserID)
			if err != nil {
				w.delivery.invariant("send_confirmation")
			}
		}
		name := "messages.sendMessage"
		if attempt > 0 {
			name += ".retry"
		}
		w.metrics.observe(name, attemptStart, err)
		outcome := classifySendAttempt(err)
		w.delivery.finish(marker, outcome, attempt > 0, messageID)
		if outcome != sendUncertain || ctx.Err() != nil {
			break
		}
	}
}

func (w *loadWorker) downloadFileChunk(ctx context.Context, raw *tg.Client) {
	if ctx.Err() != nil {
		return
	}
	fixture := w.fileFixture
	if fixture == nil || fixture.size <= 0 || fixture.chunk <= 0 {
		return
	}
	sequence := w.fileSeq.Add(1)
	offset := int64((int(sequence-1) * fixture.chunk) % fixture.size)
	limit := min(fixture.chunk, fixture.size-int(offset))
	start := time.Now()
	operationCtx, cancel := w.operationContext(ctx)
	result, err := raw.UploadGetFile(operationCtx, &tg.UploadGetFileRequest{
		Location: fixture.location, Offset: offset, Limit: limit,
	})
	cancel()
	if err == nil {
		file, ok := result.(*tg.UploadFile)
		switch {
		case !ok:
			err = invariantError("file_response_type", "upload.getFile returned %T", result)
		case len(file.Bytes) != limit:
			err = invariantError("file_response_length", "upload.getFile bytes=%d want=%d", len(file.Bytes), limit)
		case !validFixtureBytes(file.Bytes, offset):
			err = invariantError("file_response_payload", "upload.getFile payload mismatch")
		default:
			w.counters.downloadBytes.Add(uint64(len(file.Bytes)))
		}
	}
	if class := invariantClass(err); class != "" && w.counters.safety != nil {
		w.counters.safety.trip("media_invariant", class, time.Now().UTC())
	}
	w.metrics.observe("upload.getFile", start, err)
}

func prepareDownloadFixture(ctx context.Context, cfg RunConfig, manifest *Manifest, record SessionRecord, key [32]byte, publicKey *rsa.PublicKey, metrics *metricSet, preparation *FilePreparationReport) (*downloadFixture, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data := make([]byte, cfg.FileSizeBytes)
	for i := range data {
		data[i] = fixtureByte(int64(i))
	}
	fileID, err := randomNonZeroInt64()
	if err != nil {
		return nil, err
	}
	// Synthetic file IDs are kept only in the private event artifact, before
	// any upload can commit. A failed response never erases this intent.
	cfg.events.write(map[string]any{"type": "preparation_upload_intent", "at": time.Now().UTC(), "file_id": fileID, "size_bytes": len(data), "pattern_version": fixturePatternVersion, "session_index": record.Index})
	storage := &EncryptedFileStorage{Path: resolveSessionPath(cfg.ManifestPath, record), Key: key}
	client, err := newClient(manifest.Endpoint, publicKey, storage, clientHooks{})
	if err != nil {
		return nil, err
	}
	var location *tg.InputDocumentFileLocation
	preparation.ConnectionAttempts++
	err = client.Run(ctx, func(ctx context.Context) error {
		statusCtx, cancel := context.WithTimeout(ctx, cfg.OperationTimeout)
		status, err := client.Auth().Status(statusCtx)
		cancel()
		if err != nil {
			return err
		}
		if !status.Authorized || status.User == nil || status.User.ID != record.UserID {
			if cfg.safety != nil {
				cfg.safety.trip("identity", "fixture_authorization_mismatch", time.Now().UTC())
			}
			return errors.New("fixture session is not authorized")
		}
		raw := tg.NewClient(deadlineInvoker{next: workloadInvoker(record, client, cfg.wrapPreparationInvoker), timeout: cfg.OperationTimeout})
		const partSize = 512 << 10
		parts := (len(data) + partSize - 1) / partSize
		big := len(data) > 10<<20
		for part := 0; part < parts; part++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			startOffset := part * partSize
			endOffset := min(len(data), startOffset+partSize)
			start := time.Now()
			cfg.events.write(map[string]any{"type": "preparation_part_start", "at": start.UTC(), "file_id": fileID, "part": part, "bytes": endOffset - startOffset})
			preparation.PartCalls++
			var saved bool
			if big {
				saved, err = raw.UploadSaveBigFilePart(ctx, &tg.UploadSaveBigFilePartRequest{
					FileID: fileID, FilePart: part, FileTotalParts: parts, Bytes: data[startOffset:endOffset],
				})
			} else {
				saved, err = raw.UploadSaveFilePart(ctx, &tg.UploadSaveFilePartRequest{
					FileID: fileID, FilePart: part, Bytes: data[startOffset:endOffset],
				})
			}
			if err == nil && !saved {
				err = errors.New("upload part was not saved")
			}
			operation := "upload.saveFilePart"
			if big {
				operation = "upload.saveBigFilePart"
			}
			metrics.observe(operation, start, err)
			if err == nil && saved {
				preparation.PartResponsesOK++
				preparation.AcknowledgedPartBytes += uint64(endOffset - startOffset)
			}
			cfg.events.write(map[string]any{"type": "preparation_part_result", "at": time.Now().UTC(), "file_id": fileID, "part": part, "saved": saved, "class": classifyError(err)})
			if err != nil {
				return err
			}
			if !saved {
				return fmt.Errorf("upload part %d was not saved", part)
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		var file tg.InputFileClass
		if big {
			file = &tg.InputFileBig{ID: fileID, Parts: parts, Name: "telesrv-load.bin"}
		} else {
			digest := md5.Sum(data)
			file = &tg.InputFile{ID: fileID, Parts: parts, Name: "telesrv-load.bin", MD5Checksum: hex.EncodeToString(digest[:])}
		}
		start := time.Now()
		cfg.events.write(map[string]any{"type": "preparation_assemble_start", "at": start.UTC(), "file_id": fileID, "parts": parts})
		preparation.AssembleCalls++
		media, err := raw.MessagesUploadMedia(ctx, &tg.MessagesUploadMediaRequest{
			Peer: &tg.InputPeerEmpty{},
			Media: &tg.InputMediaUploadedDocument{
				File: file, MimeType: "application/octet-stream",
				Attributes: []tg.DocumentAttributeClass{&tg.DocumentAttributeFilename{FileName: "telesrv-load.bin"}},
			},
		})
		metrics.observe("messages.uploadMedia", start, err)
		cfg.events.write(map[string]any{"type": "preparation_assemble_result", "at": time.Now().UTC(), "file_id": fileID, "class": classifyError(err)})
		if err != nil {
			return err
		}
		documentMedia, ok := media.(*tg.MessageMediaDocument)
		if !ok {
			return fmt.Errorf("messages.uploadMedia returned %T", media)
		}
		documentClass, ok := documentMedia.GetDocument()
		if !ok {
			return errors.New("messages.uploadMedia omitted document")
		}
		document, ok := documentClass.(*tg.Document)
		if !ok {
			return fmt.Errorf("uploaded document is %T", documentClass)
		}
		location = &tg.InputDocumentFileLocation{
			ID: document.ID, AccessHash: document.AccessHash,
			FileReference: append([]byte(nil), document.FileReference...),
		}
		if document.Size != int64(len(data)) {
			return invariantError("uploaded_document_size", "uploaded document size mismatch")
		}
		cfg.events.write(map[string]any{"type": "preparation_document_observed", "at": time.Now().UTC(), "file_id": fileID, "document_id": document.ID, "size_bytes": document.Size})
		return nil
	})
	if err != nil {
		return nil, err
	}
	if location == nil {
		return nil, errors.New("file fixture completed without a location")
	}
	return &downloadFixture{location: location, size: len(data), chunk: cfg.FileChunkBytes}, nil
}

func loadOrCreateDownloadFixture(ctx context.Context, cfg RunConfig, manifest *Manifest, record SessionRecord, key [32]byte, publicKey *rsa.PublicKey, metrics *metricSet, preparation *FilePreparationReport) (*downloadFixture, error) {
	path := resolveFileFixturePath(cfg.ManifestPath, cfg.FileFixturePath)
	preparation.Stage = "read_location"
	fixture, err := loadPersistedFileFixture(path, manifest.Endpoint, cfg.FileSizeBytes, cfg.FileChunkBytes)
	if err == nil {
		preparation.Disposition = "reused"
		preparation.Stage = "complete"
		return fixture, nil
	}
	if !os.IsNotExist(err) {
		return nil, fmt.Errorf("load file fixture %q: %w", path, err)
	}
	setupCtx, cancel := context.WithTimeout(ctx, cfg.SetupTimeout)
	defer cancel()
	preparation.Stage = "upload"
	fixture, err = prepareDownloadFixture(setupCtx, cfg, manifest, record, key, publicKey, metrics, preparation)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(setupCtx.Err(), context.DeadlineExceeded) {
			return nil, fmt.Errorf("create file fixture timed out after %s: %w", cfg.SetupTimeout, context.DeadlineExceeded)
		}
		return nil, err
	}
	// The server-side file may now exist even if persisting its location fails.
	preparation.Disposition = "uploaded"
	preparation.Stage = "persist_location"
	if err := writePersistedFileFixture(path, manifest.Endpoint, fixture); err != nil {
		return nil, fmt.Errorf("persist file fixture: %w", err)
	}
	preparation.Stage = "complete"
	preparation.Disposition = "created"
	return fixture, nil
}

func randomNonZeroInt64() (int64, error) {
	var data [8]byte
	if _, err := cryptorand.Read(data[:]); err != nil {
		return 0, err
	}
	value := int64(binary.LittleEndian.Uint64(data[:]) & math.MaxInt64)
	if value == 0 {
		value = 1
	}
	return value, nil
}

func fixtureByte(offset int64) byte {
	return byte((uint64(offset)*31 + 17) % 251)
}

func validFixtureBytes(data []byte, offset int64) bool {
	for i, value := range data {
		if value != fixtureByte(offset+int64(i)) {
			return false
		}
	}
	return true
}

// Run executes a bounded real-client load and keeps scraping after all workers
// stop so logical-session/outbox reclamation can be proven rather than assumed.
func Run(ctx context.Context, cfg RunConfig) (*RunReport, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if err := requireNewReport(cfg.ReportPath); err != nil {
		return nil, err
	}
	manifest, err := LoadManifest(cfg.ManifestPath)
	if err != nil {
		return nil, err
	}
	key, err := LoadSessionKey(cfg.SessionKeyPath)
	if err != nil {
		return nil, err
	}
	publicKey, err := loadManifestPublicKey(cfg.ManifestPath, manifest.Endpoint, cfg.RSAKeyOverride)
	if err != nil {
		return nil, err
	}
	records := manifest.Sessions
	if cfg.SessionLimit > 0 && cfg.SessionLimit < len(records) {
		records = records[:cfg.SessionLimit]
	}
	if len(records) == 0 {
		return nil, errors.New("manifest has no selected sessions")
	}
	if cfg.OfflineFraction > 0 {
		hasSecondary := false
		for _, record := range records {
			hasSecondary = hasSecondary || record.DeviceIndex > 0
		}
		if !hasSecondary {
			return nil, errors.New("planned offline load requires selected secondary devices; senders remain online")
		}
	}
	if err := validateProcessCapacity(len(records)); err != nil {
		return nil, err
	}
	metrics := newMetricSet("auth.status", "connection.dead", "ping", "updates.getState", "updates.getDifference", "updates.getState.delivery", "updates.getDifference.delivery", "messages.getDialogs", "help.getConfig", "messages.sendMessage", "messages.sendMessage.retry", "upload.saveFilePart", "messages.uploadMedia", "upload.getFile")
	counters := &harnessCounters{}
	runID, err := newLoadRunID()
	if err != nil {
		return nil, fmt.Errorf("create load run id: %w", err)
	}
	delivery, err := newDeliveryTracker(runID, records, cfg.ReportPath+".delivery", deliveryLedgerOptions{CacheRecords: cfg.DeliveryCacheRecords, MaxBytes: cfg.DeliveryLedgerBytes})
	if err != nil {
		return nil, err
	}
	defer delivery.close()
	if err := delivery.preflight(cfg); err != nil {
		return nil, err
	}
	events, err := newBoundedEventWriter(cfg.EventsPath, cfg.EventLimits)
	if err != nil {
		return nil, err
	}
	delivery.events = events
	defer events.close()
	loadCtx, safety := newLoadSafety(ctx)
	stopLoad := safety.cancel
	defer stopLoad()
	observationCtx, stopObservation := context.WithCancel(context.WithoutCancel(ctx))
	defer stopObservation()
	events.onFailure = safety.eventsFailure
	delivery.onFailure = safety.deliveryFailure
	delivery.onInvariant = safety.invariantFailure
	counters.safety, counters.messageIntentLimit = safety, cfg.messageIntentLimit()
	cfg.safety = safety
	cfg.events = events
	business := &businessGuard{}
	metrics.onBusiness = business.observe
	lifecycle := newRunLifecycle(ctx, safety, events)
	defer lifecycle.close()
	if cfg.observeLifecycle != nil {
		cfg.observeLifecycle(lifecycle)
	}
	resources, resourceErr := newResourceGuard(observationCtx, cfg.ResourceLimits, cfg.ReportPath, cfg.EventsPath, cfg.ManifestPath, safety, events, business)
	defer resources.close()
	if resourceErr != nil {
		return nil, resourceErr
	}
	environment, environmentErr := newEnvironmentGuard(observationCtx, cfg.EnvironmentPath, cfg.ServerMetricsTargets, safety, events)
	defer environment.close()
	if environmentErr != nil {
		return nil, environmentErr
	}
	if cfg.observeEvents != nil {
		cfg.observeEvents(events)
	}
	serverMetrics := newServerMetricsCollector(cfg.ServerMetricsTargets)
	if serverMetrics != nil {
		serverMetrics.onIssue = safety.metricsFailure
	}
	var baselineServerMetrics map[string]float64
	if serverMetrics != nil {
		if sample, scrapeErr := serverMetrics.scrape(observationCtx); scrapeErr == nil {
			baselineServerMetrics = sample
			events.write(map[string]any{"type": "server_baseline", "at": time.Now().UTC(), "server_metrics": sample, "metrics_targets": serverMetrics.samples()})
		} else {
			events.write(map[string]any{"type": "server_baseline_error", "at": time.Now().UTC(), "class": classifyError(scrapeErr), "metrics_targets": serverMetrics.samples()})
			return nil, fmt.Errorf("metrics preflight failed before load; see events for target evidence")
		}
	}
	if failure := events.report().Error; failure != "" {
		return nil, fmt.Errorf("event preflight failed: %s", failure)
	}
	finishReport := func(report *RunReport) (*RunReport, error) {
		finalCtx, cancelFinal := lifecycle.finalizeContext(observationCtx)
		defer cancelFinal()
		environment.finish(finalCtx)
		resources.finish(finalCtx)
		_ = delivery.finalize(finalCtx)
		lifecycle.close()
		report.Lifecycle = lifecycle.report()
		events.write(map[string]any{"type": "run_finalizing", "lifecycle": report.Lifecycle})
		_ = events.finalize(finalCtx)
		report.FinishedAt = time.Now().UTC()
		report.Delivery = delivery.report()
		report.ResponseBytes = startupResponseBytes(report.BaselineServerMetrics, report.WorkloadEndServerMetrics)
		report.RPCDeliveryOutcomes = startupRPCDeliveryOutcomes(report.BaselineServerMetrics, report.WorkloadEndServerMetrics)
		report.DatabaseWork = startupDatabaseWork(report.BaselineServerMetrics, report.WorkloadEndServerMetrics)
		report.EventsWritten, report.EventsDropped = events.counts()
		report.EventEvidence, report.SafetyStop = events.report(), safety.report()
		report.Resources, report.BusinessGuard = resources.report(), business.report()
		report.Environment = environment.report()
		report.MessageIntentLimit, report.MessageIntentAdmissions = cfg.messageIntentLimit(), counters.messageIntentAdmissions.Load()
		evaluateReport(report, cfg)
		if err := WriteReport(cfg.ReportPath, report); err != nil {
			return nil, err
		}
		return report, nil
	}
	var fixture *downloadFixture
	preparation := FilePreparationReport{Stage: "disabled"}
	if cfg.FileSizeBytes > 0 {
		lifecycle.enter("preparation")
		preparation.Enabled = true
		preparation.StartedAt = time.Now().UTC()
		fixture, err = loadOrCreateDownloadFixture(loadCtx, cfg, manifest, records[0], key, publicKey, metrics, &preparation)
		preparation.FinishedAt = time.Now().UTC()
		if err != nil {
			lifecycle.interrupt()
			preparation.ErrorClass = invariantClass(err)
			if preparation.ErrorClass == "" {
				preparation.ErrorClass = classifyError(err)
			}
			safety.trip("preparation", preparation.Stage+":"+preparation.ErrorClass, preparation.FinishedAt)
			safety.workersStopped()
			events.write(map[string]any{"type": "preparation_finished", "preparation": preparation})
			var final map[string]float64
			if serverMetrics != nil {
				final, _ = serverMetrics.scrape(observationCtx)
			}
			return finishReport(&RunReport{Version: RunReportVersion, StartedAt: preparation.StartedAt, LoadEndedAt: preparation.FinishedAt, Preparation: preparation,
				RequestedDuration: cfg.Duration.String(), RecoveryDuration: cfg.RecoveryDuration.String(), ExpectedSessions: len(records), Operations: metrics.freeze(),
				BaselineServerMetrics: baselineServerMetrics, WorkloadEndServerMetrics: final, FinalServerMetrics: final,
				ServerMetricsTargets: serverMetrics.reports(), ServerMetricsScrapes: serverMetrics.successes(), ServerMetricsErrors: serverMetrics.failures()})
		}
		events.write(map[string]any{"type": "preparation_finished", "preparation": preparation})
	}
	targets := primaryTargets(records)
	workers := make([]*loadWorker, 0, len(records))
	for _, record := range records {
		target := targets[(record.AccountIndex+1)%len(targets)]
		workers = append(workers, newLoadWorker(
			record, target, manifest.Endpoint, publicKey,
			&EncryptedFileStorage{Path: resolveSessionPath(cfg.ManifestPath, record), Key: key},
			metrics, counters, events, cfg.RPCInterval, cfg.MessageInterval, cfg.FileInterval, cfg.OperationTimeout, fixture,
			delivery, cfg.MessageQueueDepth,
		))
		workers[len(workers)-1].wrapInvoker = cfg.wrapInvoker
		workers[len(workers)-1].wrapUpdates = cfg.wrapUpdates
		workers[len(workers)-1].configureClient = cfg.configureClient
		workers[len(workers)-1].allowPlannedOffline = cfg.OfflineFraction > 0
	}
	messageWorkers := primaryWorkers(workers)
	if cfg.observeWorkers != nil {
		cfg.observeWorkers(workers)
	}

	startedAt := time.Now()
	producerDeadline := startedAt.Add(cfg.Duration)
	for _, worker := range workers {
		worker.producerDeadline = producerDeadline
	}
	lifecycle.startWorkload(startedAt, cfg.RampDuration)
	var workerWG sync.WaitGroup
	var clientWG sync.WaitGroup
	for _, worker := range workers {
		workerWG.Add(1)
		clientWG.Add(1)
		go func(w *loadWorker) {
			defer clientWG.Done()
			w.supervise(loadCtx, &workerWG)
		}(worker)
	}
	startOrder := cfg.StartOrder
	if startOrder == "" {
		startOrder = StartupOrderAccountIndex
	}
	startOrderSeed := cfg.StartOrderSeed
	if startOrderSeed == 0 {
		startOrderSeed = 20260827
	}
	launchOrder := startupAccountOrder(len(workers), startOrder, startOrderSeed)
	for position, workerIndex := range launchOrder {
		worker := workers[workerIndex]
		delay := time.Duration(0)
		if len(workers) > 1 {
			delay = time.Duration(position) * cfg.RampDuration / time.Duration(len(workers)-1)
		}
		workerWG.Add(1)
		go func(w *loadWorker, d time.Duration) {
			defer workerWG.Done()
			timer := time.NewTimer(d)
			defer timer.Stop()
			select {
			case <-loadCtx.Done():
			case <-timer.C:
				w.setOnline(true)
			}
		}(worker, delay)
	}

	if cfg.OfflineFraction > 0 {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			runOfflineWindow(loadCtx, workers, cfg.OfflineFraction, cfg.OfflineAt, cfg.OfflineFor, events)
		}()
	}
	messageCtx, stopMessages := context.WithDeadline(loadCtx, producerDeadline)
	defer stopMessages()
	var messageWG sync.WaitGroup
	if cfg.MessageRate > 0 {
		messageWG.Add(1)
		go runFixedMessageSchedule(messageCtx, &messageWG, cfg.RampDuration, cfg.OperationTimeout, cfg.MessageRate, messageWorkers, workers, counters, events)
	}
	loadTimer := time.NewTimer(max(time.Duration(0), time.Until(producerDeadline)))
	defer loadTimer.Stop()
	sampleTicker := time.NewTicker(cfg.SampleInterval)
	peakReady := 0
	steadySamples := 0
	steadyReadySum := 0
	steadyReadyMinimum := len(workers)
	var finalServerMetrics map[string]float64
	loadPhase := false
	scheduledLoadEnd := false
	for {
		select {
		case <-ctx.Done():
			lifecycle.interrupt()
			goto loadFinished
		case <-sampleTicker.C:
			ready := countWorkerState(workers, workerReady)
			peakReady = max(peakReady, ready)
			elapsed := time.Since(startedAt)
			if !loadPhase && elapsed >= cfg.RampDuration {
				lifecycle.enter("load")
				loadPhase = true
			}
			outsideOffline := cfg.OfflineFraction == 0 || elapsed < cfg.OfflineAt || elapsed > cfg.OfflineAt+cfg.OfflineFor+30*time.Second
			if elapsed >= cfg.RampDuration+30*time.Second && outsideOffline {
				steadySamples++
				steadyReadySum += ready
				steadyReadyMinimum = min(steadyReadyMinimum, ready)
			}
			finalServerMetrics = writeSample(observationCtx, events, "load", workers, metrics, counters, serverMetrics)
		case <-loadTimer.C:
			scheduledLoadEnd = true
			goto loadFinished
		case <-safety.stopped:
			goto loadFinished
		}
	}

loadFinished:
	lifecycle.interrupt()
	sampleTicker.Stop()
	stopMessages()
	// Stop traffic generation at the scheduled boundary while retaining update
	// observation and the explicit reconciliation channel. The producer fence
	// is atomic with ticker/new-generation admission; already-admitted producer
	// operations finish before settle starts.
	for _, worker := range workers {
		worker.requestProducerStop()
	}
	producerFenceAt := time.Now().UTC()
	loadEndedAt := producerFenceAt
	if scheduledLoadEnd {
		loadEndedAt = producerDeadline.UTC()
	}
	events.write(map[string]any{"type": "producer_quiesce_begin", "at": producerFenceAt, "scheduled_deadline": producerDeadline.UTC(), "sessions": len(workers)})
	messageWG.Wait()
	producerDrainTimeout := max(runFinalizationTimeout, 3*cfg.OperationTimeout)
	producerDrainCtx, cancelProducerDrain := context.WithTimeout(loadCtx, producerDrainTimeout)
	producerDrainErr := waitWorkerProducerDrain(producerDrainCtx, workers)
	cancelProducerDrain()
	if errors.Is(producerDrainErr, context.DeadlineExceeded) {
		events.write(map[string]any{"type": "producer_quiesce_timeout", "at": time.Now().UTC(), "timeout": producerDrainTimeout.String()})
		safety.trip("lifecycle", "producer_quiesce_timeout", time.Now().UTC())
	}
	events.write(map[string]any{"type": "producer_quiesce_end", "at": time.Now().UTC(), "complete": producerDrainErr == nil, "class": classifyError(producerDrainErr)})
	if cfg.MessageRate > 0 {
		drainCtx, cancelDrain := context.WithTimeout(loadCtx, 2*cfg.OperationTimeout*(1+time.Duration(cfg.MessageQueueDepth)))
		messageDrainErr := waitMessageDrain(drainCtx, counters)
		cancelDrain()
		if messageDrainErr != nil && loadCtx.Err() == nil {
			events.write(map[string]any{"type": "message_drain_timeout", "at": time.Now().UTC(), "class": classifyError(messageDrainErr)})
			safety.trip("workload", "message_drain_timeout", time.Now().UTC())
		}
	}
	if loadCtx.Err() == nil && cfg.DeliverySettle > 0 && delivery.report().Ledger.Error == "" && delivery.report().Expected > delivery.report().Delivered {
		lifecycle.enter("settle")
		settleTimer := time.NewTimer(cfg.DeliverySettle)
		settleTicker := time.NewTicker(min(cfg.SampleInterval, time.Second))
	settling:
		for {
			select {
			case <-safety.stopped:
				settleTimer.Stop()
				settleTicker.Stop()
				break settling
			case <-ctx.Done():
				settleTimer.Stop()
				settleTicker.Stop()
				lifecycle.interrupt()
				break settling
			case <-settleTicker.C:
				if current := delivery.report(); current.Missing == 0 {
					settleTimer.Stop()
					settleTicker.Stop()
					break settling
				}
			case <-settleTimer.C:
				settleTicker.Stop()
				break settling
			}
		}
	}
	if current := delivery.report(); loadCtx.Err() == nil && current.Ledger.Error == "" && current.AttemptedMessages > 0 {
		lifecycle.enter("reconcile")
		reconcileCtx, cancelReconcile := context.WithTimeout(loadCtx, cfg.OperationTimeout*2)
		reconcileErr := reconcileDeliveries(reconcileCtx, workers)
		cancelReconcile()
		if reconcileErr != nil && loadCtx.Err() == nil {
			safety.trip("delivery_invariant", "reconciliation_incomplete", time.Now().UTC())
		}
	}
	// Close the remaining observation admission atomically and let every
	// already-admitted logical operation and update callback finish. The
	// transports remain connected but parked until the client operation cut and
	// individually timestamped server scrapes have been captured.
	events.write(map[string]any{"type": "workload_quiesce_begin", "at": time.Now().UTC(), "sessions": len(workers)})
	for _, worker := range workers {
		worker.requestWorkStop()
	}
	drainTimeout := max(runFinalizationTimeout, 3*cfg.OperationTimeout)
	drainCtx, cancelWorkDrain := context.WithTimeout(loadCtx, drainTimeout)
	drainErr := waitWorkerWorkDrain(drainCtx, workers)
	cancelWorkDrain()
	if errors.Is(drainErr, context.DeadlineExceeded) {
		events.write(map[string]any{"type": "workload_quiesce_timeout", "at": time.Now().UTC(), "timeout": drainTimeout.String()})
		safety.trip("lifecycle", "workload_quiesce_timeout", time.Now().UTC())
	}
	normalWorkloadCut := drainErr == nil && loadCtx.Err() == nil
	finalReady := countWorkerState(workers, workerReady)
	events.write(map[string]any{"type": "workload_quiesce_end", "at": time.Now().UTC(), "complete": drainErr == nil, "class": classifyError(drainErr)})

	var workloadEndServerMetrics map[string]float64
	var workloadEndOperations map[string]OperationReport
	captureWorkloadEnd := func() {
		if serverMetrics != nil {
			if sample, scrapeErr := serverMetrics.scrape(observationCtx); scrapeErr == nil {
				serverMetrics.markWorkloadEnd()
				workloadEndServerMetrics = sample
				finalServerMetrics = sample
				events.write(map[string]any{"type": "server_workload_end", "at": time.Now().UTC(), "server_metrics": sample, "metrics_targets": serverMetrics.samples()})
			} else {
				events.write(map[string]any{"type": "server_workload_end_error", "at": time.Now().UTC(), "class": classifyError(scrapeErr), "metrics_targets": serverMetrics.samples()})
			}
		}
		workloadEndOperations = metrics.freeze()
	}
	if normalWorkloadCut {
		captureWorkloadEnd()
		// A metrics target or evidence guard may trip safety during capture.
		// Reclassify that path before teardown rather than publishing it as a
		// clean workload cut based only on the pre-scrape state.
		normalWorkloadCut = loadCtx.Err() == nil
	}
	for _, worker := range workers {
		worker.requestGracefulStop()
	}
	clientWG.Wait()
	stopLoad()
	workerWG.Wait()
	safety.workersStopped()
	// A pre-existing safety stop or a quiesce timeout has already canceled
	// clients. In that failure path, join first and then take an honest terminal
	// cut that includes every cancellation outcome; the report remains failed.
	if !normalWorkloadCut {
		captureWorkloadEnd()
	}
	if finalReady > peakReady {
		peakReady = finalReady
	}

	if cfg.RecoveryDuration > 0 && ctx.Err() == nil {
		lifecycle.enter("recovery")
		lifecycle.recovery(true, false)
		recoveryDeadline := time.NewTimer(cfg.RecoveryDuration)
		recoveryTicker := time.NewTicker(cfg.SampleInterval)
		for {
			select {
			case <-ctx.Done():
				recoveryTicker.Stop()
				recoveryDeadline.Stop()
				lifecycle.interrupt()
				lifecycle.recovery(false, false)
				goto recoveryFinished
			case <-recoveryTicker.C:
				finalServerMetrics = writeSample(observationCtx, events, "recovery", workers, metrics, counters, serverMetrics)
			case <-recoveryDeadline.C:
				recoveryTicker.Stop()
				lifecycle.recovery(false, true)
				goto recoveryFinished
			}
		}
	}

recoveryFinished:
	if serverMetrics != nil {
		if sample, scrapeErr := serverMetrics.scrape(observationCtx); scrapeErr == nil {
			finalServerMetrics = sample
		} else {
			finalServerMetrics = nil
		}
	}
	steadyRatio := float64(0)
	if steadySamples > 0 {
		steadyRatio = float64(steadyReadySum) / float64(steadySamples*len(workers))
	}
	report := &RunReport{
		Version: RunReportVersion, WorkloadStarted: true, Preparation: preparation, StartedAt: startedAt.UTC(), LoadEndedAt: loadEndedAt, FinishedAt: time.Now().UTC(),
		StartOrder: startOrder, StartOrderSeed: startOrderSeed,
		RequestedDuration: cfg.Duration.String(), RecoveryDuration: cfg.RecoveryDuration.String(),
		ExpectedSessions: len(workers), PeakReadySessions: peakReady, FinalReadySessions: finalReady,
		ConnectionAttempts: counters.connectionAttempts.Load(), Reconnects: counters.reconnects.Load(),
		Disconnects: counters.disconnects.Load(), UpdatesReceived: counters.updates.Load(), DownloadedBytes: counters.downloadBytes.Load(),
		WorkerFatalErrors: counters.fatalErrors.Load(), Operations: workloadEndOperations,
		BaselineServerMetrics: baselineServerMetrics, WorkloadEndServerMetrics: workloadEndServerMetrics, FinalServerMetrics: finalServerMetrics,
		ServerMetricsScrapes: serverMetrics.successes(), ServerMetricsErrors: serverMetrics.failures(), ServerMetricsTargets: serverMetrics.reports(),
		SteadySamples: steadySamples, SteadyReadyRatio: steadyRatio, MinSteadyReadySessions: steadyReadyMinimum,
		MessageRatePerSecond: cfg.MessageRate, MessageScheduled: counters.messageScheduled.Load(),
		MessageEnqueued: counters.messageEnqueued.Load(), MessageCompleted: counters.messageCompleted.Load(), MessageQueueFull: counters.messageQueueFull.Load(),
		MessageTiming:   MessageTimingReport{SchedulerLag: counters.schedulerLag.report(), SenderQueueWait: counters.senderQueueWait.report(), PlannedToSend: counters.plannedToSend.report()},
		MessageNotReady: counters.messageNotReady.Load(), Delivery: delivery.report(),
		MessageReadiness: counters.messageReadiness.report(), MessageReadyAt: counters.messageReadyAt,
	}
	return finishReport(report)
}

func primaryTargets(records []SessionRecord) []SessionRecord {
	byAccount := make(map[int]SessionRecord)
	for _, record := range records {
		if existing, ok := byAccount[record.AccountIndex]; !ok || record.DeviceIndex < existing.DeviceIndex {
			byAccount[record.AccountIndex] = record
		}
	}
	maxAccount := -1
	for account := range byAccount {
		maxAccount = max(maxAccount, account)
	}
	targets := make([]SessionRecord, maxAccount+1)
	for account, record := range byAccount {
		targets[account] = record
	}
	return targets
}

func newLoadRunID() (string, error) {
	var value [8]byte
	if _, err := cryptorand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func primaryWorkers(workers []*loadWorker) []*loadWorker {
	primary := make([]*loadWorker, 0, len(workers))
	for _, worker := range workers {
		if worker.record.DeviceIndex == 0 && worker.target.UserID > 0 {
			primary = append(primary, worker)
		}
	}
	return primary
}

func waitMessageReadiness(ctx context.Context, workers []*loadWorker, timeout time.Duration) error {
	if len(workers) == 0 || timeout <= 0 {
		return errors.New("message readiness requires selected devices and a timeout")
	}
	readyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := readyCtx.Err(); err != nil {
			return err
		}
		ready := true
		for _, worker := range workers {
			if worker.state.Load() != workerReady || !worker.businessReady.Load() {
				ready = false
				break
			}
		}
		if ready {
			return nil
		}
		select {
		case <-readyCtx.Done():
			return readyCtx.Err()
		case <-ticker.C:
		}
	}
}

func runFixedMessageSchedule(ctx context.Context, wg *sync.WaitGroup, startDelay, readyTimeout time.Duration, rate float64, workers, allDevices []*loadWorker, counters *harnessCounters, events *eventWriter) {
	defer wg.Done()
	if rate <= 0 || len(workers) == 0 {
		return
	}
	startTimer := time.NewTimer(startDelay)
	defer startTimer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-startTimer.C:
	}
	readyStart := time.Now()
	err := waitMessageReadiness(ctx, allDevices, readyTimeout)
	counters.messageReadiness.observe(readyStart, err)
	if err != nil {
		events.write(map[string]any{"type": "fixed_message_readiness_failed", "at": time.Now().UTC(), "class": classifyError(err)})
		if ctx.Err() == nil && counters.safety != nil {
			counters.safety.trip("workload", "business_readiness_failed", time.Now().UTC())
		}
		return
	}
	interval := time.Duration(float64(time.Second) / rate)
	if interval < time.Microsecond {
		interval = time.Microsecond
	}
	next := time.Now()
	counters.messageReadyAt = next.UTC()
	events.write(map[string]any{"type": "fixed_message_rate_start", "at": next.UTC(), "rate_per_second": rate, "senders": len(workers), "ready_devices": len(allDevices)})
	defer func() {
		events.write(map[string]any{"type": "fixed_message_rate_stop", "at": time.Now().UTC(),
			"scheduled": counters.messageScheduled.Load(), "enqueued": counters.messageEnqueued.Load(),
			"queue_full": counters.messageQueueFull.Load(), "not_ready": counters.messageNotReady.Load()})
	}()
	deadline, bounded := ctx.Deadline()
	workerIndex := 0
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if ctx.Err() != nil || (bounded && !next.Before(deadline)) {
				return
			}
			worker := workers[workerIndex]
			workerIndex = (workerIndex + 1) % len(workers)
			arrival := messageArrival{plannedAt: next, enqueuedAt: time.Now()}
			counters.schedulerLag.observeDuration(arrival.enqueuedAt.Sub(arrival.plannedAt), nil)
			counters.messageScheduled.Add(1)
			if worker.state.Load() != workerReady || !worker.businessReady.Load() {
				counters.messageNotReady.Add(1)
				if counters.safety != nil && !(worker.allowPlannedOffline && !worker.desired.Load()) {
					counters.safety.trip("workload", "sender_not_ready", time.Now().UTC())
				}
			} else {
				select {
				case worker.sendQueue <- arrival:
					counters.messageEnqueued.Add(1)
				default:
					counters.messageQueueFull.Add(1)
					if counters.safety != nil {
						counters.safety.trip("workload", "sender_queue_full", time.Now().UTC())
					}
				}
			}
			next = next.Add(interval)
			timer.Reset(max(time.Until(next), time.Duration(0)))
		}
	}
}

func reconcileDeliveries(ctx context.Context, workers []*loadWorker) error {
	waits := make([]chan struct{}, 0, len(workers))
	for _, worker := range workers {
		if worker.state.Load() != workerReady || !worker.businessReady.Load() {
			return errors.New("delivery reconciliation device is not business ready")
		}
		done := make(chan struct{})
		select {
		case <-ctx.Done():
			return ctx.Err()
		case worker.reconcile <- done:
			waits = append(waits, done)
		}
	}
	for _, done := range waits {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
		}
	}

	return nil
}

func waitWorkerWorkDrain(ctx context.Context, workers []*loadWorker) error {
	for _, worker := range workers {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-worker.workDrained:
		}
	}
	return nil
}

func waitWorkerProducerDrain(ctx context.Context, workers []*loadWorker) error {
	for _, worker := range workers {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-worker.producerDrained:
		}
	}
	return nil
}

func waitMessageDrain(ctx context.Context, counters *harnessCounters) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for counters.messageCompleted.Load() < counters.messageEnqueued.Load() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	return nil
}

func minimumOpenFiles(sessions int) int {
	if sessions < 0 {
		sessions = 0
	}
	// gotd may transiently hold primary, PFS and replacement sockets together;
	// retain fixed room for the process, resolver, event/report files and scrapes.
	return 256 + sessions*6
}

func runOfflineWindow(ctx context.Context, workers []*loadWorker, fraction float64, at, duration time.Duration, events *eventWriter) {
	timer := time.NewTimer(at)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}
	var candidates []*loadWorker
	for _, worker := range workers {
		if worker.record.DeviceIndex > 0 {
			candidates = append(candidates, worker)
		}
	}
	count := int(math.Ceil(float64(len(candidates)) * fraction))
	selected := make([]*loadWorker, 0, count)
	for i, worker := range candidates {
		if i < count {
			worker.setOnline(false)
			selected = append(selected, worker)
		}
	}
	events.write(map[string]any{"type": "offline_start", "at": time.Now().UTC(), "sessions": len(selected)})
	timer.Reset(duration)
	select {
	case <-ctx.Done():
		return
	case <-timer.C:
	}
	for _, worker := range selected {
		worker.setOnline(true)
	}
	events.write(map[string]any{"type": "offline_end", "at": time.Now().UTC(), "sessions": len(selected)})
}

func countWorkerState(workers []*loadWorker, state int32) int {
	count := 0
	for _, worker := range workers {
		if worker.state.Load() == state {
			count++
		}
	}
	return count
}

func writeSample(ctx context.Context, events *eventWriter, phase string, workers []*loadWorker, metrics *metricSet, counters *harnessCounters, server *serverMetricsCollector) map[string]float64 {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	serverValues, scrapeErr := server.scrape(ctx)
	value := map[string]any{
		"type": "sample", "phase": phase, "at": time.Now().UTC(),
		"workers": map[string]int{
			"connecting": countWorkerState(workers, workerConnecting), "ready": countWorkerState(workers, workerReady),
			"disconnected": countWorkerState(workers, workerDisconnected), "offline": countWorkerState(workers, workerOffline),
			"stopped": countWorkerState(workers, workerStopped),
		},
		"client_runtime": map[string]uint64{"goroutines": uint64(runtime.NumGoroutine()), "heap_alloc_bytes": mem.HeapAlloc, "sys_bytes": mem.Sys},
		"connections": map[string]uint64{
			"attempts": counters.connectionAttempts.Load(), "reconnects": counters.reconnects.Load(),
			"disconnects": counters.disconnects.Load(), "fatal_errors": counters.fatalErrors.Load(),
			"downloaded_bytes": counters.downloadBytes.Load(),
		},
		"operations": metrics.report(), "server_metrics": serverValues, "metrics_targets": server.samples(),
	}
	if scrapeErr != nil {
		value["server_metrics_error"] = classifyError(scrapeErr)
	}
	events.write(value)
	return serverValues
}

func evaluateReport(report *RunReport, cfg RunConfig) {
	if report.Version >= 16 {
		if report.Lifecycle.FinalizationAt.IsZero() || report.Lifecycle.FinalizationMS != runFinalizationTimeout.Milliseconds() {
			report.Failures = append(report.Failures, "run lifecycle finalization evidence missing")
		}
		if report.Lifecycle.Interrupted {
			report.Failures = append(report.Failures, "run interrupted during "+report.Lifecycle.InterruptedPhase+": "+report.Lifecycle.InterruptionClass)
		}
		if cfg.RecoveryDuration > 0 && !report.Lifecycle.RecoveryComplete {
			report.Failures = append(report.Failures, "requested recovery observation window was not completed")
		}
	}
	if report.Preparation.ErrorClass != "" || (cfg.FileSizeBytes > 0 && (!report.WorkloadStarted || report.Preparation.Stage != "complete")) {
		report.Failures = append(report.Failures, "file preparation did not complete; workload admission denied")
	}
	if report.Version >= 14 && (report.Environment.Version != environmentVersion || (report.Environment.Enabled && (report.Environment.Samples == 0 || report.Environment.Errors > 0))) {
		report.Failures = append(report.Failures, "external environment evidence incomplete")
	}
	if report.Version >= 13 {
		if report.Resources.Version != 1 || report.Resources.Samples == 0 || report.Resources.Errors != 0 {
			report.Failures = append(report.Failures, "client resource evidence incomplete")
		}
		if report.MessageIntentLimit == 0 || report.MessageIntentAdmissions > report.MessageIntentLimit || report.MessageIntentAdmissions != report.Delivery.AttemptedMessages {
			report.Failures = append(report.Failures, "message intent budget/accounting mismatch")
		}
	}
	if report.Version >= 12 {
		report.Failures = append(report.Failures, eventEvidenceFailures(report.EventEvidence, report.EventsWritten, report.EventsDropped)...)
	}
	if report.SafetyStop.Source != "" {
		report.Failures = append(report.Failures, "load stopped by safety boundary: "+report.SafetyStop.Source+"/"+report.SafetyStop.Class)
	}
	if report.Version >= 11 || cfg.MessageRate > 0 || cfg.MessageInterval > 0 {
		ledger := report.Delivery.Ledger
		if ledger.Version != deliveryLedgerVersion || !ledger.AuditComplete || ledger.Error != "" || ledger.AuditRecords != report.Delivery.AttemptedMessages {
			report.Failures = append(report.Failures, "delivery ledger evidence is incomplete or failed its final audit")
		}
	}
	requiredReady := int(math.Ceil(float64(report.ExpectedSessions) * cfg.MinimumReadyRatio))
	if report.PeakReadySessions < requiredReady {
		report.Failures = append(report.Failures, fmt.Sprintf("peak ready sessions %d below required %d", report.PeakReadySessions, requiredReady))
	}
	if report.SteadySamples == 0 {
		report.Failures = append(report.Failures, "no post-ramp steady-state samples were collected")
	} else if report.SteadyReadyRatio < cfg.MinimumReadyRatio {
		report.Failures = append(report.Failures, fmt.Sprintf("steady ready ratio %.4f below required %.4f", report.SteadyReadyRatio, cfg.MinimumReadyRatio))
	}
	if report.WorkerFatalErrors > 0 {
		report.Failures = append(report.Failures, fmt.Sprintf("worker fatal errors: %d", report.WorkerFatalErrors))
	}
	if cfg.MessageRate > 0 {
		if report.MessageReadiness.Count != 1 || report.MessageReadiness.Errors > 0 || report.MessageReadiness.Canceled > 0 || report.MessageReadyAt.IsZero() {
			report.Failures = append(report.Failures, "fixed-rate workload did not pass all-device business readiness")
		}
		if report.MessageScheduled == 0 {
			report.Failures = append(report.Failures, "fixed-rate message scheduler produced no arrivals")
		}
		if report.MessageTiming.SchedulerLag.Count != report.MessageScheduled || report.MessageTiming.SenderQueueWait.Count != report.MessageCompleted || report.MessageTiming.PlannedToSend.Count != report.MessageCompleted {
			report.Failures = append(report.Failures, "fixed-rate arrival timing does not cover all scheduled/completed sends")
		}
		latencyExpected := report.Delivery.Delivered
		if report.Version >= 17 {
			latencyExpected = report.Delivery.OnlineLiveDelivered
		}
		if report.Delivery.PlannedArrivalSamples != latencyExpected {
			report.Failures = append(report.Failures, "planned-arrival E2E timing does not cover its declared delivery population")
		}
		if report.MessageNotReady > 0 {
			report.Failures = append(report.Failures, fmt.Sprintf("fixed-rate arrivals rejected because sender was not ready: %d", report.MessageNotReady))
		}
		if report.MessageQueueFull > 0 {
			report.Failures = append(report.Failures, fmt.Sprintf("fixed-rate arrivals rejected by bounded sender queues: %d", report.MessageQueueFull))
		}
		if report.MessageEnqueued != report.MessageScheduled {
			report.Failures = append(report.Failures, fmt.Sprintf("fixed-rate scheduler enqueued %d of %d arrivals", report.MessageEnqueued, report.MessageScheduled))
		}
		if report.MessageCompleted != report.MessageEnqueued {
			report.Failures = append(report.Failures, fmt.Sprintf("message workers completed %d of %d enqueued sends", report.MessageCompleted, report.MessageEnqueued))
		}
		sendOperation := report.Operations["messages.sendMessage"]
		if sendOperation.Count != report.MessageCompleted {
			report.Failures = append(report.Failures, fmt.Sprintf("messages.sendMessage recorded %d of %d completed jobs", sendOperation.Count, report.MessageCompleted))
		}
	}
	if cfg.MessageRate > 0 || cfg.MessageInterval > 0 {
		sendOperation := report.Operations["messages.sendMessage"]
		successfulSends := sendOperation.Count - min(sendOperation.Count, sendOperation.Errors+sendOperation.Canceled)
		if report.Delivery.InitialConfirmed != successfulSends || report.Delivery.AttemptedMessages != sendOperation.Count || report.Delivery.InitialConfirmed+report.Delivery.InitialRejected+report.Delivery.InitialUncertain != sendOperation.Count {
			report.Failures = append(report.Failures, "send intent accounting does not cover every initial RPC outcome")
		}
		retryOperation := report.Operations["messages.sendMessage.retry"]
		retrySuccess := retryOperation.Count - min(retryOperation.Count, retryOperation.Errors+retryOperation.Canceled)
		if report.Delivery.RetryAttempts != retryOperation.Count || report.Delivery.RetryConfirmed != retrySuccess {
			report.Failures = append(report.Failures, "send retry accounting does not match RPC evidence")
		}
		if report.Delivery.PendingMessages > 0 || report.Delivery.UnresolvedMessages > 0 || report.Delivery.NotCommittedMessages > 0 {
			report.Failures = append(report.Failures, fmt.Sprintf("send intents incomplete: pending %d, unresolved %d, not committed %d", report.Delivery.PendingMessages, report.Delivery.UnresolvedMessages, report.Delivery.NotCommittedMessages))
		}
		if report.Delivery.CommittedMessages+report.Delivery.NotCommittedMessages+report.Delivery.UnresolvedMessages != report.Delivery.AttemptedMessages {
			report.Failures = append(report.Failures, "send final classifications do not cover all attempted intents")
		}
		if report.Delivery.InvalidMessageObserved > 0 || report.Delivery.MessageIDConflicts > 0 || report.Delivery.CommitContradictions > 0 {
			report.Failures = append(report.Failures, fmt.Sprintf("invalid commit evidence: message envelope %d, conflicting message IDs %d, rejection/commit contradictions %d", report.Delivery.InvalidMessageObserved, report.Delivery.MessageIDConflicts, report.Delivery.CommitContradictions))
		}
		if report.Delivery.Missing > 0 || report.Delivery.Delivered != report.Delivery.Expected {
			report.Failures = append(report.Failures, fmt.Sprintf("device delivery incomplete: delivered %d of %d, missing %d", report.Delivery.Delivered, report.Delivery.Expected, report.Delivery.Missing))
		}
		if report.Version >= 17 {
			d := report.Delivery
			if d.AttemptedMessages > 0 && d.FinalReconciledDevices != d.SelectedDevices {
				report.Failures = append(report.Failures, "final device difference reconciliation incomplete")
			}
			if d.OnlineExpected+d.OfflineExpected+d.UnavailableExpected != d.Expected || d.OnlineLiveDelivered+d.OnlineMissing != d.OnlineExpected || d.OfflineDelivered > d.OfflineExpected {
				report.Failures = append(report.Failures, "device online expectation accounting incomplete")
			}
			if d.OnlineMissing > 0 || d.UnavailableExpected > 0 || d.StaleObservations > 0 {
				report.Failures = append(report.Failures, fmt.Sprintf("device online expectation violated: online missing %d, unavailable %d, stale observations %d", d.OnlineMissing, d.UnavailableExpected, d.StaleObservations))
			}
		} else if report.Delivery.DifferenceRecovered > 0 {
			report.Failures = append(report.Failures, fmt.Sprintf("online recipients recovered %d messages only through updates.getDifference", report.Delivery.DifferenceRecovered))
		}
		if report.Delivery.DuplicateObservations > 0 {
			report.Failures = append(report.Failures, fmt.Sprintf("recipient observed %d duplicate message updates", report.Delivery.DuplicateObservations))
		}
		if report.Delivery.WrongAccountObserved > 0 {
			report.Failures = append(report.Failures, fmt.Sprintf("load markers appeared on %d wrong recipient accounts", report.Delivery.WrongAccountObserved))
		}
		if report.Delivery.UnknownDeviceObserved > 0 || report.Delivery.OriginLiveObserved > 0 {
			report.Failures = append(report.Failures, fmt.Sprintf("delivery source violation: unknown devices %d, excluded origin live updates %d", report.Delivery.UnknownDeviceObserved, report.Delivery.OriginLiveObserved))
		}
		if report.Delivery.UnmatchedMarkers > 0 {
			report.Failures = append(report.Failures, fmt.Sprintf("observed %d load markers without a registered send intent", report.Delivery.UnmatchedMarkers))
		}
	}
	for name, operation := range report.Operations {
		if operation.FloodWaits > 0 {
			report.Failures = append(report.Failures, fmt.Sprintf("%s returned FLOOD_WAIT %d times", name, operation.FloodWaits))
		}
		unexpectedErrors := operation.Errors
		if cfg.ExpectServerRestart {
			unexpectedErrors -= min(unexpectedErrors, operation.ConnectionErrors)
		}
		if unexpectedErrors > 0 {
			report.Failures = append(report.Failures, fmt.Sprintf("%s returned %d unexpected non-cancel errors", name, unexpectedErrors))
		}
	}
	methods := make([]string, 0, len(report.RPCDeliveryOutcomes))
	for method := range report.RPCDeliveryOutcomes {
		methods = append(methods, method)
	}
	sort.Strings(methods)
	for _, method := range methods {
		outcomes := report.RPCDeliveryOutcomes[method]
		outcomeNames := make([]string, 0, len(outcomes))
		for outcome := range outcomes {
			outcomeNames = append(outcomeNames, outcome)
		}
		sort.Strings(outcomeNames)
		for _, outcome := range outcomeNames {
			if count := outcomes[outcome]; outcome != "ok" && count > 0 {
				report.Failures = append(report.Failures, fmt.Sprintf("%s rpc_result delivery outcome %s: %d", method, outcome, count))
			}
		}
	}
	methods = methods[:0]
	for method := range report.DatabaseWork {
		methods = append(methods, method)
	}
	sort.Strings(methods)
	for _, method := range methods {
		if errors := report.DatabaseWork[method].Errors; errors > 0 {
			report.Failures = append(report.Failures, fmt.Sprintf("%s database errors: %d", method, errors))
		}
	}
	if cfg.ExpectServerRestart && report.Reconnects < uint64(requiredReady) {
		report.Failures = append(report.Failures, fmt.Sprintf("server restart expected at least %d reconnect attempts, observed %d", requiredReady, report.Reconnects))
	}
	if report.FinalServerMetrics != nil && report.BaselineServerMetrics != nil && cfg.RecoveryDuration > 0 {
		checks := []string{
			"telesrv_mtproto_raw_connections", "telesrv_mtproto_logical_sessions",
			"telesrv_mtproto_logical_outbox_bytes", "telesrv_mtproto_pending_push_bytes",
			"telesrv_mtproto_outbound_tracked_bytes", "telesrv_mtproto_rpc_execution_owners",
			"telesrv_mtproto_rpc_execution_reserved_entries", "telesrv_mtproto_rpc_execution_receipts",
			"telesrv_mtproto_rpc_execution_receipt_budget_bytes", "telesrv_mtproto_rpc_execution_subscribers",
		}
		for _, name := range checks {
			baseline := metricValue(report.BaselineServerMetrics, name)
			final := metricValue(report.FinalServerMetrics, name)
			if final > baseline {
				report.Failures = append(report.Failures, fmt.Sprintf("server retained %.0f above baseline %.0f in %s after recovery", final-baseline, baseline, name))
			}
		}
	}
	if len(cfg.ServerMetricsTargets) > 0 && report.ServerMetricsScrapes == 0 {
		report.Failures = append(report.Failures, "server metrics endpoint produced no successful scrapes")
	}
	if len(cfg.ServerMetricsTargets) > 0 && report.FinalServerMetrics == nil {
		report.Failures = append(report.Failures, "final post-recovery server metrics scrape failed")
	}
	if len(cfg.ServerMetricsTargets) > 0 && report.WorkloadEndServerMetrics == nil {
		report.Failures = append(report.Failures, "pre-teardown workload-end server metrics scrape failed")
	}
	if len(cfg.ServerMetricsTargets) > 0 && report.BaselineServerMetrics == nil {
		report.Failures = append(report.Failures, "pre-load server metrics baseline scrape failed")
	}
	report.Failures = append(report.Failures, metricsEvidenceFailures(cfg.ServerMetricsTargets, report.ServerMetricsTargets)...)
	if report.EventsDropped > 0 {
		report.Failures = append(report.Failures, "load evidence events were dropped")
	}
	report.Pass = len(report.Failures) == 0
}

func metricValue(values map[string]float64, name string) float64 {
	// The scraper always stores an aggregate family value in the bare key and
	// may additionally retain bounded state/method label breakdowns. Prefer that
	// aggregate; summing both would double-count every labeled family in resource
	// recovery checks (for example retained/offline logical sessions).
	if value, ok := values[name]; ok {
		return value
	}
	var total float64
	for key, value := range values {
		if strings.HasPrefix(key, name+"{") {
			total += value
		}
	}
	return total
}

func classifyError(err error) string {
	if err == nil {
		return "ok"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	// PathError and LinkError also satisfy net.Error. Preserve the local
	// storage boundary instead of turning failed evidence writes into a
	// transport outage in the report.
	var pathErr *os.PathError
	var linkErr *os.LinkError
	if errors.As(err, &pathErr) || errors.As(err, &linkErr) {
		return "filesystem"
	}
	if errors.Is(err, mtproto.ErrPFSReconnectRequired) || errors.Is(err, mtproto.ErrPFSDropKeysRequired) || errors.Is(err, mtproto.ErrTransportNotReady) {
		return "connection"
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || errors.Is(err, pool.ErrConnDead) || errors.Is(err, tdrpc.ErrEngineClosed) ||
		errors.Is(err, syscall.ECONNABORTED) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.EHOSTUNREACH) || errors.Is(err, syscall.ENETUNREACH) || errors.Is(err, syscall.EPIPE) {
		return "connection"
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return "timeout"
		}
		return "connection"
	}
	message := strings.ToUpper(err.Error())
	switch {
	case strings.Contains(message, "FLOOD_WAIT"):
		return "flood_wait"
	case strings.Contains(message, "ENCRYPTED_MESSAGE_INVALID"):
		return "encrypted_message_invalid"
	case strings.Contains(message, "AUTH_KEY"):
		return "auth_key"
	case strings.Contains(message, "CONNECTION"), strings.Contains(message, "CONNECT"), strings.Contains(message, "EOF"),
		strings.Contains(message, "ENGINE WAS CLOSED"), strings.Contains(message, "BROKEN PIPE"),
		strings.Contains(message, "CLOSED NETWORK"), strings.Contains(message, "NO ROUTE TO HOST"),
		strings.Contains(message, "NETWORK IS UNREACHABLE"):
		return "connection"
	default:
		return "error"
	}
}

// classifyErrorReason intentionally returns a finite vocabulary. It preserves
// enough transport/PFS evidence to diagnose a failed load without persisting
// raw error strings, addresses, auth-key IDs or request payloads.
func classifyErrorReason(err error) string {
	if err == nil {
		return "ok"
	}
	message := strings.ToUpper(err.Error())
	switch {
	case errors.Is(err, mtproto.ErrPFSDropKeysRequired):
		return "pfs_drop_keys"
	case errors.Is(err, mtproto.ErrPFSReconnectRequired):
		return "pfs_reconnect"
	case errors.Is(err, mtproto.ErrTransportNotReady):
		return "transport_not_ready"
	case strings.Contains(message, "TOO MANY OPEN FILES"):
		return "file_descriptor_limit"
	case strings.Contains(message, "PFS RECONNECT"):
		return "pfs_reconnect"
	case strings.Contains(message, "AUTH KEY NOT FOUND"), strings.Contains(message, "AUTH_KEY_NOT_FOUND"), strings.Contains(message, "PROTOCOL ERROR 404"):
		return "auth_key_not_found"
	case strings.Contains(message, "ENCRYPTED_MESSAGE_INVALID"):
		return "encrypted_message_invalid"
	case strings.Contains(message, "FINGERPRINT"):
		return "rsa_fingerprint"
	case strings.Contains(message, "CONNECTION REFUSED"):
		return "connection_refused"
	case strings.Contains(message, "CONNECTION RESET"):
		return "connection_reset"
	case strings.Contains(message, "NETWORK IS UNREACHABLE"), strings.Contains(message, "NO ROUTE TO HOST"):
		return "network_unreachable"
	case strings.Contains(message, "NO SUCH HOST"):
		return "dns"
	case strings.Contains(message, "BROKEN PIPE"):
		return "broken_pipe"
	case strings.Contains(message, "ENDED BEFORE BUSINESS READINESS"):
		return "business_readiness_incomplete"
	case strings.Contains(message, "EOF"):
		return "eof"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return classifyError(err)
	}
}
