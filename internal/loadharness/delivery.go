package loadharness

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/iamxvbaba/td/tg"
)

const deliveryMarkerPrefix = "telesrv-load-v3"

type deliverySource uint8

const (
	deliveryLive deliverySource = 1 << iota
	deliveryDifference
)

type deliveryExpectation struct {
	senderSessionIndex int
	senderUserID       int64
	targetUserID       int64
	devices            []int
	startedAt          time.Time
	plannedAt          time.Time
	committed          bool
	randomID           int64
	initialOutcome     sendAttemptOutcome
	retryOutcome       sendAttemptOutcome
	senderMessageID    int
	recipientMessageID int
	frozenAt           time.Time
	stateRevision      uint64
}

type deliveryObservation struct {
	sources                                               deliverySource
	repeats                                               uint64
	firstAt                                               time.Time
	firstLiveAt                                           time.Time
	mode                                                  deliveryDeviceMode
	expectedGeneration, expectedEpoch                     uint64
	firstGeneration, liveGeneration, differenceGeneration uint64
	firstEpoch, liveEpoch, differenceEpoch                uint64
	onlineLiveAt                                          time.Time
	staleObservations                                     uint64
}

type deliveryTracker struct {
	mu                    sync.Mutex
	availability          map[int]deliveryDeviceState
	stateRevision         uint64
	events                *eventWriter
	finalReconciled       map[int]bool
	runID                 string
	devices               map[int]SessionRecord
	byUser                map[int64][]int
	ledger                *deliveryLedger
	totals                *deliveryTotals
	failed                chan struct{}
	firstError            error
	onFailure             func() // Installed before clients start; only cancels the shared load context.
	onInvariant           func(string)
	finalized             bool
	auditedE2EMax         time.Duration
	auditedPlannedMax     time.Duration
	wrongAccountObserved  uint64
	unknownDeviceObserved uint64
	unmatchedMarkers      uint64
	anomalies             []DeliveryAnomaly
	invalidMessages       uint64
	messageIDConflicts    uint64
	commitContradictions  uint64
}

type DeliveryReport struct {
	FinalReconciledDevices int    `json:"final_reconciled_devices"`
	OnlineExpected         uint64 `json:"online_expected"`
	OfflineExpected        uint64 `json:"offline_expected"`
	UnavailableExpected    uint64 `json:"unavailable_expected"`
	OnlineLiveDelivered    uint64 `json:"online_live_delivered"`
	OnlineMissing          uint64 `json:"online_missing"`
	OfflineDelivered       uint64 `json:"offline_delivered"`
	StaleObservations      uint64 `json:"stale_observations"`

	Anomalies                  []DeliveryAnomaly      `json:"anomalies,omitempty"`
	Ledger                     DeliveryLedgerReport   `json:"ledger"`
	QuantileMethod             string                 `json:"quantile_method"`
	LatencyPopulation          string                 `json:"latency_population"`
	RunID                      string                 `json:"run_id"`
	CommittedMessages          uint64                 `json:"committed_messages"`
	AttemptedMessages          uint64                 `json:"attempted_messages"`
	InitialConfirmed           uint64                 `json:"initial_confirmed"`
	InitialRejected            uint64                 `json:"initial_rejected"`
	InitialUncertain           uint64                 `json:"initial_uncertain"`
	PendingMessages            uint64                 `json:"pending_messages"`
	RetryAttempts              uint64                 `json:"retry_attempts"`
	RetryConfirmed             uint64                 `json:"retry_confirmed"`
	RetryRejected              uint64                 `json:"retry_rejected"`
	RetryUncertain             uint64                 `json:"retry_uncertain"`
	CommittedAfterUncertainty  uint64                 `json:"committed_after_uncertainty"`
	CommittedByObservationOnly uint64                 `json:"committed_by_observation_only"`
	NotCommittedMessages       uint64                 `json:"not_committed_messages"`
	UnresolvedMessages         uint64                 `json:"unresolved_messages"`
	InvalidMessageObserved     uint64                 `json:"invalid_message_observed"`
	MessageIDConflicts         uint64                 `json:"message_id_conflicts"`
	CommitContradictions       uint64                 `json:"commit_contradictions"`
	SelectedAccounts           int                    `json:"selected_accounts"`
	SelectedDevices            int                    `json:"selected_devices"`
	Devices                    []DeviceDeliveryReport `json:"devices"`
	Expected                   uint64                 `json:"expected"`
	Delivered                  uint64                 `json:"delivered"`
	Missing                    uint64                 `json:"missing"`
	LiveDelivered              uint64                 `json:"live_delivered"`
	DifferenceRecovered        uint64                 `json:"difference_recovered"`
	DuplicateObservations      uint64                 `json:"duplicate_observations"`
	WrongAccountObserved       uint64                 `json:"wrong_account_observed"`
	UnknownDeviceObserved      uint64                 `json:"unknown_device_observed"`
	OriginLiveObserved         uint64                 `json:"origin_live_observed"`
	UnmatchedMarkers           uint64                 `json:"unmatched_markers"`
	E2EP50MS                   float64                `json:"e2e_p50_ms"`
	E2EP95MS                   float64                `json:"e2e_p95_ms"`
	E2EP99MS                   float64                `json:"e2e_p99_ms"`
	E2EMaxMS                   float64                `json:"e2e_max_ms"`
	PlannedArrivalSamples      uint64                 `json:"planned_arrival_samples"`
	PlannedArrivalP50MS        float64                `json:"planned_arrival_p50_ms"`
	PlannedArrivalP95MS        float64                `json:"planned_arrival_p95_ms"`
	PlannedArrivalP99MS        float64                `json:"planned_arrival_p99_ms"`
	PlannedArrivalMaxMS        float64                `json:"planned_arrival_max_ms"`
}

// Bounded diagnostic samples; counters still include every invalid observation.
type DeliveryAnomaly struct {
	Kind         string `json:"kind"`
	Marker       string `json:"marker"`
	SessionIndex int    `json:"session_index"`
	MessageID    int    `json:"message_id"`
}

// Indices are dataset-local identities, never auth keys or server user IDs.
type DeviceDeliveryReport struct {
	OnlineExpected      uint64 `json:"online_expected"`
	OfflineExpected     uint64 `json:"offline_expected"`
	UnavailableExpected uint64 `json:"unavailable_expected"`
	OnlineLiveDelivered uint64 `json:"online_live_delivered"`
	OnlineMissing       uint64 `json:"online_missing"`
	OfflineDelivered    uint64 `json:"offline_delivered"`
	StaleObservations   uint64 `json:"stale_observations"`

	SessionIndex          int    `json:"session_index"`
	AccountIndex          int    `json:"account_index"`
	DeviceIndex           int    `json:"device_index"`
	Expected              uint64 `json:"expected"`
	Delivered             uint64 `json:"delivered"`
	Missing               uint64 `json:"missing"`
	LiveDelivered         uint64 `json:"live_delivered"`
	DifferenceRecovered   uint64 `json:"difference_recovered"`
	DuplicateObservations uint64 `json:"duplicate_observations"`
}

func newDeliveryTracker(runID string, records []SessionRecord, path string, options deliveryLedgerOptions) (*deliveryTracker, error) {
	t := &deliveryTracker{runID: runID, devices: make(map[int]SessionRecord, len(records)), byUser: make(map[int64][]int), failed: make(chan struct{}), availability: make(map[int]deliveryDeviceState, len(records))}
	accounts, users := make(map[int]int64), make(map[int64]int)
	pairs := make(map[[2]int]bool)
	if runID == "" || len(runID) > 128 || strings.Contains(runID, "/") || len(records) == 0 || len(records) > 100000 || path == "" {
		return nil, errors.New("delivery tracking requires a run ID, bounded devices and an exclusive ledger path")
	}
	for _, r := range records {
		if r.Index < 0 || int64(r.Index) > math.MaxInt32 || r.AccountIndex < 0 || r.AccountIndex >= len(records) || r.DeviceIndex < 0 || r.UserID <= 0 {
			return nil, fmt.Errorf("delivery device %d has invalid identity", r.Index)
		}
		_, duplicate := t.devices[r.Index]
		user, sameAccount := accounts[r.AccountIndex]
		account, sameUser := users[r.UserID]
		pair := [2]int{r.AccountIndex, r.DeviceIndex}
		if duplicate || pairs[pair] || (sameAccount && user != r.UserID) || (sameUser && account != r.AccountIndex) {
			return nil, fmt.Errorf("delivery device %d conflicts with selected topology", r.Index)
		}
		t.devices[r.Index] = r
		t.byUser[r.UserID] = append(t.byUser[r.UserID], r.Index)
		accounts[r.AccountIndex], users[r.UserID], pairs[pair] = r.UserID, r.AccountIndex, true
	}
	targets := primaryTargets(records)
	if len(targets) != len(accounts) {
		return nil, errors.New("delivery account indices must be contiguous")
	}
	for _, indices := range t.byUser {
		sort.Ints(indices)
	}
	routes := make([]deliveryRoute, 0, len(targets))
	for _, sender := range targets {
		if sender.UserID == 0 || sender.DeviceIndex != 0 {
			return nil, errors.New("each selected account requires its primary device")
		}
		target := targets[(sender.AccountIndex+1)%len(targets)].UserID
		if replacement, ok := options.Targets[sender.Index]; ok {
			target = replacement
		}
		if len(t.byUser[target]) == 0 {
			return nil, errors.New("delivery target outside selected topology")
		}
		devices := []int{sender.Index}
		for _, index := range t.byUser[target] {
			if index != sender.Index {
				devices = append(devices, index)
			}
		}
		if target != sender.UserID {
			for _, index := range t.byUser[sender.UserID] {
				if index != sender.Index {
					devices = append(devices, index)
				}
			}
		}
		routes = append(routes, deliveryRoute{Sender: sender.Index, Target: target, Devices: devices})
	}
	var err error
	t.ledger, err = newDeliveryLedger(path, runID, time.Now(), t.devices, routes, options)
	if err != nil {
		return nil, err
	}
	t.totals = newDeliveryTotals(runID, t.devices, len(t.byUser))
	return t, nil
}

func (t *deliveryTracker) marker(senderIndex int, sequence uint64) string {
	return fmt.Sprintf("%s/%s/%d/%d", deliveryMarkerPrefix, t.runID, senderIndex, sequence)
}

func (t *deliveryTracker) key(marker string) (deliveryKey, bool) {
	if len(marker) > 256 {
		return deliveryKey{}, false
	}
	parts := strings.Split(marker, "/")
	if len(parts) != 4 || parts[0] != deliveryMarkerPrefix || parts[1] != t.runID {
		return deliveryKey{}, false
	}
	index, err := strconv.Atoi(parts[2])
	if err != nil || index < 0 {
		return deliveryKey{}, false
	}
	sequence, err := strconv.ParseUint(parts[3], 10, 64)
	return deliveryKey{sender: index, sequence: sequence}, err == nil && sequence > 0
}
func (t *deliveryTracker) matches(marker string) bool { _, ok := t.key(marker); return ok }

// Caller holds the tracker lock. No in-memory fallback after an evidence error.
func (t *deliveryTracker) fail(err error) {
	if err != nil && t.firstError == nil {
		t.firstError = err
		t.ledger.stats.Error = err.Error()
		t.ledger.stats.AuditComplete = false
		close(t.failed)
		if t.onFailure != nil {
			t.onFailure()
		}
	}
}
func (t *deliveryTracker) load(key deliveryKey) (*deliveryRecord, bool) {
	if t.firstError != nil || t.finalized {
		return nil, false
	}
	if _, ok := t.ledger.bySender[key.sender]; !ok {
		return nil, false
	}
	record, found, err := t.ledger.get(key)
	if errors.Is(err, errDeliveryQuota) {
		return nil, false
	} // An unregistered far-future marker must not grow the file.
	t.fail(err)
	return record, found && err == nil
}
func (t *deliveryTracker) replace(before, after *deliveryRecord) error {
	if err := t.ledger.put(after); err != nil {
		t.fail(err)
		return err
	}
	if before != nil {
		t.totals.add(before, -1)
	}
	t.totals.add(after, 1)
	return nil
}

func (t *deliveryTracker) begin(marker string, senderSessionIndex int, targetUserID, randomID int64, startedAt, plannedAt time.Time) error {
	if t == nil {
		return errors.New("missing delivery tracker")
	}
	key, ok := t.key(marker)
	if !ok || marker != t.marker(key.sender, key.sequence) || key.sender != senderSessionIndex || randomID == 0 {
		return errors.New("invalid delivery intent")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.firstError != nil {
		return t.firstError
	}
	if t.finalized {
		return errors.New("delivery tracker already finalized")
	}
	route, _, err := t.ledger.address(key)
	if err != nil {
		t.fail(err)
		return err
	}
	if route.Target != targetUserID {
		return errors.New("delivery intent changed its frozen route")
	}
	if _, exists, err := t.ledger.get(key); err != nil {
		t.fail(err)
		return err
	} else if exists {
		return errors.New("delivery marker already registered")
	}
	expectation := deliveryExpectation{senderSessionIndex: senderSessionIndex, senderUserID: t.devices[senderSessionIndex].UserID, targetUserID: targetUserID, randomID: randomID, devices: route.Devices[1:], startedAt: startedAt, plannedAt: plannedAt}
	expectation.frozenAt, expectation.stateRevision = time.Now(), t.stateRevision
	observations := make([]deliveryObservation, len(route.Devices))
	for i, index := range route.Devices {
		state := t.availability[index]
		observations[i] = deliveryObservation{mode: state.mode, expectedGeneration: state.generation, expectedEpoch: state.epoch}
	}
	return t.replace(nil, &deliveryRecord{key: key, expectation: expectation, observations: observations})
}

func (t *deliveryTracker) finish(marker string, outcome sendAttemptOutcome, retry bool, senderMessageID int) {
	if t == nil {
		return
	}
	key, ok := t.key(marker)
	if !ok {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	before, found := t.load(key)
	if !found {
		return
	}
	after := before.clone()
	e := &after.expectation
	if retry {
		e.retryOutcome = outcome
	} else {
		e.initialOutcome = outcome
	}
	if outcome == sendAccepted && t.recordMessageID(e, e.senderUserID, senderMessageID) {
		e.committed = true
	}
	if !retry && outcome == sendRejected && e.committed {
		t.commitContradictions++
		t.invariant("commit_contradiction")
	}
	_ = t.replace(before, after)
}

func (t *deliveryTracker) recordMessageID(e *deliveryExpectation, viewer int64, id int) bool {
	if id <= 0 || int64(id) > math.MaxInt32 {
		t.invalidMessages++
		t.invariant("message_id_invalid")
		return false
	}
	known := &e.recipientMessageID
	if viewer == e.senderUserID {
		known = &e.senderMessageID
	}
	if *known != 0 && *known != id {
		t.messageIDConflicts++
		t.invariant("message_id_conflict")
		return false
	}
	*known = id
	return true
}

func (t *deliveryTracker) observe(message deliveryMessage, sessionIndex int, source deliverySource, generation uint64) {
	if t == nil {
		return
	}
	key, ok := t.key(message.marker)
	if !ok {
		return
	}
	observedAt := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	if message.marker != t.marker(key.sender, key.sequence) {
		t.unmatchedMarkers++
		t.sampleAnomaly("noncanonical_marker", message.marker, sessionIndex, message.id)
		return
	}
	if generation == 0 || (source != deliveryLive && source != deliveryDifference) {
		t.invalidMessages++
		t.invariant("observation_source")
		return
	}
	before, registered := t.load(key)
	if !registered {
		if t.firstError == nil {
			t.unmatchedMarkers++
			t.sampleAnomaly("unregistered_intent", message.marker, sessionIndex, message.id)
		}
		return
	}
	device, known := t.devices[sessionIndex]
	if !known {
		t.unknownDeviceObserved++
		t.sampleAnomaly("unknown_device", message.marker, sessionIndex, message.id)
		return
	}
	e := before.expectation
	if device.UserID != e.senderUserID && device.UserID != e.targetUserID {
		t.wrongAccountObserved++
		t.sampleAnomaly("wrong_account", message.marker, sessionIndex, message.id)
		return
	}
	if !validDeliveryMessage(message, device.UserID, e.senderUserID, e.targetUserID) {
		t.invalidMessages++
		t.sampleAnomaly("message_envelope", message.marker, sessionIndex, message.id)
		return
	}
	after := before.clone()
	if !t.recordMessageID(&after.expectation, device.UserID, message.id) {
		t.sampleAnomaly("message_id_conflict", message.marker, sessionIndex, message.id)
		return
	}
	if sessionIndex != e.senderSessionIndex || source == deliveryDifference {
		if e.initialOutcome == sendRejected && !e.committed {
			t.commitContradictions++
			t.invariant("commit_contradiction")
		}
		after.expectation.committed = true
	}
	if sessionIndex == e.senderSessionIndex && source == deliveryLive {
		t.invariant("origin_live")
	}
	route := t.ledger.routes[t.ledger.bySender[key.sender]]
	for i, index := range route.Devices {
		if index != sessionIndex {
			continue
		}
		observation := &after.observations[i]
		state := t.availability[sessionIndex]
		epoch := uint64(0)
		if state.active && state.generation == generation {
			epoch = state.epoch
		} else {
			observation.staleObservations++
			t.invariant("stale_client_observation")
		}
		if observation.sources == 0 {
			observation.firstAt, observation.firstGeneration, observation.firstEpoch = observedAt, generation, epoch
		}
		if source == deliveryLive && observation.sources&deliveryLive == 0 {
			observation.firstLiveAt, observation.liveGeneration, observation.liveEpoch = observedAt, generation, epoch
		}
		if source == deliveryDifference && observation.sources&deliveryDifference == 0 {
			observation.differenceGeneration, observation.differenceEpoch = generation, epoch
		}
		if source == deliveryLive && observation.mode == deliveryDeviceOnline && state.active && state.mode == deliveryDeviceOnline && generation == observation.expectedGeneration && epoch == observation.expectedEpoch && observation.onlineLiveAt.IsZero() {
			observation.onlineLiveAt = observedAt
		}
		if observation.sources&source != 0 {
			observation.repeats++
		}
		observation.sources |= source
		break
	}
	_ = t.replace(before, after)
}

func (t *deliveryTracker) report() DeliveryReport {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.reportLocked()
}
func (t *deliveryTracker) reportLocked() DeliveryReport {
	report := t.totals.report()
	report.FinalReconciledDevices = len(t.finalReconciled)
	report.Anomalies = append([]DeliveryAnomaly(nil), t.anomalies...)
	report.InvalidMessageObserved = t.invalidMessages
	report.MessageIDConflicts = t.messageIDConflicts
	report.CommitContradictions = t.commitContradictions
	report.WrongAccountObserved = t.wrongAccountObserved
	report.UnknownDeviceObserved = t.unknownDeviceObserved
	report.UnmatchedMarkers = t.unmatchedMarkers
	report.Ledger = t.ledger.stats
	if report.Ledger.AuditComplete {
		report.E2EMaxMS = durationMS(t.auditedE2EMax)
		report.PlannedArrivalMaxMS = durationMS(t.auditedPlannedMax)
		report.E2EP50MS = min(report.E2EP50MS, report.E2EMaxMS)
		report.E2EP95MS = min(report.E2EP95MS, report.E2EMaxMS)
		report.E2EP99MS = min(report.E2EP99MS, report.E2EMaxMS)
		report.PlannedArrivalP50MS = min(report.PlannedArrivalP50MS, report.PlannedArrivalMaxMS)
		report.PlannedArrivalP95MS = min(report.PlannedArrivalP95MS, report.PlannedArrivalMaxMS)
		report.PlannedArrivalP99MS = min(report.PlannedArrivalP99MS, report.PlannedArrivalMaxMS)
	}
	return report
}

func (t *deliveryTracker) sampleAnomaly(kind, marker string, device, id int) {
	t.invariant(kind)
	if len(t.anomalies) < 16 {
		t.anomalies = append(t.anomalies, DeliveryAnomaly{Kind: kind, Marker: marker, SessionIndex: device, MessageID: id})
	}
}

func (t *deliveryTracker) invariant(class string) {
	if t.onInvariant != nil {
		t.onInvariant(class)
	}
}

func (t *deliveryTracker) finalize(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finalized {
		return t.firstError
	}
	t.finalized = true
	audit := newDeliveryTotals(t.runID, t.devices, len(t.byUser))
	if t.firstError == nil {
		err := t.ledger.audit(ctx, func(record *deliveryRecord) {
			audit.add(record, 1)
			record.latencies(func(e2e, planned time.Duration) {
				t.auditedE2EMax = max(t.auditedE2EMax, e2e)
				t.auditedPlannedMax = max(t.auditedPlannedMax, planned)
			})
		})
		t.fail(err)
		if err == nil && !reflect.DeepEqual(t.totals, audit) {
			t.fail(errors.New("delivery ledger audit disagrees with online totals"))
		}
	}
	t.fail(t.ledger.close())
	if t.firstError == nil {
		t.ledger.stats.AuditComplete = true
	}
	data, err := json.MarshalIndent(t.reportLocked(), "", "  ")
	if err == nil {
		err = writeFileAtomic(filepath.Join(t.ledger.dir, "audit.json"), append(data, '\n'), 0o600)
	}
	t.fail(err)
	return t.firstError
}
func (t *deliveryTracker) close() error { t.mu.Lock(); defer t.mu.Unlock(); return t.ledger.close() }

func (t *deliveryTracker) preflight(cfg RunConfig) error {
	cycles := float64(0)
	if cfg.MessageRate > 0 {
		cycles = math.Ceil(cfg.MessageRate*cfg.Duration.Seconds()/float64(len(t.ledger.routes))) + 1
	}
	if cfg.MessageInterval > 0 {
		cycles = math.Ceil(float64(cfg.Duration)/float64(cfg.MessageInterval)) + 1
	}
	if cycles > float64(t.ledger.stats.LimitBytes/t.ledger.stride) {
		return errors.New("planned message load exceeds delivery ledger disk quota")
	}
	if cycles*float64(len(t.ledger.routes)) > float64(cfg.messageIntentLimit()) {
		return errors.New("planned message load exceeds total intent budget")
	}
	return nil
}

func observeUpdatesClass(tracker *deliveryTracker, sessionIndex int, updates tg.UpdatesClass, source deliverySource, generation uint64) {
	switch value := updates.(type) {
	case *tg.Updates:
		observeUpdateClasses(tracker, sessionIndex, value.Updates, source, generation)
	case *tg.UpdatesCombined:
		observeUpdateClasses(tracker, sessionIndex, value.Updates, source, generation)
	case *tg.UpdateShort:
		observeUpdateClass(tracker, sessionIndex, value.Update, source, generation)
	case *tg.UpdateShortMessage:
		tracker.observe(deliveryMessage{marker: value.Message, id: value.ID, out: value.Out, peerUserID: value.UserID}, sessionIndex, source, generation)
	}
}

func observeUpdateClasses(tracker *deliveryTracker, sessionIndex int, updates []tg.UpdateClass, source deliverySource, generation uint64) {
	for _, update := range updates {
		observeUpdateClass(tracker, sessionIndex, update, source, generation)
	}
}

func observeUpdateClass(tracker *deliveryTracker, sessionIndex int, update tg.UpdateClass, source deliverySource, generation uint64) {
	switch value := update.(type) {
	case *tg.UpdateNewMessage:
		observeMessageClass(tracker, sessionIndex, value.Message, source, generation)
	case *tg.UpdateNewChannelMessage:
		observeMessageClass(tracker, sessionIndex, value.Message, source, generation)
	}
}

func observeMessageClasses(tracker *deliveryTracker, sessionIndex int, messages []tg.MessageClass, source deliverySource, generation uint64) {
	for _, message := range messages {
		observeMessageClass(tracker, sessionIndex, message, source, generation)
	}
}

func observeMessageClass(tracker *deliveryTracker, sessionIndex int, message tg.MessageClass, source deliverySource, generation uint64) {
	if value, ok := message.(*tg.Message); ok {
		tracker.observe(privateDeliveryMessage(value), sessionIndex, source, generation)
	}
}
