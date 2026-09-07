package loadharness

import (
	"math"
	"math/bits"
	"sort"
	"time"
)

// Fixed memory, reversible counts. Quantiles are upper bounds with 32 buckets
// per power of two after the first 64 microseconds; raw times remain on disk.
type deliveryHistogram struct {
	counts [2048]uint64
	count  uint64
}

func deliveryBucket(d time.Duration) int {
	us := uint64(d / time.Microsecond)
	if d%time.Microsecond != 0 {
		us++
	}
	if us < 64 {
		return int(us)
	}
	exponent := bits.Len64(us) - 1
	return 64 + (exponent-6)*32 + int((us>>uint(exponent-5))-32)
}
func deliveryBucketUpper(index int) time.Duration {
	if index < 64 {
		return time.Duration(index) * time.Microsecond
	}
	exponent := (index-64)/32 + 6
	base := uint64((index-64)%32 + 32)
	us := ((base + 1) << uint(exponent-5)) - 1
	if us > uint64(math.MaxInt64/int64(time.Microsecond)) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(us) * time.Microsecond
}
func adjustCount(target *uint64, value uint64, sign int) {
	if sign > 0 {
		*target += value
	} else {
		*target -= value
	}
}
func (h *deliveryHistogram) add(value time.Duration, sign int) {
	if value < 0 {
		return
	}
	adjustCount(&h.counts[deliveryBucket(value)], 1, sign)
	adjustCount(&h.count, 1, sign)
}
func (h *deliveryHistogram) quantile(q float64) float64 {
	if h.count == 0 {
		return 0
	}
	target := uint64(math.Ceil(float64(h.count) * q))
	var total uint64
	for index, count := range h.counts {
		total += count
		if total >= target {
			return durationMS(deliveryBucketUpper(index))
		}
	}
	return durationMS(time.Duration(math.MaxInt64))
}

type deliveryTotals struct {
	value           DeliveryReport
	devicePositions map[int]int
	e2e, planned    deliveryHistogram
}

func newDeliveryTotals(runID string, records map[int]SessionRecord, accounts int) *deliveryTotals {
	t := &deliveryTotals{value: DeliveryReport{RunID: runID, SelectedAccounts: accounts, SelectedDevices: len(records)}, devicePositions: make(map[int]int, len(records))}
	indices := make([]int, 0, len(records))
	for index := range records {
		indices = append(indices, index)
	}
	sort.Ints(indices)
	for position, index := range indices {
		r := records[index]
		t.devicePositions[index] = position
		t.value.Devices = append(t.value.Devices, DeviceDeliveryReport{SessionIndex: index, AccountIndex: r.AccountIndex, DeviceIndex: r.DeviceIndex})
	}
	return t
}

func (r *deliveryRecord) latencies(consume func(time.Duration, time.Duration)) {
	if !r.expectation.committed {
		return
	}
	for _, observation := range r.observations[1:] {
		if observation.onlineLiveAt.IsZero() {
			continue
		}
		at := observation.onlineLiveAt
		e2e, planned := time.Duration(-1), time.Duration(-1)
		if !r.expectation.startedAt.IsZero() && !at.Before(r.expectation.startedAt) {
			e2e = at.Sub(r.expectation.startedAt)
		}
		if !r.expectation.plannedAt.IsZero() && !at.Before(r.expectation.plannedAt) {
			planned = at.Sub(r.expectation.plannedAt)
		}
		consume(e2e, planned)
	}
}

func (t *deliveryTotals) add(record *deliveryRecord, sign int) {
	r, e := &t.value, record.expectation
	add := func(value *uint64) { adjustCount(value, 1, sign) }
	add(&r.AttemptedMessages)
	switch e.initialOutcome {
	case sendAccepted:
		add(&r.InitialConfirmed)
	case sendRejected:
		add(&r.InitialRejected)
	case sendUncertain:
		add(&r.InitialUncertain)
	default:
		add(&r.PendingMessages)
	}
	if e.retryOutcome != sendPending {
		add(&r.RetryAttempts)
		switch e.retryOutcome {
		case sendAccepted:
			add(&r.RetryConfirmed)
		case sendRejected:
			add(&r.RetryRejected)
		case sendUncertain:
			add(&r.RetryUncertain)
		}
	}
	if !e.committed {
		if e.initialOutcome == sendRejected && e.retryOutcome == sendPending {
			add(&r.NotCommittedMessages)
		} else {
			add(&r.UnresolvedMessages)
		}
	} else {
		add(&r.CommittedMessages)
		if e.initialOutcome == sendUncertain {
			add(&r.CommittedAfterUncertainty)
		}
		if e.initialOutcome != sendAccepted && e.retryOutcome != sendAccepted {
			add(&r.CommittedByObservationOnly)
		}
		for position, index := range e.devices {
			device := &r.Devices[t.devicePositions[index]]
			observation := record.observations[position+1]
			add(&r.Expected)
			add(&device.Expected)
			switch observation.mode {
			case deliveryDeviceOnline:
				add(&r.OnlineExpected)
				add(&device.OnlineExpected)
				if observation.onlineLiveAt.IsZero() {
					add(&r.OnlineMissing)
					add(&device.OnlineMissing)
				} else {
					add(&r.OnlineLiveDelivered)
					add(&device.OnlineLiveDelivered)
				}
			case deliveryDeviceOffline:
				add(&r.OfflineExpected)
				add(&device.OfflineExpected)
				if observation.sources != 0 {
					add(&r.OfflineDelivered)
					add(&device.OfflineDelivered)
				}
			default:
				add(&r.UnavailableExpected)
				add(&device.UnavailableExpected)
			}
			adjustCount(&r.StaleObservations, observation.staleObservations, sign)
			adjustCount(&device.StaleObservations, observation.staleObservations, sign)
			if observation.sources == 0 {
				add(&r.Missing)
				add(&device.Missing)
				continue
			}
			add(&r.Delivered)
			add(&device.Delivered)
			if observation.sources&deliveryLive != 0 {
				add(&r.LiveDelivered)
				add(&device.LiveDelivered)
			} else {
				add(&r.DifferenceRecovered)
				add(&device.DifferenceRecovered)
			}
			adjustCount(&r.DuplicateObservations, observation.repeats, sign)
			adjustCount(&device.DuplicateObservations, observation.repeats, sign)
		}
	}
	if record.observations[0].sources&deliveryLive != 0 {
		add(&r.OriginLiveObserved)
	}
	record.latencies(func(e2e, planned time.Duration) { t.e2e.add(e2e, sign); t.planned.add(planned, sign) })
}

func (t *deliveryTotals) report() DeliveryReport {
	r := t.value
	r.Devices = append([]DeviceDeliveryReport(nil), t.value.Devices...)
	r.QuantileMethod = "log2 upper bounds: 32 buckets/octave, 1us resolution; final audited max exact"
	r.LatencyPopulation = "online_expectation_satisfied_live"
	r.E2EP50MS = t.e2e.quantile(.5)
	r.E2EP95MS = t.e2e.quantile(.95)
	r.E2EP99MS = t.e2e.quantile(.99)
	r.E2EMaxMS = t.e2e.quantile(1)
	r.PlannedArrivalSamples = t.planned.count
	r.PlannedArrivalP50MS = t.planned.quantile(.5)
	r.PlannedArrivalP95MS = t.planned.quantile(.95)
	r.PlannedArrivalP99MS = t.planned.quantile(.99)
	r.PlannedArrivalMaxMS = t.planned.quantile(1)
	return r
}
