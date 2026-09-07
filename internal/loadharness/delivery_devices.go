package loadharness

import (
	"fmt"
	"github.com/iamxvbaba/td/tg"
	"time"
)

type deliveryDeviceMode uint8

const (
	deliveryDeviceUnavailable deliveryDeviceMode = iota
	deliveryDeviceOnline
	deliveryDeviceOffline
)

type deliveryDeviceState struct {
	mode              deliveryDeviceMode
	generation, epoch uint64
	active, planned   bool
}

func (t *deliveryTracker) markFinalReconciled(index int, generation uint64, cursor tg.UpdatesState) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finalReconciled == nil {
		t.finalReconciled = make(map[int]bool, len(t.devices))
	}
	t.finalReconciled[index] = true
	t.events.write(map[string]any{"type": "delivery_device_reconciled", "at": time.Now().UTC(), "session_index": index, "client_generation": generation, "cursor": cursor})
}

// Serialized with intent registration: each disk record freezes one exact
// revision. Only an explicit offline request grants a recovery expectation.
func (t *deliveryTracker) deviceTransition(index int, generation uint64, transition string) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.finalized || t.firstError != nil {
		return
	}
	if _, ok := t.devices[index]; !ok {
		t.fail(fmt.Errorf("unknown delivery device transition"))
		return
	}
	state := t.availability[index]
	switch transition {
	case "offline":
		state.planned, state.mode = true, deliveryDeviceOffline
	case "start":
		if generation == 0 || generation <= state.generation || state.active {
			t.fail(fmt.Errorf("overlapping delivery client generation"))
			return
		}
		state.generation, state.active = generation, true
		state.mode = deliveryDeviceUnavailable
		if state.planned {
			state.mode = deliveryDeviceOffline
		}
	case "ready":
		if generation != state.generation || !state.active {
			t.fail(fmt.Errorf("inactive delivery client became ready"))
			return
		}
		state.epoch++
		state.planned, state.mode = false, deliveryDeviceOnline
	case "unavailable", "stop":
		if generation != state.generation {
			t.fail(fmt.Errorf("stale delivery client transition"))
			return
		}
		state.mode = deliveryDeviceUnavailable
		if state.planned {
			state.mode = deliveryDeviceOffline
		}
		if transition == "stop" {
			state.active = false
		}
	default:
		t.fail(fmt.Errorf("unknown delivery device transition"))
		return
	}
	if state == t.availability[index] {
		return
	}
	t.availability[index] = state
	t.stateRevision++
	t.events.write(map[string]any{"type": "device_delivery_state", "at": time.Now().UTC(), "revision": t.stateRevision, "session_index": index, "transition": transition, "mode": state.mode, "client_generation": state.generation, "online_epoch": state.epoch, "active": state.active})
}
