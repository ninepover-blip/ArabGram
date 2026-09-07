package loadharness

import (
	"context"
	"errors"
	"time"

	"github.com/iamxvbaba/td/telegram"
	"github.com/iamxvbaba/td/tg"
)

// Primary state callbacks are serialized by the published client's reconnect
// loop. The attempt fences admission; socket/session identity is separate evidence.
func (w *loadWorker) primaryState(generation uint64, state telegram.ConnectionState) {
	w.lifecycleMu.Lock()
	defer w.lifecycleMu.Unlock()
	if generation != w.clientGeneration.Load() {
		w.delivery.invariant("stale_primary_state")
		return
	}
	switch state {
	case telegram.ConnectionStateConnecting:
		w.primaryAttempt++
		w.state.Store(workerConnecting)
		w.counters.connectionAttempts.Add(1)
		if w.everReady.Load() {
			w.counters.reconnects.Add(1)
		}
	case telegram.ConnectionStateReady:
		w.state.Store(workerReady)
		w.everReady.Store(true)
	case telegram.ConnectionStateDisconnected:
		w.state.Store(workerDisconnected)
		if w.plannedDisconnectGeneration.Load() != generation {
			w.counters.disconnects.Add(1)
		}
	}
	if state != telegram.ConnectionStateReady {
		w.businessReady.Store(false)
		w.delivery.deviceTransition(w.record.Index, generation, "unavailable")
	}
	w.events.write(map[string]any{"type": "primary_connection_state", "at": time.Now().UTC(), "session_index": w.record.Index, "client_generation": generation, "primary_attempt": w.primaryAttempt, "state": state.String()})
}

func (w *loadWorker) primaryReady() (attempt uint64, ready, business bool) {
	w.lifecycleMu.Lock()
	defer w.lifecycleMu.Unlock()
	return w.primaryAttempt, w.desired.Load() && w.state.Load() == workerReady && w.primaryAttempt != 0, w.businessReady.Load()
}

func (w *loadWorker) markBusinessReady(ctx context.Context, attempt uint64) bool {
	w.lifecycleMu.Lock()
	defer w.lifecycleMu.Unlock()
	if ctx.Err() != nil || attempt == 0 || w.primaryAttempt != attempt || !w.desired.Load() || w.state.Load() != workerReady {
		w.events.write(map[string]any{"type": "session_business_ready_rejected", "at": time.Now().UTC(), "session_index": w.record.Index, "client_generation": w.clientGeneration.Load(), "audited_primary_attempt": attempt, "current_primary_attempt": w.primaryAttempt})
		return false
	}
	w.delivery.deviceTransition(w.record.Index, w.clientGeneration.Load(), "ready")
	w.businessReady.Store(true)
	w.events.write(map[string]any{"type": "session_business_ready", "at": time.Now().UTC(), "session_index": w.record.Index, "client_generation": w.clientGeneration.Load(), "primary_attempt": attempt})
	return true
}

// Initial callback and every Ready notification share the same gate. A queued
// notification for an already audited attempt does not trigger a second read.
func (w *loadWorker) activateBusiness(ctx context.Context, raw *tg.Client, readySignal <-chan struct{}) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		attempt, ready, business := w.primaryReady()
		if !ready {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-readySignal:
				continue
			}
		}
		if business {
			return nil
		}
		_, baseline := w.deliveryState.load()
		w.events.write(map[string]any{"type": "business_audit_begin", "at": time.Now().UTC(), "session_index": w.record.Index, "client_generation": w.clientGeneration.Load(), "primary_attempt": attempt, "has_cursor": baseline})
		var err error
		if baseline {
			err = w.catchUp(ctx, raw)
		} else {
			err = w.refreshUpdateState(ctx, raw)
		}
		w.events.write(map[string]any{"type": "business_audit_end", "at": time.Now().UTC(), "session_index": w.record.Index, "client_generation": w.clientGeneration.Load(), "primary_attempt": attempt, "class": classifyError(err)})
		if err != nil {
			return err
		}
		if _, valid := w.deliveryState.load(); !valid {
			return errors.New("business activation completed without an audit cursor")
		}
		if w.markBusinessReady(ctx, attempt) {
			return nil
		}
	}
}
