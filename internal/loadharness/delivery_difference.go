package loadharness

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/iamxvbaba/td/tg"
)

func (w *loadWorker) catchUp(ctx context.Context, raw *tg.Client) error {
	return w.readDeliveryDifference(ctx, raw, "updates.getDifference")
}

func (w *loadWorker) catchUpDelivery(ctx context.Context, raw *tg.Client) error {
	return w.readDeliveryDifference(ctx, raw, "updates.getDifference.delivery")
}

// getState probes are never a delivery cursor. Reconnect and final audit share
// the cursor established before workload admission, and consume every page.
func (w *loadWorker) readDeliveryDifference(ctx context.Context, raw *tg.Client, method string) (err error) {
	defer func() {
		if err != nil && ctx.Err() == nil && w.delivery != nil {
			w.delivery.invariant("difference_evidence")
		}
	}()
	state, valid := w.deliveryState.load()
	if !valid {
		err = errors.New("delivery difference has no audited baseline")
		w.metrics.observe(method, time.Now(), err)
		return err
	}
	generation := w.clientGeneration.Load()
	for page := 0; page < 256; page++ {
		start := time.Now()
		op, cancel := w.operationContext(ctx)
		difference, callErr := raw.UpdatesGetDifference(op, &tg.UpdatesGetDifferenceRequest{Pts: state.Pts, Date: state.Date, Qts: state.Qts})
		cancel()
		next, empty, validationErr := deliveryDifferenceState(state, difference)
		if callErr != nil {
			validationErr = callErr
		}
		if validationErr == nil && !empty && page == 255 {
			validationErr = errors.New("delivery difference exceeded page limit")
		}
		w.metrics.observe(method, start, validationErr)
		if validationErr != nil {
			return validationErr
		}
		switch value := difference.(type) {
		case *tg.UpdatesDifference:
			observeMessageClasses(w.delivery, w.record.Index, value.NewMessages, deliveryDifference, generation)
			observeUpdateClasses(w.delivery, w.record.Index, value.OtherUpdates, deliveryDifference, generation)
			w.counters.updates.Add(uint64(len(value.NewMessages) + len(value.NewEncryptedMessages) + len(value.OtherUpdates)))
		case *tg.UpdatesDifferenceSlice:
			observeMessageClasses(w.delivery, w.record.Index, value.NewMessages, deliveryDifference, generation)
			observeUpdateClasses(w.delivery, w.record.Index, value.OtherUpdates, deliveryDifference, generation)
			w.counters.updates.Add(uint64(len(value.NewMessages) + len(value.NewEncryptedMessages) + len(value.OtherUpdates)))
		}
		w.events.write(map[string]any{"type": "delivery_difference_page", "at": time.Now().UTC(), "session_index": w.record.Index, "client_generation": generation, "method": method, "page": page, "from": state, "next": next, "empty": empty})
		state = next
		w.deliveryState.store(state)
		if empty {
			return nil
		}
	}
	return errors.New("delivery difference exceeded page limit")
}

func deliveryDifferenceState(previous tg.UpdatesState, difference tg.UpdatesDifferenceClass) (next tg.UpdatesState, empty bool, err error) {
	next = previous
	slice := false
	switch value := difference.(type) {
	case *tg.UpdatesDifferenceEmpty:
		next.Date, next.Seq, empty = value.Date, value.Seq, true
	case *tg.UpdatesDifference:
		next = value.State
	case *tg.UpdatesDifferenceSlice:
		next, slice = value.IntermediateState, true
	case *tg.UpdatesDifferenceTooLong:
		return next, false, errors.New("delivery differenceTooLong cannot prove message recovery")
	default:
		return next, false, fmt.Errorf("unexpected delivery difference %T", difference)
	}
	if next.Pts < previous.Pts || next.Qts < previous.Qts || next.Date < previous.Date || next.Seq < previous.Seq {
		return previous, false, errors.New("delivery difference cursor regressed")
	}
	if slice && next.Pts == previous.Pts && next.Qts == previous.Qts && next.Date == previous.Date && next.Seq == previous.Seq {
		return previous, false, errors.New("delivery difference slice made no cursor progress")
	}
	return next, empty, nil
}
