package egress

import (
	"context"

	"telesrv/internal/store"
)

// noReadyOutboxSchedule is embedded only by unit-test stores whose cases do
// not exercise the runtime scheduling loop.
type noReadyOutboxSchedule struct{}

func (noReadyOutboxSchedule) NextReadyAt(context.Context, store.OutboxQueueKind, int, []int) (store.OutboxNextReady, bool, error) {
	return store.OutboxNextReady{}, false, nil
}
