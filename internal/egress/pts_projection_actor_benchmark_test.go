package egress

import (
	"context"
	"fmt"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

type ptsProjectionBenchmarkEventStore struct {
	events []domain.UpdateEvent
}

func (s ptsProjectionBenchmarkEventStore) BatchByCursor(context.Context, []store.EventCursor) ([]domain.UpdateEvent, error) {
	return s.events, nil
}

func BenchmarkPTSProjectionActorProjectBatch(b *testing.B) {
	for _, shape := range []struct {
		name           string
		windows, items int
	}{
		{name: "1_window_1_item", windows: 1, items: 1},
		{name: "1_window_128_items", windows: 1, items: 128},
		{name: "64_windows_2_items", windows: 64, items: 2},
	} {
		b.Run(shape.name, func(b *testing.B) {
			batch, events := ptsProjectionBenchmarkBatch(shape.windows, shape.items)
			built := make([][]byte, len(events))
			for i := range built {
				built[i] = []byte{0x41}
			}
			actor := newPTSProjectionActor(
				ptsProjectionBenchmarkEventStore{events: events},
				func(_ context.Context, requests []OutboxUpdateRequest) ([][]byte, error) {
					if len(requests) != len(built) {
						return nil, fmt.Errorf("benchmark builder requests=%d want=%d", len(requests), len(built))
					}
					return built, nil
				},
			)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				actor.projectBatch(ctx, batch)
				for j := range batch {
					result := <-batch[j].result
					if result.err != nil || len(result.items) != shape.items {
						b.Fatalf("window %d items=%d err=%v", j, len(result.items), result.err)
					}
				}
			}
		})
	}
}

func ptsProjectionBenchmarkBatch(windows, items int) ([]ptsProjectionRequest, []domain.UpdateEvent) {
	batch := make([]ptsProjectionRequest, windows)
	events := make([]domain.UpdateEvent, 0, windows*items)
	for windowIndex := 0; windowIndex < windows; windowIndex++ {
		userID := int64(windowIndex + 1)
		window := store.OutboxClaimWindow{
			QueueKind: store.OutboxQueueDispatchPTS,
			StreamID:  userID,
			Items:     make([]store.OutboxClaimedItem, items),
		}
		for itemIndex := 0; itemIndex < items; itemIndex++ {
			pts := itemIndex + 1
			window.Items[itemIndex] = store.OutboxClaimedItem{
				Ref: store.OutboxAttemptRef{
					QueueKind: store.OutboxQueueDispatchPTS,
					StreamID:  userID,
					ItemID:    userID*1000 + int64(pts),
					Sequence:  int64(pts),
				},
				Payload: store.DispatchOutboxPayload{EventType: domain.UpdateEventNewMessage},
			}
			events = append(events, domain.UpdateEvent{
				UserID: userID, Pts: pts, PtsCount: 1,
				Type: domain.UpdateEventNewMessage,
			})
		}
		batch[windowIndex] = ptsProjectionRequest{
			ctx:    context.Background(),
			window: window,
			result: make(chan ptsProjectionResult, 1),
		}
	}
	return batch, events
}
