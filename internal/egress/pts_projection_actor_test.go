package egress

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

type projectionEventStore struct {
	mu      sync.Mutex
	calls   int
	batches [][]store.EventCursor
	omit    map[store.EventCursor]bool
}

func (s *projectionEventStore) BatchByCursor(_ context.Context, cursors []store.EventCursor) ([]domain.UpdateEvent, error) {
	s.mu.Lock()
	s.calls++
	s.batches = append(s.batches, append([]store.EventCursor(nil), cursors...))
	s.mu.Unlock()
	events := make([]domain.UpdateEvent, 0, len(cursors))
	for _, cursor := range cursors {
		if s.omit[cursor] {
			continue
		}
		events = append(events, domain.UpdateEvent{
			UserID: cursor.UserID, Pts: cursor.Pts, PtsCount: 1,
			Type: domain.UpdateEventNewMessage,
		})
	}
	return events, nil
}

func (s *projectionEventStore) snapshot() (int, [][]store.EventCursor) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, append([][]store.EventCursor(nil), s.batches...)
}

func TestPTSProjectionActorBatchesIndependentWindows(t *testing.T) {
	const windows = 64
	events := &projectionEventStore{}
	var builderMu sync.Mutex
	builderCalls := 0
	builderItems := 0
	actor := newPTSProjectionActorWithLimits(events, func(_ context.Context, requests []OutboxUpdateRequest) ([][]byte, error) {
		builderMu.Lock()
		builderCalls++
		builderItems += len(requests)
		builderMu.Unlock()
		out := make([][]byte, len(requests))
		for i, request := range requests {
			out[i] = []byte{byte(request.TargetUserID)}
		}
		return out, nil
	}, windows, windows, windows, time.Hour)

	type outcome struct {
		userID int64
		items  []projectedOutboxItem
		err    error
	}
	outcomes := make(chan outcome, windows)
	for i := 0; i < windows; i++ {
		userID := int64(i + 1)
		go func() {
			items, err := actor.Project(context.Background(), projectionWindow(userID, 1))
			outcomes <- outcome{userID: userID, items: items, err: err}
		}()
	}
	waitProjectionMailbox(t, actor, windows)
	ctx, cancel := context.WithCancel(context.Background())
	var wait sync.WaitGroup
	actor.Run(ctx, &wait)
	for i := 0; i < windows; i++ {
		result := <-outcomes
		if result.err != nil || len(result.items) != 1 || result.items[0].claimed.Ref.StreamID != result.userID ||
			len(result.items[0].payload) != 1 || result.items[0].payload[0] != byte(result.userID) {
			t.Fatalf("projection user=%d items=%+v err=%v", result.userID, result.items, result.err)
		}
	}
	cancel()
	wait.Wait()
	calls, batches := events.snapshot()
	builderMu.Lock()
	gotBuilderCalls, gotBuilderItems := builderCalls, builderItems
	builderMu.Unlock()
	if calls != 1 || len(batches) != 1 || len(batches[0]) != windows {
		t.Fatalf("event store calls=%d batches=%v, want one %d-cursor batch", calls, batchSizes(batches), windows)
	}
	if gotBuilderCalls != 1 || gotBuilderItems != windows {
		t.Fatalf("builder calls=%d items=%d, want one %d-item batch", gotBuilderCalls, gotBuilderItems, windows)
	}
}

func TestPTSProjectionActorIsolatesMissingEventWindow(t *testing.T) {
	missing := store.EventCursor{UserID: 2, Pts: 1}
	events := &projectionEventStore{omit: map[store.EventCursor]bool{missing: true}}
	builderCalls, builderItems := 0, 0
	actor := newPTSProjectionActorWithLimits(events, func(_ context.Context, requests []OutboxUpdateRequest) ([][]byte, error) {
		builderCalls++
		builderItems += len(requests)
		return [][]byte{{0x41}}, nil
	}, 2, 2, 2, time.Hour)

	type outcome struct {
		items []projectedOutboxItem
		err   error
	}
	results := make(chan outcome, 2)
	for _, userID := range []int64{1, 2} {
		userID := userID
		go func() {
			items, err := actor.Project(context.Background(), projectionWindow(userID, 1))
			results <- outcome{items: items, err: err}
		}()
	}
	waitProjectionMailbox(t, actor, 2)
	ctx, cancel := context.WithCancel(context.Background())
	var wait sync.WaitGroup
	actor.Run(ctx, &wait)
	var success, missingCount int
	for i := 0; i < 2; i++ {
		result := <-results
		switch {
		case result.err == nil && len(result.items) == 1:
			success++
		case errors.Is(result.err, errMissingOutboxEvent):
			missingCount++
		default:
			t.Fatalf("unexpected isolated projection result items=%+v err=%v", result.items, result.err)
		}
	}
	cancel()
	wait.Wait()
	if success != 1 || missingCount != 1 || builderCalls != 1 || builderItems != 1 {
		t.Fatalf("success=%d missing=%d builder_calls=%d builder_items=%d", success, missingCount, builderCalls, builderItems)
	}
}

func TestPTSProjectionPoolUsesAllFixedLanesAndBatchesWithinLane(t *testing.T) {
	const windows = 64
	events := &projectionEventStore{}
	var builderMu sync.Mutex
	builderCalls, builderItems := 0, 0
	pool := newPTSProjectionPool(events, func(_ context.Context, requests []OutboxUpdateRequest) ([][]byte, error) {
		builderMu.Lock()
		builderCalls++
		builderItems += len(requests)
		builderMu.Unlock()
		out := make([][]byte, len(requests))
		for i := range out {
			out[i] = []byte{0x61}
		}
		return out, nil
	})
	used := make(map[int]bool)
	results := make(chan error, windows)
	for i := 0; i < windows; i++ {
		userID := int64(i + 1)
		index := pool.actorIndex(userID)
		if index != pool.actorIndex(userID) {
			t.Fatalf("user %d projection lane is unstable", userID)
		}
		used[index] = true
		go func() {
			items, err := pool.Project(context.Background(), projectionWindow(userID, 1))
			if err == nil && (len(items) != 1 || len(items[0].payload) != 1) {
				err = errors.New("projection pool returned an invalid window")
			}
			results <- err
		}()
	}
	deadline := time.Now().Add(time.Second)
	for projectionPoolMailboxLen(pool) != windows && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := projectionPoolMailboxLen(pool); got != windows {
		t.Fatalf("projection pool mailbox=%d want %d", got, windows)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wait sync.WaitGroup
	pool.Run(ctx, &wait)
	for i := 0; i < windows; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	cancel()
	wait.Wait()
	calls, _ := events.snapshot()
	builderMu.Lock()
	gotBuilderCalls, gotBuilderItems := builderCalls, builderItems
	builderMu.Unlock()
	if len(used) != defaultPTSProjectionActors || calls != defaultPTSProjectionActors ||
		gotBuilderCalls != defaultPTSProjectionActors || gotBuilderItems != windows {
		t.Fatalf("lanes=%d event_calls=%d builder_calls=%d builder_items=%d",
			len(used), calls, gotBuilderCalls, gotBuilderItems)
	}
}

func projectionWindow(userID int64, pts int) store.OutboxClaimWindow {
	return store.OutboxClaimWindow{
		QueueKind: store.OutboxQueueDispatchPTS,
		StreamID:  userID,
		Items: []store.OutboxClaimedItem{{
			Ref: store.OutboxAttemptRef{
				QueueKind: store.OutboxQueueDispatchPTS,
				StreamID:  userID,
				ItemID:    userID*1000 + int64(pts),
				Sequence:  int64(pts),
			},
			Payload: store.DispatchOutboxPayload{EventType: domain.UpdateEventNewMessage},
		}},
	}
}

func waitProjectionMailbox(t *testing.T, actor *ptsProjectionActor, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for len(actor.mailbox) != want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := len(actor.mailbox); got != want {
		t.Fatalf("projection mailbox=%d want %d", got, want)
	}
}

func batchSizes(batches [][]store.EventCursor) []int {
	out := make([]int, len(batches))
	for i := range batches {
		out[i] = len(batches[i])
	}
	return out
}

func projectionPoolMailboxLen(pool *ptsProjectionPool) int {
	total := 0
	for _, actor := range pool.actors {
		total += len(actor.mailbox)
	}
	return total
}
