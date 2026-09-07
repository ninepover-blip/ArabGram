package egress

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/edgecontrol"
	"telesrv/internal/store"
)

type projectionEventStoreFunc func(context.Context, []store.EventCursor) ([]domain.UpdateEvent, error)

func (f projectionEventStoreFunc) BatchByCursor(ctx context.Context, cursors []store.EventCursor) ([]domain.UpdateEvent, error) {
	return f(ctx, cursors)
}

func TestPTSProjectionActorProjectBatchContracts(t *testing.T) {
	builderFailure := errors.New("builder failed")
	tests := []struct {
		name           string
		eventType      domain.UpdateEventType
		payload        store.OutboxClaimPayload
		storeMode      string
		builderMode    string
		requestContext string
		cancelRun      bool
		wantErr        error
		wantErrText    string
		wantStoreCalls int
		wantBuildCalls int
		wantPayload    []byte
	}{
		{
			name: "success", eventType: domain.UpdateEventNewMessage,
			payload:        store.DispatchOutboxPayload{EventType: domain.UpdateEventNewMessage},
			wantStoreCalls: 1, wantBuildCalls: 1, wantPayload: []byte{0x41, 0x42},
		},
		{
			name: "noop_empty_payload", eventType: domain.UpdateEventNoop,
			payload: store.DispatchOutboxPayload{EventType: domain.UpdateEventNoop}, builderMode: "empty",
			wantStoreCalls: 1, wantBuildCalls: 1,
		},
		{
			name: "missing_event", eventType: domain.UpdateEventNewMessage,
			payload: store.DispatchOutboxPayload{EventType: domain.UpdateEventNewMessage}, storeMode: "missing",
			wantErr: errMissingOutboxEvent, wantStoreCalls: 1,
		},
		{
			name: "payload_type_mismatch", eventType: domain.UpdateEventNewMessage,
			payload:     store.AbsoluteDeliveryPayload{TL: []byte{0x01}},
			wantErrText: "durable event identity mismatch", wantStoreCalls: 1,
		},
		{
			name: "event_type_mismatch", eventType: domain.UpdateEventNoop,
			payload:     store.DispatchOutboxPayload{EventType: domain.UpdateEventNewMessage},
			wantErrText: "durable event identity mismatch", wantStoreCalls: 1,
		},
		{
			name: "builder_error", eventType: domain.UpdateEventNewMessage,
			payload: store.DispatchOutboxPayload{EventType: domain.UpdateEventNewMessage}, builderMode: "error",
			wantErr: builderFailure, wantStoreCalls: 1, wantBuildCalls: 1,
		},
		{
			name: "builder_count_mismatch", eventType: domain.UpdateEventNewMessage,
			payload: store.DispatchOutboxPayload{EventType: domain.UpdateEventNewMessage}, builderMode: "count",
			wantErr: errOutboxUpdateBuilderCount, wantStoreCalls: 1, wantBuildCalls: 1,
		},
		{
			name: "empty_non_noop_payload", eventType: domain.UpdateEventNewMessage,
			payload: store.DispatchOutboxPayload{EventType: domain.UpdateEventNewMessage}, builderMode: "empty",
			wantErr: errOutboxUpdateBuilderEmpty, wantStoreCalls: 1, wantBuildCalls: 1,
		},
		{
			name: "elapsed_request_deadline", eventType: domain.UpdateEventNewMessage,
			payload: store.DispatchOutboxPayload{EventType: domain.UpdateEventNewMessage}, requestContext: "elapsed_deadline",
			wantErr: context.DeadlineExceeded,
		},
		{
			name: "request_canceled", eventType: domain.UpdateEventNewMessage,
			payload: store.DispatchOutboxPayload{EventType: domain.UpdateEventNewMessage}, requestContext: "canceled",
			wantErr: context.Canceled,
		},
		{
			name: "run_context_canceled", eventType: domain.UpdateEventNewMessage,
			payload: store.DispatchOutboxPayload{EventType: domain.UpdateEventNewMessage}, cancelRun: true,
			wantErr: context.Canceled, wantStoreCalls: 1,
		},
		{
			name: "request_deadline_propagated", eventType: domain.UpdateEventNewMessage,
			payload: store.DispatchOutboxPayload{EventType: domain.UpdateEventNewMessage}, requestContext: "future_deadline",
			wantStoreCalls: 1, wantBuildCalls: 1, wantPayload: []byte{0x41, 0x42},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			requestCtx := context.Background()
			cancelRequest := func() {}
			var requestDeadline time.Time
			switch test.requestContext {
			case "elapsed_deadline":
				requestCtx, cancelRequest = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			case "canceled":
				var cancel context.CancelFunc
				requestCtx, cancel = context.WithCancel(context.Background())
				cancel()
			case "future_deadline":
				requestDeadline = time.Now().Add(time.Minute)
				requestCtx, cancelRequest = context.WithDeadline(context.Background(), requestDeadline)
			}
			defer cancelRequest()

			runCtx := context.Background()
			cancelRun := func() {}
			if test.cancelRun {
				var cancel context.CancelFunc
				runCtx, cancel = context.WithCancel(context.Background())
				cancel()
			}
			defer cancelRun()

			storeCalls := 0
			buildCalls := 0
			var gotCursors []store.EventCursor
			var storeDeadline, builderDeadline time.Time
			var gotBuilderRequests []OutboxUpdateRequest
			event := domain.UpdateEvent{
				UserID: 1, Pts: 1, PtsCount: 1, Type: test.eventType,
			}
			events := projectionEventStoreFunc(func(ctx context.Context, cursors []store.EventCursor) ([]domain.UpdateEvent, error) {
				storeCalls++
				gotCursors = append([]store.EventCursor(nil), cursors...)
				if deadline, ok := ctx.Deadline(); ok {
					storeDeadline = deadline
				}
				if test.cancelRun {
					return nil, ctx.Err()
				}
				if test.storeMode == "missing" {
					return nil, nil
				}
				return []domain.UpdateEvent{event}, nil
			})
			builder := func(ctx context.Context, requests []OutboxUpdateRequest) ([][]byte, error) {
				buildCalls++
				gotBuilderRequests = append([]OutboxUpdateRequest(nil), requests...)
				if deadline, ok := ctx.Deadline(); ok {
					builderDeadline = deadline
				}
				switch test.builderMode {
				case "error":
					return nil, builderFailure
				case "count":
					return nil, nil
				case "empty":
					return [][]byte{nil}, nil
				default:
					return [][]byte{{0x41, 0x42}}, nil
				}
			}
			actor := newPTSProjectionActor(events, builder)
			window := projectionWindow(1, 1)
			window.Items[0].Payload = test.payload
			result := make(chan ptsProjectionResult, 1)
			actor.projectBatch(runCtx, []ptsProjectionRequest{{ctx: requestCtx, window: window, result: result}})
			got := <-result

			if test.wantErr != nil && !errors.Is(got.err, test.wantErr) {
				t.Fatalf("error=%v want errors.Is(%v)", got.err, test.wantErr)
			}
			if test.wantErrText != "" && (got.err == nil || !strings.Contains(got.err.Error(), test.wantErrText)) {
				t.Fatalf("error=%v want text %q", got.err, test.wantErrText)
			}
			if test.wantErr == nil && test.wantErrText == "" && got.err != nil {
				t.Fatalf("unexpected error: %v", got.err)
			}
			if storeCalls != test.wantStoreCalls || buildCalls != test.wantBuildCalls {
				t.Fatalf("store calls=%d builder calls=%d, want %d/%d", storeCalls, buildCalls, test.wantStoreCalls, test.wantBuildCalls)
			}
			if storeCalls > 0 {
				wantCursors := []store.EventCursor{{UserID: 1, Pts: 1}}
				if len(gotCursors) != 1 || gotCursors[0] != wantCursors[0] {
					t.Fatalf("cursors=%+v want=%+v", gotCursors, wantCursors)
				}
			}
			if buildCalls > 0 {
				if len(gotBuilderRequests) != 1 || gotBuilderRequests[0].TargetUserID != 1 ||
					gotBuilderRequests[0].Event.UserID != 1 || gotBuilderRequests[0].Event.Pts != 1 ||
					gotBuilderRequests[0].Event.Type != test.eventType {
					t.Fatalf("builder requests=%+v", gotBuilderRequests)
				}
			}
			if test.requestContext == "future_deadline" {
				if !storeDeadline.Equal(requestDeadline) || !builderDeadline.Equal(requestDeadline) {
					t.Fatalf("deadlines store=%v builder=%v want=%v", storeDeadline, builderDeadline, requestDeadline)
				}
			}
			if got.err != nil {
				if len(got.items) != 0 {
					t.Fatalf("error result contains %d projected items", len(got.items))
				}
				return
			}
			if len(got.items) != 1 || got.items[0].claimed.Ref != window.Items[0].Ref ||
				!bytes.Equal(got.items[0].payload, test.wantPayload) || len(got.items[0].targetUsers) != 1 ||
				got.items[0].targetUsers[0] != window.StreamID {
				t.Fatalf("projected items=%+v", got.items)
			}
		})
	}
}

func TestPTSProjectionActorProjectBatchEmpty(t *testing.T) {
	storeCalls, builderCalls := 0, 0
	actor := newPTSProjectionActor(
		projectionEventStoreFunc(func(context.Context, []store.EventCursor) ([]domain.UpdateEvent, error) {
			storeCalls++
			return nil, nil
		}),
		func(context.Context, []OutboxUpdateRequest) ([][]byte, error) {
			builderCalls++
			return nil, nil
		},
	)
	actor.projectBatch(context.Background(), nil)
	if storeCalls != 0 || builderCalls != 0 {
		t.Fatalf("empty batch called store/builder %d/%d times", storeCalls, builderCalls)
	}
}

func TestPTSProjectionActorRejectsInvalidWindowBeforeStore(t *testing.T) {
	tests := []struct {
		name        string
		mutate      func(*store.OutboxClaimWindow)
		wantErrText string
	}{
		{
			name: "window_queue_kind",
			mutate: func(window *store.OutboxClaimWindow) {
				window.QueueKind = store.OutboxQueueAbsoluteDelivery
			},
			wantErrText: "invalid account PTS projection window",
		},
		{
			name: "window_stream_zero",
			mutate: func(window *store.OutboxClaimWindow) {
				window.StreamID = 0
			},
			wantErrText: "invalid account PTS projection window",
		},
		{
			name: "window_items_empty",
			mutate: func(window *store.OutboxClaimWindow) {
				window.Items = nil
			},
			wantErrText: "invalid account PTS projection window",
		},
		{
			name: "window_items_over_limit",
			mutate: func(window *store.OutboxClaimWindow) {
				item := window.Items[0]
				window.Items = make([]store.OutboxClaimedItem, edgecontrol.MaxDeliveryBatchItems+1)
				for i := range window.Items {
					window.Items[i] = item
				}
			},
			wantErrText: "invalid account PTS projection window",
		},
		{
			name: "item_queue_kind",
			mutate: func(window *store.OutboxClaimWindow) {
				window.Items[0].Ref.QueueKind = store.OutboxQueueAbsoluteDelivery
			},
			wantErrText: "invalid account PTS 1",
		},
		{
			name: "item_stream_mismatch",
			mutate: func(window *store.OutboxClaimWindow) {
				window.Items[0].Ref.StreamID++
			},
			wantErrText: "invalid account PTS 1",
		},
		{
			name: "item_sequence_zero",
			mutate: func(window *store.OutboxClaimWindow) {
				window.Items[0].Ref.Sequence = 0
			},
			wantErrText: "invalid account PTS 0",
		},
		{
			name: "item_sequence_over_int32",
			mutate: func(window *store.OutboxClaimWindow) {
				window.Items[0].Ref.Sequence = int64(^uint32(0)>>1) + 1
			},
			wantErrText: "invalid account PTS 2147483648",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			storeCalls, builderCalls := 0, 0
			actor := newPTSProjectionActor(
				projectionEventStoreFunc(func(context.Context, []store.EventCursor) ([]domain.UpdateEvent, error) {
					storeCalls++
					return nil, nil
				}),
				func(context.Context, []OutboxUpdateRequest) ([][]byte, error) {
					builderCalls++
					return nil, nil
				},
			)
			window := projectionWindow(1, 1)
			test.mutate(&window)
			result := make(chan ptsProjectionResult, 1)
			actor.projectBatch(context.Background(), []ptsProjectionRequest{{
				ctx: context.Background(), window: window, result: result,
			}})
			got := <-result
			if got.err == nil || !strings.Contains(got.err.Error(), test.wantErrText) {
				t.Fatalf("error=%v want text %q", got.err, test.wantErrText)
			}
			if len(got.items) != 0 || storeCalls != 0 || builderCalls != 0 {
				t.Fatalf("items=%d store calls=%d builder calls=%d, want all zero", len(got.items), storeCalls, builderCalls)
			}
		})
	}
}

func TestPTSProjectionActorRightSizesCursorCapacity(t *testing.T) {
	const windows, itemsPerWindow = 3, 2
	batch := make([]ptsProjectionRequest, windows)
	allEvents := make([]domain.UpdateEvent, 0, windows*itemsPerWindow)
	for i := range batch {
		window := projectionWindowItems(int64(i+1), itemsPerWindow)
		batch[i] = ptsProjectionRequest{ctx: context.Background(), window: window, result: make(chan ptsProjectionResult, 1)}
		for pts := 1; pts <= itemsPerWindow; pts++ {
			allEvents = append(allEvents, domain.UpdateEvent{
				UserID: window.StreamID, Pts: pts, PtsCount: 1, Type: domain.UpdateEventNewMessage,
			})
		}
	}
	gotLen, gotCap := 0, 0
	actor := newPTSProjectionActor(
		projectionEventStoreFunc(func(_ context.Context, cursors []store.EventCursor) ([]domain.UpdateEvent, error) {
			gotLen, gotCap = len(cursors), cap(cursors)
			return allEvents, nil
		}),
		func(_ context.Context, requests []OutboxUpdateRequest) ([][]byte, error) {
			out := make([][]byte, len(requests))
			for i := range out {
				out[i] = []byte{0x41}
			}
			return out, nil
		},
	)
	actor.projectBatch(context.Background(), batch)
	for i := range batch {
		result := <-batch[i].result
		if result.err != nil || len(result.items) != itemsPerWindow {
			t.Fatalf("window %d items=%d err=%v", i, len(result.items), result.err)
		}
	}
	want := windows * itemsPerWindow
	if gotLen != want || gotCap != want {
		t.Fatalf("cursor len/cap=%d/%d want=%d/%d", gotLen, gotCap, want, want)
	}
}

func TestPTSProjectionActorRollsBackPartialWindowBeforeLaterValidWindow(t *testing.T) {
	tests := []struct {
		name          string
		mutate        func(*store.OutboxClaimWindow)
		omit          store.EventCursor
		wantErr       error
		wantErrText   string
		wantBadCursor bool
	}{
		{
			name:    "missing_second_event",
			omit:    store.EventCursor{UserID: 2, Pts: 2},
			wantErr: errMissingOutboxEvent, wantBadCursor: true,
		},
		{
			name: "second_payload_type_mismatch",
			mutate: func(window *store.OutboxClaimWindow) {
				window.Items[1].Payload = store.DispatchOutboxPayload{EventType: domain.UpdateEventNoop}
			},
			wantErrText: "durable event identity mismatch", wantBadCursor: true,
		},
		{
			name: "second_cursor_invalid",
			mutate: func(window *store.OutboxClaimWindow) {
				window.Items[1].Ref.Sequence = 0
			},
			wantErrText: "invalid account PTS 0",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			windows := []store.OutboxClaimWindow{
				projectionWindowItems(1, 2),
				projectionWindowItems(2, 2),
				projectionWindowItems(3, 2),
			}
			if test.mutate != nil {
				test.mutate(&windows[1])
			}
			batch := make([]ptsProjectionRequest, len(windows))
			for i, window := range windows {
				batch[i] = ptsProjectionRequest{ctx: context.Background(), window: window, result: make(chan ptsProjectionResult, 1)}
			}

			var gotCursors []store.EventCursor
			var gotRequests []OutboxUpdateRequest
			actor := newPTSProjectionActor(
				projectionEventStoreFunc(func(_ context.Context, cursors []store.EventCursor) ([]domain.UpdateEvent, error) {
					gotCursors = append([]store.EventCursor(nil), cursors...)
					events := make([]domain.UpdateEvent, 0, len(cursors))
					for _, cursor := range cursors {
						if cursor == test.omit {
							continue
						}
						events = append(events, domain.UpdateEvent{
							UserID: cursor.UserID, Pts: cursor.Pts, PtsCount: 1,
							Type: domain.UpdateEventNewMessage,
						})
					}
					return events, nil
				}),
				func(_ context.Context, requests []OutboxUpdateRequest) ([][]byte, error) {
					gotRequests = append([]OutboxUpdateRequest(nil), requests...)
					built := make([][]byte, len(requests))
					for i, request := range requests {
						built[i] = []byte{byte(request.TargetUserID), byte(request.Event.Pts)}
					}
					return built, nil
				},
			)
			actor.projectBatch(context.Background(), batch)

			for i := range batch {
				result := <-batch[i].result
				if i == 1 {
					if test.wantErr != nil && !errors.Is(result.err, test.wantErr) {
						t.Fatalf("bad window error=%v want errors.Is(%v)", result.err, test.wantErr)
					}
					if test.wantErrText != "" && (result.err == nil || !strings.Contains(result.err.Error(), test.wantErrText)) {
						t.Fatalf("bad window error=%v want text %q", result.err, test.wantErrText)
					}
					if len(result.items) != 0 {
						t.Fatalf("bad window leaked %d projected items", len(result.items))
					}
					continue
				}
				if result.err != nil || len(result.items) != 2 {
					t.Fatalf("valid window %d items=%d err=%v", i, len(result.items), result.err)
				}
				for itemIndex, item := range result.items {
					want := []byte{byte(windows[i].StreamID), byte(itemIndex + 1)}
					if item.claimed.Ref != windows[i].Items[itemIndex].Ref || !bytes.Equal(item.payload, want) ||
						len(item.targetUsers) != 1 || item.targetUsers[0] != windows[i].StreamID {
						t.Fatalf("valid window %d item %d=%+v want payload=%v", i, itemIndex, item, want)
					}
				}
			}

			wantCursors := []store.EventCursor{{UserID: 1, Pts: 1}, {UserID: 1, Pts: 2}}
			if test.wantBadCursor {
				wantCursors = append(wantCursors, store.EventCursor{UserID: 2, Pts: 1}, store.EventCursor{UserID: 2, Pts: 2})
			}
			wantCursors = append(wantCursors, store.EventCursor{UserID: 3, Pts: 1}, store.EventCursor{UserID: 3, Pts: 2})
			if len(gotCursors) != len(wantCursors) {
				t.Fatalf("event cursors=%+v want=%+v", gotCursors, wantCursors)
			}
			for i := range wantCursors {
				if gotCursors[i] != wantCursors[i] {
					t.Fatalf("event cursor %d=%+v want=%+v", i, gotCursors[i], wantCursors[i])
				}
			}
			if len(gotRequests) != 4 {
				t.Fatalf("builder requests=%+v want four requests from valid windows", gotRequests)
			}
			for i, request := range gotRequests {
				wantUser := int64(1)
				if i >= 2 {
					wantUser = 3
				}
				wantPTS := i%2 + 1
				if request.TargetUserID != wantUser || request.Event.UserID != wantUser || request.Event.Pts != wantPTS {
					t.Fatalf("builder request %d=%+v want user=%d pts=%d", i, request, wantUser, wantPTS)
				}
			}
		})
	}
}

func TestPTSProjectionActorCarriesWindowAcrossMaxItemsBoundary(t *testing.T) {
	events := &projectionEventStore{}
	actor := newPTSProjectionActorWithLimits(
		events,
		func(_ context.Context, requests []OutboxUpdateRequest) ([][]byte, error) {
			out := make([][]byte, len(requests))
			for i := range out {
				out[i] = []byte{0x41}
			}
			return out, nil
		},
		2, 8, 2, time.Hour,
	)
	first := ptsProjectionRequest{
		ctx: context.Background(), window: projectionWindowItems(1, 1), result: make(chan ptsProjectionResult, 1),
	}
	carried := ptsProjectionRequest{
		ctx: context.Background(), window: projectionWindowItems(2, 2), result: make(chan ptsProjectionResult, 1),
	}
	actor.mailbox <- first
	actor.mailbox <- carried
	ctx, cancel := context.WithCancel(context.Background())
	var wait sync.WaitGroup
	actor.Run(ctx, &wait)
	for index, request := range []ptsProjectionRequest{first, carried} {
		select {
		case result := <-request.result:
			if result.err != nil || len(result.items) != len(request.window.Items) {
				t.Fatalf("request %d items=%d err=%v", index, len(result.items), result.err)
			}
		case <-time.After(time.Second):
			t.Fatalf("request %d did not complete", index)
		}
	}
	cancel()
	wait.Wait()
	storeCalls, batches := events.snapshot()
	if storeCalls != 2 || len(batches) != 2 || len(batches[0]) != 1 || len(batches[1]) != 2 {
		t.Fatalf("store calls=%d batch sizes=%v want [1 2]", storeCalls, batchSizes(batches))
	}
}

func projectionWindowItems(userID int64, count int) store.OutboxClaimWindow {
	window := store.OutboxClaimWindow{
		QueueKind: store.OutboxQueueDispatchPTS,
		StreamID:  userID,
		Items:     make([]store.OutboxClaimedItem, count),
	}
	for i := range window.Items {
		pts := i + 1
		window.Items[i] = store.OutboxClaimedItem{
			Ref: store.OutboxAttemptRef{
				QueueKind: store.OutboxQueueDispatchPTS,
				StreamID:  userID,
				ItemID:    userID*1000 + int64(pts),
				Sequence:  int64(pts),
			},
			Payload: store.DispatchOutboxPayload{EventType: domain.UpdateEventNewMessage},
		}
	}
	return window
}
