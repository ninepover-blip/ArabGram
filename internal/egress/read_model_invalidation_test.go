package egress

import (
	"context"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

type readModelPublisherCapture struct {
	items []store.ReadModelInvalidation
}

func (p *readModelPublisherCapture) PublishReadModelInvalidations(_ context.Context, items []store.ReadModelInvalidation) error {
	p.items = append(p.items, items...)
	return nil
}

func TestEgressPublishesDurableDialogInvalidationBeforeRoute(t *testing.T) {
	publisher := &readModelPublisherCapture{}
	coordinator := &deliveryCoordinator{readModelInvalidations: publisher}
	window := store.OutboxClaimWindow{
		QueueKind: store.OutboxQueueDispatchPTS,
		StreamID:  42,
		Items: []store.OutboxClaimedItem{
			{
				Ref: store.OutboxAttemptRef{QueueKind: store.OutboxQueueDispatchPTS, StreamID: 42, Sequence: 7},
				Payload: store.DispatchOutboxPayload{
					EventType:     domain.UpdateEventNewMessage,
					ReadModelPeer: domain.Peer{Type: domain.PeerTypeUser, ID: 84},
				},
			},
			{
				Ref: store.OutboxAttemptRef{QueueKind: store.OutboxQueueDispatchPTS, StreamID: 42, Sequence: 9},
				Payload: store.DispatchOutboxPayload{
					EventType:     domain.UpdateEventNewMessage,
					ReadModelPeer: domain.Peer{Type: domain.PeerTypeUser, ID: 84},
				},
			},
		},
	}
	if err := coordinator.publishDispatchReadModelInvalidations(context.Background(), window); err != nil {
		t.Fatal(err)
	}
	wantKey := store.ReadModelKey{Model: "dialog_light", OwnerUserID: 42, PeerType: domain.PeerTypeUser, PeerID: 84}
	want := store.NewSequencedReadModelInvalidation(wantKey, 9)
	if len(publisher.items) != 1 || publisher.items[0] != want {
		t.Fatalf("published = %+v, want latest exact %+v", publisher.items, want)
	}
}
