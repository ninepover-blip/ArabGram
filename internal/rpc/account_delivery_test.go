package rpc

import (
	"testing"

	"github.com/iamxvbaba/td/tg"

	"telesrv/internal/store/memory"
)

func attachDeliveryOutbox(users *memory.UserStore) *memory.DeliveryOutboxStore {
	delivery := memory.NewDeliveryOutboxStore()
	users.AttachDeliveryOutbox(delivery)
	return delivery
}

func attachPrivacyDeliveryOutbox(privacy *memory.PrivacyStore) *memory.DeliveryOutboxStore {
	delivery := memory.NewDeliveryOutboxStore()
	privacy.AttachDeliveryOutbox(delivery)
	return delivery
}

func lastQueuedDeliveryUpdates(t *testing.T, delivery *memory.DeliveryOutboxStore) *tg.Updates {
	t.Helper()
	items := delivery.Snapshot()
	if len(items) == 0 {
		t.Fatal("delivery outbox is empty")
	}
	decoded, err := decodeDeliveryUpdate(items[len(items)-1].Payload)
	if err != nil {
		t.Fatalf("decode delivery payload: %v", err)
	}
	updates, ok := decoded.(*tg.Updates)
	if !ok {
		t.Fatalf("delivery payload = %T, want *tg.Updates", decoded)
	}
	return updates
}
