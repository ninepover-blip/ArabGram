package postgres

import (
	"context"
	"testing"
)

type fakeBusinessAutomationReadModel struct {
	invalidated []int64
	flushes     int
}

func (f *fakeBusinessAutomationReadModel) InvalidateBusinessAutomationReadModel(_ context.Context, userID int64) error {
	f.invalidated = append(f.invalidated, userID)
	return nil
}

func (f *fakeBusinessAutomationReadModel) FlushBusinessAutomationReadModel() {
	f.flushes++
}

func TestReadModelChangeListenerRoutesBusinessAutomation(t *testing.T) {
	cache := &fakeBusinessAutomationReadModel{}
	listener := NewReadModelChangeListener("", ReadModelCacheSet{BusinessAutomation: cache}, nil)
	listener.handlePayload(`{"model":"business_automation","owner_user_id":77,"peer_type":"user","peer_id":77,"version":2,"hash":99}`)
	if len(cache.invalidated) != 1 || cache.invalidated[0] != 77 {
		t.Fatalf("invalidated = %v, want [77]", cache.invalidated)
	}
	listener.FlushRelayedReadModelCaches("test")
	if cache.flushes != 1 {
		t.Fatalf("flushes = %d, want 1", cache.flushes)
	}
}
