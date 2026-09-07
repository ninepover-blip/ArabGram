package businessautomation

import (
	"context"
	"errors"
	"sync"
	"testing"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

type gateSource struct {
	store.BusinessAutomationStore
	mu       sync.Mutex
	has      bool
	hasCalls int
}

func (s *gateSource) HasBusinessAutomation(context.Context, int64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hasCalls++
	return s.has, nil
}

func (s *gateSource) SaveBusinessProfile(_ context.Context, profile domain.BusinessProfile) error {
	s.mu.Lock()
	s.has = profile.Greeting != nil || profile.Away != nil
	s.mu.Unlock()
	return nil
}

type memorySharedGate struct {
	mu      sync.Mutex
	values  map[int64]bool
	getErr  error
	gets    int
	puts    int
	deletes int
}

func (s *memorySharedGate) GetBusinessAutomationGate(_ context.Context, userID int64) (bool, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	if s.getErr != nil {
		return false, false, s.getErr
	}
	v, ok := s.values[userID]
	return v, ok, nil
}

func (s *memorySharedGate) PutBusinessAutomationGate(_ context.Context, userID int64, value bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.values == nil {
		s.values = make(map[int64]bool)
	}
	s.values[userID] = value
	s.puts++
	return nil
}

func (s *memorySharedGate) DeleteBusinessAutomationGate(_ context.Context, userIDs ...int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, userID := range userIDs {
		delete(s.values, userID)
	}
	s.deletes++
	return nil
}

func TestCachedStoreCachesNegativeGateAcrossL1AndRedis(t *testing.T) {
	ctx := context.Background()
	source := &gateSource{}
	shared := &memorySharedGate{values: make(map[int64]bool)}
	first, err := NewCachedStore(source, shared, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		got, err := first.HasBusinessAutomation(ctx, 42)
		if err != nil || got {
			t.Fatalf("first gate %d = %v,%v want false,nil", i, got, err)
		}
	}
	if source.hasCalls != 1 || shared.gets != 1 || shared.puts != 1 {
		t.Fatalf("cold/L1 calls source=%d redis_get=%d redis_put=%d want 1/1/1", source.hasCalls, shared.gets, shared.puts)
	}

	second, err := NewCachedStore(source, shared, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := second.HasBusinessAutomation(ctx, 42)
	if err != nil || got {
		t.Fatalf("second instance gate = %v,%v want Redis negative hit", got, err)
	}
	if source.hasCalls != 1 || shared.gets != 2 || shared.puts != 1 {
		t.Fatalf("Redis L2 hit calls source=%d get=%d put=%d", source.hasCalls, shared.gets, shared.puts)
	}
}

func TestCachedStoreWriteInvalidatesAndRebuildsExactGate(t *testing.T) {
	ctx := context.Background()
	source := &gateSource{}
	shared := &memorySharedGate{values: make(map[int64]bool)}
	cached, err := NewCachedStore(source, shared, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := cached.HasBusinessAutomation(ctx, 7); err != nil || got {
		t.Fatalf("initial gate = %v,%v", got, err)
	}
	if err := cached.SaveBusinessProfile(ctx, domain.BusinessProfile{
		UserID: 7,
		Greeting: &domain.BusinessGreetingMessage{
			ShortcutID: 1,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if got, err := cached.HasBusinessAutomation(ctx, 7); err != nil || !got {
		t.Fatalf("gate after write = %v,%v want true,nil", got, err)
	}
	if source.hasCalls != 2 || shared.deletes != 1 || shared.puts != 2 {
		t.Fatalf("calls after invalidation source=%d deletes=%d puts=%d want 2/1/2", source.hasCalls, shared.deletes, shared.puts)
	}
}

func TestCachedStoreDoesNotFallbackToSQLWhenRedisFails(t *testing.T) {
	sharedErr := errors.New("redis unavailable")
	source := &gateSource{has: true}
	shared := &memorySharedGate{values: make(map[int64]bool), getErr: sharedErr}
	cached, err := NewCachedStore(source, shared, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cached.HasBusinessAutomation(context.Background(), 9); !errors.Is(err, sharedErr) {
		t.Fatalf("gate error = %v, want Redis error", err)
	}
	if source.hasCalls != 0 {
		t.Fatalf("SQL source called %d times after Redis failure; fallback is forbidden", source.hasCalls)
	}
}
