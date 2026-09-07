package storetest

import (
	"context"
	"fmt"
	"sync"
	"time"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

type BotCallbackRegistryStore struct {
	mu      sync.Mutex
	pending map[int64]store.BotCallbackPending
	answers map[int64]domain.BotCallbackAnswer
	waiters map[int64][]chan struct{}
}

func NewBotCallbackRegistryStore() *BotCallbackRegistryStore {
	return &BotCallbackRegistryStore{
		pending: make(map[int64]store.BotCallbackPending),
		answers: make(map[int64]domain.BotCallbackAnswer),
		waiters: make(map[int64][]chan struct{}),
	}
}

func (s *BotCallbackRegistryStore) PutBotCallbackPending(_ context.Context, pending store.BotCallbackPending, ttl time.Duration) (bool, error) {
	if s == nil || pending.QueryID == 0 || pending.BotUserID <= 0 || pending.UserID <= 0 || ttl <= 0 {
		return false, fmt.Errorf("invalid bot callback pending")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.pending[pending.QueryID]; exists {
		return false, nil
	}
	s.pending[pending.QueryID] = pending
	return true, nil
}

func (s *BotCallbackRegistryStore) ResolveBotCallback(_ context.Context, botUserID, queryID int64, answer domain.BotCallbackAnswer) (bool, error) {
	if s == nil || botUserID <= 0 || queryID == 0 {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	pending, exists := s.pending[queryID]
	if !exists || pending.BotUserID != botUserID {
		return false, nil
	}
	if _, answered := s.answers[queryID]; answered {
		return false, nil
	}
	s.answers[queryID] = answer
	s.wakeLocked(queryID)
	return true, nil
}

func (s *BotCallbackRegistryStore) GetBotCallbackAnswer(_ context.Context, botUserID, queryID int64) (domain.BotCallbackAnswer, bool, error) {
	if s == nil || botUserID <= 0 || queryID == 0 {
		return domain.BotCallbackAnswer{}, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	pending, exists := s.pending[queryID]
	if !exists || pending.BotUserID != botUserID {
		return domain.BotCallbackAnswer{}, false, nil
	}
	answer, found := s.answers[queryID]
	return answer, found, nil
}

func (s *BotCallbackRegistryStore) WaitBotCallbackAnswer(ctx context.Context, botUserID, queryID int64) (domain.BotCallbackAnswer, bool, error) {
	if s == nil || botUserID <= 0 || queryID == 0 {
		return domain.BotCallbackAnswer{}, false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		s.mu.Lock()
		pending, exists := s.pending[queryID]
		if !exists || pending.BotUserID != botUserID {
			s.mu.Unlock()
			return domain.BotCallbackAnswer{}, false, nil
		}
		if answer, found := s.answers[queryID]; found {
			s.mu.Unlock()
			return answer, true, nil
		}
		ch := make(chan struct{})
		s.waiters[queryID] = append(s.waiters[queryID], ch)
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			s.removeWaiter(queryID, ch)
			return domain.BotCallbackAnswer{}, false, nil
		case <-ch:
		}
	}
}

func (s *BotCallbackRegistryStore) DeleteBotCallbackPending(_ context.Context, botUserID, queryID int64) error {
	if s == nil || botUserID <= 0 || queryID == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	pending, exists := s.pending[queryID]
	if !exists || pending.BotUserID != botUserID {
		return nil
	}
	delete(s.pending, queryID)
	delete(s.answers, queryID)
	s.wakeLocked(queryID)
	return nil
}

func (s *BotCallbackRegistryStore) wakeLocked(queryID int64) {
	for _, ch := range s.waiters[queryID] {
		close(ch)
	}
	delete(s.waiters, queryID)
}

func (s *BotCallbackRegistryStore) removeWaiter(queryID int64, ch chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	waiters := s.waiters[queryID]
	for i, waiter := range waiters {
		if waiter == ch {
			waiters = append(waiters[:i], waiters[i+1:]...)
			break
		}
	}
	if len(waiters) == 0 {
		delete(s.waiters, queryID)
		return
	}
	s.waiters[queryID] = waiters
}
