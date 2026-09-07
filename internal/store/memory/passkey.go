package memory

import (
	"context"
	"sync"
	"time"

	"telesrv/internal/domain"
)

// PasskeyStore 是 store.PasskeyStore 的内存实现。
type PasskeyStore struct {
	mu   sync.RWMutex
	byID map[string]domain.PasskeyCredential // key = string(credentialID)
}

// NewPasskeyStore 创建内存 passkey 凭据 store。
func NewPasskeyStore() *PasskeyStore {
	return &PasskeyStore{byID: make(map[string]domain.PasskeyCredential)}
}

func (s *PasskeyStore) InsertPasskey(_ context.Context, cred domain.PasskeyCredential) error {
	if len(cred.CredentialID) == 0 || cred.UserID == 0 {
		return domain.ErrPasskeyInvalid
	}
	s.mu.Lock()
	s.byID[string(cred.CredentialID)] = cred.Clone()
	s.mu.Unlock()
	return nil
}

func (s *PasskeyStore) GetPasskeyByCredentialID(_ context.Context, credentialID []byte) (domain.PasskeyCredential, bool, error) {
	s.mu.RLock()
	cred, ok := s.byID[string(credentialID)]
	s.mu.RUnlock()
	if !ok {
		return domain.PasskeyCredential{}, false, nil
	}
	return cred.Clone(), true, nil
}

func (s *PasskeyStore) ListPasskeysByUser(_ context.Context, userID int64) ([]domain.PasskeyCredential, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]domain.PasskeyCredential, 0)
	for _, cred := range s.byID {
		if cred.UserID == userID {
			out = append(out, cred.Clone())
		}
	}
	return out, nil
}

func (s *PasskeyStore) UpdatePasskeyUsage(_ context.Context, credentialID []byte, signCount uint32, lastUsedAt int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cred, ok := s.byID[string(credentialID)]
	if !ok {
		return domain.ErrPasskeyNotFound
	}
	cred.SignCount = signCount
	cred.LastUsedAt = lastUsedAt
	s.byID[string(credentialID)] = cred
	return nil
}

func (s *PasskeyStore) DeletePasskey(_ context.Context, userID int64, credentialID []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cred, ok := s.byID[string(credentialID)]
	if !ok || cred.UserID != userID {
		return false, nil
	}
	delete(s.byID, string(credentialID))
	return true, nil
}

// PasskeyChallengeStore 是 store.PasskeyChallengeStore 的内存实现(进程内、短 TTL)。
// 与 QR 登录 token 同属进程内一次性凭据,不跨实例。
type PasskeyChallengeStore struct {
	mu sync.Mutex
	m  map[string]challengeEntry // key = string(challenge)
}

type challengeEntry struct {
	c         domain.PasskeyChallenge
	expiresAt time.Time
}

// NewPasskeyChallengeStore 创建内存挑战 store。
func NewPasskeyChallengeStore() *PasskeyChallengeStore {
	return &PasskeyChallengeStore{m: make(map[string]challengeEntry)}
}

func (s *PasskeyChallengeStore) SavePasskeyChallenge(_ context.Context, challenge []byte, c domain.PasskeyChallenge, ttl time.Duration) error {
	if len(challenge) == 0 {
		return domain.ErrPasskeyChallengeInvalid
	}
	s.mu.Lock()
	s.m[string(challenge)] = challengeEntry{c: c, expiresAt: time.Now().Add(ttl)}
	s.mu.Unlock()
	return nil
}

func (s *PasskeyChallengeStore) ConsumePasskeyChallenge(_ context.Context, challenge []byte) (domain.PasskeyChallenge, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.m[string(challenge)]
	if !ok {
		return domain.PasskeyChallenge{}, false, nil
	}
	delete(s.m, string(challenge)) // 一次性:无论是否过期都删除
	if time.Now().After(entry.expiresAt) {
		return domain.PasskeyChallenge{}, false, nil
	}
	return entry.c, true, nil
}
