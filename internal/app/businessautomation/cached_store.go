package businessautomation

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/domain"
	"telesrv/internal/readmodelcache"
	"telesrv/internal/store"
)

const (
	gateL1MaxEntries = 65536
	gateL1TTL        = 24 * time.Hour
)

// SharedGateCache is the Redis L2 for the scalar business-automation gate.
// A miss is distinct from a cached false value so negative results are reusable.
type SharedGateCache interface {
	GetBusinessAutomationGate(context.Context, int64) (value bool, found bool, err error)
	PutBusinessAutomationGate(context.Context, int64, bool) error
	DeleteBusinessAutomationGate(context.Context, ...int64) error
}

// CachedStore decorates the one production BusinessAutomationStore with a
// bounded L1 + Redis L2 for HasBusinessAutomation. All other operations are
// promoted from the embedded store; the three writes that can change the gate
// invalidate both cache levels after the durable mutation commits.
type CachedStore struct {
	store.BusinessAutomationStore
	shared SharedGateCache
	gate   *readmodelcache.Cache[int64, bool]
	log    *zap.Logger
}

func NewCachedStore(inner store.BusinessAutomationStore, shared SharedGateCache, log *zap.Logger) (*CachedStore, error) {
	if inner == nil || shared == nil {
		return nil, fmt.Errorf("business automation cached store requires PostgreSQL source and Redis cache")
	}
	if log == nil {
		log = zap.NewNop()
	}
	return &CachedStore{
		BusinessAutomationStore: inner,
		shared:                  shared,
		gate: readmodelcache.New(readmodelcache.Config[int64, bool]{
			MaxEntries: gateL1MaxEntries,
			TTL:        gateL1TTL,
			KeyString:  func(userID int64) string { return strconv.FormatInt(userID, 10) },
		}),
		log: log,
	}, nil
}

func (s *CachedStore) HasBusinessAutomation(ctx context.Context, userID int64) (bool, error) {
	if s == nil || s.BusinessAutomationStore == nil || s.shared == nil || s.gate == nil || userID <= 0 {
		return false, fmt.Errorf("business automation gate is not configured")
	}
	return s.gate.GetOrLoad(ctx, userID, func() (bool, error) {
		value, found, err := s.shared.GetBusinessAutomationGate(ctx, userID)
		if err != nil {
			return false, err
		}
		if found {
			return value, nil
		}
		value, err = s.BusinessAutomationStore.HasBusinessAutomation(ctx, userID)
		if err != nil {
			return false, err
		}
		if err := s.shared.PutBusinessAutomationGate(ctx, userID, value); err != nil {
			return false, err
		}
		return value, nil
	})
}

func (s *CachedStore) SaveBusinessProfile(ctx context.Context, profile domain.BusinessProfile) error {
	if err := s.BusinessAutomationStore.SaveBusinessProfile(ctx, profile); err != nil {
		return err
	}
	s.invalidateAfterWrite(ctx, profile.UserID)
	return nil
}

func (s *CachedStore) SaveConnectedBusinessBot(ctx context.Context, bot domain.ConnectedBusinessBot) (domain.ConnectedBusinessBot, error) {
	saved, err := s.BusinessAutomationStore.SaveConnectedBusinessBot(ctx, bot)
	if err != nil {
		return domain.ConnectedBusinessBot{}, err
	}
	s.invalidateAfterWrite(ctx, saved.OwnerUserID)
	return saved, nil
}

func (s *CachedStore) DeleteConnectedBusinessBot(ctx context.Context, ownerUserID, botUserID int64) (bool, error) {
	deleted, err := s.BusinessAutomationStore.DeleteConnectedBusinessBot(ctx, ownerUserID, botUserID)
	if err != nil {
		return false, err
	}
	if deleted {
		s.invalidateAfterWrite(ctx, ownerUserID)
	}
	return deleted, nil
}

func (s *CachedStore) invalidateAfterWrite(ctx context.Context, userID int64) {
	if err := s.InvalidateBusinessAutomationReadModel(ctx, userID); err != nil {
		// The PostgreSQL trigger has already created a durable relay record. A
		// Redis failure must not turn a committed account mutation into a false
		// RPC failure; the relay retries the same exact invalidation.
		s.log.Error("invalidate business automation gate after committed write",
			zap.Int64("owner_user_id", userID), zap.Error(err))
	}
}

// InvalidateBusinessAutomationReadModel is called by both the direct write
// path and cross-process read-model listeners. Invalidating L1 on both sides of
// the Redis delete closes the miss -> stale-L2 -> refill race.
func (s *CachedStore) InvalidateBusinessAutomationReadModel(ctx context.Context, userID int64) error {
	if s == nil || userID <= 0 {
		return nil
	}
	s.gate.Invalidate(userID)
	err := s.shared.DeleteBusinessAutomationGate(ctx, userID)
	s.gate.Invalidate(userID)
	return err
}

func (s *CachedStore) FlushBusinessAutomationReadModel() {
	if s == nil {
		return
	}
	s.gate.Flush()
}

var _ store.BusinessAutomationStore = (*CachedStore)(nil)
