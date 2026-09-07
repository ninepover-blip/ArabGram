package sfu

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrInvalidInstanceRecord  = errors.New("sfu instance: invalid instance record")
	ErrNoAvailableInstance    = errors.New("sfu instance: no available instance")
	ErrNilInstanceRedisClient = errors.New("sfu instance registry: nil redis client")
	ErrInstanceHeartbeatStale = errors.New("sfu instance heartbeat: stale")
)

const defaultInstanceHealthTimeout = time.Second

// InstanceRecord describes a live SFU media process that can own group-call rooms.
// It is discovery metadata only; business state remains in Core/group-call store.
type InstanceRecord struct {
	InstanceID    string `json:"instance_id"`
	ControlAddr   string `json:"control_addr"`
	AdvertiseIP   string `json:"advertise_ip,omitempty"`
	UDPPort       int    `json:"udp_port,omitempty"`
	ActiveCalls   int    `json:"active_calls,omitempty"`
	MaxCalls      int    `json:"max_calls,omitempty"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
	ExpiresAtUnix int64  `json:"expires_at_unix"`
}

type InstanceRegistry interface {
	Register(ctx context.Context, record InstanceRecord, ttl time.Duration) error
	List(ctx context.Context) ([]InstanceRecord, error)
	Unregister(ctx context.Context, instanceID string) error
}

type InstanceHeartbeatObserver interface {
	ObserveInstanceHeartbeat(err error)
}

type InstanceHeartbeatStatus struct {
	mu          sync.Mutex
	now         func() time.Time
	maxStale    time.Duration
	lastSuccess time.Time
	lastErr     error
}

func NewInstanceHeartbeatStatus(maxStale time.Duration) *InstanceHeartbeatStatus {
	return &InstanceHeartbeatStatus{
		now:      time.Now,
		maxStale: maxStale,
	}
}

func (s *InstanceHeartbeatStatus) ObserveInstanceHeartbeat(err error) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		s.lastErr = err
		return
	}
	s.lastSuccess = s.now().UTC()
	s.lastErr = nil
}

func (s *InstanceHeartbeatStatus) Ready(context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastErr != nil {
		return s.lastErr
	}
	if s.lastSuccess.IsZero() {
		return ErrInstanceHeartbeatStale
	}
	if s.maxStale > 0 && !s.lastSuccess.Add(s.maxStale).After(s.now().UTC()) {
		return ErrInstanceHeartbeatStale
	}
	return nil
}

func RunInstanceHeartbeat(ctx context.Context, registry InstanceRegistry, record InstanceRecord, ttl, interval time.Duration) {
	RunInstanceHeartbeatWithActiveCalls(ctx, registry, record, ttl, interval, nil)
}

func RunInstanceHeartbeatWithActiveCalls(ctx context.Context, registry InstanceRegistry, record InstanceRecord, ttl, interval time.Duration, active ActiveCallProvider) {
	RunInstanceHeartbeatWithActiveCallsAndObserver(ctx, registry, record, ttl, interval, active, nil)
}

func RunInstanceHeartbeatWithActiveCallsAndObserver(ctx context.Context, registry InstanceRegistry, record InstanceRecord, ttl, interval time.Duration, active ActiveCallProvider, observer InstanceHeartbeatObserver) {
	if registry == nil {
		return
	}
	if interval <= 0 || interval >= ttl {
		interval = ttl / 3
	}
	if interval <= 0 {
		interval = time.Second
	}
	refresh := func() {
		current := record
		if active != nil {
			current.ActiveCalls = len(uniqueActiveCallIDs(active.ActiveCallIDs()))
		}
		err := registry.Register(ctx, current, ttl)
		if observer != nil {
			observer.ObserveInstanceHeartbeat(err)
		}
	}
	refresh()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			_ = registry.Unregister(context.Background(), record.InstanceID)
			return
		case <-ticker.C:
			refresh()
		}
	}
}

type MemoryInstanceRegistry struct {
	mu        sync.Mutex
	instances map[string]InstanceRecord
	now       func() time.Time
}

func NewMemoryInstanceRegistry() *MemoryInstanceRegistry {
	return &MemoryInstanceRegistry{instances: make(map[string]InstanceRecord), now: time.Now}
}

func (r *MemoryInstanceRegistry) Register(_ context.Context, record InstanceRecord, ttl time.Duration) error {
	record.InstanceID = strings.TrimSpace(record.InstanceID)
	record.ControlAddr = strings.TrimSpace(record.ControlAddr)
	if r == nil || ttl <= 0 || record.InstanceID == "" || record.ControlAddr == "" || record.ActiveCalls < 0 || record.MaxCalls < 0 {
		return ErrInvalidInstanceRecord
	}
	now := r.now().UTC()
	record.UpdatedAtUnix = now.Unix()
	record.ExpiresAtUnix = now.Add(ttl).Unix()
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, existing := range r.instances {
		if id != record.InstanceID && strings.TrimSpace(existing.ControlAddr) == record.ControlAddr {
			delete(r.instances, id)
		}
	}
	r.instances[record.InstanceID] = record
	return nil
}

func (r *MemoryInstanceRegistry) List(_ context.Context) ([]InstanceRecord, error) {
	if r == nil {
		return nil, ErrInvalidInstanceRecord
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now().UTC().Unix()
	out := make([]InstanceRecord, 0, len(r.instances))
	for id, record := range r.instances {
		if record.ExpiresAtUnix > 0 && record.ExpiresAtUnix <= now {
			delete(r.instances, id)
			continue
		}
		out = append(out, record)
	}
	sortInstanceRecords(out)
	return out, nil
}

func (r *MemoryInstanceRegistry) Unregister(_ context.Context, instanceID string) error {
	if r == nil || strings.TrimSpace(instanceID) == "" {
		return ErrInvalidInstanceRecord
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.instances, strings.TrimSpace(instanceID))
	return nil
}

type RedisInstanceRegistry struct {
	c      redis.UniversalClient
	prefix string
	now    func() time.Time
}

type RedisInstanceOption func(*RedisInstanceRegistry)

func WithRedisInstancePrefix(prefix string) RedisInstanceOption {
	return func(r *RedisInstanceRegistry) {
		prefix = strings.TrimSpace(prefix)
		if prefix != "" {
			r.prefix = strings.TrimRight(prefix, ":")
		}
	}
}

func WithRedisInstanceNow(now func() time.Time) RedisInstanceOption {
	return func(r *RedisInstanceRegistry) {
		if now != nil {
			r.now = now
		}
	}
}

func NewRedisInstanceRegistry(c redis.UniversalClient, opts ...RedisInstanceOption) *RedisInstanceRegistry {
	r := &RedisInstanceRegistry{
		c:      c,
		prefix: "sfu:instance",
		now:    time.Now,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}
	return r
}

func (r *RedisInstanceRegistry) Register(ctx context.Context, record InstanceRecord, ttl time.Duration) error {
	if r == nil || r.c == nil {
		return ErrNilInstanceRedisClient
	}
	record.InstanceID = strings.TrimSpace(record.InstanceID)
	record.ControlAddr = strings.TrimSpace(record.ControlAddr)
	if ttl <= 0 || record.InstanceID == "" || record.ControlAddr == "" || record.ActiveCalls < 0 || record.MaxCalls < 0 {
		return ErrInvalidInstanceRecord
	}
	now := r.now().UTC()
	record.UpdatedAtUnix = now.Unix()
	record.ExpiresAtUnix = now.Add(ttl).Unix()
	raw, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal sfu instance: %w", err)
	}
	recordKey := r.recordKey(record.InstanceID)
	previousKey, err := r.c.Get(ctx, r.controlKey(record.ControlAddr)).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("redis get sfu instance control index: %w", err)
	}
	pipe := r.c.Pipeline()
	if previousKey != "" && previousKey != recordKey {
		pipe.Del(ctx, previousKey)
		pipe.SRem(ctx, r.allKey(), previousKey)
	}
	pipe.Set(ctx, recordKey, raw, ttl)
	pipe.SAdd(ctx, r.allKey(), recordKey)
	pipe.Expire(ctx, r.allKey(), ttl*2)
	pipe.Set(ctx, r.controlKey(record.ControlAddr), recordKey, ttl*2)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis register sfu instance: %w", err)
	}
	return nil
}

func (r *RedisInstanceRegistry) List(ctx context.Context) ([]InstanceRecord, error) {
	if r == nil || r.c == nil {
		return nil, ErrNilInstanceRedisClient
	}
	keys, err := r.c.SMembers(ctx, r.allKey()).Result()
	if err != nil {
		return nil, fmt.Errorf("redis list sfu instances: %w", err)
	}
	out := make([]InstanceRecord, 0, len(keys))
	stale := make([]any, 0)
	now := r.now().UTC().Unix()
	for _, key := range keys {
		raw, err := r.c.Get(ctx, key).Bytes()
		if errors.Is(err, redis.Nil) {
			stale = append(stale, key)
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("redis get sfu instance: %w", err)
		}
		var record InstanceRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			stale = append(stale, key)
			continue
		}
		if record.InstanceID == "" || record.ControlAddr == "" || (record.ExpiresAtUnix > 0 && record.ExpiresAtUnix <= now) {
			stale = append(stale, key)
			continue
		}
		out = append(out, record)
	}
	if len(stale) > 0 {
		_ = r.c.SRem(ctx, r.allKey(), stale...).Err()
	}
	sortInstanceRecords(out)
	return out, nil
}

func (r *RedisInstanceRegistry) Unregister(ctx context.Context, instanceID string) error {
	if r == nil || r.c == nil {
		return ErrNilInstanceRedisClient
	}
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" {
		return ErrInvalidInstanceRecord
	}
	key := r.recordKey(instanceID)
	controlKeys, err := r.controlKeysForRecord(ctx, key)
	if err != nil {
		return err
	}
	pipe := r.c.Pipeline()
	pipe.Del(ctx, key)
	pipe.SRem(ctx, r.allKey(), key)
	for _, controlKey := range controlKeys {
		pipe.Del(ctx, controlKey)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis unregister sfu instance: %w", err)
	}
	return nil
}

func (r *RedisInstanceRegistry) recordKey(instanceID string) string {
	return fmt.Sprintf("%s:record:%s", r.prefix, instanceID)
}

func (r *RedisInstanceRegistry) allKey() string {
	return r.prefix + ":all"
}

func (r *RedisInstanceRegistry) controlKey(controlAddr string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(controlAddr)))
	return fmt.Sprintf("%s:control:%s", r.prefix, hex.EncodeToString(sum[:]))
}

func (r *RedisInstanceRegistry) controlKeysForRecord(ctx context.Context, recordKey string) ([]string, error) {
	raw, err := r.c.Get(ctx, recordKey).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("redis get sfu instance for unregister: %w", err)
	}
	var record InstanceRecord
	if err := json.Unmarshal(raw, &record); err != nil || strings.TrimSpace(record.ControlAddr) == "" {
		return nil, nil
	}
	controlKey := r.controlKey(record.ControlAddr)
	current, err := r.c.Get(ctx, controlKey).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("redis get sfu instance control index: %w", err)
	}
	if current != recordKey {
		return nil, nil
	}
	return []string{controlKey}, nil
}

type OwnerSelector interface {
	PickOwner(ctx context.Context, callID int64) (OwnerRecord, error)
}

type InstanceHealthChecker interface {
	CheckInstance(ctx context.Context, record InstanceRecord) error
}

type RegistryOwnerSelector struct {
	registry      InstanceRegistry
	health        InstanceHealthChecker
	healthTimeout time.Duration
}

type RegistryOwnerSelectorOption func(*RegistryOwnerSelector)

func WithInstanceHealthChecker(health InstanceHealthChecker) RegistryOwnerSelectorOption {
	return func(s *RegistryOwnerSelector) {
		s.health = health
	}
}

func WithInstanceHealthTimeout(timeout time.Duration) RegistryOwnerSelectorOption {
	return func(s *RegistryOwnerSelector) {
		s.healthTimeout = timeout
	}
}

func NewRegistryOwnerSelector(registry InstanceRegistry, opts ...RegistryOwnerSelectorOption) *RegistryOwnerSelector {
	s := &RegistryOwnerSelector{
		registry:      registry,
		healthTimeout: defaultInstanceHealthTimeout,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

func (s *RegistryOwnerSelector) PickOwner(ctx context.Context, callID int64) (OwnerRecord, error) {
	if s == nil || s.registry == nil || callID <= 0 {
		return OwnerRecord{}, ErrInvalidOwnerRecord
	}
	instances, err := s.registry.List(ctx)
	if err != nil {
		return OwnerRecord{}, err
	}
	candidates := instances[:0]
	for _, record := range instances {
		if !instanceAvailable(record) {
			continue
		}
		if !s.instanceHealthy(ctx, record) {
			continue
		}
		candidates = append(candidates, record)
	}
	if len(candidates) == 0 {
		return OwnerRecord{}, ErrNoAvailableInstance
	}
	sortInstanceRecordsByLoad(candidates)
	best := candidates[:1]
	for len(best) < len(candidates) && sameInstanceLoad(best[0], candidates[len(best)]) {
		best = candidates[:len(best)+1]
	}
	idx := int(callID % int64(len(best)))
	record := best[idx]
	return OwnerRecord{
		CallID:      callID,
		InstanceID:  record.InstanceID,
		ControlAddr: record.ControlAddr,
		AdvertiseIP: record.AdvertiseIP,
		UDPPort:     record.UDPPort,
	}, nil
}

func (s *RegistryOwnerSelector) instanceHealthy(ctx context.Context, record InstanceRecord) bool {
	if s == nil || s.health == nil {
		return true
	}
	checkCtx := ctx
	cancel := func() {}
	if s.healthTimeout > 0 {
		checkCtx, cancel = context.WithTimeout(ctx, s.healthTimeout)
	}
	defer cancel()
	return s.health.CheckInstance(checkCtx, record) == nil
}

func sortInstanceRecords(records []InstanceRecord) {
	sort.Slice(records, func(i, j int) bool {
		return records[i].InstanceID < records[j].InstanceID
	})
}

func sortInstanceRecordsByLoad(records []InstanceRecord) {
	sort.Slice(records, func(i, j int) bool {
		left, right := records[i], records[j]
		if left.ActiveCalls != right.ActiveCalls {
			return left.ActiveCalls < right.ActiveCalls
		}
		if left.MaxCalls != right.MaxCalls {
			if left.MaxCalls == 0 {
				return true
			}
			if right.MaxCalls == 0 {
				return false
			}
			return left.MaxCalls > right.MaxCalls
		}
		return left.InstanceID < right.InstanceID
	})
}

func sameInstanceLoad(left, right InstanceRecord) bool {
	return left.ActiveCalls == right.ActiveCalls && left.MaxCalls == right.MaxCalls
}

func instanceAvailable(record InstanceRecord) bool {
	if strings.TrimSpace(record.InstanceID) == "" || strings.TrimSpace(record.ControlAddr) == "" {
		return false
	}
	if record.ActiveCalls < 0 || record.MaxCalls < 0 {
		return false
	}
	return record.MaxCalls == 0 || record.ActiveCalls < record.MaxCalls
}

func uniqueActiveCallIDs(ids []int64) []int64 {
	if len(ids) == 0 {
		return nil
	}
	if len(ids) == 1 {
		if ids[0] <= 0 {
			return nil
		}
		return ids
	}
	seen := make(map[int64]struct{}, len(ids))
	out := ids[:0]
	for _, id := range ids {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
