package sfu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrNilOwnerRedisClient = errors.New("sfu owner registry: nil redis client")

type RedisOwnerRegistry struct {
	c      redis.UniversalClient
	prefix string
	now    func() time.Time
}

type RedisOwnerOption func(*RedisOwnerRegistry)

func WithRedisOwnerPrefix(prefix string) RedisOwnerOption {
	return func(r *RedisOwnerRegistry) {
		prefix = strings.TrimSpace(prefix)
		if prefix != "" {
			r.prefix = strings.TrimRight(prefix, ":")
		}
	}
}

func WithRedisOwnerNow(now func() time.Time) RedisOwnerOption {
	return func(r *RedisOwnerRegistry) {
		if now != nil {
			r.now = now
		}
	}
}

func NewRedisOwnerRegistry(c redis.UniversalClient, opts ...RedisOwnerOption) *RedisOwnerRegistry {
	r := &RedisOwnerRegistry{
		c:      c,
		prefix: "sfu:owner",
		now:    time.Now,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}
	return r
}

func (r *RedisOwnerRegistry) Claim(ctx context.Context, record OwnerRecord, ttl time.Duration) (OwnerRecord, bool, error) {
	if r == nil || r.c == nil {
		return OwnerRecord{}, false, ErrNilOwnerRedisClient
	}
	record.InstanceID = strings.TrimSpace(record.InstanceID)
	if ttl <= 0 || record.CallID <= 0 || record.InstanceID == "" {
		return OwnerRecord{}, false, ErrInvalidOwnerRecord
	}
	for attempt := 0; attempt < 2; attempt++ {
		record = r.stamp(record, ttl)
		raw, err := json.Marshal(record)
		if err != nil {
			return OwnerRecord{}, false, fmt.Errorf("marshal sfu owner: %w", err)
		}
		created, err := r.c.SetNX(ctx, r.callKey(record.CallID), raw, ttl).Result()
		if err != nil {
			return OwnerRecord{}, false, fmt.Errorf("redis claim sfu owner: %w", err)
		}
		if created {
			if err := r.index(ctx, record, ttl); err != nil {
				return OwnerRecord{}, false, err
			}
			return record, true, nil
		}
		existing, found, err := r.Get(ctx, record.CallID)
		if err != nil {
			return OwnerRecord{}, false, err
		}
		if !found {
			continue
		}
		if existing.InstanceID != record.InstanceID {
			return existing, false, nil
		}
		if err := r.c.Set(ctx, r.callKey(record.CallID), raw, ttl).Err(); err != nil {
			return OwnerRecord{}, false, fmt.Errorf("redis refresh sfu owner: %w", err)
		}
		if err := r.index(ctx, record, ttl); err != nil {
			return OwnerRecord{}, false, err
		}
		return record, true, nil
	}
	return OwnerRecord{}, false, fmt.Errorf("sfu owner: claim race exhausted for call %d", record.CallID)
}

func (r *RedisOwnerRegistry) Get(ctx context.Context, callID int64) (OwnerRecord, bool, error) {
	if r == nil || r.c == nil {
		return OwnerRecord{}, false, ErrNilOwnerRedisClient
	}
	if callID <= 0 {
		return OwnerRecord{}, false, ErrInvalidOwnerRecord
	}
	raw, err := r.c.Get(ctx, r.callKey(callID)).Bytes()
	if errors.Is(err, redis.Nil) {
		return OwnerRecord{}, false, nil
	}
	if err != nil {
		return OwnerRecord{}, false, fmt.Errorf("redis get sfu owner: %w", err)
	}
	var record OwnerRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		_ = r.c.Del(ctx, r.callKey(callID)).Err()
		return OwnerRecord{}, false, fmt.Errorf("unmarshal sfu owner: %w", err)
	}
	if record.ExpiresAtUnix > 0 && record.ExpiresAtUnix <= r.now().UTC().Unix() {
		_ = r.c.Del(ctx, r.callKey(callID)).Err()
		return OwnerRecord{}, false, nil
	}
	return record, true, nil
}

func (r *RedisOwnerRegistry) Release(ctx context.Context, callID int64, instanceID string) error {
	if r == nil || r.c == nil {
		return ErrNilOwnerRedisClient
	}
	instanceID = strings.TrimSpace(instanceID)
	if callID <= 0 || instanceID == "" {
		return ErrInvalidOwnerRecord
	}
	record, found, err := r.Get(ctx, callID)
	if err != nil || !found {
		return err
	}
	if record.InstanceID != instanceID {
		return nil
	}
	pipe := r.c.Pipeline()
	pipe.Del(ctx, r.callKey(callID))
	pipe.SRem(ctx, r.instanceKey(instanceID), r.callKey(callID))
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis release sfu owner: %w", err)
	}
	return nil
}

func (r *RedisOwnerRegistry) ListInstance(ctx context.Context, instanceID string) ([]OwnerRecord, error) {
	if r == nil || r.c == nil {
		return nil, ErrNilOwnerRedisClient
	}
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" {
		return nil, ErrInvalidOwnerRecord
	}
	keys, err := r.c.SMembers(ctx, r.instanceKey(instanceID)).Result()
	if err != nil {
		return nil, fmt.Errorf("redis list sfu owner index: %w", err)
	}
	out := make([]OwnerRecord, 0, len(keys))
	stale := make([]any, 0)
	for _, key := range keys {
		raw, err := r.c.Get(ctx, key).Bytes()
		if errors.Is(err, redis.Nil) {
			stale = append(stale, key)
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("redis get sfu owner index member: %w", err)
		}
		var record OwnerRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			stale = append(stale, key)
			continue
		}
		if record.InstanceID != instanceID || (record.ExpiresAtUnix > 0 && record.ExpiresAtUnix <= r.now().UTC().Unix()) {
			stale = append(stale, key)
			continue
		}
		out = append(out, record)
	}
	if len(stale) > 0 {
		_ = r.c.SRem(ctx, r.instanceKey(instanceID), stale...).Err()
	}
	return out, nil
}

func (r *RedisOwnerRegistry) stamp(record OwnerRecord, ttl time.Duration) OwnerRecord {
	now := r.now().UTC()
	record.UpdatedAtUnix = now.Unix()
	record.ExpiresAtUnix = now.Add(ttl).Unix()
	return record
}

func (r *RedisOwnerRegistry) index(ctx context.Context, record OwnerRecord, ttl time.Duration) error {
	indexTTL := ttl * 2
	if indexTTL <= 0 {
		indexTTL = ttl
	}
	pipe := r.c.Pipeline()
	pipe.SAdd(ctx, r.instanceKey(record.InstanceID), r.callKey(record.CallID))
	pipe.Expire(ctx, r.instanceKey(record.InstanceID), indexTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis index sfu owner: %w", err)
	}
	return nil
}

func (r *RedisOwnerRegistry) callKey(callID int64) string {
	return fmt.Sprintf("%s:call:%d", r.prefix, callID)
}

func (r *RedisOwnerRegistry) instanceKey(instanceID string) string {
	return fmt.Sprintf("%s:instance:%s", r.prefix, instanceID)
}
