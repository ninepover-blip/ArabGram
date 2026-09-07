package redisstore

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"telesrv/internal/store"
)

const (
	authKeyLayerIdentityPrefix = "protocol:layer:identity:v1:"
	authKeyLayerSessionPrefix  = "protocol:layer:session:v1:"
	authKeyLayerDefaultPrefix  = "protocol:layer:default:v1:"
	authKeyLayerObservationKey = "protocol:layer:observation:v1"
)

// RedisAuthKeySessionLayerStore is the production authority for exact-session
// Layer watermarks and auth-key-wide defaults. PostgreSQL is consulted only on
// an identity-key cold miss; Redis errors never fall back to PostgreSQL state.
type RedisAuthKeySessionLayerStore struct {
	c        redis.UniversalClient
	identity store.AuthKeyLayerIdentitySource
}

func NewAuthKeySessionLayerStore(
	c redis.UniversalClient,
	identity store.AuthKeyLayerIdentitySource,
) (*RedisAuthKeySessionLayerStore, error) {
	if c == nil || identity == nil {
		return nil, store.ErrAuthKeySessionLayerStoreRequired
	}
	return &RedisAuthKeySessionLayerStore{c: c, identity: identity}, nil
}

var getAuthKeySessionLayerScript = redis.NewScript(`
local effective = redis.call('GET', KEYS[1])
if not effective then
  return {'identity_missing'}
end
local encoded = redis.call('GET', KEYS[2])
if not encoded then
  return {'not_found'}
end
local ttl = redis.call('PTTL', KEYS[2])
if ttl <= 0 then
  redis.call('DEL', KEYS[2])
  return {'not_found'}
end
local layer, msg, observation, expires = string.match(encoded, '^(%-?%d+)|(%d+)|(%d+)|(%d+)$')
if not layer then
  return {'corrupt'}
end
local shared = '0'
local default = redis.call('GET', ARGV[1] .. effective)
if default then
  local default_layer, default_observation = string.match(default, '^(%-?%d+)|(%d+)$')
  if not default_layer then
    return {'corrupt'}
  end
  if default_layer == layer and default_observation == observation then
    shared = '1'
  end
end
return {'ok', layer, msg, observation, expires, shared}
`)

var advanceAuthKeySessionLayerScript = redis.NewScript(`
local effective = redis.call('GET', KEYS[1])
if not effective then
  return {'identity_missing'}
end
local redis_time = redis.call('TIME')
local now_ms = tonumber(redis_time[1]) * 1000 + math.floor(tonumber(redis_time[2]) / 1000)
local expires_ms = tonumber(ARGV[4])
local created_ms = tonumber(ARGV[5])
if not expires_ms or not created_ms or expires_ms <= now_ms or created_ms > now_ms + 30000 then
  return {'invalid'}
end

local current = redis.call('GET', KEYS[2])
if current and redis.call('PTTL', KEYS[2]) > 0 then
  local current_layer, current_msg, current_observation, current_expires = string.match(current, '^(%-?%d+)|(%d+)|(%d+)|(%d+)$')
  if not current_layer then
    return {'corrupt'}
  end
  if ARGV[2] < current_msg then
    local shared = '0'
    local default = redis.call('GET', ARGV[6] .. effective)
    if default then
      local default_layer, default_observation = string.match(default, '^(%-?%d+)|(%d+)$')
      if not default_layer then return {'corrupt'} end
      if default_layer == current_layer and default_observation == current_observation then shared = '1' end
    end
    return {'ok', current_layer, current_msg, current_observation, current_expires, shared, '0'}
  end
  if ARGV[2] == current_msg then
    if ARGV[1] ~= current_layer then
      return {'conflict', current_layer, current_msg, current_observation, current_expires}
    end
    local shared = '0'
    local default = redis.call('GET', ARGV[6] .. effective)
    if default then
      local default_layer, default_observation = string.match(default, '^(%-?%d+)|(%d+)$')
      if not default_layer then return {'corrupt'} end
      if default_layer == current_layer and default_observation == current_observation then shared = '1' end
    end
    return {'ok', current_layer, current_msg, current_observation, current_expires, shared, '0'}
  end
end

local observation_number = redis.call('INCR', KEYS[3])
local observation = string.format('%020d', observation_number)
local encoded = ARGV[1] .. '|' .. ARGV[2] .. '|' .. observation .. '|' .. ARGV[4]
redis.call('SET', KEYS[2], encoded, 'PXAT', ARGV[4])
redis.call('SET', ARGV[6] .. effective, ARGV[1] .. '|' .. observation)
return {'ok', ARGV[1], ARGV[2], observation, ARGV[4], '1', '1'}
`)

var getAuthKeyLayerDefaultScript = redis.NewScript(`
local effective = redis.call('GET', KEYS[1])
if not effective then
  return {'identity_missing'}
end
local encoded = redis.call('GET', ARGV[1] .. effective)
if not encoded then
  return {'not_found'}
end
local layer, observation = string.match(encoded, '^(%-?%d+)|(%d+)$')
if not layer then
  return {'corrupt'}
end
return {'ok', layer, observation}
`)

var getAuthKeyLayerIdentityScript = redis.NewScript(`
local effective = redis.call('GET', KEYS[1])
if not effective then
  return {'identity_missing'}
end
local ttl = redis.call('PTTL', KEYS[1])
if ttl == 0 or ttl == -2 then
  return {'identity_missing'}
end
return {'ok', effective, tostring(ttl)}
`)

var bindAuthKeyLayerIdentityScript = redis.NewScript(`
local raw_default = redis.call('GET', KEYS[2])
local effective_default = redis.call('GET', KEYS[3])
local winner = nil
if raw_default then
  local layer, observation = string.match(raw_default, '^(%-?%d+)|(%d+)$')
  if not layer then return {'corrupt'} end
  winner = {layer, observation}
end
if effective_default then
  local layer, observation = string.match(effective_default, '^(%-?%d+)|(%d+)$')
  if not layer then return {'corrupt'} end
  if not winner or observation > winner[2] then
    winner = {layer, observation}
  elseif observation == winner[2] and layer ~= winner[1] then
    return {'conflict'}
  end
end
redis.call('SET', KEYS[1], ARGV[1], 'EXAT', ARGV[2])
if winner then
  local encoded = winner[1] .. '|' .. winner[2]
  redis.call('SET', KEYS[2], encoded)
  redis.call('SET', KEYS[3], encoded)
  return {'ok', winner[1], winner[2]}
end
return {'ok'}
`)

func (s *RedisAuthKeySessionLayerStore) GetSessionLayer(
	ctx context.Context,
	rawAuthKeyID [8]byte,
	sessionID int64,
) (store.AuthKeySessionLayer, bool, error) {
	if s == nil || s.c == nil || s.identity == nil {
		return store.AuthKeySessionLayer{}, false, store.ErrAuthKeySessionLayerStoreRequired
	}
	if rawAuthKeyID == ([8]byte{}) || sessionID == 0 {
		return store.AuthKeySessionLayer{}, false, store.ErrAuthKeySessionLayerInvalid
	}
	for attempt := 0; attempt < 2; attempt++ {
		values, err := getAuthKeySessionLayerScript.Run(ctx, s.c, []string{
			authKeyLayerIdentityKey(rawAuthKeyID), authKeyLayerSessionKey(rawAuthKeyID, sessionID),
		}, authKeyLayerDefaultPrefix).Slice()
		if err != nil {
			return store.AuthKeySessionLayer{}, false, fmt.Errorf("redis get auth key session Layer: %w", err)
		}
		status, err := redisLayerStatus(values)
		if err != nil {
			return store.AuthKeySessionLayer{}, false, err
		}
		switch status {
		case "identity_missing":
			if attempt != 0 {
				return store.AuthKeySessionLayer{}, false, store.ErrAuthKeySessionLayerInvalid
			}
			if err := s.installAuthKeyLayerIdentity(ctx, rawAuthKeyID); err != nil {
				return store.AuthKeySessionLayer{}, false, err
			}
			continue
		case "not_found":
			return store.AuthKeySessionLayer{}, false, nil
		case "ok":
			value, parseErr := decodeRedisSessionLayer(values)
			return value, parseErr == nil, parseErr
		default:
			return store.AuthKeySessionLayer{}, false, fmt.Errorf("redis get auth key session Layer: %s state", status)
		}
	}
	return store.AuthKeySessionLayer{}, false, store.ErrAuthKeySessionLayerInvalid
}

func (s *RedisAuthKeySessionLayerStore) AdvanceSessionLayer(
	ctx context.Context,
	rawAuthKeyID [8]byte,
	sessionID int64,
	layer int,
	msgID int64,
) (store.AuthKeySessionLayer, bool, error) {
	if s == nil || s.c == nil || s.identity == nil {
		return store.AuthKeySessionLayer{}, false, store.ErrAuthKeySessionLayerStoreRequired
	}
	expiresAt, valid := store.AuthKeySessionLayerExpiry(msgID)
	if rawAuthKeyID == ([8]byte{}) || sessionID == 0 || layer <= 0 || !valid {
		return store.AuthKeySessionLayer{}, false, store.ErrAuthKeySessionLayerInvalid
	}
	msg := fmt.Sprintf("%020d", msgID)
	expiresMillis := expiresAt.UnixMilli()
	createdMillis := expiresAt.Add(-301 * time.Second).UnixMilli()
	for attempt := 0; attempt < 2; attempt++ {
		values, err := advanceAuthKeySessionLayerScript.Run(ctx, s.c, []string{
			authKeyLayerIdentityKey(rawAuthKeyID),
			authKeyLayerSessionKey(rawAuthKeyID, sessionID),
			authKeyLayerObservationKey,
		}, strconv.Itoa(layer), msg, strconv.FormatInt(sessionID, 10),
			strconv.FormatInt(expiresMillis, 10), strconv.FormatInt(createdMillis, 10),
			authKeyLayerDefaultPrefix).Slice()
		if err != nil {
			return store.AuthKeySessionLayer{}, false, fmt.Errorf("redis advance auth key session Layer: %w", err)
		}
		status, err := redisLayerStatus(values)
		if err != nil {
			return store.AuthKeySessionLayer{}, false, err
		}
		switch status {
		case "identity_missing":
			if attempt != 0 {
				return store.AuthKeySessionLayer{}, false, store.ErrAuthKeySessionLayerInvalid
			}
			if err := s.installAuthKeyLayerIdentity(ctx, rawAuthKeyID); err != nil {
				return store.AuthKeySessionLayer{}, false, err
			}
			continue
		case "invalid":
			return store.AuthKeySessionLayer{}, false, store.ErrAuthKeySessionLayerInvalid
		case "conflict":
			current, decodeErr := decodeRedisConflictSessionLayer(values)
			return current, false, errors.Join(store.ErrAuthKeySessionLayerConflict, decodeErr)
		case "ok":
			value, parseErr := decodeRedisSessionLayer(values)
			if parseErr != nil {
				return store.AuthKeySessionLayer{}, false, parseErr
			}
			applied, parseErr := redisLayerBool(values, 6)
			return value, applied, parseErr
		default:
			return store.AuthKeySessionLayer{}, false, fmt.Errorf("redis advance auth key session Layer: %s state", status)
		}
	}
	return store.AuthKeySessionLayer{}, false, store.ErrAuthKeySessionLayerInvalid
}

func (s *RedisAuthKeySessionLayerStore) GetAuthKeyLayerDefault(
	ctx context.Context,
	rawAuthKeyID [8]byte,
) (store.AuthKeyLayerDefault, bool, error) {
	if s == nil || s.c == nil || s.identity == nil {
		return store.AuthKeyLayerDefault{}, false, store.ErrAuthKeySessionLayerStoreRequired
	}
	if rawAuthKeyID == ([8]byte{}) {
		return store.AuthKeyLayerDefault{}, false, store.ErrAuthKeySessionLayerInvalid
	}
	for attempt := 0; attempt < 2; attempt++ {
		values, err := getAuthKeyLayerDefaultScript.Run(ctx, s.c,
			[]string{authKeyLayerIdentityKey(rawAuthKeyID)}, authKeyLayerDefaultPrefix).Slice()
		if err != nil {
			return store.AuthKeyLayerDefault{}, false, fmt.Errorf("redis get auth key Layer default: %w", err)
		}
		status, err := redisLayerStatus(values)
		if err != nil {
			return store.AuthKeyLayerDefault{}, false, err
		}
		switch status {
		case "identity_missing":
			if attempt != 0 {
				return store.AuthKeyLayerDefault{}, false, store.ErrAuthKeySessionLayerInvalid
			}
			if err := s.installAuthKeyLayerIdentity(ctx, rawAuthKeyID); err != nil {
				return store.AuthKeyLayerDefault{}, false, err
			}
			continue
		case "not_found":
			return store.AuthKeyLayerDefault{}, false, nil
		case "ok":
			layer, err := redisLayerInt(values, 1)
			if err != nil || layer <= 0 {
				return store.AuthKeyLayerDefault{}, false, store.ErrAuthKeySessionLayerInvalid
			}
			observation, err := redisLayerInt64(values, 2)
			if err != nil || observation <= 0 {
				return store.AuthKeyLayerDefault{}, false, store.ErrAuthKeySessionLayerInvalid
			}
			return store.AuthKeyLayerDefault{Layer: layer, ObservationID: observation}, true, nil
		default:
			return store.AuthKeyLayerDefault{}, false, fmt.Errorf("redis get auth key Layer default: %s state", status)
		}
	}
	return store.AuthKeyLayerDefault{}, false, store.ErrAuthKeySessionLayerInvalid
}

// ResolveCachedAuthKeyLayerIdentity validates an Edge/CoreExec identity hint
// against Redis. A missing mapping is installed from the one-shot PostgreSQL
// source; a Redis error is returned directly and never triggers a PG fallback.
func (s *RedisAuthKeySessionLayerStore) ResolveCachedAuthKeyLayerIdentity(
	ctx context.Context,
	rawAuthKeyID [8]byte,
) (store.AuthKeyLayerIdentity, bool, error) {
	if s == nil || s.c == nil || s.identity == nil {
		return store.AuthKeyLayerIdentity{}, false, store.ErrAuthKeySessionLayerStoreRequired
	}
	if rawAuthKeyID == ([8]byte{}) {
		return store.AuthKeyLayerIdentity{}, false, store.ErrAuthKeySessionLayerInvalid
	}
	for attempt := 0; attempt < 2; attempt++ {
		identity, found, err := ReadInstalledAuthKeyLayerIdentity(ctx, s.c, rawAuthKeyID)
		if err != nil {
			return store.AuthKeyLayerIdentity{}, false, err
		}
		if !found {
			if attempt != 0 {
				return store.AuthKeyLayerIdentity{}, false, store.ErrAuthKeyNotFound
			}
			if err := s.installAuthKeyLayerIdentity(ctx, rawAuthKeyID); err != nil {
				return store.AuthKeyLayerIdentity{}, false, err
			}
			continue
		}
		return identity, true, nil
	}
	return store.AuthKeyLayerIdentity{}, false, store.ErrAuthKeyNotFound
}

// ReadInstalledAuthKeyLayerIdentity reads only the Redis raw-to-canonical
// mapping. It never invokes a cold source and is therefore safe for Edge to use
// after Core has explicitly primed a temporary-key identity. A missing mapping
// is returned as found=false; Redis and structural errors remain fail-closed.
func ReadInstalledAuthKeyLayerIdentity(
	ctx context.Context,
	c redis.UniversalClient,
	rawAuthKeyID [8]byte,
) (store.AuthKeyLayerIdentity, bool, error) {
	if c == nil {
		return store.AuthKeyLayerIdentity{}, false, store.ErrAuthKeySessionLayerStoreRequired
	}
	if rawAuthKeyID == ([8]byte{}) {
		return store.AuthKeyLayerIdentity{}, false, store.ErrAuthKeySessionLayerInvalid
	}
	values, err := getAuthKeyLayerIdentityScript.Run(ctx, c,
		[]string{authKeyLayerIdentityKey(rawAuthKeyID)}).Slice()
	if err != nil {
		return store.AuthKeyLayerIdentity{}, false, fmt.Errorf("redis get auth key Layer identity: %w", err)
	}
	status, err := redisLayerStatus(values)
	if err != nil {
		return store.AuthKeyLayerIdentity{}, false, err
	}
	switch status {
	case "identity_missing":
		return store.AuthKeyLayerIdentity{}, false, nil
	case "ok":
		effectiveHex, err := redisLayerStringAt(values, 1)
		if err != nil {
			return store.AuthKeyLayerIdentity{}, false, err
		}
		effectiveBytes, err := hex.DecodeString(effectiveHex)
		if err != nil || len(effectiveBytes) != len(rawAuthKeyID) {
			return store.AuthKeyLayerIdentity{}, false, store.ErrAuthKeyBindingInvalid
		}
		var effective [8]byte
		copy(effective[:], effectiveBytes)
		ttlMillis, err := redisLayerInt64(values, 2)
		if err != nil || ttlMillis == 0 || ttlMillis < -1 {
			return store.AuthKeyLayerIdentity{}, false, store.ErrAuthKeyBindingInvalid
		}
		rawExpiresAt := 0
		if ttlMillis > 0 {
			rawExpiresAt = int(time.Now().Add(time.Duration(ttlMillis) * time.Millisecond).Unix())
		}
		return store.AuthKeyLayerIdentity{
			EffectiveAuthKeyID: effective,
			RawExpiresAt:       rawExpiresAt,
		}, true, nil
	default:
		return store.AuthKeyLayerIdentity{}, false, fmt.Errorf("redis get auth key Layer identity: %s state", status)
	}
}

func (s *RedisAuthKeySessionLayerStore) DeleteSessionLayer(
	ctx context.Context,
	rawAuthKeyID [8]byte,
	sessionID int64,
) (bool, error) {
	if s == nil || s.c == nil {
		return false, store.ErrAuthKeySessionLayerStoreRequired
	}
	if rawAuthKeyID == ([8]byte{}) || sessionID == 0 {
		return false, store.ErrAuthKeySessionLayerInvalid
	}
	n, err := s.c.Del(ctx, authKeyLayerSessionKey(rawAuthKeyID, sessionID)).Result()
	if err != nil {
		return false, fmt.Errorf("redis delete auth key session Layer: %w", err)
	}
	return n > 0, nil
}

// Redis owns expiry intrinsically; there is no scan/retention worker.
func (*RedisAuthKeySessionLayerStore) DeleteExpiredSessionLayers(context.Context, int) (int, error) {
	return 0, nil
}

func (s *RedisAuthKeySessionLayerStore) BindAuthKeyLayerIdentity(
	ctx context.Context,
	rawAuthKeyID, effectiveAuthKeyID [8]byte,
	rawExpiresAt int,
) error {
	if s == nil || s.c == nil {
		return store.ErrAuthKeySessionLayerStoreRequired
	}
	if rawAuthKeyID == ([8]byte{}) || effectiveAuthKeyID == ([8]byte{}) || rawExpiresAt <= 0 ||
		time.Unix(int64(rawExpiresAt), 0).Before(time.Now()) {
		return store.ErrAuthKeyBindingInvalid
	}
	rawHex := authKeyLayerID(rawAuthKeyID)
	effectiveHex := authKeyLayerID(effectiveAuthKeyID)
	values, err := bindAuthKeyLayerIdentityScript.Run(ctx, s.c, []string{
		authKeyLayerIdentityKey(rawAuthKeyID),
		authKeyLayerDefaultPrefix + rawHex,
		authKeyLayerDefaultPrefix + effectiveHex,
	}, effectiveHex, strconv.Itoa(rawExpiresAt)).Slice()
	if err != nil {
		return fmt.Errorf("redis bind auth key Layer identity: %w", err)
	}
	status, err := redisLayerStatus(values)
	if err != nil {
		return err
	}
	switch status {
	case "ok":
		return nil
	case "conflict":
		return store.ErrAuthKeySessionLayerConflict
	default:
		return fmt.Errorf("redis bind auth key Layer identity: %s state", status)
	}
}

func (s *RedisAuthKeySessionLayerStore) installAuthKeyLayerIdentity(ctx context.Context, rawAuthKeyID [8]byte) error {
	identity, found, err := s.identity.ResolveAuthKeyLayerIdentity(ctx, rawAuthKeyID)
	if err != nil {
		return fmt.Errorf("load auth key Layer identity: %w", err)
	}
	if !found {
		return store.ErrAuthKeyNotFound
	}
	if identity.EffectiveAuthKeyID == ([8]byte{}) || identity.RawExpiresAt < 0 {
		return store.ErrAuthKeyBindingInvalid
	}
	if identity.EffectiveAuthKeyID != rawAuthKeyID {
		return s.BindAuthKeyLayerIdentity(ctx, rawAuthKeyID, identity.EffectiveAuthKeyID, identity.RawExpiresAt)
	}
	key := authKeyLayerIdentityKey(rawAuthKeyID)
	value := authKeyLayerID(rawAuthKeyID)
	if identity.RawExpiresAt > 0 {
		expiresAt := time.Unix(int64(identity.RawExpiresAt), 0)
		if !time.Now().Before(expiresAt) {
			return store.ErrAuthKeyNotFound
		}
		if err := s.c.SetArgs(ctx, key, value, redis.SetArgs{ExpireAt: expiresAt}).Err(); err != nil {
			return fmt.Errorf("redis install temporary auth key Layer identity: %w", err)
		}
		return nil
	}
	if err := s.c.Set(ctx, key, value, 0).Err(); err != nil {
		return fmt.Errorf("redis install permanent auth key Layer identity: %w", err)
	}
	return nil
}

func authKeyLayerIdentityKey(id [8]byte) string {
	return authKeyLayerIdentityPrefix + authKeyLayerID(id)
}

func authKeyLayerSessionKey(id [8]byte, sessionID int64) string {
	return authKeyLayerSessionPrefix + authKeyLayerID(id) + ":" + strconv.FormatInt(sessionID, 10)
}

func authKeyLayerID(id [8]byte) string { return hex.EncodeToString(id[:]) }

func redisLayerStatus(values []any) (string, error) {
	if len(values) == 0 {
		return "", errors.New("redis Layer script returned no status")
	}
	status, err := redisLayerString(values[0])
	if err != nil || strings.TrimSpace(status) != status || status == "" {
		return "", errors.New("redis Layer script returned invalid status")
	}
	return status, nil
}

func decodeRedisSessionLayer(values []any) (store.AuthKeySessionLayer, error) {
	if len(values) < 6 {
		return store.AuthKeySessionLayer{}, errors.New("redis Layer session result is incomplete")
	}
	layer, err := redisLayerInt(values, 1)
	if err != nil || layer <= 0 {
		return store.AuthKeySessionLayer{}, store.ErrAuthKeySessionLayerInvalid
	}
	msgID, err := redisLayerInt64(values, 2)
	if err != nil || msgID <= 0 {
		return store.AuthKeySessionLayer{}, store.ErrAuthKeySessionLayerInvalid
	}
	observation, err := redisLayerInt64(values, 3)
	if err != nil || observation <= 0 {
		return store.AuthKeySessionLayer{}, store.ErrAuthKeySessionLayerInvalid
	}
	expiresMillis, err := redisLayerInt64(values, 4)
	if err != nil || expiresMillis <= 0 {
		return store.AuthKeySessionLayer{}, store.ErrAuthKeySessionLayerInvalid
	}
	shared, err := redisLayerBool(values, 5)
	if err != nil {
		return store.AuthKeySessionLayer{}, err
	}
	return store.AuthKeySessionLayer{
		Layer: layer, MessageID: msgID, ObservationID: observation,
		ExpiresAt: time.UnixMilli(expiresMillis).UTC(), SharedDefault: shared,
	}, nil
}

func decodeRedisConflictSessionLayer(values []any) (store.AuthKeySessionLayer, error) {
	if len(values) < 5 {
		return store.AuthKeySessionLayer{}, errors.New("redis Layer conflict result is incomplete")
	}
	copyValues := append([]any{"ok"}, values[1:]...)
	copyValues = append(copyValues, "0")
	return decodeRedisSessionLayer(copyValues)
}

func redisLayerBool(values []any, index int) (bool, error) {
	value, err := redisLayerStringAt(values, index)
	if err != nil {
		return false, err
	}
	switch value {
	case "0":
		return false, nil
	case "1":
		return true, nil
	default:
		return false, errors.New("redis Layer result contains invalid boolean")
	}
}

func redisLayerInt(values []any, index int) (int, error) {
	value, err := redisLayerInt64(values, index)
	if err != nil {
		return 0, err
	}
	if int64(int(value)) != value {
		return 0, errors.New("redis Layer result integer overflows int")
	}
	return int(value), nil
}

func redisLayerInt64(values []any, index int) (int64, error) {
	value, err := redisLayerStringAt(values, index)
	if err != nil {
		return 0, err
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, errors.New("redis Layer result contains invalid integer")
	}
	return parsed, nil
}

func redisLayerStringAt(values []any, index int) (string, error) {
	if index < 0 || index >= len(values) {
		return "", errors.New("redis Layer result is incomplete")
	}
	return redisLayerString(values[index])
}

func redisLayerString(value any) (string, error) {
	switch item := value.(type) {
	case string:
		return item, nil
	case []byte:
		return string(item), nil
	case int64:
		return strconv.FormatInt(item, 10), nil
	default:
		return "", fmt.Errorf("redis Layer result has type %T", value)
	}
}

var _ store.AuthKeySessionLayerStore = (*RedisAuthKeySessionLayerStore)(nil)
var _ store.AuthKeySessionLayerIdentityBinder = (*RedisAuthKeySessionLayerStore)(nil)
