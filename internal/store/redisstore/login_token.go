package redisstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"telesrv/internal/domain"
	"telesrv/internal/store"
)

type LoginTokenRegistryStore struct {
	c redis.UniversalClient
}

func NewLoginTokenRegistryStore(c redis.UniversalClient) *LoginTokenRegistryStore {
	return &LoginTokenRegistryStore{c: c}
}

func loginTokenKey(token []byte) string {
	sum := sha256.Sum256(token)
	return "telesrv:login_token:token:" + hex.EncodeToString(sum[:])
}

func loginTokenTargetKey(target store.LoginTokenTarget) string {
	raw, _ := json.Marshal(target)
	sum := sha256.Sum256(raw)
	return "telesrv:login_token:target:" + hex.EncodeToString(sum[:])
}

var putLoginTokenScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) ~= 0 then
  return 0
end
if redis.call('EXISTS', KEYS[2]) ~= 0 then
  return 0
end
redis.call('HSET', KEYS[1],
  'record', ARGV[1],
  'accepting', '0',
  'accepted', '0')
redis.call('PEXPIRE', KEYS[1], ARGV[2])
redis.call('SET', KEYS[2], KEYS[1], 'PX', ARGV[2])
return 1
`)

func (s *LoginTokenRegistryStore) PutLoginToken(ctx context.Context, record store.LoginTokenRecord, ttl time.Duration) (bool, error) {
	if s == nil || s.c == nil || len(record.Token) == 0 || ttl <= 0 || record.ExpiresAtUnixMilli <= 0 {
		return false, fmt.Errorf("invalid login token record")
	}
	record.ExceptIDs = normalizedLoginTokenExceptIDs(record.ExceptIDs)
	raw, err := json.Marshal(record)
	if err != nil {
		return false, fmt.Errorf("marshal login token: %w", err)
	}
	result, err := putLoginTokenScript.Run(ctx, s.c,
		[]string{loginTokenKey(record.Token), loginTokenTargetKey(record.Target)},
		raw, ttl.Milliseconds()).Int64()
	if err != nil {
		return false, fmt.Errorf("put login token: %w", err)
	}
	return result == 1, nil
}

func (s *LoginTokenRegistryStore) GetLoginTokenByTarget(ctx context.Context, target store.LoginTokenTarget) (store.LoginTokenRecord, bool, error) {
	if s == nil || s.c == nil {
		return store.LoginTokenRecord{}, false, nil
	}
	targetKey := loginTokenTargetKey(target)
	tokenKey, err := s.c.Get(ctx, targetKey).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return store.LoginTokenRecord{}, false, nil
		}
		return store.LoginTokenRecord{}, false, fmt.Errorf("get login token target: %w", err)
	}
	record, found, err := s.getLoginTokenByKey(ctx, tokenKey)
	if err != nil {
		return store.LoginTokenRecord{}, false, err
	}
	if !found || !loginTokenTargetEqual(record.Target, target) {
		_ = s.c.Del(ctx, targetKey).Err()
		return store.LoginTokenRecord{}, false, nil
	}
	return record, true, nil
}

func (s *LoginTokenRegistryStore) GetLoginToken(ctx context.Context, token []byte) (store.LoginTokenRecord, bool, error) {
	if s == nil || s.c == nil || len(token) == 0 {
		return store.LoginTokenRecord{}, false, nil
	}
	return s.getLoginTokenByKey(ctx, loginTokenKey(token))
}

func (s *LoginTokenRegistryStore) getLoginTokenByKey(ctx context.Context, key string) (store.LoginTokenRecord, bool, error) {
	values, err := s.c.HMGet(ctx, key,
		"record",
		"accepted",
		"accepted_user_id",
		"accepted_auth",
		"accepted_at_unix_milli",
	).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return store.LoginTokenRecord{}, false, nil
		}
		return store.LoginTokenRecord{}, false, fmt.Errorf("get login token: %w", err)
	}
	if len(values) == 0 || values[0] == nil {
		return store.LoginTokenRecord{}, false, nil
	}
	record, ok := decodeLoginTokenRecord([]byte(fmt.Sprint(values[0])))
	if !ok {
		_ = s.c.Del(ctx, key).Err()
		return store.LoginTokenRecord{}, false, nil
	}
	if len(values) > 1 && fmt.Sprint(values[1]) == "1" {
		record.Accepted = true
		if len(values) > 2 && values[2] != nil {
			record.AcceptedUserID, _ = strconv.ParseInt(fmt.Sprint(values[2]), 10, 64)
		}
		if len(values) > 3 && values[3] != nil {
			if err := json.Unmarshal([]byte(fmt.Sprint(values[3])), &record.AcceptedAuthorization); err != nil {
				_ = s.c.Del(ctx, key).Err()
				return store.LoginTokenRecord{}, false, nil
			}
		}
		if len(values) > 4 && values[4] != nil {
			record.AcceptedAtUnixMilli, _ = strconv.ParseInt(fmt.Sprint(values[4]), 10, 64)
		}
	}
	return record, true, nil
}

var beginLoginTokenAcceptScript = redis.NewScript(`
local raw = redis.call('HGET', KEYS[1], 'record')
if not raw then
  return {ARGV[4], ''}
end
local record = cjson.decode(raw)
if tonumber(record['expires_at_unix_milli'] or '0') <= tonumber(ARGV[2]) then
  return {ARGV[5], raw}
end
if redis.call('HGET', KEYS[1], 'accepted') == '1' or redis.call('HGET', KEYS[1], 'accepting') == '1' then
  return {ARGV[6], raw}
end
local except = record['except_ids']
if type(except) == 'table' then
  for _, denied in ipairs(except) do
    if tostring(denied) == ARGV[1] then
      return {ARGV[6], raw}
    end
  end
end
redis.call('HSET', KEYS[1], 'accepting', '1')
return {ARGV[3], raw}
`)

func (s *LoginTokenRegistryStore) BeginLoginTokenAccept(ctx context.Context, token []byte, userID int64, now time.Time) (store.LoginTokenRecord, store.LoginTokenAcceptStatus, error) {
	if s == nil || s.c == nil || len(token) == 0 || userID <= 0 {
		return store.LoginTokenRecord{}, store.LoginTokenAcceptMissing, nil
	}
	result, err := beginLoginTokenAcceptScript.Run(ctx, s.c, []string{loginTokenKey(token)},
		strconv.FormatInt(userID, 10),
		strconv.FormatInt(now.UnixMilli(), 10),
		string(store.LoginTokenAcceptStarted),
		string(store.LoginTokenAcceptMissing),
		string(store.LoginTokenAcceptExpired),
		string(store.LoginTokenAcceptAlreadyAccepted),
	).Result()
	if err != nil {
		return store.LoginTokenRecord{}, "", fmt.Errorf("begin login token accept: %w", err)
	}
	status, raw := loginTokenScriptStatusAndRaw(result)
	if status == "" {
		return store.LoginTokenRecord{}, "", fmt.Errorf("begin login token accept: unexpected script result %T", result)
	}
	if raw == "" {
		return store.LoginTokenRecord{}, status, nil
	}
	record, ok := decodeLoginTokenRecord([]byte(raw))
	if !ok {
		return store.LoginTokenRecord{}, status, nil
	}
	return record, status, nil
}

var finishLoginTokenAcceptScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end
if redis.call('HGET', KEYS[1], 'accepted') == '1' or redis.call('HGET', KEYS[1], 'accepting') ~= '1' then
  return 0
end
redis.call('HSET', KEYS[1],
  'accepting', '0',
  'accepted', '1',
  'accepted_user_id', ARGV[1],
  'accepted_auth', ARGV[2],
  'accepted_at_unix_milli', ARGV[3])
return 1
`)

func (s *LoginTokenRegistryStore) FinishLoginTokenAccept(ctx context.Context, token []byte, userID int64, accepted domain.Authorization, now time.Time) (bool, error) {
	if s == nil || s.c == nil || len(token) == 0 || userID <= 0 {
		return false, nil
	}
	rawAuth, err := json.Marshal(accepted)
	if err != nil {
		return false, fmt.Errorf("marshal accepted login token auth: %w", err)
	}
	result, err := finishLoginTokenAcceptScript.Run(ctx, s.c, []string{loginTokenKey(token)},
		strconv.FormatInt(userID, 10),
		rawAuth,
		strconv.FormatInt(now.UnixMilli(), 10),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("finish login token accept: %w", err)
	}
	return result == 1, nil
}

var failLoginTokenAcceptScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end
if redis.call('HGET', KEYS[1], 'accepted') == '1' then
  return 0
end
redis.call('HSET', KEYS[1], 'accepting', '0')
return 1
`)

func (s *LoginTokenRegistryStore) FailLoginTokenAccept(ctx context.Context, token []byte) error {
	if s == nil || s.c == nil || len(token) == 0 {
		return nil
	}
	if _, err := failLoginTokenAcceptScript.Run(ctx, s.c, []string{loginTokenKey(token)}).Result(); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("fail login token accept: %w", err)
	}
	return nil
}

var deleteLoginTokenScript = redis.NewScript(`
if redis.call('GET', KEYS[2]) == KEYS[1] then
  redis.call('DEL', KEYS[2])
end
return redis.call('DEL', KEYS[1])
`)

func (s *LoginTokenRegistryStore) DeleteLoginToken(ctx context.Context, token []byte, target store.LoginTokenTarget) error {
	if s == nil || s.c == nil || len(token) == 0 {
		return nil
	}
	if _, err := deleteLoginTokenScript.Run(ctx, s.c,
		[]string{loginTokenKey(token), loginTokenTargetKey(target)}).Result(); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("delete login token: %w", err)
	}
	return nil
}

func decodeLoginTokenRecord(raw []byte) (store.LoginTokenRecord, bool) {
	var record store.LoginTokenRecord
	if err := json.Unmarshal(raw, &record); err != nil {
		return store.LoginTokenRecord{}, false
	}
	if len(record.Token) == 0 || record.ExpiresAtUnixMilli <= 0 {
		return store.LoginTokenRecord{}, false
	}
	record.ExceptIDs = normalizedLoginTokenExceptIDs(record.ExceptIDs)
	return record, true
}

func normalizedLoginTokenExceptIDs(ids []int64) []int64 {
	if len(ids) == 0 {
		return []int64{}
	}
	out := make([]int64, 0, len(ids))
	seen := make(map[int64]struct{}, len(ids))
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

func loginTokenTargetEqual(a, b store.LoginTokenTarget) bool {
	return a.RawAuthKeyID == b.RawAuthKeyID && a.AuthKeyID == b.AuthKeyID && a.SessionID == b.SessionID
}

func loginTokenScriptStatusAndRaw(result any) (store.LoginTokenAcceptStatus, string) {
	items, ok := result.([]any)
	if !ok || len(items) < 1 {
		return "", ""
	}
	status := store.LoginTokenAcceptStatus(fmt.Sprint(items[0]))
	raw := ""
	if len(items) > 1 && items[1] != nil {
		raw = fmt.Sprint(items[1])
	}
	return status, raw
}
