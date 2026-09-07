package phone

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"telesrv/internal/domain"
)

const (
	defaultRedisActiveCallPrefix  = "telesrv:phone:active:v1"
	defaultRedisActiveCallLockTTL = 30 * time.Second
	redisActiveCallLockRetry      = 10 * time.Millisecond
)

type RedisActiveCallStore struct {
	c       redis.UniversalClient
	prefix  string
	lockTTL time.Duration
}

type RedisActiveCallStoreOption func(*RedisActiveCallStore)

func WithRedisActiveCallPrefix(prefix string) RedisActiveCallStoreOption {
	return func(s *RedisActiveCallStore) {
		prefix = strings.TrimSpace(prefix)
		if prefix != "" {
			s.prefix = strings.TrimRight(prefix, ":")
		}
	}
}

func WithRedisActiveCallLockTTL(ttl time.Duration) RedisActiveCallStoreOption {
	return func(s *RedisActiveCallStore) {
		if ttl > 0 {
			s.lockTTL = ttl
		}
	}
}

func NewRedisActiveCallStore(c redis.UniversalClient, opts ...RedisActiveCallStoreOption) *RedisActiveCallStore {
	s := &RedisActiveCallStore{
		c:       c,
		prefix:  defaultRedisActiveCallPrefix,
		lockTTL: defaultRedisActiveCallLockTTL,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

type activeCallRecord struct {
	Call         domain.PhoneCall `json:"call"`
	SigWindowSec int64            `json:"sig_window_sec,omitempty"`
	SigCount     int              `json:"sig_count,omitempty"`
}

func (s *RedisActiveCallStore) RequestCall(ctx context.Context, callerID int64, in domain.PhoneCallRequest, now time.Time, cfg Config) (domain.PhoneCall, error) {
	var out domain.PhoneCall
	err := s.withLock(ctx, s.globalLockKey(), func() error {
		randomKey := s.randomKey(callerID, in.RandomID)
		if existingID, ok, err := s.getRandomCallID(ctx, randomKey); err != nil {
			return err
		} else if ok {
			if rec, found, err := s.getRecord(ctx, existingID); err != nil {
				return err
			} else if found && !rec.Call.Terminal() {
				out = rec.Call
				return nil
			}
		}
		active, err := s.activeCount(ctx, callerID)
		if err != nil {
			return err
		}
		if active >= int64(cfg.MaxActivePerUser) {
			return ErrOccupyFailed
		}
		total, err := s.c.SCard(ctx, s.indexKey()).Result()
		if err != nil {
			return fmt.Errorf("phone redis count active calls: %w", err)
		}
		if total >= int64(cfg.MaxRegistryEntries) {
			return ErrOccupyFailed
		}
		id, err := s.newCallID(ctx)
		if err != nil {
			return err
		}
		accessHash, err := randomPositiveInt64()
		if err != nil {
			return err
		}
		call := newRequestedCall(id, accessHash, callerID, in, now.Unix())
		rec := activeCallRecord{Call: call}
		if err := s.putRequestedRecord(ctx, rec); err != nil {
			return err
		}
		out = call
		return nil
	})
	return out, err
}

func (s *RedisActiveCallStore) ReceivedCall(ctx context.Context, userID, callID, accessHash int64, now time.Time) (domain.PhoneCall, bool, error) {
	var out domain.PhoneCall
	var transitioned bool
	err := s.withCallLock(ctx, callID, func() error {
		rec, err := s.requireRecord(ctx, callID, accessHash)
		if err != nil {
			return err
		}
		if userID != rec.Call.ParticipantID {
			return ErrPeerInvalid
		}
		if rec.Call.State == domain.PhoneCallStateRequested {
			rec.Call.State = domain.PhoneCallStateRinging
			rec.Call.ReceiveDate = int(now.Unix())
			transitioned = true
			if err := s.putActiveRecord(ctx, rec); err != nil {
				return err
			}
		}
		out = rec.Call
		return nil
	})
	return out, transitioned, err
}

func (s *RedisActiveCallStore) AcceptCall(ctx context.Context, userID, callID, accessHash int64, gb []byte, proto domain.PhoneCallProtocol, device domain.SessionRef) (domain.PhoneCall, error) {
	var out domain.PhoneCall
	err := s.withCallLock(ctx, callID, func() error {
		rec, err := s.requireRecord(ctx, callID, accessHash)
		if err != nil {
			return err
		}
		if userID != rec.Call.ParticipantID {
			return ErrPeerInvalid
		}
		if err := ensureAcceptable(rec.Call); err != nil {
			return err
		}
		negotiated, err := negotiateProtocol(rec.Call.CallerProtocol, proto)
		if err != nil {
			return err
		}
		applyAcceptedCall(&rec.Call, gb, proto, negotiated, device)
		if err := s.putActiveRecord(ctx, rec); err != nil {
			return err
		}
		out = rec.Call
		return nil
	})
	return out, err
}

func (s *RedisActiveCallStore) ConfirmCall(ctx context.Context, userID, callID, accessHash int64, ga []byte, keyFingerprint int64, proto domain.PhoneCallProtocol, now time.Time, cfg Config) (domain.PhoneCall, bool, error) {
	var out domain.PhoneCall
	var forced bool
	err := s.withCallLock(ctx, callID, func() error {
		rec, err := s.requireRecord(ctx, callID, accessHash)
		if err != nil {
			return err
		}
		if userID != rec.Call.AdminID {
			return ErrPeerInvalid
		}
		if err := ensureConfirmable(rec.Call); err != nil {
			return err
		}
		if err := validateProtocol(proto); err != nil {
			return err
		}
		if len(ga) != dhPubSize || sha256Mismatch(ga, rec.Call.GAHash) {
			markRecordDiscarded(&rec, domain.PhoneCallDiscardReasonDisconnect, "", 0, int(now.Unix()))
			if err := s.putTerminalRecord(ctx, rec, cfg.TombstoneTTL); err != nil {
				return err
			}
			out = rec.Call
			forced = true
			return ErrGAHashMismatch
		}
		applyConfirmedCall(&rec.Call, ga, keyFingerprint, now)
		if err := s.putActiveRecord(ctx, rec); err != nil {
			return err
		}
		out = rec.Call
		return nil
	})
	if errors.Is(err, ErrGAHashMismatch) {
		return out, forced, err
	}
	return out, forced, err
}

func (s *RedisActiveCallStore) DiscardCallWithSlug(ctx context.Context, userID, callID, accessHash int64, reason domain.PhoneCallDiscardReason, reasonSlug string, duration int, now time.Time, cfg Config) (domain.PhoneCall, bool, error) {
	var out domain.PhoneCall
	var already bool
	err := s.withCallLock(ctx, callID, func() error {
		rec, err := s.requireRecord(ctx, callID, accessHash)
		if err != nil {
			return err
		}
		if !rec.Call.HasParticipant(userID) {
			return ErrPeerInvalid
		}
		if rec.Call.Terminal() {
			out = rec.Call
			already = true
			return nil
		}
		duration = normalizedCallDuration(rec.Call, duration)
		markRecordDiscarded(&rec, reason, reasonSlug, duration, int(now.Unix()))
		if err := s.putTerminalRecord(ctx, rec, cfg.TombstoneTTL); err != nil {
			return err
		}
		out = rec.Call
		return nil
	})
	return out, already, err
}

func (s *RedisActiveCallStore) ExpireDue(ctx context.Context, now time.Time, cfg Config) []domain.PhoneCall {
	if s == nil || s.c == nil {
		return nil
	}
	ids, err := s.c.SMembers(ctx, s.indexKey()).Result()
	if err != nil {
		return nil
	}
	nowUnix := now.Unix()
	ringSec := int64(cfg.RingTimeout / time.Second)
	expired := make([]domain.PhoneCall, 0)
	for _, rawID := range ids {
		callID, err := strconv.ParseInt(rawID, 10, 64)
		if err != nil {
			_ = s.c.SRem(ctx, s.indexKey(), rawID).Err()
			continue
		}
		_ = s.withCallLock(ctx, callID, func() error {
			rec, found, err := s.getRecord(ctx, callID)
			if err != nil {
				return err
			}
			if !found {
				_ = s.c.SRem(ctx, s.indexKey(), callID).Err()
				return nil
			}
			if !callExpired(rec.Call, nowUnix, ringSec) {
				return nil
			}
			markRecordDiscarded(&rec, expireReason(rec.Call), "", 0, int(nowUnix))
			if err := s.putTerminalRecord(ctx, rec, cfg.TombstoneTTL); err != nil {
				return err
			}
			expired = append(expired, rec.Call)
			return nil
		})
	}
	return expired
}

func (s *RedisActiveCallStore) Signal(ctx context.Context, userID, callID, accessHash int64, now time.Time, rateLimit int, forward func(peerUserID int64, peerDevice domain.SessionRef)) (bool, error) {
	var drop bool
	err := s.withCallLock(ctx, callID, func() error {
		rec, err := s.requireRecord(ctx, callID, accessHash)
		if err != nil {
			return err
		}
		if !rec.Call.HasParticipant(userID) {
			return ErrPeerInvalid
		}
		switch rec.Call.State {
		case domain.PhoneCallStateAccepted, domain.PhoneCallStateConfirmed:
		case domain.PhoneCallStateDiscarded:
			drop = true
			return nil
		default:
			return ErrPeerInvalid
		}
		if signalRateExceeded(&rec.SigWindowSec, &rec.SigCount, now.Unix(), rateLimit) {
			drop = true
			return s.putActiveRecord(ctx, rec)
		}
		if err := s.putActiveRecord(ctx, rec); err != nil {
			return err
		}
		peer := rec.Call.PeerOf(userID)
		forward(peer, peerDeviceFor(rec.Call, peer))
		return nil
	})
	return drop, err
}

func (s *RedisActiveCallStore) Lookup(ctx context.Context, callID, accessHash int64) (domain.PhoneCall, bool) {
	rec, found, err := s.getRecord(ctx, callID)
	if err != nil || !found || rec.Call.AccessHash != accessHash {
		return domain.PhoneCall{}, false
	}
	return rec.Call, true
}

func (s *RedisActiveCallStore) newCallID(ctx context.Context) (int64, error) {
	for i := 0; i < 32; i++ {
		id, err := randomPositiveInt64()
		if err != nil {
			return 0, err
		}
		exists, err := s.c.Exists(ctx, s.callKey(id)).Result()
		if err != nil {
			return 0, fmt.Errorf("phone redis check call id: %w", err)
		}
		if exists == 0 {
			return id, nil
		}
	}
	return 0, fmt.Errorf("phone: exhausted call id attempts")
}

func (s *RedisActiveCallStore) requireRecord(ctx context.Context, callID, accessHash int64) (activeCallRecord, error) {
	rec, found, err := s.getRecord(ctx, callID)
	if err != nil {
		return activeCallRecord{}, err
	}
	if !found || rec.Call.AccessHash != accessHash {
		return activeCallRecord{}, ErrPeerInvalid
	}
	return rec, nil
}

func (s *RedisActiveCallStore) getRecord(ctx context.Context, callID int64) (activeCallRecord, bool, error) {
	if s == nil || s.c == nil || callID <= 0 {
		return activeCallRecord{}, false, nil
	}
	raw, err := s.c.Get(ctx, s.callKey(callID)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return activeCallRecord{}, false, nil
		}
		return activeCallRecord{}, false, fmt.Errorf("phone redis get call: %w", err)
	}
	var rec activeCallRecord
	if err := json.Unmarshal(raw, &rec); err != nil || rec.Call.ID != callID {
		_ = s.c.Del(ctx, s.callKey(callID)).Err()
		return activeCallRecord{}, false, nil
	}
	return rec, true, nil
}

func (s *RedisActiveCallStore) putActiveRecord(ctx context.Context, rec activeCallRecord) error {
	raw, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("phone redis marshal call: %w", err)
	}
	if err := s.c.Set(ctx, s.callKey(rec.Call.ID), raw, 0).Err(); err != nil {
		return fmt.Errorf("phone redis store call: %w", err)
	}
	return nil
}

func (s *RedisActiveCallStore) putRequestedRecord(ctx context.Context, rec activeCallRecord) error {
	raw, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("phone redis marshal requested call: %w", err)
	}
	pipe := s.c.TxPipeline()
	pipe.Set(ctx, s.callKey(rec.Call.ID), raw, 0)
	pipe.Set(ctx, s.randomKey(rec.Call.AdminID, rec.Call.RandomID), strconv.FormatInt(rec.Call.ID, 10), 0)
	pipe.SAdd(ctx, s.activeKey(rec.Call.AdminID), rec.Call.ID)
	pipe.SAdd(ctx, s.indexKey(), rec.Call.ID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("phone redis store requested call: %w", err)
	}
	return nil
}

func (s *RedisActiveCallStore) putTerminalRecord(ctx context.Context, rec activeCallRecord, tombstoneTTL time.Duration) error {
	if tombstoneTTL <= 0 {
		tombstoneTTL = Config{}.withDefaults().TombstoneTTL
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("phone redis marshal terminal call: %w", err)
	}
	pipe := s.c.TxPipeline()
	pipe.Set(ctx, s.callKey(rec.Call.ID), raw, tombstoneTTL)
	pipe.Expire(ctx, s.randomKey(rec.Call.AdminID, rec.Call.RandomID), tombstoneTTL)
	pipe.SRem(ctx, s.activeKey(rec.Call.AdminID), rec.Call.ID)
	pipe.SRem(ctx, s.indexKey(), rec.Call.ID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("phone redis store terminal call: %w", err)
	}
	return nil
}

func (s *RedisActiveCallStore) activeCount(ctx context.Context, userID int64) (int64, error) {
	n, err := s.c.SCard(ctx, s.activeKey(userID)).Result()
	if err != nil {
		return 0, fmt.Errorf("phone redis count active user calls: %w", err)
	}
	return n, nil
}

func (s *RedisActiveCallStore) getRandomCallID(ctx context.Context, key string) (int64, bool, error) {
	raw, err := s.c.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("phone redis get random index: %w", err)
	}
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		_ = s.c.Del(ctx, key).Err()
		return 0, false, nil
	}
	return id, true, nil
}

func markRecordDiscarded(rec *activeCallRecord, reason domain.PhoneCallDiscardReason, reasonSlug string, duration, nowUnix int) {
	if rec.Call.Terminal() {
		return
	}
	rec.Call.State = domain.PhoneCallStateDiscarded
	rec.Call.DiscardReason = reason
	rec.Call.DiscardReasonSlug = reasonSlug
	rec.Call.Duration = duration
	rec.Call.DiscardedAt = nowUnix
}

var releaseRedisActiveCallLockScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`)

func (s *RedisActiveCallStore) withCallLock(ctx context.Context, callID int64, fn func() error) error {
	return s.withLock(ctx, s.callLockKey(callID), fn)
}

func (s *RedisActiveCallStore) withLock(ctx context.Context, key string, fn func() error) error {
	if s == nil || s.c == nil {
		return ErrMissingActiveCallStore
	}
	if fn == nil {
		return nil
	}
	token, err := redisLockToken()
	if err != nil {
		return err
	}
	for {
		ok, err := s.c.SetNX(ctx, key, token, s.lockTTL).Result()
		if err != nil {
			return fmt.Errorf("phone redis acquire lock: %w", err)
		}
		if ok {
			break
		}
		timer := time.NewTimer(redisActiveCallLockRetry)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	defer func() {
		_, _ = releaseRedisActiveCallLockScript.Run(context.Background(), s.c, []string{key}, token).Result()
	}()
	return fn()
}

func redisLockToken() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("phone redis lock token: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

func (s *RedisActiveCallStore) callKey(callID int64) string {
	return fmt.Sprintf("%s:call:%d", s.prefix, callID)
}

func (s *RedisActiveCallStore) randomKey(callerID, randomID int64) string {
	return fmt.Sprintf("%s:random:%d:%d", s.prefix, callerID, randomID)
}

func (s *RedisActiveCallStore) activeKey(userID int64) string {
	return fmt.Sprintf("%s:active:%d", s.prefix, userID)
}

func (s *RedisActiveCallStore) indexKey() string {
	return s.prefix + ":calls"
}

func (s *RedisActiveCallStore) globalLockKey() string {
	return s.prefix + ":lock:global"
}

func (s *RedisActiveCallStore) callLockKey(callID int64) string {
	return fmt.Sprintf("%s:lock:call:%d", s.prefix, callID)
}
