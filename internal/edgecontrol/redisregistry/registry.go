package redisregistry

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"telesrv/internal/edgecontrol"
)

var (
	ErrNilClient       = errors.New("edge location registry: nil redis client")
	ErrInvalidRecord   = errors.New("edge location registry: invalid location record")
	ErrInvalidRegistry = errors.New("edge location registry: invalid registry query")
)

const (
	locationRecordBatch                  = 1024
	leaseField                           = "__lease"
	recordFieldPrefix                    = "record:"
	indexesFieldPrefix                   = "indexes:"
	membershipRecordFieldPrefix          = "membership:"
	membershipChannelIDsFieldPrefix      = "membership-channel-ids:"
	channelPresenceFieldPrefix           = "channel-presence:"
	memberPresenceCountFieldPrefix       = "channel-member-count:"
	interestPresenceCountFieldPrefix     = "channel-interest-count:"
	subscriptionPresenceCountFieldPrefix = "channel-subscription-count:"
	maxLocationLeaseIDBytes              = 255
)

func validInstanceID(value string) bool {
	return value != "" && len(value) <= edgecontrol.MaxDeliveryInstanceIDBytes && strings.TrimSpace(value) == value
}

func validLeaseID(value string) bool {
	return value != "" && len(value) <= maxLocationLeaseIDBytes && strings.TrimSpace(value) == value
}

func validLocationRecord(record edgecontrol.LocationRecord) bool {
	return validInstanceID(record.InstanceID) && record.RawAuthKeyID != ([8]byte{}) && record.SessionID != 0 && record.LocationRevision > 0
}

const acquireLeaseScript = `
local current = redis.call('HGET', KEYS[1], ARGV[1])
if current and current ~= ARGV[2] then
  return 0
end
if redis.call('EXISTS', KEYS[1]) == 1 and not current then
  return 0
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1
`

const renewLeaseScript = `
if redis.call('HGET', KEYS[1], ARGV[1]) ~= ARGV[2] then
  return 0
end
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1
`

const releaseLeaseScript = `
if redis.call('HGET', KEYS[1], ARGV[1]) ~= ARGV[2] then
  return 0
end
redis.call('UNLINK', KEYS[1])
return 1
`

// applyLocationMutationScript fences every mutation with the instance lease.
// The instance hash owns both the lease and all records, so one PEXPIRE renews
// the complete location set regardless of connection count. Secondary index
// members are replaced atomically for one session and cleaned lazily after an
// ungraceful instance expiry.
const applyLocationMutationScript = `
if redis.call('HGET', KEYS[1], '` + leaseField + `') ~= ARGV[1] then
  return 0
end

local oldIndexesRaw = redis.call('HGET', KEYS[1], ARGV[3])
if oldIndexesRaw then
  local ok, oldIndexes = pcall(cjson.decode, oldIndexesRaw)
  if ok then
    for _, indexKey in ipairs(oldIndexes) do
      redis.call('SREM', indexKey, ARGV[4])
    end
  end
end

local indexCount = tonumber(ARGV[8])
local pos = 9 + indexCount
local subscribedChannelsKey = ARGV[pos]
pos = pos + 1
local subscribedCount = tonumber(ARGV[pos])
pos = pos + 1
local subscriptionStart = pos
pos = pos + subscribedCount
local presenceField = ARGV[pos]
local presenceRaw = ARGV[pos + 1]
local instanceID = ARGV[pos + 2]
local edgeInterestPrefix = ARGV[pos + 3]
local edgeSubscriptionPrefix = ARGV[pos + 4]
local interestCountPrefix = ARGV[pos + 5]
local subscriptionCountPrefix = ARGV[pos + 6]

local oldPresence = {interest = {}, subscription = {}}
local oldPresenceRaw = redis.call('HGET', KEYS[1], presenceField)
if oldPresenceRaw then
  local ok, decoded = pcall(cjson.decode, oldPresenceRaw)
  if ok then oldPresence = decoded end
end
local newPresence = {interest = {}, subscription = {}}
if ARGV[5] ~= '1' then
  local ok, decoded = pcall(cjson.decode, presenceRaw)
  if not ok then return -2 end
  newPresence = decoded
end

local function idSet(values)
  local out = {}
  if values then
    for _, value in ipairs(values) do out[tostring(value)] = true end
  end
  return out
end
local function updatePresence(oldValues, newValues, edgePrefix, countPrefix)
  local oldSet = idSet(oldValues)
  local newSet = idSet(newValues)
  for id, _ in pairs(oldSet) do
    if not newSet[id] then
      local count = redis.call('HINCRBY', KEYS[1], countPrefix .. id, -1)
      if count <= 0 then
        redis.call('HDEL', KEYS[1], countPrefix .. id)
        redis.call('SREM', edgePrefix .. id, instanceID)
      end
    end
  end
  for id, _ in pairs(newSet) do
    if not oldSet[id] then
      local count = redis.call('HINCRBY', KEYS[1], countPrefix .. id, 1)
      if count == 1 then redis.call('SADD', edgePrefix .. id, instanceID) end
    end
  end
end

updatePresence(oldPresence.interest, newPresence.interest, edgeInterestPrefix, interestCountPrefix)
updatePresence(oldPresence.subscription, newPresence.subscription, edgeSubscriptionPrefix, subscriptionCountPrefix)

if ARGV[5] == '1' then
  redis.call('HDEL', KEYS[1], ARGV[2], ARGV[3], presenceField)
else
  local revision = redis.call('INCR', KEYS[2])
  local recordRaw, replacements = string.gsub(ARGV[6], '"LocationRevision":0', '"LocationRevision":' .. tostring(revision), 1)
  if replacements ~= 1 then
    return -1
  end
  redis.call('HSET', KEYS[1], ARGV[2], recordRaw, ARGV[3], ARGV[7], presenceField, presenceRaw)
end

pos = 9
for i = 1, indexCount do
  redis.call('SADD', ARGV[pos], ARGV[4])
  pos = pos + 1
end

subscribedChannelsKey = ARGV[pos]
pos = pos + 1
local subscribedCount = tonumber(ARGV[pos])
pos = pos + 1
for i = 1, subscribedCount do
  redis.call('SADD', subscribedChannelsKey, ARGV[pos])
  pos = pos + 1
end
return 1
`

// applyChannelMembershipMutationScript owns the user-level online membership
// projection for one Edge instance. The ref is (instance,user), not a session,
// so multiple devices on the same Edge do not multiply channel index members.
const applyChannelMembershipMutationScript = `
if redis.call('HGET', KEYS[1], '` + leaseField + `') ~= ARGV[1] then
  return 0
end

local oldIDs = {}
local oldIDsRaw = redis.call('HGET', KEYS[1], ARGV[3])
if oldIDsRaw then
  local ok, decoded = pcall(cjson.decode, oldIDsRaw)
  if ok then
    for _, channelID in ipairs(decoded) do
      local id = tostring(channelID)
      oldIDs[id] = true
      redis.call('SREM', ARGV[8] .. id, ARGV[4])
    end
  end
end

if ARGV[5] == '1' then
  redis.call('HDEL', KEYS[1], ARGV[2], ARGV[3])
else
  redis.call('HSET', KEYS[1], ARGV[2], ARGV[6], ARGV[3], ARGV[7])
end

local memberChannelsKey = ARGV[9]
local memberCount = tonumber(ARGV[10])
local instanceID = ARGV[11]
local edgeMemberPrefix = ARGV[12]
local countFieldPrefix = ARGV[13]
local pos = 14
local newIDs = {}
for i = 1, memberCount do
  local id = ARGV[pos]
  newIDs[id] = true
  redis.call('SADD', ARGV[8] .. id, ARGV[4])
  redis.call('SADD', memberChannelsKey, id)
  pos = pos + 1
end

for id, _ in pairs(oldIDs) do
  if not newIDs[id] then
    local count = redis.call('HINCRBY', KEYS[1], countFieldPrefix .. id, -1)
    if count <= 0 then
      redis.call('HDEL', KEYS[1], countFieldPrefix .. id)
      redis.call('SREM', edgeMemberPrefix .. id, instanceID)
    end
  end
end
for id, _ in pairs(newIDs) do
  if not oldIDs[id] then
    local count = redis.call('HINCRBY', KEYS[1], countFieldPrefix .. id, 1)
    if count == 1 then
      redis.call('SADD', edgeMemberPrefix .. id, instanceID)
    end
  end
end
return 1
`

type Registry struct {
	c      *redis.Client
	prefix string
	now    func() time.Time
}

type Option func(*Registry)

func WithPrefix(prefix string) Option {
	return func(r *Registry) {
		prefix = strings.TrimSpace(prefix)
		if prefix != "" {
			r.prefix = strings.TrimRight(prefix, ":")
		}
	}
}

func WithNow(now func() time.Time) Option {
	return func(r *Registry) {
		if now != nil {
			r.now = now
		}
	}
}

func New(c *redis.Client, opts ...Option) *Registry {
	r := &Registry{c: c, prefix: "edge:location", now: time.Now}
	for _, opt := range opts {
		if opt != nil {
			opt(r)
		}
	}
	return r
}

func (r *Registry) AcquireInstanceLease(ctx context.Context, instanceID, leaseID string, ttl time.Duration) error {
	if r == nil || r.c == nil {
		return ErrNilClient
	}
	if !validInstanceID(instanceID) || !validLeaseID(leaseID) || ttl <= 0 {
		return ErrInvalidRegistry
	}
	result, err := r.c.Eval(ctx, acquireLeaseScript, []string{r.instanceKey(instanceID)}, leaseField, leaseID, ttlMillis(ttl)).Int()
	if err != nil {
		return fmt.Errorf("redis acquire edge location lease: %w", err)
	}
	if result != 1 {
		return edgecontrol.ErrLocationLeaseHeld
	}
	return nil
}

func (r *Registry) RenewInstanceLease(ctx context.Context, instanceID, leaseID string, ttl time.Duration) error {
	if r == nil || r.c == nil {
		return ErrNilClient
	}
	if !validInstanceID(instanceID) || !validLeaseID(leaseID) || ttl <= 0 {
		return ErrInvalidRegistry
	}
	result, err := r.c.Eval(ctx, renewLeaseScript, []string{r.instanceKey(instanceID)}, leaseField, leaseID, ttlMillis(ttl)).Int()
	if err != nil {
		return fmt.Errorf("redis renew edge location lease: %w", err)
	}
	if result != 1 {
		return edgecontrol.ErrLocationLeaseLost
	}
	return nil
}

func (r *Registry) ApplyLocationMutations(ctx context.Context, instanceID, leaseID string, mutations []edgecontrol.LocationMutation) error {
	if r == nil || r.c == nil {
		return ErrNilClient
	}
	if !validInstanceID(instanceID) || !validLeaseID(leaseID) {
		return ErrInvalidRegistry
	}
	if len(mutations) == 0 {
		return nil
	}
	now := r.now().UTC()
	pipe := r.c.Pipeline()
	cmds := make([]*redis.Cmd, 0, len(mutations))
	for _, mutation := range mutations {
		record := mutation.Record
		if record.InstanceID != instanceID || !validInstanceID(record.InstanceID) || record.RawAuthKeyID == ([8]byte{}) || record.SessionID == 0 || record.LocationRevision != 0 {
			return ErrInvalidRecord
		}
		record.UpdatedAtUnix = now.Unix()
		record.UpdatedAtUnixNano = now.UnixNano()
		ref := r.locationRef(record)
		recordField := r.recordField(record.RawAuthKeyID, record.SessionID)
		indexesField := r.indexesField(record.RawAuthKeyID, record.SessionID)
		indexes := []string(nil)
		recordRaw := []byte("{}")
		if !mutation.Deleted {
			indexes = r.locationIndexKeys(record, now.UnixNano())
			var err error
			recordRaw, err = json.Marshal(record)
			if err != nil {
				return fmt.Errorf("marshal edge location: %w", err)
			}
		}
		indexesRaw, err := json.Marshal(indexes)
		if err != nil {
			return fmt.Errorf("marshal edge location indexes: %w", err)
		}
		activeSubscriptions := activeChannelSubscriptionIDs(record.ChannelSubscriptions, now.UnixNano())
		presence := struct {
			Interest     []int64 `json:"interest"`
			Subscription []int64 `json:"subscription"`
		}{
			Interest:     uniquePositiveInt64sSorted(record.ActiveChannelIDs),
			Subscription: uniquePositiveInt64sSorted(activeSubscriptions),
		}
		if mutation.Deleted {
			presence.Interest = nil
			presence.Subscription = nil
		}
		presenceRaw, err := json.Marshal(presence)
		if err != nil {
			return fmt.Errorf("marshal edge channel presence: %w", err)
		}
		args := make([]any, 0, 19+len(indexes)+len(activeSubscriptions))
		args = append(args, leaseID, recordField, indexesField, ref, boolFlag(mutation.Deleted), string(recordRaw), string(indexesRaw), len(indexes))
		for _, indexKey := range indexes {
			args = append(args, indexKey)
		}
		args = append(args, r.subscribedChannelsKey(), len(activeSubscriptions))
		for _, channelID := range activeSubscriptions {
			args = append(args, strconv.FormatInt(channelID, 10))
		}
		args = append(args,
			r.channelPresenceField(record.RawAuthKeyID, record.SessionID), string(presenceRaw), instanceID,
			r.channelEdgeInterestKeyPrefix(), r.channelEdgeSubscriptionKeyPrefix(),
			interestPresenceCountFieldPrefix, subscriptionPresenceCountFieldPrefix,
		)
		cmds = append(cmds, pipe.Eval(ctx, applyLocationMutationScript, []string{r.instanceKey(instanceID), r.locationRevisionKey()}, args...))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis apply edge location mutations: %w", err)
	}
	for _, cmd := range cmds {
		result, err := cmd.Int()
		if err != nil {
			return fmt.Errorf("redis apply edge location mutation: %w", err)
		}
		if result == -1 {
			return fmt.Errorf("redis apply edge location mutation: revision placeholder missing")
		}
		if result == -2 {
			return fmt.Errorf("redis apply edge location mutation: invalid channel presence payload")
		}
		if result != 1 {
			return edgecontrol.ErrLocationLeaseLost
		}
	}
	return nil
}

func (r *Registry) ApplyChannelMembershipMutations(ctx context.Context, instanceID, leaseID string, mutations []edgecontrol.ChannelMembershipMutation) error {
	if r == nil || r.c == nil {
		return ErrNilClient
	}
	if !validInstanceID(instanceID) || !validLeaseID(leaseID) {
		return ErrInvalidRegistry
	}
	if len(mutations) == 0 {
		return nil
	}
	now := r.now().UTC()
	pipe := r.c.Pipeline()
	cmds := make([]*redis.Cmd, 0, len(mutations))
	for _, mutation := range mutations {
		record := mutation.Record
		if record.InstanceID != instanceID || !validInstanceID(record.InstanceID) || record.UserID <= 0 {
			return ErrInvalidRecord
		}
		record.UpdatedAtUnix = now.Unix()
		ref := r.channelMembershipRef(record.InstanceID, record.UserID)
		recordField := r.membershipRecordField(record.UserID)
		channelIDsField := r.membershipChannelIDsField(record.UserID)
		recordRaw := []byte("{}")
		channelIDs := []int64(nil)
		if !mutation.Deleted {
			channelIDs = uniquePositiveInt64sSorted(record.ChannelIDs)
			record.ChannelIDs = channelIDs
			var err error
			recordRaw, err = json.Marshal(record)
			if err != nil {
				return fmt.Errorf("marshal edge channel membership: %w", err)
			}
		}
		channelIDsRaw, err := json.Marshal(channelIDs)
		if err != nil {
			return fmt.Errorf("marshal edge channel membership ids: %w", err)
		}
		args := make([]any, 0, 13+len(channelIDs))
		args = append(args, leaseID, recordField, channelIDsField, ref, boolFlag(mutation.Deleted), string(recordRaw), string(channelIDsRaw), r.channelMemberKeyPrefix(), r.memberChannelsKey(), len(channelIDs), instanceID, r.channelEdgeMemberKeyPrefix(), memberPresenceCountFieldPrefix)
		for _, channelID := range channelIDs {
			args = append(args, strconv.FormatInt(channelID, 10))
		}
		cmds = append(cmds, pipe.Eval(ctx, applyChannelMembershipMutationScript, []string{r.instanceKey(instanceID)}, args...))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis apply edge channel membership mutations: %w", err)
	}
	for _, cmd := range cmds {
		result, err := cmd.Int()
		if err != nil {
			return fmt.Errorf("redis apply edge channel membership mutation: %w", err)
		}
		if result != 1 {
			return edgecontrol.ErrLocationLeaseLost
		}
	}
	return nil
}

func (r *Registry) ReleaseInstanceLease(ctx context.Context, instanceID, leaseID string) error {
	if r == nil || r.c == nil {
		return ErrNilClient
	}
	if !validInstanceID(instanceID) || !validLeaseID(leaseID) {
		return ErrInvalidRegistry
	}
	result, err := r.c.Eval(ctx, releaseLeaseScript, []string{r.instanceKey(instanceID)}, leaseField, leaseID).Int()
	if err != nil {
		return fmt.Errorf("redis release edge location lease: %w", err)
	}
	if result != 1 {
		return edgecontrol.ErrLocationLeaseLost
	}
	return nil
}

func (r *Registry) ListUser(ctx context.Context, userID int64) ([]edgecontrol.LocationRecord, error) {
	if userID <= 0 {
		return nil, ErrInvalidRegistry
	}
	return r.listIndex(ctx, r.userKey(userID), func(record edgecontrol.LocationRecord) bool { return record.UserID == userID })
}

func (r *Registry) ListUsers(ctx context.Context, userIDs []int64) (map[int64][]edgecontrol.LocationRecord, error) {
	if r == nil || r.c == nil {
		return nil, ErrNilClient
	}
	ids := uniquePositiveUserIDs(userIDs)
	out := make(map[int64][]edgecontrol.LocationRecord, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	pipe := r.c.Pipeline()
	cmds := make(map[int64]*redis.StringSliceCmd, len(ids))
	for _, userID := range ids {
		cmds[userID] = pipe.SMembers(ctx, r.userKey(userID))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("redis list edge user location indexes: %w", err)
	}
	refsByUser := make(map[int64][]string, len(ids))
	allRefs := make([]string, 0)
	for _, userID := range ids {
		refs, err := cmds[userID].Result()
		if err != nil {
			return nil, fmt.Errorf("redis list edge user location index: %w", err)
		}
		refsByUser[userID] = refs
		allRefs = append(allRefs, refs...)
	}
	recordsByRef, staleRefs, err := r.locationRecordsByRefs(ctx, allRefs)
	staleSet := stringSet(staleRefs)
	staleByIndex := make(map[string][]any)
	for _, userID := range ids {
		indexKey := r.userKey(userID)
		for _, ref := range refsByUser[userID] {
			record, ok := recordsByRef[ref]
			if _, stale := staleSet[ref]; stale || !ok || record.UserID != userID {
				staleByIndex[indexKey] = append(staleByIndex[indexKey], ref)
				continue
			}
			out[userID] = append(out[userID], record)
		}
	}
	r.removeStaleIndexMembers(ctx, staleByIndex)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (r *Registry) ListBusinessAuthKey(ctx context.Context, authKeyID [8]byte) ([]edgecontrol.LocationRecord, error) {
	if authKeyID == ([8]byte{}) {
		return nil, ErrInvalidRegistry
	}
	return r.listIndex(ctx, r.businessAuthKey(authKeyID), func(record edgecontrol.LocationRecord) bool { return record.BusinessAuthKeyID == authKeyID })
}

func (r *Registry) ListRawAuthKey(ctx context.Context, authKeyID [8]byte) ([]edgecontrol.LocationRecord, error) {
	if authKeyID == ([8]byte{}) {
		return nil, ErrInvalidRegistry
	}
	return r.listIndex(ctx, r.rawAuthKey(authKeyID), func(record edgecontrol.LocationRecord) bool { return record.RawAuthKeyID == authKeyID })
}

func (r *Registry) ListInstance(ctx context.Context, instanceID string) ([]edgecontrol.LocationRecord, error) {
	if r == nil || r.c == nil {
		return nil, ErrNilClient
	}
	if !validInstanceID(instanceID) {
		return nil, ErrInvalidRegistry
	}
	values, err := r.c.HGetAll(ctx, r.instanceKey(instanceID)).Result()
	if err != nil {
		return nil, fmt.Errorf("redis list edge instance locations: %w", err)
	}
	if values[leaseField] == "" {
		return nil, nil
	}
	records := make([]edgecontrol.LocationRecord, 0, len(values)/2)
	for field, raw := range values {
		if !strings.HasPrefix(field, recordFieldPrefix) {
			continue
		}
		var record edgecontrol.LocationRecord
		if err := json.Unmarshal([]byte(raw), &record); err != nil {
			return nil, fmt.Errorf("unmarshal edge instance location %q: %w", field, err)
		}
		if record.InstanceID == instanceID && validLocationRecord(record) {
			records = append(records, record)
		}
	}
	return records, nil
}

func (r *Registry) ListActiveRawAuthKeyIDs(ctx context.Context) ([][8]byte, error) {
	if r == nil || r.c == nil {
		return nil, ErrNilClient
	}
	key := r.activeRawAuthKeysKey()
	refs, err := r.c.SMembers(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("redis list active raw auth-key locations: %w", err)
	}
	recordsByRef, stale, err := r.locationRecordsByRefs(ctx, refs)
	if len(stale) > 0 {
		_ = r.c.SRem(ctx, key, stringsToAny(stale)...).Err()
	}
	if err != nil {
		return nil, err
	}
	seen := make(map[[8]byte]struct{}, len(recordsByRef))
	out := make([][8]byte, 0, len(recordsByRef))
	for _, record := range recordsByRef {
		if record.RawAuthKeyID == ([8]byte{}) {
			continue
		}
		if _, ok := seen[record.RawAuthKeyID]; ok {
			continue
		}
		seen[record.RawAuthKeyID] = struct{}{}
		out = append(out, record.RawAuthKeyID)
	}
	return out, nil
}

func (r *Registry) ListChannelDeliveryTargets(ctx context.Context, routes []edgecontrol.ChannelDeliveryRoute) ([]string, error) {
	if r == nil || r.c == nil {
		return nil, ErrNilClient
	}
	if len(routes) == 0 || len(routes) > edgecontrol.MaxDeliveryBatchItems {
		return nil, ErrInvalidRegistry
	}
	indexKeys := make(map[string]struct{})
	explicitUsers := make([]int64, 0)
	for _, route := range routes {
		if !route.ValidFor(edgecontrol.OrderingDomain{Kind: edgecontrol.QueueChannelPTS, StreamID: route.ChannelID}) {
			return nil, ErrInvalidRegistry
		}
		switch route.Audience {
		case edgecontrol.ChannelAudienceMembers:
			indexKeys[r.channelEdgeMemberKey(route.ChannelID)] = struct{}{}
			indexKeys[r.channelEdgeInterestKey(route.ChannelID)] = struct{}{}
			indexKeys[r.channelEdgeSubscriptionKey(route.ChannelID)] = struct{}{}
		case edgecontrol.ChannelAudienceMessageBox:
			indexKeys[r.channelEdgeInterestKey(route.ChannelID)] = struct{}{}
			indexKeys[r.channelEdgeSubscriptionKey(route.ChannelID)] = struct{}{}
		}
		explicitUsers = append(explicitUsers, route.AudienceUsers...)
		explicitUsers = append(explicitUsers, route.AffectedUsers...)
	}

	pipe := r.c.Pipeline()
	commands := make(map[string]*redis.StringSliceCmd, len(indexKeys))
	for key := range indexKeys {
		commands[key] = pipe.SMembers(ctx, key)
	}
	if len(commands) > 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			return nil, fmt.Errorf("redis list channel Edge presence: %w", err)
		}
	}
	instances := make(map[string]struct{})
	instanceIndexes := make(map[string][]string)
	staleByIndex := make(map[string][]any)
	for key, command := range commands {
		values, err := command.Result()
		if err != nil {
			return nil, fmt.Errorf("redis read channel Edge presence: %w", err)
		}
		for _, instanceID := range values {
			if !validInstanceID(instanceID) {
				staleByIndex[key] = append(staleByIndex[key], instanceID)
				continue
			}
			instances[instanceID] = struct{}{}
			instanceIndexes[instanceID] = append(instanceIndexes[instanceID], key)
		}
	}
	if len(instances) > 0 {
		livePipe := r.c.Pipeline()
		live := make(map[string]*redis.BoolCmd, len(instances))
		for instanceID := range instances {
			live[instanceID] = livePipe.HExists(ctx, r.instanceKey(instanceID), leaseField)
		}
		if _, err := livePipe.Exec(ctx); err != nil {
			return nil, fmt.Errorf("redis validate channel Edge leases: %w", err)
		}
		for instanceID, command := range live {
			ok, err := command.Result()
			if err != nil {
				return nil, fmt.Errorf("redis read channel Edge lease: %w", err)
			}
			if ok {
				continue
			}
			delete(instances, instanceID)
			for _, key := range instanceIndexes[instanceID] {
				staleByIndex[key] = append(staleByIndex[key], instanceID)
			}
		}
	}
	r.removeStaleIndexMembers(ctx, staleByIndex)
	users, err := r.ListUsers(ctx, explicitUsers)
	if err != nil {
		return nil, err
	}
	for _, records := range users {
		for _, record := range records {
			if validLocationRecord(record) && record.ReceivesUpdates {
				instances[record.InstanceID] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(instances))
	for instanceID := range instances {
		out = append(out, instanceID)
	}
	sort.Strings(out)
	return out, nil
}

func (r *Registry) ListChannelInterest(ctx context.Context, channelID int64) ([]edgecontrol.LocationRecord, error) {
	if channelID <= 0 {
		return nil, ErrInvalidRegistry
	}
	return r.listIndex(ctx, r.channelInterestKey(channelID), func(record edgecontrol.LocationRecord) bool { return containsInt64(record.ActiveChannelIDs, channelID) })
}

func (r *Registry) ListChannelMember(ctx context.Context, channelID int64) ([]edgecontrol.LocationRecord, error) {
	if r == nil || r.c == nil {
		return nil, ErrNilClient
	}
	if channelID <= 0 {
		return nil, ErrInvalidRegistry
	}
	indexKey := r.channelMemberKey(channelID)
	refs, err := r.c.SMembers(ctx, indexKey).Result()
	if err != nil {
		return nil, fmt.Errorf("redis list edge channel membership index: %w", err)
	}
	recordsByRef, stale, err := r.channelMembershipRecordsByRefs(ctx, refs)
	if err == nil {
		for _, ref := range refs {
			record, ok := recordsByRef[ref]
			if ok && containsInt64(record.ChannelIDs, channelID) {
				continue
			}
			stale = append(stale, ref)
		}
	}
	if len(stale) > 0 {
		_ = r.c.SRem(ctx, indexKey, stringsToAny(uniqueNonEmptyStrings(stale))...).Err()
	}
	if err != nil {
		return nil, err
	}
	out := make([]edgecontrol.LocationRecord, 0, len(recordsByRef))
	for _, ref := range refs {
		record, ok := recordsByRef[ref]
		if !ok || !containsInt64(record.ChannelIDs, channelID) {
			continue
		}
		out = append(out, edgecontrol.LocationRecord{
			InstanceID:      record.InstanceID,
			UserID:          record.UserID,
			ReceivesUpdates: true,
			UpdatedAtUnix:   record.UpdatedAtUnix,
		})
	}
	return out, nil
}

func (r *Registry) ListChannelSubscription(ctx context.Context, channelID int64) ([]edgecontrol.LocationRecord, error) {
	if channelID <= 0 {
		return nil, ErrInvalidRegistry
	}
	now := r.now().UnixNano()
	return r.listIndex(ctx, r.channelSubscriptionKey(channelID), func(record edgecontrol.LocationRecord) bool {
		return containsActiveSubscription(record.ChannelSubscriptions, channelID, now)
	})
}

func (r *Registry) ListOnlineChannelIDsSnapshot(ctx context.Context) ([]int64, error) {
	if r == nil || r.c == nil {
		return nil, ErrNilClient
	}
	seen := make(map[int64]struct{})
	out := make([]int64, 0)
	if err := r.appendLiveChannelIDs(ctx, r.memberChannelsKey(), r.ListChannelMember, seen, &out); err != nil {
		return nil, err
	}
	if err := r.appendLiveChannelIDs(ctx, r.subscribedChannelsKey(), r.ListChannelSubscription, seen, &out); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func (r *Registry) appendLiveChannelIDs(ctx context.Context, indexKey string, load func(context.Context, int64) ([]edgecontrol.LocationRecord, error), seen map[int64]struct{}, out *[]int64) error {
	rawIDs, err := r.c.SMembers(ctx, indexKey).Result()
	if err != nil {
		return fmt.Errorf("redis list edge channel ids: %w", err)
	}
	stale := make([]any, 0)
	for _, rawID := range rawIDs {
		channelID, err := strconv.ParseInt(rawID, 10, 64)
		if err != nil || channelID <= 0 {
			stale = append(stale, rawID)
			continue
		}
		records, err := load(ctx, channelID)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			stale = append(stale, rawID)
			continue
		}
		if _, ok := seen[channelID]; ok {
			continue
		}
		seen[channelID] = struct{}{}
		*out = append(*out, channelID)
	}
	if len(stale) > 0 {
		_ = r.c.SRem(ctx, indexKey, stale...).Err()
	}
	return nil
}

func (r *Registry) listIndex(ctx context.Context, indexKey string, keep func(edgecontrol.LocationRecord) bool) ([]edgecontrol.LocationRecord, error) {
	if r == nil || r.c == nil {
		return nil, ErrNilClient
	}
	refs, err := r.c.SMembers(ctx, indexKey).Result()
	if err != nil {
		return nil, fmt.Errorf("redis list edge location index: %w", err)
	}
	recordsByRef, staleRefs, err := r.locationRecordsByRefs(ctx, refs)
	stale := stringsToAny(staleRefs)
	if err == nil {
		for _, ref := range refs {
			record, ok := recordsByRef[ref]
			if ok && keep(record) {
				continue
			}
			stale = append(stale, ref)
		}
	}
	if len(stale) > 0 {
		_ = r.c.SRem(ctx, indexKey, stale...).Err()
	}
	if err != nil {
		return nil, err
	}
	records := make([]edgecontrol.LocationRecord, 0, len(recordsByRef))
	for _, ref := range refs {
		if record, ok := recordsByRef[ref]; ok && keep(record) {
			records = append(records, record)
		}
	}
	return records, nil
}

type locationReference struct {
	ref         string
	instanceID  string
	recordField string
}

type locationLookup struct {
	instanceID string
	references []locationReference
	cmd        *redis.SliceCmd
}

type channelMembershipReference struct {
	ref         string
	instanceID  string
	userID      int64
	recordField string
}

type channelMembershipLookup struct {
	instanceID string
	references []channelMembershipReference
	cmd        *redis.SliceCmd
}

func (r *Registry) channelMembershipRecordsByRefs(ctx context.Context, refs []string) (map[string]edgecontrol.ChannelMembershipRecord, []string, error) {
	uniqueRefs := uniqueNonEmptyStrings(refs)
	records := make(map[string]edgecontrol.ChannelMembershipRecord, len(uniqueRefs))
	if len(uniqueRefs) == 0 {
		return records, nil, nil
	}
	groups := make(map[string][]channelMembershipReference)
	stale := make([]string, 0)
	for _, ref := range uniqueRefs {
		parsed, err := r.parseChannelMembershipRef(ref)
		if err != nil {
			stale = append(stale, ref)
			continue
		}
		groups[parsed.instanceID] = append(groups[parsed.instanceID], parsed)
	}
	pipe := r.c.Pipeline()
	lookups := make([]channelMembershipLookup, 0, len(groups))
	for instanceID, group := range groups {
		for start := 0; start < len(group); start += locationRecordBatch {
			end := min(start+locationRecordBatch, len(group))
			batch := group[start:end]
			fields := make([]string, 1, len(batch)+1)
			fields[0] = leaseField
			for _, ref := range batch {
				fields = append(fields, ref.recordField)
			}
			lookups = append(lookups, channelMembershipLookup{instanceID: instanceID, references: batch, cmd: pipe.HMGet(ctx, r.instanceKey(instanceID), fields...)})
		}
	}
	if len(lookups) == 0 {
		return records, stale, nil
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return records, stale, fmt.Errorf("redis read edge channel memberships: %w", err)
	}
	for _, lookup := range lookups {
		values, err := lookup.cmd.Result()
		if err != nil {
			return records, stale, fmt.Errorf("redis read edge instance channel memberships: %w", err)
		}
		if len(values) == 0 || values[0] == nil {
			for _, ref := range lookup.references {
				stale = append(stale, ref.ref)
			}
			continue
		}
		for i, ref := range lookup.references {
			value := values[i+1]
			if value == nil {
				stale = append(stale, ref.ref)
				continue
			}
			raw, ok := redisValueBytes(value)
			if !ok {
				stale = append(stale, ref.ref)
				return records, stale, fmt.Errorf("unexpected edge channel membership value type %T for %q", value, ref.ref)
			}
			var record edgecontrol.ChannelMembershipRecord
			if err := json.Unmarshal(raw, &record); err != nil {
				stale = append(stale, ref.ref)
				return records, stale, fmt.Errorf("unmarshal edge channel membership %q: %w", ref.ref, err)
			}
			if record.InstanceID != lookup.instanceID || !validInstanceID(record.InstanceID) || record.UserID != ref.userID {
				stale = append(stale, ref.ref)
				continue
			}
			records[ref.ref] = record
		}
	}
	return records, stale, nil
}

func (r *Registry) locationRecordsByRefs(ctx context.Context, refs []string) (map[string]edgecontrol.LocationRecord, []string, error) {
	uniqueRefs := uniqueNonEmptyStrings(refs)
	records := make(map[string]edgecontrol.LocationRecord, len(uniqueRefs))
	if len(uniqueRefs) == 0 {
		return records, nil, nil
	}
	groups := make(map[string][]locationReference)
	stale := make([]string, 0)
	for _, ref := range uniqueRefs {
		parsed, err := r.parseLocationRef(ref)
		if err != nil {
			stale = append(stale, ref)
			continue
		}
		groups[parsed.instanceID] = append(groups[parsed.instanceID], parsed)
	}
	pipe := r.c.Pipeline()
	lookups := make([]locationLookup, 0, len(groups))
	for instanceID, group := range groups {
		for start := 0; start < len(group); start += locationRecordBatch {
			end := min(start+locationRecordBatch, len(group))
			batch := group[start:end]
			fields := make([]string, 1, len(batch)+1)
			fields[0] = leaseField
			for _, ref := range batch {
				fields = append(fields, ref.recordField)
			}
			lookups = append(lookups, locationLookup{instanceID: instanceID, references: batch, cmd: pipe.HMGet(ctx, r.instanceKey(instanceID), fields...)})
		}
	}
	if len(lookups) == 0 {
		return records, stale, nil
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return records, stale, fmt.Errorf("redis read edge location records: %w", err)
	}
	for _, lookup := range lookups {
		values, err := lookup.cmd.Result()
		if err != nil {
			return records, stale, fmt.Errorf("redis read edge instance locations: %w", err)
		}
		if len(values) == 0 || values[0] == nil {
			for _, ref := range lookup.references {
				stale = append(stale, ref.ref)
			}
			continue
		}
		for i, ref := range lookup.references {
			value := values[i+1]
			if value == nil {
				stale = append(stale, ref.ref)
				continue
			}
			raw, ok := redisValueBytes(value)
			if !ok {
				stale = append(stale, ref.ref)
				return records, stale, fmt.Errorf("unexpected edge location value type %T for %q", value, ref.ref)
			}
			var record edgecontrol.LocationRecord
			if err := json.Unmarshal(raw, &record); err != nil {
				stale = append(stale, ref.ref)
				return records, stale, fmt.Errorf("unmarshal edge location %q: %w", ref.ref, err)
			}
			if record.InstanceID != lookup.instanceID || !validLocationRecord(record) {
				stale = append(stale, ref.ref)
				continue
			}
			records[ref.ref] = record
		}
	}
	return records, stale, nil
}

func (r *Registry) locationIndexKeys(record edgecontrol.LocationRecord, nowUnixNano int64) []string {
	indexes := []string{r.rawAuthKey(record.RawAuthKeyID), r.activeRawAuthKeysKey()}
	if record.UserID > 0 {
		indexes = append(indexes, r.userKey(record.UserID))
	}
	if record.BusinessAuthKeyID != ([8]byte{}) {
		indexes = append(indexes, r.businessAuthKey(record.BusinessAuthKeyID))
	}
	for _, channelID := range record.ActiveChannelIDs {
		if channelID != 0 {
			indexes = append(indexes, r.channelInterestKey(channelID))
		}
	}
	for _, channelID := range activeChannelSubscriptionIDs(record.ChannelSubscriptions, nowUnixNano) {
		indexes = append(indexes, r.channelSubscriptionKey(channelID))
	}
	return uniqueNonEmptyStrings(indexes)
}

func (r *Registry) removeStaleIndexMembers(ctx context.Context, staleByIndex map[string][]any) {
	if len(staleByIndex) == 0 || r == nil || r.c == nil {
		return
	}
	pipe := r.c.Pipeline()
	for indexKey, stale := range staleByIndex {
		if indexKey != "" && len(stale) > 0 {
			pipe.SRem(ctx, indexKey, stale...)
		}
	}
	_, _ = pipe.Exec(ctx)
}

func (r *Registry) locationRef(record edgecontrol.LocationRecord) string {
	encodedInstance := base64.RawURLEncoding.EncodeToString([]byte(record.InstanceID))
	return fmt.Sprintf("%s:%s:%d", encodedInstance, keyIDHex(record.RawAuthKeyID), record.SessionID)
}

func (r *Registry) parseLocationRef(ref string) (locationReference, error) {
	parts := strings.SplitN(ref, ":", 3)
	if len(parts) != 3 {
		return locationReference{}, fmt.Errorf("invalid edge location reference")
	}
	instanceRaw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || !validInstanceID(string(instanceRaw)) {
		return locationReference{}, fmt.Errorf("invalid edge location instance")
	}
	rawAuthKeyID, err := parseKeyIDHex(parts[1])
	if err != nil {
		return locationReference{}, err
	}
	sessionID, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || sessionID == 0 {
		return locationReference{}, fmt.Errorf("invalid edge location session")
	}
	return locationReference{ref: ref, instanceID: string(instanceRaw), recordField: r.recordField(rawAuthKeyID, sessionID)}, nil
}

func (r *Registry) recordField(rawAuthKeyID [8]byte, sessionID int64) string {
	return fmt.Sprintf("%s%s:%d", recordFieldPrefix, keyIDHex(rawAuthKeyID), sessionID)
}

func (r *Registry) indexesField(rawAuthKeyID [8]byte, sessionID int64) string {
	return fmt.Sprintf("%s%s:%d", indexesFieldPrefix, keyIDHex(rawAuthKeyID), sessionID)
}

func (r *Registry) channelPresenceField(rawAuthKeyID [8]byte, sessionID int64) string {
	return fmt.Sprintf("%s%s:%d", channelPresenceFieldPrefix, keyIDHex(rawAuthKeyID), sessionID)
}

func (r *Registry) channelMembershipRef(instanceID string, userID int64) string {
	encodedInstance := base64.RawURLEncoding.EncodeToString([]byte(instanceID))
	return fmt.Sprintf("member:%s:%d", encodedInstance, userID)
}

func (r *Registry) parseChannelMembershipRef(ref string) (channelMembershipReference, error) {
	parts := strings.SplitN(ref, ":", 3)
	if len(parts) != 3 || parts[0] != "member" {
		return channelMembershipReference{}, fmt.Errorf("invalid edge channel membership reference")
	}
	instanceRaw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || !validInstanceID(string(instanceRaw)) {
		return channelMembershipReference{}, fmt.Errorf("invalid edge channel membership instance")
	}
	userID, err := strconv.ParseInt(parts[2], 10, 64)
	if err != nil || userID <= 0 {
		return channelMembershipReference{}, fmt.Errorf("invalid edge channel membership user")
	}
	return channelMembershipReference{ref: ref, instanceID: string(instanceRaw), userID: userID, recordField: r.membershipRecordField(userID)}, nil
}

func (r *Registry) membershipRecordField(userID int64) string {
	return fmt.Sprintf("%s%d", membershipRecordFieldPrefix, userID)
}

func (r *Registry) membershipChannelIDsField(userID int64) string {
	return fmt.Sprintf("%s%d", membershipChannelIDsFieldPrefix, userID)
}

func (r *Registry) userKey(userID int64) string { return fmt.Sprintf("%s:user:%d", r.prefix, userID) }
func (r *Registry) rawAuthKey(authKeyID [8]byte) string {
	return fmt.Sprintf("%s:raw:%s", r.prefix, keyIDHex(authKeyID))
}
func (r *Registry) businessAuthKey(authKeyID [8]byte) string {
	return fmt.Sprintf("%s:auth:%s", r.prefix, keyIDHex(authKeyID))
}
func (r *Registry) instanceKey(instanceID string) string {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(instanceID))
	return fmt.Sprintf("%s:instance:%s", r.prefix, encoded)
}
func (r *Registry) channelInterestKey(channelID int64) string {
	return fmt.Sprintf("%s:channel:interest:%d", r.prefix, channelID)
}
func (r *Registry) channelMemberKey(channelID int64) string {
	return fmt.Sprintf("%s:channel:member:%d", r.prefix, channelID)
}
func (r *Registry) channelMemberKeyPrefix() string {
	return fmt.Sprintf("%s:channel:member:", r.prefix)
}
func (r *Registry) channelEdgeMemberKey(channelID int64) string {
	return fmt.Sprintf("%s:channel-edge:member:%d", r.prefix, channelID)
}
func (r *Registry) channelEdgeMemberKeyPrefix() string {
	return fmt.Sprintf("%s:channel-edge:member:", r.prefix)
}
func (r *Registry) channelEdgeInterestKey(channelID int64) string {
	return fmt.Sprintf("%s:channel-edge:interest:%d", r.prefix, channelID)
}
func (r *Registry) channelEdgeInterestKeyPrefix() string {
	return fmt.Sprintf("%s:channel-edge:interest:", r.prefix)
}
func (r *Registry) channelEdgeSubscriptionKey(channelID int64) string {
	return fmt.Sprintf("%s:channel-edge:subscription:%d", r.prefix, channelID)
}
func (r *Registry) channelEdgeSubscriptionKeyPrefix() string {
	return fmt.Sprintf("%s:channel-edge:subscription:", r.prefix)
}
func (r *Registry) channelSubscriptionKey(channelID int64) string {
	return fmt.Sprintf("%s:channel:subscription:%d", r.prefix, channelID)
}
func (r *Registry) memberChannelsKey() string { return fmt.Sprintf("%s:channels:member", r.prefix) }
func (r *Registry) subscribedChannelsKey() string {
	return fmt.Sprintf("%s:channels:subscription", r.prefix)
}
func (r *Registry) activeRawAuthKeysKey() string {
	return fmt.Sprintf("%s:active_raw_auth_keys", r.prefix)
}

func (r *Registry) locationRevisionKey() string {
	return fmt.Sprintf("%s:location_revision", r.prefix)
}

func keyIDHex(authKeyID [8]byte) string { return hex.EncodeToString(authKeyID[:]) }

func parseKeyIDHex(raw string) ([8]byte, error) {
	body, err := hex.DecodeString(raw)
	if err != nil {
		return [8]byte{}, err
	}
	if len(body) != 8 {
		return [8]byte{}, fmt.Errorf("invalid key id length %d", len(body))
	}
	var id [8]byte
	copy(id[:], body)
	return id, nil
}

func containsInt64(values []int64, needle int64) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}

func containsActiveSubscription(values []edgecontrol.ChannelSubscriptionLocation, channelID, nowUnixNano int64) bool {
	for _, value := range values {
		if value.ChannelID == channelID && value.ExpiresAtUnixNano > nowUnixNano {
			return true
		}
	}
	return false
}

func activeChannelSubscriptionIDs(values []edgecontrol.ChannelSubscriptionLocation, nowUnixNano int64) []int64 {
	out := make([]int64, 0, len(values))
	for _, value := range values {
		if value.ChannelID != 0 && value.ExpiresAtUnixNano > nowUnixNano {
			out = append(out, value.ChannelID)
		}
	}
	return out
}

func uniquePositiveInt64sSorted(values []int64) []int64 {
	seen := make(map[int64]struct{}, len(values))
	out := make([]int64, 0, len(values))
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func uniquePositiveUserIDs(values []int64) []int64 {
	seen := make(map[int64]struct{}, len(values))
	out := make([]int64, 0, len(values))
	for _, value := range values {
		if value <= 0 {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func uniqueNonEmptyStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func redisValueBytes(value any) ([]byte, bool) {
	switch typed := value.(type) {
	case string:
		return []byte(typed), true
	case []byte:
		return typed, true
	default:
		return nil, false
	}
}

func boolFlag(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func ttlMillis(ttl time.Duration) int64 {
	millis := ttl.Milliseconds()
	if millis < 1 {
		return 1
	}
	return millis
}

func stringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out
}

func stringsToAny(values []string) []any {
	out := make([]any, 0, len(values))
	for _, value := range values {
		out = append(out, value)
	}
	return out
}
