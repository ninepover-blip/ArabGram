package store

import (
	"context"
	"errors"
	"math"
)

var (
	// ErrInvalidAuthKeyProtocolExpiry 表示新握手试图写入 migration-only
	// unknown sentinel 或超出 TL int32 时间戳范围的协议寿命。
	ErrInvalidAuthKeyProtocolExpiry = errors.New("invalid auth key protocol expiry")
	// ErrAuthKeyProtocolMetadataConflict 表示同一 cryptographic auth_key_id
	// 被尝试改写为另一 key body 或另一 permanent/temp 类型/寿命。
	ErrAuthKeyProtocolMetadataConflict = errors.New("auth key protocol metadata conflict")
	// ErrAuthKeyNotFound prevents client metadata writes from silently succeeding
	// after the protocol key row has already disappeared. auth_keys is the
	// authoritative Layer source; an authorization mirror must never advance on
	// its own when that primary write did not happen.
	ErrAuthKeyNotFound = errors.New("auth key not found")
	// ErrAuthKeyNotPermanent 防止 authorization 落到 temporary/legacy-unknown key。
	ErrAuthKeyNotPermanent = errors.New("auth key is not permanent")
	// ErrAuthKeyBindingInvalid 表示 temp/perm 引用缺失、类型错误，或 binding
	// expiry 未归一到握手权威值。
	ErrAuthKeyBindingInvalid = errors.New("invalid temporary auth key binding")
)

// ValidNewAuthKeyProtocolExpiry 只允许握手写 permanent(0) 或 TL int32
// 范围内的 positive temporary expiry。-1 仅能由 migration 0086 写入。
func ValidNewAuthKeyProtocolExpiry(expiresAt int) bool {
	return expiresAt >= 0 && int64(expiresAt) <= math.MaxInt32
}

// AuthKeyData 是一条持久化的 MTProto auth key 记录。
//
// 不依赖 td 协议类型：连接层在边界做 crypto.AuthKey ↔ AuthKeyData 转换。
type AuthKeyData struct {
	ID         [8]byte   // auth_key_id（key 的 SHA1 低 64 位）
	Value      [256]byte // 2048-bit auth key
	ServerSalt int64     // 密钥交换产出的初始 server salt
	CreatedAt  int64     // unix 秒
	// ExpiresAt 是 temporary/media-temporary auth key 的协议失效时间（unix 秒）。
	// 0 只允许表示 permanent key；-1 仅表示 migration 0086 无法证明类型的历史 key，
	// edge 必须用 -404 拒绝并迫使客户端重握手。key 类型是握手事实，不能由
	// authorization 是否存在推断。
	ExpiresAt int
	Layer     int
	// LayerObservationID globally orders durable explicit Layer observations
	// across sessions, processes and restarts. Zero means legacy/no ordered
	// evidence; it is still a usable inherited default but cannot outrank a
	// positive observation during temp-to-perm identity merge.
	LayerObservationID int64
	DeviceModel        string
	Platform           string
	SystemVersion      string
	APIID              int
	AppVersion         string
	// 用户绑定不在此处：auth_key 是协议产物，授权（auth_key↔user + 设备信息）由 authorization 承载（P2）。
}

type AuthKeyClientInfo struct {
	// Layer is the durable last-known default. The RPC boundary validates it
	// against the generated profile set before use; stores must preserve the
	// exact value and must never clamp a future unsupported Layer.
	Layer         int
	DeviceModel   string
	Platform      string
	SystemVersion string
	APIID         int
	AppVersion    string
}

// AuthKeyBindingKeys is the authoritative pair used to verify one
// auth.bindTempAuthKey proof. Both rows are loaded and activity-touched in one
// store operation.
type AuthKeyBindingKeys struct {
	Temporary      AuthKeyData
	TemporaryFound bool
	Permanent      AuthKeyData
	PermanentFound bool
}

type authKeyDeleteOriginContextKey struct{}

// AuthKeyDeleteOrigin carries wire-level destroy_auth_key origin metadata across
// the AuthKeyStore boundary. It is intentionally optional: stores ignore it, but
// remote CoreExec stores use it to keep the requester writable long enough to
// receive destroy_auth_key_ok while every other Edge/session is fenced.
type AuthKeyDeleteOrigin struct {
	ExceptSessionID int64
}

func WithAuthKeyDeleteOrigin(ctx context.Context, origin AuthKeyDeleteOrigin) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if origin.ExceptSessionID == 0 {
		return ctx
	}
	return context.WithValue(ctx, authKeyDeleteOriginContextKey{}, origin)
}

func AuthKeyDeleteOriginFrom(ctx context.Context) (AuthKeyDeleteOrigin, bool) {
	if ctx == nil {
		return AuthKeyDeleteOrigin{}, false
	}
	origin, ok := ctx.Value(authKeyDeleteOriginContextKey{}).(AuthKeyDeleteOrigin)
	return origin, ok && origin.ExceptSessionID != 0
}

// MergeAuthKeyLayerObservations resolves the inherited default when a raw
// temporary key is bound to its permanent identity. Positive observation IDs
// are globally ordered durable evidence. Equal positive IDs must describe the
// same Layer; zero is legacy/unordered and therefore defers to the permanent
// identity. Both rows must be written to the returned tuple atomically.
func MergeAuthKeyLayerObservations(
	tempLayer int,
	tempObservationID int64,
	permLayer int,
	permObservationID int64,
) (layer int, observationID int64, err error) {
	if tempLayer < 0 || permLayer < 0 || tempObservationID < 0 || permObservationID < 0 ||
		(tempObservationID > 0 && tempLayer == 0) ||
		(permObservationID > 0 && permLayer == 0) {
		return 0, 0, ErrAuthKeySessionLayerInvalid
	}
	switch {
	case tempObservationID > permObservationID:
		return tempLayer, tempObservationID, nil
	case permObservationID > tempObservationID:
		return permLayer, permObservationID, nil
	case tempObservationID > 0 && tempLayer != permLayer:
		return 0, 0, ErrAuthKeySessionLayerConflict
	case tempObservationID > 0:
		return tempLayer, tempObservationID, nil
	default:
		return permLayer, 0, nil
	}
}

// AuthKeyStore 持久化 auth key。实现见 store/memory（测试替身）、store/postgres。
type AuthKeyStore interface {
	// Save 保存一条 auth key 记录；同 ID 重试只能保持 key body 与协议类型/寿命不变。
	Save(ctx context.Context, k AuthKeyData) error
	// Get 按 auth_key_id 查询；不存在时 found=false。
	Get(ctx context.Context, id [8]byte) (data AuthKeyData, found bool, err error)
	Revalidate(ctx context.Context, id [8]byte) (data AuthKeyData, found bool, err error)
	LoadBindingKeys(ctx context.Context, tempID, permID [8]byte) (AuthKeyBindingKeys, error)
	// UpdateClientInfo 合并更新 auth key 的客户端协商元数据。目标 key 不存在时
	// 必须返回 ErrAuthKeyNotFound，禁止把缺失 primary 当成成功后继续更新 mirror。
	// 空字段不覆盖已有值，layer/api_id 为 0 时不覆盖。
	UpdateClientInfo(ctx context.Context, id [8]byte, info AuthKeyClientInfo) error
	// Delete 删除一条 auth key 记录（destroy_auth_key）。不存在时静默成功。
	// 连接层每帧按 auth_key_id 回查本接口，删除后该 key 的入站帧立即失效。
	Delete(ctx context.Context, id [8]byte) error
}
