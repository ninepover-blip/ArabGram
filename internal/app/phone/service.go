package phone

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/iamxvbaba/td/clock"

	"telesrv/internal/domain"
)

// 客户端可识别的业务错误；rpc 层映射为对应 RPC_ERROR（CALL_* 等）。
var (
	ErrPeerInvalid                = errors.New("phone: call peer invalid")
	ErrAlreadyAccepted            = errors.New("phone: call already accepted")
	ErrAlreadyDeclined            = errors.New("phone: call already declined")
	ErrOccupyFailed               = errors.New("phone: too many active calls")
	ErrMissingActiveCallStore     = errors.New("phone: active call store is required")
	ErrProtocolLayerInvalid       = errors.New("phone: protocol layer invalid")
	ErrProtocolCompatLayerInvalid = errors.New("phone: protocol compat layer invalid")
	ErrProtocolFlagsInvalid       = errors.New("phone: protocol flags invalid")
	// ErrGAHashMismatch：confirmCall 揭示的 g_a 与 requestCall 承诺的 SHA256 不符。
	// 服务端同时把通话强制置为 discarded(disconnect)，防止攻击者卡死状态机。
	ErrGAHashMismatch = errors.New("phone: g_a does not match committed hash")
)

// minSupportedLayer 是 libtgvoip/tgcalls 的最低协议层（TDesktop kMinLayer、
// DrKLO VoIPService.CALL_MIN_LAYER 均为 65）。
const minSupportedLayer = 65

const (
	gaHashSize = sha256.Size // 32
	dhPubSize  = 256         // g_a / g_b 都是 2048-bit
)

// Config 是通话服务的运行参数。
type Config struct {
	// RingTimeout 是服务端兜底超时（与下发给客户端的 callRingTimeoutMs 同源，默认 90s）。
	RingTimeout time.Duration
	// TombstoneTTL 是终态 tombstone 保留期（吸收双方同时挂断/晚到 RPC 的幂等窗口）。
	TombstoneTTL time.Duration
	// MaxActivePerUser 是单用户并发非终态通话上限（防呼叫轰炸自锁）。
	MaxActivePerUser int
	// MaxRegistryEntries 是 active-call store 的硬上限。达到上限时拒绝新通话，
	// 不驱逐可能仍在进行的 Confirmed 通话。
	MaxRegistryEntries int
	// SignalingRatePerSecond 是单通话每秒信令转发上限；超限静默丢弃（不破坏客户端状态机）。
	SignalingRatePerSecond int
}

func (c Config) withDefaults() Config {
	if c.RingTimeout <= 0 {
		c.RingTimeout = 90 * time.Second
	}
	if c.TombstoneTTL <= 0 {
		c.TombstoneTTL = 60 * time.Second
	}
	if c.MaxActivePerUser <= 0 {
		c.MaxActivePerUser = 4
	}
	if c.MaxRegistryEntries <= 0 {
		c.MaxRegistryEntries = 10_000
	}
	if c.SignalingRatePerSecond <= 0 {
		c.SignalingRatePerSecond = 50
	}
	return c
}

// ActiveCallStore is the multi-Core state boundary for short-lived private call
// signaling state. Production Core must inject a shared implementation.
type ActiveCallStore interface {
	RequestCall(ctx context.Context, callerID int64, in domain.PhoneCallRequest, now time.Time, cfg Config) (domain.PhoneCall, error)
	ReceivedCall(ctx context.Context, userID, callID, accessHash int64, now time.Time) (domain.PhoneCall, bool, error)
	AcceptCall(ctx context.Context, userID, callID, accessHash int64, gb []byte, proto domain.PhoneCallProtocol, device domain.SessionRef) (domain.PhoneCall, error)
	ConfirmCall(ctx context.Context, userID, callID, accessHash int64, ga []byte, keyFingerprint int64, proto domain.PhoneCallProtocol, now time.Time, cfg Config) (domain.PhoneCall, bool, error)
	DiscardCallWithSlug(ctx context.Context, userID, callID, accessHash int64, reason domain.PhoneCallDiscardReason, reasonSlug string, duration int, now time.Time, cfg Config) (domain.PhoneCall, bool, error)
	ExpireDue(ctx context.Context, now time.Time, cfg Config) []domain.PhoneCall
	Signal(ctx context.Context, userID, callID, accessHash int64, now time.Time, rateLimit int, forward func(peerUserID int64, peerDevice domain.SessionRef)) (bool, error)
	Lookup(ctx context.Context, callID, accessHash int64) (domain.PhoneCall, bool)
}

// Service 实现私聊通话信令状态机。所有方法返回的 domain.PhoneCall 都是当时快照。
type Service struct {
	cfg   Config
	clk   clock.Clock
	calls ActiveCallStore
}

// Option 配置 Service。
type Option func(*Service)

// WithClock 注入测试时钟。
func WithClock(clk clock.Clock) Option {
	return func(s *Service) { s.clk = clk }
}

// NewService 创建通话服务。
func NewService(cfg Config, calls ActiveCallStore, opts ...Option) (*Service, error) {
	if calls == nil {
		return nil, ErrMissingActiveCallStore
	}
	s := &Service{cfg: cfg.withDefaults(), clk: clock.System, calls: calls}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// RequestCall 受理主叫请求：校验协议与配额、(callerID, randomID) 幂等去重、建档。
// 隐私/拉黑/目标用户合法性由 rpc 层先行校验。
func (s *Service) RequestCall(ctx context.Context, callerID int64, in domain.PhoneCallRequest) (domain.PhoneCall, error) {
	if err := validateProtocol(in.Protocol); err != nil {
		return domain.PhoneCall{}, err
	}
	if len(in.GAHash) != gaHashSize {
		return domain.PhoneCall{}, ErrProtocolFlagsInvalid
	}
	return s.calls.RequestCall(ctx, callerID, in, s.clk.Now(), s.cfg)
}

// ReceivedCall 标记被叫设备已收到来电。首次（Requested→Ringing）返回 transitioned=true，
// 其余状态幂等成功（多设备各自上报、晚到无害）。
func (s *Service) ReceivedCall(ctx context.Context, userID, callID, accessHash int64) (domain.PhoneCall, bool, error) {
	return s.calls.ReceivedCall(ctx, userID, callID, accessHash, s.clk.Now())
}

// AcceptCall 受理被叫接听。多设备/多 Core 并发竞争由 active-call store 串行化：
// 首个完成迁移者赢，后到者收 ErrAlreadyAccepted（其 UI 自行收场）。
func (s *Service) AcceptCall(ctx context.Context, userID, callID, accessHash int64, gb []byte, proto domain.PhoneCallProtocol, device domain.SessionRef) (domain.PhoneCall, error) {
	if err := validateProtocol(proto); err != nil {
		return domain.PhoneCall{}, err
	}
	if len(gb) != dhPubSize || allZero(gb) {
		return domain.PhoneCall{}, ErrProtocolFlagsInvalid
	}
	return s.calls.AcceptCall(ctx, userID, callID, accessHash, gb, proto, device)
}

// ConfirmCall 受理主叫确认：核验 SHA256(g_a) 与承诺一致后进入 Confirmed。
// 核验失败时通话被强制置为 discarded(disconnect)，返回 (终态快照, true, ErrGAHashMismatch)，
// 调用方须把终态推送给双方。
func (s *Service) ConfirmCall(ctx context.Context, userID, callID, accessHash int64, ga []byte, keyFingerprint int64, proto domain.PhoneCallProtocol) (domain.PhoneCall, bool, error) {
	return s.calls.ConfirmCall(ctx, userID, callID, accessHash, ga, keyFingerprint, proto, s.clk.Now(), s.cfg)
}

// DiscardCall 挂断：任意非终态可达，幂等。already=true 表示通话此前已是终态
// （双方同时挂断：先到者定 reason，后到者拿快照）。
func (s *Service) DiscardCall(ctx context.Context, userID, callID, accessHash int64, reason domain.PhoneCallDiscardReason, duration int) (domain.PhoneCall, bool, error) {
	return s.DiscardCallWithSlug(ctx, userID, callID, accessHash, reason, "", duration)
}

func (s *Service) DiscardCallWithSlug(ctx context.Context, userID, callID, accessHash int64, reason domain.PhoneCallDiscardReason, reasonSlug string, duration int) (domain.PhoneCall, bool, error) {
	if reason == "" {
		reason = domain.PhoneCallDiscardReasonHangup
	}
	if reason != domain.PhoneCallDiscardReasonMigrateConference {
		reasonSlug = ""
	}
	return s.calls.DiscardCallWithSlug(ctx, userID, callID, accessHash, reason, reasonSlug, duration, s.clk.Now(), s.cfg)
}

// ExpireDue 把超时的非终态通话迁入终态并返回快照（调用方负责推送与落历史）：
// Requested/Ringing 超时 → missed（即「未接来电」来源）；Accepted 悬挂（主叫
// 永不 confirm）→ disconnect。Confirmed 通话没有服务端时长上限，不在此回收。
// 具体 store 负责终态 tombstone 生命周期。
func (s *Service) ExpireDue(ctx context.Context, now time.Time) []domain.PhoneCall {
	return s.calls.ExpireDue(ctx, now, s.cfg)
}

// Signal 校验并串行转发一条信令。forward 在该通话专属的顺序边界内执行
// （保证转发顺序与受理顺序一致）。peerDevice 是对端受理设备锚点
// （可能为零值/已失效），仅作定向推送 fast-path 提示。
// drop=true 表示按契约静默吞掉（tombstone 尾包 / 超过速率上限），调用方应返回成功。
func (s *Service) Signal(ctx context.Context, userID, callID, accessHash int64, forward func(peerUserID int64, peerDevice domain.SessionRef)) (drop bool, err error) {
	return s.calls.Signal(ctx, userID, callID, accessHash, s.clk.Now(), s.cfg.SignalingRatePerSecond, forward)
}

// Lookup 返回通话快照（rpc 层宽容校验 setCallRating/saveCallDebug 等晚到请求用）。
func (s *Service) Lookup(ctx context.Context, callID, accessHash int64) (domain.PhoneCall, bool) {
	return s.calls.Lookup(ctx, callID, accessHash)
}

func validateProtocol(p domain.PhoneCallProtocol) error {
	if p.MinLayer > p.MaxLayer {
		return ErrProtocolLayerInvalid
	}
	if p.MaxLayer < minSupportedLayer {
		return ErrProtocolCompatLayerInvalid
	}
	if !p.UDPP2P && !p.UDPReflector {
		return ErrProtocolFlagsInvalid
	}
	if len(p.LibraryVersions) == 0 {
		return ErrProtocolFlagsInvalid
	}
	return nil
}

// negotiateProtocol 在 accept 时合并双方 protocol（官方语义：服务端只回「最优」单值
// library version）。版本无交集时透传被叫列表（被叫是先实例化 tgcalls 的一方，
// 主叫侧有 kDefaultVersion 兜底）——绝不因版本差异拒绝通话。
func negotiateProtocol(caller, callee domain.PhoneCallProtocol) (domain.PhoneCallProtocol, error) {
	out := domain.PhoneCallProtocol{
		UDPP2P:       caller.UDPP2P && callee.UDPP2P,
		UDPReflector: caller.UDPReflector || callee.UDPReflector,
		MinLayer:     maxInt(caller.MinLayer, callee.MinLayer),
		MaxLayer:     minInt(caller.MaxLayer, callee.MaxLayer),
	}
	if out.MinLayer > out.MaxLayer {
		return domain.PhoneCallProtocol{}, ErrProtocolCompatLayerInvalid
	}
	if best, ok := bestCommonVersion(caller.LibraryVersions, callee.LibraryVersions); ok {
		out.LibraryVersions = []string{best}
	} else {
		out.LibraryVersions = append([]string(nil), callee.LibraryVersions...)
	}
	return out, nil
}

// preferredVersions 是交集命中时的优先选择序（读客户端源码定下的硬约束）：
//   - "9.0.0"=InstanceV2Impl+V2 消息级信令，TDesktop 与 DrKLO 注册表都有，最稳；
//   - ⚠ 绝不能选 "10.0.0"/"11.0.0"/"12.0.0"/"13.0.0" 当 [0]：DrKLO 的视频可用性
//     判断是 `"2.7.7".compareTo(versions[0]) <= 0` 的**字符串字典序**比较
//     （VoIPService.java:3464），"1x.0.0" 字典序小于 "2.7.7" 会让 Android 直接
//     销毁摄像头采集（视频通话黑屏）；"12/13" 还是 V3 SCTP-over-signaling，
//     对服务端信令限速不友好；
//   - 也绝不能选 "2.7.7"/"5.0.0"/"2.4.4"：legacy 实现依赖我们不下发的
//     reflector endpoints，无路可走。
var preferredVersions = []string{"9.0.0", "8.0.0", "7.0.0"}

// bestCommonVersion 取两侧版本集合交集：优先 preferredVersions 顺位命中，
// 否则退化为语义化最高者（容忍未来未知版本集）。
func bestCommonVersion(a, b []string) (string, bool) {
	inB := make(map[string]struct{}, len(b))
	for _, v := range b {
		inB[v] = struct{}{}
	}
	inBoth := make(map[string]struct{}, len(a))
	for _, v := range a {
		if _, ok := inB[v]; ok {
			inBoth[v] = struct{}{}
		}
	}
	for _, v := range preferredVersions {
		if _, ok := inBoth[v]; ok {
			return v, true
		}
	}
	best, found := "", false
	for v := range inBoth {
		if !found || compareVersion(v, best) > 0 {
			best, found = v, true
		}
	}
	return best, found
}

// compareVersion 按点分十进制比较（"9.0.0" < "10.0.0"）；非数字段退化为字符串比较。
func compareVersion(a, b string) int {
	as, bs := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < len(as) || i < len(bs); i++ {
		var av, bv string
		if i < len(as) {
			av = as[i]
		}
		if i < len(bs) {
			bv = bs[i]
		}
		an, aerr := strconv.Atoi(av)
		bn, berr := strconv.Atoi(bv)
		switch {
		case aerr == nil && berr == nil:
			if an != bn {
				return an - bn
			}
		default:
			if c := strings.Compare(av, bv); c != 0 {
				return c
			}
		}
	}
	return 0
}

func sha256Mismatch(data, want []byte) bool {
	got := sha256.Sum256(data)
	return !bytes.Equal(got[:], want)
}

func allZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
