// Package sfu 是群通话媒体面（Selective Forwarding Unit）的接口与实现。
//
// 角色契约（来自 tgcalls GroupNetworkManager 的硬性要求）：客户端 ICE=CONTROLLED、
// DTLS setup="passive"（等待握手）；SFU 必须跑 full-ICE CONTROLLING（ICE-Lite 不可用）
// 并以 DTLS client（setup="active"）主动发起握手。上行 join JSON 不含 ICE candidates，
// SFU 只能凭 ufrag/pwd 认证的 STUN Binding Request 学到客户端地址（peer-reflexive）。
//
// Disabled 只用于显式测试或探针。
// 生产群通话 join 必须通过远端 standalone SFU owner；不得因为依赖未注入而自动创建
// Disabled 纯信令 fallback。
package sfu

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
)

// SsrcGroup 是上行 join JSON 的 ssrc-groups 条目：semantics ∈ {"SIM","FID"}。
// SIM 的 sources 按质量低→高排列；FID 是 [媒体 ssrc, RTX 重传 ssrc] 对。
type SsrcGroup struct {
	Semantics string
	Sources   []uint32
}

// EndpointKind 区分同一参与者的两条媒体连接（M4：屏幕共享是独立的第二连接，
// 独立 ICE/DTLS/ssrc 集，但 TL 层归属同一 participant）。
type EndpointKind int

const (
	EndpointMain EndpointKind = iota
	EndpointPresentation
)

// ClientOffer 解析自 phone.joinGroupCall / joinGroupCallPresentation 的上行 params JSON。
type ClientOffer struct {
	AudioSSRC         uint32
	Ufrag             string
	Pwd               string
	FingerprintSHA256 string // 形如 "AA:BB:..."（RFC 4572 hex）
	// SsrcGroups 是视频 simulcast/RTX 源组（主 join 即带，无论摄像头开关）。
	// SFU 据此做层选择：订阅端只为 SIM[0]（最低层）建解码 sink，发到其它层
	// 原始 ssrc 的包会被客户端静默丢弃——v1 转发策略=只转 SIM[0] 层与其 FID
	// RTX 伙伴，其余层在 SFU 侧丢弃（省下行且行为正确）。
	SsrcGroups []SsrcGroup
}

// Candidate 是下发给客户端的 ICE 候选（updateGroupCallConnection JSON 的
// transport.candidates 条目，字段语义照抄 tgcalls 解析代码）。
type Candidate struct {
	IP       string
	Port     int
	Protocol string // "udp"
	Type     string // "host"
}

// ServerAnswer 是 SFU 端的传输参数，由 rpc 层组装进下行 JSON。
type ServerAnswer struct {
	Ufrag             string
	Pwd               string
	FingerprintSHA256 string
	Candidates        []Candidate
}

// Service 是信令层⇄媒体面的边界（未来横向扩展/远端 SFU 的替换点）。
type Service interface {
	// Enabled 报告媒体面是否真实可用。Disabled 只能显式注入，不能作为缺失依赖兜底。
	Enabled() bool
	// Join 为参与者分配/重建媒体 endpoint，返回 SFU 端传输参数。
	// kind=EndpointPresentation 时是同一参与者的第二连接（独立 ICE/DTLS）。
	Join(ctx context.Context, callID, userID int64, kind EndpointKind, offer ClientOffer) (ServerAnswer, error)
	// Leave 拆除参与者的 endpoint；kind=EndpointMain 时联动拆其 presentation
	//（客户端整体离会从不补发 leaveGroupCallPresentation）。
	Leave(ctx context.Context, callID, userID int64, kind EndpointKind) error
	CloseRoom(ctx context.Context, callID int64) error
	// AliveUserIDs 返回 callID 房间内媒体面仍存活（ICE consent/近期 SRTP 收包）
	// 的参与者。sweeper 的判死条件 = 心跳过期 ∧ 媒体面不存活（双过期）。
	AliveUserIDs(callID int64) []int64
}

// disabled 是显式测试/探针用的纯信令实现。
type disabled struct{}

// Disabled 返回纯信令 SFU。生产 RPC 与 Core remote owner 不得在依赖缺失时自动创建它。
func Disabled() Service {
	return disabled{}
}

func (disabled) Enabled() bool { return false }

func (disabled) Join(_ context.Context, _, _ int64, _ EndpointKind, _ ClientOffer) (ServerAnswer, error) {
	ufrag, err := randomICEString(8)
	if err != nil {
		return ServerAnswer{}, err
	}
	pwd, err := randomICEString(24)
	if err != nil {
		return ServerAnswer{}, err
	}
	fp, err := randomFingerprint()
	if err != nil {
		return ServerAnswer{}, err
	}
	// 空 candidates：客户端无可连地址，保持 Connecting（M0 预期）。
	return ServerAnswer{Ufrag: ufrag, Pwd: pwd, FingerprintSHA256: fp, Candidates: nil}, nil
}

func (disabled) Leave(context.Context, int64, int64, EndpointKind) error { return nil }
func (disabled) CloseRoom(context.Context, int64) error                  { return nil }
func (disabled) AliveUserIDs(int64) []int64                              { return nil }

const iceAlphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func randomICEString(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("sfu: random ice string: %w", err)
	}
	var b strings.Builder
	for _, v := range buf {
		b.WriteByte(iceAlphabet[int(v)%len(iceAlphabet)])
	}
	return b.String(), nil
}

// randomFingerprint 生成语法合法的 sha-256 指纹串（32 字节 hex，冒号分隔）。
// M0 无 DTLS 端点，指纹值不会被验证（客户端连不上任何 candidate）。
func randomFingerprint() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("sfu: random fingerprint: %w", err)
	}
	parts := make([]string, len(buf))
	for i, v := range buf {
		parts[i] = strings.ToUpper(hex.EncodeToString([]byte{v}))
	}
	return strings.Join(parts, ":"), nil
}
