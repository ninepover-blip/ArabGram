// Package turnsrv 是私聊通话 P3 的 TURN/STUN 媒体与信令边界（pion/turn/v5）。
//
// 角色：私聊媒体面是客户端间 P2P（tgcalls/WebRTC），服务端只在 NAT 穿透失败时
// 充当中继。SFU role 持有 UDP listener/relay allocations，Core role 只使用同一
// shared secret 签发 phoneCall.connections 的临时凭据，不持有媒体 socket。
//
// 凭据采用 TURN REST 规范（draft-uberti-behave-turn-rest-00，coturn 同款）：
// username = "<unix 过期时间>:<标识>"，password = base64(HMAC-SHA1(secret, username))。
// 这使得未来切换到外部 coturn 时只需共享 secret，信令侧零改动。
package turnsrv

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"time"

	"github.com/pion/logging"
	"github.com/pion/turn/v5"
	"go.uber.org/zap"
)

// Config 是 TURN credential/listener 边界的运行参数。
type Config struct {
	// UDPPort 是 TURN/STUN 监听端口（独立于 SFU 端口：两者都要独占消费各自
	// socket 上的 STUN 流量，不能共用）。Windows 防火墙需放行。
	UDPPort int
	// BindIP is the local IPv4 address used by both the TURN listener and relay
	// allocation sockets. It matters with host networking because Docker no
	// longer applies a host_ip publishing boundary.
	BindIP string
	// AdvertiseIP 是写进 phoneConnectionWebrtc 与 relay 分配地址的客户端可达
	// 地址。⚠ 127.0.0.1 时真机拿到的 relay candidate 不可达（媒体面静默失败）。
	AdvertiseIP string
	// Realm 是 TURN long-term credential 的 realm（任意稳定串即可）。
	Realm string
	// SharedSecret 是 REST 凭据的 HMAC 密钥；为空则进程级随机生成
	// （单实例自洽；多实例/外部 coturn 必须显式配置同一值）。
	SharedSecret string
	// RelayMinPort/RelayMaxPort 限定 relay 分配的 UDP 端口段（防火墙放行范围）。
	RelayMinPort int
	RelayMaxPort int
	// CredentialTTL 是签发凭据的有效期。⚠ 必须 ≥ 单次通话时长上限：libwebrtc
	// TurnPort 的 allocation refresh 复用初始凭据，凭据过期会让进行中的中继
	// 通话断流（TDesktop 读码结论），缺省 24h。
	CredentialTTL time.Duration
	Logger        *zap.Logger
}

// Service 是信令层可见的 TURN 边界。
type Service interface {
	// Enabled 报告中继是否真实可用（false 时 connections 只能下发空列表，
	// 行为退回 P1 的 LAN 直连）。
	Enabled() bool
	// Credentials 为一通通话签发临时凭据（user 任意标识，惯例用 callID）。
	Credentials(user string) (username, password string, err error)
	// IP/Port 返回客户端可达的服务地址。
	IP() string
	Port() int
	Close() error
}

// Disabled 返回未启用实现。
func Disabled() Service { return disabled{} }

type disabled struct{}

func (disabled) Enabled() bool { return false }
func (disabled) Credentials(string) (string, string, error) {
	return "", "", fmt.Errorf("turnsrv: disabled")
}
func (disabled) IP() string   { return "" }
func (disabled) Port() int    { return 0 }
func (disabled) Close() error { return nil }

type pionTURN struct {
	*credentialIssuer
	server *turn.Server
}

type credentialIssuer struct {
	cfg Config
}

// NewCredentialIssuer creates the Core-side signaling projection without
// opening a TURN listener. A stable shared secret is mandatory because the SFU
// role validates these credentials in a different process.
func NewCredentialIssuer(cfg Config) (Service, error) {
	if cfg.SharedSecret == "" {
		return nil, fmt.Errorf("turnsrv: shared secret is required for split credential issuer")
	}
	normalized, err := normalizeCredentialConfig(cfg, false)
	if err != nil {
		return nil, err
	}
	return &credentialIssuer{cfg: normalized}, nil
}

// New starts the SFU-owned TURN media listener and relay allocator.
func New(cfg Config) (Service, error) {
	var err error
	cfg, err = normalizeCredentialConfig(cfg, true)
	if err != nil {
		return nil, err
	}
	if cfg.Realm == "" {
		cfg.Realm = "telesrv"
	}
	if cfg.RelayMinPort <= 0 || cfg.RelayMaxPort < cfg.RelayMinPort {
		return nil, fmt.Errorf("turnsrv: invalid relay port range [%d,%d]", cfg.RelayMinPort, cfg.RelayMaxPort)
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	relayIP := net.ParseIP(cfg.AdvertiseIP)
	if relayIP == nil {
		return nil, fmt.Errorf("turnsrv: invalid advertise ip %q", cfg.AdvertiseIP)
	}
	conn, err := net.ListenPacket("udp4", net.JoinHostPort(cfg.BindIP, fmt.Sprint(cfg.UDPPort)))
	if err != nil {
		return nil, fmt.Errorf("turnsrv: listen udp %d: %w", cfg.UDPPort, err)
	}
	pionLog := logging.NewDefaultLoggerFactory()
	server, err := turn.NewServer(turn.ServerConfig{
		Realm:         cfg.Realm,
		LoggerFactory: pionLog,
		AuthHandler:   turn.LongTermTURNRESTAuthHandler(cfg.SharedSecret, pionLog.NewLogger("turn-auth")),
		PacketConnConfigs: []turn.PacketConnConfig{{
			PacketConn: conn,
			RelayAddressGenerator: &turn.RelayAddressGeneratorPortRange{
				RelayAddress: relayIP,
				MinPort:      uint16(cfg.RelayMinPort),
				MaxPort:      uint16(cfg.RelayMaxPort),
				Address:      cfg.BindIP,
			},
		}},
	})
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("turnsrv: new server: %w", err)
	}
	cfg.Logger.Info("turn listening",
		zap.Int("udp_port", cfg.UDPPort),
		zap.String("bind_ip", cfg.BindIP),
		zap.String("advertise_ip", cfg.AdvertiseIP),
		zap.Int("relay_min_port", cfg.RelayMinPort),
		zap.Int("relay_max_port", cfg.RelayMaxPort))
	if cfg.AdvertiseIP == "127.0.0.1" {
		cfg.Logger.Warn("TELESRV_TURN_ADVERTISE_IP 为 127.0.0.1：真机拿到的 relay candidate 不可达（跨网媒体面静默失败），跨设备通话必须设为客户端可达 IP")
	}
	return &pionTURN{credentialIssuer: &credentialIssuer{cfg: cfg}, server: server}, nil
}

func normalizeCredentialConfig(cfg Config, allowRandomSecret bool) (Config, error) {
	if cfg.UDPPort <= 0 {
		return Config{}, fmt.Errorf("turnsrv: invalid udp port %d", cfg.UDPPort)
	}
	if cfg.AdvertiseIP == "" {
		cfg.AdvertiseIP = "127.0.0.1"
	}
	if net.ParseIP(cfg.AdvertiseIP) == nil {
		return Config{}, fmt.Errorf("turnsrv: invalid advertise ip %q", cfg.AdvertiseIP)
	}
	if cfg.BindIP == "" {
		cfg.BindIP = "0.0.0.0"
	}
	bindIP := net.ParseIP(cfg.BindIP)
	if bindIP == nil || bindIP.To4() == nil {
		return Config{}, fmt.Errorf("turnsrv: invalid IPv4 bind ip %q", cfg.BindIP)
	}
	if cfg.SharedSecret == "" {
		if !allowRandomSecret {
			return Config{}, fmt.Errorf("turnsrv: shared secret is required")
		}
		secret, err := randomSecret()
		if err != nil {
			return Config{}, err
		}
		cfg.SharedSecret = secret
	}
	if cfg.CredentialTTL <= 0 {
		cfg.CredentialTTL = 24 * time.Hour
	}
	return cfg, nil
}

func (t *credentialIssuer) Enabled() bool { return true }

func (t *credentialIssuer) Credentials(user string) (string, string, error) {
	username, password, err := turn.GenerateLongTermTURNRESTCredentials(t.cfg.SharedSecret, user, t.cfg.CredentialTTL)
	if err != nil {
		return "", "", fmt.Errorf("turnsrv: generate credentials: %w", err)
	}
	return username, password, nil
}

func (t *credentialIssuer) IP() string   { return t.cfg.AdvertiseIP }
func (t *credentialIssuer) Port() int    { return t.cfg.UDPPort }
func (t *credentialIssuer) Close() error { return nil }

func (t *pionTURN) Close() error { return t.server.Close() }

func randomSecret() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("turnsrv: random secret: %w", err)
	}
	return hex.EncodeToString(buf), nil
}
