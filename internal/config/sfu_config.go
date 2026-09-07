package config

import (
	"time"

	"telesrv/internal/branding"
)

type SFUConfigYAML struct {
	Version    int `yaml:"version"`
	CommonYAML `yaml:",inline"`
	SFU        SFUYAML       `yaml:"sfu"`
	TURN       SFUTURNYAML   `yaml:"turn"`
	GroupCall  GroupCallYAML `yaml:"group_call"`
}

type SFUYAML struct {
	UDPPort                   *int           `yaml:"udp_port"`
	BindIP                    string         `yaml:"bind_ip"`
	AdvertiseIP               string         `yaml:"advertise_ip"`
	InstanceTTL               string         `yaml:"instance_ttl"`
	InstanceHeartbeatInterval string         `yaml:"instance_heartbeat_interval"`
	MaxActiveCalls            *int           `yaml:"max_active_calls"`
	Control                   GRPCServerYAML `yaml:"control"`
}

// SFUTURNYAML contains the media-plane settings owned by telesrv-sfu. Core has
// a separate signaling-only shape and cannot configure relay allocation ports.
type SFUTURNYAML struct {
	Enabled      *bool  `yaml:"enabled"`
	UDPPort      *int   `yaml:"udp_port"`
	BindIP       string `yaml:"bind_ip"`
	AdvertiseIP  string `yaml:"advertise_ip"`
	Secret       string `yaml:"secret"`
	RelayMinPort *int   `yaml:"relay_min_port"`
	RelayMaxPort *int   `yaml:"relay_max_port"`
}

// LoadSFU reads the SFU role YAML config and returns the runtime snapshot used by the SFU process.
func LoadSFU() (SFUConfig, error) {
	cfg, err := loadRoleYAML(roleSFU, func(b *envBuilder, y SFUConfigYAML) error {
		if err := requireYAMLVersion(y.Version); err != nil {
			return err
		}
		if err := applyCommonYAML(b, y.CommonYAML); err != nil {
			return err
		}
		b.setInt("TELESRV_SFU_UDP_PORT", y.SFU.UDPPort)
		b.setString("TELESRV_SFU_BIND_IP", y.SFU.BindIP)
		b.setString("TELESRV_SFU_ADVERTISE_IP", y.SFU.AdvertiseIP)
		if err := b.setDuration("TELESRV_SFU_INSTANCE_TTL", y.SFU.InstanceTTL); err != nil {
			return err
		}
		if err := b.setDuration("TELESRV_SFU_INSTANCE_HEARTBEAT_INTERVAL", y.SFU.InstanceHeartbeatInterval); err != nil {
			return err
		}
		b.setInt("TELESRV_SFU_INSTANCE_MAX_ACTIVE_CALLS", y.SFU.MaxActiveCalls)
		applySFUControlServerYAML(b, y.SFU.Control)
		b.setBool("TELESRV_TURN_ENABLE", y.TURN.Enabled)
		b.setInt("TELESRV_TURN_UDP_PORT", y.TURN.UDPPort)
		b.setString("TELESRV_TURN_BIND_IP", y.TURN.BindIP)
		b.setString("TELESRV_TURN_ADVERTISE_IP", y.TURN.AdvertiseIP)
		b.setString("TELESRV_TURN_SECRET", y.TURN.Secret)
		b.setInt("TELESRV_TURN_RELAY_MIN_PORT", y.TURN.RelayMinPort)
		b.setInt("TELESRV_TURN_RELAY_MAX_PORT", y.TURN.RelayMaxPort)
		b.setString("TELESRV_GROUPCALL_CONTROL_URL", y.GroupCall.ControlURL)
		b.setString("TELESRV_GROUPCALL_CONTROL_TOKEN", y.GroupCall.ControlToken)
		return nil
	})
	if err != nil {
		return SFUConfig{}, err
	}
	return sfuConfigFromConfig(cfg), nil
}

type SFUConfig struct {
	DebugAddr string

	Branding           branding.Config
	PremiumBotUsername string
	PremiumBotUserID   int64

	AdvertiseIP                   string
	RedisAddr                     string
	RedisPassword                 string
	RedisDB                       int
	InstanceID                    string
	GroupCallControlURL           string
	GroupCallControlToken         string
	SFUUDPPort                    int
	SFUBindIP                     string
	SFUAdvertiseIP                string
	SFUInstanceTTL                time.Duration
	SFUInstanceHeartbeatInterval  time.Duration
	SFUInstanceMaxActiveCalls     int
	SFUControlGRPCAddr            string
	SFUControlGRPCURL             string
	SFUControlGRPCTLSCertFile     string
	SFUControlGRPCTLSKeyFile      string
	SFUControlGRPCTLSClientCAFile string
	SFUControlToken               string
	TURNEnable                    bool
	TURNUDPPort                   int
	TURNBindIP                    string
	TURNAdvertiseIP               string
	TURNSecret                    string
	TURNRelayMinPort              int
	TURNRelayMaxPort              int
}

func (c SFUConfig) ProcessGlobals() ProcessGlobals {
	return ProcessGlobals{Branding: c.Branding, PremiumBotUsername: c.PremiumBotUsername, PremiumBotUserID: c.PremiumBotUserID}
}

func (c SFUConfig) DebugAddress() string {
	return c.DebugAddr
}

func sfuConfigFromConfig(c Config) SFUConfig {
	return SFUConfig{
		DebugAddr: c.DebugAddr, Branding: c.Branding, PremiumBotUsername: c.PremiumBotUsername, PremiumBotUserID: c.PremiumBotUserID,
		AdvertiseIP: c.AdvertiseIP, RedisAddr: c.RedisAddr, RedisPassword: c.RedisPassword, RedisDB: c.RedisDB, InstanceID: c.InstanceID,
		GroupCallControlURL: c.GroupCallControlURL, GroupCallControlToken: c.GroupCallControlToken,
		SFUUDPPort: c.SFUUDPPort, SFUBindIP: c.SFUBindIP, SFUAdvertiseIP: c.SFUAdvertiseIP, SFUInstanceTTL: c.SFUInstanceTTL,
		SFUInstanceHeartbeatInterval: c.SFUInstanceHeartbeatInterval, SFUInstanceMaxActiveCalls: c.SFUInstanceMaxActiveCalls,
		SFUControlGRPCAddr: c.SFUControlGRPCAddr, SFUControlGRPCURL: c.SFUControlGRPCURL,
		SFUControlGRPCTLSCertFile: c.SFUControlGRPCTLSCertFile, SFUControlGRPCTLSKeyFile: c.SFUControlGRPCTLSKeyFile,
		SFUControlGRPCTLSClientCAFile: c.SFUControlGRPCTLSClientCAFile, SFUControlToken: c.SFUControlToken,
		TURNEnable: c.TURNEnable, TURNUDPPort: c.TURNUDPPort, TURNBindIP: c.TURNBindIP, TURNAdvertiseIP: c.TURNAdvertiseIP,
		TURNSecret: c.TURNSecret, TURNRelayMinPort: c.TURNRelayMinPort, TURNRelayMaxPort: c.TURNRelayMaxPort,
	}
}
