package config

import (
	"time"

	"telesrv/internal/branding"
	"telesrv/internal/domain"
)

type EgressConfigYAML struct {
	Version    int `yaml:"version"`
	CommonYAML `yaml:",inline"`
	Egress     EgressYAML `yaml:"egress"`
}

type EgressYAML struct {
	DeliveryServer             GRPCServerYAML `yaml:"delivery_server"`
	Workers                    *int           `yaml:"workers"`
	Batch                      *int           `yaml:"batch"`
	LeaseTimeout               string         `yaml:"lease_timeout"`
	OutboundPushTimeout        string         `yaml:"outbound_push_timeout"`
	DeliveryAttemptTimeout     string         `yaml:"delivery_attempt_timeout"`
	DeliveryClockSkewAllowance string         `yaml:"delivery_clock_skew_allowance"`
}

// LoadEgress reads the Egress role YAML config and returns the runtime snapshot used by the Egress process.
func LoadEgress() (EgressConfig, error) {
	cfg, err := loadRoleYAML(roleEgress, func(b *envBuilder, y EgressConfigYAML) error {
		if err := requireYAMLVersion(y.Version); err != nil {
			return err
		}
		if err := applyCommonYAML(b, y.CommonYAML); err != nil {
			return err
		}
		applyEgressDeliveryServerYAML(b, y.Egress.DeliveryServer)
		b.setInt("TELESRV_OUTBOX_WORKERS", y.Egress.Workers)
		b.setInt("TELESRV_OUTBOX_BATCH", y.Egress.Batch)
		if err := b.setDuration("TELESRV_OUTBOX_LEASE_TIMEOUT", y.Egress.LeaseTimeout); err != nil {
			return err
		}
		if err := b.setDuration("TELESRV_OUTBOUND_PUSH_TIMEOUT", y.Egress.OutboundPushTimeout); err != nil {
			return err
		}
		if err := b.setDuration("TELESRV_DELIVERY_ATTEMPT_TIMEOUT", y.Egress.DeliveryAttemptTimeout); err != nil {
			return err
		}
		return b.setDuration("TELESRV_DELIVERY_CLOCK_SKEW_ALLOWANCE", y.Egress.DeliveryClockSkewAllowance)
	})
	if err != nil {
		return EgressConfig{}, err
	}
	return egressConfigFromConfig(cfg), nil
}

type EgressConfig struct {
	DebugAddr string

	Branding           branding.Config
	PremiumBotUsername string
	PremiumBotUserID   int64

	DC                 int
	DefaultCountryCode string
	AdvertiseIP        string
	AdvertisePort      int
	InstanceID         string
	PostgresDSN        string
	PostgresMaxConns   int
	PostgresMinConns   int
	RedisAddr          string
	RedisPassword      string
	RedisDB            int

	EgressDeliveryGRPCAddr            string
	EgressDeliveryGRPCTLSCertFile     string
	EgressDeliveryGRPCTLSKeyFile      string
	EgressDeliveryGRPCTLSClientCAFile string
	EgressDeliveryToken               string
	OutboxWorkers                     int
	OutboxBatch                       int
	OutboxLeaseTimeout                time.Duration
	OutboundPushTimeout               time.Duration
	DeliveryAttemptTimeout            time.Duration
	DeliveryClockSkewAllowance        time.Duration

	ChannelRowCacheMaxEntries         int
	ChannelMemberCacheMaxEntries      int
	ChannelDialogCacheMaxEntries      int
	ChannelBoostCacheMaxEntries       int
	ChannelBoostCacheTTL              time.Duration
	OfficialGiftsDir                  string
	PublicBaseURL                     string
	PublicAppScheme                   string
	PublicAppLinkBase                 string
	UpdatePublicURL                   string
	CollectibleUsernameURLTemplate    string
	RatingEnabled                     bool
	RatingPendingDelay                time.Duration
	RatingStaleAfter                  time.Duration
	RatingWeightStarsReceivedPermille int64
	RatingWeightStarsSpentPermille    int64
	RatingWeightMessageSent           int64
	RatingWeightAccountAgeDay         int64
	RatingWeightGiftReceived          int64
	RatingWeightModerationCase        int64
	RatingWeightScamPenalty           int64
	RatingWeightFakePenalty           int64
	RatingActivityCap                 int64
	TempKeyResolveCacheMaxEntries     int
	TempKeyResolveCacheTTL            time.Duration
	AuthUserCacheTTL                  time.Duration
}

func (c EgressConfig) ProcessGlobals() ProcessGlobals {
	return ProcessGlobals{Branding: c.Branding, PremiumBotUsername: c.PremiumBotUsername, PremiumBotUserID: c.PremiumBotUserID}
}

func (c EgressConfig) DebugAddress() string {
	return c.DebugAddr
}

func (c EgressConfig) AccountRatingWeights() domain.AccountRatingWeights {
	return domain.AccountRatingWeights{
		StarsReceivedPermille: c.RatingWeightStarsReceivedPermille,
		StarsSpentPermille:    c.RatingWeightStarsSpentPermille,
		PerMessageSent:        c.RatingWeightMessageSent,
		PerAccountAgeDay:      c.RatingWeightAccountAgeDay,
		PerGiftReceived:       c.RatingWeightGiftReceived,
		PerModerationCase:     c.RatingWeightModerationCase,
		ScamPenalty:           c.RatingWeightScamPenalty,
		FakePenalty:           c.RatingWeightFakePenalty,
		ActivityCap:           c.RatingActivityCap,
	}
}

func egressConfigFromConfig(c Config) EgressConfig {
	return EgressConfig{
		DebugAddr: c.DebugAddr, Branding: c.Branding, PremiumBotUsername: c.PremiumBotUsername, PremiumBotUserID: c.PremiumBotUserID,
		DC: c.DC, DefaultCountryCode: c.DefaultCountryCode, AdvertiseIP: c.AdvertiseIP, AdvertisePort: c.AdvertisePort, InstanceID: c.InstanceID,
		PostgresDSN: c.PostgresDSN, PostgresMaxConns: c.PostgresMaxConns, PostgresMinConns: c.PostgresMinConns,
		RedisAddr: c.RedisAddr, RedisPassword: c.RedisPassword, RedisDB: c.RedisDB,
		EgressDeliveryGRPCAddr: c.EgressDeliveryGRPCAddr, EgressDeliveryGRPCTLSCertFile: c.EgressDeliveryGRPCTLSCertFile,
		EgressDeliveryGRPCTLSKeyFile: c.EgressDeliveryGRPCTLSKeyFile, EgressDeliveryGRPCTLSClientCAFile: c.EgressDeliveryGRPCTLSClientCAFile,
		EgressDeliveryToken: c.EgressDeliveryToken, OutboxWorkers: c.OutboxWorkers, OutboxBatch: c.OutboxBatch,
		OutboxLeaseTimeout: c.OutboxLeaseTimeout, OutboundPushTimeout: c.OutboundPushTimeout,
		DeliveryAttemptTimeout: c.DeliveryAttemptTimeout, DeliveryClockSkewAllowance: c.DeliveryClockSkewAllowance,
		ChannelRowCacheMaxEntries: c.ChannelRowCacheMaxEntries, ChannelMemberCacheMaxEntries: c.ChannelMemberCacheMaxEntries,
		ChannelDialogCacheMaxEntries: c.ChannelDialogCacheMaxEntries, ChannelBoostCacheMaxEntries: c.ChannelBoostCacheMaxEntries,
		ChannelBoostCacheTTL: c.ChannelBoostCacheTTL, OfficialGiftsDir: c.OfficialGiftsDir, PublicBaseURL: c.PublicBaseURL,
		PublicAppScheme: c.PublicAppScheme, PublicAppLinkBase: c.PublicAppLinkBase, UpdatePublicURL: c.UpdatePublicURL,
		CollectibleUsernameURLTemplate: c.CollectibleUsernameURLTemplate, RatingEnabled: c.RatingEnabled,
		RatingPendingDelay: c.RatingPendingDelay, RatingStaleAfter: c.RatingStaleAfter,
		RatingWeightStarsReceivedPermille: c.RatingWeightStarsReceivedPermille, RatingWeightStarsSpentPermille: c.RatingWeightStarsSpentPermille,
		RatingWeightMessageSent: c.RatingWeightMessageSent, RatingWeightAccountAgeDay: c.RatingWeightAccountAgeDay,
		RatingWeightGiftReceived: c.RatingWeightGiftReceived, RatingWeightModerationCase: c.RatingWeightModerationCase,
		RatingWeightScamPenalty: c.RatingWeightScamPenalty, RatingWeightFakePenalty: c.RatingWeightFakePenalty, RatingActivityCap: c.RatingActivityCap,
		TempKeyResolveCacheMaxEntries: c.TempKeyResolveCacheMaxEntries, TempKeyResolveCacheTTL: c.TempKeyResolveCacheTTL,
		AuthUserCacheTTL: c.AuthUserCacheTTL,
	}
}
