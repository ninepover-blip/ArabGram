package config

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	TONWalletVersionV4R2 = "v4r2"
	TONNetworkMainnet    = "mainnet"
	TONNetworkTestnet    = "testnet"
)

var tonAmountPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]{1,9})?$`)

type TONConfigYAML struct {
	Version    int `yaml:"version"`
	CommonYAML `yaml:",inline"`
	TON        TONYAML `yaml:"ton"`
}

type TONYAML struct {
	Network      string              `yaml:"network"`
	LiteServers  []TONLiteServerYAML `yaml:"lite_servers"`
	TrustedBlock TONTrustedBlockYAML `yaml:"trusted_block"`
	Collection   TONCollectionYAML   `yaml:"collection"`
	Relayer      TONRelayerYAML      `yaml:"relayer"`
	Mint         TONMintYAML         `yaml:"mint"`
	Worker       TONWorkerYAML       `yaml:"worker"`
}

type TONTrustedBlockYAML struct {
	Workchain *int32  `yaml:"workchain"`
	Shard     *int64  `yaml:"shard"`
	Seqno     *uint32 `yaml:"seqno"`
	RootHash  string  `yaml:"root_hash"`
	FileHash  string  `yaml:"file_hash"`
}

type TONLiteServerYAML struct {
	Address   string `yaml:"address"`
	PublicKey string `yaml:"public_key"`
}

type TONCollectionYAML struct {
	Address          string `yaml:"address"`
	CodeHash         string `yaml:"code_hash"`
	MintABI          string `yaml:"mint_abi"`
	InitialItemIndex string `yaml:"initial_item_index"`
}

type TONRelayerYAML struct {
	MnemonicFile  string `yaml:"mnemonic_file"`
	WalletVersion string `yaml:"wallet_version"`
	MinBalance    string `yaml:"min_balance"`
	MessageTTL    string `yaml:"message_ttl"`
}

type TONMintYAML struct {
	Enabled       *bool  `yaml:"enabled"`
	SendAmount    string `yaml:"send_amount"`
	ForwardAmount string `yaml:"forward_amount"`
	FinalityDepth *int   `yaml:"finality_depth"`
}

type TONWorkerYAML struct {
	Batch          *int   `yaml:"batch"`
	PollInterval   string `yaml:"poll_interval"`
	LeaseTimeout   string `yaml:"lease_timeout"`
	RequestTimeout string `yaml:"request_timeout"`
}

type TONConfig struct {
	DebugAddr        string
	InstanceID       string
	PostgresDSN      string
	PostgresMaxConns int
	PostgresMinConns int

	Network              string
	LiteServers          []TONLiteServerConfig
	TrustedBlock         TONTrustedBlockConfig
	CollectionAddress    string
	CollectionCodeHash   string
	MintABI              string
	InitialItemIndex     string
	RelayerMnemonicFile  string
	RelayerWalletVersion string
	RelayerMinBalance    string
	RelayerMessageTTL    time.Duration
	MintEnabled          bool
	MintSendAmount       string
	MintForwardAmount    string
	FinalityDepth        int
	WorkerBatch          int
	PollInterval         time.Duration
	LeaseTimeout         time.Duration
	RequestTimeout       time.Duration
}

type TONLiteServerConfig struct {
	Address   string
	PublicKey string
}

type TONTrustedBlockConfig struct {
	Workchain int32
	Shard     int64
	Seqno     uint32
	RootHash  string
	FileHash  string
}

// LoadTON reads the dedicated TON role snapshot. Process environment variables
// are used only by readStrictYAML for explicit ${NAME} substitutions.
func LoadTON() (TONConfig, error) {
	path, err := roleConfigPath(roleTON)
	if err != nil {
		return TONConfig{}, err
	}
	var y TONConfigYAML
	if err := readStrictYAML(path, &y); err != nil {
		return TONConfig{}, fmt.Errorf("config file %q: %w", path, err)
	}
	cfg, err := tonConfigFromYAML(y)
	if err != nil {
		return TONConfig{}, fmt.Errorf("config file %q: %w", path, err)
	}
	return cfg, nil
}

func (c TONConfig) DebugAddress() string { return c.DebugAddr }

func tonConfigFromYAML(y TONConfigYAML) (TONConfig, error) {
	if err := requireYAMLVersion(y.Version); err != nil {
		return TONConfig{}, err
	}
	cfg := TONConfig{
		InstanceID:           strings.TrimSpace(y.Instance.ID),
		PostgresDSN:          strings.TrimSpace(y.Postgres.DSN),
		Network:              strings.ToLower(strings.TrimSpace(y.TON.Network)),
		CollectionAddress:    strings.TrimSpace(y.TON.Collection.Address),
		CollectionCodeHash:   strings.TrimSpace(y.TON.Collection.CodeHash),
		MintABI:              strings.TrimSpace(y.TON.Collection.MintABI),
		InitialItemIndex:     strings.TrimSpace(y.TON.Collection.InitialItemIndex),
		RelayerMnemonicFile:  strings.TrimSpace(y.TON.Relayer.MnemonicFile),
		RelayerWalletVersion: strings.ToLower(strings.TrimSpace(y.TON.Relayer.WalletVersion)),
		RelayerMinBalance:    strings.TrimSpace(y.TON.Relayer.MinBalance),
		MintSendAmount:       strings.TrimSpace(y.TON.Mint.SendAmount),
		MintForwardAmount:    strings.TrimSpace(y.TON.Mint.ForwardAmount),
		TrustedBlock: TONTrustedBlockConfig{
			RootHash: strings.TrimSpace(y.TON.TrustedBlock.RootHash), FileHash: strings.TrimSpace(y.TON.TrustedBlock.FileHash),
		},
	}
	if y.TON.TrustedBlock.Workchain != nil {
		cfg.TrustedBlock.Workchain = *y.TON.TrustedBlock.Workchain
	}
	if y.TON.TrustedBlock.Shard != nil {
		cfg.TrustedBlock.Shard = *y.TON.TrustedBlock.Shard
	}
	if y.TON.TrustedBlock.Seqno != nil {
		cfg.TrustedBlock.Seqno = *y.TON.TrustedBlock.Seqno
	}
	if y.Debug.Addr != nil {
		cfg.DebugAddr = strings.TrimSpace(*y.Debug.Addr)
	}
	if y.Postgres.MaxConns != nil {
		cfg.PostgresMaxConns = *y.Postgres.MaxConns
	}
	if y.Postgres.MinConns != nil {
		cfg.PostgresMinConns = *y.Postgres.MinConns
	}
	if y.TON.Mint.Enabled != nil {
		cfg.MintEnabled = *y.TON.Mint.Enabled
	}
	if y.TON.Mint.FinalityDepth == nil {
		cfg.FinalityDepth = 12
	} else {
		cfg.FinalityDepth = *y.TON.Mint.FinalityDepth
	}
	if y.TON.Worker.Batch == nil {
		cfg.WorkerBatch = 10
	} else {
		cfg.WorkerBatch = *y.TON.Worker.Batch
	}
	var err error
	if cfg.PollInterval, err = parseTONDuration("ton.worker.poll_interval", y.TON.Worker.PollInterval, 2*time.Second); err != nil {
		return TONConfig{}, err
	}
	if cfg.LeaseTimeout, err = parseTONDuration("ton.worker.lease_timeout", y.TON.Worker.LeaseTimeout, 2*time.Minute); err != nil {
		return TONConfig{}, err
	}
	if cfg.RequestTimeout, err = parseTONDuration("ton.worker.request_timeout", y.TON.Worker.RequestTimeout, 90*time.Second); err != nil {
		return TONConfig{}, err
	}
	if cfg.RelayerMessageTTL, err = parseTONDuration("ton.relayer.message_ttl", y.TON.Relayer.MessageTTL, 15*time.Minute); err != nil {
		return TONConfig{}, err
	}
	for _, server := range y.TON.LiteServers {
		cfg.LiteServers = append(cfg.LiteServers, TONLiteServerConfig{
			Address: strings.TrimSpace(server.Address), PublicKey: strings.TrimSpace(server.PublicKey),
		})
	}
	if cfg.RelayerWalletVersion == "" {
		cfg.RelayerWalletVersion = TONWalletVersionV4R2
	}
	if cfg.RelayerMinBalance == "" {
		cfg.RelayerMinBalance = "1"
	}
	if cfg.MintSendAmount == "" {
		cfg.MintSendAmount = "0.05"
	}
	if cfg.MintForwardAmount == "" {
		cfg.MintForwardAmount = "0.01"
	}
	if initialItemIndex, err := strconv.ParseUint(cfg.InitialItemIndex, 10, 64); err == nil {
		cfg.InitialItemIndex = strconv.FormatUint(initialItemIndex, 10)
	}
	if err := cfg.Validate(); err != nil {
		return TONConfig{}, err
	}
	return cfg, nil
}

func (c TONConfig) Validate() error {
	if c.InstanceID == "" || len(c.InstanceID) > 128 {
		return fmt.Errorf("instance.id is required and must be at most 128 bytes")
	}
	if c.PostgresDSN == "" {
		return fmt.Errorf("postgres.dsn is required")
	}
	if c.PostgresMaxConns < 0 || c.PostgresMinConns < 0 || (c.PostgresMaxConns > 0 && c.PostgresMinConns > c.PostgresMaxConns) {
		return fmt.Errorf("postgres connection limits are invalid")
	}
	if c.Network != TONNetworkMainnet && c.Network != TONNetworkTestnet {
		return fmt.Errorf("ton.network must be mainnet or testnet")
	}
	if len(c.LiteServers) == 0 {
		return fmt.Errorf("ton.lite_servers must contain at least one fixed endpoint")
	}
	if c.TrustedBlock.Workchain != -1 || c.TrustedBlock.Shard == 0 || c.TrustedBlock.Seqno == 0 ||
		c.TrustedBlock.RootHash == "" || c.TrustedBlock.FileHash == "" {
		return fmt.Errorf("ton.trusted_block must contain a masterchain checkpoint and both hashes")
	}
	seen := make(map[string]struct{}, len(c.LiteServers))
	for i, server := range c.LiteServers {
		if server.Address == "" || server.PublicKey == "" {
			return fmt.Errorf("ton.lite_servers[%d] address and public_key are required", i)
		}
		key := server.Address + "\x00" + server.PublicKey
		if _, ok := seen[key]; ok {
			return fmt.Errorf("ton.lite_servers[%d] duplicates an earlier endpoint", i)
		}
		seen[key] = struct{}{}
	}
	if c.CollectionAddress == "" || c.CollectionCodeHash == "" || c.MintABI != "basic-collection-v1" || !validDecimalUint64(c.InitialItemIndex) {
		return fmt.Errorf("ton.collection.address, code_hash, basic-collection-v1 mint_abi and uint64 initial_item_index are required")
	}
	if c.RelayerWalletVersion != TONWalletVersionV4R2 {
		return fmt.Errorf("ton.relayer.wallet_version must be v4r2")
	}
	if c.MintEnabled && c.RelayerMnemonicFile == "" {
		return fmt.Errorf("ton.relayer.mnemonic_file is required when mint.enabled is true")
	}
	if c.RelayerMessageTTL < 2*c.RequestTimeout || c.RelayerMessageTTL > time.Hour {
		return fmt.Errorf("ton.relayer.message_ttl must be at least twice request_timeout and at most 1h")
	}
	for name, amount := range map[string]string{
		"ton.relayer.min_balance": c.RelayerMinBalance,
		"ton.mint.send_amount":    c.MintSendAmount,
		"ton.mint.forward_amount": c.MintForwardAmount,
	} {
		if !tonAmountPattern.MatchString(amount) {
			return fmt.Errorf("%s must be a non-negative TON decimal with at most 9 fractional digits", name)
		}
	}
	if c.RelayerMinBalance == "0" || c.MintSendAmount == "0" {
		return fmt.Errorf("ton.relayer.min_balance and ton.mint.send_amount must be positive")
	}
	if c.FinalityDepth < 1 || c.FinalityDepth > 1000 {
		return fmt.Errorf("ton.mint.finality_depth must be between 1 and 1000")
	}
	if c.WorkerBatch < 1 || c.WorkerBatch > 100 {
		return fmt.Errorf("ton.worker.batch must be between 1 and 100")
	}
	if c.PollInterval < 100*time.Millisecond || c.PollInterval > time.Minute {
		return fmt.Errorf("ton.worker.poll_interval must be between 100ms and 1m")
	}
	if c.LeaseTimeout < 10*time.Second || c.LeaseTimeout > time.Hour {
		return fmt.Errorf("ton.worker.lease_timeout must be between 10s and 1h")
	}
	if c.RequestTimeout < time.Second || c.RequestTimeout >= c.LeaseTimeout {
		return fmt.Errorf("ton.worker.request_timeout must be at least 1s and shorter than lease_timeout")
	}
	return nil
}

func parseTONDuration(name, value string, fallback time.Duration) (time.Duration, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback, nil
	}
	duration, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", name, err)
	}
	return duration, nil
}
