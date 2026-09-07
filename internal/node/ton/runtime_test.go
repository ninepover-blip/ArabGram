package ton

import (
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"telesrv/internal/config"
	"telesrv/internal/domain"
	"telesrv/internal/node/common"
)

func TestTONChainConfigPreservesAdmissionAndSigningSettings(t *testing.T) {
	cfg := config.TONConfig{
		Network:           config.TONNetworkMainnet,
		LiteServers:       []config.TONLiteServerConfig{{Address: "1.2.3.4:19949", PublicKey: "key"}},
		TrustedBlock:      config.TONTrustedBlockConfig{Workchain: -1, Shard: -9223372036854775808, Seqno: 12, RootHash: "root", FileHash: "file"},
		CollectionAddress: "collection", CollectionCodeHash: "code", MintABI: domain.StarGiftTONMintABIBasicCollectionV1,
		RelayerMnemonicFile: "seed.txt",
		RelayerMinBalance:   "2", RelayerMessageTTL: 15 * time.Minute,
		MintSendAmount: "0.05", MintForwardAmount: "0.01", FinalityDepth: 12,
	}
	got := tonChainConfig(cfg)
	if got.Network != domain.TONNetworkMainnet || len(got.LiteServers) != 1 || got.TrustedBlock.Seqno != 12 ||
		got.CollectionCodeHash != "code" || got.MintABI != domain.StarGiftTONMintABIBasicCollectionV1 ||
		got.RelayerMessageTTL != 15*time.Minute || got.FinalityDepth != 12 {
		t.Fatalf("tonChainConfig = %+v", got)
	}
}

func TestResumeExportRequiresExplicitAuditMetadata(t *testing.T) {
	err := ResumeExport(zap.NewNop(), common.BuildMetadata{}, ResumeExportOptions{ExportID: 1})
	if err == nil || !strings.Contains(err.Error(), "--actor") {
		t.Fatalf("ResumeExport missing audit metadata err=%v", err)
	}
}
