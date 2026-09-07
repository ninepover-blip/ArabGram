package config

import (
	"time"

	"telesrv/internal/branding"
)

type FileConfigYAML struct {
	Version    int `yaml:"version"`
	CommonYAML `yaml:",inline"`
	FileData   GRPCServerYAML `yaml:"file_data"`
	Storage    StorageYAML    `yaml:"storage"`
	Upload     UploadYAML     `yaml:"upload"`
	Media      MediaYAML      `yaml:"media"`
}

type StorageYAML struct {
	BlobBackendKind      string `yaml:"blob_backend"`
	BlobDir              string `yaml:"blob_dir"`
	BlobStagingDir       string `yaml:"blob_staging_dir"`
	S3                   S3YAML `yaml:"s3"`
	LowSpaceGuardEnabled *bool  `yaml:"low_space_guard_enabled"`
	MinFreeBytes         *int64 `yaml:"min_free_bytes"`
	MaxTotalBytes        *int64 `yaml:"max_total_bytes"`
	UsageRefreshInterval string `yaml:"usage_refresh_interval"`
}

type S3YAML struct {
	Endpoint        string `yaml:"endpoint"`
	Region          string `yaml:"region"`
	Bucket          string `yaml:"bucket"`
	AccessKeyID     string `yaml:"access_key_id"`
	SecretAccessKey string `yaml:"secret_access_key"`
	UseSSL          *bool  `yaml:"use_ssl"`
	PathStyle       *bool  `yaml:"path_style"`
	CreateBucket    *bool  `yaml:"create_bucket"`
}

type UploadYAML struct {
	PartTTL          string `yaml:"part_ttl"`
	PartGCInterval   string `yaml:"part_gc_interval"`
	PartGCBatch      *int   `yaml:"part_gc_batch"`
	InFlightMaxBytes *int64 `yaml:"in_flight_max_bytes"`
	InFlightMaxParts *int   `yaml:"in_flight_max_parts"`
	InFlightMaxFiles *int   `yaml:"in_flight_max_files"`
}

// LoadFile reads the FileData role YAML config and returns the runtime snapshot used by the File process.
func LoadFile() (FileConfig, error) {
	cfg, err := loadRoleYAML(roleFile, func(b *envBuilder, y FileConfigYAML) error {
		if err := requireYAMLVersion(y.Version); err != nil {
			return err
		}
		if err := applyCommonYAML(b, y.CommonYAML); err != nil {
			return err
		}
		applyFileServerYAML(b, y.FileData)
		if err := applyStorageYAML(b, y.Storage); err != nil {
			return err
		}
		if err := applyUploadYAML(b, y.Upload); err != nil {
			return err
		}
		return applyMediaYAML(b, y.Media)
	})
	if err != nil {
		return FileConfig{}, err
	}
	return fileConfigFromConfig(cfg), nil
}

func applyStorageYAML(b *envBuilder, y StorageYAML) error {
	b.setString("TELESRV_BLOB_BACKEND", y.BlobBackendKind)
	b.setString("TELESRV_BLOB_DIR", y.BlobDir)
	b.setString("TELESRV_BLOB_STAGING_DIR", y.BlobStagingDir)
	b.setString("TELESRV_S3_ENDPOINT", y.S3.Endpoint)
	b.setString("TELESRV_S3_REGION", y.S3.Region)
	b.setString("TELESRV_S3_BUCKET", y.S3.Bucket)
	b.setString("TELESRV_S3_ACCESS_KEY_ID", y.S3.AccessKeyID)
	b.setString("TELESRV_S3_SECRET_ACCESS_KEY", y.S3.SecretAccessKey)
	b.setBool("TELESRV_S3_USE_SSL", y.S3.UseSSL)
	b.setBool("TELESRV_S3_PATH_STYLE", y.S3.PathStyle)
	b.setBool("TELESRV_S3_CREATE_BUCKET", y.S3.CreateBucket)
	b.setBool("TELESRV_STORAGE_LOW_SPACE_GUARD_ENABLE", y.LowSpaceGuardEnabled)
	b.setInt64("TELESRV_STORAGE_MIN_FREE_BYTES", y.MinFreeBytes)
	b.setInt64("TELESRV_STORAGE_MAX_TOTAL_BYTES", y.MaxTotalBytes)
	return b.setDuration("TELESRV_STORAGE_USAGE_REFRESH_INTERVAL", y.UsageRefreshInterval)
}

func applyUploadYAML(b *envBuilder, y UploadYAML) error {
	if err := b.setDuration("TELESRV_UPLOAD_PART_TTL", y.PartTTL); err != nil {
		return err
	}
	if err := b.setDuration("TELESRV_UPLOAD_PART_GC_INTERVAL", y.PartGCInterval); err != nil {
		return err
	}
	b.setInt("TELESRV_UPLOAD_PART_GC_BATCH", y.PartGCBatch)
	b.setInt64("TELESRV_UPLOAD_INFLIGHT_MAX_BYTES", y.InFlightMaxBytes)
	b.setInt("TELESRV_UPLOAD_INFLIGHT_MAX_PARTS", y.InFlightMaxParts)
	b.setInt("TELESRV_UPLOAD_INFLIGHT_MAX_FILES", y.InFlightMaxFiles)
	return nil
}

type FileConfig struct {
	DebugAddr string

	Branding           branding.Config
	PremiumBotUsername string
	PremiumBotUserID   int64

	DC                      int
	InstanceID              string
	PostgresDSN             string
	PostgresMaxConns        int
	PostgresMinConns        int
	FileGRPCAddr            string
	FileGRPCTLSCertFile     string
	FileGRPCTLSKeyFile      string
	FileGRPCTLSClientCAFile string
	FileToken               string

	BlobBackendKind             string
	BlobDir                     string
	BlobStagingDir              string
	S3Endpoint                  string
	S3Region                    string
	S3Bucket                    string
	S3AccessKeyID               string
	S3SecretAccessKey           string
	S3UseSSL                    bool
	S3PathStyle                 bool
	S3CreateBucket              bool
	StorageLowSpaceGuardEnable  bool
	StorageMinFreeBytes         int64
	StorageMaxTotalBytes        int64
	StorageUsageRefreshInterval time.Duration
	UploadPartTTL               time.Duration
	UploadPartGCInterval        time.Duration
	UploadPartGCBatch           int
	UploadInFlightMaxBytes      int64
	UploadInFlightMaxParts      int
	UploadInFlightMaxFiles      int
	MapboxToken                 string
	MapTileCacheDir             string
	ExternalMediaEnable         bool
	ExternalMediaMaxBytes       int64
	ExternalMediaRatePerMin     int
	WebPagePreviewEnable        bool
	WebPagePreviewMaxBytes      int64
	WebPagePreviewRatePerMin    int
}

func (c FileConfig) ProcessGlobals() ProcessGlobals {
	return ProcessGlobals{Branding: c.Branding, PremiumBotUsername: c.PremiumBotUsername, PremiumBotUserID: c.PremiumBotUserID}
}

func (c FileConfig) DebugAddress() string {
	return c.DebugAddr
}

func fileConfigFromConfig(c Config) FileConfig {
	return FileConfig{
		DebugAddr: c.DebugAddr, Branding: c.Branding, PremiumBotUsername: c.PremiumBotUsername, PremiumBotUserID: c.PremiumBotUserID,
		DC: c.DC, InstanceID: c.InstanceID, PostgresDSN: c.PostgresDSN, PostgresMaxConns: c.PostgresMaxConns, PostgresMinConns: c.PostgresMinConns,
		FileGRPCAddr: c.FileGRPCAddr, FileGRPCTLSCertFile: c.FileGRPCTLSCertFile, FileGRPCTLSKeyFile: c.FileGRPCTLSKeyFile,
		FileGRPCTLSClientCAFile: c.FileGRPCTLSClientCAFile, FileToken: c.FileToken, BlobBackendKind: c.BlobBackendKind,
		BlobDir: c.BlobDir, BlobStagingDir: c.BlobStagingDir, S3Endpoint: c.S3Endpoint, S3Region: c.S3Region, S3Bucket: c.S3Bucket,
		S3AccessKeyID: c.S3AccessKeyID, S3SecretAccessKey: c.S3SecretAccessKey, S3UseSSL: c.S3UseSSL, S3PathStyle: c.S3PathStyle,
		S3CreateBucket: c.S3CreateBucket, StorageLowSpaceGuardEnable: c.StorageLowSpaceGuardEnable, StorageMinFreeBytes: c.StorageMinFreeBytes,
		StorageMaxTotalBytes: c.StorageMaxTotalBytes, StorageUsageRefreshInterval: c.StorageUsageRefreshInterval, UploadPartTTL: c.UploadPartTTL,
		UploadPartGCInterval: c.UploadPartGCInterval,
		UploadPartGCBatch:    c.UploadPartGCBatch, UploadInFlightMaxBytes: c.UploadInFlightMaxBytes, UploadInFlightMaxParts: c.UploadInFlightMaxParts,
		UploadInFlightMaxFiles: c.UploadInFlightMaxFiles, MapboxToken: c.MapboxToken, MapTileCacheDir: c.MapTileCacheDir,
		ExternalMediaEnable: c.ExternalMediaEnable, ExternalMediaMaxBytes: c.ExternalMediaMaxBytes, ExternalMediaRatePerMin: c.ExternalMediaRatePerMin,
		WebPagePreviewEnable: c.WebPagePreviewEnable, WebPagePreviewMaxBytes: c.WebPagePreviewMaxBytes, WebPagePreviewRatePerMin: c.WebPagePreviewRatePerMin,
	}
}
