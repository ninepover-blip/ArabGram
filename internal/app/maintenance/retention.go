package maintenance

import (
	"context"
	"time"

	"go.uber.org/zap"
)

// TempAuthKeyRetentionStore 回收过期的 PFS temp auth key（含未绑定 key）。
type TempAuthKeyRetentionStore interface {
	DeleteExpired(ctx context.Context, expiredBefore int64, limit int) (int, error)
}

// AuthKeySessionLayerRetentionStore reclaims expired short-lived Layer
// watermarks. Selector freshness, not retention timing, is the correctness
// gate; this worker only bounds durable storage.
type AuthKeySessionLayerRetentionStore interface {
	DeleteExpiredSessionLayers(ctx context.Context, limit int) (int, error)
}

// OrphanAuthKeyRetentionStore 回收从未形成授权/temp binding 的旧握手 key。
// protected 是当前连接注册表实际使用的 raw auth_key_id 快照。
type OrphanAuthKeyRetentionStore interface {
	DeleteOrphaned(ctx context.Context, olderThan time.Duration, limit int, protected [][8]byte) (int, error)
}

// ActiveRawAuthKeySnapshotProvider exposes the distributed active raw-key
// snapshot. It can fail; orphan auth-key GC must stop for that pass rather than
// delete against an incomplete view.
type ActiveRawAuthKeySnapshotProvider interface {
	ActiveRawAuthKeySnapshot(ctx context.Context) ([][8]byte, error)
}

// ActiveAuthKeyHeartbeatStore 把仍在使用的 raw auth key 活性持久化。多实例下
// orphan GC 不能只看某个进程的 active 快照；分布式 snapshot 会先保护候选，heartbeat
// 再推进数据库 last_used_at，给后续回收 pass 留下持久证据。
type ActiveAuthKeyHeartbeatStore interface {
	TouchActiveRawAuthKeys(ctx context.Context, ids [][8]byte) error
}

// BotAPIUpdateRetentionStore 回收 Bot API getUpdates 投递队列的死行（性能审计 H1）：
// 已确认且超过宽限期的行 + 按消息 date 超过保留期的行（官方 Bot API updates 最多保留 24h）。
type BotAPIUpdateRetentionStore interface {
	DeleteDeliveredOrExpired(ctx context.Context, confirmedGrace, maxAge time.Duration, limit int) (int, error)
}

// UserUpdateEventRetentionStore 只回收所有当前授权设备都明确确认过的账号事件前缀。
// 它不是普通 TTL：任一授权缺 state 时确认水位为 0，不得删除该设备可能仍需的事件。
type UserUpdateEventRetentionStore interface {
	DeleteConfirmedPrefix(ctx context.Context, olderThan time.Duration, limit int) (int, error)
}

// ChannelUpdateEventRetentionStore 回收超过保留期的 channel durable update 连续前缀。
// 具体 store 必须在同一事务内删除事件并推进 retained floor，低于 floor 的客户端由
// updates.getChannelDifference 走 channelDifferenceTooLong 快照恢复。
type ChannelUpdateEventRetentionStore interface {
	DeleteExpiredChannelUpdateEvents(ctx context.Context, olderThan time.Duration, limit int) (int, error)
}

// LoginCodeDeliveryRetentionStore reclaims only compact idempotency receipts
// after their associated opaque code lifetime. It must not delete the message,
// durable update event, or outbox facts created by the delivery transaction.
type LoginCodeDeliveryRetentionStore interface {
	DeleteExpiredLoginCodeDeliveries(ctx context.Context, expiredBefore time.Time, limit int) (int, error)
}

type ClientTelemetryRetentionStore interface {
	DeleteExpiredClientTelemetry(ctx context.Context, olderThan time.Time, limit int) (int, error)
}

type AuthDeliveryReportRetentionStore interface {
	DeleteExpiredAuthDeliveryReports(ctx context.Context, olderThan time.Time, limit int) (int, error)
}

type ModerationRetentionStore interface {
	DeleteExpiredSponsoredMessageImpressions(ctx context.Context, olderThan time.Time, limit int) (int, error)
	DeleteExpiredModerationAppealLinks(ctx context.Context, olderThan time.Time, limit int) (int, error)
}

// botAPIConfirmedGrace 是已确认 Bot API update 行的删除宽限：确认水位之下的行不会再被
// getUpdates 读取（fromID 恒 > confirmed），宽限仅防御 offset 回拨调试；回收目标是清堆积。
const botAPIConfirmedGrace = 15 * time.Minute

// tempAuthKeyExpiryGrace 只是一段数据库物理回收宽限。MTProto edge 在 expires_at
// 到点即停止入站 RPC、主动推送和重发，并断开连接；ResolveAuthKey 不容忍过期 key。
// 晚一天删除用于吸收客户端轮换/诊断窗口，不会延长协议有效期。
const tempAuthKeyExpiryGrace = 24 * time.Hour

// RetentionWorker 周期性回收存储中的死数据。
//
// 注意：TDesktop 不支持账号级 updates.differenceTooLong（api_updates.cpp 收到该响应只
// 记录日志且不清 requesting，会永久锁死 update 引擎），因此绝不能按普通 TTL 硬裁剪
// user_update_events。本 worker 只允许 store 删除“所有当前授权设备都明确确认”的连续安全
// 前缀；落后或缺 state 的任一设备都会把 floor 压回 0。客户端偶然带回已确认前的旧 pts 时，
// updates 服务通过普通 differenceSlice checkpoint 推进，不发送 differenceTooLong。
type RetentionWorker struct {
	tempKeys                    TempAuthKeyRetentionStore // 可为 nil（不回收 temp key 绑定）
	authKeySessionLayers        AuthKeySessionLayerRetentionStore
	botAPIUpdates               BotAPIUpdateRetentionStore // 可为 nil（不回收 Bot API 队列）
	userUpdates                 UserUpdateEventRetentionStore
	channelUpdates              ChannelUpdateEventRetentionStore
	loginCodeDeliveries         LoginCodeDeliveryRetentionStore
	clientTelemetry             ClientTelemetryRetentionStore
	authDeliveryReports         AuthDeliveryReportRetentionStore
	moderation                  ModerationRetentionStore
	orphanAuthKeys              OrphanAuthKeyRetentionStore
	activeAuthKeySnapshot       ActiveRawAuthKeySnapshotProvider
	activeAuthKeyHeartbeat      ActiveAuthKeyHeartbeatStore
	logger                      *zap.Logger
	retention                   time.Duration
	botAPIRetention             time.Duration
	orphanRetention             time.Duration
	clientTelemetryRetention    time.Duration
	authDeliveryReportRetention time.Duration
	interval                    time.Duration
	batch                       int
}

func NewRetentionWorker(tempKeys TempAuthKeyRetentionStore, logger *zap.Logger, retention, interval time.Duration, batch int) *RetentionWorker {
	if logger == nil {
		logger = zap.NewNop()
	}
	if retention <= 0 {
		retention = 168 * time.Hour
	}
	if interval <= 0 {
		interval = time.Hour
	}
	if batch <= 0 {
		batch = 10000
	}
	return &RetentionWorker{
		tempKeys:  tempKeys,
		logger:    logger,
		retention: retention,
		interval:  interval,
		batch:     batch,
	}
}

// WithBotAPIUpdateRetention 启用 bot_api_updates 队列回收；retention <=0 时用官方语义默认 24h。
func (w *RetentionWorker) WithBotAPIUpdateRetention(store BotAPIUpdateRetentionStore, retention time.Duration) *RetentionWorker {
	if retention <= 0 {
		retention = 24 * time.Hour
	}
	w.botAPIUpdates = store
	w.botAPIRetention = retention
	return w
}

// WithUserUpdateRetention 启用账号 update 的共同确认安全前缀回收。TDesktop 不支持
// account differenceTooLong，具体 store 必须保证未确认前缀永不删除。
func (w *RetentionWorker) WithUserUpdateRetention(store UserUpdateEventRetentionStore) *RetentionWorker {
	w.userUpdates = store
	return w
}

// WithChannelUpdateRetention 启用 channel durable update 的有界 TTL 回收；复用 worker 的
// retention/interval/batch，并由 store 的 retained floor 保证旧 pts 不会读到静默空洞。
func (w *RetentionWorker) WithChannelUpdateRetention(store ChannelUpdateEventRetentionStore) *RetentionWorker {
	w.channelUpdates = store
	return w
}

// WithLoginCodeDeliveryRetention enables bounded seek cleanup for compact
// phone_code_hash receipts. Each row carries its own expiry derived from the
// code TTL, so this cleanup intentionally does not reuse update-log retention.
func (w *RetentionWorker) WithLoginCodeDeliveryRetention(store LoginCodeDeliveryRetentionStore) *RetentionWorker {
	w.loginCodeDeliveries = store
	return w
}

// WithAuthKeySessionLayerRetention enables bounded seek cleanup for expired
// per-session Layer evidence.
func (w *RetentionWorker) WithAuthKeySessionLayerRetention(store AuthKeySessionLayerRetentionStore) *RetentionWorker {
	w.authKeySessionLayers = store
	return w
}

func (w *RetentionWorker) WithClientTelemetryRetention(store ClientTelemetryRetentionStore, retention time.Duration) *RetentionWorker {
	if retention <= 0 {
		retention = 30 * 24 * time.Hour
	}
	w.clientTelemetry = store
	w.clientTelemetryRetention = retention
	return w
}

func (w *RetentionWorker) WithAuthDeliveryReportRetention(store AuthDeliveryReportRetentionStore, retention time.Duration) *RetentionWorker {
	if retention <= 0 {
		retention = 30 * 24 * time.Hour
	}
	w.authDeliveryReports = store
	w.authDeliveryReportRetention = retention
	return w
}

func (w *RetentionWorker) WithModerationRetention(store ModerationRetentionStore) *RetentionWorker {
	w.moderation = store
	return w
}

// WithOrphanAuthKeyRetention 启用未授权握手 key 的有界回收。active 必须提供 raw key，
// 不能提供 temp→perm business key；否则未登录或 PFS 连接会被误判为 orphan。
func (w *RetentionWorker) WithOrphanAuthKeyRetention(store OrphanAuthKeyRetentionStore, active ActiveRawAuthKeySnapshotProvider, retention time.Duration) *RetentionWorker {
	w.orphanAuthKeys = store
	w.activeAuthKeySnapshot = active
	w.activeAuthKeyHeartbeat, _ = store.(ActiveAuthKeyHeartbeatStore)
	w.orphanRetention = retention
	return w
}

func (w *RetentionWorker) Run(ctx context.Context) {
	w.runOnce(ctx)
	retentionTicker := time.NewTicker(w.interval)
	defer retentionTicker.Stop()
	var (
		heartbeatTicker *time.Ticker
		heartbeatC      <-chan time.Time
	)
	if interval := w.orphanHeartbeatInterval(); interval > 0 {
		heartbeatTicker = time.NewTicker(interval)
		heartbeatC = heartbeatTicker.C
		defer heartbeatTicker.Stop()
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-retentionTicker.C:
			w.runRetentionOnce(ctx)
		case <-heartbeatC:
			w.heartbeatActiveAuthKeys(ctx)
		}
	}
}

func (w *RetentionWorker) runOnce(ctx context.Context) {
	w.runRetentionOnce(ctx)
}

func (w *RetentionWorker) runRetentionOnce(ctx context.Context) {
	if w.authKeySessionLayers != nil {
		deleted, err := w.authKeySessionLayers.DeleteExpiredSessionLayers(ctx, w.batch)
		if err != nil {
			w.logger.Warn("回收过期 auth-key session Layer 证据失败", zap.Error(err))
		} else if deleted > 0 {
			w.logger.Info("回收过期 auth-key session Layer 证据完成", zap.Int("deleted", deleted))
		}
	}
	if w.loginCodeDeliveries != nil {
		deleted, err := w.loginCodeDeliveries.DeleteExpiredLoginCodeDeliveries(ctx, time.Now(), w.batch)
		if err != nil {
			w.logger.Warn("回收过期 login-code delivery 回执失败", zap.Error(err))
		} else if deleted > 0 {
			w.logger.Info("回收过期 login-code delivery 回执完成", zap.Int("deleted", deleted))
		}
	}
	if w.clientTelemetry != nil {
		deleted, err := w.clientTelemetry.DeleteExpiredClientTelemetry(
			ctx, time.Now().Add(-w.clientTelemetryRetention), w.batch,
		)
		if err != nil {
			w.logger.Warn("回收过期客户端 telemetry 失败", zap.Error(err))
		} else if deleted > 0 {
			w.logger.Info("回收过期客户端 telemetry 完成", zap.Int("deleted", deleted))
		}
	}
	if w.authDeliveryReports != nil {
		deleted, err := w.authDeliveryReports.DeleteExpiredAuthDeliveryReports(
			ctx, time.Now().Add(-w.authDeliveryReportRetention), w.batch,
		)
		if err != nil {
			w.logger.Warn("回收过期验证码投递诊断失败", zap.Error(err))
		} else if deleted > 0 {
			w.logger.Info("回收过期验证码投递诊断完成", zap.Int("deleted", deleted))
		}
	}
	if w.moderation != nil {
		now := time.Now()
		impressions, err := w.moderation.DeleteExpiredSponsoredMessageImpressions(
			ctx, now, w.batch,
		)
		if err != nil {
			w.logger.Warn("回收过期 sponsored impression 失败", zap.Error(err))
		} else if impressions > 0 {
			w.logger.Info("回收过期 sponsored impression 完成", zap.Int("deleted", impressions))
		}
		links, err := w.moderation.DeleteExpiredModerationAppealLinks(
			ctx, now, w.batch,
		)
		if err != nil {
			w.logger.Warn("回收过期审核申诉链接失败", zap.Error(err))
		} else if links > 0 {
			w.logger.Info("回收过期审核申诉链接完成", zap.Int("deleted", links))
		}
	}
	if w.tempKeys != nil {
		expiredBefore := time.Now().Add(-tempAuthKeyExpiryGrace).Unix()
		tempDeleted, err := w.tempKeys.DeleteExpired(ctx, expiredBefore, w.batch)
		if err != nil {
			w.logger.Warn("回收过期 temp auth key 绑定失败", zap.Error(err))
		} else if tempDeleted > 0 {
			w.logger.Info("回收过期 temp auth key 绑定完成", zap.Int("deleted", tempDeleted))
		}
	}
	if w.orphanAuthKeys != nil && w.orphanRetention > 0 {
		protected, ok := w.activeRawAuthKeySnapshot(ctx)
		if !ok {
			// Fail safe: without a complete distributed active-key view, orphan
			// deletion cannot distinguish dead handshakes from live Edge sessions.
		} else if !w.touchActiveAuthKeys(ctx, protected) {
			// Fail safe: if this instance cannot publish the active set, deleting against a
			// stale database heartbeat could evict keys used by another instance too. Keep all
			// candidates for this pass and retry after the next heartbeat.
		} else {
			orphanDeleted, err := w.orphanAuthKeys.DeleteOrphaned(ctx, w.orphanRetention, w.batch, protected)
			if err != nil {
				w.logger.Warn("回收未授权 orphan auth key 失败", zap.Error(err))
			} else if orphanDeleted > 0 {
				w.logger.Info("回收未授权 orphan auth key 完成", zap.Int("deleted", orphanDeleted))
			}
		}
	}
	if w.botAPIUpdates != nil {
		botAPIDeleted, err := w.botAPIUpdates.DeleteDeliveredOrExpired(ctx, botAPIConfirmedGrace, w.botAPIRetention, w.batch)
		if err != nil {
			w.logger.Warn("回收 bot_api_updates 队列失败", zap.Error(err))
		} else if botAPIDeleted > 0 {
			w.logger.Info("回收 bot_api_updates 队列完成", zap.Int("deleted", botAPIDeleted))
		}
	}
	if w.userUpdates != nil {
		userDeleted, err := w.userUpdates.DeleteConfirmedPrefix(ctx, w.retention, w.batch)
		if err != nil {
			w.logger.Warn("回收已共同确认的 user_update_events 前缀失败", zap.Error(err))
		} else if userDeleted > 0 {
			w.logger.Info("回收已共同确认的 user_update_events 前缀完成", zap.Int("deleted", userDeleted))
		}
	}
	if w.channelUpdates != nil {
		channelDeleted, err := w.channelUpdates.DeleteExpiredChannelUpdateEvents(ctx, w.retention, w.batch)
		if err != nil {
			// store 会逐频道隔离坏 gap 后继续本轮；deleted 可能非零，必须同时记录，
			// 既不能把全局 pass 伪装成完全失败，也不能吞掉不变量错误。
			w.logger.Warn("回收过期 channel_update_events 存在隔离频道",
				zap.Int("deleted", channelDeleted),
				zap.Error(err),
			)
		} else if channelDeleted > 0 {
			w.logger.Info("回收过期 channel_update_events 连续前缀完成", zap.Int("deleted", channelDeleted))
		}
	}
}

func (w *RetentionWorker) orphanHeartbeatInterval() time.Duration {
	if w.activeAuthKeyHeartbeat == nil || w.activeAuthKeySnapshot == nil || w.orphanRetention <= 0 {
		return 0
	}
	interval := w.orphanRetention / 3
	if interval <= 0 {
		interval = time.Nanosecond
	}
	if w.interval > 0 && w.interval < interval {
		interval = w.interval
	}
	return interval
}

func (w *RetentionWorker) heartbeatActiveAuthKeys(ctx context.Context) {
	if w.activeAuthKeySnapshot == nil {
		return
	}
	protected, ok := w.activeRawAuthKeySnapshot(ctx)
	if !ok {
		return
	}
	w.touchActiveAuthKeys(ctx, protected)
}

// touchActiveAuthKeys requires the durable database heartbeat boundary. Orphan
// deletion is unsafe without both the distributed snapshot and that heartbeat.
func (w *RetentionWorker) touchActiveAuthKeys(ctx context.Context, protected [][8]byte) bool {
	if w.activeAuthKeyHeartbeat == nil {
		w.logger.Error("缺少 active raw auth key heartbeat，本轮跳过 orphan GC",
			zap.String("signal", "active_raw_auth_key_heartbeat_missing"),
		)
		return false
	}
	if err := w.activeAuthKeyHeartbeat.TouchActiveRawAuthKeys(ctx, protected); err != nil {
		w.logger.Error("刷新 active raw auth key heartbeat 失败，本轮跳过 orphan GC",
			zap.String("signal", "auth_key_heartbeat_failed"),
			zap.Int("active_keys", len(protected)),
			zap.Error(err),
		)
		return false
	}
	return true
}

func (w *RetentionWorker) activeRawAuthKeySnapshot(ctx context.Context) ([][8]byte, bool) {
	if w.activeAuthKeySnapshot == nil {
		w.logger.Error("缺少 active raw auth key snapshot，本轮跳过 orphan GC",
			zap.String("signal", "active_raw_auth_key_snapshot_missing"),
		)
		return nil, false
	}
	protected, err := w.activeAuthKeySnapshot.ActiveRawAuthKeySnapshot(ctx)
	if err != nil {
		w.logger.Error("读取 active raw auth key snapshot 失败，本轮跳过 orphan GC",
			zap.String("signal", "active_raw_auth_key_snapshot_failed"),
			zap.Error(err),
		)
		return nil, false
	}
	return compactRawAuthKeyIDs(protected), true
}

func compactRawAuthKeyIDs(ids [][8]byte) [][8]byte {
	if len(ids) == 0 {
		return nil
	}
	seen := make(map[[8]byte]struct{}, len(ids))
	out := make([][8]byte, 0, len(ids))
	for _, id := range ids {
		if id == ([8]byte{}) {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
