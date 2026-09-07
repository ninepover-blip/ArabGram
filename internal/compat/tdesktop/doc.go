// Package tdesktop 集中存放 Telegram Desktop 兼容逻辑：目标版本/layer 记录、客户端 patch 说明、
// 启动 RPC 顺序、兼容矩阵辅助、TDesktop 专属 stub 与 feature flags。
//
// 兼容代码只允许出现在本包，禁止散落进业务 handler。后续 Android/iOS 另开 internal/compat/android|ios。
// 当前基线：TDesktop dev 9caf32dffc（v6.8.4+15），Layer 225。
//
// TDesktop 兼容逻辑集中在这里，避免散落到业务服务中。
package tdesktop
