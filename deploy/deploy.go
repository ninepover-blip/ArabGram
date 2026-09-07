// Package deploy 提供部署期资源（迁移脚本）的嵌入访问。
package deploy

import "embed"

// Migrations 是嵌入的 golang-migrate 迁移脚本（deploy/migrations/*.sql）。
// 供 store/postgres 的迁移 runner 在启动时执行，避免运行时依赖外部文件路径。
//
//go:embed migrations/*.sql
var Migrations embed.FS
