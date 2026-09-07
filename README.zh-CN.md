# gramsrv

**掌控服务器，使用 MTProto，连接真正的 Telegram 客户端。**

`gramsrv` 是一个用 Go 编写的开源 Telegram 兼容服务器和 MTProto 后端，
面向自主部署网络、协议研究，以及需要真实客户端兼容能力的社区聊天系统——
它不只是一个相似的聊天界面。

[English README](README.md) · [官网](https://telesrv.net) · [OwpenGram 客户端](https://owpengram.org/) · [讨论群](https://t.me/telesrv_chat) · [频道](https://t.me/telesrv)

<p align="center">
  <img src="docs/assets/gramsrv-telegram-desktop.png" width="68%" alt="Telegram Desktop 正在连接 gramsrv">
  <img src="docs/assets/gramsrv-android.png" width="23%" alt="Android 客户端正在连接 gramsrv">
</p>

## macOS/Linux 一键体验（v2）

先安装 Git 和 Docker Desktop（或带 Compose v2 的 Docker Engine）并启动 Docker，
然后执行：

```bash
git clone --branch v2 --single-branch https://github.com/iamxvbaba/gramsrv.git
cd gramsrv
./scripts/start-docker.sh
```

本机首次体验只需要这些命令。脚本会自动创建 `deploy/docker/.env`，无需登录即可从
`ghcr.io/iamxvbaba/gramsrv` 拉取七个已经构建好的 `v2` 镜像，按照依赖顺序启动
PostgreSQL、Redis、Migrate、File、Core、Egress、SFU、Admin 和 Edge，并等待全部
服务就绪。
用户不需要安装 Go、PostgreSQL 或 Redis，也不需要本地构建镜像或执行
`docker login`。

- 开发环境登录验证码固定为 **`12345`**。
- 零参数启动默认只监听 `127.0.0.1`，供同一台电脑上的客户端体验。
- 生成的部署默认启用 RTMP ingest 和 IPv4 TURN。远程使用时，除客户端和 SFU 端口外，
  还要在防火墙/NAT 放行并转发 `2400/tcp`、`12400/udp` 和 `12500-12999/udp`；启动脚本
  会打印实际端点。v2 Compose 已根据 `.env` 发布这些端口，不需要手改 `compose.yaml`。
- 客户端使用的匹配测试公钥位于
  [`deploy/docker/assets/test-server-rsa.pub`](deploy/docker/assets/test-server-rsa.pub)。
- 账号、数据库、媒体、Redis 状态和 RSA 身份保存在 Docker named volumes 中，
  重建容器或正常执行 Compose 停止后仍会保留。除非明确要清空数据，否则不要使用
  Compose `down -v`。
- 不带 `--build` 会直接使用发布镜像；`--build` 仅用于验证本地源码修改。

局域网临时体验时，把示例地址替换为 Docker 宿主机的局域网地址：

```bash
./scripts/start-docker.sh \
  --advertise-ip 192.168.1.20 \
  --public-base-url http://192.168.1.20:2401 \
  --public-web-base-url http://192.168.1.20:2401 \
  --allow-insecure-development-auth
```

官方 Telegram 客户端需要修改服务器 endpoint 和 RSA key 才能连接；请从
[项目官网](https://telesrv.net)获取兼容客户端。端口与防火墙、备份、升级、
远程访问和正式环境说明见 [Docker 部署手册](docs/docker-deployment.md)。

## 为什么选择 gramsrv

大多数 Telegram clone 复刻的是界面；`gramsrv` 实现的是服务器协议，让兼容
客户端能够通过你掌控的基础设施进行通信。

- 完整的 MTProto transport、认证、加密 session、RPC 调度、updates 和多端同步。
- 覆盖聊天、频道、媒体、reactions、gifts、bots、通话和管理功能的实用能力集。
- 从协议接入、业务服务到存储和实时媒体，服务器代码全部开放。
- 面向兼容性研究、实验和长期社区协作的 Go 代码库。

协议栈基于已发布的
[`github.com/iamxvbaba/td`](https://github.com/iamxvbaba/td) module，
并持续跟进 Telegram Desktop 的真实 wire 行为。

## 选择适合的架构

| 分支 | 架构 | 适用场景 |
|---|---|---|
| [`main`](../../tree/main) | 单体 | 单进程运行，适合开发、体验和较小规模的部署。 |
| [`v2`](../../tree/v2) | 微服务 | 独立服务边界，面向扩展性、可靠性和生产化运行。 |

## v2 架构一览

```mermaid
%%{init: {"theme":"neutral","flowchart":{"curve":"basis","nodeSpacing":32,"rankSpacing":48}}}%%
flowchart LR
  Clients["Telegram 客户端<br/>Desktop · Android · iOS · Web"]
  Edge["Edge<br/>MTProto · sessions"]
  Core["Core<br/>业务 RPC"]
  Egress["Egress<br/>可靠投递"]
  FileData["FileData<br/>媒体字节"]
  SFU["SFU<br/>实时媒体"]
  Postgres[("PostgreSQL<br/>业务状态 · outbox")]
  Redis[("Redis<br/>位置 · 推送 · 控制")]
  Blob[("Blob 存储")]

  Clients <-->|MTProto| Edge
  Edge -->|"CoreExec gRPC<br/>TL bytes"| Core
  Core -->|"业务状态 + 持久事件"| Postgres
  Egress -->|"领取 · 投影 · ACK"| Postgres
  Egress -->|"有界投递"| Redis
  Redis -->|"推送 / 控制"| Edge
  Edge -->|"客户端 ACK gRPC"| Egress
  Core -->|协调| Redis
  Edge -->|FileData gRPC| FileData
  Core -->|FileData gRPC| FileData
  FileData --> Blob
  Core -->|SFU 控制| SFU
  SFU -->|"注册 / 所有权"| Redis
  Clients <-->|"语音 · 视频"| SFU
```

长连接留在 Edge，业务执行留在 Core，可靠更新则通过 Egress 持久化投递。

## 当前能力

- 账号、联系人、用户资料、隐私、在线状态和多会话。
- 私聊、群组、超级群、频道、话题、邀请链接和公开链接。
- Durable updates、dialogs、已读状态、草稿、reactions、离线恢复和多端同步。
- 照片、文档、stickers、GIF、语音消息、网页预览、上传和下载。
- Gifts、Stars、Premium、bots、mini apps、翻译和 AI 输入辅助。
- 私聊通话信令、群通话、RTMP 直播和独立 SFU 媒体面。

Telegram Desktop 是第一兼容目标，Android、iOS 和 Web 路径也在持续覆盖。
部分高级功能仍以兼容性验证或实验为主，但对应实现都开放在本仓库中。

## 客户端

官方 Telegram 客户端信任 Telegram 生产环境的 DC 列表和 RSA keys，因此不能
直接连接私有服务器。你可以使用[项目官网](https://telesrv.net)提供的兼容客户端，
也可以自行构建只修改 endpoint 和 key 的客户端。

[OwpenGram](https://owpengram.org/) 是一个支持多服务器的 Telegram 风格客户端，
内置 `gramsrv` 支持，可以在官方网络、私有部署和社区节点之间切换。

## 一起建设

欢迎提交兼容性报告、聚焦的修复、测试和性能优化。最有价值的问题报告应包含
客户端版本、可复现步骤，以及受影响的 RPC 或功能路径。

贡献者：[ajarshia](https://github.com/ajarshia) — Android Persian (`fa`) 语言包。

## 授权与独立性

`gramsrv` 使用 [Apache License 2.0](LICENSE) 发布。本项目是独立的非官方项目，
与 Telegram 官方及其团队没有关联，也未获得其背书或赞助。
