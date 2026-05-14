<p align="center">
  <img src="frontend/public/aether_adaptive.svg" width="120" height="120" alt="Aether Logo">
</p>

<h1 align="center">Aether</h1>

<p align="center">
  <strong>一站式 AI 基础设施平台</strong><br>
  支持 Claude / OpenAI / Gemini 及其 CLI 客户端的统一接入、格式转换、正/反向代理, 致力于成为用户驱动AI服务的底座
</p>
<p align="center">
  <a href="#简介">简介</a> •
  <a href="#部署">部署</a> •
  <a href="#api-文档">API 文档</a> •
  <a href="#环境变量">环境变量</a> •
  <a href="#qa">Q&A</a>
</p>


---

## 简介

Aether 是一个自托管的 AI API 网关，为团队和个人提供多租户管理、智能负载均衡、成本配额控制和健康监控能力。通过统一的 API 入口，可以无缝对接 Claude、OpenAI、Gemini 等主流 AI 服务及其 CLI 工具。

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/architecture/architecture-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="docs/architecture/architecture-light.svg">
    <img src="docs/architecture/architecture-light.svg" width="680" alt="Aether Architecture">
  </picture>
</p>

页面预览: https://fawney19.github.io/Aether/

## 部署

### Docker Compose（推荐：预构建镜像）

```bash
# 1. 克隆代码
git clone https://github.com/fawney19/Aether.git
cd Aether

# 2. 配置环境变量
cp .env.example .env
# 生成 JWT_SECRET_KEY / ENCRYPTION_KEY, 并填入 .env
./generate_keys.sh
# 编辑 .env 设置 ADMIN_PASSWORD

# 3. 首次部署 / 更新 (从以下数据库、内存策略任选其一)
# Postgres + Redis (适用于企业或多人使用)
docker compose pull && docker compose up -d
# 仅SQLite (适用于个人用户或朋友分享)
docker compose -f docker-compose.sqlite.yml pull && docker compose -f docker-compose.sqlite.yml up -d
```

### 一键安装（可选部署方式）

```bash
curl -fsSL https://raw.githubusercontent.com/fawney19/Aether/main/install.sh | sudo bash
```

```text
请选择安装语言 / Choose installer language:
  1) 中文
  2) 英语 / English

请输入选项 / Enter choice [1]:

请选择 Aether 版本:
  1) 最新正式版
  2) 最新 RC 预发布版
  3) 最新 Beta 预发布版
  4) 指定 tag，例如 v0.7.0-rc.1

请输入选项 [1]:

请选择 Aether 部署模式:
  1) Docker Compose: 应用 + Postgres + Redis
  2) 单机服务: systemd/launchd + SQLite + 进程内运行时
  3) 集群节点服务: systemd/launchd + 共享数据库 + Redis
  4) Docker Compose: 应用 + SQLite

请输入选项 [2]:

是否使用下载加速源?
  1) 否，使用原始 GitHub 地址
  2) 是，手动填写新的下载 URL

请输入选项 [1]:
```

安装后的常用命令（Linux systemd）：

```bash
sudo systemctl status aether-gateway --no-pager
sudo journalctl -u aether-gateway -f
sudo systemctl restart aether-gateway
```

安装后的常用命令（macOS launchd）：

```bash
sudo launchctl print system/com.aether.gateway
sudo launchctl kickstart -k system/com.aether.gateway
sudo launchctl bootout system /Library/LaunchDaemons/com.aether.gateway.plist
tail -f /var/log/aether/aether-gateway.out.log /var/log/aether/aether-gateway.err.log
```

macOS 原生安装使用系统级 LaunchDaemon，默认以专用 `_aether` 服务账号运行；配置和密钥写入 `/etc/aether/aether-gateway.env`，数据和应用日志仍在 `/opt/aether`，launchd stdout/stderr 在 `/var/log/aether`。

默认单机数据和应用日志都在安装目录内：

```text
/opt/aether/data/aether.db
/opt/aether/logs
```

多节点不能使用 SQLite 或 `AETHER_RUNTIME_BACKEND=memory`。如果先只生成了多节点模板，编辑 `/etc/aether/aether-gateway.env` 后重跑安装脚本即可：

```env
AETHER_GATEWAY_DEPLOYMENT_TOPOLOGY=multi-node
AETHER_GATEWAY_NODE_ROLE=frontdoor
DATABASE_URL=postgresql://...
REDIS_URL=redis://...
```

## Aether Proxy (可选)

Aether Proxy 是配套的正向代理节点，部署在海外 VPS 上，为墙内的 Aether 实例中转 API 流量。或者部署在其他服务器为指定的提供商、账号、Key使用不同的节点访问。支持 TUI 向导一键配置、systemd 服务管理、TLS 加密、DNS 缓存及连接池调优。

- Docker Compose 部署或下载预编译二进制直接运行
- 通过 `aether-proxy setup` 完成交互式配置，自动注册为系统服务
- 详细文档见 [apps/aether-proxy/README.md](apps/aether-proxy/README.md)

## API 文档

- Embeddings: [OpenAI compatible `POST /v1/embeddings`](docs/api/embeddings.md)
- Rerank: [OpenAI/Jina compatible `POST /v1/rerank`](docs/api/rerank.md)

## 环境变量

部署建议：

- Docker Compose：根目录 [`.env.example`](.env.example)
- systemd 二进制部署：使用根目录 `install.sh` 生成 `/etc/aether/aether-gateway.env`

当前主链路真正要关注的是这组变量：

- `APP_PORT`：`aether-gateway` 唯一监听端口，固定绑定 `0.0.0.0:${APP_PORT}`
- `AETHER_DATABASE_DRIVER` / `AETHER_DATABASE_URL`：二进制单机部署可用 `sqlite`，例如 `sqlite:///opt/aether/data/aether.db`
- `DATABASE_URL` / `REDIS_URL`：共享后端连接串；多节点必须配置共享数据库和 Redis
- `AETHER_RUNTIME_BACKEND=memory|redis`：运行时缓存/协调后端。单机 SQLite 默认用 `memory`，不会连接 Redis；显式设为 `redis` 或多节点部署才会把 `REDIS_URL` 注入运行时 Redis 后端
- `AETHER_GATEWAY_AUTO_PREPARE_DATABASE`：常规启动前自动执行挂起的 schema migration 和 backfill；仓库自带的 `docker-compose.yml` 默认开启
- `JWT_SECRET_KEY` / `ENCRYPTION_KEY`：认证和敏感数据加密所需密钥
- `API_KEY_PREFIX`：用户和管理员新建 API Key 时使用的前缀，默认 `sk`
- `ADMIN_USERNAME` / `ADMIN_PASSWORD` / `ADMIN_EMAIL`：首次启动时自举首个本地管理员；`install.sh` 会提示输入管理员密码
- `CORS_ORIGINS` / `CORS_ALLOW_CREDENTIALS`：前端跨域来源控制；如果要跨域带登录 Cookie，`CORS_ORIGINS` 不能写 `*`
- `AETHER_GATEWAY_DEPLOYMENT_TOPOLOGY=single-node|multi-node`
- `AETHER_GATEWAY_NODE_ROLE=all|frontdoor|background`
- `RUST_LOG`：Rust 日志过滤，例如 `aether_gateway=info`、`aether_gateway=debug,sqlx=warn`
- Docker Compose 的 `DB_PASSWORD` / `REDIS_PASSWORD` 默认使用 `aether`

systemd 的 `.env` 必须保持简单 `KEY=VALUE` 形式，不要写 `export`、`${VAR}` 或命令替换。

---

## 许可证

本项目采用 [Aether 非商业开源许可证](LICENSE)。允许个人学习、教育研究、非盈利组织及企业内部非盈利性质的使用；禁止用于盈利目的。商业使用请联系获取商业许可。

## 联系作者

<p align="center">
  <img src="docs/author/qq_qrcode.jpg" width="200" alt="QQ二维码">
  &nbsp;&nbsp;&nbsp;&nbsp;
  <img src="docs/author/qrcode_1770574997172.jpg" width="200" alt="QQ群二维码">
</p>

## Star History

[![Star History Chart](https://api.star-history.com/svg?repos=fawney19/Aether&type=Date)](https://star-history.com/#fawney19/Aether&Date)
