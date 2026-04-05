# Aether Gateway Systemd 部署

目标形态：

- `aether-gateway` 作为宿主机上的 `systemd` 服务运行
- Docker 只保留 `Postgres` 和 `Redis`
- 前端静态资源由 `aether-gateway` 直接服务

适用场景：

- 单机/单节点部署
- 允许短暂停机更新
- 未来需要网页一键更新

## 目录约定

安装脚本默认使用以下路径：

- Gateway release: `/opt/aether/releases/<release-id>`
- 当前版本软链接: `/opt/aether/current`
- systemd env 文件: `/etc/aether/aether-gateway.env`
- 数据栈 compose: `/opt/aether/shared/docker-compose.data.yml`

## 1. 构建二进制与前端

在目标机器或 CI 产物目录中准备：

```bash
cargo build --release -p aether-gateway
(cd frontend && npm ci && npm run build)
```

构建完成后需要存在：

- `target/release/aether-gateway`
- `frontend/dist`

## 2. 准备环境变量

复制示例文件并修改：

```bash
sudo mkdir -p /etc/aether
sudo cp deploy/systemd/aether-gateway.env.example /etc/aether/aether-gateway.env
sudo chmod 600 /etc/aether/aether-gateway.env
sudo vim /etc/aether/aether-gateway.env
```

注意：

- `systemd` 的 `EnvironmentFile` 只适合简单 `KEY=VALUE`
- 不要写 `export`
- 不要写 `${VAR}`、命令替换、shell 函数等复杂语法
- `DATABASE_URL` / `REDIS_URL` 请直接写完整值
- 文件日志建议直接配 `AETHER_LOG_DESTINATION=both` 和 `AETHER_LOG_DIR=/var/log/aether`
- 现在支持 `AETHER_GATEWAY_DEPLOYMENT_TOPOLOGY=single-node|multi-node`
- 现在支持 `AETHER_GATEWAY_NODE_ROLE=all|frontdoor|background`
- 如果未来准备跑多实例，先把它切成 `multi-node`，让启动期校验替你拦掉单机残留配置
- 安装脚本会拒绝示例占位值，例如 `change-me-*`、`change-this-*`

三种推荐运行档位：

- `single-node + all + Postgres/Redis`：完整单机，功能最全
- `single-node + all + 无 Postgres/Redis`：轻量单机，本地文件/内存兜底，适合最小化运行
- `multi-node + frontdoor/background + Postgres/Redis`：集群预备形态

## 3. 启动数据服务

这套 compose 只负责 `Postgres` 和 `Redis`：

```bash
docker compose \
  --env-file /etc/aether/aether-gateway.env \
  -f deploy/docker-compose.data.yml \
  up -d
```

其中：

- `Postgres` 数据目录走 Docker volume
- `Redis` 默认开启 `AOF + everysec`，避免 usage stream、分布式限流和锁状态在容器重启时全部丢失
- 端口默认只绑定到 `127.0.0.1`
- 如果给 gateway 打开 `AETHER_LOG_DESTINATION=file|both`，建议把 `AETHER_LOG_DIR` 指到持久目录

如果你已经执行过安装脚本，也可以使用安装后的固定路径：

```bash
docker compose \
  --env-file /etc/aether/aether-gateway.env \
  -f /opt/aether/shared/docker-compose.data.yml \
  up -d
```

## 4. 安装 systemd 服务

```bash
sudo deploy/systemd/install-systemd.sh --env-file /etc/aether/aether-gateway.env
```

这个脚本会完成：

- 创建 `aether` 系统用户/组
- 复制 release 到 `/opt/aether/releases/<release-id>`
- 更新 `/opt/aether/current` 软链接
- 安装 `aether-gateway.service`
- 安装数据栈 compose 到 `/opt/aether/shared/docker-compose.data.yml`
- 校验 env 文件语法、拓扑模式和关键密钥占位值
- 重载并启动 `aether-gateway`

日志约定：

- 默认 stdout 仍进入 `journald`
- 如果设置 `AETHER_LOG_DESTINATION=both`，gateway 会同时把日志写到 `AETHER_LOG_DIR`
- 文件日志目前支持 `daily/hourly` 轮转
- 文件日志会按 `AETHER_LOG_RETENTION_DAYS` 和 `AETHER_LOG_MAX_FILES` 自动清理
- 不建议再叠加外部 `logrotate` 对同一目录做二次轮转
- 更完整的组合策略和排障命令见 `docs/deploy/logging.md`

## 5. 验证

```bash
sudo systemctl status aether-gateway --no-pager
sudo journalctl -u aether-gateway -n 100 --no-pager
sudo ls -lah /var/log/aether
sudo tail -n 100 /var/log/aether/aether-gateway.*.log

curl -fsS http://127.0.0.1:8084/_gateway/health
curl -fsS http://127.0.0.1:8084/readyz
curl -fsS http://127.0.0.1:8084/.well-known/aether/frontdoor.json
```

## 6. 升级前备份

如果你使用仓库内置的数据栈 compose，升级前至少备份 `Postgres`：

```bash
docker compose \
  --env-file /etc/aether/aether-gateway.env \
  -f /opt/aether/shared/docker-compose.data.yml \
  exec -T postgres pg_dump -U postgres aether | gzip > backup_$(date +%Y%m%d_%H%M%S).sql.gz
```

如果你改过 `DB_USER` / `DB_NAME`，把命令里的 `postgres` / `aether` 替换成实际值。

## 7. 更新流程

重新构建新版本后，重复执行安装脚本即可：

```bash
cargo build --release -p aether-gateway
(cd frontend && npm ci && npm run build)
sudo deploy/systemd/install-systemd.sh \
  --env-file /etc/aether/aether-gateway.env \
  --release-id "$(date +%Y%m%d%H%M%S)"
```

这会：

- 安装新 release
- 切换 `/opt/aether/current`
- 重启 `aether-gateway`

## 8. 回滚

列出历史 release：

```bash
ls -1 /opt/aether/releases
```

把软链接切回旧版本并重启：

```bash
sudo ln -sfn /opt/aether/releases/<old-release-id> /opt/aether/current
sudo systemctl restart aether-gateway
```

如果这次更新包含数据库结构变更，单纯切回旧 release 不一定够，还需要把 `Postgres` 恢复到升级前备份。当前这套 Rust 迁移链路不应该再按老的 `alembic downgrade` 方式处理。

## 9. 集群演进注意点

这套方式适合单机，但不会阻塞以后做集群。要提前避免的坑：

- 不要把长期业务状态继续写进本地文件
- `AETHER_GATEWAY_VIDEO_TASK_STORE_PATH` 只适合单机临时方案
- 共享状态优先放 `Postgres` / `Redis`
- 以后扩成多实例时，替换的是上层托管者，不是 `aether-gateway` 二进制形态本身

建议先把单机 env 收敛成未来可迁移的基线：

- 保留 `DATABASE_URL` 和 `REDIS_URL`，不要跑“无 Redis 的单机特例”
- 默认就使用 `AETHER_GATEWAY_VIDEO_TASK_TRUTH_SOURCE_MODE=rust-authoritative`
- 只有单机临时排障时才启用 `AETHER_GATEWAY_VIDEO_TASK_STORE_PATH`
- 如果未来要接 `aether-proxy` / tunnel owner relay，多实例下要给每个节点单独设置 `AETHER_GATEWAY_INSTANCE_ID`
- 如果未来要跨节点转发 tunnel owner 请求，还要给每个节点设置外部可达的 `AETHER_TUNNEL_RELAY_BASE_URL`
- `AETHER_GATEWAY_DISTRIBUTED_REQUEST_LIMIT` 是可选项，但要开的话现在已经可以直接复用 `REDIS_URL`，不必再维护第二份 Redis 地址
- 多实例下不要再用 `AETHER_GATEWAY_NODE_ROLE=all`；前台节点用 `frontdoor`，后台节点用 `background`
- 多实例下前台限流不会再退回本地内存计数；单机模式才允许这种兜底
