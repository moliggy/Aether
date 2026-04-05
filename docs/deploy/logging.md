# Aether Logging Guide

目标：

- 明确 `stdout`、文件日志和 `journald` 的职责边界
- 给 `aether-gateway` / `aether-proxy` 提供统一的查看与排障手册
- 避免重复轮转、重复采集和“日志写了但不知道去哪找”的问题

## 1. 组合策略

推荐组合不是“二选一”，而是按运行方式选一套固定策略：

| 场景 | 推荐配置 | 说明 |
|------|----------|------|
| 本地开发 | `stdout` | 直接看终端输出，最简单 |
| Docker Compose 开发 | `stdout` | 交给容器日志驱动，避免和文件日志重复 |
| systemd 单机生产 | `both` | `journald` 负责最近事件，文件日志负责留痕和 grep |
| 裸机临时排障 | `file` 或 `both` | 需要长时间保留时用文件日志 |

规则：

- `stdout` 进入容器日志驱动或 `journald`
- `file` 由应用自己轮转和清理
- `both` 适合宿主机生产
- 不要再给同一目录额外叠 `logrotate`

## 2. 关键配置

### aether-gateway

- `AETHER_LOG_DESTINATION=stdout|file|both`
- `AETHER_LOG_FORMAT=pretty|json`
- `AETHER_LOG_DIR=/var/log/aether`
- `AETHER_LOG_ROTATION=hourly|daily`
- `AETHER_LOG_RETENTION_DAYS=7`
- `AETHER_LOG_MAX_FILES=30`

### aether-proxy

- `AETHER_PROXY_LOG_DESTINATION=stdout|file|both`
- `AETHER_PROXY_LOG_JSON=false|true`
- `AETHER_PROXY_LOG_DIR=/var/log/aether-proxy`
- `AETHER_PROXY_LOG_ROTATION=hourly|daily`
- `AETHER_PROXY_LOG_RETENTION_DAYS=7`
- `AETHER_PROXY_LOG_MAX_FILES=30`

注意：

- `file` / `both` 必须同时给出 `*_LOG_DIR`
- Docker Compose 默认建议 `stdout`
- systemd 默认建议 `both`

## 3. 日志落点

### systemd

`aether-gateway`

- `journalctl -u aether-gateway`
- 文件日志目录默认建议 `/var/log/aether`

`aether-proxy`

- `journalctl -u aether-proxy`
- 文件日志目录默认建议 `/var/log/aether-proxy`

### Docker Compose

- `docker compose logs -f gateway`
- `docker compose logs -f aether-proxy`
- 如果显式启用了 `file/both`，再去看容器内挂载出来的日志目录

## 4. 常用排障命令

### gateway

```bash
sudo systemctl status aether-gateway --no-pager
sudo journalctl -u aether-gateway -n 200 --no-pager
sudo tail -n 200 /var/log/aether/aether-gateway.*.log
sudo rg "trace_id|request_id|error" /var/log/aether/aether-gateway.*.log
```

### proxy

```bash
sudo systemctl status aether-proxy --no-pager
sudo journalctl -u aether-proxy -n 200 --no-pager
sudo tail -n 200 /var/log/aether-proxy/aether-proxy.*.log
sudo rg "node_id|server|error" /var/log/aether-proxy/aether-proxy.*.log
```

### Docker

```bash
docker compose logs -f gateway
docker compose logs -f aether-proxy
```

## 5. 故障定位顺序

1. 先看 `systemctl status`，判断是不是进程本身没起来
2. 再看 `journalctl`，确认启动报错、权限报错、配置报错
3. 如果启用了 `both`，再查文件日志做结构化检索
4. 按 `trace_id` / `request_id` 串联同一请求
5. 对长时间问题看文件日志，对最近故障先看 `journald`

## 6. 常见问题

### 没有生成文件日志

优先检查：

- `*_LOG_DESTINATION` 是否是 `file` 或 `both`
- `*_LOG_DIR` 是否已设置
- systemd 的 `LogsDirectory=` 是否生效
- 进程用户对日志目录是否有写权限

### 日志重复

通常是以下原因之一：

- 容器里开了 `both`，同时又在看 `docker logs`
- 宿主机上既开了应用文件日志，又配了外部 `logrotate` / 采集器重复收集同一路径

处理方式：

- Docker 开发环境保持 `stdout`
- systemd 生产环境用 `both`
- 同一文件目录不要再叠第二套轮转器

### 磁盘持续增长

先看：

- `*_LOG_RETENTION_DAYS`
- `*_LOG_MAX_FILES`
- 是否误开 `hourly` 且保留天数过大
- 是否有旧目录不再被应用清理

### grep 不到请求

优先用这些字段：

- `trace_id`
- `request_id`
- `node_id`
- `route_class`
- `execution_path`

如果 `stdout` 噪声太大，就切到文件日志查。

## 7. 脱敏边界

常规日志不应该直接出现：

- `Authorization`
- `x-api-key`
- `x-goog-api-key`
- 完整请求体
- 完整响应体
- 完整 `report_context`

当前约定是：

- body 只记录摘要，例如字节数和 hash
- 真正的原文只允许进入显式调试通道，且必须先过 redaction

## 8. 关键事件名

当前先固定这批事件名，后续新增事件优先复用同一命名风格：

| event_name | log_type | 用途 |
|------------|----------|------|
| `http_request_started` | `access` | 请求进入 gateway |
| `http_request_completed` | `access` | 请求正常完成 |
| `http_request_failed` | `access` | 请求以 5xx 结束 |
| `local_sync_candidate_retry_scheduled` | `event` | 本地 sync 候选命中可重试结果，调度下一个候选 |
| `local_stream_candidate_retry_scheduled` | `event` | 本地 stream 候选命中可重试状态，调度下一个候选 |
| `local_openai_chat_candidates_exhausted` | `event` | 本地 OpenAI Chat 路径耗尽全部候选 |
| `local_core_finalize_fallback_raw_response_body` | `event` | 本地核心 finalize 无法映射成结构化响应，退回原始 body |
| `local_core_finalize_missing_error_report_mapping` | `event` | 本地核心 finalize 缺少错误上报映射 |
| `local_core_finalize_missing_success_report_mapping` | `event` | 本地核心 finalize 缺少成功上报映射 |
| `usage_terminal_settlement_failed` | `event` | usage 终态直写后结算失败 |
| `admin_billing_preset_applied` | `audit` | 管理员应用计费预设 |
| `admin_system_settings_updated` | `audit` | 管理员更新系统设置 |
| `admin_system_config_updated` | `audit` | 管理员更新系统配置项 |
| `admin_system_config_deleted` | `audit` | 管理员删除系统配置项 |
| `admin_wallet_balance_adjusted` | `audit` | 管理员手动调整钱包余额 |
| `admin_wallet_manual_recharge_created` | `audit` | 管理员创建手动充值 |
| `admin_wallet_refund_processed` | `audit` | 管理员开始处理退款 |
| `admin_wallet_refund_completed` | `audit` | 管理员完成退款 |
| `admin_wallet_refund_failed` | `audit` | 管理员标记退款失败 |
| `video_task_status_updated` | `event` | 异步视频任务轮询后状态发生更新 |
| `video_task_finalize_settlement_failed` | `event` | 异步视频任务终态结算失败 |
| `maintenance_worker_failed` | `ops` | 定时任务启动、tick 或调度查找失败 |
| `usage_cleanup_completed` | `ops` | usage 清理批次完成 |
| `provider_checkin_completed` | `ops` | provider checkin 批次完成 |
| `log_retention_cleanup_failed` | `ops` | 日志保留清理失败，但不阻断服务启动 |
| `execution_runtime_stream_flush_skipped` | `debug` | 下游断开，跳过 stream flush |
| `execution_runtime_stream_report_skipped` | `debug` | 下游断开，跳过 stream report |

命名规则：

- 统一小写蛇形
- 前缀先写模块，再写动作，再写结果
- `access`、`event`、`debug`、`ops`、`audit` 分层不要混用
- 管理员高风险写操作优先落 `audit`
- 定时任务批次结果和失败统一落 `ops`

## 9. 字段字典

这些字段应该优先保持稳定，方便 grep、告警和日志采集：

| 字段 | 含义 |
|------|------|
| `event_name` | 机器可依赖的稳定事件名 |
| `log_type` | 日志类别，例如 `access` / `event` / `debug` / `ops` / `audit` |
| `service` | 服务名，例如 `aether-gateway` / `aether-proxy` |
| `node_role` | 节点角色，例如 `gateway` / `proxy` |
| `instance_id` | 进程或节点实例标识 |
| `trace_id` | 跨链路关联键 |
| `request_id` | 业务请求标识 |
| `route_class` | 路由大类，例如 `passthrough` / `control` |
| `execution_path` | 实际执行路径，例如 `local` / `proxy` / `passthrough` |
| `status` | 事件状态，例如 `started` / `completed` / `failed` |
| `status_code` | HTTP 或上游状态码 |
| `elapsed_ms` | 整体耗时 |
| `error` | 错误摘要，不放原始敏感内容 |
| `candidate_id` | 候选执行槽位标识 |
| `candidate_count` | 候选数量或耗尽数量 |
| `provider_id` | 供应商标识 |
| `endpoint_id` | endpoint 标识 |
| `key_id` | provider key 标识 |
| `task_id` | 异步任务标识 |

约束：

- 没有稳定含义的临时字段不要进入长期事件模板
- 敏感原文不进入 `error`
- body 相关字段默认只允许摘要，不允许原文
