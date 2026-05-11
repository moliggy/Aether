use std::collections::{BTreeMap, HashSet};

use async_trait::async_trait;
use sqlx::{sqlite::SqliteRow, Row};

use super::{
    provider_api_key_usage_is_error, provider_api_key_usage_is_success,
    strip_deprecated_usage_display_fields, usage_can_recover_terminal_failure,
    InMemoryUsageReadRepository, PendingUsageCleanupSummary, StoredRequestUsageAudit,
    UpsertUsageRecord, UsageWriteRepository,
};
use crate::driver::sqlite::{sqlite_optional_real, sqlite_real, SqlitePool};
use crate::error::SqlResultExt;
use crate::DataLayerError;

const USAGE_COLUMNS: &str = r#"
SELECT
  id,
  request_id,
  user_id,
  api_key_id,
  provider_name,
  model,
  target_model,
  provider_id,
  provider_endpoint_id,
  provider_api_key_id,
  request_type,
  api_format,
  api_family,
  endpoint_kind,
  endpoint_api_format,
  provider_api_family,
  provider_endpoint_kind,
  has_format_conversion,
  is_stream,
  upstream_is_stream,
  input_tokens,
  output_tokens,
  total_tokens,
  cache_creation_input_tokens,
  cache_creation_ephemeral_5m_input_tokens,
  cache_creation_ephemeral_1h_input_tokens,
  cache_read_input_tokens,
  CAST(cache_creation_cost_usd AS REAL) AS cache_creation_cost_usd,
  CAST(cache_read_cost_usd AS REAL) AS cache_read_cost_usd,
  CAST(output_price_per_1m AS REAL) AS output_price_per_1m,
  CAST(total_cost_usd AS REAL) AS total_cost_usd,
  CAST(actual_total_cost_usd AS REAL) AS actual_total_cost_usd,
  status_code,
  error_message,
  error_category,
  response_time_ms,
  first_byte_time_ms,
  status,
  billing_status,
  request_metadata,
  candidate_id,
  candidate_index,
  key_name,
  planner_kind,
  route_family,
  route_kind,
  execution_path,
  local_execution_runtime_miss_reason,
  finalized_at AS finalized_at_unix_secs,
  created_at_unix_ms,
  updated_at_unix_secs
FROM "usage"
"#;

const UPSERT_USAGE_SQL: &str = r#"
INSERT INTO "usage" (
  request_id,
  id,
  user_id,
  api_key_id,
  provider_name,
  model,
  target_model,
  provider_id,
  provider_endpoint_id,
  provider_api_key_id,
  request_type,
  api_format,
  api_family,
  endpoint_kind,
  endpoint_api_format,
  provider_api_family,
  provider_endpoint_kind,
  has_format_conversion,
  is_stream,
  upstream_is_stream,
  input_tokens,
  output_tokens,
  total_tokens,
  cache_creation_input_tokens,
  cache_creation_ephemeral_5m_input_tokens,
  cache_creation_ephemeral_1h_input_tokens,
  cache_read_input_tokens,
  cache_creation_cost_usd,
  cache_read_cost_usd,
  output_price_per_1m,
  total_cost_usd,
  actual_total_cost_usd,
  status_code,
  error_message,
  error_category,
  response_time_ms,
  first_byte_time_ms,
  status,
  billing_status,
  request_metadata,
  candidate_id,
  candidate_index,
  key_name,
  planner_kind,
  route_family,
  route_kind,
  execution_path,
  local_execution_runtime_miss_reason,
  finalized_at,
  created_at_unix_ms,
  updated_at_unix_secs
) VALUES (
  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
  ?
)
ON CONFLICT (request_id) DO UPDATE SET
  user_id = excluded.user_id,
  api_key_id = excluded.api_key_id,
  provider_name = excluded.provider_name,
  model = excluded.model,
  target_model = excluded.target_model,
  provider_id = excluded.provider_id,
  provider_endpoint_id = excluded.provider_endpoint_id,
  provider_api_key_id = excluded.provider_api_key_id,
  request_type = excluded.request_type,
  api_format = excluded.api_format,
  api_family = excluded.api_family,
  endpoint_kind = excluded.endpoint_kind,
  endpoint_api_format = excluded.endpoint_api_format,
  provider_api_family = excluded.provider_api_family,
  provider_endpoint_kind = excluded.provider_endpoint_kind,
  has_format_conversion = excluded.has_format_conversion,
  is_stream = excluded.is_stream,
  upstream_is_stream = excluded.upstream_is_stream,
  input_tokens = excluded.input_tokens,
  output_tokens = excluded.output_tokens,
  total_tokens = excluded.total_tokens,
  cache_creation_input_tokens = excluded.cache_creation_input_tokens,
  cache_creation_ephemeral_5m_input_tokens = excluded.cache_creation_ephemeral_5m_input_tokens,
  cache_creation_ephemeral_1h_input_tokens = excluded.cache_creation_ephemeral_1h_input_tokens,
  cache_read_input_tokens = excluded.cache_read_input_tokens,
  cache_creation_cost_usd = excluded.cache_creation_cost_usd,
  cache_read_cost_usd = excluded.cache_read_cost_usd,
  output_price_per_1m = excluded.output_price_per_1m,
  total_cost_usd = excluded.total_cost_usd,
  actual_total_cost_usd = excluded.actual_total_cost_usd,
  status_code = excluded.status_code,
  error_message = excluded.error_message,
  error_category = excluded.error_category,
  response_time_ms = excluded.response_time_ms,
  first_byte_time_ms = excluded.first_byte_time_ms,
  status = excluded.status,
  billing_status = excluded.billing_status,
  request_metadata = excluded.request_metadata,
  candidate_id = excluded.candidate_id,
  candidate_index = excluded.candidate_index,
  key_name = excluded.key_name,
  planner_kind = excluded.planner_kind,
  route_family = excluded.route_family,
  route_kind = excluded.route_kind,
  execution_path = excluded.execution_path,
  local_execution_runtime_miss_reason = excluded.local_execution_runtime_miss_reason,
  finalized_at = excluded.finalized_at,
  updated_at_unix_secs = excluded.updated_at_unix_secs
"#;

const SELECT_STALE_PENDING_USAGE_BATCH_SQL: &str = r#"
SELECT
  "usage".request_id,
  "usage".status,
  COALESCE(usage_settlement_snapshots.billing_status, "usage".billing_status) AS billing_status
FROM "usage"
LEFT JOIN usage_settlement_snapshots
  ON usage_settlement_snapshots.request_id = "usage".request_id
WHERE "usage".status IN ('pending', 'streaming')
  AND "usage".created_at_unix_ms < ?
ORDER BY "usage".created_at_unix_ms ASC, "usage".request_id ASC
LIMIT ?
"#;

const SELECT_COMPLETED_REQUEST_CANDIDATES_SQL: &str = r#"
SELECT status, extra_data
FROM request_candidates
WHERE request_id = ?
  AND status IN ('streaming', 'success')
"#;

#[derive(Debug, Clone)]
pub struct SqliteUsageWriteRepository {
    pool: SqlitePool,
}

#[derive(Debug, Clone)]
pub struct SqliteUsageReadRepository {
    pool: SqlitePool,
}

impl SqliteUsageReadRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    async fn materialize_read_model(&self) -> Result<InMemoryUsageReadRepository, DataLayerError> {
        let rows = sqlx::query(&format!(
            "{USAGE_COLUMNS} ORDER BY created_at_unix_ms ASC, request_id ASC"
        ))
        .fetch_all(&self.pool)
        .await
        .map_sql_err()?;

        let items = rows
            .iter()
            .map(map_usage_row)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(InMemoryUsageReadRepository::seed(items))
    }
}

impl_materialized_usage_read_repository!(SqliteUsageReadRepository);

impl SqliteUsageWriteRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn find_by_request_id(
        &self,
        request_id: &str,
    ) -> Result<Option<StoredRequestUsageAudit>, DataLayerError> {
        let row = sqlx::query(&format!("{USAGE_COLUMNS} WHERE request_id = ? LIMIT 1"))
            .bind(request_id)
            .fetch_optional(&self.pool)
            .await
            .map_sql_err()?;
        row.as_ref().map(map_usage_row).transpose()
    }
}

#[async_trait]
impl UsageWriteRepository for SqliteUsageWriteRepository {
    async fn upsert(
        &self,
        usage: UpsertUsageRecord,
    ) -> Result<StoredRequestUsageAudit, DataLayerError> {
        let usage = strip_deprecated_usage_display_fields(usage);
        usage.validate()?;

        if let Some(existing) = self.find_by_request_id(&usage.request_id).await? {
            if (existing.billing_status == "settled" || existing.billing_status == "void")
                && !usage_can_recover_terminal_failure(
                    &existing.status,
                    &existing.billing_status,
                    &usage.status,
                    &usage.billing_status,
                )
            {
                return Ok(existing);
            }
        }

        bind_upsert(sqlx::query(UPSERT_USAGE_SQL), &usage)?
            .execute(&self.pool)
            .await
            .map_sql_err()?;
        self.rebuild_api_key_usage_stats().await?;
        self.rebuild_provider_api_key_usage_stats().await?;
        self.find_by_request_id(&usage.request_id)
            .await?
            .ok_or_else(|| {
                DataLayerError::UnexpectedValue("usage upsert returned no row".to_string())
            })
    }

    async fn rebuild_api_key_usage_stats(&self) -> Result<u64, DataLayerError> {
        sqlx::query(
            r#"
UPDATE api_keys
SET total_requests = 0,
    total_tokens = 0,
    total_cost_usd = 0.0,
    last_used_at = NULL
"#,
        )
        .execute(&self.pool)
        .await
        .map_sql_err()?;

        let rows = sqlx::query(
            r#"
SELECT
  api_key_id,
  COUNT(*) AS total_requests,
  COALESCE(SUM(total_tokens), 0) AS total_tokens,
  CAST(COALESCE(SUM(total_cost_usd), 0) AS REAL) AS total_cost_usd,
  MAX(updated_at_unix_secs) AS last_used_at
FROM "usage"
WHERE api_key_id IS NOT NULL AND api_key_id <> ''
GROUP BY api_key_id
"#,
        )
        .fetch_all(&self.pool)
        .await
        .map_sql_err()?;

        for row in &rows {
            sqlx::query(
                r#"
UPDATE api_keys
SET total_requests = ?,
    total_tokens = ?,
    total_cost_usd = ?,
    last_used_at = ?
WHERE id = ?
"#,
            )
            .bind(row.try_get::<i64, _>("total_requests").map_sql_err()?)
            .bind(row.try_get::<i64, _>("total_tokens").map_sql_err()?)
            .bind(sqlite_real(row, "total_cost_usd")?)
            .bind(
                row.try_get::<Option<i64>, _>("last_used_at")
                    .map_sql_err()?,
            )
            .bind(row.try_get::<String, _>("api_key_id").map_sql_err()?)
            .execute(&self.pool)
            .await
            .map_sql_err()?;
        }

        Ok(rows.len() as u64)
    }

    async fn rebuild_provider_api_key_usage_stats(&self) -> Result<u64, DataLayerError> {
        sqlx::query(
            r#"
UPDATE provider_api_keys
SET request_count = 0,
    success_count = 0,
    error_count = 0,
    total_tokens = 0,
    total_cost_usd = 0.0,
    total_response_time_ms = 0,
    last_used_at = NULL
"#,
        )
        .execute(&self.pool)
        .await
        .map_sql_err()?;

        let rows = sqlx::query(
            r#"
SELECT
  provider_api_key_id,
  status,
  status_code,
  error_message,
  total_tokens,
  CAST(total_cost_usd AS REAL) AS total_cost_usd,
  response_time_ms,
  updated_at_unix_secs
FROM "usage"
WHERE provider_api_key_id IS NOT NULL AND provider_api_key_id <> ''
"#,
        )
        .fetch_all(&self.pool)
        .await
        .map_sql_err()?;

        let mut stats = BTreeMap::<String, ProviderKeyStats>::new();
        for row in rows {
            let key_id: String = row.try_get("provider_api_key_id").map_sql_err()?;
            let status: String = row.try_get("status").map_sql_err()?;
            let status_code = row.try_get::<Option<i64>, _>("status_code").map_sql_err()?;
            let status_code_u16 = status_code.and_then(|value| u16::try_from(value).ok());
            let error_message: Option<String> = row.try_get("error_message").map_sql_err()?;
            let entry = stats.entry(key_id).or_default();
            entry.request_count += 1;
            if provider_api_key_usage_is_success(&status, status_code_u16, error_message.as_deref())
            {
                entry.success_count += 1;
            }
            if provider_api_key_usage_is_error(&status, status_code_u16, error_message.as_deref()) {
                entry.error_count += 1;
            }
            entry.total_tokens += row.try_get::<i64, _>("total_tokens").map_sql_err()?;
            entry.total_cost_usd += sqlite_real(&row, "total_cost_usd")?;
            entry.total_response_time_ms += row
                .try_get::<Option<i64>, _>("response_time_ms")
                .map_sql_err()?
                .unwrap_or_default();
            entry.last_used_at = entry.last_used_at.max(
                row.try_get::<Option<i64>, _>("updated_at_unix_secs")
                    .map_sql_err()?,
            );
        }

        for (key_id, stat) in &stats {
            sqlx::query(
                r#"
UPDATE provider_api_keys
SET request_count = ?,
    success_count = ?,
    error_count = ?,
    total_tokens = ?,
    total_cost_usd = ?,
    total_response_time_ms = ?,
    last_used_at = ?
WHERE id = ?
"#,
            )
            .bind(stat.request_count)
            .bind(stat.success_count)
            .bind(stat.error_count)
            .bind(stat.total_tokens)
            .bind(stat.total_cost_usd)
            .bind(stat.total_response_time_ms)
            .bind(stat.last_used_at)
            .bind(key_id)
            .execute(&self.pool)
            .await
            .map_sql_err()?;
        }

        Ok(stats.len() as u64)
    }

    async fn cleanup_stale_pending_requests(
        &self,
        cutoff_unix_secs: u64,
        now_unix_secs: u64,
        timeout_minutes: u64,
        batch_size: usize,
    ) -> Result<PendingUsageCleanupSummary, DataLayerError> {
        if batch_size == 0 {
            return Ok(PendingUsageCleanupSummary::default());
        }

        let cutoff_unix_ms = cutoff_unix_secs.saturating_mul(1000);
        let now_unix_ms = now_unix_secs.saturating_mul(1000);
        let mut summary = PendingUsageCleanupSummary::default();
        let batch_size_u64 = u64::try_from(batch_size).map_err(|_| {
            DataLayerError::InvalidInput(format!(
                "invalid stale pending usage batch size: {batch_size}"
            ))
        })?;

        loop {
            let mut tx = self.pool.begin().await.map_sql_err()?;
            let stale_rows = sqlx::query(SELECT_STALE_PENDING_USAGE_BATCH_SQL)
                .bind(to_i64(cutoff_unix_ms, "stale pending usage cutoff")?)
                .bind(to_i64(batch_size_u64, "stale pending usage batch size")?)
                .fetch_all(&mut *tx)
                .await
                .map_sql_err()?;

            if stale_rows.is_empty() {
                tx.rollback().await.map_sql_err()?;
                break;
            }

            let stale_rows = stale_rows
                .iter()
                .map(|row| {
                    Ok(StalePendingUsageRow {
                        request_id: row.try_get("request_id").map_sql_err()?,
                        status: row.try_get("status").map_sql_err()?,
                        billing_status: row.try_get("billing_status").map_sql_err()?,
                    })
                })
                .collect::<Result<Vec<_>, DataLayerError>>()?;
            let completed_request_ids =
                completed_request_ids_sqlite(&mut tx, stale_rows.iter().map(|row| &row.request_id))
                    .await?;

            for row in stale_rows {
                if completed_request_ids.contains(&row.request_id) {
                    sqlx::query(
                        r#"
UPDATE "usage"
SET status = 'completed',
    status_code = 200,
    error_message = NULL
WHERE request_id = ?
"#,
                    )
                    .bind(&row.request_id)
                    .execute(&mut *tx)
                    .await
                    .map_sql_err()?;
                    sqlx::query(
                        r#"
UPDATE request_candidates
SET status = 'success',
    finished_at = ?
WHERE request_id = ?
  AND status = 'streaming'
"#,
                    )
                    .bind(to_i64(now_unix_ms, "request candidate finished_at")?)
                    .bind(&row.request_id)
                    .execute(&mut *tx)
                    .await
                    .map_sql_err()?;
                    summary.recovered += 1;
                    continue;
                }

                let error_message = stale_pending_error_message(&row.status, timeout_minutes);
                if row.billing_status == "pending" {
                    sqlx::query(
                        r#"
UPDATE "usage"
SET status = 'failed',
    status_code = 504,
    error_message = ?,
    billing_status = 'void',
    finalized_at = ?,
    total_cost_usd = 0.0,
    actual_total_cost_usd = 0.0
WHERE request_id = ?
"#,
                    )
                    .bind(&error_message)
                    .bind(to_i64(now_unix_secs, "usage finalized_at")?)
                    .bind(&row.request_id)
                    .execute(&mut *tx)
                    .await
                    .map_sql_err()?;
                    upsert_void_usage_settlement_snapshot_sqlite(
                        &mut tx,
                        &row.request_id,
                        now_unix_secs,
                    )
                    .await?;
                } else {
                    sqlx::query(
                        r#"
UPDATE "usage"
SET status = 'failed',
    status_code = 504,
    error_message = ?
WHERE request_id = ?
"#,
                    )
                    .bind(&error_message)
                    .bind(&row.request_id)
                    .execute(&mut *tx)
                    .await
                    .map_sql_err()?;
                }

                sqlx::query(
                    r#"
UPDATE request_candidates
SET status = 'failed',
    finished_at = ?,
    error_message = '请求超时（服务器可能已重启）'
WHERE request_id = ?
  AND status IN ('pending', 'streaming')
"#,
                )
                .bind(to_i64(now_unix_ms, "request candidate finished_at")?)
                .bind(&row.request_id)
                .execute(&mut *tx)
                .await
                .map_sql_err()?;
                summary.failed += 1;
            }

            tx.commit().await.map_sql_err()?;
        }

        Ok(summary)
    }
}

struct StalePendingUsageRow {
    request_id: String,
    status: String,
    billing_status: String,
}

#[derive(Default)]
struct ProviderKeyStats {
    request_count: i64,
    success_count: i64,
    error_count: i64,
    total_tokens: i64,
    total_cost_usd: f64,
    total_response_time_ms: i64,
    last_used_at: Option<i64>,
}

async fn completed_request_ids_sqlite<'a>(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    request_ids: impl Iterator<Item = &'a String>,
) -> Result<HashSet<String>, DataLayerError> {
    let mut completed = HashSet::new();
    for request_id in request_ids {
        let rows = sqlx::query(SELECT_COMPLETED_REQUEST_CANDIDATES_SQL)
            .bind(request_id)
            .fetch_all(&mut **tx)
            .await
            .map_sql_err()?;
        let mut is_completed = false;
        for row in &rows {
            if candidate_row_is_completed(row)? {
                is_completed = true;
                break;
            }
        }
        if is_completed {
            completed.insert(request_id.clone());
        }
    }
    Ok(completed)
}

fn candidate_row_is_completed(row: &SqliteRow) -> Result<bool, DataLayerError> {
    let status: String = row.try_get("status").map_sql_err()?;
    if status == "streaming" {
        return Ok(true);
    }
    if status != "success" {
        return Ok(false);
    }
    let Some(extra_data) = row
        .try_get::<Option<String>, _>("extra_data")
        .map_sql_err()?
    else {
        return Ok(false);
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(&extra_data) else {
        return Ok(false);
    };
    Ok(value
        .get("stream_completed")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false))
}

async fn upsert_void_usage_settlement_snapshot_sqlite(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    request_id: &str,
    now_unix_secs: u64,
) -> Result<(), DataLayerError> {
    let now = to_i64(now_unix_secs, "usage settlement snapshot timestamp")?;
    sqlx::query(
        r#"
INSERT INTO usage_settlement_snapshots (
  request_id,
  billing_status,
  finalized_at,
  created_at,
  updated_at
) VALUES (?, 'void', ?, ?, ?)
ON CONFLICT (request_id)
DO UPDATE SET
  billing_status = excluded.billing_status,
  finalized_at = COALESCE(usage_settlement_snapshots.finalized_at, excluded.finalized_at),
  updated_at = excluded.updated_at
"#,
    )
    .bind(request_id)
    .bind(now)
    .bind(now)
    .bind(now)
    .execute(&mut **tx)
    .await
    .map_sql_err()?;
    Ok(())
}

fn stale_pending_error_message(status: &str, timeout_minutes: u64) -> String {
    format!("请求超时: 状态 '{status}' 超过 {timeout_minutes} 分钟未完成")
}

fn bind_upsert<'q>(
    mut query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    usage: &'q UpsertUsageRecord,
) -> Result<sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>, DataLayerError>
{
    let input_tokens = usage.input_tokens.unwrap_or_default();
    let output_tokens = usage.output_tokens.unwrap_or_default();
    let cache_creation_tokens = usage
        .cache_creation_input_tokens
        .or_else(|| {
            Some(
                usage
                    .cache_creation_ephemeral_5m_input_tokens
                    .unwrap_or_default()
                    + usage
                        .cache_creation_ephemeral_1h_input_tokens
                        .unwrap_or_default(),
            )
        })
        .unwrap_or_default();
    let cache_read_tokens = usage.cache_read_input_tokens.unwrap_or_default();
    let total_tokens = usage
        .total_tokens
        .unwrap_or(input_tokens + output_tokens + cache_creation_tokens + cache_read_tokens);
    let created_at = usage
        .created_at_unix_ms
        .unwrap_or(usage.updated_at_unix_secs.saturating_mul(1000));
    let request_metadata = usage
        .request_metadata
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(|err| DataLayerError::InvalidInput(err.to_string()))?;

    query = query
        .bind(&usage.request_id)
        .bind(&usage.request_id)
        .bind(usage.user_id.as_deref())
        .bind(usage.api_key_id.as_deref())
        .bind(&usage.provider_name)
        .bind(&usage.model)
        .bind(usage.target_model.as_deref())
        .bind(usage.provider_id.as_deref())
        .bind(usage.provider_endpoint_id.as_deref())
        .bind(usage.provider_api_key_id.as_deref())
        .bind(usage.request_type.as_deref())
        .bind(usage.api_format.as_deref())
        .bind(usage.api_family.as_deref())
        .bind(usage.endpoint_kind.as_deref())
        .bind(usage.endpoint_api_format.as_deref())
        .bind(usage.provider_api_family.as_deref())
        .bind(usage.provider_endpoint_kind.as_deref())
        .bind(i64::from(usage.has_format_conversion.unwrap_or(false)))
        .bind(i64::from(usage.is_stream.unwrap_or(false)))
        .bind(i64::from(usage_upstream_is_stream(usage)))
        .bind(to_i64(input_tokens, "input_tokens")?)
        .bind(to_i64(output_tokens, "output_tokens")?)
        .bind(to_i64(total_tokens, "total_tokens")?)
        .bind(to_i64(
            cache_creation_tokens,
            "cache_creation_input_tokens",
        )?)
        .bind(to_i64(
            usage
                .cache_creation_ephemeral_5m_input_tokens
                .unwrap_or_default(),
            "cache_creation_ephemeral_5m_input_tokens",
        )?)
        .bind(to_i64(
            usage
                .cache_creation_ephemeral_1h_input_tokens
                .unwrap_or_default(),
            "cache_creation_ephemeral_1h_input_tokens",
        )?)
        .bind(to_i64(cache_read_tokens, "cache_read_input_tokens")?)
        .bind(usage.cache_creation_cost_usd.unwrap_or_default())
        .bind(usage.cache_read_cost_usd.unwrap_or_default())
        .bind(usage.output_price_per_1m)
        .bind(usage.total_cost_usd.unwrap_or_default())
        .bind(usage.actual_total_cost_usd.unwrap_or_default())
        .bind(usage.status_code.map(i64::from))
        .bind(usage.error_message.as_deref())
        .bind(usage.error_category.as_deref())
        .bind(usage.response_time_ms.map(|value| value as i64))
        .bind(usage.first_byte_time_ms.map(|value| value as i64))
        .bind(&usage.status)
        .bind(&usage.billing_status)
        .bind(request_metadata)
        .bind(usage.candidate_id.as_deref())
        .bind(usage.candidate_index.map(|value| value as i64))
        .bind(usage.key_name.as_deref())
        .bind(usage.planner_kind.as_deref())
        .bind(usage.route_family.as_deref())
        .bind(usage.route_kind.as_deref())
        .bind(usage.execution_path.as_deref())
        .bind(usage.local_execution_runtime_miss_reason.as_deref())
        .bind(usage.finalized_at_unix_secs.map(|value| value as i64))
        .bind(to_i64(created_at, "created_at_unix_ms")?)
        .bind(to_i64(usage.updated_at_unix_secs, "updated_at_unix_secs")?);
    Ok(query)
}

fn map_usage_row(row: &SqliteRow) -> Result<StoredRequestUsageAudit, DataLayerError> {
    let mut audit = StoredRequestUsageAudit::new(
        row.try_get("id").map_sql_err()?,
        row.try_get("request_id").map_sql_err()?,
        row.try_get("user_id").map_sql_err()?,
        row.try_get("api_key_id").map_sql_err()?,
        None,
        None,
        row.try_get("provider_name").map_sql_err()?,
        row.try_get("model").map_sql_err()?,
        row.try_get("target_model").map_sql_err()?,
        row.try_get("provider_id").map_sql_err()?,
        row.try_get("provider_endpoint_id").map_sql_err()?,
        row.try_get("provider_api_key_id").map_sql_err()?,
        row.try_get("request_type").map_sql_err()?,
        row.try_get("api_format").map_sql_err()?,
        row.try_get("api_family").map_sql_err()?,
        row.try_get("endpoint_kind").map_sql_err()?,
        row.try_get("endpoint_api_format").map_sql_err()?,
        row.try_get("provider_api_family").map_sql_err()?,
        row.try_get("provider_endpoint_kind").map_sql_err()?,
        row.try_get::<i64, _>("has_format_conversion")
            .map_sql_err()?
            != 0,
        row.try_get::<i64, _>("is_stream").map_sql_err()? != 0,
        row_i32(row, "input_tokens")?,
        row_i32(row, "output_tokens")?,
        row_i32(row, "total_tokens")?,
        sqlite_real(row, "total_cost_usd")?,
        sqlite_real(row, "actual_total_cost_usd")?,
        row_optional_i32(row, "status_code")?,
        row.try_get("error_message").map_sql_err()?,
        row.try_get("error_category").map_sql_err()?,
        row_optional_i32(row, "response_time_ms")?,
        row_optional_i32(row, "first_byte_time_ms")?,
        row.try_get("status").map_sql_err()?,
        row.try_get("billing_status").map_sql_err()?,
        row.try_get("created_at_unix_ms").map_sql_err()?,
        row.try_get("updated_at_unix_secs").map_sql_err()?,
        row.try_get("finalized_at_unix_secs").map_sql_err()?,
    )?;
    audit.cache_creation_input_tokens = row_u64(row, "cache_creation_input_tokens")?;
    audit.cache_creation_ephemeral_5m_input_tokens =
        row_u64(row, "cache_creation_ephemeral_5m_input_tokens")?;
    audit.cache_creation_ephemeral_1h_input_tokens =
        row_u64(row, "cache_creation_ephemeral_1h_input_tokens")?;
    audit.cache_read_input_tokens = row_u64(row, "cache_read_input_tokens")?;
    audit.cache_creation_cost_usd =
        sqlite_optional_real(row, "cache_creation_cost_usd")?.unwrap_or(0.0);
    audit.cache_read_cost_usd = sqlite_optional_real(row, "cache_read_cost_usd")?.unwrap_or(0.0);
    audit.output_price_per_1m = sqlite_optional_real(row, "output_price_per_1m")?;
    audit.request_metadata = row
        .try_get::<Option<String>, _>("request_metadata")
        .map_sql_err()?
        .map(|raw| serde_json::from_str(&raw))
        .transpose()
        .map_err(|err| DataLayerError::UnexpectedValue(err.to_string()))?;
    let upstream_is_stream = row
        .try_get::<Option<i64>, _>("upstream_is_stream")
        .map_sql_err()?
        .map(|value| value != 0);
    merge_usage_stream_metadata(&mut audit.request_metadata, upstream_is_stream);
    audit.candidate_id = row.try_get("candidate_id").map_sql_err()?;
    audit.candidate_index = row
        .try_get::<Option<i64>, _>("candidate_index")
        .map_sql_err()?
        .map(|value| value as u64);
    audit.key_name = row.try_get("key_name").map_sql_err()?;
    audit.planner_kind = row.try_get("planner_kind").map_sql_err()?;
    audit.route_family = row.try_get("route_family").map_sql_err()?;
    audit.route_kind = row.try_get("route_kind").map_sql_err()?;
    audit.execution_path = row.try_get("execution_path").map_sql_err()?;
    audit.local_execution_runtime_miss_reason = row
        .try_get("local_execution_runtime_miss_reason")
        .map_sql_err()?;
    Ok(audit)
}

fn to_i64(value: u64, field: &str) -> Result<i64, DataLayerError> {
    i64::try_from(value).map_err(|_| DataLayerError::InvalidInput(format!("{field} overflow")))
}

fn usage_upstream_is_stream(usage: &UpsertUsageRecord) -> bool {
    usage
        .request_metadata
        .as_ref()
        .and_then(serde_json::Value::as_object)
        .and_then(|metadata| metadata.get("upstream_is_stream"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or_else(|| usage.is_stream.unwrap_or(false))
}

fn merge_usage_stream_metadata(metadata: &mut Option<serde_json::Value>, upstream: Option<bool>) {
    let Some(upstream) = upstream else {
        return;
    };
    let value = metadata.get_or_insert_with(|| serde_json::json!({}));
    let Some(object) = value.as_object_mut() else {
        return;
    };
    object
        .entry("upstream_is_stream")
        .or_insert(serde_json::Value::Bool(upstream));
}

fn row_i32(row: &SqliteRow, field: &str) -> Result<i32, DataLayerError> {
    let value: i64 = row.try_get(field).map_sql_err()?;
    i32::try_from(value).map_err(|_| DataLayerError::UnexpectedValue(format!("{field} overflow")))
}

fn row_optional_i32(row: &SqliteRow, field: &str) -> Result<Option<i32>, DataLayerError> {
    row.try_get::<Option<i64>, _>(field)
        .map_sql_err()?
        .map(|value| {
            i32::try_from(value)
                .map_err(|_| DataLayerError::UnexpectedValue(format!("{field} overflow")))
        })
        .transpose()
}

fn row_u64(row: &SqliteRow, field: &str) -> Result<u64, DataLayerError> {
    let value: i64 = row.try_get(field).map_sql_err()?;
    u64::try_from(value).map_err(|_| DataLayerError::UnexpectedValue(format!("{field} negative")))
}

#[cfg(test)]
mod tests {
    use super::{SqliteUsageReadRepository, SqliteUsageWriteRepository};
    use crate::lifecycle::migrate::run_sqlite_migrations;
    use crate::repository::usage::{
        UpsertUsageRecord, UsageAuditListQuery, UsageDashboardSummaryQuery, UsageReadRepository,
        UsageWriteRepository,
    };

    #[tokio::test]
    async fn sqlite_usage_write_repository_upserts_and_rebuilds_stats() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite pool should connect");
        run_sqlite_migrations(&pool)
            .await
            .expect("sqlite migrations should run");
        seed_stats_targets(&pool).await;

        let repository = SqliteUsageWriteRepository::new(pool.clone());
        let record = repository
            .upsert(sample_usage("request-1", "completed", "pending", 1_000))
            .await
            .expect("usage should upsert");

        assert_eq!(record.request_id, "request-1");
        assert_eq!(record.api_key_id.as_deref(), Some("api-key-1"));
        assert_eq!(record.total_tokens, 7);
        assert_eq!(record.cache_read_input_tokens, 2);
        assert_eq!(
            record.request_metadata.as_ref().unwrap()["trace_id"],
            "trace-1"
        );
        assert_eq!(
            record.request_metadata.as_ref().unwrap()["upstream_is_stream"],
            true
        );
        let upstream_is_stream: Option<i64> =
            sqlx::query_scalar("SELECT upstream_is_stream FROM \"usage\" WHERE request_id = ?")
                .bind("request-1")
                .fetch_one(&pool)
                .await
                .expect("usage stream mode should load");
        assert_eq!(upstream_is_stream, Some(1));

        let loaded = repository
            .find_by_request_id("request-1")
            .await
            .expect("usage should load")
            .expect("usage should exist");
        assert_eq!(
            loaded.provider_api_key_id.as_deref(),
            Some("provider-key-1")
        );

        let stats = sqlx::query_as::<_, (i64, i64, f64, Option<i64>)>(
            "SELECT total_requests, total_tokens, total_cost_usd, last_used_at FROM api_keys WHERE id = 'api-key-1'",
        )
        .fetch_one(&pool)
        .await
        .expect("api key stats should load");
        assert_eq!(stats, (1, 7, 0.5, Some(1_000)));

        let provider_stats = sqlx::query_as::<_, (i64, i64, i64, i64, f64, i64, Option<i64>)>(
            "SELECT request_count, success_count, error_count, total_tokens, total_cost_usd, total_response_time_ms, last_used_at FROM provider_api_keys WHERE id = 'provider-key-1'",
        )
        .fetch_one(&pool)
        .await
        .expect("provider key stats should load");
        assert_eq!(provider_stats, (1, 1, 0, 7, 0.5, 42, Some(1_000)));
    }

    #[tokio::test]
    async fn sqlite_usage_write_repository_does_not_regress_void_usage() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite pool should connect");
        run_sqlite_migrations(&pool)
            .await
            .expect("sqlite migrations should run");
        seed_stats_targets(&pool).await;

        let repository = SqliteUsageWriteRepository::new(pool);
        repository
            .upsert(sample_usage("request-1", "failed", "void", 1_000))
            .await
            .expect("void usage should upsert");
        let existing = repository
            .upsert(sample_usage("request-1", "pending", "pending", 1_001))
            .await
            .expect("stale usage should be ignored");

        assert_eq!(existing.status, "failed");
        assert_eq!(existing.billing_status, "void");
        assert_eq!(existing.updated_at_unix_secs, 1_000);
    }

    #[tokio::test]
    async fn sqlite_usage_write_repository_cleans_stale_pending_requests() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite pool should connect");
        run_sqlite_migrations(&pool)
            .await
            .expect("sqlite migrations should run");
        seed_stats_targets(&pool).await;

        let repository = SqliteUsageWriteRepository::new(pool.clone());
        repository
            .upsert(sample_usage("request-recovered", "streaming", "pending", 1))
            .await
            .expect("streaming usage should upsert");
        repository
            .upsert(sample_usage("request-failed", "pending", "pending", 1))
            .await
            .expect("pending usage should upsert");

        sqlx::query(
            r#"
INSERT INTO request_candidates (
  id,
  request_id,
  candidate_index,
  retry_index,
  status,
  is_cached,
  created_at
) VALUES
  ('candidate-recovered', 'request-recovered', 0, 0, 'streaming', 0, 1),
  ('candidate-failed', 'request-failed', 0, 0, 'pending', 0, 1)
"#,
        )
        .execute(&pool)
        .await
        .expect("request candidates should seed");

        let summary = repository
            .cleanup_stale_pending_requests(2, 10, 5, 1)
            .await
            .expect("cleanup should run");
        assert_eq!(summary.recovered, 1);
        assert_eq!(summary.failed, 1);

        let recovered = repository
            .find_by_request_id("request-recovered")
            .await
            .expect("recovered usage should load")
            .expect("recovered usage should exist");
        assert_eq!(recovered.status, "completed");
        assert_eq!(recovered.status_code, Some(200));

        let failed = repository
            .find_by_request_id("request-failed")
            .await
            .expect("failed usage should load")
            .expect("failed usage should exist");
        assert_eq!(failed.status, "failed");
        assert_eq!(failed.status_code, Some(504));
        assert_eq!(failed.billing_status, "void");
        assert_eq!(failed.total_cost_usd, 0.0);
        assert_eq!(failed.finalized_at_unix_secs, Some(10));

        let candidate_statuses = sqlx::query_as::<_, (String, String, Option<i64>)>(
            r#"
SELECT request_id, status, finished_at
FROM request_candidates
ORDER BY request_id
"#,
        )
        .fetch_all(&pool)
        .await
        .expect("candidate statuses should load");
        assert_eq!(
            candidate_statuses,
            vec![
                (
                    "request-failed".to_string(),
                    "failed".to_string(),
                    Some(10_000)
                ),
                (
                    "request-recovered".to_string(),
                    "success".to_string(),
                    Some(10_000)
                ),
            ]
        );

        let snapshot = sqlx::query_as::<_, (String, Option<i64>)>(
            "SELECT billing_status, finalized_at FROM usage_settlement_snapshots WHERE request_id = 'request-failed'",
        )
        .fetch_one(&pool)
        .await
        .expect("void settlement snapshot should load");
        assert_eq!(snapshot, ("void".to_string(), Some(10)));
    }

    #[tokio::test]
    async fn sqlite_usage_read_repository_reads_usage_contract_views() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite pool should connect");
        run_sqlite_migrations(&pool)
            .await
            .expect("sqlite migrations should run");
        seed_stats_targets(&pool).await;

        let writer = SqliteUsageWriteRepository::new(pool.clone());
        writer
            .upsert(sample_usage("request-1", "completed", "settled", 1_000))
            .await
            .expect("usage should upsert");
        writer
            .upsert(sample_usage("request-2", "failed", "void", 1_010))
            .await
            .expect("usage should upsert");

        let reader = SqliteUsageReadRepository::new(pool);
        let loaded = reader
            .find_by_request_id("request-1")
            .await
            .expect("usage should load")
            .expect("usage should exist");
        assert_eq!(loaded.total_tokens, 7);
        assert_eq!(loaded.billing_status, "settled");

        let listed = reader
            .list_usage_audits(&UsageAuditListQuery {
                provider_name: Some("Provider One".to_string()),
                newest_first: true,
                ..UsageAuditListQuery::default()
            })
            .await
            .expect("usage list should load");
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].request_id, "request-2");

        let summary = reader
            .summarize_dashboard_usage(&UsageDashboardSummaryQuery {
                created_from_unix_secs: 999,
                created_until_unix_secs: 1_020,
                user_id: Some("user-1".to_string()),
            })
            .await
            .expect("dashboard summary should load");
        assert_eq!(summary.total_requests, 2);
        assert_eq!(summary.error_requests, 1);
        assert_eq!(summary.total_tokens, 10);
    }

    async fn seed_stats_targets(pool: &sqlx::SqlitePool) {
        sqlx::query(
            r#"
INSERT INTO users (id, auth_source, created_at, updated_at)
VALUES ('user-1', 'local', 1, 1);
INSERT INTO api_keys (id, user_id, key_hash, created_at, updated_at)
VALUES ('api-key-1', 'user-1', 'hash-1', 1, 1);
INSERT INTO providers (id, name, provider_type, created_at, updated_at)
VALUES ('provider-1', 'Provider One', 'openai', 1, 1);
INSERT INTO provider_api_keys (id, provider_id, name, created_at, updated_at)
VALUES ('provider-key-1', 'provider-1', 'Provider Key One', 1, 1);
"#,
        )
        .execute(pool)
        .await
        .expect("stats targets should seed");
    }

    fn sample_usage(
        request_id: &str,
        status: &str,
        billing_status: &str,
        updated_at: u64,
    ) -> UpsertUsageRecord {
        UpsertUsageRecord {
            request_id: request_id.to_string(),
            user_id: Some("user-1".to_string()),
            api_key_id: Some("api-key-1".to_string()),
            username: Some("legacy-user".to_string()),
            api_key_name: Some("legacy-key".to_string()),
            provider_name: "Provider One".to_string(),
            model: "model-1".to_string(),
            target_model: Some("target-model".to_string()),
            provider_id: Some("provider-1".to_string()),
            provider_endpoint_id: Some("endpoint-1".to_string()),
            provider_api_key_id: Some("provider-key-1".to_string()),
            request_type: Some("chat".to_string()),
            api_format: Some("openai".to_string()),
            api_family: Some("chat".to_string()),
            endpoint_kind: Some("chat".to_string()),
            endpoint_api_format: Some("openai".to_string()),
            provider_api_family: Some("chat".to_string()),
            provider_endpoint_kind: Some("chat".to_string()),
            has_format_conversion: Some(true),
            is_stream: Some(false),
            input_tokens: Some(2),
            output_tokens: Some(3),
            total_tokens: None,
            cache_creation_input_tokens: None,
            cache_creation_ephemeral_5m_input_tokens: Some(0),
            cache_creation_ephemeral_1h_input_tokens: Some(0),
            cache_read_input_tokens: Some(2),
            cache_creation_cost_usd: Some(0.0),
            cache_read_cost_usd: Some(0.1),
            output_price_per_1m: Some(2.0),
            total_cost_usd: Some(0.5),
            actual_total_cost_usd: Some(0.4),
            status_code: Some(200),
            error_message: None,
            error_category: None,
            response_time_ms: Some(42),
            first_byte_time_ms: Some(12),
            status: status.to_string(),
            billing_status: billing_status.to_string(),
            request_headers: None,
            request_body: None,
            request_body_ref: None,
            request_body_state: None,
            provider_request_headers: None,
            provider_request_body: None,
            provider_request_body_ref: None,
            provider_request_body_state: None,
            response_headers: None,
            response_body: None,
            response_body_ref: None,
            response_body_state: None,
            client_response_headers: None,
            client_response_body: None,
            client_response_body_ref: None,
            client_response_body_state: None,
            candidate_id: Some("candidate-1".to_string()),
            candidate_index: Some(1),
            key_name: Some("key-one".to_string()),
            planner_kind: Some("default".to_string()),
            route_family: Some("chat".to_string()),
            route_kind: Some("completion".to_string()),
            execution_path: Some("remote".to_string()),
            local_execution_runtime_miss_reason: None,
            request_metadata: Some(serde_json::json!({
                "trace_id": "trace-1",
                "upstream_is_stream": true,
            })),
            finalized_at_unix_secs: Some(updated_at),
            created_at_unix_ms: Some(updated_at),
            updated_at_unix_secs: updated_at,
        }
    }
}
