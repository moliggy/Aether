use futures_util::TryStreamExt;
use sqlx::Row;

use super::{MysqlBackend, PostgresBackend, SqliteBackend};
use crate::error::{SqlResultExt, SqlxResultExt};
use crate::repository::system::{
    AdminSystemPurgeSummary, AdminSystemPurgeTarget, AdminSystemStats, StoredSystemConfigEntry,
};
use crate::DataLayerError;

const POSTGRES_FIND_SYSTEM_CONFIG_VALUE_SQL: &str = r#"
SELECT value
FROM system_configs
WHERE key = $1
LIMIT 1
"#;

const POSTGRES_UPSERT_SYSTEM_CONFIG_VALUE_SQL: &str = r#"
INSERT INTO system_configs (id, key, value, description, created_at, updated_at)
VALUES ($1, $2, $3, $4, NOW(), NOW())
ON CONFLICT (key) DO UPDATE
SET value = EXCLUDED.value,
    description = COALESCE(EXCLUDED.description, system_configs.description),
    updated_at = NOW()
RETURNING value
"#;

const POSTGRES_LIST_SYSTEM_CONFIG_ENTRIES_SQL: &str = r#"
SELECT
    key,
    value,
    description,
    EXTRACT(EPOCH FROM updated_at)::bigint AS updated_at_unix_secs
FROM system_configs
ORDER BY key ASC
"#;

const POSTGRES_UPSERT_SYSTEM_CONFIG_ENTRY_SQL: &str = r#"
INSERT INTO system_configs (id, key, value, description, created_at, updated_at)
VALUES ($1, $2, $3, $4, NOW(), NOW())
ON CONFLICT (key) DO UPDATE
SET value = EXCLUDED.value,
    description = COALESCE(EXCLUDED.description, system_configs.description),
    updated_at = NOW()
RETURNING
    key,
    value,
    description,
    EXTRACT(EPOCH FROM updated_at)::bigint AS updated_at_unix_secs
"#;

const POSTGRES_DELETE_SYSTEM_CONFIG_VALUE_SQL: &str = r#"
DELETE FROM system_configs
WHERE key = $1
"#;

const POSTGRES_READ_ADMIN_SYSTEM_STATS_SQL: &str = r#"
SELECT
    (SELECT COUNT(*) FROM users) AS total_users,
    (SELECT COUNT(*) FROM users WHERE is_active IS TRUE) AS active_users,
    (SELECT COUNT(*) FROM api_keys) AS total_api_keys,
    (SELECT COUNT(*) FROM usage) AS total_requests
"#;

const MYSQL_READ_ADMIN_SYSTEM_STATS_SQL: &str = r#"
SELECT
    (SELECT COUNT(*) FROM users) AS total_users,
    (SELECT COUNT(*) FROM users WHERE is_active = 1) AS active_users,
    (SELECT COUNT(*) FROM api_keys) AS total_api_keys,
    (SELECT COUNT(*) FROM `usage`) AS total_requests
"#;

const SQLITE_READ_ADMIN_SYSTEM_STATS_SQL: &str = r#"
SELECT
    (SELECT COUNT(*) FROM users) AS total_users,
    (SELECT COUNT(*) FROM users WHERE is_active = 1) AS active_users,
    (SELECT COUNT(*) FROM api_keys) AS total_api_keys,
    (SELECT COUNT(*) FROM "usage") AS total_requests
"#;

impl PostgresBackend {
    pub async fn purge_admin_system_data(
        &self,
        target: AdminSystemPurgeTarget,
    ) -> Result<AdminSystemPurgeSummary, DataLayerError> {
        let mut tx = self.pool().begin().await.map_postgres_err()?;
        let mut summary = AdminSystemPurgeSummary::default();
        purge_postgres_admin_system_data(&mut tx, target, &mut summary).await?;
        tx.commit().await.map_postgres_err()?;
        Ok(summary)
    }

    pub async fn purge_admin_request_bodies_batch(
        &self,
        batch_size: usize,
    ) -> Result<AdminSystemPurgeSummary, DataLayerError> {
        if batch_size == 0 {
            return Ok(AdminSystemPurgeSummary::default());
        }
        let mut tx = self.pool().begin().await.map_postgres_err()?;
        let mut summary = AdminSystemPurgeSummary::default();
        purge_postgres_request_bodies_batch(&mut tx, batch_size, &mut summary).await?;
        tx.commit().await.map_postgres_err()?;
        Ok(summary)
    }

    pub async fn find_system_config_value(
        &self,
        key: &str,
    ) -> Result<Option<serde_json::Value>, DataLayerError> {
        let row = sqlx::query(POSTGRES_FIND_SYSTEM_CONFIG_VALUE_SQL)
            .bind(key)
            .fetch_optional(self.pool())
            .await
            .map_postgres_err()?;
        row.map(|row| row.try_get("value"))
            .transpose()
            .map_postgres_err()
    }

    pub async fn upsert_system_config_value(
        &self,
        key: &str,
        value: &serde_json::Value,
        description: Option<&str>,
    ) -> Result<serde_json::Value, DataLayerError> {
        let row = sqlx::query(POSTGRES_UPSERT_SYSTEM_CONFIG_VALUE_SQL)
            .bind(uuid::Uuid::new_v4().to_string())
            .bind(key)
            .bind(value)
            .bind(description)
            .fetch_one(self.pool())
            .await
            .map_postgres_err()?;
        row.try_get("value").map_postgres_err()
    }

    pub async fn list_system_config_entries(
        &self,
    ) -> Result<Vec<StoredSystemConfigEntry>, DataLayerError> {
        let mut rows = sqlx::query(POSTGRES_LIST_SYSTEM_CONFIG_ENTRIES_SQL).fetch(self.pool());
        let mut entries = Vec::new();
        while let Some(row) = rows.try_next().await.map_postgres_err()? {
            entries.push(StoredSystemConfigEntry {
                key: row.try_get("key").map_postgres_err()?,
                value: row.try_get("value").map_postgres_err()?,
                description: row.try_get("description").map_postgres_err()?,
                updated_at_unix_secs: row
                    .try_get::<Option<i64>, _>("updated_at_unix_secs")
                    .map_postgres_err()?
                    .map(|value| value.max(0) as u64),
            });
        }
        Ok(entries)
    }

    pub async fn upsert_system_config_entry(
        &self,
        key: &str,
        value: &serde_json::Value,
        description: Option<&str>,
    ) -> Result<StoredSystemConfigEntry, DataLayerError> {
        let row = sqlx::query(POSTGRES_UPSERT_SYSTEM_CONFIG_ENTRY_SQL)
            .bind(uuid::Uuid::new_v4().to_string())
            .bind(key)
            .bind(value)
            .bind(description)
            .fetch_one(self.pool())
            .await
            .map_postgres_err()?;
        Ok(StoredSystemConfigEntry {
            key: row.try_get("key").map_postgres_err()?,
            value: row.try_get("value").map_postgres_err()?,
            description: row.try_get("description").map_postgres_err()?,
            updated_at_unix_secs: row
                .try_get::<Option<i64>, _>("updated_at_unix_secs")
                .map_postgres_err()?
                .map(|value| value.max(0) as u64),
        })
    }

    pub async fn delete_system_config_value(&self, key: &str) -> Result<bool, DataLayerError> {
        let result = sqlx::query(POSTGRES_DELETE_SYSTEM_CONFIG_VALUE_SQL)
            .bind(key)
            .execute(self.pool())
            .await
            .map_postgres_err()?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn read_admin_system_stats(&self) -> Result<AdminSystemStats, DataLayerError> {
        let row = sqlx::query(POSTGRES_READ_ADMIN_SYSTEM_STATS_SQL)
            .fetch_one(self.pool())
            .await
            .map_postgres_err()?;
        postgres_admin_system_stats(row)
    }
}

impl MysqlBackend {
    pub async fn purge_admin_system_data(
        &self,
        target: AdminSystemPurgeTarget,
    ) -> Result<AdminSystemPurgeSummary, DataLayerError> {
        let mut tx = self.pool().begin().await.map_sql_err()?;
        let mut summary = AdminSystemPurgeSummary::default();
        purge_mysql_admin_system_data(&mut tx, target, &mut summary).await?;
        tx.commit().await.map_sql_err()?;
        Ok(summary)
    }

    pub async fn purge_admin_request_bodies_batch(
        &self,
        batch_size: usize,
    ) -> Result<AdminSystemPurgeSummary, DataLayerError> {
        if batch_size == 0 {
            return Ok(AdminSystemPurgeSummary::default());
        }
        let mut tx = self.pool().begin().await.map_sql_err()?;
        let mut summary = AdminSystemPurgeSummary::default();
        purge_mysql_request_bodies_batch(&mut tx, batch_size, &mut summary).await?;
        tx.commit().await.map_sql_err()?;
        Ok(summary)
    }

    pub async fn find_system_config_value(
        &self,
        key: &str,
    ) -> Result<Option<serde_json::Value>, DataLayerError> {
        let row = sqlx::query(
            r#"
SELECT value
FROM system_configs
WHERE `key` = ?
LIMIT 1
"#,
        )
        .bind(key)
        .fetch_optional(self.pool())
        .await
        .map_sql_err()?;

        row.map(|row| {
            row.try_get("value")
                .map_sql_err()
                .and_then(parse_json_value)
        })
        .transpose()
    }

    pub async fn upsert_system_config_value(
        &self,
        key: &str,
        value: &serde_json::Value,
        description: Option<&str>,
    ) -> Result<serde_json::Value, DataLayerError> {
        Ok(self
            .upsert_system_config_entry(key, value, description)
            .await?
            .value)
    }

    pub async fn list_system_config_entries(
        &self,
    ) -> Result<Vec<StoredSystemConfigEntry>, DataLayerError> {
        let rows = sqlx::query(
            r#"
SELECT `key`, value, description, updated_at
FROM system_configs
ORDER BY `key` ASC
"#,
        )
        .fetch_all(self.pool())
        .await
        .map_sql_err()?;

        rows.into_iter()
            .map(|row| {
                Ok(StoredSystemConfigEntry {
                    key: row.try_get("key").map_sql_err()?,
                    value: parse_json_value(row.try_get("value").map_sql_err()?)?,
                    description: row.try_get("description").map_sql_err()?,
                    updated_at_unix_secs: row
                        .try_get::<Option<i64>, _>("updated_at")
                        .map_sql_err()?
                        .map(|value| value.max(0) as u64),
                })
            })
            .collect()
    }

    pub async fn upsert_system_config_entry(
        &self,
        key: &str,
        value: &serde_json::Value,
        description: Option<&str>,
    ) -> Result<StoredSystemConfigEntry, DataLayerError> {
        let now = current_unix_secs();
        let serialized = serialize_json_value(value)?;
        sqlx::query(
            r#"
INSERT INTO system_configs (id, `key`, value, description, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
    value = VALUES(value),
    description = COALESCE(VALUES(description), description),
    updated_at = VALUES(updated_at)
"#,
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(key)
        .bind(serialized)
        .bind(description)
        .bind(now as i64)
        .bind(now as i64)
        .execute(self.pool())
        .await
        .map_sql_err()?;

        self.list_system_config_entries()
            .await?
            .into_iter()
            .find(|entry| entry.key == key)
            .ok_or_else(|| {
                DataLayerError::UnexpectedValue(format!(
                    "system config key '{key}' missing after mysql upsert"
                ))
            })
    }

    pub async fn delete_system_config_value(&self, key: &str) -> Result<bool, DataLayerError> {
        let result = sqlx::query(
            r#"
DELETE FROM system_configs
WHERE `key` = ?
"#,
        )
        .bind(key)
        .execute(self.pool())
        .await
        .map_sql_err()?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn read_admin_system_stats(&self) -> Result<AdminSystemStats, DataLayerError> {
        let row = sqlx::query(MYSQL_READ_ADMIN_SYSTEM_STATS_SQL)
            .fetch_one(self.pool())
            .await
            .map_sql_err()?;
        mysql_admin_system_stats(row)
    }
}

impl SqliteBackend {
    pub async fn purge_admin_system_data(
        &self,
        target: AdminSystemPurgeTarget,
    ) -> Result<AdminSystemPurgeSummary, DataLayerError> {
        let mut tx = self.pool().begin().await.map_sql_err()?;
        let mut summary = AdminSystemPurgeSummary::default();
        purge_sqlite_admin_system_data(&mut tx, target, &mut summary).await?;
        tx.commit().await.map_sql_err()?;
        Ok(summary)
    }

    pub async fn purge_admin_request_bodies_batch(
        &self,
        batch_size: usize,
    ) -> Result<AdminSystemPurgeSummary, DataLayerError> {
        if batch_size == 0 {
            return Ok(AdminSystemPurgeSummary::default());
        }
        let mut tx = self.pool().begin().await.map_sql_err()?;
        let mut summary = AdminSystemPurgeSummary::default();
        purge_sqlite_request_bodies_batch(&mut tx, batch_size, &mut summary).await?;
        tx.commit().await.map_sql_err()?;
        Ok(summary)
    }

    pub async fn find_system_config_value(
        &self,
        key: &str,
    ) -> Result<Option<serde_json::Value>, DataLayerError> {
        let row = sqlx::query(
            r#"
SELECT value
FROM system_configs
WHERE key = ?
LIMIT 1
"#,
        )
        .bind(key)
        .fetch_optional(self.pool())
        .await
        .map_sql_err()?;

        row.map(|row| {
            row.try_get("value")
                .map_sql_err()
                .and_then(parse_json_value)
        })
        .transpose()
    }

    pub async fn upsert_system_config_value(
        &self,
        key: &str,
        value: &serde_json::Value,
        description: Option<&str>,
    ) -> Result<serde_json::Value, DataLayerError> {
        Ok(self
            .upsert_system_config_entry(key, value, description)
            .await?
            .value)
    }

    pub async fn list_system_config_entries(
        &self,
    ) -> Result<Vec<StoredSystemConfigEntry>, DataLayerError> {
        let rows = sqlx::query(
            r#"
SELECT key, value, description, updated_at
FROM system_configs
ORDER BY key ASC
"#,
        )
        .fetch_all(self.pool())
        .await
        .map_sql_err()?;

        rows.into_iter()
            .map(|row| {
                Ok(StoredSystemConfigEntry {
                    key: row.try_get("key").map_sql_err()?,
                    value: parse_json_value(row.try_get("value").map_sql_err()?)?,
                    description: row.try_get("description").map_sql_err()?,
                    updated_at_unix_secs: row
                        .try_get::<Option<i64>, _>("updated_at")
                        .map_sql_err()?
                        .map(|value| value.max(0) as u64),
                })
            })
            .collect()
    }

    pub async fn upsert_system_config_entry(
        &self,
        key: &str,
        value: &serde_json::Value,
        description: Option<&str>,
    ) -> Result<StoredSystemConfigEntry, DataLayerError> {
        let now = current_unix_secs();
        let serialized = serialize_json_value(value)?;
        sqlx::query(
            r#"
INSERT INTO system_configs (id, key, value, description, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (key) DO UPDATE
SET value = excluded.value,
    description = COALESCE(excluded.description, system_configs.description),
    updated_at = excluded.updated_at
"#,
        )
        .bind(uuid::Uuid::new_v4().to_string())
        .bind(key)
        .bind(serialized)
        .bind(description)
        .bind(now as i64)
        .bind(now as i64)
        .execute(self.pool())
        .await
        .map_sql_err()?;

        self.list_system_config_entries()
            .await?
            .into_iter()
            .find(|entry| entry.key == key)
            .ok_or_else(|| {
                DataLayerError::UnexpectedValue(format!(
                    "system config key '{key}' missing after sqlite upsert"
                ))
            })
    }

    pub async fn delete_system_config_value(&self, key: &str) -> Result<bool, DataLayerError> {
        let result = sqlx::query(
            r#"
DELETE FROM system_configs
WHERE key = ?
"#,
        )
        .bind(key)
        .execute(self.pool())
        .await
        .map_sql_err()?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn read_admin_system_stats(&self) -> Result<AdminSystemStats, DataLayerError> {
        let row = sqlx::query(SQLITE_READ_ADMIN_SYSTEM_STATS_SQL)
            .fetch_one(self.pool())
            .await
            .map_sql_err()?;
        sqlite_admin_system_stats(row)
    }
}

const ADMIN_CONFIG_PURGE_TABLES: &[&str] = &[
    "api_key_provider_mappings",
    "gemini_file_mappings",
    "provider_usage_tracking",
    "billing_rules",
    "dimension_collectors",
    "models",
    "provider_endpoints",
    "provider_api_keys",
    "providers",
    "global_models",
    "proxy_node_events",
    "proxy_nodes",
    "user_oauth_links",
    "ldap_configs",
    "oauth_providers",
    "auth_modules",
    "system_configs",
];

const ADMIN_STATS_PURGE_TABLES: &[&str] = &[
    "stats_user_daily_cost_savings_model_provider",
    "stats_user_daily_cost_savings_model",
    "stats_user_daily_cost_savings_provider",
    "stats_user_daily_cost_savings",
    "stats_daily_cost_savings_model_provider",
    "stats_daily_cost_savings_model",
    "stats_daily_cost_savings_provider",
    "stats_daily_cost_savings",
    "stats_user_daily_model_provider",
    "stats_daily_model_provider",
    "stats_user_daily_api_format",
    "stats_user_daily_provider",
    "stats_hourly_user_model",
    "stats_user_daily_model",
    "stats_user_summary",
    "stats_hourly_user",
    "stats_user_daily",
    "stats_daily_api_key",
    "stats_daily_error",
    "stats_daily_model",
    "stats_daily_provider",
    "stats_hourly_model",
    "stats_hourly_provider",
    "stats_summary",
    "stats_hourly",
    "stats_daily",
];

const ADMIN_USAGE_CHILD_TABLES: &[&str] = &[
    "usage_body_blobs",
    "usage_http_audits",
    "usage_routing_snapshots",
    "usage_settlement_snapshots",
];

const ADMIN_USER_SCOPED_TABLES: &[&str] = &[
    "stats_user_daily_cost_savings_model_provider",
    "stats_user_daily_cost_savings_model",
    "stats_user_daily_cost_savings_provider",
    "stats_user_daily_cost_savings",
    "stats_user_daily_model_provider",
    "stats_user_daily_api_format",
    "stats_user_daily_provider",
    "stats_hourly_user_model",
    "stats_user_daily_model",
    "stats_user_summary",
    "stats_hourly_user",
    "stats_user_daily",
    "user_model_usage_counts",
    "announcement_reads",
    "management_tokens",
    "user_preferences",
    "user_sessions",
    "user_oauth_links",
];

fn checked_sql_identifier(value: &str) -> Result<&str, DataLayerError> {
    if !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    {
        Ok(value)
    } else {
        Err(DataLayerError::InvalidInput(format!(
            "invalid SQL identifier: {value}"
        )))
    }
}

async fn purge_postgres_admin_system_data(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    target: AdminSystemPurgeTarget,
    summary: &mut AdminSystemPurgeSummary,
) -> Result<(), DataLayerError> {
    match target {
        AdminSystemPurgeTarget::Config => {
            pg_execute_if_table(
                tx,
                "usage",
                "usage_provider_refs_cleared",
                r#"
UPDATE public.usage
SET provider_id = NULL,
    provider_endpoint_id = NULL,
    provider_api_key_id = NULL
WHERE provider_id IS NOT NULL
   OR provider_endpoint_id IS NOT NULL
   OR provider_api_key_id IS NOT NULL
"#,
                summary,
            )
            .await?;
            pg_execute_if_table(
                tx,
                "request_candidates",
                "request_candidate_provider_refs_cleared",
                r#"
UPDATE public.request_candidates
SET provider_id = NULL,
    endpoint_id = NULL,
    key_id = NULL
WHERE provider_id IS NOT NULL
   OR endpoint_id IS NOT NULL
   OR key_id IS NOT NULL
"#,
                summary,
            )
            .await?;
            pg_execute_if_table(
                tx,
                "video_tasks",
                "video_task_provider_refs_cleared",
                r#"
UPDATE public.video_tasks
SET provider_id = NULL,
    endpoint_id = NULL,
    key_id = NULL
WHERE provider_id IS NOT NULL
   OR endpoint_id IS NOT NULL
   OR key_id IS NOT NULL
"#,
                summary,
            )
            .await?;
            pg_execute_if_table(
                tx,
                "user_preferences",
                "user_default_provider_refs_cleared",
                "UPDATE public.user_preferences SET default_provider_id = NULL WHERE default_provider_id IS NOT NULL",
                summary,
            )
            .await?;
            for table in ADMIN_CONFIG_PURGE_TABLES {
                pg_delete_table(tx, table, summary).await?;
            }
        }
        AdminSystemPurgeTarget::Users => {
            purge_postgres_non_admin_users(tx, summary).await?;
        }
        AdminSystemPurgeTarget::Usage => {
            pg_delete_table(tx, "request_candidates", summary).await?;
            for table in ADMIN_USAGE_CHILD_TABLES {
                pg_delete_table(tx, table, summary).await?;
            }
            pg_delete_table(tx, "usage", summary).await?;
            pg_execute_if_table(
                tx,
                "api_keys",
                "api_key_usage_stats_reset",
                r#"
UPDATE public.api_keys
SET total_requests = 0,
    total_tokens = 0,
    total_cost_usd = 0,
    last_used_at = NULL
WHERE total_requests <> 0
   OR total_tokens <> 0
   OR total_cost_usd <> 0
   OR last_used_at IS NOT NULL
"#,
                summary,
            )
            .await?;
            pg_execute_if_table(
                tx,
                "provider_api_keys",
                "provider_key_usage_stats_reset",
                r#"
UPDATE public.provider_api_keys
SET request_count = 0,
    success_count = 0,
    error_count = 0,
    total_tokens = 0,
    total_cost_usd = 0,
    total_response_time_ms = 0,
    last_used_at = NULL
WHERE request_count <> 0
   OR success_count <> 0
   OR error_count <> 0
   OR total_tokens <> 0
   OR total_cost_usd <> 0
   OR total_response_time_ms <> 0
   OR last_used_at IS NOT NULL
"#,
                summary,
            )
            .await?;
        }
        AdminSystemPurgeTarget::AuditLogs => {
            pg_delete_table(tx, "audit_logs", summary).await?;
        }
        AdminSystemPurgeTarget::RequestBodies => {
            pg_delete_table(tx, "usage_body_blobs", summary).await?;
            pg_execute_if_table(
                tx,
                "usage",
                "usage_body_fields_cleaned",
                r#"
UPDATE public.usage
SET request_body = NULL,
    response_body = NULL,
    provider_request_body = NULL,
    client_response_body = NULL,
    request_body_compressed = NULL,
    response_body_compressed = NULL,
    provider_request_body_compressed = NULL,
    client_response_body_compressed = NULL
WHERE request_body IS NOT NULL
   OR response_body IS NOT NULL
   OR provider_request_body IS NOT NULL
   OR client_response_body IS NOT NULL
   OR request_body_compressed IS NOT NULL
   OR response_body_compressed IS NOT NULL
   OR provider_request_body_compressed IS NOT NULL
   OR client_response_body_compressed IS NOT NULL
"#,
                summary,
            )
            .await?;
            pg_execute_if_table(
                tx,
                "usage_http_audits",
                "usage_http_audit_body_refs_cleaned",
                r#"
UPDATE public.usage_http_audits
SET request_body_ref = NULL,
    provider_request_body_ref = NULL,
    response_body_ref = NULL,
    client_response_body_ref = NULL,
    request_body_state = NULL,
    provider_request_body_state = NULL,
    response_body_state = NULL,
    client_response_body_state = NULL,
    body_capture_mode = 'none',
    updated_at = NOW()
WHERE request_body_ref IS NOT NULL
   OR provider_request_body_ref IS NOT NULL
   OR response_body_ref IS NOT NULL
   OR client_response_body_ref IS NOT NULL
   OR request_body_state IS NOT NULL
   OR provider_request_body_state IS NOT NULL
   OR response_body_state IS NOT NULL
   OR client_response_body_state IS NOT NULL
   OR body_capture_mode <> 'none'
"#,
                summary,
            )
            .await?;
        }
        AdminSystemPurgeTarget::Stats => {
            for table in ADMIN_STATS_PURGE_TABLES {
                pg_delete_table(tx, table, summary).await?;
            }
        }
    }
    Ok(())
}

async fn purge_postgres_non_admin_users(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    summary: &mut AdminSystemPurgeSummary,
) -> Result<(), DataLayerError> {
    let non_admin_users = "SELECT id FROM public.users WHERE COALESCE(role::text, '') <> 'admin'";
    let non_admin_keys = "SELECT id FROM public.api_keys WHERE user_id IN (SELECT id FROM public.users WHERE COALESCE(role::text, '') <> 'admin')";

    pg_execute_if_table(
        tx,
        "api_key_provider_mappings",
        "api_key_provider_mappings",
        &format!(
            "DELETE FROM public.api_key_provider_mappings WHERE api_key_id IN ({non_admin_keys})"
        ),
        summary,
    )
    .await?;
    pg_execute_if_table(
        tx,
        "usage",
        "usage_user_refs_cleared",
        &format!(
            r#"
UPDATE public.usage
SET user_id = CASE WHEN user_id IN ({non_admin_users}) THEN NULL ELSE user_id END,
    api_key_id = CASE WHEN api_key_id IN ({non_admin_keys}) THEN NULL ELSE api_key_id END
WHERE user_id IN ({non_admin_users})
   OR api_key_id IN ({non_admin_keys})
"#
        ),
        summary,
    )
    .await?;
    pg_execute_if_table(
        tx,
        "request_candidates",
        "request_candidate_user_refs_cleared",
        &format!(
            r#"
UPDATE public.request_candidates
SET user_id = CASE WHEN user_id IN ({non_admin_users}) THEN NULL ELSE user_id END,
    api_key_id = CASE WHEN api_key_id IN ({non_admin_keys}) THEN NULL ELSE api_key_id END
WHERE user_id IN ({non_admin_users})
   OR api_key_id IN ({non_admin_keys})
"#
        ),
        summary,
    )
    .await?;
    pg_execute_if_table(
        tx,
        "audit_logs",
        "audit_log_user_refs_cleared",
        &format!(
            r#"
UPDATE public.audit_logs
SET user_id = CASE WHEN user_id IN ({non_admin_users}) THEN NULL ELSE user_id END,
    api_key_id = CASE WHEN api_key_id IN ({non_admin_keys}) THEN NULL ELSE api_key_id END
WHERE user_id IN ({non_admin_users})
   OR api_key_id IN ({non_admin_keys})
"#
        ),
        summary,
    )
    .await?;
    pg_execute_if_table(
        tx,
        "video_tasks",
        "video_task_user_refs_cleared",
        &format!(
            r#"
UPDATE public.video_tasks
SET user_id = CASE WHEN user_id IN ({non_admin_users}) THEN NULL ELSE user_id END,
    api_key_id = CASE WHEN api_key_id IN ({non_admin_keys}) THEN NULL ELSE api_key_id END
WHERE user_id IN ({non_admin_users})
   OR api_key_id IN ({non_admin_keys})
"#
        ),
        summary,
    )
    .await?;
    pg_execute_if_table(
        tx,
        "wallets",
        "wallet_user_refs_cleared",
        &format!(
            r#"
UPDATE public.wallets
SET user_id = CASE WHEN user_id IN ({non_admin_users}) THEN NULL ELSE user_id END,
    api_key_id = CASE WHEN api_key_id IN ({non_admin_keys}) THEN NULL ELSE api_key_id END
WHERE user_id IN ({non_admin_users})
   OR api_key_id IN ({non_admin_keys})
"#
        ),
        summary,
    )
    .await?;
    for (table, column, key) in [
        (
            "wallet_transactions",
            "operator_id",
            "wallet_transaction_operator_refs_cleared",
        ),
        (
            "payment_orders",
            "user_id",
            "payment_order_user_refs_cleared",
        ),
        (
            "announcements",
            "author_id",
            "announcement_author_refs_cleared",
        ),
        (
            "proxy_nodes",
            "registered_by",
            "proxy_node_registrant_refs_cleared",
        ),
    ] {
        pg_execute_if_table(
            tx,
            table,
            key,
            &format!(
                "UPDATE public.\"{table}\" SET {column} = NULL WHERE {column} IN ({non_admin_users})"
            ),
            summary,
        )
        .await?;
    }
    pg_execute_if_table(
        tx,
        "refund_requests",
        "refund_request_user_refs_cleared",
        &format!(
            r#"
UPDATE public.refund_requests
SET user_id = CASE WHEN user_id IN ({non_admin_users}) THEN NULL ELSE user_id END,
    requested_by = CASE WHEN requested_by IN ({non_admin_users}) THEN NULL ELSE requested_by END,
    approved_by = CASE WHEN approved_by IN ({non_admin_users}) THEN NULL ELSE approved_by END,
    processed_by = CASE WHEN processed_by IN ({non_admin_users}) THEN NULL ELSE processed_by END
WHERE user_id IN ({non_admin_users})
   OR requested_by IN ({non_admin_users})
   OR approved_by IN ({non_admin_users})
   OR processed_by IN ({non_admin_users})
"#
        ),
        summary,
    )
    .await?;
    pg_delete_non_admin_api_key_rows(tx, "stats_daily_api_key", summary).await?;
    for table in ADMIN_USER_SCOPED_TABLES {
        pg_delete_non_admin_user_rows(tx, table, summary).await?;
    }
    pg_execute_if_table(
        tx,
        "api_keys",
        "api_keys",
        &format!("DELETE FROM public.api_keys WHERE user_id IN ({non_admin_users})"),
        summary,
    )
    .await?;
    pg_execute_if_table(
        tx,
        "users",
        "users",
        "DELETE FROM public.users WHERE COALESCE(role::text, '') <> 'admin'",
        summary,
    )
    .await?;
    Ok(())
}

async fn purge_mysql_admin_system_data(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    target: AdminSystemPurgeTarget,
    summary: &mut AdminSystemPurgeSummary,
) -> Result<(), DataLayerError> {
    match target {
        AdminSystemPurgeTarget::Config => {
            mysql_execute_if_table(
                tx,
                "usage",
                "usage_provider_refs_cleared",
                r#"
UPDATE `usage`
SET provider_id = NULL,
    provider_endpoint_id = NULL,
    provider_api_key_id = NULL
WHERE provider_id IS NOT NULL
   OR provider_endpoint_id IS NOT NULL
   OR provider_api_key_id IS NOT NULL
"#,
                summary,
            )
            .await?;
            mysql_execute_if_table(
                tx,
                "request_candidates",
                "request_candidate_provider_refs_cleared",
                r#"
UPDATE request_candidates
SET provider_id = NULL,
    endpoint_id = NULL,
    key_id = NULL
WHERE provider_id IS NOT NULL
   OR endpoint_id IS NOT NULL
   OR key_id IS NOT NULL
"#,
                summary,
            )
            .await?;
            mysql_execute_if_table(
                tx,
                "video_tasks",
                "video_task_provider_refs_cleared",
                r#"
UPDATE video_tasks
SET provider_id = NULL,
    endpoint_id = NULL,
    key_id = NULL
WHERE provider_id IS NOT NULL
   OR endpoint_id IS NOT NULL
   OR key_id IS NOT NULL
"#,
                summary,
            )
            .await?;
            mysql_execute_if_table(
                tx,
                "user_preferences",
                "user_default_provider_refs_cleared",
                "UPDATE user_preferences SET default_provider_id = NULL WHERE default_provider_id IS NOT NULL",
                summary,
            )
            .await?;
            for table in ADMIN_CONFIG_PURGE_TABLES {
                mysql_delete_table(tx, table, summary).await?;
            }
        }
        AdminSystemPurgeTarget::Users => purge_mysql_non_admin_users(tx, summary).await?,
        AdminSystemPurgeTarget::Usage => {
            mysql_delete_table(tx, "request_candidates", summary).await?;
            for table in ADMIN_USAGE_CHILD_TABLES {
                mysql_delete_table(tx, table, summary).await?;
            }
            mysql_delete_table(tx, "usage", summary).await?;
            mysql_execute_if_table(
                tx,
                "api_keys",
                "api_key_usage_stats_reset",
                r#"
UPDATE api_keys
SET total_requests = 0,
    total_tokens = 0,
    total_cost_usd = 0,
    last_used_at = NULL
WHERE total_requests <> 0
   OR total_tokens <> 0
   OR total_cost_usd <> 0
   OR last_used_at IS NOT NULL
"#,
                summary,
            )
            .await?;
            mysql_execute_if_table(
                tx,
                "provider_api_keys",
                "provider_key_usage_stats_reset",
                r#"
UPDATE provider_api_keys
SET request_count = 0,
    success_count = 0,
    error_count = 0,
    total_tokens = 0,
    total_cost_usd = 0,
    total_response_time_ms = 0,
    last_used_at = NULL
WHERE request_count <> 0
   OR success_count <> 0
   OR error_count <> 0
   OR total_tokens <> 0
   OR total_cost_usd <> 0
   OR total_response_time_ms <> 0
   OR last_used_at IS NOT NULL
"#,
                summary,
            )
            .await?;
        }
        AdminSystemPurgeTarget::AuditLogs => {
            mysql_delete_table(tx, "audit_logs", summary).await?;
        }
        AdminSystemPurgeTarget::RequestBodies => {
            mysql_delete_table(tx, "usage_body_blobs", summary).await?;
            mysql_execute_if_table(
                tx,
                "usage_http_audits",
                "usage_http_audit_body_refs_cleaned",
                r#"
UPDATE usage_http_audits
SET request_body_ref = NULL,
    provider_request_body_ref = NULL,
    response_body_ref = NULL,
    client_response_body_ref = NULL,
    request_body_state = NULL,
    provider_request_body_state = NULL,
    response_body_state = NULL,
    client_response_body_state = NULL,
    body_capture_mode = 'none'
WHERE request_body_ref IS NOT NULL
   OR provider_request_body_ref IS NOT NULL
   OR response_body_ref IS NOT NULL
   OR client_response_body_ref IS NOT NULL
   OR request_body_state IS NOT NULL
   OR provider_request_body_state IS NOT NULL
   OR response_body_state IS NOT NULL
   OR client_response_body_state IS NOT NULL
   OR body_capture_mode <> 'none'
"#,
                summary,
            )
            .await?;
        }
        AdminSystemPurgeTarget::Stats => {
            for table in ADMIN_STATS_PURGE_TABLES {
                mysql_delete_table(tx, table, summary).await?;
            }
        }
    }
    Ok(())
}

async fn purge_mysql_non_admin_users(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    summary: &mut AdminSystemPurgeSummary,
) -> Result<(), DataLayerError> {
    let non_admin_users = "SELECT id FROM users WHERE LOWER(COALESCE(role, '')) <> 'admin'";
    let non_admin_keys = "SELECT id FROM api_keys WHERE user_id IN (SELECT id FROM users WHERE LOWER(COALESCE(role, '')) <> 'admin')";

    mysql_execute_if_table(
        tx,
        "api_key_provider_mappings",
        "api_key_provider_mappings",
        &format!("DELETE FROM api_key_provider_mappings WHERE api_key_id IN ({non_admin_keys})"),
        summary,
    )
    .await?;
    mysql_execute_if_table(
        tx,
        "usage",
        "usage_user_refs_cleared",
        &format!("UPDATE `usage` SET user_id = NULL WHERE user_id IN ({non_admin_users})"),
        summary,
    )
    .await?;
    mysql_execute_if_table(
        tx,
        "usage",
        "usage_api_key_refs_cleared",
        &format!("UPDATE `usage` SET api_key_id = NULL WHERE api_key_id IN ({non_admin_keys})"),
        summary,
    )
    .await?;
    for (table, column, key) in [
        (
            "request_candidates",
            "user_id",
            "request_candidate_user_refs_cleared",
        ),
        ("audit_logs", "user_id", "audit_log_user_refs_cleared"),
        ("video_tasks", "user_id", "video_task_user_refs_cleared"),
        ("wallets", "user_id", "wallet_user_refs_cleared"),
        (
            "payment_orders",
            "user_id",
            "payment_order_user_refs_cleared",
        ),
        (
            "wallet_transactions",
            "operator_id",
            "wallet_transaction_operator_refs_cleared",
        ),
        (
            "announcements",
            "author_id",
            "announcement_author_refs_cleared",
        ),
        (
            "proxy_nodes",
            "registered_by",
            "proxy_node_registrant_refs_cleared",
        ),
    ] {
        mysql_execute_if_table(
            tx,
            table,
            key,
            &format!(
                "UPDATE `{table}` SET `{column}` = NULL WHERE `{column}` IN ({non_admin_users})"
            ),
            summary,
        )
        .await?;
    }
    for (table, key) in [
        (
            "request_candidates",
            "request_candidate_api_key_refs_cleared",
        ),
        ("audit_logs", "audit_log_api_key_refs_cleared"),
        ("video_tasks", "video_task_api_key_refs_cleared"),
        ("wallets", "wallet_api_key_refs_cleared"),
    ] {
        mysql_execute_if_table(
            tx,
            table,
            key,
            &format!(
                "UPDATE `{table}` SET api_key_id = NULL WHERE api_key_id IN ({non_admin_keys})"
            ),
            summary,
        )
        .await?;
    }
    mysql_delete_non_admin_api_key_rows(tx, "stats_daily_api_key", summary).await?;
    mysql_execute_if_table(
        tx,
        "refund_requests",
        "refund_request_user_refs_cleared",
        &format!(
            r#"
UPDATE refund_requests
SET user_id = CASE WHEN user_id IN ({non_admin_users}) THEN NULL ELSE user_id END,
    requested_by = CASE WHEN requested_by IN ({non_admin_users}) THEN NULL ELSE requested_by END,
    approved_by = CASE WHEN approved_by IN ({non_admin_users}) THEN NULL ELSE approved_by END,
    processed_by = CASE WHEN processed_by IN ({non_admin_users}) THEN NULL ELSE processed_by END
WHERE user_id IN ({non_admin_users})
   OR requested_by IN ({non_admin_users})
   OR approved_by IN ({non_admin_users})
   OR processed_by IN ({non_admin_users})
"#
        ),
        summary,
    )
    .await?;
    for table in ADMIN_USER_SCOPED_TABLES {
        mysql_delete_non_admin_user_rows(tx, table, summary).await?;
    }
    mysql_execute_if_table(
        tx,
        "api_keys",
        "api_keys",
        &format!("DELETE FROM api_keys WHERE user_id IN ({non_admin_users})"),
        summary,
    )
    .await?;
    mysql_execute_if_table(
        tx,
        "users",
        "users",
        "DELETE FROM users WHERE LOWER(COALESCE(role, '')) <> 'admin'",
        summary,
    )
    .await?;
    Ok(())
}

async fn purge_sqlite_admin_system_data(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    target: AdminSystemPurgeTarget,
    summary: &mut AdminSystemPurgeSummary,
) -> Result<(), DataLayerError> {
    match target {
        AdminSystemPurgeTarget::Config => {
            sqlite_execute_if_table(
                tx,
                "usage",
                "usage_provider_refs_cleared",
                r#"
UPDATE "usage"
SET provider_id = NULL,
    provider_endpoint_id = NULL,
    provider_api_key_id = NULL
WHERE provider_id IS NOT NULL
   OR provider_endpoint_id IS NOT NULL
   OR provider_api_key_id IS NOT NULL
"#,
                summary,
            )
            .await?;
            sqlite_execute_if_table(
                tx,
                "request_candidates",
                "request_candidate_provider_refs_cleared",
                r#"
UPDATE request_candidates
SET provider_id = NULL,
    endpoint_id = NULL,
    key_id = NULL
WHERE provider_id IS NOT NULL
   OR endpoint_id IS NOT NULL
   OR key_id IS NOT NULL
"#,
                summary,
            )
            .await?;
            sqlite_execute_if_table(
                tx,
                "video_tasks",
                "video_task_provider_refs_cleared",
                r#"
UPDATE video_tasks
SET provider_id = NULL,
    endpoint_id = NULL,
    key_id = NULL
WHERE provider_id IS NOT NULL
   OR endpoint_id IS NOT NULL
   OR key_id IS NOT NULL
"#,
                summary,
            )
            .await?;
            sqlite_execute_if_table(
                tx,
                "user_preferences",
                "user_default_provider_refs_cleared",
                "UPDATE user_preferences SET default_provider_id = NULL WHERE default_provider_id IS NOT NULL",
                summary,
            )
            .await?;
            for table in ADMIN_CONFIG_PURGE_TABLES {
                sqlite_delete_table(tx, table, summary).await?;
            }
        }
        AdminSystemPurgeTarget::Users => purge_sqlite_non_admin_users(tx, summary).await?,
        AdminSystemPurgeTarget::Usage => {
            sqlite_delete_table(tx, "request_candidates", summary).await?;
            for table in ADMIN_USAGE_CHILD_TABLES {
                sqlite_delete_table(tx, table, summary).await?;
            }
            sqlite_delete_table(tx, "usage", summary).await?;
            sqlite_execute_if_table(
                tx,
                "api_keys",
                "api_key_usage_stats_reset",
                r#"
UPDATE api_keys
SET total_requests = 0,
    total_tokens = 0,
    total_cost_usd = 0,
    last_used_at = NULL
WHERE total_requests <> 0
   OR total_tokens <> 0
   OR total_cost_usd <> 0
   OR last_used_at IS NOT NULL
"#,
                summary,
            )
            .await?;
            sqlite_execute_if_table(
                tx,
                "provider_api_keys",
                "provider_key_usage_stats_reset",
                r#"
UPDATE provider_api_keys
SET request_count = 0,
    success_count = 0,
    error_count = 0,
    total_tokens = 0,
    total_cost_usd = 0,
    total_response_time_ms = 0,
    last_used_at = NULL
WHERE request_count <> 0
   OR success_count <> 0
   OR error_count <> 0
   OR total_tokens <> 0
   OR total_cost_usd <> 0
   OR total_response_time_ms <> 0
   OR last_used_at IS NOT NULL
"#,
                summary,
            )
            .await?;
        }
        AdminSystemPurgeTarget::AuditLogs => {
            sqlite_delete_table(tx, "audit_logs", summary).await?;
        }
        AdminSystemPurgeTarget::RequestBodies => {
            sqlite_delete_table(tx, "usage_body_blobs", summary).await?;
            sqlite_execute_if_table(
                tx,
                "usage_http_audits",
                "usage_http_audit_body_refs_cleaned",
                r#"
UPDATE usage_http_audits
SET request_body_ref = NULL,
    provider_request_body_ref = NULL,
    response_body_ref = NULL,
    client_response_body_ref = NULL,
    request_body_state = NULL,
    provider_request_body_state = NULL,
    response_body_state = NULL,
    client_response_body_state = NULL,
    body_capture_mode = 'none'
WHERE request_body_ref IS NOT NULL
   OR provider_request_body_ref IS NOT NULL
   OR response_body_ref IS NOT NULL
   OR client_response_body_ref IS NOT NULL
   OR request_body_state IS NOT NULL
   OR provider_request_body_state IS NOT NULL
   OR response_body_state IS NOT NULL
   OR client_response_body_state IS NOT NULL
   OR body_capture_mode <> 'none'
"#,
                summary,
            )
            .await?;
        }
        AdminSystemPurgeTarget::Stats => {
            for table in ADMIN_STATS_PURGE_TABLES {
                sqlite_delete_table(tx, table, summary).await?;
            }
        }
    }
    Ok(())
}

async fn purge_sqlite_non_admin_users(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    summary: &mut AdminSystemPurgeSummary,
) -> Result<(), DataLayerError> {
    let non_admin_users = "SELECT id FROM users WHERE LOWER(COALESCE(role, '')) <> 'admin'";
    let non_admin_keys = "SELECT id FROM api_keys WHERE user_id IN (SELECT id FROM users WHERE LOWER(COALESCE(role, '')) <> 'admin')";

    sqlite_execute_if_table(
        tx,
        "api_key_provider_mappings",
        "api_key_provider_mappings",
        &format!("DELETE FROM api_key_provider_mappings WHERE api_key_id IN ({non_admin_keys})"),
        summary,
    )
    .await?;
    sqlite_execute_if_table(
        tx,
        "usage",
        "usage_user_refs_cleared",
        &format!(r#"UPDATE "usage" SET user_id = NULL WHERE user_id IN ({non_admin_users})"#),
        summary,
    )
    .await?;
    sqlite_execute_if_table(
        tx,
        "usage",
        "usage_api_key_refs_cleared",
        &format!(r#"UPDATE "usage" SET api_key_id = NULL WHERE api_key_id IN ({non_admin_keys})"#),
        summary,
    )
    .await?;
    for (table, column, key) in [
        (
            "request_candidates",
            "user_id",
            "request_candidate_user_refs_cleared",
        ),
        ("audit_logs", "user_id", "audit_log_user_refs_cleared"),
        ("video_tasks", "user_id", "video_task_user_refs_cleared"),
        ("wallets", "user_id", "wallet_user_refs_cleared"),
        (
            "payment_orders",
            "user_id",
            "payment_order_user_refs_cleared",
        ),
        (
            "wallet_transactions",
            "operator_id",
            "wallet_transaction_operator_refs_cleared",
        ),
        (
            "announcements",
            "author_id",
            "announcement_author_refs_cleared",
        ),
        (
            "proxy_nodes",
            "registered_by",
            "proxy_node_registrant_refs_cleared",
        ),
    ] {
        sqlite_execute_if_table(
            tx,
            table,
            key,
            &format!(
                r#"UPDATE "{table}" SET "{column}" = NULL WHERE "{column}" IN ({non_admin_users})"#
            ),
            summary,
        )
        .await?;
    }
    for (table, key) in [
        (
            "request_candidates",
            "request_candidate_api_key_refs_cleared",
        ),
        ("audit_logs", "audit_log_api_key_refs_cleared"),
        ("video_tasks", "video_task_api_key_refs_cleared"),
        ("wallets", "wallet_api_key_refs_cleared"),
    ] {
        sqlite_execute_if_table(
            tx,
            table,
            key,
            &format!(
                r#"UPDATE "{table}" SET api_key_id = NULL WHERE api_key_id IN ({non_admin_keys})"#
            ),
            summary,
        )
        .await?;
    }
    sqlite_delete_non_admin_api_key_rows(tx, "stats_daily_api_key", summary).await?;
    sqlite_execute_if_table(
        tx,
        "refund_requests",
        "refund_request_user_refs_cleared",
        &format!(
            r#"
UPDATE refund_requests
SET user_id = CASE WHEN user_id IN ({non_admin_users}) THEN NULL ELSE user_id END,
    requested_by = CASE WHEN requested_by IN ({non_admin_users}) THEN NULL ELSE requested_by END,
    approved_by = CASE WHEN approved_by IN ({non_admin_users}) THEN NULL ELSE approved_by END,
    processed_by = CASE WHEN processed_by IN ({non_admin_users}) THEN NULL ELSE processed_by END
WHERE user_id IN ({non_admin_users})
   OR requested_by IN ({non_admin_users})
   OR approved_by IN ({non_admin_users})
   OR processed_by IN ({non_admin_users})
"#
        ),
        summary,
    )
    .await?;
    for table in ADMIN_USER_SCOPED_TABLES {
        sqlite_delete_non_admin_user_rows(tx, table, summary).await?;
    }
    sqlite_execute_if_table(
        tx,
        "api_keys",
        "api_keys",
        &format!("DELETE FROM api_keys WHERE user_id IN ({non_admin_users})"),
        summary,
    )
    .await?;
    sqlite_execute_if_table(
        tx,
        "users",
        "users",
        "DELETE FROM users WHERE LOWER(COALESCE(role, '')) <> 'admin'",
        summary,
    )
    .await?;
    Ok(())
}

async fn purge_postgres_request_bodies_batch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    batch_size: usize,
    summary: &mut AdminSystemPurgeSummary,
) -> Result<(), DataLayerError> {
    let limit = i64::try_from(batch_size).unwrap_or(i64::MAX);
    pg_execute_batch_if_table(
        tx,
        "usage_body_blobs",
        "usage_body_blobs",
        r#"
WITH doomed AS (
    SELECT body_ref
    FROM public.usage_body_blobs
    ORDER BY body_ref ASC
    LIMIT $1
)
DELETE FROM public.usage_body_blobs AS blobs
USING doomed
WHERE blobs.body_ref = doomed.body_ref
"#,
        summary,
        limit,
    )
    .await?;
    pg_execute_batch_if_table(
        tx,
        "usage",
        "usage_body_fields_cleaned",
        r#"
WITH batch AS (
    SELECT request_id
    FROM public.usage
    WHERE request_body IS NOT NULL
       OR response_body IS NOT NULL
       OR provider_request_body IS NOT NULL
       OR client_response_body IS NOT NULL
       OR request_body_compressed IS NOT NULL
       OR response_body_compressed IS NOT NULL
       OR provider_request_body_compressed IS NOT NULL
       OR client_response_body_compressed IS NOT NULL
    ORDER BY created_at_unix_ms ASC, request_id ASC
    LIMIT $1
)
UPDATE public.usage AS usage_rows
SET request_body = NULL,
    response_body = NULL,
    provider_request_body = NULL,
    client_response_body = NULL,
    request_body_compressed = NULL,
    response_body_compressed = NULL,
    provider_request_body_compressed = NULL,
    client_response_body_compressed = NULL
FROM batch
WHERE usage_rows.request_id = batch.request_id
"#,
        summary,
        limit,
    )
    .await?;
    pg_execute_batch_if_table(
        tx,
        "usage_http_audits",
        "usage_http_audit_body_refs_cleaned",
        r#"
WITH batch AS (
    SELECT request_id
    FROM public.usage_http_audits
    WHERE request_body_ref IS NOT NULL
       OR provider_request_body_ref IS NOT NULL
       OR response_body_ref IS NOT NULL
       OR client_response_body_ref IS NOT NULL
       OR request_body_state IS NOT NULL
       OR provider_request_body_state IS NOT NULL
       OR response_body_state IS NOT NULL
       OR client_response_body_state IS NOT NULL
       OR body_capture_mode <> 'none'
    ORDER BY request_id ASC
    LIMIT $1
)
UPDATE public.usage_http_audits AS audits
SET request_body_ref = NULL,
    provider_request_body_ref = NULL,
    response_body_ref = NULL,
    client_response_body_ref = NULL,
    request_body_state = NULL,
    provider_request_body_state = NULL,
    response_body_state = NULL,
    client_response_body_state = NULL,
    body_capture_mode = 'none',
    updated_at = NOW()
FROM batch
WHERE audits.request_id = batch.request_id
"#,
        summary,
        limit,
    )
    .await?;
    Ok(())
}

async fn purge_mysql_request_bodies_batch(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    batch_size: usize,
    summary: &mut AdminSystemPurgeSummary,
) -> Result<(), DataLayerError> {
    let limit = i64::try_from(batch_size).unwrap_or(i64::MAX);
    mysql_execute_batch_if_table(
        tx,
        "usage_body_blobs",
        "usage_body_blobs",
        r#"
DELETE FROM usage_body_blobs
WHERE body_ref IN (
    SELECT body_ref FROM (
        SELECT body_ref
        FROM usage_body_blobs
        ORDER BY body_ref ASC
        LIMIT ?
    ) AS doomed
)
"#,
        summary,
        limit,
    )
    .await?;
    mysql_execute_batch_if_table(
        tx,
        "usage",
        "usage_body_fields_cleaned",
        r#"
UPDATE `usage`
SET request_body = NULL,
    response_body = NULL,
    provider_request_body = NULL,
    client_response_body = NULL,
    request_body_compressed = NULL,
    response_body_compressed = NULL,
    provider_request_body_compressed = NULL,
    client_response_body_compressed = NULL
WHERE request_id IN (
    SELECT request_id FROM (
        SELECT request_id
        FROM `usage`
        WHERE request_body IS NOT NULL
           OR response_body IS NOT NULL
           OR provider_request_body IS NOT NULL
           OR client_response_body IS NOT NULL
           OR request_body_compressed IS NOT NULL
           OR response_body_compressed IS NOT NULL
           OR provider_request_body_compressed IS NOT NULL
           OR client_response_body_compressed IS NOT NULL
        ORDER BY created_at_unix_ms ASC, request_id ASC
        LIMIT ?
    ) AS batch
)
"#,
        summary,
        limit,
    )
    .await?;
    mysql_execute_batch_if_table(
        tx,
        "usage_http_audits",
        "usage_http_audit_body_refs_cleaned",
        r#"
UPDATE usage_http_audits
SET request_body_ref = NULL,
    provider_request_body_ref = NULL,
    response_body_ref = NULL,
    client_response_body_ref = NULL,
    request_body_state = NULL,
    provider_request_body_state = NULL,
    response_body_state = NULL,
    client_response_body_state = NULL,
    body_capture_mode = 'none'
WHERE request_id IN (
    SELECT request_id FROM (
        SELECT request_id
        FROM usage_http_audits
        WHERE request_body_ref IS NOT NULL
           OR provider_request_body_ref IS NOT NULL
           OR response_body_ref IS NOT NULL
           OR client_response_body_ref IS NOT NULL
           OR request_body_state IS NOT NULL
           OR provider_request_body_state IS NOT NULL
           OR response_body_state IS NOT NULL
           OR client_response_body_state IS NOT NULL
           OR body_capture_mode <> 'none'
        ORDER BY request_id ASC
        LIMIT ?
    ) AS batch
)
"#,
        summary,
        limit,
    )
    .await?;
    Ok(())
}

async fn purge_sqlite_request_bodies_batch(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    batch_size: usize,
    summary: &mut AdminSystemPurgeSummary,
) -> Result<(), DataLayerError> {
    let limit = i64::try_from(batch_size).unwrap_or(i64::MAX);
    sqlite_execute_batch_if_table(
        tx,
        "usage_body_blobs",
        "usage_body_blobs",
        r#"
DELETE FROM usage_body_blobs
WHERE body_ref IN (
    SELECT body_ref
    FROM usage_body_blobs
    ORDER BY body_ref ASC
    LIMIT ?
)
"#,
        summary,
        limit,
    )
    .await?;
    sqlite_execute_batch_if_table(
        tx,
        "usage",
        "usage_body_fields_cleaned",
        r#"
UPDATE "usage"
SET request_body = NULL,
    response_body = NULL,
    provider_request_body = NULL,
    client_response_body = NULL,
    request_body_compressed = NULL,
    response_body_compressed = NULL,
    provider_request_body_compressed = NULL,
    client_response_body_compressed = NULL
WHERE request_id IN (
    SELECT request_id
    FROM "usage"
    WHERE request_body IS NOT NULL
       OR response_body IS NOT NULL
       OR provider_request_body IS NOT NULL
       OR client_response_body IS NOT NULL
       OR request_body_compressed IS NOT NULL
       OR response_body_compressed IS NOT NULL
       OR provider_request_body_compressed IS NOT NULL
       OR client_response_body_compressed IS NOT NULL
    ORDER BY created_at_unix_ms ASC, request_id ASC
    LIMIT ?
)
"#,
        summary,
        limit,
    )
    .await?;
    sqlite_execute_batch_if_table(
        tx,
        "usage_http_audits",
        "usage_http_audit_body_refs_cleaned",
        r#"
UPDATE usage_http_audits
SET request_body_ref = NULL,
    provider_request_body_ref = NULL,
    response_body_ref = NULL,
    client_response_body_ref = NULL,
    request_body_state = NULL,
    provider_request_body_state = NULL,
    response_body_state = NULL,
    client_response_body_state = NULL,
    body_capture_mode = 'none'
WHERE request_id IN (
    SELECT request_id
    FROM usage_http_audits
    WHERE request_body_ref IS NOT NULL
       OR provider_request_body_ref IS NOT NULL
       OR response_body_ref IS NOT NULL
       OR client_response_body_ref IS NOT NULL
       OR request_body_state IS NOT NULL
       OR provider_request_body_state IS NOT NULL
       OR response_body_state IS NOT NULL
       OR client_response_body_state IS NOT NULL
       OR body_capture_mode <> 'none'
    ORDER BY request_id ASC
    LIMIT ?
)
"#,
        summary,
        limit,
    )
    .await?;
    Ok(())
}

async fn pg_delete_table(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    table: &str,
    summary: &mut AdminSystemPurgeSummary,
) -> Result<(), DataLayerError> {
    let table = checked_sql_identifier(table)?;
    if !pg_table_exists(tx, table).await? {
        return Ok(());
    }
    let sql = format!("DELETE FROM public.\"{table}\"");
    let rows = sqlx::query(&sql)
        .execute(&mut **tx)
        .await
        .map_postgres_err()?
        .rows_affected();
    summary.add(table, rows);
    Ok(())
}

async fn pg_delete_non_admin_user_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    table: &str,
    summary: &mut AdminSystemPurgeSummary,
) -> Result<(), DataLayerError> {
    let table = checked_sql_identifier(table)?;
    if !pg_table_exists(tx, table).await? {
        return Ok(());
    }
    let sql = format!(
        "DELETE FROM public.\"{table}\" WHERE user_id IN (SELECT id FROM public.users WHERE COALESCE(role::text, '') <> 'admin')"
    );
    let rows = sqlx::query(&sql)
        .execute(&mut **tx)
        .await
        .map_postgres_err()?
        .rows_affected();
    summary.add(table, rows);
    Ok(())
}

async fn pg_delete_non_admin_api_key_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    table: &str,
    summary: &mut AdminSystemPurgeSummary,
) -> Result<(), DataLayerError> {
    let table = checked_sql_identifier(table)?;
    if !pg_table_exists(tx, table).await? {
        return Ok(());
    }
    let sql = format!(
        "DELETE FROM public.\"{table}\" WHERE api_key_id IN (SELECT id FROM public.api_keys WHERE user_id IN (SELECT id FROM public.users WHERE COALESCE(role::text, '') <> 'admin'))"
    );
    let rows = sqlx::query(&sql)
        .execute(&mut **tx)
        .await
        .map_postgres_err()?
        .rows_affected();
    summary.add(table, rows);
    Ok(())
}

async fn pg_execute_if_table(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    table: &str,
    key: &str,
    sql: &str,
    summary: &mut AdminSystemPurgeSummary,
) -> Result<(), DataLayerError> {
    if !pg_table_exists(tx, checked_sql_identifier(table)?).await? {
        return Ok(());
    }
    let rows = sqlx::query(sql)
        .execute(&mut **tx)
        .await
        .map_postgres_err()?
        .rows_affected();
    summary.add(key, rows);
    Ok(())
}

async fn pg_execute_batch_if_table(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    table: &str,
    key: &str,
    sql: &str,
    summary: &mut AdminSystemPurgeSummary,
    limit: i64,
) -> Result<(), DataLayerError> {
    if !pg_table_exists(tx, checked_sql_identifier(table)?).await? {
        return Ok(());
    }
    let rows = sqlx::query(sql)
        .bind(limit)
        .execute(&mut **tx)
        .await
        .map_postgres_err()?
        .rows_affected();
    summary.add(key, rows);
    Ok(())
}

async fn pg_table_exists(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    table: &str,
) -> Result<bool, DataLayerError> {
    let table = checked_sql_identifier(table)?;
    sqlx::query_scalar::<_, bool>("SELECT to_regclass($1) IS NOT NULL")
        .bind(format!("public.{table}"))
        .fetch_one(&mut **tx)
        .await
        .map_postgres_err()
}

async fn mysql_delete_table(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    table: &str,
    summary: &mut AdminSystemPurgeSummary,
) -> Result<(), DataLayerError> {
    let table = checked_sql_identifier(table)?;
    if !mysql_table_exists(tx, table).await? {
        return Ok(());
    }
    let sql = format!("DELETE FROM `{table}`");
    let rows = sqlx::query(&sql)
        .execute(&mut **tx)
        .await
        .map_sql_err()?
        .rows_affected();
    summary.add(table, rows);
    Ok(())
}

async fn mysql_delete_non_admin_user_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    table: &str,
    summary: &mut AdminSystemPurgeSummary,
) -> Result<(), DataLayerError> {
    let table = checked_sql_identifier(table)?;
    if !mysql_table_exists(tx, table).await? {
        return Ok(());
    }
    let sql = format!(
        "DELETE FROM `{table}` WHERE user_id IN (SELECT id FROM users WHERE LOWER(COALESCE(role, '')) <> 'admin')"
    );
    let rows = sqlx::query(&sql)
        .execute(&mut **tx)
        .await
        .map_sql_err()?
        .rows_affected();
    summary.add(table, rows);
    Ok(())
}

async fn mysql_delete_non_admin_api_key_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    table: &str,
    summary: &mut AdminSystemPurgeSummary,
) -> Result<(), DataLayerError> {
    let table = checked_sql_identifier(table)?;
    if !mysql_table_exists(tx, table).await? {
        return Ok(());
    }
    let sql = format!(
        "DELETE FROM `{table}` WHERE api_key_id IN (SELECT id FROM api_keys WHERE user_id IN (SELECT id FROM users WHERE LOWER(COALESCE(role, '')) <> 'admin'))"
    );
    let rows = sqlx::query(&sql)
        .execute(&mut **tx)
        .await
        .map_sql_err()?
        .rows_affected();
    summary.add(table, rows);
    Ok(())
}

async fn mysql_execute_if_table(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    table: &str,
    key: &str,
    sql: &str,
    summary: &mut AdminSystemPurgeSummary,
) -> Result<(), DataLayerError> {
    if !mysql_table_exists(tx, checked_sql_identifier(table)?).await? {
        return Ok(());
    }
    let rows = sqlx::query(sql)
        .execute(&mut **tx)
        .await
        .map_sql_err()?
        .rows_affected();
    summary.add(key, rows);
    Ok(())
}

async fn mysql_execute_batch_if_table(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    table: &str,
    key: &str,
    sql: &str,
    summary: &mut AdminSystemPurgeSummary,
    limit: i64,
) -> Result<(), DataLayerError> {
    if !mysql_table_exists(tx, checked_sql_identifier(table)?).await? {
        return Ok(());
    }
    let rows = sqlx::query(sql)
        .bind(limit)
        .execute(&mut **tx)
        .await
        .map_sql_err()?
        .rows_affected();
    summary.add(key, rows);
    Ok(())
}

async fn mysql_table_exists(
    tx: &mut sqlx::Transaction<'_, sqlx::MySql>,
    table: &str,
) -> Result<bool, DataLayerError> {
    let table = checked_sql_identifier(table)?;
    let total: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = ?",
    )
    .bind(table)
    .fetch_one(&mut **tx)
    .await
    .map_sql_err()?;
    Ok(total > 0)
}

async fn sqlite_delete_table(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table: &str,
    summary: &mut AdminSystemPurgeSummary,
) -> Result<(), DataLayerError> {
    let table = checked_sql_identifier(table)?;
    if !sqlite_table_exists(tx, table).await? {
        return Ok(());
    }
    let sql = format!("DELETE FROM \"{table}\"");
    let rows = sqlx::query(&sql)
        .execute(&mut **tx)
        .await
        .map_sql_err()?
        .rows_affected();
    summary.add(table, rows);
    Ok(())
}

async fn sqlite_delete_non_admin_user_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table: &str,
    summary: &mut AdminSystemPurgeSummary,
) -> Result<(), DataLayerError> {
    let table = checked_sql_identifier(table)?;
    if !sqlite_table_exists(tx, table).await? {
        return Ok(());
    }
    let sql = format!(
        "DELETE FROM \"{table}\" WHERE user_id IN (SELECT id FROM users WHERE LOWER(COALESCE(role, '')) <> 'admin')"
    );
    let rows = sqlx::query(&sql)
        .execute(&mut **tx)
        .await
        .map_sql_err()?
        .rows_affected();
    summary.add(table, rows);
    Ok(())
}

async fn sqlite_delete_non_admin_api_key_rows(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table: &str,
    summary: &mut AdminSystemPurgeSummary,
) -> Result<(), DataLayerError> {
    let table = checked_sql_identifier(table)?;
    if !sqlite_table_exists(tx, table).await? {
        return Ok(());
    }
    let sql = format!(
        "DELETE FROM \"{table}\" WHERE api_key_id IN (SELECT id FROM api_keys WHERE user_id IN (SELECT id FROM users WHERE LOWER(COALESCE(role, '')) <> 'admin'))"
    );
    let rows = sqlx::query(&sql)
        .execute(&mut **tx)
        .await
        .map_sql_err()?
        .rows_affected();
    summary.add(table, rows);
    Ok(())
}

async fn sqlite_execute_if_table(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table: &str,
    key: &str,
    sql: &str,
    summary: &mut AdminSystemPurgeSummary,
) -> Result<(), DataLayerError> {
    if !sqlite_table_exists(tx, checked_sql_identifier(table)?).await? {
        return Ok(());
    }
    let rows = sqlx::query(sql)
        .execute(&mut **tx)
        .await
        .map_sql_err()?
        .rows_affected();
    summary.add(key, rows);
    Ok(())
}

async fn sqlite_execute_batch_if_table(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table: &str,
    key: &str,
    sql: &str,
    summary: &mut AdminSystemPurgeSummary,
    limit: i64,
) -> Result<(), DataLayerError> {
    if !sqlite_table_exists(tx, checked_sql_identifier(table)?).await? {
        return Ok(());
    }
    let rows = sqlx::query(sql)
        .bind(limit)
        .execute(&mut **tx)
        .await
        .map_sql_err()?
        .rows_affected();
    summary.add(key, rows);
    Ok(())
}

async fn sqlite_table_exists(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    table: &str,
) -> Result<bool, DataLayerError> {
    let table = checked_sql_identifier(table)?;
    let total: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?")
            .bind(table)
            .fetch_one(&mut **tx)
            .await
            .map_sql_err()?;
    Ok(total > 0)
}

fn current_unix_secs() -> u64 {
    chrono::Utc::now().timestamp().max(0) as u64
}

fn serialize_json_value(value: &serde_json::Value) -> Result<String, DataLayerError> {
    serde_json::to_string(value).map_err(|err| {
        DataLayerError::UnexpectedValue(format!("invalid system config JSON value: {err}"))
    })
}

fn parse_json_value(value: String) -> Result<serde_json::Value, DataLayerError> {
    serde_json::from_str(&value).map_err(|err| {
        DataLayerError::UnexpectedValue(format!("invalid system config JSON value: {err}"))
    })
}

fn postgres_admin_system_stats(
    row: sqlx::postgres::PgRow,
) -> Result<AdminSystemStats, DataLayerError> {
    Ok(AdminSystemStats {
        total_users: row
            .try_get::<i64, _>("total_users")
            .map_postgres_err()?
            .max(0) as u64,
        active_users: row
            .try_get::<i64, _>("active_users")
            .map_postgres_err()?
            .max(0) as u64,
        total_api_keys: row
            .try_get::<i64, _>("total_api_keys")
            .map_postgres_err()?
            .max(0) as u64,
        total_requests: row
            .try_get::<i64, _>("total_requests")
            .map_postgres_err()?
            .max(0) as u64,
    })
}

fn mysql_admin_system_stats(
    row: sqlx::mysql::MySqlRow,
) -> Result<AdminSystemStats, DataLayerError> {
    Ok(AdminSystemStats {
        total_users: row.try_get::<i64, _>("total_users").map_sql_err()?.max(0) as u64,
        active_users: row.try_get::<i64, _>("active_users").map_sql_err()?.max(0) as u64,
        total_api_keys: row
            .try_get::<i64, _>("total_api_keys")
            .map_sql_err()?
            .max(0) as u64,
        total_requests: row
            .try_get::<i64, _>("total_requests")
            .map_sql_err()?
            .max(0) as u64,
    })
}

fn sqlite_admin_system_stats(
    row: sqlx::sqlite::SqliteRow,
) -> Result<AdminSystemStats, DataLayerError> {
    Ok(AdminSystemStats {
        total_users: row.try_get::<i64, _>("total_users").map_sql_err()?.max(0) as u64,
        active_users: row.try_get::<i64, _>("active_users").map_sql_err()?.max(0) as u64,
        total_api_keys: row
            .try_get::<i64, _>("total_api_keys")
            .map_sql_err()?
            .max(0) as u64,
        total_requests: row
            .try_get::<i64, _>("total_requests")
            .map_sql_err()?
            .max(0) as u64,
    })
}
