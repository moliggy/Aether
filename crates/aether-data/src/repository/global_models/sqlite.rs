use async_trait::async_trait;
use sqlx::{sqlite::SqliteRow, Row};

use super::{
    metadata_supports_embedding, AdminGlobalModelListQuery, AdminProviderModelListQuery,
    CreateAdminGlobalModelRecord, GlobalModelReadRepository, GlobalModelWriteRepository,
    InMemoryGlobalModelReadRepository, PublicCatalogModelListQuery, PublicCatalogModelSearchQuery,
    PublicGlobalModelQuery, StoredAdminGlobalModel, StoredAdminGlobalModelPage,
    StoredAdminProviderModel, StoredProviderActiveGlobalModel, StoredProviderModelStats,
    StoredPublicCatalogModel, StoredPublicGlobalModel, StoredPublicGlobalModelPage,
    UpdateAdminGlobalModelRecord, UpsertAdminProviderModelRecord,
};
use crate::driver::sqlite::SqlitePool;
use crate::error::SqlResultExt;
use crate::DataLayerError;

#[derive(Debug, Clone)]
pub struct SqliteGlobalModelReadRepository {
    pool: SqlitePool,
}

impl SqliteGlobalModelReadRepository {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    async fn load_memory(&self) -> Result<InMemoryGlobalModelReadRepository, DataLayerError> {
        let public_models = self.load_public_global_models().await?;
        let admin_global_models = self.load_admin_global_models().await?;
        let admin_provider_models = self.load_admin_provider_models().await?;
        let public_catalog_models = self.load_public_catalog_models().await?;
        let provider_model_stats = self.load_provider_model_stats().await?;
        let active_global_model_refs = self.load_active_global_model_refs().await?;

        Ok(InMemoryGlobalModelReadRepository::seed(public_models)
            .with_admin_global_models(admin_global_models)
            .with_admin_provider_models(admin_provider_models)
            .with_public_catalog_models(public_catalog_models)
            .with_provider_model_stats(provider_model_stats)
            .with_active_global_model_refs(active_global_model_refs))
    }

    async fn load_public_global_models(
        &self,
    ) -> Result<Vec<StoredPublicGlobalModel>, DataLayerError> {
        let rows = sqlx::query(
            r#"
SELECT
  id,
  name,
  display_name,
  is_active,
  default_price_per_request,
  default_tiered_pricing,
  supported_capabilities,
  config,
  usage_count
FROM global_models
"#,
        )
        .fetch_all(&self.pool)
        .await
        .map_sql_err()?;
        rows.iter().map(map_public_global_model_row).collect()
    }

    async fn load_admin_global_models(
        &self,
    ) -> Result<Vec<StoredAdminGlobalModel>, DataLayerError> {
        let rows = sqlx::query(
            r#"
SELECT
  id,
  name,
  COALESCE(NULLIF(display_name, ''), name) AS display_name,
  is_active,
  default_price_per_request,
  default_tiered_pricing,
  supported_capabilities,
  config,
  usage_count,
  created_at AS created_at_unix_ms,
  updated_at AS updated_at_unix_secs
FROM global_models
"#,
        )
        .fetch_all(&self.pool)
        .await
        .map_sql_err()?;
        rows.iter().map(map_admin_global_model_row).collect()
    }

    async fn load_admin_provider_models(
        &self,
    ) -> Result<Vec<StoredAdminProviderModel>, DataLayerError> {
        let rows = sqlx::query(
            r#"
SELECT
  m.id,
  m.provider_id,
  m.global_model_id,
  m.provider_model_name,
  m.provider_model_mappings,
  m.price_per_request,
  m.tiered_pricing,
  m.supports_vision,
  m.supports_function_calling,
  m.supports_streaming,
  m.supports_extended_thinking,
  m.supports_image_generation,
  m.is_active,
  m.is_available,
  m.config,
  m.created_at AS created_at_unix_ms,
  m.updated_at AS updated_at_unix_secs,
  gm.name AS global_model_name,
  gm.display_name AS global_model_display_name,
  gm.default_price_per_request AS global_model_default_price_per_request,
  gm.default_tiered_pricing AS global_model_default_tiered_pricing,
  gm.supported_capabilities AS global_model_supported_capabilities,
  gm.config AS global_model_config
FROM models m
LEFT JOIN global_models gm ON gm.id = m.global_model_id
WHERE m.global_model_id IS NOT NULL
"#,
        )
        .fetch_all(&self.pool)
        .await
        .map_sql_err()?;
        rows.iter().map(map_admin_provider_model_row).collect()
    }

    async fn load_public_catalog_models(
        &self,
    ) -> Result<Vec<StoredPublicCatalogModel>, DataLayerError> {
        let rows = sqlx::query(
            r#"
SELECT
  m.id,
  m.provider_id,
  p.name AS provider_name,
  p.is_active AS provider_is_active,
  m.provider_model_name,
  COALESCE(gm.name, m.provider_model_name) AS name,
  COALESCE(NULLIF(gm.display_name, ''), m.provider_model_name) AS display_name,
  gm.config AS global_model_config,
  gm.supported_capabilities AS global_model_supported_capabilities,
  m.config AS model_config,
  m.tiered_pricing,
  gm.default_tiered_pricing,
  m.supports_vision,
  m.supports_function_calling,
  m.supports_streaming,
  m.is_active,
  gm.is_active AS global_model_is_active
FROM models m
JOIN providers p ON p.id = m.provider_id
LEFT JOIN global_models gm ON gm.id = m.global_model_id
"#,
        )
        .fetch_all(&self.pool)
        .await
        .map_sql_err()?;
        rows.iter().map(map_public_catalog_model_row).collect()
    }

    async fn load_provider_model_stats(
        &self,
    ) -> Result<Vec<StoredProviderModelStats>, DataLayerError> {
        let rows = sqlx::query(
            r#"
SELECT
  provider_id,
  COUNT(id) AS total_models,
  SUM(CASE WHEN is_active = 1 THEN 1 ELSE 0 END) AS active_models
FROM models
GROUP BY provider_id
ORDER BY provider_id ASC
"#,
        )
        .fetch_all(&self.pool)
        .await
        .map_sql_err()?;
        rows.iter().map(map_provider_model_stats_row).collect()
    }

    async fn load_active_global_model_refs(
        &self,
    ) -> Result<Vec<StoredProviderActiveGlobalModel>, DataLayerError> {
        let rows = sqlx::query(
            r#"
SELECT DISTINCT provider_id, global_model_id
FROM models
WHERE is_active = 1
  AND global_model_id IS NOT NULL
ORDER BY provider_id ASC, global_model_id ASC
"#,
        )
        .fetch_all(&self.pool)
        .await
        .map_sql_err()?;
        rows.iter().map(map_active_global_model_row).collect()
    }

    pub async fn create_admin_provider_model(
        &self,
        record: &UpsertAdminProviderModelRecord,
    ) -> Result<Option<StoredAdminProviderModel>, DataLayerError> {
        let now = current_unix_secs();
        sqlx::query(
            r#"
INSERT INTO models (
  id,
  provider_id,
  global_model_id,
  provider_model_name,
  provider_model_mappings,
  price_per_request,
  tiered_pricing,
  supports_vision,
  supports_function_calling,
  supports_streaming,
  supports_extended_thinking,
  supports_image_generation,
  is_active,
  is_available,
  config,
  created_at,
  updated_at
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"#,
        )
        .bind(&record.id)
        .bind(&record.provider_id)
        .bind(&record.global_model_id)
        .bind(&record.provider_model_name)
        .bind(optional_json_to_string(
            &record.provider_model_mappings,
            "models.provider_model_mappings",
        )?)
        .bind(record.price_per_request)
        .bind(optional_json_to_string(
            &record.tiered_pricing,
            "models.tiered_pricing",
        )?)
        .bind(record.supports_vision)
        .bind(record.supports_function_calling)
        .bind(record.supports_streaming)
        .bind(record.supports_extended_thinking)
        .bind(record.supports_image_generation)
        .bind(record.is_active)
        .bind(record.is_available)
        .bind(optional_json_to_string(&record.config, "models.config")?)
        .bind(now as i64)
        .bind(now as i64)
        .execute(&self.pool)
        .await
        .map_sql_err()?;

        self.get_admin_provider_model(&record.provider_id, &record.id)
            .await
    }

    pub async fn update_admin_provider_model(
        &self,
        record: &UpsertAdminProviderModelRecord,
    ) -> Result<Option<StoredAdminProviderModel>, DataLayerError> {
        let now = current_unix_secs();
        let updated = sqlx::query(
            r#"
UPDATE models
SET
  global_model_id = ?,
  provider_model_name = ?,
  provider_model_mappings = ?,
  price_per_request = ?,
  tiered_pricing = ?,
  supports_vision = ?,
  supports_function_calling = ?,
  supports_streaming = ?,
  supports_extended_thinking = ?,
  supports_image_generation = ?,
  is_active = ?,
  is_available = ?,
  config = ?,
  updated_at = ?
WHERE id = ?
  AND provider_id = ?
"#,
        )
        .bind(&record.global_model_id)
        .bind(&record.provider_model_name)
        .bind(optional_json_to_string(
            &record.provider_model_mappings,
            "models.provider_model_mappings",
        )?)
        .bind(record.price_per_request)
        .bind(optional_json_to_string(
            &record.tiered_pricing,
            "models.tiered_pricing",
        )?)
        .bind(record.supports_vision)
        .bind(record.supports_function_calling)
        .bind(record.supports_streaming)
        .bind(record.supports_extended_thinking)
        .bind(record.supports_image_generation)
        .bind(record.is_active)
        .bind(record.is_available)
        .bind(optional_json_to_string(&record.config, "models.config")?)
        .bind(now as i64)
        .bind(&record.id)
        .bind(&record.provider_id)
        .execute(&self.pool)
        .await
        .map_sql_err()?;

        if updated.rows_affected() == 0 {
            return Ok(None);
        }

        self.get_admin_provider_model(&record.provider_id, &record.id)
            .await
    }

    pub async fn delete_admin_provider_model(
        &self,
        provider_id: &str,
        model_id: &str,
    ) -> Result<bool, DataLayerError> {
        let deleted = sqlx::query(
            r#"
DELETE FROM models
WHERE provider_id = ?
  AND id = ?
"#,
        )
        .bind(provider_id)
        .bind(model_id)
        .execute(&self.pool)
        .await
        .map_sql_err()?;

        Ok(deleted.rows_affected() > 0)
    }

    pub async fn create_admin_global_model(
        &self,
        record: &CreateAdminGlobalModelRecord,
    ) -> Result<Option<StoredAdminGlobalModel>, DataLayerError> {
        let now = current_unix_secs();
        sqlx::query(
            r#"
INSERT INTO global_models (
  id,
  name,
  display_name,
  is_active,
  default_price_per_request,
  default_tiered_pricing,
  supported_capabilities,
  usage_count,
  config,
  created_at,
  updated_at
)
VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)
"#,
        )
        .bind(&record.id)
        .bind(&record.name)
        .bind(&record.display_name)
        .bind(record.is_active)
        .bind(record.default_price_per_request)
        .bind(optional_json_to_string(
            &record.default_tiered_pricing,
            "global_models.default_tiered_pricing",
        )?)
        .bind(optional_json_to_string(
            &record.supported_capabilities,
            "global_models.supported_capabilities",
        )?)
        .bind(optional_json_to_string(
            &record.config,
            "global_models.config",
        )?)
        .bind(now as i64)
        .bind(now as i64)
        .execute(&self.pool)
        .await
        .map_sql_err()?;

        self.get_admin_global_model_by_id(&record.id).await
    }

    pub async fn update_admin_global_model(
        &self,
        record: &UpdateAdminGlobalModelRecord,
    ) -> Result<Option<StoredAdminGlobalModel>, DataLayerError> {
        let now = current_unix_secs();
        let updated = sqlx::query(
            r#"
UPDATE global_models
SET
  display_name = ?,
  is_active = ?,
  default_price_per_request = ?,
  default_tiered_pricing = ?,
  supported_capabilities = ?,
  config = ?,
  updated_at = ?
WHERE id = ?
"#,
        )
        .bind(&record.display_name)
        .bind(record.is_active)
        .bind(record.default_price_per_request)
        .bind(optional_json_to_string(
            &record.default_tiered_pricing,
            "global_models.default_tiered_pricing",
        )?)
        .bind(optional_json_to_string(
            &record.supported_capabilities,
            "global_models.supported_capabilities",
        )?)
        .bind(optional_json_to_string(
            &record.config,
            "global_models.config",
        )?)
        .bind(now as i64)
        .bind(&record.id)
        .execute(&self.pool)
        .await
        .map_sql_err()?;

        if updated.rows_affected() == 0 {
            return Ok(None);
        }

        self.get_admin_global_model_by_id(&record.id).await
    }

    pub async fn delete_admin_global_model(
        &self,
        global_model_id: &str,
    ) -> Result<bool, DataLayerError> {
        let mut tx = self.pool.begin().await.map_sql_err()?;

        sqlx::query(
            r#"
DELETE FROM models
WHERE global_model_id = ?
"#,
        )
        .bind(global_model_id)
        .execute(&mut *tx)
        .await
        .map_sql_err()?;

        let deleted = sqlx::query(
            r#"
DELETE FROM global_models
WHERE id = ?
"#,
        )
        .bind(global_model_id)
        .execute(&mut *tx)
        .await
        .map_sql_err()?;

        tx.commit().await.map_sql_err()?;

        Ok(deleted.rows_affected() > 0)
    }
}

#[async_trait]
impl GlobalModelReadRepository for SqliteGlobalModelReadRepository {
    async fn list_public_models(
        &self,
        query: &PublicGlobalModelQuery,
    ) -> Result<StoredPublicGlobalModelPage, DataLayerError> {
        self.load_memory().await?.list_public_models(query).await
    }

    async fn get_public_model_by_name(
        &self,
        model_name: &str,
    ) -> Result<Option<StoredPublicGlobalModel>, DataLayerError> {
        self.load_memory()
            .await?
            .get_public_model_by_name(model_name)
            .await
    }

    async fn list_public_catalog_models(
        &self,
        query: &PublicCatalogModelListQuery,
    ) -> Result<Vec<StoredPublicCatalogModel>, DataLayerError> {
        self.load_memory()
            .await?
            .list_public_catalog_models(query)
            .await
    }

    async fn search_public_catalog_models(
        &self,
        query: &PublicCatalogModelSearchQuery,
    ) -> Result<Vec<StoredPublicCatalogModel>, DataLayerError> {
        self.load_memory()
            .await?
            .search_public_catalog_models(query)
            .await
    }

    async fn list_admin_global_models(
        &self,
        query: &AdminGlobalModelListQuery,
    ) -> Result<StoredAdminGlobalModelPage, DataLayerError> {
        self.load_memory()
            .await?
            .list_admin_global_models(query)
            .await
    }

    async fn list_admin_provider_models(
        &self,
        query: &AdminProviderModelListQuery,
    ) -> Result<Vec<StoredAdminProviderModel>, DataLayerError> {
        self.load_memory()
            .await?
            .list_admin_provider_models(query)
            .await
    }

    async fn list_admin_provider_available_source_models(
        &self,
        provider_id: &str,
    ) -> Result<Vec<StoredAdminProviderModel>, DataLayerError> {
        self.load_memory()
            .await?
            .list_admin_provider_available_source_models(provider_id)
            .await
    }

    async fn get_admin_provider_model(
        &self,
        provider_id: &str,
        model_id: &str,
    ) -> Result<Option<StoredAdminProviderModel>, DataLayerError> {
        self.load_memory()
            .await?
            .get_admin_provider_model(provider_id, model_id)
            .await
    }

    async fn get_admin_global_model_by_id(
        &self,
        global_model_id: &str,
    ) -> Result<Option<StoredAdminGlobalModel>, DataLayerError> {
        self.load_memory()
            .await?
            .get_admin_global_model_by_id(global_model_id)
            .await
    }

    async fn get_admin_global_model_by_name(
        &self,
        model_name: &str,
    ) -> Result<Option<StoredAdminGlobalModel>, DataLayerError> {
        self.load_memory()
            .await?
            .get_admin_global_model_by_name(model_name)
            .await
    }

    async fn list_admin_provider_models_by_global_model_id(
        &self,
        global_model_id: &str,
    ) -> Result<Vec<StoredAdminProviderModel>, DataLayerError> {
        self.load_memory()
            .await?
            .list_admin_provider_models_by_global_model_id(global_model_id)
            .await
    }

    async fn list_provider_model_stats(
        &self,
        provider_ids: &[String],
    ) -> Result<Vec<StoredProviderModelStats>, DataLayerError> {
        self.load_memory()
            .await?
            .list_provider_model_stats(provider_ids)
            .await
    }

    async fn list_active_global_model_ids_by_provider_ids(
        &self,
        provider_ids: &[String],
    ) -> Result<Vec<StoredProviderActiveGlobalModel>, DataLayerError> {
        self.load_memory()
            .await?
            .list_active_global_model_ids_by_provider_ids(provider_ids)
            .await
    }
}

#[async_trait]
impl GlobalModelWriteRepository for SqliteGlobalModelReadRepository {
    async fn create_admin_provider_model(
        &self,
        record: &UpsertAdminProviderModelRecord,
    ) -> Result<Option<StoredAdminProviderModel>, DataLayerError> {
        Self::create_admin_provider_model(self, record).await
    }

    async fn update_admin_provider_model(
        &self,
        record: &UpsertAdminProviderModelRecord,
    ) -> Result<Option<StoredAdminProviderModel>, DataLayerError> {
        Self::update_admin_provider_model(self, record).await
    }

    async fn delete_admin_provider_model(
        &self,
        provider_id: &str,
        model_id: &str,
    ) -> Result<bool, DataLayerError> {
        Self::delete_admin_provider_model(self, provider_id, model_id).await
    }

    async fn create_admin_global_model(
        &self,
        record: &CreateAdminGlobalModelRecord,
    ) -> Result<Option<StoredAdminGlobalModel>, DataLayerError> {
        Self::create_admin_global_model(self, record).await
    }

    async fn update_admin_global_model(
        &self,
        record: &UpdateAdminGlobalModelRecord,
    ) -> Result<Option<StoredAdminGlobalModel>, DataLayerError> {
        Self::update_admin_global_model(self, record).await
    }

    async fn delete_admin_global_model(
        &self,
        global_model_id: &str,
    ) -> Result<bool, DataLayerError> {
        Self::delete_admin_global_model(self, global_model_id).await
    }
}

fn current_unix_secs() -> u64 {
    chrono::Utc::now().timestamp().max(0) as u64
}

fn optional_json_to_string(
    value: &Option<serde_json::Value>,
    field_name: &str,
) -> Result<Option<String>, DataLayerError> {
    value
        .as_ref()
        .map(|value| {
            serde_json::to_string(value).map_err(|err| {
                DataLayerError::UnexpectedValue(format!(
                    "{field_name} contains unserializable JSON: {err}"
                ))
            })
        })
        .transpose()
}

fn optional_json_from_string(
    value: Option<String>,
    field_name: &str,
) -> Result<Option<serde_json::Value>, DataLayerError> {
    value
        .map(|value| {
            serde_json::from_str(&value).map_err(|err| {
                DataLayerError::UnexpectedValue(format!(
                    "{field_name} contains invalid JSON: {err}"
                ))
            })
        })
        .transpose()
}

fn optional_u64(value: Option<i64>, field_name: &str) -> Result<Option<u64>, DataLayerError> {
    value
        .map(|value| {
            u64::try_from(value).map_err(|_| {
                DataLayerError::UnexpectedValue(format!("invalid {field_name}: {value}"))
            })
        })
        .transpose()
}

fn first_tier_price(value: Option<&serde_json::Value>, key: &str) -> Option<f64> {
    value
        .and_then(|value| value.get("tiers"))
        .and_then(serde_json::Value::as_array)
        .and_then(|tiers| tiers.first())
        .and_then(|tier| tier.get(key))
        .and_then(serde_json::Value::as_f64)
}

fn map_public_global_model_row(row: &SqliteRow) -> Result<StoredPublicGlobalModel, DataLayerError> {
    StoredPublicGlobalModel::new(
        row.try_get("id").map_sql_err()?,
        row.try_get("name").map_sql_err()?,
        row.try_get("display_name").map_sql_err()?,
        row.try_get("is_active").map_sql_err()?,
        row.try_get("default_price_per_request").map_sql_err()?,
        optional_json_from_string(
            row.try_get("default_tiered_pricing").map_sql_err()?,
            "global_models.default_tiered_pricing",
        )?,
        optional_json_from_string(
            row.try_get("supported_capabilities").map_sql_err()?,
            "global_models.supported_capabilities",
        )?,
        optional_json_from_string(row.try_get("config").map_sql_err()?, "global_models.config")?,
        row.try_get::<i64, _>("usage_count").map_sql_err()?.max(0) as u64,
    )
}

fn map_admin_global_model_row(row: &SqliteRow) -> Result<StoredAdminGlobalModel, DataLayerError> {
    StoredAdminGlobalModel::new(
        row.try_get("id").map_sql_err()?,
        row.try_get("name").map_sql_err()?,
        row.try_get("display_name").map_sql_err()?,
        row.try_get("is_active").map_sql_err()?,
        row.try_get("default_price_per_request").map_sql_err()?,
        optional_json_from_string(
            row.try_get("default_tiered_pricing").map_sql_err()?,
            "global_models.default_tiered_pricing",
        )?,
        optional_json_from_string(
            row.try_get("supported_capabilities").map_sql_err()?,
            "global_models.supported_capabilities",
        )?,
        optional_json_from_string(row.try_get("config").map_sql_err()?, "global_models.config")?,
        0,
        0,
        row.try_get::<i64, _>("usage_count").map_sql_err()?.max(0) as u64,
        optional_u64(
            row.try_get("created_at_unix_ms").map_sql_err()?,
            "global_models.created_at",
        )?,
        optional_u64(
            row.try_get("updated_at_unix_secs").map_sql_err()?,
            "global_models.updated_at",
        )?,
    )
}

fn map_admin_provider_model_row(
    row: &SqliteRow,
) -> Result<StoredAdminProviderModel, DataLayerError> {
    StoredAdminProviderModel::new(
        row.try_get("id").map_sql_err()?,
        row.try_get("provider_id").map_sql_err()?,
        row.try_get("global_model_id").map_sql_err()?,
        row.try_get("provider_model_name").map_sql_err()?,
        optional_json_from_string(
            row.try_get("provider_model_mappings").map_sql_err()?,
            "models.provider_model_mappings",
        )?,
        row.try_get("price_per_request").map_sql_err()?,
        optional_json_from_string(
            row.try_get("tiered_pricing").map_sql_err()?,
            "models.tiered_pricing",
        )?,
        row.try_get("supports_vision").map_sql_err()?,
        row.try_get("supports_function_calling").map_sql_err()?,
        row.try_get("supports_streaming").map_sql_err()?,
        row.try_get("supports_extended_thinking").map_sql_err()?,
        row.try_get("supports_image_generation").map_sql_err()?,
        row.try_get("is_active").map_sql_err()?,
        row.try_get("is_available").map_sql_err()?,
        optional_json_from_string(row.try_get("config").map_sql_err()?, "models.config")?,
        optional_u64(
            row.try_get("created_at_unix_ms").map_sql_err()?,
            "models.created_at",
        )?,
        optional_u64(
            row.try_get("updated_at_unix_secs").map_sql_err()?,
            "models.updated_at",
        )?,
        row.try_get("global_model_name").map_sql_err()?,
        row.try_get("global_model_display_name").map_sql_err()?,
        row.try_get("global_model_default_price_per_request")
            .map_sql_err()?,
        optional_json_from_string(
            row.try_get("global_model_default_tiered_pricing")
                .map_sql_err()?,
            "global_models.default_tiered_pricing",
        )?,
        optional_json_from_string(
            row.try_get("global_model_supported_capabilities")
                .map_sql_err()?,
            "global_models.supported_capabilities",
        )?,
        optional_json_from_string(
            row.try_get("global_model_config").map_sql_err()?,
            "global_models.config",
        )?,
    )
}

fn map_public_catalog_model_row(
    row: &SqliteRow,
) -> Result<StoredPublicCatalogModel, DataLayerError> {
    let global_model_config = optional_json_from_string(
        row.try_get("global_model_config").map_sql_err()?,
        "global_models.config",
    )?;
    let global_model_supported_capabilities = optional_json_from_string(
        row.try_get("global_model_supported_capabilities")
            .map_sql_err()?,
        "global_models.supported_capabilities",
    )?;
    let model_config =
        optional_json_from_string(row.try_get("model_config").map_sql_err()?, "models.config")?;
    let tiered_pricing = optional_json_from_string(
        row.try_get("tiered_pricing").map_sql_err()?,
        "models.tiered_pricing",
    )?;
    let default_tiered_pricing = optional_json_from_string(
        row.try_get("default_tiered_pricing").map_sql_err()?,
        "global_models.default_tiered_pricing",
    )?;
    let pricing = tiered_pricing.as_ref().or(default_tiered_pricing.as_ref());
    let global_model_is_active = row
        .try_get::<Option<bool>, _>("global_model_is_active")
        .map_sql_err()?
        .unwrap_or(true);
    let model_is_active: bool = row.try_get("is_active").map_sql_err()?;
    let provider_is_active: bool = row.try_get("provider_is_active").map_sql_err()?;

    StoredPublicCatalogModel::new(
        row.try_get("id").map_sql_err()?,
        row.try_get("provider_id").map_sql_err()?,
        row.try_get("provider_name").map_sql_err()?,
        row.try_get("provider_model_name").map_sql_err()?,
        row.try_get("name").map_sql_err()?,
        row.try_get("display_name").map_sql_err()?,
        global_model_config
            .as_ref()
            .and_then(|value| value.get("description"))
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string),
        global_model_config
            .as_ref()
            .and_then(|value| value.get("icon_url"))
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string),
        first_tier_price(pricing, "input_price_per_1m"),
        first_tier_price(pricing, "output_price_per_1m"),
        first_tier_price(pricing, "cache_creation_price_per_1m"),
        first_tier_price(pricing, "cache_read_price_per_1m"),
        row.try_get("supports_vision").map_sql_err()?,
        row.try_get("supports_function_calling").map_sql_err()?,
        row.try_get("supports_streaming").map_sql_err()?,
        metadata_supports_embedding(
            global_model_supported_capabilities.as_ref(),
            global_model_config.as_ref(),
            model_config.as_ref(),
        ),
        model_is_active && provider_is_active && global_model_is_active,
    )
}

fn map_provider_model_stats_row(
    row: &SqliteRow,
) -> Result<StoredProviderModelStats, DataLayerError> {
    StoredProviderModelStats::new(
        row.try_get("provider_id").map_sql_err()?,
        row.try_get("total_models").map_sql_err()?,
        row.try_get::<Option<i64>, _>("active_models")
            .map_sql_err()?
            .unwrap_or(0),
    )
}

fn map_active_global_model_row(
    row: &SqliteRow,
) -> Result<StoredProviderActiveGlobalModel, DataLayerError> {
    StoredProviderActiveGlobalModel::new(
        row.try_get("provider_id").map_sql_err()?,
        row.try_get("global_model_id").map_sql_err()?,
    )
}

#[cfg(test)]
mod tests {
    use super::SqliteGlobalModelReadRepository;
    use crate::lifecycle::migrate::run_sqlite_migrations;
    use crate::repository::global_models::{
        AdminGlobalModelListQuery, AdminProviderModelListQuery, CreateAdminGlobalModelRecord,
        GlobalModelReadRepository, PublicCatalogModelSearchQuery, PublicGlobalModelQuery,
        UpdateAdminGlobalModelRecord, UpsertAdminProviderModelRecord,
    };
    use serde_json::json;

    #[tokio::test]
    async fn sqlite_repository_reads_global_model_contract_views() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite pool should connect");
        run_sqlite_migrations(&pool)
            .await
            .expect("sqlite migrations should run");
        seed_rows(&pool).await;

        let repository = SqliteGlobalModelReadRepository::new(pool);
        let public = repository
            .list_public_models(&PublicGlobalModelQuery {
                offset: 0,
                limit: 10,
                is_active: Some(true),
                search: Some("gpt".to_string()),
            })
            .await
            .expect("public models should load");
        assert_eq!(public.total, 1);
        assert_eq!(public.items[0].name, "gpt-4.1");

        let catalog = repository
            .search_public_catalog_models(&PublicCatalogModelSearchQuery {
                search: "provider".to_string(),
                provider_id: Some("provider-1".to_string()),
                limit: 10,
            })
            .await
            .expect("catalog search should load");
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog[0].input_price_per_1m, Some(2.0));

        let admin_globals = repository
            .list_admin_global_models(&AdminGlobalModelListQuery {
                offset: 0,
                limit: 10,
                is_active: None,
                search: None,
            })
            .await
            .expect("admin globals should load");
        assert_eq!(admin_globals.total, 1);
        assert_eq!(admin_globals.items[0].provider_count, 1);

        let admin_models = repository
            .list_admin_provider_models(&AdminProviderModelListQuery {
                provider_id: "provider-1".to_string(),
                is_active: Some(true),
                offset: 0,
                limit: 10,
            })
            .await
            .expect("admin provider models should load");
        assert_eq!(admin_models.len(), 1);
        assert_eq!(
            admin_models[0].global_model_name,
            Some("gpt-4.1".to_string())
        );

        let stats = repository
            .list_provider_model_stats(&["provider-1".to_string()])
            .await
            .expect("stats should load");
        assert_eq!(stats[0].total_models, 1);
        assert_eq!(stats[0].active_models, 1);

        let refs = repository
            .list_active_global_model_ids_by_provider_ids(&["provider-1".to_string()])
            .await
            .expect("active refs should load");
        assert_eq!(refs[0].global_model_id, "global-1");
    }

    #[tokio::test]
    async fn sqlite_repository_writes_global_models_and_provider_models() {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("sqlite pool should connect");
        run_sqlite_migrations(&pool)
            .await
            .expect("sqlite migrations should run");
        seed_provider(&pool).await;

        let repository = SqliteGlobalModelReadRepository::new(pool);
        let created_global = repository
            .create_admin_global_model(
                &CreateAdminGlobalModelRecord::new(
                    "global-write-1".to_string(),
                    "claude-3.7".to_string(),
                    "Claude 3.7".to_string(),
                    true,
                    Some(0.25),
                    Some(json!({"tiers":[{"input_price_per_1m":3.0}]})),
                    Some(json!(["chat", "vision"])),
                    Some(json!({"description":"write path"})),
                )
                .expect("create global input should validate"),
            )
            .await
            .expect("global model should create")
            .expect("created global model should return");
        assert_eq!(created_global.name, "claude-3.7");
        assert_eq!(
            created_global.supported_capabilities,
            Some(json!(["chat", "vision"]))
        );

        let updated_global = repository
            .update_admin_global_model(
                &UpdateAdminGlobalModelRecord::new(
                    "global-write-1".to_string(),
                    "Claude 3.7 Sonnet".to_string(),
                    false,
                    Some(0.35),
                    None,
                    Some(json!(["chat"])),
                    Some(json!({"description":"updated"})),
                )
                .expect("update global input should validate"),
            )
            .await
            .expect("global model should update")
            .expect("updated global model should return");
        assert_eq!(updated_global.display_name, "Claude 3.7 Sonnet");
        assert!(!updated_global.is_active);

        let created_provider_model = repository
            .create_admin_provider_model(
                &UpsertAdminProviderModelRecord::new(
                    "model-write-1".to_string(),
                    "provider-1".to_string(),
                    "global-write-1".to_string(),
                    "provider-claude-3.7".to_string(),
                    Some(json!(["claude-3.7", "claude-sonnet"])),
                    Some(0.75),
                    Some(json!({"tiers":[{"output_price_per_1m":15.0}]})),
                    Some(true),
                    Some(true),
                    Some(true),
                    Some(false),
                    Some(false),
                    true,
                    true,
                    Some(json!({"routing":"primary"})),
                )
                .expect("create provider model input should validate"),
            )
            .await
            .expect("provider model should create")
            .expect("created provider model should return");
        assert_eq!(
            created_provider_model.global_model_name,
            Some("claude-3.7".to_string())
        );
        assert_eq!(
            created_provider_model.provider_model_mappings,
            Some(json!(["claude-3.7", "claude-sonnet"]))
        );

        let updated_provider_model = repository
            .update_admin_provider_model(
                &UpsertAdminProviderModelRecord::new(
                    "model-write-1".to_string(),
                    "provider-1".to_string(),
                    "global-write-1".to_string(),
                    "provider-claude-3.7-v2".to_string(),
                    None,
                    Some(0.95),
                    None,
                    Some(false),
                    Some(true),
                    Some(false),
                    Some(true),
                    Some(false),
                    false,
                    true,
                    Some(json!({"routing":"secondary"})),
                )
                .expect("update provider model input should validate"),
            )
            .await
            .expect("provider model should update")
            .expect("updated provider model should return");
        assert_eq!(
            updated_provider_model.provider_model_name,
            "provider-claude-3.7-v2"
        );
        assert!(!updated_provider_model.is_active);

        assert!(repository
            .delete_admin_global_model("global-write-1")
            .await
            .expect("global model should delete"));
        assert!(repository
            .get_admin_provider_model("provider-1", "model-write-1")
            .await
            .expect("deleted provider model lookup should succeed")
            .is_none());
    }

    async fn seed_rows(pool: &sqlx::SqlitePool) {
        seed_provider(pool).await;
        sqlx::query(
            r#"
INSERT INTO global_models (
  id, name, display_name, is_active, default_tiered_pricing,
  supported_capabilities, usage_count, config, created_at, updated_at
) VALUES (
  'global-1', 'gpt-4.1', 'GPT 4.1', 1,
  '{"tiers":[{"input_price_per_1m":2.0,"output_price_per_1m":8.0}]}',
  '["chat"]', 7, '{"description":"Flagship","icon_url":"https://example.com/icon.png"}', 2, 3
)
"#,
        )
        .execute(pool)
        .await
        .expect("global model should seed");
        sqlx::query(
            r#"
INSERT INTO models (
  id, provider_id, global_model_id, provider_model_name, provider_model_mappings,
  supports_vision, supports_function_calling, supports_streaming, is_active,
  is_available, created_at, updated_at
) VALUES (
  'model-1', 'provider-1', 'global-1', 'provider-gpt-4.1', '["gpt-4.1"]',
  1, 1, 1, 1, 1, 4, 5
)
"#,
        )
        .execute(pool)
        .await
        .expect("provider model should seed");
    }

    async fn seed_provider(pool: &sqlx::SqlitePool) {
        sqlx::query(
            r#"
INSERT INTO providers (
  id, name, provider_type, is_active, created_at, updated_at
) VALUES (
  'provider-1', 'Provider One', 'custom', 1, 1, 1
)
"#,
        )
        .execute(pool)
        .await
        .expect("provider should seed");
    }
}
