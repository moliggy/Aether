use async_trait::async_trait;
use futures_util::TryStreamExt;
use sqlx::{PgPool, Postgres, QueryBuilder, Row};

use super::types::{
    LdapAuthUserProvisioningOutcome, StoredUserAuthRecord, StoredUserExportRow,
    StoredUserOAuthLinkSummary, StoredUserPreferenceRecord, StoredUserSessionRecord,
    StoredUserSummary, UserExportListQuery, UserExportSummary, UserReadRepository,
};
use crate::{error::SqlxResultExt, DataLayerError};

const LIST_USERS_BY_IDS_SQL: &str = r#"
SELECT
  id,
  username,
  email,
  role::text AS role,
  is_active,
  is_deleted
FROM users
WHERE id = ANY($1::text[])
ORDER BY id ASC
"#;

const LIST_USERS_BY_USERNAME_SEARCH_SQL: &str = r#"
SELECT
  id,
  username,
  email,
  role::text AS role,
  is_active,
  is_deleted
FROM users
WHERE is_deleted IS FALSE
  AND LOWER(username) LIKE $1
ORDER BY id ASC
"#;

const LIST_NON_ADMIN_EXPORT_USERS_SQL: &str = r#"
SELECT
  id,
  email,
  email_verified,
  username,
  password_hash,
  role::text AS role,
  auth_source::text AS auth_source,
  allowed_providers,
  allowed_api_formats,
  allowed_models,
  rate_limit,
  model_capability_settings,
  is_active
FROM users
WHERE is_deleted IS FALSE
  AND role::text != 'admin'
ORDER BY id ASC
"#;

const LIST_EXPORT_USERS_SQL: &str = r#"
SELECT
  id,
  email,
  email_verified,
  username,
  password_hash,
  role::text AS role,
  auth_source::text AS auth_source,
  allowed_providers,
  allowed_api_formats,
  allowed_models,
  rate_limit,
  model_capability_settings,
  is_active
FROM users
WHERE is_deleted IS FALSE
ORDER BY id ASC
"#;

const LIST_EXPORT_USERS_PAGE_PREFIX: &str = r#"
SELECT
  id,
  email,
  email_verified,
  username,
  password_hash,
  role::text AS role,
  auth_source::text AS auth_source,
  allowed_providers,
  allowed_api_formats,
  allowed_models,
  rate_limit,
  model_capability_settings,
  is_active
FROM users
WHERE is_deleted IS FALSE
"#;

const SUMMARIZE_EXPORT_USERS_SQL: &str = r#"
SELECT
  COUNT(*)::BIGINT AS total,
  COUNT(*) FILTER (WHERE is_active = TRUE)::BIGINT AS active
FROM users
WHERE is_deleted IS FALSE
"#;

const COUNT_ACTIVE_ADMIN_USERS_SQL: &str = r#"
SELECT COUNT(*)::BIGINT AS total
FROM users
WHERE role = 'admin'::userrole
  AND is_deleted IS FALSE
  AND is_active IS TRUE
"#;

const COUNT_ACTIVE_LOCAL_ADMIN_USERS_WITH_VALID_PASSWORD_SQL: &str = r#"
SELECT COUNT(*)::BIGINT AS total
FROM users
WHERE role = 'admin'::userrole
  AND auth_source = 'local'::authsource
  AND is_deleted IS FALSE
  AND is_active IS TRUE
  AND password_hash ~ '^\$2[aby]\$\d{2}\$.{53}$'
"#;

const FIND_EXPORT_USER_BY_ID_SQL: &str = r#"
SELECT
  id,
  email,
  email_verified,
  username,
  password_hash,
  role::text AS role,
  auth_source::text AS auth_source,
  allowed_providers,
  allowed_api_formats,
  allowed_models,
  rate_limit,
  model_capability_settings,
  is_active
FROM users
WHERE is_deleted IS FALSE
  AND id = $1
LIMIT 1
"#;

const FIND_USER_AUTH_BY_ID_SQL: &str = r#"
SELECT
  id,
  email,
  email_verified,
  username,
  password_hash,
  role::text AS role,
  auth_source::text AS auth_source,
  allowed_providers,
  allowed_api_formats,
  allowed_models,
  is_active,
  is_deleted,
  created_at,
  last_login_at
FROM users
WHERE id = $1
LIMIT 1
"#;

const LIST_USER_AUTH_BY_IDS_SQL: &str = r#"
SELECT
  id,
  email,
  email_verified,
  username,
  password_hash,
  role::text AS role,
  auth_source::text AS auth_source,
  allowed_providers,
  allowed_api_formats,
  allowed_models,
  is_active,
  is_deleted,
  created_at,
  last_login_at
FROM users
WHERE id = ANY($1::text[])
ORDER BY id ASC
"#;

const FIND_USER_AUTH_BY_IDENTIFIER_SQL: &str = r#"
SELECT
  id,
  email,
  email_verified,
  username,
  password_hash,
  role::text AS role,
  auth_source::text AS auth_source,
  allowed_providers,
  allowed_api_formats,
  allowed_models,
  is_active,
  is_deleted,
  created_at,
  last_login_at
FROM users
WHERE email = $1 OR username = $1
LIMIT 1
"#;

const FIND_USER_AUTH_BY_EMAIL_SQL: &str = r#"
SELECT
  id,
  email,
  email_verified,
  username,
  password_hash,
  role::text AS role,
  auth_source::text AS auth_source,
  allowed_providers,
  allowed_api_formats,
  allowed_models,
  is_active,
  is_deleted,
  created_at,
  last_login_at
FROM users
WHERE email = $1
LIMIT 1
"#;

const FIND_ACTIVE_USER_AUTH_BY_EMAIL_CI_SQL: &str = r#"
SELECT
  id,
  email,
  email_verified,
  username,
  password_hash,
  role::text AS role,
  auth_source::text AS auth_source,
  allowed_providers,
  allowed_api_formats,
  allowed_models,
  is_active,
  is_deleted,
  created_at,
  last_login_at
FROM users
WHERE LOWER(email) = LOWER($1)
  AND is_deleted IS FALSE
LIMIT 1
"#;

const FIND_USER_AUTH_BY_USERNAME_SQL: &str = r#"
SELECT
  id,
  email,
  email_verified,
  username,
  password_hash,
  role::text AS role,
  auth_source::text AS auth_source,
  allowed_providers,
  allowed_api_formats,
  allowed_models,
  is_active,
  is_deleted,
  created_at,
  last_login_at
FROM users
WHERE username = $1
LIMIT 1
"#;

const LIST_USER_OAUTH_LINKS_SQL: &str = r#"
SELECT
  user_oauth_links.provider_type,
  oauth_providers.display_name,
  user_oauth_links.provider_username,
  user_oauth_links.provider_email,
  user_oauth_links.linked_at,
  user_oauth_links.last_login_at,
  oauth_providers.is_enabled AS provider_enabled
FROM user_oauth_links
JOIN oauth_providers
  ON oauth_providers.provider_type = user_oauth_links.provider_type
WHERE user_oauth_links.user_id = $1
ORDER BY user_oauth_links.linked_at ASC
"#;

const FIND_OAUTH_LINKED_USER_SQL: &str = r#"
SELECT
  users.id,
  users.email,
  users.email_verified,
  users.username,
  users.password_hash,
  users.role::text AS role,
  users.auth_source::text AS auth_source,
  users.allowed_providers,
  users.allowed_api_formats,
  users.allowed_models,
  users.is_active,
  users.is_deleted,
  users.created_at,
  users.last_login_at
FROM user_oauth_links
JOIN users ON users.id = user_oauth_links.user_id
WHERE user_oauth_links.provider_type = $1
  AND user_oauth_links.provider_user_id = $2
LIMIT 1
"#;

const TOUCH_OAUTH_LINK_SQL: &str = r#"
UPDATE user_oauth_links
SET provider_username = COALESCE($3, provider_username),
    provider_email = COALESCE($4, provider_email),
    extra_data = COALESCE($5, extra_data),
    last_login_at = $6
WHERE provider_type = $1
  AND provider_user_id = $2
"#;

const FIND_OAUTH_LINK_OWNER_SQL: &str = r#"
SELECT user_id
FROM user_oauth_links
WHERE provider_type = $1
  AND provider_user_id = $2
LIMIT 1
"#;

const FIND_USER_PROVIDER_LINK_OWNER_SQL: &str = r#"
SELECT user_id
FROM user_oauth_links
WHERE user_id = $1
  AND provider_type = $2
LIMIT 1
"#;

const COUNT_USER_OAUTH_LINKS_SQL: &str = r#"
SELECT COUNT(*)::bigint AS link_count
FROM user_oauth_links
WHERE user_id = $1
"#;

const DELETE_USER_OAUTH_LINK_SQL: &str = r#"
DELETE FROM user_oauth_links
WHERE user_id = $1
  AND provider_type = $2
"#;

const UPSERT_OAUTH_LINK_SQL: &str = r#"
INSERT INTO user_oauth_links (
  id,
  user_id,
  provider_type,
  provider_user_id,
  provider_username,
  provider_email,
  extra_data,
  linked_at,
  last_login_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $8)
ON CONFLICT (user_id, provider_type) DO UPDATE
SET provider_user_id = EXCLUDED.provider_user_id,
    provider_username = EXCLUDED.provider_username,
    provider_email = EXCLUDED.provider_email,
    extra_data = EXCLUDED.extra_data,
    last_login_at = EXCLUDED.last_login_at
"#;

const TOUCH_AUTH_USER_LAST_LOGIN_SQL: &str = r#"
UPDATE users
SET
  last_login_at = $2,
  updated_at = $2
WHERE id = $1
"#;

const READ_USER_PREFERENCES_SQL: &str = r#"
SELECT
  up.user_id,
  up.avatar_url,
  up.bio,
  up.default_provider_id,
  p.name AS default_provider_name,
  up.theme,
  up.language,
  up.timezone,
  up.email_notifications,
  up.usage_alerts,
  up.announcement_notifications
FROM user_preferences up
LEFT JOIN providers p
  ON p.id = up.default_provider_id
WHERE up.user_id = $1
LIMIT 1
"#;

const UPSERT_USER_PREFERENCES_SQL: &str = r#"
WITH upserted AS (
  INSERT INTO user_preferences (
    id,
    user_id,
    avatar_url,
    bio,
    default_provider_id,
    theme,
    language,
    timezone,
    email_notifications,
    usage_alerts,
    announcement_notifications,
    created_at,
    updated_at
  ) VALUES (
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    $9,
    $10,
    $11,
    NOW(),
    NOW()
  )
  ON CONFLICT (user_id) DO UPDATE SET
    avatar_url = EXCLUDED.avatar_url,
    bio = EXCLUDED.bio,
    default_provider_id = EXCLUDED.default_provider_id,
    theme = EXCLUDED.theme,
    language = EXCLUDED.language,
    timezone = EXCLUDED.timezone,
    email_notifications = EXCLUDED.email_notifications,
    usage_alerts = EXCLUDED.usage_alerts,
    announcement_notifications = EXCLUDED.announcement_notifications,
    updated_at = NOW()
  RETURNING
    user_id,
    avatar_url,
    bio,
    default_provider_id,
    theme,
    language,
    timezone,
    email_notifications,
    usage_alerts,
    announcement_notifications
)
SELECT
  upserted.user_id,
  upserted.avatar_url,
  upserted.bio,
  upserted.default_provider_id,
  p.name AS default_provider_name,
  upserted.theme,
  upserted.language,
  upserted.timezone,
  upserted.email_notifications,
  upserted.usage_alerts,
  upserted.announcement_notifications
FROM upserted
LEFT JOIN providers p
  ON p.id = upserted.default_provider_id
"#;

const FIND_USER_SESSION_SQL: &str = r#"
SELECT
  id, user_id, client_device_id, device_label, refresh_token_hash,
  prev_refresh_token_hash, rotated_at, last_seen_at, expires_at, revoked_at,
  revoke_reason, ip_address, user_agent, created_at, updated_at
FROM user_sessions
WHERE user_id = $1 AND id = $2
LIMIT 1
"#;

const LIST_USER_SESSIONS_SQL: &str = r#"
SELECT
  id, user_id, client_device_id, device_label, refresh_token_hash,
  prev_refresh_token_hash, rotated_at, last_seen_at, expires_at, revoked_at,
  revoke_reason, ip_address, user_agent, created_at, updated_at
FROM user_sessions
WHERE user_id = $1
  AND revoked_at IS NULL
  AND expires_at > NOW()
ORDER BY last_seen_at DESC, created_at DESC
"#;

const REVOKE_ACTIVE_DEVICE_SESSIONS_SQL: &str = r#"
UPDATE user_sessions
SET revoked_at = $3, revoke_reason = 'replaced_by_new_login', updated_at = $3
WHERE user_id = $1
  AND client_device_id = $2
  AND revoked_at IS NULL
  AND expires_at > $3
"#;

const CREATE_USER_SESSION_SQL: &str = r#"
INSERT INTO user_sessions (
  id, user_id, client_device_id, device_label, device_type, ip_address,
  user_agent, refresh_token_hash, last_seen_at, expires_at, created_at, updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
RETURNING
  id, user_id, client_device_id, device_label, refresh_token_hash,
  prev_refresh_token_hash, rotated_at, last_seen_at, expires_at, revoked_at,
  revoke_reason, ip_address, user_agent, created_at, updated_at
"#;

const TOUCH_USER_SESSION_SQL: &str = r#"
UPDATE user_sessions
SET last_seen_at = $3,
    ip_address = COALESCE($4, ip_address),
    user_agent = COALESCE($5, user_agent),
    updated_at = $3
WHERE user_id = $1 AND id = $2
"#;

const UPDATE_USER_SESSION_DEVICE_LABEL_SQL: &str = r#"
UPDATE user_sessions
SET device_label = $3, updated_at = $4
WHERE user_id = $1 AND id = $2
"#;

const ROTATE_USER_SESSION_REFRESH_SQL: &str = r#"
UPDATE user_sessions
SET prev_refresh_token_hash = $3,
    rotated_at = $4,
    refresh_token_hash = $5,
    expires_at = $6,
    last_seen_at = $4,
    ip_address = COALESCE($7, ip_address),
    user_agent = COALESCE($8, user_agent),
    updated_at = $4
WHERE user_id = $1 AND id = $2
"#;

const REVOKE_USER_SESSION_SQL: &str = r#"
UPDATE user_sessions
SET revoked_at = $3, revoke_reason = $4, updated_at = $3
WHERE user_id = $1 AND id = $2
"#;

const REVOKE_ALL_USER_SESSIONS_SQL: &str = r#"
UPDATE user_sessions
SET revoked_at = $2, revoke_reason = $3, updated_at = $2
WHERE user_id = $1 AND revoked_at IS NULL
"#;

#[derive(Debug, Clone)]
pub struct SqlxUserReadRepository {
    pool: PgPool,
}

impl SqlxUserReadRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn list_users_by_ids(
        &self,
        user_ids: &[String],
    ) -> Result<Vec<StoredUserSummary>, DataLayerError> {
        if user_ids.is_empty() {
            return Ok(Vec::new());
        }
        collect_query_rows(
            sqlx::query(LIST_USERS_BY_IDS_SQL)
                .bind(user_ids)
                .fetch(&self.pool),
            map_user_row,
        )
        .await
    }

    pub async fn list_users_by_username_search(
        &self,
        username_search: &str,
    ) -> Result<Vec<StoredUserSummary>, DataLayerError> {
        let username_search = username_search.trim();
        if username_search.is_empty() {
            return Ok(Vec::new());
        }

        collect_query_rows(
            sqlx::query(LIST_USERS_BY_USERNAME_SEARCH_SQL)
                .bind(format!("%{}%", username_search.to_ascii_lowercase()))
                .fetch(&self.pool),
            map_user_row,
        )
        .await
    }

    pub async fn list_non_admin_export_users(
        &self,
    ) -> Result<Vec<StoredUserExportRow>, DataLayerError> {
        collect_query_rows(
            sqlx::query(LIST_NON_ADMIN_EXPORT_USERS_SQL).fetch(&self.pool),
            map_user_export_row,
        )
        .await
    }

    pub async fn list_export_users(&self) -> Result<Vec<StoredUserExportRow>, DataLayerError> {
        collect_query_rows(
            sqlx::query(LIST_EXPORT_USERS_SQL).fetch(&self.pool),
            map_user_export_row,
        )
        .await
    }

    pub async fn list_export_users_page(
        &self,
        query: &UserExportListQuery,
    ) -> Result<Vec<StoredUserExportRow>, DataLayerError> {
        let mut builder = QueryBuilder::<Postgres>::new(LIST_EXPORT_USERS_PAGE_PREFIX);

        if let Some(role) = query.role.as_deref() {
            builder
                .push(" AND LOWER(role::text) = ")
                .push_bind(role.trim().to_ascii_lowercase());
        }
        if let Some(is_active) = query.is_active {
            builder.push(" AND is_active = ").push_bind(is_active);
        }

        builder
            .push(" ORDER BY id ASC OFFSET ")
            .push_bind(i64::try_from(query.skip).map_err(|_| {
                DataLayerError::InvalidInput(format!("invalid user export skip: {}", query.skip))
            })?)
            .push(" LIMIT ")
            .push_bind(i64::try_from(query.limit).map_err(|_| {
                DataLayerError::InvalidInput(format!("invalid user export limit: {}", query.limit))
            })?);

        let query = builder.build();
        collect_query_rows(query.fetch(&self.pool), map_user_export_row).await
    }

    pub async fn summarize_export_users(&self) -> Result<UserExportSummary, DataLayerError> {
        let row = sqlx::query(SUMMARIZE_EXPORT_USERS_SQL)
            .fetch_one(&self.pool)
            .await
            .map_postgres_err()?;
        Ok(UserExportSummary {
            total: row.try_get::<i64, _>("total").map_postgres_err()?.max(0) as u64,
            active: row.try_get::<i64, _>("active").map_postgres_err()?.max(0) as u64,
        })
    }

    pub async fn find_export_user_by_id(
        &self,
        user_id: &str,
    ) -> Result<Option<StoredUserExportRow>, DataLayerError> {
        let row = sqlx::query(FIND_EXPORT_USER_BY_ID_SQL)
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await
            .map_postgres_err()?;
        row.as_ref().map(map_user_export_row).transpose()
    }

    pub async fn list_user_auth_by_ids(
        &self,
        user_ids: &[String],
    ) -> Result<Vec<StoredUserAuthRecord>, DataLayerError> {
        if user_ids.is_empty() {
            return Ok(Vec::new());
        }

        collect_query_rows(
            sqlx::query(LIST_USER_AUTH_BY_IDS_SQL)
                .bind(user_ids)
                .fetch(&self.pool),
            map_user_auth_row,
        )
        .await
    }

    pub async fn find_user_auth_by_id(
        &self,
        user_id: &str,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        let row = sqlx::query(FIND_USER_AUTH_BY_ID_SQL)
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await
            .map_postgres_err()?;
        row.as_ref().map(map_user_auth_row).transpose()
    }

    pub async fn find_user_auth_by_identifier(
        &self,
        identifier: &str,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        let row = sqlx::query(FIND_USER_AUTH_BY_IDENTIFIER_SQL)
            .bind(identifier)
            .fetch_optional(&self.pool)
            .await
            .map_postgres_err()?;
        row.as_ref().map(map_user_auth_row).transpose()
    }

    pub async fn find_user_auth_by_email(
        &self,
        email: &str,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        let row = sqlx::query(FIND_USER_AUTH_BY_EMAIL_SQL)
            .bind(email)
            .fetch_optional(&self.pool)
            .await
            .map_postgres_err()?;
        row.as_ref().map(map_user_auth_row).transpose()
    }

    pub async fn find_active_user_auth_by_email_ci(
        &self,
        email: &str,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        let row = sqlx::query(FIND_ACTIVE_USER_AUTH_BY_EMAIL_CI_SQL)
            .bind(email)
            .fetch_optional(&self.pool)
            .await
            .map_postgres_err()?;
        row.as_ref().map(map_user_auth_row).transpose()
    }

    pub async fn find_user_auth_by_username(
        &self,
        username: &str,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        let row = sqlx::query(FIND_USER_AUTH_BY_USERNAME_SQL)
            .bind(username)
            .fetch_optional(&self.pool)
            .await
            .map_postgres_err()?;
        row.as_ref().map(map_user_auth_row).transpose()
    }

    pub async fn list_user_oauth_links(
        &self,
        user_id: &str,
    ) -> Result<Vec<StoredUserOAuthLinkSummary>, DataLayerError> {
        collect_query_rows(
            sqlx::query(LIST_USER_OAUTH_LINKS_SQL)
                .bind(user_id)
                .fetch(&self.pool),
            map_oauth_link_summary_row,
        )
        .await
    }

    pub async fn find_oauth_linked_user(
        &self,
        provider_type: &str,
        provider_user_id: &str,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        let row = sqlx::query(FIND_OAUTH_LINKED_USER_SQL)
            .bind(provider_type)
            .bind(provider_user_id)
            .fetch_optional(&self.pool)
            .await
            .map_postgres_err()?;
        row.as_ref().map(map_user_auth_row).transpose()
    }

    pub async fn touch_oauth_link(
        &self,
        provider_type: &str,
        provider_user_id: &str,
        provider_username: Option<&str>,
        provider_email: Option<&str>,
        extra_data: Option<serde_json::Value>,
        touched_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, DataLayerError> {
        let result = sqlx::query(TOUCH_OAUTH_LINK_SQL)
            .bind(provider_type)
            .bind(provider_user_id)
            .bind(provider_username)
            .bind(provider_email)
            .bind(extra_data)
            .bind(touched_at)
            .execute(&self.pool)
            .await
            .map_postgres_err()?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn create_oauth_auth_user(
        &self,
        email: Option<String>,
        username: String,
        created_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        let user_id = uuid::Uuid::new_v4().to_string();
        sqlx::query(
            r#"
INSERT INTO users (
  id, email, email_verified, username, password_hash, role, auth_source,
  is_active, is_deleted, created_at, updated_at, last_login_at
)
VALUES (
  $1, $2, TRUE, $3, NULL, 'user'::userrole, 'oauth'::authsource,
  TRUE, FALSE, $4, $4, $4
)
"#,
        )
        .bind(&user_id)
        .bind(email)
        .bind(username)
        .bind(created_at)
        .execute(&self.pool)
        .await
        .map_postgres_err()?;
        self.find_user_auth_by_id(&user_id).await
    }

    pub async fn find_oauth_link_owner(
        &self,
        provider_type: &str,
        provider_user_id: &str,
    ) -> Result<Option<String>, DataLayerError> {
        sqlx::query_scalar(FIND_OAUTH_LINK_OWNER_SQL)
            .bind(provider_type)
            .bind(provider_user_id)
            .fetch_optional(&self.pool)
            .await
            .map_postgres_err()
    }

    pub async fn has_user_oauth_provider_link(
        &self,
        user_id: &str,
        provider_type: &str,
    ) -> Result<bool, DataLayerError> {
        let owner: Option<String> = sqlx::query_scalar(FIND_USER_PROVIDER_LINK_OWNER_SQL)
            .bind(user_id)
            .bind(provider_type)
            .fetch_optional(&self.pool)
            .await
            .map_postgres_err()?;
        Ok(owner.is_some())
    }

    pub async fn count_user_oauth_links(&self, user_id: &str) -> Result<u64, DataLayerError> {
        let row = sqlx::query(COUNT_USER_OAUTH_LINKS_SQL)
            .bind(user_id)
            .fetch_one(&self.pool)
            .await
            .map_postgres_err()?;
        Ok(row
            .try_get::<i64, _>("link_count")
            .map_postgres_err()?
            .max(0) as u64)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_user_oauth_link(
        &self,
        user_id: &str,
        provider_type: &str,
        provider_user_id: &str,
        provider_username: Option<&str>,
        provider_email: Option<&str>,
        extra_data: Option<serde_json::Value>,
        linked_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), DataLayerError> {
        sqlx::query(UPSERT_OAUTH_LINK_SQL)
            .bind(uuid::Uuid::new_v4().to_string())
            .bind(user_id)
            .bind(provider_type)
            .bind(provider_user_id)
            .bind(provider_username)
            .bind(provider_email)
            .bind(extra_data)
            .bind(linked_at)
            .execute(&self.pool)
            .await
            .map_postgres_err()?;
        Ok(())
    }

    pub async fn delete_user_oauth_link(
        &self,
        user_id: &str,
        provider_type: &str,
    ) -> Result<bool, DataLayerError> {
        let result = sqlx::query(DELETE_USER_OAUTH_LINK_SQL)
            .bind(user_id)
            .bind(provider_type)
            .execute(&self.pool)
            .await
            .map_postgres_err()?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn get_or_create_ldap_auth_user(
        &self,
        email: String,
        username: String,
        ldap_dn: Option<String>,
        ldap_username: Option<String>,
        logged_in_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<LdapAuthUserProvisioningOutcome>, DataLayerError> {
        let mut tx = self.pool.begin().await.map_postgres_err()?;
        let existing = find_postgres_ldap_user_for_update(
            &mut tx,
            ldap_dn.as_deref(),
            ldap_username.as_deref(),
            &email,
        )
        .await?;
        if let Some(existing) = existing {
            if existing.is_deleted
                || !existing.is_active
                || !existing.auth_source.eq_ignore_ascii_case("ldap")
            {
                tx.commit().await.map_err(crate::error::postgres_error)?;
                return Ok(None);
            }
            if existing.email.as_deref() != Some(email.as_str()) {
                let taken =
                    sqlx::query("SELECT 1 FROM users WHERE email = $1 AND id <> $2 LIMIT 1")
                        .bind(&email)
                        .bind(&existing.id)
                        .fetch_optional(&mut *tx)
                        .await
                        .map_postgres_err()?;
                if taken.is_some() {
                    tx.commit().await.map_err(crate::error::postgres_error)?;
                    return Ok(None);
                }
            }
            let row = sqlx::query(
                r#"
UPDATE users
SET email = $2,
    email_verified = TRUE,
    ldap_dn = COALESCE($3, ldap_dn),
    ldap_username = COALESCE($4, ldap_username),
    last_login_at = $5,
    updated_at = $5
WHERE id = $1
RETURNING
  id, email, email_verified, username, password_hash, role::text AS role,
  auth_source::text AS auth_source, allowed_providers, allowed_api_formats,
  allowed_models, is_active, is_deleted, created_at, last_login_at
"#,
            )
            .bind(&existing.id)
            .bind(&email)
            .bind(ldap_dn.as_deref())
            .bind(ldap_username.as_deref())
            .bind(logged_in_at)
            .fetch_one(&mut *tx)
            .await
            .map_postgres_err()?;
            tx.commit().await.map_err(crate::error::postgres_error)?;
            return Ok(Some(LdapAuthUserProvisioningOutcome {
                user: map_user_auth_row(&row)?,
                created: false,
            }));
        }

        let base_username = ldap_username
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(username.as_str())
            .trim()
            .to_string();
        let mut candidate_username = base_username.clone();
        for _attempt in 0..3 {
            let taken = sqlx::query("SELECT 1 FROM users WHERE username = $1 LIMIT 1")
                .bind(&candidate_username)
                .fetch_optional(&mut *tx)
                .await
                .map_postgres_err()?;
            if taken.is_some() {
                let suffix = uuid::Uuid::new_v4().simple().to_string();
                candidate_username = format!(
                    "{}_ldap_{}{}",
                    base_username,
                    logged_in_at.timestamp(),
                    &suffix[..4]
                );
                continue;
            }
            let row = sqlx::query(
                r#"
INSERT INTO users (
  id, email, email_verified, username, password_hash, role, auth_source,
  ldap_dn, ldap_username, is_active, is_deleted, created_at, updated_at, last_login_at
)
VALUES ($1, $2, TRUE, $3, NULL, 'user'::userrole, 'ldap'::authsource, $4, $5, TRUE, FALSE, $6, $6, $6)
RETURNING
  id, email, email_verified, username, password_hash, role::text AS role,
  auth_source::text AS auth_source, allowed_providers, allowed_api_formats,
  allowed_models, is_active, is_deleted, created_at, last_login_at
"#,
            )
            .bind(uuid::Uuid::new_v4().to_string())
            .bind(&email)
            .bind(&candidate_username)
            .bind(ldap_dn.as_deref())
            .bind(ldap_username.as_deref())
            .bind(logged_in_at)
            .fetch_one(&mut *tx)
            .await
            .map_postgres_err()?;
            tx.commit().await.map_err(crate::error::postgres_error)?;
            return Ok(Some(LdapAuthUserProvisioningOutcome {
                user: map_user_auth_row(&row)?,
                created: true,
            }));
        }
        tx.commit().await.map_err(crate::error::postgres_error)?;
        Ok(None)
    }

    pub async fn count_active_admin_users(&self) -> Result<u64, DataLayerError> {
        let total: i64 = sqlx::query_scalar(COUNT_ACTIVE_ADMIN_USERS_SQL)
            .fetch_one(&self.pool)
            .await
            .map_postgres_err()?;
        Ok(total.max(0) as u64)
    }

    pub async fn touch_auth_user_last_login(
        &self,
        user_id: &str,
        logged_in_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, DataLayerError> {
        let result = sqlx::query(TOUCH_AUTH_USER_LAST_LOGIN_SQL)
            .bind(user_id)
            .bind(logged_in_at)
            .execute(&self.pool)
            .await
            .map_postgres_err()?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn update_local_auth_user_profile(
        &self,
        user_id: &str,
        email: Option<String>,
        username: Option<String>,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        let result = sqlx::query(
            r#"
UPDATE users
SET email = COALESCE($2, email),
    username = COALESCE($3, username),
    updated_at = NOW()
WHERE id = $1
"#,
        )
        .bind(user_id)
        .bind(email)
        .bind(username)
        .execute(&self.pool)
        .await
        .map_postgres_err()?;
        if result.rows_affected() == 0 {
            return Ok(None);
        }
        self.find_user_auth_by_id(user_id).await
    }

    pub async fn update_local_auth_user_password_hash(
        &self,
        user_id: &str,
        password_hash: String,
        updated_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        let result = sqlx::query(
            r#"
UPDATE users
SET password_hash = $2,
    updated_at = $3
WHERE id = $1
"#,
        )
        .bind(user_id)
        .bind(password_hash)
        .bind(updated_at)
        .execute(&self.pool)
        .await
        .map_postgres_err()?;
        if result.rows_affected() == 0 {
            return Ok(None);
        }
        self.find_user_auth_by_id(user_id).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_local_auth_user_admin_fields(
        &self,
        user_id: &str,
        role: Option<String>,
        allowed_providers_present: bool,
        allowed_providers: Option<Vec<String>>,
        allowed_api_formats_present: bool,
        allowed_api_formats: Option<Vec<String>>,
        allowed_models_present: bool,
        allowed_models: Option<Vec<String>>,
        rate_limit_present: bool,
        rate_limit: Option<i32>,
        is_active: Option<bool>,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        let result = sqlx::query(
            r#"
UPDATE users
SET role = CASE
        WHEN $2::BOOLEAN AND $3 IS NOT NULL THEN $3::userrole
        ELSE role
    END,
    allowed_providers = CASE
        WHEN $4::BOOLEAN THEN $5::json
        ELSE allowed_providers
    END,
    allowed_api_formats = CASE
        WHEN $6::BOOLEAN THEN $7::json
        ELSE allowed_api_formats
    END,
    allowed_models = CASE
        WHEN $8::BOOLEAN THEN $9::json
        ELSE allowed_models
    END,
    rate_limit = CASE
        WHEN $10::BOOLEAN THEN $11
        ELSE rate_limit
    END,
    is_active = CASE
        WHEN $12::BOOLEAN AND $13 IS NOT NULL THEN $13
        ELSE is_active
    END,
    updated_at = NOW()
WHERE id = $1
"#,
        )
        .bind(user_id)
        .bind(role.is_some())
        .bind(role)
        .bind(allowed_providers_present)
        .bind(allowed_providers.map(serde_json::Value::from))
        .bind(allowed_api_formats_present)
        .bind(allowed_api_formats.map(serde_json::Value::from))
        .bind(allowed_models_present)
        .bind(allowed_models.map(serde_json::Value::from))
        .bind(rate_limit_present)
        .bind(rate_limit)
        .bind(is_active.is_some())
        .bind(is_active)
        .execute(&self.pool)
        .await
        .map_postgres_err()?;
        if result.rows_affected() == 0 {
            return Ok(None);
        }
        self.find_user_auth_by_id(user_id).await
    }

    pub async fn update_user_model_capability_settings(
        &self,
        user_id: &str,
        settings: Option<serde_json::Value>,
    ) -> Result<Option<serde_json::Value>, DataLayerError> {
        let normalized = normalize_optional_json_value(settings);
        let result = sqlx::query(
            r#"
UPDATE users
SET model_capability_settings = $2,
    updated_at = NOW()
WHERE id = $1
"#,
        )
        .bind(user_id)
        .bind(normalized.clone())
        .execute(&self.pool)
        .await
        .map_postgres_err()?;
        if result.rows_affected() == 0 {
            return Ok(None);
        }
        Ok(normalized)
    }

    pub async fn create_local_auth_user(
        &self,
        email: Option<String>,
        email_verified: bool,
        username: String,
        password_hash: String,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        self.create_local_auth_user_with_settings(
            email,
            email_verified,
            username,
            password_hash,
            "user".to_string(),
            None,
            None,
            None,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_local_auth_user_with_settings(
        &self,
        email: Option<String>,
        email_verified: bool,
        username: String,
        password_hash: String,
        role: String,
        allowed_providers: Option<Vec<String>>,
        allowed_api_formats: Option<Vec<String>>,
        allowed_models: Option<Vec<String>>,
        rate_limit: Option<i32>,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        let user_id = uuid::Uuid::new_v4().to_string();
        sqlx::query(
            r#"
INSERT INTO users (
  id, email, email_verified, username, password_hash, role, auth_source,
  allowed_providers, allowed_api_formats, allowed_models, rate_limit,
  is_active, is_deleted, created_at, updated_at
)
VALUES (
  $1, $2, $3, $4, $5, $6::userrole, 'local'::authsource,
  $7::json, $8::json, $9::json, $10,
  TRUE, FALSE, NOW(), NOW()
)
"#,
        )
        .bind(&user_id)
        .bind(email)
        .bind(email_verified)
        .bind(username)
        .bind(password_hash)
        .bind(role)
        .bind(allowed_providers.map(serde_json::Value::from))
        .bind(allowed_api_formats.map(serde_json::Value::from))
        .bind(allowed_models.map(serde_json::Value::from))
        .bind(rate_limit)
        .execute(&self.pool)
        .await
        .map_postgres_err()?;
        self.find_user_auth_by_id(&user_id).await
    }

    pub async fn delete_local_auth_user(&self, user_id: &str) -> Result<bool, DataLayerError> {
        let result = sqlx::query("DELETE FROM users WHERE id = $1")
            .bind(user_id)
            .execute(&self.pool)
            .await
            .map_postgres_err()?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn read_user_preferences(
        &self,
        user_id: &str,
    ) -> Result<Option<StoredUserPreferenceRecord>, DataLayerError> {
        let row = sqlx::query(READ_USER_PREFERENCES_SQL)
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await
            .map_postgres_err()?;
        row.as_ref().map(map_user_preference_row).transpose()
    }

    pub async fn write_user_preferences(
        &self,
        preferences: &StoredUserPreferenceRecord,
    ) -> Result<Option<StoredUserPreferenceRecord>, DataLayerError> {
        let row = sqlx::query(UPSERT_USER_PREFERENCES_SQL)
            .bind(uuid::Uuid::new_v4().to_string())
            .bind(&preferences.user_id)
            .bind(preferences.avatar_url.as_deref())
            .bind(preferences.bio.as_deref())
            .bind(preferences.default_provider_id.as_deref())
            .bind(&preferences.theme)
            .bind(&preferences.language)
            .bind(&preferences.timezone)
            .bind(preferences.email_notifications)
            .bind(preferences.usage_alerts)
            .bind(preferences.announcement_notifications)
            .fetch_optional(&self.pool)
            .await
            .map_postgres_err()?;
        row.as_ref().map(map_user_preference_row).transpose()
    }

    pub async fn find_user_session(
        &self,
        user_id: &str,
        session_id: &str,
    ) -> Result<Option<StoredUserSessionRecord>, DataLayerError> {
        let row = sqlx::query(FIND_USER_SESSION_SQL)
            .bind(user_id)
            .bind(session_id)
            .fetch_optional(&self.pool)
            .await
            .map_postgres_err()?;
        row.as_ref().map(map_user_session_row).transpose()
    }

    pub async fn list_user_sessions(
        &self,
        user_id: &str,
    ) -> Result<Vec<StoredUserSessionRecord>, DataLayerError> {
        collect_query_rows(
            sqlx::query(LIST_USER_SESSIONS_SQL)
                .bind(user_id)
                .fetch(&self.pool),
            map_user_session_row,
        )
        .await
    }

    pub async fn create_user_session(
        &self,
        session: &StoredUserSessionRecord,
    ) -> Result<Option<StoredUserSessionRecord>, DataLayerError> {
        let now = session
            .created_at
            .or(session.updated_at)
            .or(session.last_seen_at)
            .unwrap_or_else(chrono::Utc::now);
        sqlx::query(REVOKE_ACTIVE_DEVICE_SESSIONS_SQL)
            .bind(&session.user_id)
            .bind(&session.client_device_id)
            .bind(now)
            .execute(&self.pool)
            .await
            .map_postgres_err()?;
        let row = sqlx::query(CREATE_USER_SESSION_SQL)
            .bind(&session.id)
            .bind(&session.user_id)
            .bind(&session.client_device_id)
            .bind(session.device_label.as_deref())
            .bind("unknown")
            .bind(session.ip_address.as_deref())
            .bind(session.user_agent.as_deref())
            .bind(&session.refresh_token_hash)
            .bind(session.last_seen_at.unwrap_or(now))
            .bind(session.expires_at.unwrap_or(now))
            .bind(session.created_at.unwrap_or(now))
            .bind(session.updated_at.unwrap_or(now))
            .fetch_one(&self.pool)
            .await
            .map_postgres_err()?;
        Ok(Some(map_user_session_row(&row)?))
    }

    pub async fn touch_user_session(
        &self,
        user_id: &str,
        session_id: &str,
        touched_at: chrono::DateTime<chrono::Utc>,
        ip_address: Option<&str>,
        user_agent: Option<&str>,
    ) -> Result<bool, DataLayerError> {
        let result = sqlx::query(TOUCH_USER_SESSION_SQL)
            .bind(user_id)
            .bind(session_id)
            .bind(touched_at)
            .bind(ip_address)
            .bind(user_agent.map(|value| value.chars().take(1000).collect::<String>()))
            .execute(&self.pool)
            .await
            .map_postgres_err()?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn update_user_session_device_label(
        &self,
        user_id: &str,
        session_id: &str,
        device_label: &str,
        updated_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, DataLayerError> {
        let result = sqlx::query(UPDATE_USER_SESSION_DEVICE_LABEL_SQL)
            .bind(user_id)
            .bind(session_id)
            .bind(device_label.chars().take(120).collect::<String>())
            .bind(updated_at)
            .execute(&self.pool)
            .await
            .map_postgres_err()?;
        Ok(result.rows_affected() > 0)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn rotate_user_session_refresh_token(
        &self,
        user_id: &str,
        session_id: &str,
        previous_refresh_token_hash: &str,
        next_refresh_token_hash: &str,
        rotated_at: chrono::DateTime<chrono::Utc>,
        expires_at: chrono::DateTime<chrono::Utc>,
        ip_address: Option<&str>,
        user_agent: Option<&str>,
    ) -> Result<bool, DataLayerError> {
        let result = sqlx::query(ROTATE_USER_SESSION_REFRESH_SQL)
            .bind(user_id)
            .bind(session_id)
            .bind(previous_refresh_token_hash)
            .bind(rotated_at)
            .bind(next_refresh_token_hash)
            .bind(expires_at)
            .bind(ip_address)
            .bind(user_agent.map(|value| value.chars().take(1000).collect::<String>()))
            .execute(&self.pool)
            .await
            .map_postgres_err()?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn revoke_user_session(
        &self,
        user_id: &str,
        session_id: &str,
        revoked_at: chrono::DateTime<chrono::Utc>,
        reason: &str,
    ) -> Result<bool, DataLayerError> {
        let result = sqlx::query(REVOKE_USER_SESSION_SQL)
            .bind(user_id)
            .bind(session_id)
            .bind(revoked_at)
            .bind(reason.chars().take(100).collect::<String>())
            .execute(&self.pool)
            .await
            .map_postgres_err()?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn revoke_all_user_sessions(
        &self,
        user_id: &str,
        revoked_at: chrono::DateTime<chrono::Utc>,
        reason: &str,
    ) -> Result<u64, DataLayerError> {
        let result = sqlx::query(REVOKE_ALL_USER_SESSIONS_SQL)
            .bind(user_id)
            .bind(revoked_at)
            .bind(reason.chars().take(100).collect::<String>())
            .execute(&self.pool)
            .await
            .map_postgres_err()?;
        Ok(result.rows_affected())
    }

    pub async fn count_active_local_admin_users_with_valid_password(
        &self,
    ) -> Result<u64, DataLayerError> {
        let total: i64 = sqlx::query_scalar(COUNT_ACTIVE_LOCAL_ADMIN_USERS_WITH_VALID_PASSWORD_SQL)
            .fetch_one(&self.pool)
            .await
            .map_postgres_err()?;
        Ok(total.max(0) as u64)
    }
}

fn map_user_preference_row(
    row: &sqlx::postgres::PgRow,
) -> Result<StoredUserPreferenceRecord, DataLayerError> {
    let user_id: String = row.try_get("user_id").map_postgres_err()?;
    if user_id.trim().is_empty() {
        return Err(DataLayerError::UnexpectedValue(
            "user_preferences.user_id is empty".to_string(),
        ));
    }

    Ok(StoredUserPreferenceRecord {
        user_id,
        avatar_url: row.try_get("avatar_url").map_postgres_err()?,
        bio: row.try_get("bio").map_postgres_err()?,
        default_provider_id: row.try_get("default_provider_id").map_postgres_err()?,
        default_provider_name: row.try_get("default_provider_name").map_postgres_err()?,
        theme: row.try_get("theme").map_postgres_err()?,
        language: row.try_get("language").map_postgres_err()?,
        timezone: row.try_get("timezone").map_postgres_err()?,
        email_notifications: row.try_get("email_notifications").map_postgres_err()?,
        usage_alerts: row.try_get("usage_alerts").map_postgres_err()?,
        announcement_notifications: row
            .try_get("announcement_notifications")
            .map_postgres_err()?,
    })
}

fn map_user_session_row(
    row: &sqlx::postgres::PgRow,
) -> Result<StoredUserSessionRecord, DataLayerError> {
    StoredUserSessionRecord::new(
        row.try_get("id").map_postgres_err()?,
        row.try_get("user_id").map_postgres_err()?,
        row.try_get("client_device_id").map_postgres_err()?,
        row.try_get("device_label").map_postgres_err()?,
        row.try_get("refresh_token_hash").map_postgres_err()?,
        row.try_get("prev_refresh_token_hash").map_postgres_err()?,
        row.try_get("rotated_at").map_postgres_err()?,
        row.try_get("last_seen_at").map_postgres_err()?,
        row.try_get("expires_at").map_postgres_err()?,
        row.try_get("revoked_at").map_postgres_err()?,
        row.try_get("revoke_reason").map_postgres_err()?,
        row.try_get("ip_address").map_postgres_err()?,
        row.try_get("user_agent").map_postgres_err()?,
        row.try_get("created_at").map_postgres_err()?,
        row.try_get("updated_at").map_postgres_err()?,
    )
}

fn normalize_optional_json_value(value: Option<serde_json::Value>) -> Option<serde_json::Value> {
    match value {
        Some(serde_json::Value::Null) | None => None,
        Some(value) => Some(value),
    }
}

async fn find_postgres_ldap_user_for_update(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ldap_dn: Option<&str>,
    ldap_username: Option<&str>,
    email: &str,
) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
    let select_columns = r#"
SELECT
  id, email, email_verified, username, password_hash, role::text AS role,
  auth_source::text AS auth_source, allowed_providers, allowed_api_formats,
  allowed_models, is_active, is_deleted, created_at, last_login_at
FROM users
"#;
    if let Some(ldap_dn) = ldap_dn.filter(|value| !value.trim().is_empty()) {
        let row = sqlx::query(&format!(
            "{select_columns} WHERE auth_source = 'ldap'::authsource AND ldap_dn = $1 LIMIT 1 FOR UPDATE"
        ))
        .bind(ldap_dn)
        .fetch_optional(&mut **tx)
        .await
        .map_postgres_err()?;
        if let Some(row) = row.as_ref() {
            return map_user_auth_row(row).map(Some);
        }
    }
    if let Some(ldap_username) = ldap_username.filter(|value| !value.trim().is_empty()) {
        let row = sqlx::query(&format!(
            "{select_columns} WHERE auth_source = 'ldap'::authsource AND ldap_username = $1 LIMIT 1 FOR UPDATE"
        ))
        .bind(ldap_username)
        .fetch_optional(&mut **tx)
        .await
        .map_postgres_err()?;
        if let Some(row) = row.as_ref() {
            return map_user_auth_row(row).map(Some);
        }
    }
    let row = sqlx::query(&format!(
        "{select_columns} WHERE email = $1 LIMIT 1 FOR UPDATE"
    ))
    .bind(email)
    .fetch_optional(&mut **tx)
    .await
    .map_postgres_err()?;
    row.as_ref().map(map_user_auth_row).transpose()
}

fn map_user_row(row: &sqlx::postgres::PgRow) -> Result<StoredUserSummary, DataLayerError> {
    StoredUserSummary::new(
        row.try_get("id").map_postgres_err()?,
        row.try_get("username").map_postgres_err()?,
        row.try_get("email").map_postgres_err()?,
        row.try_get("role").map_postgres_err()?,
        row.try_get("is_active").map_postgres_err()?,
        row.try_get("is_deleted").map_postgres_err()?,
    )
}

fn map_user_export_row(row: &sqlx::postgres::PgRow) -> Result<StoredUserExportRow, DataLayerError> {
    StoredUserExportRow::new(
        row.try_get("id").map_postgres_err()?,
        row.try_get("email").map_postgres_err()?,
        row.try_get("email_verified").map_postgres_err()?,
        row.try_get("username").map_postgres_err()?,
        row.try_get("password_hash").map_postgres_err()?,
        row.try_get("role").map_postgres_err()?,
        row.try_get("auth_source").map_postgres_err()?,
        row.try_get("allowed_providers").map_postgres_err()?,
        row.try_get("allowed_api_formats").map_postgres_err()?,
        row.try_get("allowed_models").map_postgres_err()?,
        row.try_get("rate_limit").map_postgres_err()?,
        row.try_get("model_capability_settings")
            .map_postgres_err()?,
        row.try_get("is_active").map_postgres_err()?,
    )
}

fn map_user_auth_row(row: &sqlx::postgres::PgRow) -> Result<StoredUserAuthRecord, DataLayerError> {
    StoredUserAuthRecord::new(
        row.try_get("id").map_postgres_err()?,
        row.try_get("email").map_postgres_err()?,
        row.try_get("email_verified").map_postgres_err()?,
        row.try_get("username").map_postgres_err()?,
        row.try_get("password_hash").map_postgres_err()?,
        row.try_get("role").map_postgres_err()?,
        row.try_get("auth_source").map_postgres_err()?,
        row.try_get("allowed_providers").map_postgres_err()?,
        row.try_get("allowed_api_formats").map_postgres_err()?,
        row.try_get("allowed_models").map_postgres_err()?,
        row.try_get("is_active").map_postgres_err()?,
        row.try_get("is_deleted").map_postgres_err()?,
        row.try_get("created_at").map_postgres_err()?,
        row.try_get("last_login_at").map_postgres_err()?,
    )
}

fn map_oauth_link_summary_row(
    row: &sqlx::postgres::PgRow,
) -> Result<StoredUserOAuthLinkSummary, DataLayerError> {
    StoredUserOAuthLinkSummary::new(
        row.try_get("provider_type").map_postgres_err()?,
        row.try_get("display_name").map_postgres_err()?,
        row.try_get("provider_username").map_postgres_err()?,
        row.try_get("provider_email").map_postgres_err()?,
        row.try_get("linked_at").map_postgres_err()?,
        row.try_get("last_login_at").map_postgres_err()?,
        row.try_get("provider_enabled").map_postgres_err()?,
    )
}

async fn collect_query_rows<T, S>(
    mut rows: S,
    mapper: fn(&sqlx::postgres::PgRow) -> Result<T, DataLayerError>,
) -> Result<Vec<T>, DataLayerError>
where
    S: futures_util::TryStream<Ok = sqlx::postgres::PgRow, Error = sqlx::Error> + Unpin,
{
    let mut items = Vec::new();
    while let Some(row) = rows.try_next().await.map_postgres_err()? {
        items.push(mapper(&row)?);
    }
    Ok(items)
}

#[async_trait]
impl UserReadRepository for SqlxUserReadRepository {
    async fn list_users_by_ids(
        &self,
        user_ids: &[String],
    ) -> Result<Vec<StoredUserSummary>, DataLayerError> {
        self.list_users_by_ids(user_ids).await
    }

    async fn list_users_by_username_search(
        &self,
        username_search: &str,
    ) -> Result<Vec<StoredUserSummary>, DataLayerError> {
        self.list_users_by_username_search(username_search).await
    }

    async fn list_non_admin_export_users(
        &self,
    ) -> Result<Vec<StoredUserExportRow>, DataLayerError> {
        self.list_non_admin_export_users().await
    }

    async fn list_export_users(&self) -> Result<Vec<StoredUserExportRow>, DataLayerError> {
        self.list_export_users().await
    }

    async fn list_export_users_page(
        &self,
        query: &UserExportListQuery,
    ) -> Result<Vec<StoredUserExportRow>, DataLayerError> {
        self.list_export_users_page(query).await
    }

    async fn summarize_export_users(&self) -> Result<UserExportSummary, DataLayerError> {
        self.summarize_export_users().await
    }

    async fn find_export_user_by_id(
        &self,
        user_id: &str,
    ) -> Result<Option<StoredUserExportRow>, DataLayerError> {
        self.find_export_user_by_id(user_id).await
    }

    async fn find_user_auth_by_id(
        &self,
        user_id: &str,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        self.find_user_auth_by_id(user_id).await
    }

    async fn list_user_auth_by_ids(
        &self,
        user_ids: &[String],
    ) -> Result<Vec<StoredUserAuthRecord>, DataLayerError> {
        self.list_user_auth_by_ids(user_ids).await
    }

    async fn find_user_auth_by_identifier(
        &self,
        identifier: &str,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        self.find_user_auth_by_identifier(identifier).await
    }

    async fn find_user_auth_by_email(
        &self,
        email: &str,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        self.find_user_auth_by_email(email).await
    }

    async fn find_active_user_auth_by_email_ci(
        &self,
        email: &str,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        self.find_active_user_auth_by_email_ci(email).await
    }

    async fn find_user_auth_by_username(
        &self,
        username: &str,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        self.find_user_auth_by_username(username).await
    }

    async fn list_user_oauth_links(
        &self,
        user_id: &str,
    ) -> Result<Vec<StoredUserOAuthLinkSummary>, DataLayerError> {
        self.list_user_oauth_links(user_id).await
    }

    async fn find_oauth_linked_user(
        &self,
        provider_type: &str,
        provider_user_id: &str,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        self.find_oauth_linked_user(provider_type, provider_user_id)
            .await
    }

    async fn touch_oauth_link(
        &self,
        provider_type: &str,
        provider_user_id: &str,
        provider_username: Option<&str>,
        provider_email: Option<&str>,
        extra_data: Option<serde_json::Value>,
        touched_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, DataLayerError> {
        self.touch_oauth_link(
            provider_type,
            provider_user_id,
            provider_username,
            provider_email,
            extra_data,
            touched_at,
        )
        .await
    }

    async fn create_oauth_auth_user(
        &self,
        email: Option<String>,
        username: String,
        created_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        self.create_oauth_auth_user(email, username, created_at)
            .await
    }

    async fn find_oauth_link_owner(
        &self,
        provider_type: &str,
        provider_user_id: &str,
    ) -> Result<Option<String>, DataLayerError> {
        self.find_oauth_link_owner(provider_type, provider_user_id)
            .await
    }

    async fn has_user_oauth_provider_link(
        &self,
        user_id: &str,
        provider_type: &str,
    ) -> Result<bool, DataLayerError> {
        self.has_user_oauth_provider_link(user_id, provider_type)
            .await
    }

    async fn count_user_oauth_links(&self, user_id: &str) -> Result<u64, DataLayerError> {
        self.count_user_oauth_links(user_id).await
    }

    async fn upsert_user_oauth_link(
        &self,
        user_id: &str,
        provider_type: &str,
        provider_user_id: &str,
        provider_username: Option<&str>,
        provider_email: Option<&str>,
        extra_data: Option<serde_json::Value>,
        linked_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), DataLayerError> {
        self.upsert_user_oauth_link(
            user_id,
            provider_type,
            provider_user_id,
            provider_username,
            provider_email,
            extra_data,
            linked_at,
        )
        .await
    }

    async fn delete_user_oauth_link(
        &self,
        user_id: &str,
        provider_type: &str,
    ) -> Result<bool, DataLayerError> {
        self.delete_user_oauth_link(user_id, provider_type).await
    }

    async fn get_or_create_ldap_auth_user(
        &self,
        email: String,
        username: String,
        ldap_dn: Option<String>,
        ldap_username: Option<String>,
        logged_in_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<LdapAuthUserProvisioningOutcome>, DataLayerError> {
        self.get_or_create_ldap_auth_user(email, username, ldap_dn, ldap_username, logged_in_at)
            .await
    }

    async fn touch_auth_user_last_login(
        &self,
        user_id: &str,
        logged_in_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, DataLayerError> {
        self.touch_auth_user_last_login(user_id, logged_in_at).await
    }

    async fn update_local_auth_user_profile(
        &self,
        user_id: &str,
        email: Option<String>,
        username: Option<String>,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        self.update_local_auth_user_profile(user_id, email, username)
            .await
    }

    async fn update_local_auth_user_password_hash(
        &self,
        user_id: &str,
        password_hash: String,
        updated_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        self.update_local_auth_user_password_hash(user_id, password_hash, updated_at)
            .await
    }

    async fn update_local_auth_user_admin_fields(
        &self,
        user_id: &str,
        role: Option<String>,
        allowed_providers_present: bool,
        allowed_providers: Option<Vec<String>>,
        allowed_api_formats_present: bool,
        allowed_api_formats: Option<Vec<String>>,
        allowed_models_present: bool,
        allowed_models: Option<Vec<String>>,
        rate_limit_present: bool,
        rate_limit: Option<i32>,
        is_active: Option<bool>,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        self.update_local_auth_user_admin_fields(
            user_id,
            role,
            allowed_providers_present,
            allowed_providers,
            allowed_api_formats_present,
            allowed_api_formats,
            allowed_models_present,
            allowed_models,
            rate_limit_present,
            rate_limit,
            is_active,
        )
        .await
    }

    async fn update_user_model_capability_settings(
        &self,
        user_id: &str,
        settings: Option<serde_json::Value>,
    ) -> Result<Option<serde_json::Value>, DataLayerError> {
        self.update_user_model_capability_settings(user_id, settings)
            .await
    }

    async fn create_local_auth_user(
        &self,
        email: Option<String>,
        email_verified: bool,
        username: String,
        password_hash: String,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        self.create_local_auth_user(email, email_verified, username, password_hash)
            .await
    }

    async fn create_local_auth_user_with_settings(
        &self,
        email: Option<String>,
        email_verified: bool,
        username: String,
        password_hash: String,
        role: String,
        allowed_providers: Option<Vec<String>>,
        allowed_api_formats: Option<Vec<String>>,
        allowed_models: Option<Vec<String>>,
        rate_limit: Option<i32>,
    ) -> Result<Option<StoredUserAuthRecord>, DataLayerError> {
        self.create_local_auth_user_with_settings(
            email,
            email_verified,
            username,
            password_hash,
            role,
            allowed_providers,
            allowed_api_formats,
            allowed_models,
            rate_limit,
        )
        .await
    }

    async fn delete_local_auth_user(&self, user_id: &str) -> Result<bool, DataLayerError> {
        self.delete_local_auth_user(user_id).await
    }

    async fn read_user_preferences(
        &self,
        user_id: &str,
    ) -> Result<Option<StoredUserPreferenceRecord>, DataLayerError> {
        self.read_user_preferences(user_id).await
    }

    async fn write_user_preferences(
        &self,
        preferences: &StoredUserPreferenceRecord,
    ) -> Result<Option<StoredUserPreferenceRecord>, DataLayerError> {
        self.write_user_preferences(preferences).await
    }

    async fn find_user_session(
        &self,
        user_id: &str,
        session_id: &str,
    ) -> Result<Option<StoredUserSessionRecord>, DataLayerError> {
        self.find_user_session(user_id, session_id).await
    }

    async fn list_user_sessions(
        &self,
        user_id: &str,
    ) -> Result<Vec<StoredUserSessionRecord>, DataLayerError> {
        self.list_user_sessions(user_id).await
    }

    async fn create_user_session(
        &self,
        session: &StoredUserSessionRecord,
    ) -> Result<Option<StoredUserSessionRecord>, DataLayerError> {
        self.create_user_session(session).await
    }

    async fn touch_user_session(
        &self,
        user_id: &str,
        session_id: &str,
        touched_at: chrono::DateTime<chrono::Utc>,
        ip_address: Option<&str>,
        user_agent: Option<&str>,
    ) -> Result<bool, DataLayerError> {
        self.touch_user_session(user_id, session_id, touched_at, ip_address, user_agent)
            .await
    }

    async fn update_user_session_device_label(
        &self,
        user_id: &str,
        session_id: &str,
        device_label: &str,
        updated_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, DataLayerError> {
        self.update_user_session_device_label(user_id, session_id, device_label, updated_at)
            .await
    }

    async fn rotate_user_session_refresh_token(
        &self,
        user_id: &str,
        session_id: &str,
        previous_refresh_token_hash: &str,
        next_refresh_token_hash: &str,
        rotated_at: chrono::DateTime<chrono::Utc>,
        expires_at: chrono::DateTime<chrono::Utc>,
        ip_address: Option<&str>,
        user_agent: Option<&str>,
    ) -> Result<bool, DataLayerError> {
        self.rotate_user_session_refresh_token(
            user_id,
            session_id,
            previous_refresh_token_hash,
            next_refresh_token_hash,
            rotated_at,
            expires_at,
            ip_address,
            user_agent,
        )
        .await
    }

    async fn revoke_user_session(
        &self,
        user_id: &str,
        session_id: &str,
        revoked_at: chrono::DateTime<chrono::Utc>,
        reason: &str,
    ) -> Result<bool, DataLayerError> {
        self.revoke_user_session(user_id, session_id, revoked_at, reason)
            .await
    }

    async fn revoke_all_user_sessions(
        &self,
        user_id: &str,
        revoked_at: chrono::DateTime<chrono::Utc>,
        reason: &str,
    ) -> Result<u64, DataLayerError> {
        self.revoke_all_user_sessions(user_id, revoked_at, reason)
            .await
    }

    async fn count_active_admin_users(&self) -> Result<u64, DataLayerError> {
        self.count_active_admin_users().await
    }

    async fn count_active_local_admin_users_with_valid_password(
        &self,
    ) -> Result<u64, DataLayerError> {
        self.count_active_local_admin_users_with_valid_password()
            .await
    }
}
