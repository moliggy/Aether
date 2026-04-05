use std::fmt;
use std::sync::Arc;

use super::PostgresBackend;
use crate::repository::announcements::AnnouncementWriteRepository;
use crate::repository::auth::AuthApiKeyWriteRepository;
use crate::repository::auth_modules::AuthModuleWriteRepository;
use crate::repository::candidates::RequestCandidateWriteRepository;
use crate::repository::gemini_file_mappings::GeminiFileMappingWriteRepository;
use crate::repository::global_models::GlobalModelWriteRepository;
use crate::repository::management_tokens::ManagementTokenWriteRepository;
use crate::repository::oauth_providers::OAuthProviderWriteRepository;
use crate::repository::provider_catalog::ProviderCatalogWriteRepository;
use crate::repository::proxy_nodes::ProxyNodeWriteRepository;
use crate::repository::quota::ProviderQuotaWriteRepository;
use crate::repository::settlement::SettlementWriteRepository;
use crate::repository::shadow_results::ShadowResultWriteRepository;
use crate::repository::usage::UsageWriteRepository;
use crate::repository::video_tasks::VideoTaskWriteRepository;
use crate::repository::wallet::WalletWriteRepository;

#[derive(Clone, Default)]
pub struct DataWriteRepositories {
    announcements: Option<Arc<dyn AnnouncementWriteRepository>>,
    auth_api_keys: Option<Arc<dyn AuthApiKeyWriteRepository>>,
    auth_modules: Option<Arc<dyn AuthModuleWriteRepository>>,
    shadow_results: Option<Arc<dyn ShadowResultWriteRepository>>,
    request_candidates: Option<Arc<dyn RequestCandidateWriteRepository>>,
    gemini_file_mappings: Option<Arc<dyn GeminiFileMappingWriteRepository>>,
    global_models: Option<Arc<dyn GlobalModelWriteRepository>>,
    management_tokens: Option<Arc<dyn ManagementTokenWriteRepository>>,
    oauth_providers: Option<Arc<dyn OAuthProviderWriteRepository>>,
    proxy_nodes: Option<Arc<dyn ProxyNodeWriteRepository>>,
    provider_catalog: Option<Arc<dyn ProviderCatalogWriteRepository>>,
    provider_quotas: Option<Arc<dyn ProviderQuotaWriteRepository>>,
    settlement: Option<Arc<dyn SettlementWriteRepository>>,
    usage: Option<Arc<dyn UsageWriteRepository>>,
    video_tasks: Option<Arc<dyn VideoTaskWriteRepository>>,
    wallets: Option<Arc<dyn WalletWriteRepository>>,
}

impl fmt::Debug for DataWriteRepositories {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataWriteRepositories")
            .field("has_announcements", &self.announcements.is_some())
            .field("has_auth_api_keys", &self.auth_api_keys.is_some())
            .field("has_auth_modules", &self.auth_modules.is_some())
            .field("has_shadow_results", &self.shadow_results.is_some())
            .field("has_request_candidates", &self.request_candidates.is_some())
            .field(
                "has_gemini_file_mappings",
                &self.gemini_file_mappings.is_some(),
            )
            .field("has_global_models", &self.global_models.is_some())
            .field("has_management_tokens", &self.management_tokens.is_some())
            .field("has_oauth_providers", &self.oauth_providers.is_some())
            .field("has_proxy_nodes", &self.proxy_nodes.is_some())
            .field("has_provider_catalog", &self.provider_catalog.is_some())
            .field("has_provider_quotas", &self.provider_quotas.is_some())
            .field("has_settlement", &self.settlement.is_some())
            .field("has_usage", &self.usage.is_some())
            .field("has_video_tasks", &self.video_tasks.is_some())
            .field("has_wallets", &self.wallets.is_some())
            .finish()
    }
}

impl DataWriteRepositories {
    pub(crate) fn from_postgres(postgres: Option<&PostgresBackend>) -> Self {
        Self {
            announcements: postgres.map(PostgresBackend::announcement_write_repository),
            auth_api_keys: postgres.map(PostgresBackend::auth_api_key_write_repository),
            auth_modules: postgres.map(PostgresBackend::auth_module_write_repository),
            shadow_results: postgres.map(PostgresBackend::shadow_result_write_repository),
            request_candidates: postgres.map(PostgresBackend::request_candidate_write_repository),
            gemini_file_mappings: postgres
                .map(PostgresBackend::gemini_file_mapping_write_repository),
            global_models: postgres.map(PostgresBackend::global_model_write_repository),
            management_tokens: postgres.map(PostgresBackend::management_token_write_repository),
            oauth_providers: postgres.map(PostgresBackend::oauth_provider_write_repository),
            proxy_nodes: postgres.map(PostgresBackend::proxy_node_write_repository),
            provider_catalog: postgres.map(PostgresBackend::provider_catalog_write_repository),
            provider_quotas: postgres.map(PostgresBackend::provider_quota_write_repository),
            settlement: postgres.map(PostgresBackend::settlement_write_repository),
            usage: postgres.map(PostgresBackend::usage_write_repository),
            video_tasks: postgres.map(PostgresBackend::video_task_write_repository),
            wallets: postgres.map(PostgresBackend::wallet_write_repository),
        }
    }

    pub fn shadow_results(&self) -> Option<Arc<dyn ShadowResultWriteRepository>> {
        self.shadow_results.clone()
    }

    pub fn announcements(&self) -> Option<Arc<dyn AnnouncementWriteRepository>> {
        self.announcements.clone()
    }

    pub fn auth_api_keys(&self) -> Option<Arc<dyn AuthApiKeyWriteRepository>> {
        self.auth_api_keys.clone()
    }

    pub fn auth_modules(&self) -> Option<Arc<dyn AuthModuleWriteRepository>> {
        self.auth_modules.clone()
    }

    pub fn usage(&self) -> Option<Arc<dyn UsageWriteRepository>> {
        self.usage.clone()
    }

    pub fn request_candidates(&self) -> Option<Arc<dyn RequestCandidateWriteRepository>> {
        self.request_candidates.clone()
    }

    pub fn gemini_file_mappings(&self) -> Option<Arc<dyn GeminiFileMappingWriteRepository>> {
        self.gemini_file_mappings.clone()
    }

    pub fn global_models(&self) -> Option<Arc<dyn GlobalModelWriteRepository>> {
        self.global_models.clone()
    }

    pub fn management_tokens(&self) -> Option<Arc<dyn ManagementTokenWriteRepository>> {
        self.management_tokens.clone()
    }

    pub fn oauth_providers(&self) -> Option<Arc<dyn OAuthProviderWriteRepository>> {
        self.oauth_providers.clone()
    }

    pub fn proxy_nodes(&self) -> Option<Arc<dyn ProxyNodeWriteRepository>> {
        self.proxy_nodes.clone()
    }

    pub fn provider_quotas(&self) -> Option<Arc<dyn ProviderQuotaWriteRepository>> {
        self.provider_quotas.clone()
    }

    pub fn provider_catalog(&self) -> Option<Arc<dyn ProviderCatalogWriteRepository>> {
        self.provider_catalog.clone()
    }

    pub fn settlement(&self) -> Option<Arc<dyn SettlementWriteRepository>> {
        self.settlement.clone()
    }

    pub fn video_tasks(&self) -> Option<Arc<dyn VideoTaskWriteRepository>> {
        self.video_tasks.clone()
    }

    pub fn wallets(&self) -> Option<Arc<dyn WalletWriteRepository>> {
        self.wallets.clone()
    }

    pub fn has_any(&self) -> bool {
        self.announcements.is_some()
            || self.auth_api_keys.is_some()
            || self.auth_modules.is_some()
            || self.shadow_results.is_some()
            || self.request_candidates.is_some()
            || self.gemini_file_mappings.is_some()
            || self.global_models.is_some()
            || self.management_tokens.is_some()
            || self.oauth_providers.is_some()
            || self.proxy_nodes.is_some()
            || self.provider_catalog.is_some()
            || self.provider_quotas.is_some()
            || self.settlement.is_some()
            || self.usage.is_some()
            || self.video_tasks.is_some()
            || self.wallets.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::DataWriteRepositories;
    use crate::backends::PostgresBackend;
    use crate::postgres::PostgresPoolConfig;

    #[tokio::test]
    async fn builds_shadow_result_writer_from_postgres_backend() {
        let backend = PostgresBackend::from_config(PostgresPoolConfig {
            database_url: "postgres://localhost/aether".to_string(),
            min_connections: 1,
            max_connections: 4,
            acquire_timeout_ms: 1_000,
            idle_timeout_ms: 5_000,
            max_lifetime_ms: 30_000,
            statement_cache_capacity: 64,
            require_ssl: false,
        })
        .expect("postgres backend should build");

        let write = DataWriteRepositories::from_postgres(Some(&backend));

        assert!(write.has_any());
        assert!(write.announcements().is_some());
        assert!(write.auth_api_keys().is_some());
        assert!(write.auth_modules().is_some());
        assert!(write.shadow_results().is_some());
        assert!(write.request_candidates().is_some());
        assert!(write.gemini_file_mappings().is_some());
        assert!(write.global_models().is_some());
        assert!(write.management_tokens().is_some());
        assert!(write.oauth_providers().is_some());
        assert!(write.proxy_nodes().is_some());
        assert!(write.provider_catalog().is_some());
        assert!(write.provider_quotas().is_some());
        assert!(write.settlement().is_some());
        assert!(write.usage().is_some());
        assert!(write.video_tasks().is_some());
        assert!(write.wallets().is_some());
    }
}
