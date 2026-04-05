use std::time::Duration;

use aether_data::repository::candidate_selection::StoredMinimalCandidateSelectionRow;
use aether_data::repository::candidates::StoredRequestCandidate;
use aether_data::repository::provider_catalog::{
    StoredProviderCatalogKey, StoredProviderCatalogProvider,
};
use aether_data::repository::quota::StoredProviderQuotaSnapshot;
use aether_data::DataLayerError;
use aether_scheduler_core::SchedulerAffinityTarget;
use async_trait::async_trait;

use crate::GatewayError;

use super::GatewayMinimalCandidateSelectionCandidate;

#[async_trait]
pub(crate) trait SchedulerCandidateSelectionRowSource {
    async fn read_minimal_candidate_selection_rows_for_api_format_and_global_model(
        &self,
        api_format: &str,
        global_model_name: &str,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError>;

    async fn read_minimal_candidate_selection_rows_for_api_format(
        &self,
        api_format: &str,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError>;
}

#[async_trait]
pub(crate) trait SchedulerRuntimeState {
    async fn read_provider_quota_snapshot(
        &self,
        provider_id: &str,
    ) -> Result<Option<StoredProviderQuotaSnapshot>, GatewayError>;

    async fn read_provider_catalog_providers_by_ids(
        &self,
        provider_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogProvider>, GatewayError>;

    async fn read_provider_catalog_keys_by_ids(
        &self,
        key_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogKey>, GatewayError>;

    async fn read_recent_request_candidates(
        &self,
        limit: usize,
    ) -> Result<Vec<StoredRequestCandidate>, GatewayError>;

    async fn read_minimal_candidate_selection(
        &self,
        api_format: &str,
        global_model_name: &str,
        require_streaming: bool,
        auth_snapshot: Option<&crate::data::auth::GatewayAuthApiKeySnapshot>,
    ) -> Result<Vec<GatewayMinimalCandidateSelectionCandidate>, GatewayError>;

    fn provider_key_rpm_reset_at(&self, key_id: &str, now_unix_secs: u64) -> Option<u64>;

    fn read_cached_scheduler_affinity_target(
        &self,
        cache_key: &str,
        ttl: Duration,
    ) -> Option<SchedulerAffinityTarget>;

    fn remember_scheduler_affinity_target(
        &self,
        cache_key: &str,
        target: SchedulerAffinityTarget,
        ttl: Duration,
        max_entries: usize,
    );
}
