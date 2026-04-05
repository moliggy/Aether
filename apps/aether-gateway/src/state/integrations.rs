use std::time::Duration;

use aether_contracts::{ExecutionPlan, ExecutionResult, ProxySnapshot};
use aether_data::repository::candidate_selection::StoredMinimalCandidateSelectionRow;
use aether_data::repository::candidates::{StoredRequestCandidate, UpsertRequestCandidateRecord};
use aether_data::repository::global_models::{
    AdminGlobalModelListQuery, AdminProviderModelListQuery, StoredAdminGlobalModelPage,
    StoredAdminProviderModel, UpsertAdminProviderModelRecord,
};
use aether_data::repository::provider_catalog::{
    StoredProviderCatalogEndpoint, StoredProviderCatalogKey, StoredProviderCatalogProvider,
};
use aether_data::repository::quota::StoredProviderQuotaSnapshot;
use aether_data::DataLayerError;
use aether_model_fetch::{
    aggregate_models_for_cache, model_fetch_interval_minutes, ModelFetchAssociationStore,
    ModelFetchTransportRuntime,
};
use aether_scheduler_core::SchedulerAffinityTarget;
use async_trait::async_trait;
use serde_json::Value;
use tracing::debug;

use super::{AppState, GatewayError};
use crate::model_fetch::ModelFetchRuntimeState;
use crate::provider_transport::{
    resolve_transport_proxy_snapshot_with_tunnel_affinity, GatewayProviderTransportSnapshot,
    LocalResolvedOAuthRequestAuth,
};
use crate::scheduler::{
    GatewayMinimalCandidateSelectionCandidate, SchedulerCandidateSelectionRowSource,
    SchedulerRequestCandidateRuntimeState, SchedulerRuntimeState,
};
use crate::{execution_runtime, provider_transport};

#[async_trait]
impl provider_transport::TransportTunnelAffinityLookup for AppState {
    async fn lookup_tunnel_attachment_owner(
        &self,
        node_id: &str,
    ) -> Result<Option<provider_transport::TransportTunnelAttachmentOwner>, String> {
        self.tunnel
            .lookup_attachment_owner(self.data.as_ref(), node_id)
            .await
            .map(|owner| {
                owner.map(|owner| provider_transport::TransportTunnelAttachmentOwner {
                    gateway_instance_id: owner.gateway_instance_id,
                    relay_base_url: owner.relay_base_url,
                    observed_at_unix_secs: owner.observed_at_unix_secs,
                })
            })
    }
}

#[async_trait]
impl provider_transport::VideoTaskTransportSnapshotLookup for AppState {
    async fn read_video_task_provider_transport_snapshot(
        &self,
        provider_id: &str,
        endpoint_id: &str,
        key_id: &str,
    ) -> Result<Option<GatewayProviderTransportSnapshot>, String> {
        self.read_provider_transport_snapshot(provider_id, endpoint_id, key_id)
            .await
            .map_err(|err| match err {
                GatewayError::UpstreamUnavailable { message, .. }
                | GatewayError::ControlUnavailable { message, .. }
                | GatewayError::Internal(message) => message,
            })
    }
}

#[async_trait]
impl ModelFetchTransportRuntime for AppState {
    async fn resolve_local_oauth_request_auth(
        &self,
        transport: &GatewayProviderTransportSnapshot,
    ) -> Result<Option<LocalResolvedOAuthRequestAuth>, String> {
        AppState::resolve_local_oauth_request_auth(self, transport)
            .await
            .map_err(|err| match err {
                GatewayError::UpstreamUnavailable { message, .. }
                | GatewayError::ControlUnavailable { message, .. }
                | GatewayError::Internal(message) => message,
            })
    }

    async fn resolve_model_fetch_proxy(
        &self,
        transport: &GatewayProviderTransportSnapshot,
    ) -> Option<ProxySnapshot> {
        resolve_transport_proxy_snapshot_with_tunnel_affinity(self, transport).await
    }
}

#[async_trait]
impl ModelFetchRuntimeState for AppState {
    fn has_provider_catalog_data_reader(&self) -> bool {
        AppState::has_provider_catalog_data_reader(self)
    }

    fn has_provider_catalog_data_writer(&self) -> bool {
        AppState::has_provider_catalog_data_writer(self)
    }

    async fn list_provider_catalog_providers(
        &self,
        active_only: bool,
    ) -> Result<Vec<StoredProviderCatalogProvider>, GatewayError> {
        AppState::list_provider_catalog_providers(self, active_only).await
    }

    async fn list_provider_catalog_endpoints_by_provider_ids(
        &self,
        provider_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogEndpoint>, GatewayError> {
        AppState::list_provider_catalog_endpoints_by_provider_ids(self, provider_ids).await
    }

    async fn read_provider_transport_snapshot(
        &self,
        provider_id: &str,
        endpoint_id: &str,
        key_id: &str,
    ) -> Result<Option<GatewayProviderTransportSnapshot>, GatewayError> {
        AppState::read_provider_transport_snapshot(self, provider_id, endpoint_id, key_id).await
    }

    async fn execute_execution_runtime_sync_plan(
        &self,
        plan: &ExecutionPlan,
    ) -> Result<ExecutionResult, GatewayError> {
        execution_runtime::execute_execution_runtime_sync_plan(self, None, plan).await
    }

    async fn update_provider_catalog_key(
        &self,
        key: &StoredProviderCatalogKey,
    ) -> Result<(), GatewayError> {
        AppState::update_provider_catalog_key(self, key).await?;
        Ok(())
    }

    async fn write_upstream_models_cache(
        &self,
        provider_id: &str,
        key_id: &str,
        cached_models: &[Value],
    ) {
        let Some(runner) = AppState::redis_kv_runner(self) else {
            return;
        };
        let Ok(serialized) = serde_json::to_string(&aggregate_models_for_cache(cached_models))
        else {
            return;
        };
        let cache_key = format!("upstream_models:{provider_id}:{key_id}");
        if let Err(err) = runner
            .setex(
                &cache_key,
                &serialized,
                Some(model_fetch_interval_minutes().saturating_mul(60)),
            )
            .await
        {
            debug!(
                provider_id = %provider_id,
                key_id = %key_id,
                error = %err,
                "gateway model fetch cache write failed"
            );
        }
    }
}

#[async_trait]
impl ModelFetchAssociationStore for AppState {
    type Error = String;

    fn has_global_model_reader(&self) -> bool {
        self.data.has_global_model_reader()
    }

    fn has_global_model_writer(&self) -> bool {
        self.data.has_global_model_writer()
    }

    fn model_fetch_internal_error(&self, message: String) -> Self::Error {
        message
    }

    async fn list_admin_provider_models(
        &self,
        query: &AdminProviderModelListQuery,
    ) -> Result<Vec<StoredAdminProviderModel>, Self::Error> {
        AppState::list_admin_provider_models(self, query)
            .await
            .map_err(|err| format!("{err:?}"))
    }

    async fn list_admin_global_models(
        &self,
        query: &AdminGlobalModelListQuery,
    ) -> Result<StoredAdminGlobalModelPage, Self::Error> {
        AppState::list_admin_global_models(self, query)
            .await
            .map_err(|err| format!("{err:?}"))
    }

    async fn create_admin_provider_model(
        &self,
        record: &UpsertAdminProviderModelRecord,
    ) -> Result<Option<StoredAdminProviderModel>, Self::Error> {
        AppState::create_admin_provider_model(self, record)
            .await
            .map_err(|err| format!("{err:?}"))
    }

    async fn list_provider_catalog_keys_by_provider_ids(
        &self,
        provider_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogKey>, Self::Error> {
        AppState::list_provider_catalog_keys_by_provider_ids(self, provider_ids)
            .await
            .map_err(|err| format!("{err:?}"))
    }

    async fn delete_admin_provider_model(
        &self,
        provider_id: &str,
        model_id: &str,
    ) -> Result<bool, Self::Error> {
        AppState::delete_admin_provider_model(self, provider_id, model_id)
            .await
            .map_err(|err| format!("{err:?}"))
    }
}

#[async_trait]
impl SchedulerRequestCandidateRuntimeState for AppState {
    fn has_request_candidate_data_writer(&self) -> bool {
        AppState::has_request_candidate_data_writer(self)
    }

    async fn read_request_candidates_by_request_id(
        &self,
        request_id: &str,
    ) -> Result<Vec<StoredRequestCandidate>, GatewayError> {
        AppState::read_request_candidates_by_request_id(self, request_id).await
    }

    async fn upsert_request_candidate(
        &self,
        candidate: UpsertRequestCandidateRecord,
    ) -> Result<Option<StoredRequestCandidate>, GatewayError> {
        AppState::upsert_request_candidate(self, candidate).await
    }
}

#[async_trait]
impl SchedulerCandidateSelectionRowSource for AppState {
    async fn read_minimal_candidate_selection_rows_for_api_format_and_global_model(
        &self,
        api_format: &str,
        global_model_name: &str,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        self.data
            .list_minimal_candidate_selection_rows(api_format, global_model_name)
            .await
    }

    async fn read_minimal_candidate_selection_rows_for_api_format(
        &self,
        api_format: &str,
    ) -> Result<Vec<StoredMinimalCandidateSelectionRow>, DataLayerError> {
        self.data
            .list_minimal_candidate_selection_rows_for_api_format(api_format)
            .await
    }
}

#[async_trait]
impl SchedulerRuntimeState for AppState {
    async fn read_provider_quota_snapshot(
        &self,
        provider_id: &str,
    ) -> Result<Option<StoredProviderQuotaSnapshot>, GatewayError> {
        AppState::read_provider_quota_snapshot(self, provider_id).await
    }

    async fn read_provider_catalog_providers_by_ids(
        &self,
        provider_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogProvider>, GatewayError> {
        AppState::read_provider_catalog_providers_by_ids(self, provider_ids).await
    }

    async fn read_provider_catalog_keys_by_ids(
        &self,
        key_ids: &[String],
    ) -> Result<Vec<StoredProviderCatalogKey>, GatewayError> {
        AppState::read_provider_catalog_keys_by_ids(self, key_ids).await
    }

    async fn read_recent_request_candidates(
        &self,
        limit: usize,
    ) -> Result<Vec<StoredRequestCandidate>, GatewayError> {
        AppState::read_recent_request_candidates(self, limit).await
    }

    async fn read_minimal_candidate_selection(
        &self,
        api_format: &str,
        global_model_name: &str,
        require_streaming: bool,
        auth_snapshot: Option<&crate::data::auth::GatewayAuthApiKeySnapshot>,
    ) -> Result<Vec<GatewayMinimalCandidateSelectionCandidate>, GatewayError> {
        AppState::read_minimal_candidate_selection(
            self,
            api_format,
            global_model_name,
            require_streaming,
            auth_snapshot,
        )
        .await
    }

    fn provider_key_rpm_reset_at(&self, key_id: &str, now_unix_secs: u64) -> Option<u64> {
        AppState::provider_key_rpm_reset_at(self, key_id, now_unix_secs)
    }

    fn read_cached_scheduler_affinity_target(
        &self,
        cache_key: &str,
        ttl: Duration,
    ) -> Option<SchedulerAffinityTarget> {
        AppState::read_scheduler_affinity_target(self, cache_key, ttl)
    }

    fn remember_scheduler_affinity_target(
        &self,
        cache_key: &str,
        target: SchedulerAffinityTarget,
        ttl: Duration,
        max_entries: usize,
    ) {
        AppState::remember_scheduler_affinity_target(self, cache_key, target, ttl, max_entries);
    }
}
