use crate::{AppState, GatewayError};
use aether_data_contracts::repository::{candidate_selection, candidates, quota};

impl AppState {
    pub(crate) async fn list_minimal_candidate_selection_rows_for_api_format(
        &self,
        api_format: &str,
    ) -> Result<Vec<candidate_selection::StoredMinimalCandidateSelectionRow>, GatewayError> {
        self.data
            .list_minimal_candidate_selection_rows_for_api_format(api_format)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_minimal_candidate_selection_rows_for_api_format_and_global_model(
        &self,
        api_format: &str,
        global_model_name: &str,
    ) -> Result<Vec<candidate_selection::StoredMinimalCandidateSelectionRow>, GatewayError> {
        self.data
            .list_minimal_candidate_selection_rows(api_format, global_model_name)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_minimal_candidate_selection_rows_for_api_format_and_requested_model(
        &self,
        api_format: &str,
        requested_model_name: &str,
    ) -> Result<Vec<candidate_selection::StoredMinimalCandidateSelectionRow>, GatewayError> {
        self.data
            .list_minimal_candidate_selection_rows_for_requested_model(
                api_format,
                requested_model_name,
            )
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_minimal_candidate_selection_rows_for_api_format_and_requested_model_page(
        &self,
        query: &candidate_selection::StoredRequestedModelCandidateRowsQuery,
    ) -> Result<Vec<candidate_selection::StoredMinimalCandidateSelectionRow>, GatewayError> {
        self.data
            .list_minimal_candidate_selection_rows_for_requested_model_page(query)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn list_pool_key_candidate_rows_for_group(
        &self,
        query: &candidate_selection::StoredPoolKeyCandidateRowsQuery,
    ) -> Result<Vec<candidate_selection::StoredMinimalCandidateSelectionRow>, GatewayError> {
        self.data
            .list_pool_key_candidate_rows_for_group(query)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn read_provider_quota_snapshot(
        &self,
        provider_id: &str,
    ) -> Result<Option<quota::StoredProviderQuotaSnapshot>, GatewayError> {
        self.data
            .find_provider_quota_by_provider_id(provider_id)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn read_provider_quota_snapshots(
        &self,
        provider_ids: &[String],
    ) -> Result<Vec<quota::StoredProviderQuotaSnapshot>, GatewayError> {
        self.data
            .find_provider_quotas_by_provider_ids(provider_ids)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn read_recent_request_candidates(
        &self,
        limit: usize,
    ) -> Result<Vec<candidates::StoredRequestCandidate>, GatewayError> {
        self.data
            .list_recent_request_candidates(limit)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }

    pub(crate) async fn upsert_request_candidate(
        &self,
        candidate: candidates::UpsertRequestCandidateRecord,
    ) -> Result<Option<candidates::StoredRequestCandidate>, GatewayError> {
        self.data
            .upsert_request_candidate(candidate)
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))
    }
}
