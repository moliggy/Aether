use aether_data::repository::candidate_selection::StoredMinimalCandidateSelectionRow;
use aether_data::DataLayerError;
pub(super) use aether_scheduler_core::{
    auth_constraints_allow_api_format, auth_constraints_allow_model,
    auth_constraints_allow_provider, candidate_model_names, candidate_supports_required_capability,
    extract_global_priority_for_format, matches_model_mapping, normalize_api_format,
    resolve_provider_model_name, resolve_requested_global_model_name,
    row_supports_required_capability, select_provider_model_name, SchedulerAuthConstraints,
};

use crate::data::auth::GatewayAuthApiKeySnapshot;

use super::state::SchedulerCandidateSelectionRowSource;
use super::GatewayMinimalCandidateSelectionCandidate;

pub(super) fn auth_snapshot_allows_provider(
    auth_snapshot: Option<&GatewayAuthApiKeySnapshot>,
    provider_id: &str,
    provider_name: &str,
) -> bool {
    auth_constraints_allow_provider(
        auth_snapshot.map(auth_snapshot_constraints).as_ref(),
        provider_id,
        provider_name,
    )
}

pub(super) fn auth_snapshot_allows_api_format(
    auth_snapshot: Option<&GatewayAuthApiKeySnapshot>,
    api_format: &str,
) -> bool {
    auth_constraints_allow_api_format(
        auth_snapshot.map(auth_snapshot_constraints).as_ref(),
        api_format,
    )
}

pub(super) fn auth_snapshot_allows_model(
    auth_snapshot: Option<&GatewayAuthApiKeySnapshot>,
    requested_model_name: &str,
    resolved_global_model_name: &str,
) -> bool {
    auth_constraints_allow_model(
        auth_snapshot.map(auth_snapshot_constraints).as_ref(),
        requested_model_name,
        resolved_global_model_name,
    )
}

pub(super) async fn read_requested_model_rows(
    state: &(impl SchedulerCandidateSelectionRowSource + Sync),
    api_format: &str,
    requested_model_name: &str,
) -> Result<Option<(String, Vec<StoredMinimalCandidateSelectionRow>)>, DataLayerError> {
    let exact_rows = state
        .read_minimal_candidate_selection_rows_for_api_format_and_global_model(
            api_format,
            requested_model_name,
        )
        .await?;
    if !exact_rows.is_empty() {
        return Ok(Some((requested_model_name.to_string(), exact_rows)));
    }

    let rows = state
        .read_minimal_candidate_selection_rows_for_api_format(api_format)
        .await?;
    let Some(resolved_global_model_name) =
        resolve_requested_global_model_name(&rows, requested_model_name, api_format)
    else {
        return Ok(None);
    };

    Ok(Some((
        resolved_global_model_name.clone(),
        rows.into_iter()
            .filter(|row| row.global_model_name == resolved_global_model_name)
            .collect(),
    )))
}

pub(super) fn auth_snapshot_constraints(
    snapshot: &GatewayAuthApiKeySnapshot,
) -> SchedulerAuthConstraints {
    SchedulerAuthConstraints {
        allowed_providers: snapshot
            .effective_allowed_providers()
            .map(|items| items.to_vec()),
        allowed_api_formats: snapshot
            .effective_allowed_api_formats()
            .map(|items| items.to_vec()),
        allowed_models: snapshot
            .effective_allowed_models()
            .map(|items| items.to_vec()),
    }
}
