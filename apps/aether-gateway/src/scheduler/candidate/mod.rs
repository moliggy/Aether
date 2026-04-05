use self::affinity::{
    build_scheduler_affinity_cache_key, build_scheduler_affinity_cache_key_for_api_key_id,
    candidate_affinity_hash, candidate_key, remember_scheduler_affinity,
};
use self::model::{
    auth_snapshot_allows_api_format, auth_snapshot_constraints, candidate_model_names,
    candidate_supports_required_capability, matches_model_mapping, normalize_api_format,
    read_requested_model_rows, resolve_provider_model_name, resolve_requested_global_model_name,
    select_provider_model_name,
};
use self::selection::{
    is_candidate_selectable, read_provider_concurrent_limits, read_provider_key_rpm_states,
    reorder_candidates_by_scheduler_health, should_skip_provider_quota,
};
pub(crate) use self::state::{
    SchedulerCandidateSelectionRowSource, SchedulerRuntimeState,
};

mod affinity;
mod model;
mod selection;
mod state;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use aether_data::repository::candidate_selection::{
    StoredMinimalCandidateSelectionRow, StoredProviderModelMapping,
};
use aether_data::repository::candidates::StoredRequestCandidate;
use aether_data::repository::provider_catalog::{
    StoredProviderCatalogKey, StoredProviderCatalogProvider,
};
use aether_data::repository::quota::StoredProviderQuotaSnapshot;
use aether_data::DataLayerError;
use aether_scheduler_core::{
    auth_api_key_concurrency_limit_reached, build_minimal_candidate_selection,
    collect_global_model_names_for_required_capability, collect_selectable_candidates_from_keys,
    SchedulerAffinityTarget, SchedulerMinimalCandidateSelectionCandidate,
};
use aether_wallet::{ProviderBillingType, ProviderQuotaSnapshot};
use regex::Regex;
use sha2::{Digest, Sha256};

use crate::data::auth::GatewayAuthApiKeySnapshot;
use crate::{AppState, GatewayError};

const SCHEDULER_AFFINITY_TTL: Duration = Duration::from_secs(300);
#[cfg_attr(not(test), allow(dead_code))]
const SCHEDULER_AFFINITY_MAX_ENTRIES: usize = 10_000;

pub(crate) use aether_scheduler_core::SchedulerMinimalCandidateSelectionCandidate as GatewayMinimalCandidateSelectionCandidate;

#[allow(dead_code)]
pub(crate) async fn read_minimal_candidate_selection(
    state: &(impl SchedulerCandidateSelectionRowSource + Sync),
    api_format: &str,
    requested_model_name: &str,
    require_streaming: bool,
    auth_snapshot: Option<&GatewayAuthApiKeySnapshot>,
) -> Result<Vec<GatewayMinimalCandidateSelectionCandidate>, DataLayerError> {
    let normalized_api_format = normalize_api_format(api_format);
    if normalized_api_format.is_empty() {
        return Ok(Vec::new());
    }

    if !auth_snapshot_allows_api_format(auth_snapshot, &normalized_api_format) {
        return Ok(Vec::new());
    }

    let Some((resolved_global_model_name, rows)) =
        read_requested_model_rows(state, &normalized_api_format, requested_model_name).await?
    else {
        return Ok(Vec::new());
    };
    let auth_constraints = auth_snapshot.map(auth_snapshot_constraints);
    let affinity_key = auth_snapshot
        .map(|snapshot| snapshot.api_key_id.trim())
        .filter(|value| !value.is_empty());
    build_minimal_candidate_selection(
        rows,
        &normalized_api_format,
        requested_model_name,
        resolved_global_model_name.as_str(),
        require_streaming,
        auth_constraints.as_ref(),
        affinity_key,
    )
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn select_minimal_candidate(
    state: &AppState,
    api_format: &str,
    global_model_name: &str,
    require_streaming: bool,
    auth_snapshot: Option<&GatewayAuthApiKeySnapshot>,
    now_unix_secs: u64,
) -> Result<Option<GatewayMinimalCandidateSelectionCandidate>, GatewayError> {
    let affinity_cache_key =
        build_scheduler_affinity_cache_key(auth_snapshot, api_format, global_model_name);
    let selected = collect_selectable_candidates(
        state,
        api_format,
        global_model_name,
        require_streaming,
        auth_snapshot,
        now_unix_secs,
    )
    .await?
    .into_iter()
    .next();
    if let Some(candidate) = selected.as_ref() {
        remember_scheduler_affinity(affinity_cache_key.as_deref(), state, candidate);
    }
    Ok(selected)
}

pub(crate) async fn list_selectable_candidates(
    state: &AppState,
    api_format: &str,
    global_model_name: &str,
    require_streaming: bool,
    auth_snapshot: Option<&GatewayAuthApiKeySnapshot>,
    now_unix_secs: u64,
) -> Result<Vec<GatewayMinimalCandidateSelectionCandidate>, GatewayError> {
    collect_selectable_candidates(
        state,
        api_format,
        global_model_name,
        require_streaming,
        auth_snapshot,
        now_unix_secs,
    )
    .await
}

pub(crate) async fn list_selectable_candidates_for_required_capability_without_requested_model(
    state: &AppState,
    candidate_api_format: &str,
    required_capability: &str,
    require_streaming: bool,
    auth_snapshot: Option<&GatewayAuthApiKeySnapshot>,
    now_unix_secs: u64,
) -> Result<Vec<GatewayMinimalCandidateSelectionCandidate>, GatewayError> {
    let normalized_api_format = normalize_api_format(candidate_api_format);
    let required_capability = required_capability.trim();
    if normalized_api_format.is_empty() || required_capability.is_empty() {
        return Ok(Vec::new());
    }

    if !auth_snapshot_allows_api_format(auth_snapshot, &normalized_api_format) {
        return Ok(Vec::new());
    }

    let rows = state
        .read_minimal_candidate_selection_rows_for_api_format(&normalized_api_format)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    let auth_constraints = auth_snapshot.map(auth_snapshot_constraints);
    let model_names = collect_global_model_names_for_required_capability(
        rows,
        &normalized_api_format,
        required_capability,
        require_streaming,
        auth_constraints.as_ref(),
    );

    for global_model_name in model_names {
        let candidates = list_selectable_candidates(
            state,
            &normalized_api_format,
            &global_model_name,
            require_streaming,
            auth_snapshot,
            now_unix_secs,
        )
        .await?;
        let filtered = candidates
            .into_iter()
            .filter(|candidate| {
                candidate_supports_required_capability(candidate, required_capability)
            })
            .collect::<Vec<_>>();
        if !filtered.is_empty() {
            return Ok(filtered);
        }
    }

    Ok(Vec::new())
}

pub(crate) fn read_cached_scheduler_affinity_target(
    state: &AppState,
    api_key_id: &str,
    api_format: &str,
    global_model_name: &str,
) -> Option<SchedulerAffinityTarget> {
    let cache_key = build_scheduler_affinity_cache_key_for_api_key_id(
        api_key_id,
        api_format,
        global_model_name,
    )?;
    state.read_cached_scheduler_affinity_target(&cache_key, SCHEDULER_AFFINITY_TTL)
}

async fn collect_selectable_candidates(
    state: &AppState,
    api_format: &str,
    global_model_name: &str,
    require_streaming: bool,
    auth_snapshot: Option<&GatewayAuthApiKeySnapshot>,
    now_unix_secs: u64,
) -> Result<Vec<GatewayMinimalCandidateSelectionCandidate>, GatewayError> {
    let mut candidates = state
        .read_minimal_candidate_selection(
            api_format,
            global_model_name,
            require_streaming,
            auth_snapshot,
        )
        .await?;
    let recent_candidates = state.read_recent_request_candidates(128).await?;
    let provider_concurrent_limits = read_provider_concurrent_limits(state, &candidates).await?;
    let provider_key_rpm_states = read_provider_key_rpm_states(state, &candidates).await?;
    reorder_candidates_by_scheduler_health(
        &mut candidates,
        &provider_key_rpm_states,
        auth_snapshot,
    );
    let affinity_cache_key =
        build_scheduler_affinity_cache_key(auth_snapshot, api_format, global_model_name);
    let cached_affinity_target = affinity_cache_key.as_deref().and_then(|cache_key| {
        state.read_cached_scheduler_affinity_target(cache_key, SCHEDULER_AFFINITY_TTL)
    });

    if let Some((api_key_id, limit)) = auth_snapshot.and_then(|snapshot| {
        usize::try_from(snapshot.api_key_concurrent_limit?)
            .ok()
            .and_then(|limit| {
                if limit == 0 {
                    return None;
                }
                Some((snapshot.api_key_id.as_str(), limit))
            })
    }) {
        if auth_api_key_concurrency_limit_reached(
            &recent_candidates,
            now_unix_secs,
            api_key_id,
            limit,
        ) {
            return Ok(Vec::new());
        }
    }

    let mut selected_keys = BTreeSet::new();

    for candidate in &candidates {
        if !is_candidate_selectable(
            candidate,
            &recent_candidates,
            &provider_concurrent_limits,
            &provider_key_rpm_states,
            now_unix_secs,
            cached_affinity_target.as_ref(),
            state,
        )
        .await?
        {
            continue;
        }
        selected_keys.insert(candidate_key(candidate));
    }

    Ok(collect_selectable_candidates_from_keys(
        candidates,
        &selected_keys,
        cached_affinity_target.as_ref(),
    ))
}
