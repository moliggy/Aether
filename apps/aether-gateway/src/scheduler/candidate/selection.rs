use std::collections::{BTreeMap, BTreeSet};

use aether_data::repository::candidates::StoredRequestCandidate;
use aether_data::repository::provider_catalog::StoredProviderCatalogKey;
use aether_scheduler_core::{
    build_provider_concurrent_limit_map, candidate_is_selectable_with_runtime_state,
    reorder_candidates_by_scheduler_health as reorder_candidates_by_scheduler_health_in_core,
    SchedulerAffinityTarget,
};

use crate::data::auth::GatewayAuthApiKeySnapshot;
use crate::GatewayError;

use super::{GatewayMinimalCandidateSelectionCandidate, SchedulerRuntimeState};

pub(super) fn reorder_candidates_by_scheduler_health(
    candidates: &mut [GatewayMinimalCandidateSelectionCandidate],
    provider_key_rpm_states: &BTreeMap<String, StoredProviderCatalogKey>,
    auth_snapshot: Option<&GatewayAuthApiKeySnapshot>,
) {
    let affinity_key = auth_snapshot
        .map(|snapshot| snapshot.api_key_id.trim())
        .filter(|value| !value.is_empty());
    reorder_candidates_by_scheduler_health_in_core(
        candidates,
        provider_key_rpm_states,
        affinity_key,
    );
}

pub(super) use aether_scheduler_core::should_skip_provider_quota;

pub(super) async fn is_candidate_selectable(
    candidate: &GatewayMinimalCandidateSelectionCandidate,
    recent_candidates: &[StoredRequestCandidate],
    provider_concurrent_limits: &BTreeMap<String, usize>,
    provider_key_rpm_states: &BTreeMap<String, StoredProviderCatalogKey>,
    now_unix_secs: u64,
    cached_affinity_target: Option<&SchedulerAffinityTarget>,
    state: &(impl SchedulerRuntimeState + ?Sized),
) -> Result<bool, GatewayError> {
    let provider_quota_blocks_requests = state
        .read_provider_quota_snapshot(&candidate.provider_id)
        .await?
        .as_ref()
        .is_some_and(|quota| should_skip_provider_quota(quota, now_unix_secs));
    let rpm_reset_at = state.provider_key_rpm_reset_at(candidate.key_id.as_str(), now_unix_secs);

    Ok(candidate_is_selectable_with_runtime_state(
        candidate,
        recent_candidates,
        provider_concurrent_limits,
        provider_key_rpm_states,
        now_unix_secs,
        cached_affinity_target,
        provider_quota_blocks_requests,
        rpm_reset_at,
    ))
}

pub(super) async fn read_provider_concurrent_limits(
    state: &(impl SchedulerRuntimeState + ?Sized),
    candidates: &[GatewayMinimalCandidateSelectionCandidate],
) -> Result<BTreeMap<String, usize>, GatewayError> {
    let provider_ids = candidates
        .iter()
        .map(|candidate| candidate.provider_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if provider_ids.is_empty() {
        return Ok(BTreeMap::new());
    }

    let providers = state
        .read_provider_catalog_providers_by_ids(&provider_ids)
        .await?;
    Ok(build_provider_concurrent_limit_map(providers))
}

pub(super) async fn read_provider_key_rpm_states(
    state: &(impl SchedulerRuntimeState + ?Sized),
    candidates: &[GatewayMinimalCandidateSelectionCandidate],
) -> Result<BTreeMap<String, StoredProviderCatalogKey>, GatewayError> {
    let key_ids = candidates
        .iter()
        .map(|candidate| candidate.key_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if key_ids.is_empty() {
        return Ok(BTreeMap::new());
    }

    let keys = state.read_provider_catalog_keys_by_ids(&key_ids).await?;
    Ok(keys
        .into_iter()
        .map(|key| (key.id.clone(), key))
        .collect::<BTreeMap<_, _>>())
}
