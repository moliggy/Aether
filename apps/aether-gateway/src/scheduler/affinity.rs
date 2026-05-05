use std::time::Duration;

use aether_scheduler_core::{
    build_scheduler_affinity_cache_key_for_api_key_id_with_client_session, ClientSessionAffinity,
    SchedulerAffinityTarget,
};

use super::state::SchedulerRuntimeState;

pub(crate) const SCHEDULER_AFFINITY_TTL: Duration = Duration::from_secs(300);

pub(crate) fn read_cached_scheduler_affinity_target(
    state: &(impl SchedulerRuntimeState + ?Sized),
    api_key_id: &str,
    client_session_affinity: Option<&ClientSessionAffinity>,
    api_format: &str,
    global_model_name: &str,
) -> Option<SchedulerAffinityTarget> {
    let cache_key = build_scheduler_affinity_cache_key_for_api_key_id_with_client_session(
        api_key_id,
        api_format,
        global_model_name,
        client_session_affinity,
    )?;
    state.read_cached_scheduler_affinity_target(&cache_key, SCHEDULER_AFFINITY_TTL)
}
