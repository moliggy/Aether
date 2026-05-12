use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aether_data_contracts::repository::global_models::AdminProviderModelListQuery;
use aether_data_contracts::repository::pool_scores::GetPoolMemberScoresByIdsQuery;
use aether_data_contracts::repository::provider_catalog::{
    StoredProviderCatalogEndpoint, StoredProviderCatalogKey, StoredProviderCatalogProvider,
};
use tracing::{debug, info, warn};

use crate::admin_api::admin_provider_pool_config;
use crate::ai_serving::build_provider_key_pool_score_upsert;
use crate::handlers::shared::provider_pool::AdminProviderPoolConfig;
use crate::{AppState, GatewayError};

const POOL_SCORE_REBUILD_DEFAULT_INTERVAL_SECONDS: u64 = 300;
const POOL_SCORE_REBUILD_MIN_INTERVAL_SECONDS: u64 = 30;
const POOL_SCORE_REBUILD_DEFAULT_MAX_UPSERTS_PER_TICK: usize = 20_000;
const POOL_SCORE_REBUILD_PROVIDER_CURSOR_KEY: &str = "ap:pool_score_rebuild:provider_cursor";
const POOL_SCORE_REBUILD_PROVIDER_OFFSET_PREFIX: &str = "ap:pool_score_rebuild:provider_offset";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PoolScoreRebuildRunSummary {
    pub(crate) providers_checked: usize,
    pub(crate) providers_scored: usize,
    pub(crate) keys_seen: usize,
    pub(crate) scores_upserted: usize,
}

impl PoolScoreRebuildRunSummary {
    const fn empty() -> Self {
        Self {
            providers_checked: 0,
            providers_scored: 0,
            keys_seen: 0,
            scores_upserted: 0,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PoolScoreRebuildWorkerConfig {
    pub(crate) interval: Duration,
    pub(crate) max_upserts_per_tick: usize,
}

impl PoolScoreRebuildWorkerConfig {
    fn from_env() -> Self {
        let interval_seconds = env_u64(
            "POOL_SCORE_REBUILD_INTERVAL_SECONDS",
            POOL_SCORE_REBUILD_DEFAULT_INTERVAL_SECONDS,
        )
        .max(POOL_SCORE_REBUILD_MIN_INTERVAL_SECONDS);
        let max_upserts_per_tick = env_usize(
            "POOL_SCORE_REBUILD_MAX_UPSERTS_PER_TICK",
            POOL_SCORE_REBUILD_DEFAULT_MAX_UPSERTS_PER_TICK,
        )
        .max(1);
        Self {
            interval: Duration::from_secs(interval_seconds),
            max_upserts_per_tick,
        }
    }
}

fn env_u64(name: &str, default_value: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(default_value)
}

fn env_usize(name: &str, default_value: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(default_value)
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

async fn load_runtime_usize(state: &AppState, key: &str) -> usize {
    state
        .runtime_state
        .kv_get(key)
        .await
        .ok()
        .flatten()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(0)
}

async fn store_runtime_usize(state: &AppState, key: &str, value: usize) {
    if let Err(err) = state
        .runtime_state
        .kv_set(key, value.to_string(), None)
        .await
    {
        debug!(
            key,
            error = ?err,
            "gateway pool score rebuild: failed to store cursor"
        );
    }
}

fn provider_offset_cursor_key(provider_id: &str) -> String {
    format!("{POOL_SCORE_REBUILD_PROVIDER_OFFSET_PREFIX}:{provider_id}")
}

fn score_combo_indices(
    flat_index: usize,
    model_count: usize,
    key_count: usize,
) -> (usize, usize, usize) {
    let endpoint_stride = model_count.saturating_mul(key_count).max(1);
    let endpoint_index = flat_index / endpoint_stride;
    let remainder = flat_index % endpoint_stride;
    let model_index = remainder / key_count.max(1);
    let key_index = remainder % key_count.max(1);
    (endpoint_index, model_index, key_index)
}

#[derive(Debug, Clone)]
struct ProviderScoreBuildItem {
    endpoint_index: usize,
    model_index: usize,
    key_index: usize,
    score_id: String,
}

pub(crate) async fn ensure_provider_key_pool_scores_for_keys(
    state: &AppState,
    provider: &StoredProviderCatalogProvider,
    pool_config: &AdminProviderPoolConfig,
    endpoints: &[StoredProviderCatalogEndpoint],
    keys: &[StoredProviderCatalogKey],
    now_unix_secs: u64,
    max_upserts: usize,
) -> Result<usize, GatewayError> {
    if max_upserts == 0
        || keys.is_empty()
        || !state.data.has_pool_score_reader()
        || !state.data.has_pool_score_writer()
    {
        return Ok(0);
    }

    let endpoints = endpoints
        .iter()
        .filter(|endpoint| endpoint.is_active && !endpoint.api_format.trim().is_empty())
        .collect::<Vec<_>>();
    let keys = keys
        .iter()
        .filter(|key| key.is_active && key.provider_id == provider.id)
        .collect::<Vec<_>>();
    if endpoints.is_empty() || keys.is_empty() {
        return Ok(0);
    }

    let models = state
        .list_admin_provider_models(&AdminProviderModelListQuery {
            provider_id: provider.id.clone(),
            is_active: Some(true),
            offset: 0,
            limit: 10_000,
        })
        .await?
        .into_iter()
        .filter(|model| model.is_available)
        .collect::<Vec<_>>();
    if models.is_empty() {
        return Ok(0);
    }

    let max_items = endpoints
        .len()
        .saturating_mul(models.len())
        .saturating_mul(keys.len())
        .min(max_upserts);
    let mut build_items = Vec::with_capacity(max_items);
    'outer: for (endpoint_index, endpoint) in endpoints.iter().enumerate() {
        for (model_index, model) in models.iter().enumerate() {
            for (key_index, key) in keys.iter().enumerate() {
                let draft = build_provider_key_pool_score_upsert(
                    key,
                    provider.provider_type.as_str(),
                    endpoint.api_format.trim(),
                    Some(model.id.as_str()),
                    None,
                    now_unix_secs,
                    pool_config.score_rules,
                );
                build_items.push(ProviderScoreBuildItem {
                    endpoint_index,
                    model_index,
                    key_index,
                    score_id: draft.id,
                });
                if build_items.len() >= max_upserts {
                    break 'outer;
                }
            }
        }
    }
    if build_items.is_empty() {
        return Ok(0);
    }

    let existing_score_ids = state
        .data
        .get_pool_member_scores_by_ids(&GetPoolMemberScoresByIdsQuery {
            ids: build_items
                .iter()
                .map(|item| item.score_id.clone())
                .collect(),
        })
        .await
        .unwrap_or_else(|err| {
            debug!(
                provider_id = %provider.id,
                error = ?err,
                "gateway pool score ensure: failed to read existing scores by id"
            );
            Vec::new()
        })
        .into_iter()
        .map(|score| score.id)
        .collect::<std::collections::BTreeSet<_>>();

    let mut upserted = 0usize;
    for item in &build_items {
        if existing_score_ids.contains(&item.score_id) {
            continue;
        }
        let endpoint = endpoints[item.endpoint_index];
        let model = &models[item.model_index];
        let key = keys[item.key_index];
        let upsert = build_provider_key_pool_score_upsert(
            key,
            provider.provider_type.as_str(),
            endpoint.api_format.trim(),
            Some(model.id.as_str()),
            None,
            now_unix_secs,
            pool_config.score_rules,
        );
        if state
            .data
            .upsert_pool_member_score(upsert)
            .await
            .map_err(|err| GatewayError::Internal(format!("{err:?}")))?
            .is_some()
        {
            upserted = upserted.saturating_add(1);
        }
    }

    Ok(upserted)
}

pub(crate) async fn perform_pool_score_rebuild_once_with_config(
    state: &AppState,
    config: PoolScoreRebuildWorkerConfig,
) -> Result<PoolScoreRebuildRunSummary, GatewayError> {
    if !state.has_provider_catalog_data_reader()
        || !state.data.has_pool_score_reader()
        || !state.data.has_pool_score_writer()
    {
        return Ok(PoolScoreRebuildRunSummary::empty());
    }

    let mut providers = state
        .list_provider_catalog_providers(true)
        .await?
        .into_iter()
        .filter_map(|provider| {
            admin_provider_pool_config(&provider).map(|config| (provider, config))
        })
        .collect::<Vec<_>>();
    providers.sort_by(|left, right| left.0.id.cmp(&right.0.id));
    if providers.is_empty() {
        return Ok(PoolScoreRebuildRunSummary::empty());
    }

    let provider_ids = providers
        .iter()
        .map(|(provider, _)| provider.id.clone())
        .collect::<Vec<_>>();
    let mut endpoints_by_provider = BTreeMap::new();
    for endpoint in state
        .list_provider_catalog_endpoints_by_provider_ids(&provider_ids)
        .await?
        .into_iter()
        .filter(|endpoint| endpoint.is_active)
    {
        endpoints_by_provider
            .entry(endpoint.provider_id.clone())
            .or_insert_with(Vec::new)
            .push(endpoint);
    }

    let mut keys_by_provider = BTreeMap::new();
    for key in state
        .list_provider_catalog_keys_by_provider_ids(&provider_ids)
        .await?
    {
        keys_by_provider
            .entry(key.provider_id.clone())
            .or_insert_with(Vec::new)
            .push(key);
    }

    let now = now_unix_secs();
    let mut summary = PoolScoreRebuildRunSummary {
        providers_checked: providers.len(),
        ..PoolScoreRebuildRunSummary::empty()
    };

    let start_provider_index =
        load_runtime_usize(state, POOL_SCORE_REBUILD_PROVIDER_CURSOR_KEY).await % providers.len();
    let mut last_provider_index = None;
    for provider_index in
        (0..providers.len()).map(|offset| (start_provider_index + offset) % providers.len())
    {
        if summary.scores_upserted >= config.max_upserts_per_tick {
            break;
        }
        last_provider_index = Some(provider_index);
        let (provider, pool_config) = providers[provider_index].clone();
        let endpoints = endpoints_by_provider
            .remove(&provider.id)
            .unwrap_or_default();
        let keys = keys_by_provider.remove(&provider.id).unwrap_or_default();
        if endpoints.is_empty() || keys.is_empty() {
            continue;
        }
        let models = state
            .list_admin_provider_models(&AdminProviderModelListQuery {
                provider_id: provider.id.clone(),
                is_active: Some(true),
                offset: 0,
                limit: 10_000,
            })
            .await?
            .into_iter()
            .filter(|model| model.is_available)
            .collect::<Vec<_>>();
        if models.is_empty() {
            continue;
        }

        let total_combinations = endpoints
            .len()
            .saturating_mul(models.len())
            .saturating_mul(keys.len());
        if total_combinations == 0 {
            continue;
        }
        let provider_cursor_key = provider_offset_cursor_key(&provider.id);
        let provider_cursor =
            load_runtime_usize(state, &provider_cursor_key).await % total_combinations.max(1);
        let remaining_budget = config
            .max_upserts_per_tick
            .saturating_sub(summary.scores_upserted);
        let provider_budget = remaining_budget.min(total_combinations);
        let mut build_items = Vec::with_capacity(provider_budget);
        for offset in 0..provider_budget {
            let flat_index = (provider_cursor + offset) % total_combinations;
            let (endpoint_index, model_index, key_index) =
                score_combo_indices(flat_index, models.len(), keys.len());
            let endpoint = &endpoints[endpoint_index];
            let api_format = endpoint.api_format.trim();
            if api_format.is_empty() {
                continue;
            }
            let model = &models[model_index];
            let key = &keys[key_index];
            let draft = build_provider_key_pool_score_upsert(
                key,
                provider.provider_type.as_str(),
                api_format,
                Some(model.id.as_str()),
                None,
                now,
                pool_config.score_rules,
            );
            build_items.push(ProviderScoreBuildItem {
                endpoint_index,
                model_index,
                key_index,
                score_id: draft.id,
            });
        }
        if build_items.is_empty() {
            store_runtime_usize(
                state,
                &provider_cursor_key,
                (provider_cursor + provider_budget) % total_combinations,
            )
            .await;
            continue;
        }
        let existing_scores = state
            .data
            .get_pool_member_scores_by_ids(&GetPoolMemberScoresByIdsQuery {
                ids: build_items
                    .iter()
                    .map(|item| item.score_id.clone())
                    .collect(),
            })
            .await
            .unwrap_or_else(|err| {
                debug!(
                    provider_id = %provider.id,
                    error = ?err,
                    "gateway pool score rebuild: failed to read existing scores by id"
                );
                Vec::new()
            })
            .into_iter()
            .map(|score| (score.id.clone(), score))
            .collect::<BTreeMap<_, _>>();
        let mut provider_upserts = 0usize;
        summary.keys_seen = summary.keys_seen.saturating_add(keys.len());
        for item in &build_items {
            if summary.scores_upserted >= config.max_upserts_per_tick {
                break;
            }
            let endpoint = &endpoints[item.endpoint_index];
            let model = &models[item.model_index];
            let key = &keys[item.key_index];
            let existing = existing_scores.get(&item.score_id);
            let upsert = build_provider_key_pool_score_upsert(
                key,
                provider.provider_type.as_str(),
                endpoint.api_format.trim(),
                Some(model.id.as_str()),
                existing,
                now,
                pool_config.score_rules,
            );
            if state
                .data
                .upsert_pool_member_score(upsert)
                .await
                .map_err(|err| GatewayError::Internal(format!("{err:?}")))?
                .is_some()
            {
                summary.scores_upserted = summary.scores_upserted.saturating_add(1);
                provider_upserts = provider_upserts.saturating_add(1);
            }
        }
        store_runtime_usize(
            state,
            &provider_cursor_key,
            (provider_cursor + provider_budget) % total_combinations,
        )
        .await;
        if provider_upserts > 0 {
            summary.providers_scored = summary.providers_scored.saturating_add(1);
        }
    }
    if let Some(last_provider_index) = last_provider_index {
        store_runtime_usize(
            state,
            POOL_SCORE_REBUILD_PROVIDER_CURSOR_KEY,
            (last_provider_index + 1) % providers.len(),
        )
        .await;
    }

    Ok(summary)
}

pub(crate) async fn perform_pool_score_rebuild_once(
    state: &AppState,
) -> Result<PoolScoreRebuildRunSummary, GatewayError> {
    perform_pool_score_rebuild_once_with_config(state, PoolScoreRebuildWorkerConfig::from_env())
        .await
}

pub(crate) fn spawn_pool_score_rebuild_worker(
    state: AppState,
) -> Option<tokio::task::JoinHandle<()>> {
    if !state.has_provider_catalog_data_reader()
        || !state.data.has_pool_score_reader()
        || !state.data.has_pool_score_writer()
    {
        return None;
    }

    let config = PoolScoreRebuildWorkerConfig::from_env();
    Some(tokio::spawn(async move {
        if let Err(err) = perform_pool_score_rebuild_once_with_config(&state, config).await {
            warn!(
                error = ?err,
                "gateway pool score rebuild initial tick failed"
            );
        }
        let mut interval = tokio::time::interval(config.interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            match perform_pool_score_rebuild_once_with_config(&state, config).await {
                Ok(summary) if summary.scores_upserted > 0 => {
                    info!(
                        providers_checked = summary.providers_checked,
                        providers_scored = summary.providers_scored,
                        keys_seen = summary.keys_seen,
                        scores_upserted = summary.scores_upserted,
                        "gateway pool score rebuild completed"
                    );
                }
                Ok(_) => {}
                Err(err) => {
                    warn!(
                        error = ?err,
                        "gateway pool score rebuild worker tick failed"
                    );
                }
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::score_combo_indices;

    #[test]
    fn score_combo_indices_walks_endpoint_model_key_order() {
        assert_eq!(score_combo_indices(0, 2, 3), (0, 0, 0));
        assert_eq!(score_combo_indices(2, 2, 3), (0, 0, 2));
        assert_eq!(score_combo_indices(3, 2, 3), (0, 1, 0));
        assert_eq!(score_combo_indices(6, 2, 3), (1, 0, 0));
    }
}
