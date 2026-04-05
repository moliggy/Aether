use std::collections::HashMap;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use aether_contracts::ExecutionResult;
use aether_data::repository::provider_catalog::{
    StoredProviderCatalogEndpoint, StoredProviderCatalogKey, StoredProviderCatalogProvider,
};
use aether_model_fetch::{
    apply_model_filters, build_models_fetch_execution_plan, extract_error_message,
    json_string_list, model_fetch_interval_minutes, model_fetch_startup_delay_seconds,
    model_fetch_startup_enabled, parse_models_response, select_models_fetch_endpoint,
    sync_provider_model_whitelist_associations, ModelFetchAssociationStore, ModelFetchRunSummary,
    ModelsFetchSuccess,
};
use serde_json::{json, Value};
use tracing::{debug, info, warn};

use crate::provider_transport::GatewayProviderTransportSnapshot;
use crate::{AppState, GatewayError};

pub(crate) mod state;

use self::state::ModelFetchRuntimeState;

#[derive(Debug, Clone)]
struct SelectedFetchTarget {
    provider: StoredProviderCatalogProvider,
    endpoint: StoredProviderCatalogEndpoint,
    key: StoredProviderCatalogKey,
}

pub(crate) fn spawn_model_fetch_worker(state: AppState) -> Option<tokio::task::JoinHandle<()>> {
    if !state.has_provider_catalog_data_reader() || !state.has_provider_catalog_data_writer() {
        return None;
    }

    Some(tokio::spawn(async move {
        if model_fetch_startup_enabled() {
            let startup_delay = model_fetch_startup_delay_seconds();
            if startup_delay > 0 {
                tokio::time::sleep(Duration::from_secs(startup_delay)).await;
            }
            if let Err(err) = run_model_fetch_cycle(&state, "startup").await {
                warn!(error = ?err, "gateway model fetch startup failed");
            }
        } else {
            info!("gateway model fetch startup disabled");
        }

        let mut interval = tokio::time::interval(Duration::from_secs(
            model_fetch_interval_minutes().saturating_mul(60),
        ));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(err) = run_model_fetch_cycle(&state, "tick").await {
                warn!(error = ?err, "gateway model fetch tick failed");
            }
        }
    }))
}

pub(crate) async fn perform_model_fetch_once(
    state: &AppState,
) -> Result<ModelFetchRunSummary, GatewayError> {
    perform_model_fetch_once_with_state(state).await
}

async fn perform_model_fetch_once_with_state<S>(
    state: &S,
) -> Result<ModelFetchRunSummary, GatewayError>
where
    S: ModelFetchRuntimeState + ?Sized,
{
    if !state.has_provider_catalog_data_reader() || !state.has_provider_catalog_data_writer() {
        return Ok(ModelFetchRunSummary {
            attempted: 0,
            succeeded: 0,
            failed: 0,
            skipped: 0,
        });
    }

    let providers = state.list_provider_catalog_providers(true).await?;
    if providers.is_empty() {
        return Ok(ModelFetchRunSummary {
            attempted: 0,
            succeeded: 0,
            failed: 0,
            skipped: 0,
        });
    }

    let provider_ids = providers
        .iter()
        .map(|provider| provider.id.clone())
        .collect::<Vec<_>>();
    let mut endpoints_by_provider = HashMap::<String, Vec<StoredProviderCatalogEndpoint>>::new();
    for endpoint in state
        .list_provider_catalog_endpoints_by_provider_ids(&provider_ids)
        .await?
    {
        endpoints_by_provider
            .entry(endpoint.provider_id.clone())
            .or_default()
            .push(endpoint);
    }
    let mut keys_by_provider = HashMap::<String, Vec<StoredProviderCatalogKey>>::new();
    for key in <S as ModelFetchAssociationStore>::list_provider_catalog_keys_by_provider_ids(
        state,
        &provider_ids,
    )
    .await
    .map_err(GatewayError::Internal)?
    {
        keys_by_provider
            .entry(key.provider_id.clone())
            .or_default()
            .push(key);
    }

    let mut targets = Vec::new();
    for provider in providers {
        let endpoints = endpoints_by_provider
            .remove(&provider.id)
            .unwrap_or_default();
        let keys = keys_by_provider.remove(&provider.id).unwrap_or_default();
        for key in keys {
            if !key.is_active || !key.auto_fetch_models {
                continue;
            }
            if let Some(endpoint) = select_models_fetch_endpoint(&endpoints, &key) {
                targets.push(SelectedFetchTarget {
                    provider: provider.clone(),
                    endpoint,
                    key,
                });
            } else {
                targets.push(SelectedFetchTarget {
                    provider: provider.clone(),
                    endpoint: StoredProviderCatalogEndpoint::new(
                        "__unsupported__".to_string(),
                        provider.id.clone(),
                        "__unsupported__".to_string(),
                        None,
                        None,
                        false,
                    )
                    .expect("unsupported sentinel endpoint should build")
                    .with_transport_fields(
                        "https://unsupported.invalid".to_string(),
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                    )
                    .expect("unsupported sentinel endpoint transport should build"),
                    key,
                });
            }
        }
    }

    let mut summary = ModelFetchRunSummary {
        attempted: targets.len(),
        succeeded: 0,
        failed: 0,
        skipped: 0,
    };
    for target in targets {
        match fetch_and_persist_key_models(state, &target).await? {
            KeyFetchDisposition::Succeeded => summary.succeeded += 1,
            KeyFetchDisposition::Failed => summary.failed += 1,
            KeyFetchDisposition::Skipped => summary.skipped += 1,
        }
    }
    Ok(summary)
}

async fn run_model_fetch_cycle<S>(state: &S, phase: &'static str) -> Result<(), GatewayError>
where
    S: ModelFetchRuntimeState + ?Sized,
{
    let summary = perform_model_fetch_once_with_state(state).await?;
    if summary.attempted == 0 {
        debug!(phase, "gateway model fetch found no eligible keys");
        return Ok(());
    }

    info!(
        phase,
        attempted = summary.attempted,
        succeeded = summary.succeeded,
        failed = summary.failed,
        skipped = summary.skipped,
        "gateway model fetch cycle completed"
    );
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum KeyFetchDisposition {
    Succeeded,
    Failed,
    Skipped,
}

async fn fetch_and_persist_key_models(
    state: &(impl ModelFetchRuntimeState + ?Sized),
    target: &SelectedFetchTarget,
) -> Result<KeyFetchDisposition, GatewayError> {
    let now_unix_secs = now_unix_secs();
    if target.endpoint.api_format == "__unsupported__" {
        persist_key_fetch_failure(
            state,
            &target.key,
            now_unix_secs,
            "No supported endpoint for Rust models fetch".to_string(),
        )
        .await?;
        return Ok(KeyFetchDisposition::Skipped);
    }

    let Some(transport) = state
        .read_provider_transport_snapshot(&target.provider.id, &target.endpoint.id, &target.key.id)
        .await?
    else {
        persist_key_fetch_failure(
            state,
            &target.key,
            now_unix_secs,
            "Provider transport snapshot unavailable".to_string(),
        )
        .await?;
        return Ok(KeyFetchDisposition::Skipped);
    };

    let result = match execute_models_fetch_request(state, &transport).await {
        Ok(result) => result,
        Err(err) => {
            persist_key_fetch_failure(state, &target.key, now_unix_secs, err.clone()).await?;
            warn!(
                provider_id = %target.provider.id,
                key_id = %target.key.id,
                message = %err,
                "gateway model fetch failed"
            );
            return Ok(KeyFetchDisposition::Failed);
        }
    };

    let filtered_models = apply_model_filters(
        &result.fetched_model_ids,
        json_string_list(target.key.locked_models.as_ref()),
        json_string_list(target.key.model_include_patterns.as_ref()),
        json_string_list(target.key.model_exclude_patterns.as_ref()),
    );

    persist_key_fetch_success(state, &target.key, now_unix_secs, &filtered_models).await?;
    state
        .write_upstream_models_cache(&target.provider.id, &target.key.id, &result.cached_models)
        .await;
    sync_provider_model_whitelist_associations(state, &target.provider.id, &filtered_models)
        .await
        .map_err(GatewayError::Internal)?;
    Ok(KeyFetchDisposition::Succeeded)
}

async fn execute_models_fetch_request(
    state: &(impl ModelFetchRuntimeState + ?Sized),
    transport: &GatewayProviderTransportSnapshot,
) -> Result<ModelsFetchSuccess, String> {
    let plan = build_models_fetch_execution_plan(state, transport).await?;

    let result = state
        .execute_execution_runtime_sync_plan(&plan)
        .await
        .map_err(|err| format!("{err:?}"))?;

    if result.status_code != 200 {
        let message = result
            .body
            .as_ref()
            .and_then(|body| body.json_body.as_ref())
            .and_then(extract_error_message)
            .or_else(|| {
                result.error.as_ref().and_then(|error| {
                    let message = error.message.trim();
                    (!message.is_empty()).then_some(message.to_string())
                })
            })
            .unwrap_or_else(|| format!("upstream returned status {}", result.status_code));
        return Err(message);
    }

    let body_json = result
        .body
        .as_ref()
        .and_then(|body| body.json_body.as_ref())
        .ok_or_else(|| "models fetch response body is missing JSON payload".to_string())?;
    parse_models_response(&transport.endpoint.api_format, body_json)
}

async fn persist_key_fetch_failure(
    state: &(impl ModelFetchRuntimeState + ?Sized),
    key: &StoredProviderCatalogKey,
    now_unix_secs: u64,
    error: String,
) -> Result<(), GatewayError> {
    let mut updated = key.clone();
    updated.last_models_fetch_at_unix_secs = Some(now_unix_secs);
    updated.last_models_fetch_error = Some(error);
    updated.updated_at_unix_secs = Some(now_unix_secs);
    state.update_provider_catalog_key(&updated).await?;
    Ok(())
}

async fn persist_key_fetch_success(
    state: &(impl ModelFetchRuntimeState + ?Sized),
    key: &StoredProviderCatalogKey,
    now_unix_secs: u64,
    allowed_models: &[String],
) -> Result<(), GatewayError> {
    let mut updated = key.clone();
    updated.allowed_models = if allowed_models.is_empty() {
        None
    } else {
        Some(json!(allowed_models))
    };
    updated.last_models_fetch_at_unix_secs = Some(now_unix_secs);
    updated.last_models_fetch_error = None;
    updated.updated_at_unix_secs = Some(now_unix_secs);
    state.update_provider_catalog_key(&updated).await?;
    Ok(())
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
