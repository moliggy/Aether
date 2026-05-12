use super::{
    admin_pool_provider_id_from_path, admin_provider_pool_config, build_admin_pool_error_response,
    parse_admin_pool_key_sort, parse_admin_pool_page, parse_admin_pool_page_size,
    parse_admin_pool_quick_selectors, parse_admin_pool_search, parse_admin_pool_status_filter,
    pool_payloads, pool_selection, read_admin_provider_pool_cooldown_key_ids,
    read_admin_provider_pool_runtime_state, AdminPoolKeySort, AdminPoolKeySortDirection,
    AdminPoolKeySortField, AdminProviderPoolRuntimeState, ProviderCatalogKeyListOrder,
    ProviderCatalogKeyListQuery, ADMIN_POOL_PROVIDER_CATALOG_READER_UNAVAILABLE_DETAIL,
};
use crate::ai_serving::{provider_key_pool_score_id, provider_key_pool_score_scope};
use crate::handlers::admin::request::{AdminAppState, AdminRequestContext};
use crate::GatewayError;
use aether_admin::provider::pool as admin_provider_pool_pure;
use aether_data_contracts::repository::pool_scores::{
    GetPoolMemberScoresByIdsQuery, PoolMemberIdentity, StoredPoolMemberScore,
};
use aether_data_contracts::repository::provider_catalog::StoredProviderCatalogKey;
use aether_data_contracts::repository::usage::{
    ProviderApiKeyWindowUsageRequest, StoredProviderApiKeyWindowUsageSummary,
};
use axum::{
    body::Body,
    http,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use std::{
    cmp::Ordering,
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

type AdminPoolCodexCycleUsageByKey =
    BTreeMap<String, BTreeMap<String, StoredProviderApiKeyWindowUsageSummary>>;

fn admin_pool_json_u64(value: Option<&serde_json::Value>) -> Option<u64> {
    match value {
        Some(serde_json::Value::Number(number)) => number.as_u64(),
        Some(serde_json::Value::String(text)) => text.trim().parse::<u64>().ok(),
        _ => None,
    }
}

fn admin_pool_codex_default_window_minutes(code: &str) -> Option<u64> {
    if code.eq_ignore_ascii_case("5h") {
        Some(300)
    } else if code.eq_ignore_ascii_case("weekly") {
        Some(10_080)
    } else {
        None
    }
}

fn admin_pool_current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

async fn read_admin_pool_scores_by_key_id(
    state: &AdminAppState<'_>,
    provider_id: &str,
    key_ids: &[String],
) -> Result<BTreeMap<String, StoredPoolMemberScore>, GatewayError> {
    if key_ids.is_empty() {
        return Ok(BTreeMap::new());
    }

    let score_scope = provider_key_pool_score_scope();
    let score_ids = key_ids
        .iter()
        .map(|key_id| {
            let identity =
                PoolMemberIdentity::provider_api_key(provider_id.to_string(), key_id.clone());
            provider_key_pool_score_id(&identity, &score_scope)
        })
        .collect::<Vec<_>>();
    let scores = state
        .app()
        .data
        .get_pool_member_scores_by_ids(&GetPoolMemberScoresByIdsQuery { ids: score_ids })
        .await
        .map_err(|err| GatewayError::Internal(format!("{err:?}")))?;
    Ok(scores
        .into_iter()
        .map(|score| (score.member_id.clone(), score))
        .collect::<BTreeMap<_, _>>())
}

fn admin_pool_codex_cycle_usage_request(
    key: &StoredProviderCatalogKey,
    window: &serde_json::Map<String, serde_json::Value>,
    now_unix_secs: u64,
) -> Option<ProviderApiKeyWindowUsageRequest> {
    let window_code = window
        .get("code")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|code| code.eq_ignore_ascii_case("5h") || code.eq_ignore_ascii_case("weekly"))?
        .to_ascii_lowercase();
    let reset_at = admin_pool_json_u64(window.get("reset_at"))?;
    let window_seconds = admin_pool_json_u64(window.get("window_minutes"))
        .or_else(|| admin_pool_codex_default_window_minutes(&window_code))?
        .checked_mul(60)?;
    if reset_at <= now_unix_secs {
        return None;
    }
    let mut start_unix_secs = reset_at.checked_sub(window_seconds)?;
    if let Some(usage_reset_at) = admin_pool_json_u64(window.get("usage_reset_at")) {
        start_unix_secs = start_unix_secs.max(usage_reset_at);
    }
    if start_unix_secs >= reset_at || start_unix_secs >= now_unix_secs {
        return None;
    }

    Some(ProviderApiKeyWindowUsageRequest {
        provider_api_key_id: key.id.clone(),
        window_code,
        start_unix_secs,
        end_unix_secs: now_unix_secs,
    })
}

fn admin_pool_codex_cycle_usage_requests(
    keys: &[StoredProviderCatalogKey],
    now_unix_secs: u64,
) -> Vec<ProviderApiKeyWindowUsageRequest> {
    keys.iter()
        .flat_map(|key| {
            key.status_snapshot
                .as_ref()
                .and_then(serde_json::Value::as_object)
                .and_then(|snapshot| snapshot.get("quota"))
                .and_then(serde_json::Value::as_object)
                .and_then(|quota| quota.get("windows"))
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(serde_json::Value::as_object)
                .filter_map(|window| {
                    admin_pool_codex_cycle_usage_request(key, window, now_unix_secs)
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

async fn read_admin_pool_codex_cycle_usage_by_key(
    state: &AdminAppState<'_>,
    provider_type: &str,
    keys: &[StoredProviderCatalogKey],
    now_unix_secs: u64,
) -> Result<AdminPoolCodexCycleUsageByKey, GatewayError> {
    if !provider_type.trim().eq_ignore_ascii_case("codex") || keys.is_empty() {
        return Ok(BTreeMap::new());
    }

    let requests = admin_pool_codex_cycle_usage_requests(keys, now_unix_secs);
    if requests.is_empty() {
        return Ok(BTreeMap::new());
    }

    let summaries = state
        .app()
        .summarize_usage_by_provider_api_key_windows(&requests)
        .await?;
    let mut usage_by_key = AdminPoolCodexCycleUsageByKey::new();
    for summary in summaries {
        let window_code = summary.window_code.trim().to_ascii_lowercase();
        if window_code.is_empty() {
            continue;
        }
        usage_by_key
            .entry(summary.provider_api_key_id.clone())
            .or_default()
            .insert(window_code, summary);
    }
    Ok(usage_by_key)
}

fn admin_pool_compare_optional_unix_secs(
    left: Option<u64>,
    right: Option<u64>,
    direction: AdminPoolKeySortDirection,
) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) => match direction {
            AdminPoolKeySortDirection::Asc => left.cmp(&right),
            AdminPoolKeySortDirection::Desc => right.cmp(&left),
        },
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn admin_pool_sort_keys_for_request(keys: &mut [StoredProviderCatalogKey], sort: AdminPoolKeySort) {
    match sort.field {
        AdminPoolKeySortField::Default => pool_selection::admin_pool_sort_keys(keys),
        AdminPoolKeySortField::ImportedAt => {
            keys.sort_by(|left, right| {
                admin_pool_compare_optional_unix_secs(
                    left.created_at_unix_ms,
                    right.created_at_unix_ms,
                    sort.direction,
                )
                .then(left.name.cmp(&right.name))
                .then(left.id.cmp(&right.id))
            });
        }
        AdminPoolKeySortField::LastUsedAt => {
            keys.sort_by(|left, right| {
                admin_pool_compare_optional_unix_secs(
                    left.last_used_at_unix_secs,
                    right.last_used_at_unix_secs,
                    sort.direction,
                )
                .then(left.name.cmp(&right.name))
                .then(left.id.cmp(&right.id))
            });
        }
    }
}

fn admin_pool_repository_key_order(sort: AdminPoolKeySort) -> ProviderCatalogKeyListOrder {
    match (sort.field, sort.direction) {
        (AdminPoolKeySortField::Default, _) => ProviderCatalogKeyListOrder::Name,
        (AdminPoolKeySortField::ImportedAt, AdminPoolKeySortDirection::Asc) => {
            ProviderCatalogKeyListOrder::CreatedAtAsc
        }
        (AdminPoolKeySortField::ImportedAt, AdminPoolKeySortDirection::Desc) => {
            ProviderCatalogKeyListOrder::CreatedAtDesc
        }
        (AdminPoolKeySortField::LastUsedAt, AdminPoolKeySortDirection::Asc) => {
            ProviderCatalogKeyListOrder::LastUsedAtAsc
        }
        (AdminPoolKeySortField::LastUsedAt, AdminPoolKeySortDirection::Desc) => {
            ProviderCatalogKeyListOrder::LastUsedAtDesc
        }
    }
}

pub(super) async fn build_admin_pool_list_keys_response(
    state: &AdminAppState<'_>,
    request_context: &AdminRequestContext<'_>,
) -> Result<Response<Body>, GatewayError> {
    if !state.has_provider_catalog_data_reader() {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::SERVICE_UNAVAILABLE,
            ADMIN_POOL_PROVIDER_CATALOG_READER_UNAVAILABLE_DETAIL,
        ));
    }

    let Some(provider_id) = admin_pool_provider_id_from_path(request_context.path()) else {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::BAD_REQUEST,
            "provider_id 无效",
        ));
    };
    let query = request_context.query_string();
    let page = match parse_admin_pool_page(query) {
        Ok(value) => value,
        Err(detail) => {
            return Ok(build_admin_pool_error_response(
                http::StatusCode::BAD_REQUEST,
                detail,
            ));
        }
    };
    let page_size = match parse_admin_pool_page_size(query) {
        Ok(value) => value,
        Err(detail) => {
            return Ok(build_admin_pool_error_response(
                http::StatusCode::BAD_REQUEST,
                detail,
            ));
        }
    };
    let search = parse_admin_pool_search(query).map(|value| value.to_ascii_lowercase());
    let quick_selectors = admin_provider_pool_pure::admin_pool_sanitize_quick_selectors(
        parse_admin_pool_quick_selectors(query),
    );
    let status = match parse_admin_pool_status_filter(query) {
        Ok(value) => value,
        Err(detail) => {
            return Ok(build_admin_pool_error_response(
                http::StatusCode::BAD_REQUEST,
                detail,
            ));
        }
    };
    let sort = match parse_admin_pool_key_sort(query) {
        Ok(value) => value,
        Err(detail) => {
            return Ok(build_admin_pool_error_response(
                http::StatusCode::BAD_REQUEST,
                detail,
            ));
        }
    };

    let Some(provider) = state
        .read_provider_catalog_providers_by_ids(std::slice::from_ref(&provider_id))
        .await?
        .into_iter()
        .next()
    else {
        return Ok(build_admin_pool_error_response(
            http::StatusCode::NOT_FOUND,
            format!("Provider {provider_id} 不存在"),
        ));
    };

    let pool_config = admin_provider_pool_config(&provider);
    let page_offset = page.saturating_sub(1).saturating_mul(page_size);

    let (keys, total) = if status == "cooldown" {
        let cooldown_key_ids =
            read_admin_provider_pool_cooldown_key_ids(state.runtime_state(), &provider.id).await;
        let mut keys = if cooldown_key_ids.is_empty() {
            Vec::new()
        } else {
            state
                .read_provider_catalog_keys_by_ids(&cooldown_key_ids)
                .await?
        };
        if let Some(keyword) = search.as_ref() {
            keys.retain(|key| {
                pool_selection::admin_pool_matches_search(
                    state,
                    key,
                    &provider.provider_type,
                    Some(keyword),
                )
            });
        }
        if !quick_selectors.is_empty() {
            keys.retain(|key| {
                quick_selectors.iter().all(|selector| {
                    pool_selection::admin_pool_matches_quick_selector(
                        state,
                        key,
                        &provider.provider_type,
                        selector,
                    )
                })
            });
        }
        admin_pool_sort_keys_for_request(&mut keys, sort);
        let total = keys.len();
        let keys = keys
            .into_iter()
            .skip(page_offset)
            .take(page_size)
            .collect::<Vec<_>>();
        (keys, total)
    } else if !quick_selectors.is_empty() {
        let mut keys = state
            .list_provider_catalog_keys_by_provider_ids(std::slice::from_ref(&provider.id))
            .await?
            .into_iter()
            .filter(|key| match status.as_str() {
                "active" => key.is_active,
                "inactive" => !key.is_active,
                _ => true,
            })
            .filter(|key| {
                pool_selection::admin_pool_matches_search(
                    state,
                    key,
                    &provider.provider_type,
                    search.as_deref(),
                )
            })
            .filter(|key| {
                quick_selectors.iter().all(|selector| {
                    pool_selection::admin_pool_matches_quick_selector(
                        state,
                        key,
                        &provider.provider_type,
                        selector,
                    )
                })
            })
            .collect::<Vec<_>>();
        admin_pool_sort_keys_for_request(&mut keys, sort);
        let total = keys.len();
        let keys = keys
            .into_iter()
            .skip(page_offset)
            .take(page_size)
            .collect::<Vec<_>>();
        (keys, total)
    } else {
        let key_page = state
            .list_provider_catalog_key_page(&ProviderCatalogKeyListQuery {
                provider_id: provider.id.clone(),
                search: search.clone(),
                is_active: match status.as_str() {
                    "active" => Some(true),
                    "inactive" => Some(false),
                    _ => None,
                },
                offset: page_offset,
                limit: page_size,
                order: admin_pool_repository_key_order(sort),
            })
            .await?;
        (key_page.items, key_page.total)
    };

    let key_ids = keys.iter().map(|key| key.id.clone()).collect::<Vec<_>>();
    let pool_scores_by_key_id = read_admin_pool_scores_by_key_id(state, &provider.id, &key_ids)
        .await
        .unwrap_or_default();
    let endpoints = state
        .list_provider_catalog_endpoints_by_provider_ids(std::slice::from_ref(&provider.id))
        .await?;
    let runtime = match pool_config.as_ref() {
        Some(pool_config) if !key_ids.is_empty() => {
            read_admin_provider_pool_runtime_state(
                state.runtime_state(),
                &provider.id,
                &key_ids,
                pool_config,
                None,
            )
            .await
        }
        _ => AdminProviderPoolRuntimeState::default(),
    };
    let now_unix_secs = admin_pool_current_unix_secs();
    let codex_cycle_usage_by_key = read_admin_pool_codex_cycle_usage_by_key(
        state,
        &provider.provider_type,
        &keys,
        now_unix_secs,
    )
    .await?;

    let items = keys
        .into_iter()
        .map(|key| {
            pool_payloads::build_admin_pool_key_payload(
                state,
                &provider.provider_type,
                &endpoints,
                &key,
                &runtime,
                pool_config.clone(),
                pool_scores_by_key_id.get(&key.id),
                codex_cycle_usage_by_key.get(&key.id),
                now_unix_secs,
            )
        })
        .collect::<Vec<_>>();

    Ok(Json(json!({
        "total": total,
        "page": page,
        "page_size": page_size,
        "keys": items,
    }))
    .into_response())
}
