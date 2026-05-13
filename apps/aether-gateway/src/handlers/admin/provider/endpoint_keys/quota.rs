use crate::handlers::admin::provider::shared::paths::admin_provider_id_for_refresh_quota;
use crate::handlers::admin::provider::shared::payloads::{
    AdminProviderQuotaRefreshRequest, OAUTH_ACCOUNT_BLOCK_PREFIX,
};
use crate::handlers::admin::request::{AdminAppState, AdminRequestContext};
use crate::GatewayError;
use axum::{
    body::{Body, Bytes},
    http,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};

use super::super::oauth::quota::dispatch::refresh_provider_pool_quota_locally;
use super::super::oauth::quota::shared::normalize_string_id_list;
use super::super::oauth::quota::shared::{
    provider_quota_refresh_endpoint_for_provider, provider_quota_refresh_missing_endpoint_message,
    provider_type_supports_quota_refresh, unsupported_provider_quota_refresh_message,
};
use super::super::write::provider::reconcile_admin_fixed_provider_template_endpoints;

fn unsupported_provider_quota_refresh_response(provider_type: &str) -> Response<Body> {
    let message = unsupported_provider_quota_refresh_message(provider_type);
    Json(json!({
        "success": 0,
        "failed": 0,
        "total": 0,
        "results": [],
        "message": message,
        "auto_removed": 0,
    }))
    .into_response()
}

pub(super) async fn maybe_handle(
    state: &AdminAppState<'_>,
    request_context: &AdminRequestContext<'_>,
    request_body: Option<&Bytes>,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(decision) = request_context.decision() else {
        return Ok(None);
    };

    if decision.route_family.as_deref() != Some("endpoints_manage")
        || decision.route_kind.as_deref() != Some("refresh_quota")
        || request_context.method() != http::Method::POST
        || !request_context
            .path()
            .starts_with("/api/admin/endpoints/providers/")
        || !request_context.path().ends_with("/refresh-quota")
    {
        return Ok(None);
    }

    let Some(provider_id) = admin_provider_id_for_refresh_quota(request_context.path()) else {
        return Ok(Some(
            (
                http::StatusCode::NOT_FOUND,
                Json(json!({ "detail": "Provider 不存在" })),
            )
                .into_response(),
        ));
    };
    let Some(provider) = state
        .read_provider_catalog_providers_by_ids(std::slice::from_ref(&provider_id))
        .await?
        .into_iter()
        .next()
    else {
        return Ok(Some(
            (
                http::StatusCode::NOT_FOUND,
                Json(json!({ "detail": format!("Provider {provider_id} 不存在") })),
            )
                .into_response(),
        ));
    };

    let normalized_provider_type = provider.provider_type.trim().to_ascii_lowercase();

    let payload = if let Some(request_body) = request_body.filter(|body| !body.is_empty()) {
        match serde_json::from_slice::<AdminProviderQuotaRefreshRequest>(request_body) {
            Ok(payload) => payload,
            Err(_) => {
                return Ok(Some(
                    (
                        http::StatusCode::BAD_REQUEST,
                        Json(json!({ "detail": "请求体必须是合法的 JSON 对象" })),
                    )
                        .into_response(),
                ));
            }
        }
    } else {
        AdminProviderQuotaRefreshRequest { key_ids: None }
    };

    let raw_key_ids = payload.key_ids;
    let selected_key_ids = normalize_string_id_list(raw_key_ids.clone());
    let explicit_key_ids_requested = raw_key_ids.is_some();

    let is_fixed_provider = state
        .fixed_provider_template(&provider.provider_type)
        .is_some();
    if !is_fixed_provider && !provider_type_supports_quota_refresh(&normalized_provider_type) {
        return Ok(None);
    }

    let mut endpoints = state
        .list_provider_catalog_endpoints_by_provider_ids(std::slice::from_ref(&provider_id))
        .await?;
    let mut endpoint =
        provider_quota_refresh_endpoint_for_provider(&normalized_provider_type, &endpoints, true);

    if endpoint.is_none() && is_fixed_provider {
        if !state.has_provider_catalog_data_writer() {
            if !provider_type_supports_quota_refresh(&normalized_provider_type) {
                return Ok(Some(unsupported_provider_quota_refresh_response(
                    &normalized_provider_type,
                )));
            }
            return Ok(Some(
                (
                    http::StatusCode::BAD_REQUEST,
                    Json(json!({ "detail": "固定 Provider 端点缺失，且 provider catalog writer 不可用，无法自动补全端点" })),
                )
                    .into_response(),
            ));
        }
        reconcile_admin_fixed_provider_template_endpoints(state, &provider).await?;
        endpoints = state
            .list_provider_catalog_endpoints_by_provider_ids(std::slice::from_ref(&provider_id))
            .await?;
        endpoint = provider_quota_refresh_endpoint_for_provider(
            &normalized_provider_type,
            &endpoints,
            true,
        );
    }

    if !provider_type_supports_quota_refresh(&normalized_provider_type) {
        return Ok(Some(unsupported_provider_quota_refresh_response(
            &normalized_provider_type,
        )));
    }

    let Some(endpoint) = endpoint else {
        let detail = provider_quota_refresh_missing_endpoint_message(&normalized_provider_type);
        return Ok(Some(
            (
                http::StatusCode::BAD_REQUEST,
                Json(json!({ "detail": detail })),
            )
                .into_response(),
        ));
    };

    let keys = if let Some(selected_key_ids) = selected_key_ids.as_ref() {
        if selected_key_ids.is_empty() {
            Vec::new()
        } else {
            let selected = selected_key_ids.iter().cloned().collect::<BTreeSet<_>>();
            let mut by_id = state
                .read_provider_catalog_keys_by_ids(selected_key_ids)
                .await?
                .into_iter()
                .filter(|key| key.provider_id == provider_id && selected.contains(&key.id))
                .map(|key| (key.id.clone(), key))
                .collect::<BTreeMap<_, _>>();
            selected_key_ids
                .iter()
                .filter_map(|key_id| by_id.remove(key_id))
                .collect()
        }
    } else {
        state
            .list_provider_catalog_keys_by_provider_ids(std::slice::from_ref(&provider_id))
            .await?
            .into_iter()
            .filter(|key| {
                key.is_active
                    || key
                        .oauth_invalid_reason
                        .as_deref()
                        .map(str::trim)
                        .is_some_and(|value| value.starts_with(OAUTH_ACCOUNT_BLOCK_PREFIX))
            })
            .collect()
    };

    if explicit_key_ids_requested && selected_key_ids.is_none() {
        return Ok(Some(
            Json(json!({
                "success": 0,
                "failed": 0,
                "total": 0,
                "results": [],
                "message": "未提供可刷新的 Key",
                "auto_removed": 0,
            }))
            .into_response(),
        ));
    }

    if keys.is_empty() {
        let message = if explicit_key_ids_requested {
            "未提供可刷新的 Key"
        } else {
            "没有可刷新的 Key"
        };
        return Ok(Some(
            Json(json!({
                "success": 0,
                "failed": 0,
                "total": 0,
                "results": [],
                "message": message,
                "auto_removed": 0,
            }))
            .into_response(),
        ));
    }

    let Some(payload) = refresh_provider_pool_quota_locally(
        state,
        &provider,
        &endpoint,
        &normalized_provider_type,
        keys,
        None,
    )
    .await?
    else {
        return Ok(None);
    };
    Ok(Some(Json(payload).into_response()))
}
