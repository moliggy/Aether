use aether_contracts::{ExecutionPlan, RequestBody};

use super::{augment_sync_report_context, LocalStreamPlanAndReport, LocalSyncPlanAndReport};
use crate::{GatewayControlSyncDecisionResponse, GatewayError};

pub(crate) fn build_passthrough_sync_plan_from_decision(
    parts: &http::request::Parts,
    payload: GatewayControlSyncDecisionResponse,
) -> Result<Option<LocalSyncPlanAndReport>, GatewayError> {
    let Some(request_id) = payload
        .request_id
        .clone()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    let Some(provider_id) = payload
        .provider_id
        .clone()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    let Some(endpoint_id) = payload
        .endpoint_id
        .clone()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    let Some(key_id) = payload
        .key_id
        .clone()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    let Some(provider_api_format) = payload
        .provider_api_format
        .clone()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    let Some(client_api_format) = payload
        .client_api_format
        .clone()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    let Some(upstream_url) = payload
        .upstream_url
        .clone()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    let (request_body, provider_request_body_for_report) = resolve_passthrough_sync_request_body(
        payload.provider_request_body.clone(),
        payload.provider_request_body_base64.clone(),
    );

    let plan = ExecutionPlan {
        request_id,
        candidate_id: payload.candidate_id.clone(),
        provider_name: payload.provider_name.clone(),
        provider_id,
        endpoint_id,
        key_id,
        method: payload
            .provider_request_method
            .clone()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| parts.method.to_string()),
        url: upstream_url,
        headers: payload.provider_request_headers.clone(),
        content_type: payload.content_type.clone().or_else(|| {
            payload
                .provider_request_headers
                .get("content-type")
                .cloned()
        }),
        content_encoding: None,
        body: request_body,
        stream: false,
        client_api_format,
        provider_api_format,
        model_name: payload.model_name.clone(),
        proxy: payload.proxy.clone(),
        tls_profile: payload.tls_profile.clone(),
        timeouts: payload.timeouts.clone(),
    };

    let report_context = augment_sync_report_context(
        payload.report_context,
        &plan.headers,
        &provider_request_body_for_report,
    )?;

    Ok(Some(LocalSyncPlanAndReport {
        plan,
        report_kind: payload.report_kind,
        report_context,
    }))
}

pub(crate) fn build_passthrough_stream_plan_from_decision(
    parts: &http::request::Parts,
    payload: GatewayControlSyncDecisionResponse,
) -> Result<Option<LocalStreamPlanAndReport>, GatewayError> {
    let Some(request_id) = payload
        .request_id
        .clone()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    let Some(provider_id) = payload
        .provider_id
        .clone()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    let Some(endpoint_id) = payload
        .endpoint_id
        .clone()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    let Some(key_id) = payload
        .key_id
        .clone()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    let Some(provider_api_format) = payload
        .provider_api_format
        .clone()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    let Some(client_api_format) = payload
        .client_api_format
        .clone()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    let Some(upstream_url) = payload
        .upstream_url
        .clone()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };
    let plan = ExecutionPlan {
        request_id,
        candidate_id: payload.candidate_id.clone(),
        provider_name: payload.provider_name.clone(),
        provider_id,
        endpoint_id,
        key_id,
        method: parts.method.to_string(),
        url: upstream_url,
        headers: payload.provider_request_headers.clone(),
        content_type: payload.content_type.clone().or_else(|| {
            payload
                .provider_request_headers
                .get("content-type")
                .cloned()
        }),
        content_encoding: None,
        body: RequestBody {
            json_body: None,
            body_bytes_b64: None,
            body_ref: None,
        },
        stream: true,
        client_api_format,
        provider_api_format,
        model_name: payload.model_name.clone(),
        proxy: payload.proxy.clone(),
        tls_profile: payload.tls_profile.clone(),
        timeouts: payload.timeouts.clone(),
    };

    Ok(Some(LocalStreamPlanAndReport {
        plan,
        report_kind: payload.report_kind,
        report_context: payload.report_context,
    }))
}

fn resolve_passthrough_sync_request_body(
    provider_request_body: Option<serde_json::Value>,
    provider_request_body_base64: Option<String>,
) -> (RequestBody, serde_json::Value) {
    if let Some(body_bytes_b64) = provider_request_body_base64
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
    {
        return (
            RequestBody {
                json_body: None,
                body_bytes_b64: Some(body_bytes_b64.clone()),
                body_ref: None,
            },
            serde_json::json!({"body_bytes_b64": body_bytes_b64}),
        );
    }

    match provider_request_body.unwrap_or(serde_json::Value::Null) {
        serde_json::Value::Null => (
            RequestBody {
                json_body: None,
                body_bytes_b64: None,
                body_ref: None,
            },
            serde_json::Value::Null,
        ),
        other => {
            let report_body = other.clone();
            (RequestBody::from_json(other), report_body)
        }
    }
}
