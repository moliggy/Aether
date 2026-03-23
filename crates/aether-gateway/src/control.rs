use std::time::{Duration, Instant};

use axum::body::{Body, Bytes};
use axum::http::{Response, StatusCode, Uri};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::gateway::constants::*;
use crate::gateway::headers::{
    collect_control_headers, header_equals, header_value_str, header_value_u64, is_json_request,
};
use crate::gateway::{build_client_response, AppState, CachedAuthContextEntry, GatewayError};

const AUTH_CONTEXT_CACHE_TTL: Duration = Duration::from_secs(60);
const AUTH_CONTEXT_CACHE_MAX_ENTRIES: usize = 256;

#[derive(Debug, Serialize)]
struct GatewayControlLegacyResolveRequest {
    trace_id: String,
    method: String,
    path: String,
    query_string: Option<String>,
    headers: std::collections::BTreeMap<String, String>,
    has_body: bool,
    content_type: Option<String>,
    content_length: Option<u64>,
}

#[derive(Debug, Serialize)]
struct GatewayControlAuthContextRequest {
    trace_id: String,
    query_string: Option<String>,
    headers: std::collections::BTreeMap<String, String>,
    auth_endpoint_signature: String,
}

#[derive(Debug, Serialize)]
struct GatewayControlExecuteRequest {
    trace_id: String,
    method: String,
    path: String,
    query_string: Option<String>,
    headers: std::collections::BTreeMap<String, String>,
    body_json: serde_json::Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    body_base64: Option<String>,
    auth_context: Option<GatewayControlAuthContext>,
}

#[derive(Debug, Deserialize)]
struct GatewayControlLegacyResolveResponse {
    action: String,
    public_path: Option<String>,
    public_query_string: Option<String>,
    route_class: Option<String>,
    route_family: Option<String>,
    route_kind: Option<String>,
    auth_endpoint_signature: Option<String>,
    executor_candidate: Option<bool>,
    auth_context: Option<GatewayControlAuthContext>,
}

#[derive(Debug, Deserialize)]
struct GatewayControlAuthContextResponse {
    auth_context: Option<GatewayControlAuthContext>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct GatewayControlAuthContext {
    pub(crate) user_id: String,
    pub(crate) api_key_id: String,
    pub(crate) balance_remaining: Option<f64>,
    pub(crate) access_allowed: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct GatewayControlDecision {
    pub(crate) public_path: String,
    pub(crate) public_query_string: Option<String>,
    pub(crate) route_class: Option<String>,
    pub(crate) route_family: Option<String>,
    pub(crate) route_kind: Option<String>,
    pub(crate) auth_endpoint_signature: Option<String>,
    pub(crate) executor_candidate: bool,
    pub(crate) auth_context: Option<GatewayControlAuthContext>,
}

impl GatewayControlDecision {
    pub(crate) fn proxy_path_and_query(&self) -> String {
        if let Some(query) = self
            .public_query_string
            .as_deref()
            .filter(|value| !value.is_empty())
        {
            format!("{}?{}", self.public_path, query)
        } else {
            self.public_path.clone()
        }
    }
}

pub(crate) async fn resolve_control_route(
    state: &AppState,
    method: &http::Method,
    uri: &Uri,
    headers: &http::HeaderMap,
    trace_id: &str,
) -> Result<Option<GatewayControlDecision>, GatewayError> {
    let Some(control_base_url) = state.control_base_url.as_deref() else {
        return Ok(None);
    };

    let Some(mut decision) = classify_control_route(method, uri.path(), headers) else {
        return Ok(None);
    };
    decision.public_query_string = uri.query().map(ToOwned::to_owned);

    if state.executor_base_url.is_some() && decision.executor_candidate {
        return Ok(Some(decision));
    }

    match fetch_auth_context(
        state,
        control_base_url,
        headers,
        uri,
        trace_id,
        decision
            .auth_endpoint_signature
            .as_deref()
            .unwrap_or_default(),
    )
    .await
    {
        Ok(auth_context) => {
            decision.auth_context = auth_context;
            Ok(Some(decision))
        }
        Err(GatewayError::ControlUnavailable { .. }) => {
            resolve_control_route_legacy(state, control_base_url, method, uri, headers, trace_id)
                .await
        }
        Err(err) => Err(err),
    }
}

pub(crate) async fn resolve_executor_auth_context(
    state: &AppState,
    decision: &GatewayControlDecision,
    headers: &http::HeaderMap,
    uri: &Uri,
    trace_id: &str,
) -> Result<Option<GatewayControlAuthContext>, GatewayError> {
    let _ = trace_id;

    if let Some(auth_context) = decision.auth_context.clone() {
        return Ok(Some(auth_context));
    }

    let Some(auth_endpoint_signature) = decision.auth_endpoint_signature.as_deref() else {
        return Ok(None);
    };
    let Some(cache_key) = build_auth_context_cache_key(headers, uri, auth_endpoint_signature)
    else {
        return Ok(None);
    };

    if let Some(auth_context) = get_cached_auth_context(state, &cache_key) {
        return Ok(Some(auth_context));
    }

    Ok(None)
}

pub(crate) fn cache_executor_auth_context(
    state: &AppState,
    decision: &GatewayControlDecision,
    headers: &http::HeaderMap,
    uri: &Uri,
    auth_context: GatewayControlAuthContext,
) {
    let Some(auth_endpoint_signature) = decision.auth_endpoint_signature.as_deref() else {
        return;
    };
    let Some(cache_key) = build_auth_context_cache_key(headers, uri, auth_endpoint_signature)
    else {
        return;
    };
    put_cached_auth_context(state, cache_key, auth_context);
}

async fn fetch_auth_context(
    state: &AppState,
    control_base_url: &str,
    headers: &http::HeaderMap,
    uri: &Uri,
    trace_id: &str,
    auth_endpoint_signature: &str,
) -> Result<Option<GatewayControlAuthContext>, GatewayError> {
    if auth_endpoint_signature.trim().is_empty() {
        return Ok(None);
    }

    let request_payload = GatewayControlAuthContextRequest {
        trace_id: trace_id.to_string(),
        query_string: uri.query().map(ToOwned::to_owned),
        headers: collect_control_headers(headers),
        auth_endpoint_signature: auth_endpoint_signature.to_string(),
    };

    let response = state
        .client
        .post(format!(
            "{control_base_url}/api/internal/gateway/auth-context"
        ))
        .header(TRACE_ID_HEADER, trace_id)
        .json(&request_payload)
        .send()
        .await
        .map_err(|err| GatewayError::ControlUnavailable {
            trace_id: trace_id.to_string(),
            message: err.to_string(),
        })?;

    let response = response
        .error_for_status()
        .map_err(|err| GatewayError::ControlUnavailable {
            trace_id: trace_id.to_string(),
            message: err.to_string(),
        })?;

    let payload: GatewayControlAuthContextResponse = response
        .json()
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;

    Ok(payload.auth_context)
}

async fn resolve_control_route_legacy(
    state: &AppState,
    control_base_url: &str,
    method: &http::Method,
    uri: &Uri,
    headers: &http::HeaderMap,
    trace_id: &str,
) -> Result<Option<GatewayControlDecision>, GatewayError> {
    let path = uri.path();
    if !should_consult_control_api(path) {
        return Ok(None);
    }

    let control_request = GatewayControlLegacyResolveRequest {
        trace_id: trace_id.to_string(),
        method: method.to_string(),
        path: path.to_string(),
        query_string: uri.query().map(ToOwned::to_owned),
        headers: collect_control_headers(headers),
        has_body: header_value_u64(headers, http::header::CONTENT_LENGTH.as_str()).unwrap_or(0) > 0
            || headers.contains_key(http::header::CONTENT_TYPE),
        content_type: header_value_str(headers, http::header::CONTENT_TYPE.as_str()),
        content_length: header_value_u64(headers, http::header::CONTENT_LENGTH.as_str()),
    };

    let response = state
        .client
        .post(format!("{control_base_url}/api/internal/gateway/resolve"))
        .header(TRACE_ID_HEADER, trace_id)
        .json(&control_request)
        .send()
        .await
        .map_err(|err| GatewayError::ControlUnavailable {
            trace_id: trace_id.to_string(),
            message: err.to_string(),
        })?;

    let response = response
        .error_for_status()
        .map_err(|err| GatewayError::ControlUnavailable {
            trace_id: trace_id.to_string(),
            message: err.to_string(),
        })?;

    let payload: GatewayControlLegacyResolveResponse = response
        .json()
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;

    if payload.action != "proxy_public" {
        return Err(GatewayError::Internal(format!(
            "unsupported gateway control action: {}",
            payload.action
        )));
    }

    Ok(Some(GatewayControlDecision {
        public_path: payload.public_path.unwrap_or_else(|| path.to_string()),
        public_query_string: payload
            .public_query_string
            .or_else(|| uri.query().map(ToOwned::to_owned)),
        route_class: payload.route_class,
        route_family: payload.route_family,
        route_kind: payload.route_kind,
        auth_endpoint_signature: payload.auth_endpoint_signature,
        executor_candidate: payload.executor_candidate.unwrap_or(false),
        auth_context: payload.auth_context,
    }))
}

fn build_auth_context_cache_key(
    headers: &http::HeaderMap,
    uri: &Uri,
    auth_endpoint_signature: &str,
) -> Option<String> {
    let signature = auth_endpoint_signature.trim();
    if signature.is_empty() {
        return None;
    }

    let authorization =
        header_value_str(headers, http::header::AUTHORIZATION.as_str()).unwrap_or_default();
    let x_api_key = header_value_str(headers, "x-api-key").unwrap_or_default();
    let api_key = header_value_str(headers, "api-key").unwrap_or_default();
    let query = uri.query().unwrap_or_default().trim();

    if authorization.is_empty() && x_api_key.is_empty() && api_key.is_empty() && query.is_empty() {
        return None;
    }

    Some(format!(
        "{signature}\n{authorization}\n{x_api_key}\n{api_key}\n{query}"
    ))
}

fn get_cached_auth_context(state: &AppState, cache_key: &str) -> Option<GatewayControlAuthContext> {
    let mut cache = state.auth_context_cache.lock().ok()?;
    let entry = cache.get(cache_key)?.clone();
    if entry.cached_at.elapsed() > AUTH_CONTEXT_CACHE_TTL {
        cache.remove(cache_key);
        return None;
    }
    Some(entry.auth_context)
}

fn put_cached_auth_context(
    state: &AppState,
    cache_key: String,
    auth_context: GatewayControlAuthContext,
) {
    let Ok(mut cache) = state.auth_context_cache.lock() else {
        return;
    };

    cache.retain(|_, entry| entry.cached_at.elapsed() <= AUTH_CONTEXT_CACHE_TTL);
    if cache.len() >= AUTH_CONTEXT_CACHE_MAX_ENTRIES {
        if let Some(oldest_key) = cache
            .iter()
            .min_by_key(|(_, entry)| entry.cached_at)
            .map(|(key, _)| key.clone())
        {
            cache.remove(&oldest_key);
        }
    }

    cache.insert(
        cache_key,
        CachedAuthContextEntry {
            auth_context,
            cached_at: Instant::now(),
        },
    );
}

fn classify_control_route(
    method: &http::Method,
    path: &str,
    headers: &http::HeaderMap,
) -> Option<GatewayControlDecision> {
    let normalized_path = if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };

    let (route_family, route_kind, auth_endpoint_signature) =
        if method == http::Method::POST && normalized_path == "/v1/chat/completions" {
            ("openai", "chat", "openai:chat")
        } else if method == http::Method::POST
            && matches!(
                normalized_path.as_str(),
                "/v1/responses" | "/v1/responses/compact"
            )
        {
            if normalized_path.ends_with("/compact") {
                ("openai", "compact", "openai:compact")
            } else {
                ("openai", "cli", "openai:cli")
            }
        } else if method == http::Method::POST && normalized_path == "/v1/messages" {
            if is_claude_cli_request(headers) {
                ("claude", "cli", "claude:cli")
            } else {
                ("claude", "chat", "claude:chat")
            }
        } else if normalized_path.starts_with("/v1/videos") {
            ("openai", "video", "openai:video")
        } else if is_gemini_models_route(&normalized_path) {
            if normalized_path.ends_with(":predictLongRunning") {
                ("gemini", "video", "gemini:video")
            } else if is_gemini_cli_request(headers) {
                ("gemini", "cli", "gemini:cli")
            } else {
                ("gemini", "chat", "gemini:chat")
            }
        } else if is_gemini_operation_route(&normalized_path) {
            ("gemini", "video", "gemini:video")
        } else if (method == http::Method::POST && normalized_path == "/upload/v1beta/files")
            || normalized_path.starts_with("/v1beta/files")
        {
            ("gemini", "files", "gemini:chat")
        } else {
            return None;
        };

    Some(GatewayControlDecision {
        public_path: normalized_path,
        public_query_string: None,
        route_class: Some("ai_public".to_string()),
        route_family: Some(route_family.to_string()),
        route_kind: Some(route_kind.to_string()),
        auth_endpoint_signature: Some(auth_endpoint_signature.to_string()),
        executor_candidate: true,
        auth_context: None,
    })
}

fn is_claude_cli_request(headers: &http::HeaderMap) -> bool {
    let auth_header = header_value_str(headers, http::header::AUTHORIZATION.as_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let has_bearer = auth_header.starts_with("bearer ");
    let has_api_key =
        header_value_str(headers, "x-api-key").is_some_and(|value| !value.trim().is_empty());
    has_bearer && !has_api_key
}

fn is_gemini_cli_request(headers: &http::HeaderMap) -> bool {
    let x_app = header_value_str(headers, "x-app")
        .unwrap_or_default()
        .to_ascii_lowercase();
    if x_app.contains("cli") {
        return true;
    }

    let user_agent = header_value_str(headers, http::header::USER_AGENT.as_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    user_agent.contains("geminicli") || user_agent.contains("gemini-cli")
}

fn is_stream_route(path: &str) -> bool {
    path.contains(":streamGenerateContent")
}

fn is_video_route(decision: &GatewayControlDecision) -> bool {
    decision.route_kind.as_deref() == Some("video")
}

fn is_files_route(decision: &GatewayControlDecision) -> bool {
    decision.route_kind.as_deref() == Some("files")
        && decision.route_family.as_deref() == Some("gemini")
}

pub(crate) async fn maybe_execute_via_control(
    state: &AppState,
    parts: &http::request::Parts,
    body_bytes: Bytes,
    trace_id: &str,
    decision: Option<&GatewayControlDecision>,
) -> Result<Option<Response<Body>>, GatewayError> {
    let Some(control_base_url) = state.control_base_url.as_deref() else {
        return Ok(None);
    };
    let Some(decision) = decision else {
        return Ok(None);
    };
    if !decision.executor_candidate {
        return Ok(None);
    }
    if decision.route_class.as_deref() != Some("ai_public") {
        return Ok(None);
    }
    let is_files_route = is_files_route(decision);
    let is_video_route = is_video_route(decision);
    if is_files_route || is_video_route {
        if !matches!(
            parts.method,
            http::Method::GET | http::Method::POST | http::Method::DELETE
        ) {
            return Ok(None);
        }
    } else if parts.method != http::Method::POST || !is_json_request(&parts.headers) {
        return Ok(None);
    }

    let body_json = if is_json_request(&parts.headers) {
        match serde_json::from_slice::<serde_json::Value>(&body_bytes) {
            Ok(value) if value.is_object() => value,
            _ if !is_files_route && !is_video_route => return Ok(None),
            _ => json!({}),
        }
    } else if (is_video_route && body_bytes.is_empty()) || is_files_route {
        json!({})
    } else {
        return Ok(None);
    };
    let is_stream_request = if is_files_route || is_video_route {
        false
    } else {
        is_stream_route(parts.uri.path())
            || body_json
                .get("stream")
                .and_then(|value| value.as_bool())
                .unwrap_or(false)
    };

    let control_endpoint = if is_stream_request {
        "execute-stream"
    } else {
        "execute-sync"
    };

    let auth_context =
        resolve_executor_auth_context(state, decision, &parts.headers, &parts.uri, trace_id)
            .await?;

    let request_payload = GatewayControlExecuteRequest {
        trace_id: trace_id.to_string(),
        method: parts.method.to_string(),
        path: parts.uri.path().to_string(),
        query_string: parts.uri.query().map(ToOwned::to_owned),
        headers: collect_control_headers(&parts.headers),
        body_json,
        body_base64: if is_files_route && !body_bytes.is_empty() {
            Some(BASE64_STANDARD.encode(&body_bytes))
        } else {
            None
        },
        auth_context,
    };

    let response = state
        .client
        .post(format!(
            "{control_base_url}/api/internal/gateway/{control_endpoint}"
        ))
        .header(TRACE_ID_HEADER, trace_id)
        .json(&request_payload)
        .send()
        .await
        .map_err(|err| GatewayError::ControlUnavailable {
            trace_id: trace_id.to_string(),
            message: err.to_string(),
        })?;

    if response.status() == StatusCode::CONFLICT
        && header_equals(response.headers(), CONTROL_ACTION_HEADER, "proxy_public")
    {
        return Ok(None);
    }
    if !header_equals(response.headers(), CONTROL_EXECUTED_HEADER, "true") {
        return Ok(None);
    }

    Ok(Some(build_client_response(
        response,
        trace_id,
        Some(decision),
    )?))
}

fn should_consult_control_api(path: &str) -> bool {
    matches!(
        path,
        "/v1/chat/completions" | "/v1/messages" | "/v1/responses" | "/v1/responses/compact"
    ) || path.starts_with("/v1/videos")
        || path == "/upload/v1beta/files"
        || path.starts_with("/v1beta/files")
        || is_gemini_models_route(path)
        || is_gemini_operation_route(path)
}

fn is_gemini_models_route(path: &str) -> bool {
    (path.starts_with("/v1/models/") || path.starts_with("/v1beta/models/"))
        && (path.contains(":generateContent")
            || path.contains(":streamGenerateContent")
            || path.contains(":predictLongRunning"))
}

fn is_gemini_operation_route(path: &str) -> bool {
    (path.starts_with("/v1beta/models/") && path.contains("/operations/"))
        || path == "/v1beta/operations"
        || path.starts_with("/v1beta/operations/")
}
