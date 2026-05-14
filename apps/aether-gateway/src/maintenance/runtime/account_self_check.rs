use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aether_contracts::{ExecutionPlan, ExecutionResult, RequestBody};
use aether_data_contracts::repository::pool_scores::{
    PoolMemberHardState, PoolMemberIdentity, PoolMemberProbeAttempt, PoolMemberProbeResult,
    PoolMemberProbeStatus,
};
use aether_data_contracts::repository::provider_catalog::{
    StoredProviderCatalogEndpoint, StoredProviderCatalogKey, StoredProviderCatalogProvider,
};
use aether_runtime_state::{RuntimeLockLease, RuntimeState};
use base64::Engine as _;
use futures_util::{stream, StreamExt};
use serde_json::{json, Value};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::admin_api::{
    admin_provider_pool_config, persist_provider_quota_refresh_state,
    provider_quota_refresh_endpoint_for_provider, provider_type_supports_quota_refresh,
    refresh_provider_pool_quota_locally, AdminAppState, AdminGatewayProviderTransportSnapshot,
    OAUTH_ACCOUNT_BLOCK_PREFIX, OAUTH_REQUEST_FAILED_PREFIX,
};
use crate::{AppState, GatewayError};

const ACCOUNT_SELF_CHECK_REDIS_PREFIX: &str = "ap:account_self_check:last";
const ACCOUNT_SELF_CHECK_LOCK_TTL_MS: u64 = 30_000;
const ACCOUNT_SELF_CHECK_DEFAULT_SCAN_INTERVAL_SECONDS: u64 = 60;
const ACCOUNT_SELF_CHECK_MIN_SCAN_INTERVAL_SECONDS: u64 = 15;
const ACCOUNT_SELF_CHECK_DEFAULT_MAX_KEYS_PER_PROVIDER: usize = 200;
const ACCOUNT_SELF_CHECK_DEFAULT_GLOBAL_CONCURRENCY: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub(crate) struct AccountSelfCheckRunSummary {
    pub(crate) providers_checked: usize,
    pub(crate) providers_checked_with_keys: usize,
    pub(crate) providers_skipped: usize,
    pub(crate) scanned_keys: usize,
    pub(crate) selected_keys: usize,
    pub(crate) succeeded: usize,
    pub(crate) blocked: usize,
    pub(crate) failed: usize,
    pub(crate) skipped: usize,
    pub(crate) auto_removed: usize,
}

impl AccountSelfCheckRunSummary {
    const fn empty() -> Self {
        Self {
            providers_checked: 0,
            providers_checked_with_keys: 0,
            providers_skipped: 0,
            scanned_keys: 0,
            selected_keys: 0,
            succeeded: 0,
            blocked: 0,
            failed: 0,
            skipped: 0,
            auto_removed: 0,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct AccountSelfCheckWorkerConfig {
    pub(crate) scan_interval: Duration,
    pub(crate) max_keys_per_provider: usize,
    pub(crate) global_concurrency: usize,
}

impl AccountSelfCheckWorkerConfig {
    fn from_env() -> Self {
        let scan_interval_seconds = env_u64(
            "ACCOUNT_SELF_CHECK_SCAN_INTERVAL_SECONDS",
            ACCOUNT_SELF_CHECK_DEFAULT_SCAN_INTERVAL_SECONDS,
        )
        .max(ACCOUNT_SELF_CHECK_MIN_SCAN_INTERVAL_SECONDS);
        let max_keys_per_provider = env_usize(
            "ACCOUNT_SELF_CHECK_MAX_KEYS_PER_PROVIDER",
            ACCOUNT_SELF_CHECK_DEFAULT_MAX_KEYS_PER_PROVIDER,
        )
        .max(1);
        let global_concurrency = env_usize(
            "ACCOUNT_SELF_CHECK_GLOBAL_CONCURRENCY",
            ACCOUNT_SELF_CHECK_DEFAULT_GLOBAL_CONCURRENCY,
        )
        .clamp(1, 256);
        Self {
            scan_interval: Duration::from_secs(scan_interval_seconds),
            max_keys_per_provider,
            global_concurrency,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AccountSelfCheckRequestConfig {
    pub(crate) method: String,
    pub(crate) url: Option<String>,
    pub(crate) path: Option<String>,
    pub(crate) headers: BTreeMap<String, String>,
    pub(crate) json_body: Option<Value>,
    pub(crate) body: Option<String>,
    pub(crate) body_bytes_b64: Option<String>,
    pub(crate) content_type: Option<String>,
    pub(crate) success_status_codes: BTreeSet<u16>,
    pub(crate) blocked_status_codes: BTreeSet<u16>,
}

impl Default for AccountSelfCheckRequestConfig {
    fn default() -> Self {
        Self {
            method: "GET".to_string(),
            url: None,
            path: None,
            headers: BTreeMap::new(),
            json_body: None,
            body: None,
            body_bytes_b64: None,
            content_type: None,
            success_status_codes: BTreeSet::from([200]),
            blocked_status_codes: BTreeSet::from([401, 403, 423]),
        }
    }
}

enum AccountSelfCheckOutcome {
    Success {
        status_code: Option<u16>,
        message: Option<String>,
    },
    Blocked {
        status_code: Option<u16>,
        message: String,
    },
    Failed {
        status_code: Option<u16>,
        message: String,
    },
    Skipped {
        message: String,
    },
}

impl AccountSelfCheckOutcome {
    fn score_status(&self) -> &'static str {
        match self {
            Self::Success { .. } => "success",
            Self::Blocked { .. } => "blocked",
            Self::Failed { .. } => "failed",
            Self::Skipped { .. } => "skipped",
        }
    }

    fn status_code(&self) -> Option<u16> {
        match self {
            Self::Success { status_code, .. }
            | Self::Blocked { status_code, .. }
            | Self::Failed { status_code, .. } => *status_code,
            Self::Skipped { .. } => None,
        }
    }

    fn message(&self) -> Option<&str> {
        match self {
            Self::Success { message, .. } => message.as_deref(),
            Self::Blocked { message, .. }
            | Self::Failed { message, .. }
            | Self::Skipped { message, .. } => Some(message.as_str()),
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
        .map(|duration| duration.as_secs())
        .unwrap_or_default()
}

fn parse_check_stamp(raw_value: Option<&str>) -> Option<u64> {
    let parsed = raw_value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .and_then(|value| value.parse::<f64>().ok())?;
    if parsed <= 0.0 {
        return None;
    }
    Some(parsed as u64)
}

fn check_stamp_key(provider_id: &str, key_id: &str) -> String {
    format!("{ACCOUNT_SELF_CHECK_REDIS_PREFIX}:{provider_id}:{key_id}")
}

async fn load_check_timestamps(
    runtime: &RuntimeState,
    provider_id: &str,
    key_ids: &[String],
) -> BTreeMap<String, u64> {
    if key_ids.is_empty() {
        return BTreeMap::new();
    }

    let runtime_keys = key_ids
        .iter()
        .map(|key_id| check_stamp_key(provider_id, key_id))
        .collect::<Vec<_>>();
    let Ok(values) = runtime.kv_get_many(&runtime_keys).await else {
        debug!("gateway account self-check: failed to read runtime check stamps");
        return BTreeMap::new();
    };

    key_ids
        .iter()
        .zip(values)
        .filter_map(|(key_id, raw)| {
            parse_check_stamp(raw.as_deref()).map(|ts| (key_id.clone(), ts))
        })
        .collect()
}

async fn mark_check_timestamps(
    runtime: &RuntimeState,
    provider_id: &str,
    key_ids: &[String],
    now_ts: u64,
    interval_seconds: u64,
) {
    if key_ids.is_empty() {
        return;
    }

    let ttl_seconds = interval_seconds.saturating_mul(2).max(120);
    let value = now_ts.to_string();
    for key_id in key_ids {
        if runtime
            .kv_set(
                &check_stamp_key(provider_id, key_id),
                value.clone(),
                Some(Duration::from_secs(ttl_seconds)),
            )
            .await
            .is_err()
        {
            debug!("gateway account self-check: failed to write runtime check stamp");
        }
    }
}

async fn acquire_provider_self_check_lock(
    runtime: &RuntimeState,
    provider_id: &str,
) -> Option<RuntimeLockLease> {
    let owner = format!("aether-gateway-account-self-check-{}", std::process::id());
    match runtime
        .lock_try_acquire(
            &format!("account_self_check:{provider_id}"),
            &owner,
            Duration::from_millis(ACCOUNT_SELF_CHECK_LOCK_TTL_MS),
        )
        .await
    {
        Ok(lease) => lease,
        Err(err) => {
            debug!(
                provider_id,
                error = %err,
                "gateway account self-check: failed to acquire runtime provider lock"
            );
            None
        }
    }
}

async fn release_provider_self_check_lock(runtime: &RuntimeState, lease: Option<RuntimeLockLease>) {
    let Some(lease) = lease else {
        return;
    };
    if let Err(err) = runtime.lock_release(&lease).await {
        debug!(
            error = %err,
            "gateway account self-check: failed to release runtime provider lock"
        );
    }
}

pub(crate) fn select_account_self_check_key_ids(
    key_ids: &[String],
    now_ts: u64,
    interval_seconds: u64,
    last_check_timestamps: &BTreeMap<String, u64>,
    limit: usize,
) -> Vec<String> {
    if limit == 0 {
        return Vec::new();
    }

    let mut stale = key_ids
        .iter()
        .filter_map(|key_id| {
            let last_check_ts = last_check_timestamps.get(key_id).copied().unwrap_or(0);
            (last_check_ts == 0 || now_ts.saturating_sub(last_check_ts) >= interval_seconds)
                .then(|| (last_check_ts, key_id.clone()))
        })
        .collect::<Vec<_>>();
    stale.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    stale
        .into_iter()
        .take(limit)
        .map(|(_, key_id)| key_id)
        .collect()
}

async fn select_keys_for_provider(
    state: &AppState,
    runtime: &RuntimeState,
    provider: &StoredProviderCatalogProvider,
    interval_seconds: u64,
    max_keys_per_provider: usize,
    now_ts: u64,
) -> Result<Vec<StoredProviderCatalogKey>, GatewayError> {
    let lease = acquire_provider_self_check_lock(runtime, &provider.id).await;
    if lease.is_none() {
        return Ok(Vec::new());
    }

    let result = async {
        let keys = state
            .list_provider_catalog_keys_by_provider_ids(std::slice::from_ref(&provider.id))
            .await?
            .into_iter()
            .filter(|key| key.is_active)
            .collect::<Vec<_>>();
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let key_ids = keys.iter().map(|key| key.id.clone()).collect::<Vec<_>>();
        let check_stamps = load_check_timestamps(runtime, &provider.id, &key_ids).await;
        let selected_ids = select_account_self_check_key_ids(
            &key_ids,
            now_ts,
            interval_seconds,
            &check_stamps,
            max_keys_per_provider,
        );
        if selected_ids.is_empty() {
            return Ok(Vec::new());
        }

        mark_check_timestamps(
            runtime,
            &provider.id,
            &selected_ids,
            now_ts,
            interval_seconds,
        )
        .await;

        let mut keys_by_id = keys
            .into_iter()
            .map(|key| (key.id.clone(), key))
            .collect::<BTreeMap<_, _>>();
        Ok(selected_ids
            .into_iter()
            .filter_map(|key_id| keys_by_id.remove(&key_id))
            .collect::<Vec<_>>())
    }
    .await;

    release_provider_self_check_lock(runtime, lease).await;
    result
}

fn normalize_http_method(raw: Option<&Value>) -> String {
    let method = raw
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("GET")
        .to_ascii_uppercase();
    match method.as_str() {
        "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD" => method,
        _ => "GET".to_string(),
    }
}

fn json_string(raw: Option<&Value>) -> Option<String> {
    raw.and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn parse_status_codes(raw: Option<&Value>, fallback: &[u16]) -> BTreeSet<u16> {
    let mut out = BTreeSet::new();
    if let Some(array) = raw.and_then(Value::as_array) {
        for item in array {
            if let Some(value) = item
                .as_u64()
                .and_then(|value| u16::try_from(value).ok())
                .filter(|value| (100..=599).contains(value))
            {
                out.insert(value);
            }
        }
    }
    if out.is_empty() {
        out.extend(fallback.iter().copied());
    }
    out
}

fn parse_headers(raw: Option<&Value>) -> BTreeMap<String, String> {
    let Some(object) = raw.and_then(Value::as_object) else {
        return BTreeMap::new();
    };
    object
        .iter()
        .filter_map(|(key, value)| {
            let key = key.trim().to_ascii_lowercase();
            let value = match value {
                Value::String(value) => value.trim().to_string(),
                _ => value.to_string(),
            };
            (!key.is_empty()).then_some((key, value))
        })
        .collect()
}

pub(crate) fn parse_account_self_check_request_config(
    raw: Option<&Value>,
) -> AccountSelfCheckRequestConfig {
    let Some(object) = raw.and_then(Value::as_object) else {
        return AccountSelfCheckRequestConfig::default();
    };
    AccountSelfCheckRequestConfig {
        method: normalize_http_method(object.get("method")),
        url: json_string(object.get("url")),
        path: json_string(object.get("path")),
        headers: parse_headers(object.get("headers")),
        json_body: object
            .get("json_body")
            .or_else(|| object.get("json"))
            .or_else(|| object.get("body_json"))
            .cloned(),
        body: json_string(object.get("body")),
        body_bytes_b64: json_string(
            object
                .get("body_bytes_b64")
                .or_else(|| object.get("body_base64")),
        ),
        content_type: json_string(object.get("content_type")),
        success_status_codes: parse_status_codes(object.get("success_status_codes"), &[200]),
        blocked_status_codes: parse_status_codes(
            object
                .get("blocked_status_codes")
                .or_else(|| object.get("banned_status_codes")),
            &[401, 403, 423],
        ),
    }
}

fn custom_check_url(
    state: &AdminAppState<'_>,
    transport: &AdminGatewayProviderTransportSnapshot,
    request_config: &AccountSelfCheckRequestConfig,
) -> Option<String> {
    if let Some(url) = request_config.url.as_deref() {
        let parsed = url::Url::parse(url).ok()?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return None;
        }
        return Some(url.to_string());
    }
    let path = request_config.path.as_deref()?;
    state.build_passthrough_path_url(&transport.endpoint.base_url, path, None, &["key"])
}

async fn resolve_self_check_auth(
    state: &AdminAppState<'_>,
    transport: &AdminGatewayProviderTransportSnapshot,
) -> Result<Option<(String, String)>, GatewayError> {
    if let Some(auth) = state.resolve_local_oauth_header_auth(transport).await? {
        return Ok(Some(auth));
    }
    if let Some(auth) = crate::provider_transport::auth::resolve_local_openai_bearer_auth(transport)
    {
        return Ok(Some(auth));
    }
    if let Some(auth) = crate::provider_transport::auth::resolve_local_standard_auth(transport) {
        return Ok(Some(auth));
    }
    Ok(state.resolve_local_gemini_auth(transport))
}

fn custom_check_body(request_config: &AccountSelfCheckRequestConfig) -> RequestBody {
    if let Some(json_body) = request_config.json_body.clone() {
        return RequestBody::from_json(json_body);
    }
    if let Some(body_bytes_b64) = request_config.body_bytes_b64.as_ref() {
        return RequestBody {
            json_body: None,
            body_bytes_b64: Some(body_bytes_b64.clone()),
            body_ref: None,
        };
    }
    if let Some(body) = request_config.body.as_ref() {
        return RequestBody {
            json_body: None,
            body_bytes_b64: Some(base64::engine::general_purpose::STANDARD.encode(body.as_bytes())),
            body_ref: None,
        };
    }
    RequestBody {
        json_body: None,
        body_bytes_b64: None,
        body_ref: None,
    }
}

fn build_custom_check_plan(
    state: &AdminAppState<'_>,
    transport: &AdminGatewayProviderTransportSnapshot,
    request_config: &AccountSelfCheckRequestConfig,
    url: String,
    auth: Option<(String, String)>,
    proxy: Option<aether_contracts::ProxySnapshot>,
) -> ExecutionPlan {
    let mut headers = request_config.headers.clone();
    if let Some((auth_header, auth_value)) = auth {
        crate::provider_transport::ensure_upstream_auth_header(
            &mut headers,
            &auth_header,
            &auth_value,
        );
    }

    let content_type = request_config.content_type.clone().or_else(|| {
        request_config
            .json_body
            .is_some()
            .then(|| "application/json".to_string())
    });
    if let Some(content_type) = content_type.as_ref() {
        headers
            .entry("content-type".to_string())
            .or_insert_with(|| content_type.clone());
    }

    ExecutionPlan {
        request_id: format!("account-self-check-{}", Uuid::new_v4()),
        candidate_id: None,
        provider_name: Some(transport.provider.name.clone()),
        provider_id: transport.provider.id.clone(),
        endpoint_id: transport.endpoint.id.clone(),
        key_id: transport.key.id.clone(),
        method: request_config.method.clone(),
        url,
        headers,
        content_type,
        content_encoding: None,
        body: custom_check_body(request_config),
        stream: false,
        client_api_format: transport.endpoint.api_format.clone(),
        provider_api_format: transport.endpoint.api_format.clone(),
        model_name: None,
        proxy,
        transport_profile: state.resolve_transport_profile(transport),
        timeouts: state.resolve_transport_execution_timeouts(transport),
    }
}

fn execution_error_message(result: &ExecutionResult) -> Option<String> {
    if let Some(body_json) = result
        .body
        .as_ref()
        .and_then(|body| body.json_body.as_ref())
        .and_then(Value::as_object)
    {
        if let Some(error) = body_json.get("error") {
            if let Some(message) = error
                .get("message")
                .or_else(|| error.get("error_description"))
                .and_then(Value::as_str)
            {
                let trimmed = message.trim();
                if !trimmed.is_empty() {
                    return Some(trimmed.to_string());
                }
            }
            if let Some(text) = error
                .as_str()
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                return Some(text.to_string());
            }
        }
        if let Some(message) = body_json
            .get("message")
            .or_else(|| body_json.get("error_description"))
            .and_then(Value::as_str)
        {
            let trimmed = message.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    result
        .error
        .as_ref()
        .map(|error| error.message.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn custom_result_to_outcome(
    result: ExecutionResult,
    request_config: &AccountSelfCheckRequestConfig,
) -> AccountSelfCheckOutcome {
    let message = execution_error_message(&result);
    if request_config
        .success_status_codes
        .contains(&result.status_code)
    {
        return AccountSelfCheckOutcome::Success {
            status_code: Some(result.status_code),
            message,
        };
    }
    if request_config
        .blocked_status_codes
        .contains(&result.status_code)
    {
        let detail = message.unwrap_or_else(|| format!("HTTP {}", result.status_code));
        return AccountSelfCheckOutcome::Blocked {
            status_code: Some(result.status_code),
            message: detail,
        };
    }
    let detail = message.unwrap_or_else(|| format!("HTTP {}", result.status_code));
    AccountSelfCheckOutcome::Failed {
        status_code: Some(result.status_code),
        message: detail,
    }
}

async fn perform_custom_request_check(
    state: &AdminAppState<'_>,
    provider: &StoredProviderCatalogProvider,
    endpoint: &StoredProviderCatalogEndpoint,
    key: &StoredProviderCatalogKey,
    request_config: &AccountSelfCheckRequestConfig,
) -> Result<AccountSelfCheckOutcome, GatewayError> {
    let Some(transport) = state
        .read_provider_transport_snapshot(&provider.id, &endpoint.id, &key.id)
        .await?
    else {
        return Ok(AccountSelfCheckOutcome::Skipped {
            message: "Provider transport snapshot unavailable".to_string(),
        });
    };
    let Some(url) = custom_check_url(state, &transport, request_config) else {
        return Ok(AccountSelfCheckOutcome::Skipped {
            message: "account_self_check_request missing valid url/path".to_string(),
        });
    };
    let auth = resolve_self_check_auth(state, &transport).await?;
    let proxy = state
        .resolve_transport_proxy_snapshot_with_tunnel_affinity(&transport)
        .await;
    let plan = build_custom_check_plan(state, &transport, request_config, url, auth, proxy);
    match state.execute_execution_runtime_sync_plan(None, &plan).await {
        Ok(result) => Ok(custom_result_to_outcome(result, request_config)),
        Err(err) => Ok(AccountSelfCheckOutcome::Failed {
            status_code: None,
            message: gateway_error_message(err),
        }),
    }
}

fn quota_payload_result_for_key(key_id: &str, payload: Option<Value>) -> AccountSelfCheckOutcome {
    let Some(payload) = payload else {
        return AccountSelfCheckOutcome::Failed {
            status_code: None,
            message: "quota refresh returned no payload".to_string(),
        };
    };
    let Some(results) = payload.get("results").and_then(Value::as_array) else {
        return AccountSelfCheckOutcome::Failed {
            status_code: None,
            message: "quota refresh returned no result list".to_string(),
        };
    };
    let Some(item) = results.iter().find(|item| {
        item.get("key_id")
            .and_then(Value::as_str)
            .is_some_and(|value| value == key_id)
    }) else {
        return AccountSelfCheckOutcome::Failed {
            status_code: None,
            message: "quota refresh result missing key".to_string(),
        };
    };

    let status = item
        .get("status")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let status_code = item
        .get("status_code")
        .and_then(Value::as_u64)
        .and_then(|value| u16::try_from(value).ok());
    let message = item
        .get("message")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);

    if status == "success" {
        return AccountSelfCheckOutcome::Success {
            status_code,
            message,
        };
    }
    if quota_result_status_is_blocked(&status, status_code, message.as_deref()) {
        return AccountSelfCheckOutcome::Blocked {
            status_code,
            message: message.unwrap_or_else(|| status.clone()),
        };
    }
    AccountSelfCheckOutcome::Failed {
        status_code,
        message: message.unwrap_or_else(|| status.clone()),
    }
}

fn quota_result_status_is_blocked(
    status: &str,
    status_code: Option<u16>,
    message: Option<&str>,
) -> bool {
    matches!(
        status,
        "banned" | "forbidden" | "workspace_deactivated" | "auth_invalid"
    ) || matches!(status_code, Some(401 | 403 | 423))
        || aether_admin::provider::status::resolve_pool_account_state(None, None, message).blocked
}

async fn perform_quota_refresh_check(
    state: &AdminAppState<'_>,
    provider: &StoredProviderCatalogProvider,
    endpoint: &StoredProviderCatalogEndpoint,
    provider_type: &str,
    key: StoredProviderCatalogKey,
) -> Result<AccountSelfCheckOutcome, GatewayError> {
    let key_id = key.id.clone();
    let payload = refresh_provider_pool_quota_locally(
        state,
        provider,
        endpoint,
        provider_type,
        vec![key],
        None,
    )
    .await?;
    Ok(quota_payload_result_for_key(&key_id, payload))
}

fn account_block_reason(message: &str) -> String {
    let detail = message.trim();
    if detail.starts_with(OAUTH_ACCOUNT_BLOCK_PREFIX) {
        detail.to_string()
    } else {
        format!("{OAUTH_ACCOUNT_BLOCK_PREFIX}{detail}")
    }
}

fn request_failure_reason(message: &str) -> String {
    let detail = message.trim();
    if detail.starts_with(OAUTH_REQUEST_FAILED_PREFIX) {
        detail.to_string()
    } else {
        format!("{OAUTH_REQUEST_FAILED_PREFIX}{detail}")
    }
}

fn success_invalid_state(key: &StoredProviderCatalogKey) -> (Option<u64>, Option<String>) {
    let current_reason = key
        .oauth_invalid_reason
        .as_deref()
        .map(str::trim)
        .unwrap_or_default();
    if current_reason.starts_with("[REFRESH_FAILED] ") {
        return (
            key.oauth_invalid_at_unix_secs,
            (!current_reason.is_empty()).then_some(current_reason.to_string()),
        );
    }
    (None, None)
}

async fn persist_custom_check_outcome(
    state: &AdminAppState<'_>,
    key: &StoredProviderCatalogKey,
    outcome: &AccountSelfCheckOutcome,
    now_ts: u64,
) -> Result<bool, GatewayError> {
    match outcome {
        AccountSelfCheckOutcome::Success { .. } => {
            let (invalid_at, invalid_reason) = success_invalid_state(key);
            persist_provider_quota_refresh_state(
                state,
                &key.id,
                None,
                invalid_at,
                invalid_reason,
                None,
            )
            .await
        }
        AccountSelfCheckOutcome::Blocked { message, .. } => {
            persist_provider_quota_refresh_state(
                state,
                &key.id,
                None,
                Some(now_ts),
                Some(account_block_reason(message)),
                None,
            )
            .await
        }
        AccountSelfCheckOutcome::Failed { message, .. } => {
            persist_provider_quota_refresh_state(
                state,
                &key.id,
                None,
                Some(now_ts),
                Some(request_failure_reason(message)),
                None,
            )
            .await
        }
        AccountSelfCheckOutcome::Skipped { .. } => Ok(false),
    }
}

async fn persist_self_check_outcome(
    state: &AdminAppState<'_>,
    method: &str,
    key: &StoredProviderCatalogKey,
    outcome: &AccountSelfCheckOutcome,
    now_ts: u64,
) -> Result<bool, GatewayError> {
    if method == "custom_request" {
        return persist_custom_check_outcome(state, key, outcome, now_ts).await;
    }
    Ok(!matches!(outcome, AccountSelfCheckOutcome::Skipped { .. }))
}

async fn record_score_probe_in_progress_for_key(
    state: &AppState,
    provider_id: &str,
    key_id: &str,
    attempted_at: u64,
) {
    if !state.data.has_pool_score_writer() {
        return;
    }
    let attempt = PoolMemberProbeAttempt {
        identity: PoolMemberIdentity::provider_api_key(provider_id.to_string(), key_id.to_string()),
        scope: None,
        attempted_at,
        score_reason_patch: Some(json!({
            "last_probe": {
                "source": "account_self_check",
                "status": "in_progress"
            },
            "last_self_check": {
                "source": "account_self_check",
                "status": "in_progress",
                "attempted_at": attempted_at
            }
        })),
    };
    if let Err(err) = state.data.mark_pool_member_probe_in_progress(attempt).await {
        debug!(
            provider_id,
            key_id,
            error = ?err,
            "gateway account self-check: failed to mark score probe in progress"
        );
    }
}

async fn record_score_probe_result_for_key(
    state: &AppState,
    provider_id: &str,
    key_id: &str,
    attempted_at: u64,
    outcome: &AccountSelfCheckOutcome,
) {
    if !state.data.has_pool_score_writer() {
        return;
    }
    let (succeeded, hard_state, probe_status) = match outcome {
        AccountSelfCheckOutcome::Success { .. } => (
            true,
            Some(PoolMemberHardState::Available),
            PoolMemberProbeStatus::Ok,
        ),
        AccountSelfCheckOutcome::Blocked { .. } => (
            false,
            Some(PoolMemberHardState::Banned),
            PoolMemberProbeStatus::Failed,
        ),
        AccountSelfCheckOutcome::Failed { .. } => (
            false,
            Some(PoolMemberHardState::Cooldown),
            PoolMemberProbeStatus::Failed,
        ),
        AccountSelfCheckOutcome::Skipped { .. } => (
            false,
            Some(PoolMemberHardState::Unknown),
            PoolMemberProbeStatus::Never,
        ),
    };
    let result = PoolMemberProbeResult {
        identity: PoolMemberIdentity::provider_api_key(provider_id.to_string(), key_id.to_string()),
        scope: None,
        attempted_at,
        succeeded,
        hard_state,
        probe_status,
        score_reason_patch: Some(json!({
            "last_probe": {
                "source": "account_self_check",
                "status": outcome.score_status(),
                "status_code": outcome.status_code(),
                "message": outcome.message()
            },
            "last_self_check": {
                "source": "account_self_check",
                "status": outcome.score_status(),
                "status_code": outcome.status_code(),
                "message": outcome.message(),
                "attempted_at": attempted_at
            }
        })),
    };
    if let Err(err) = state.data.record_pool_member_probe_result(result).await {
        debug!(
            provider_id,
            key_id,
            error = ?err,
            "gateway account self-check: failed to record score probe result"
        );
    }
}

fn endpoint_for_self_check(
    provider_type: &str,
    endpoints: &[StoredProviderCatalogEndpoint],
) -> Option<StoredProviderCatalogEndpoint> {
    provider_quota_refresh_endpoint_for_provider(provider_type, endpoints, true)
        .or_else(|| {
            endpoints
                .iter()
                .find(|endpoint| endpoint.is_active)
                .cloned()
        })
        .or_else(|| endpoints.first().cloned())
}

fn gateway_error_message(err: GatewayError) -> String {
    match err {
        GatewayError::UpstreamUnavailable { message, .. }
        | GatewayError::ControlUnavailable { message, .. }
        | GatewayError::Client { message, .. }
        | GatewayError::Internal(message) => message,
    }
}

fn update_summary_from_outcome(
    summary: &mut AccountSelfCheckRunSummary,
    outcome: &AccountSelfCheckOutcome,
) {
    match outcome {
        AccountSelfCheckOutcome::Success { .. } => {
            summary.succeeded = summary.succeeded.saturating_add(1);
        }
        AccountSelfCheckOutcome::Blocked { .. } => {
            summary.blocked = summary.blocked.saturating_add(1);
        }
        AccountSelfCheckOutcome::Failed { .. } => {
            summary.failed = summary.failed.saturating_add(1);
        }
        AccountSelfCheckOutcome::Skipped { .. } => {
            summary.skipped = summary.skipped.saturating_add(1);
        }
    }
}

pub(crate) async fn perform_account_self_check_once_with_config(
    state: &AppState,
    config: AccountSelfCheckWorkerConfig,
) -> Result<AccountSelfCheckRunSummary, GatewayError> {
    if !state.has_provider_catalog_data_reader() || !state.has_provider_catalog_data_writer() {
        return Ok(AccountSelfCheckRunSummary::empty());
    }

    let providers = state
        .list_provider_catalog_providers(true)
        .await?
        .into_iter()
        .filter_map(|provider| {
            let provider_type = provider.provider_type.trim().to_ascii_lowercase();
            let pool_config = admin_provider_pool_config(&provider)?;
            if pool_config.account_self_check_enabled {
                Some((provider, provider_type, pool_config))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    if providers.is_empty() {
        return Ok(AccountSelfCheckRunSummary::empty());
    }

    let provider_ids = providers
        .iter()
        .map(|(provider, _, _)| provider.id.clone())
        .collect::<Vec<_>>();
    let mut endpoints_by_provider = BTreeMap::<String, Vec<StoredProviderCatalogEndpoint>>::new();
    for endpoint in state
        .list_provider_catalog_endpoints_by_provider_ids(&provider_ids)
        .await?
    {
        endpoints_by_provider
            .entry(endpoint.provider_id.clone())
            .or_default()
            .push(endpoint);
    }

    let admin_state = AdminAppState::new(state);
    let now_ts = now_unix_secs();
    let mut summary = AccountSelfCheckRunSummary {
        providers_checked: providers.len(),
        ..AccountSelfCheckRunSummary::empty()
    };

    for (provider, provider_type, pool_config) in providers {
        let provider_endpoints = endpoints_by_provider
            .get(&provider.id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let Some(endpoint) = endpoint_for_self_check(&provider_type, provider_endpoints) else {
            summary.providers_skipped = summary.providers_skipped.saturating_add(1);
            continue;
        };
        if pool_config.account_self_check_method == "quota_refresh"
            && !provider_type_supports_quota_refresh(&provider_type)
        {
            summary.providers_skipped = summary.providers_skipped.saturating_add(1);
            continue;
        }

        let interval_seconds = pool_config
            .account_self_check_interval_minutes
            .clamp(1, 1440)
            .saturating_mul(60);
        let provider_limit = config.max_keys_per_provider;
        let keys = select_keys_for_provider(
            state,
            state.runtime_state.as_ref(),
            &provider,
            interval_seconds,
            provider_limit,
            now_ts,
        )
        .await?;
        if keys.is_empty() {
            continue;
        }

        let selected_count = keys.len();
        summary.scanned_keys = summary.scanned_keys.saturating_add(selected_count);
        summary.selected_keys = summary.selected_keys.saturating_add(selected_count);
        summary.providers_checked_with_keys = summary.providers_checked_with_keys.saturating_add(1);
        for key in &keys {
            record_score_probe_in_progress_for_key(state, &provider.id, &key.id, now_ts).await;
        }

        let method = pool_config.account_self_check_method.clone();
        let request_config = parse_account_self_check_request_config(
            pool_config.account_self_check_request.as_ref(),
        );
        let provider_short_id = provider.id.chars().take(8).collect::<String>();
        let concurrency = (pool_config.account_self_check_concurrency as usize)
            .clamp(1, 64)
            .min(config.global_concurrency)
            .max(1);
        let check_results = stream::iter(keys.into_iter().map(|key| {
            let admin_state = &admin_state;
            let provider = &provider;
            let endpoint = &endpoint;
            let provider_type = provider_type.as_str();
            let method = method.as_str();
            let request_config = &request_config;
            async move {
                let key_for_check = key.clone();
                let result = if method == "custom_request" {
                    perform_custom_request_check(
                        admin_state,
                        provider,
                        endpoint,
                        &key_for_check,
                        request_config,
                    )
                    .await
                } else {
                    perform_quota_refresh_check(
                        admin_state,
                        provider,
                        endpoint,
                        provider_type,
                        key_for_check,
                    )
                    .await
                };
                (key, result)
            }
        }))
        .buffer_unordered(concurrency)
        .collect::<Vec<_>>()
        .await;

        for (key, result) in check_results {
            let outcome = match result {
                Ok(outcome) => outcome,
                Err(err) => AccountSelfCheckOutcome::Failed {
                    status_code: None,
                    message: gateway_error_message(err),
                },
            };
            let persisted =
                persist_self_check_outcome(&admin_state, &method, &key, &outcome, now_ts).await?;
            if !persisted && !matches!(outcome, AccountSelfCheckOutcome::Skipped { .. }) {
                warn!(
                    provider_id = %provider.id,
                    key_id = %key.id,
                    "gateway account self-check: key state was not updated"
                );
            }
            record_score_probe_result_for_key(state, &provider.id, &key.id, now_ts, &outcome).await;
            update_summary_from_outcome(&mut summary, &outcome);
        }

        info!(
            provider_id = %provider_short_id,
            provider_type,
            selected = selected_count,
            concurrency,
            "gateway account self-check completed"
        );
    }

    Ok(summary)
}

pub(crate) async fn perform_account_self_check_once(
    state: &AppState,
) -> Result<AccountSelfCheckRunSummary, GatewayError> {
    perform_account_self_check_once_with_config(state, AccountSelfCheckWorkerConfig::from_env())
        .await
}

pub(crate) fn spawn_account_self_check_worker(
    state: AppState,
) -> Option<tokio::task::JoinHandle<()>> {
    if !state.has_provider_catalog_data_reader() || !state.has_provider_catalog_data_writer() {
        return None;
    }

    let config = AccountSelfCheckWorkerConfig::from_env();
    Some(tokio::spawn(async move {
        let mut interval = tokio::time::interval(config.scan_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(err) = perform_account_self_check_once_with_config(&state, config).await {
                warn!(
                    error = ?err,
                    "gateway account self-check worker tick failed"
                );
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::{
        parse_account_self_check_request_config, select_account_self_check_key_ids,
        AccountSelfCheckRequestConfig,
    };
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};

    #[test]
    fn selects_never_and_stale_self_check_keys_first() {
        let key_ids = vec![
            "fresh".to_string(),
            "never".to_string(),
            "stale".to_string(),
        ];
        let stamps = BTreeMap::from([("fresh".to_string(), 1_950), ("stale".to_string(), 1_000)]);

        let selected = select_account_self_check_key_ids(&key_ids, 2_000, 600, &stamps, 2);

        assert_eq!(selected, vec!["never".to_string(), "stale".to_string()]);
    }

    #[test]
    fn parses_custom_self_check_request_config() {
        let parsed = parse_account_self_check_request_config(Some(&json!({
            "method": "post",
            "path": "/v1/me?trace=1",
            "headers": {"X-Test": "yes"},
            "json_body": {"ping": true},
            "success_status_codes": [200, 204],
            "blocked_status_codes": [401, 403, 423, 451]
        })));

        assert_eq!(parsed.method, "POST");
        assert_eq!(parsed.path.as_deref(), Some("/v1/me?trace=1"));
        assert_eq!(
            parsed.headers.get("x-test").map(String::as_str),
            Some("yes")
        );
        assert_eq!(parsed.json_body, Some(json!({"ping": true})));
        assert_eq!(parsed.success_status_codes, BTreeSet::from([200, 204]));
        assert_eq!(
            parsed.blocked_status_codes,
            BTreeSet::from([401, 403, 423, 451])
        );
    }

    #[test]
    fn defaults_custom_self_check_request_config() {
        assert_eq!(
            parse_account_self_check_request_config(None),
            AccountSelfCheckRequestConfig::default()
        );
    }
}
