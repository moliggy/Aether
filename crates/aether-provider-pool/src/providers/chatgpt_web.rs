use std::collections::BTreeMap;

use aether_data_contracts::repository::provider_catalog::StoredProviderCatalogEndpoint;
use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::capability::ProviderPoolCapabilities;
use crate::provider::{
    provider_pool_endpoint_format_matches, provider_pool_matching_endpoint, ProviderPoolAdapter,
    ProviderPoolMemberInput,
};
use crate::quota::{
    provider_pool_json_bool, provider_pool_json_f64, provider_pool_metadata_bucket,
    provider_pool_quota_snapshot_exhausted_decision,
};
use crate::quota_refresh::ProviderPoolQuotaRequestSpec;

pub const CHATGPT_WEB_DEFAULT_BASE_URL: &str = "https://chatgpt.com";
pub const CHATGPT_WEB_CONVERSATION_INIT_PATH: &str = "/backend-api/conversation/init";

const CHATGPT_WEB_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36 Edg/143.0.0.0";
const CHATGPT_WEB_CLIENT_VERSION: &str = "prod-be885abbfcfe7b1f511e88b3003d9ee44757fbad";
const CHATGPT_WEB_BUILD_NUMBER: &str = "5955942";
const CHATGPT_WEB_SEC_CH_UA: &str =
    r#""Microsoft Edge";v="143", "Chromium";v="143", "Not A(Brand";v="24""#;
const CHATGPT_WEB_FREE_IMAGE_QUOTA_LIMIT: f64 = 25.0;

#[derive(Debug, Clone, Default)]
pub struct ChatGptWebProviderPoolAdapter;

impl ProviderPoolAdapter for ChatGptWebProviderPoolAdapter {
    fn provider_type(&self) -> &'static str {
        "chatgpt_web"
    }

    fn capabilities(&self) -> ProviderPoolCapabilities {
        ProviderPoolCapabilities {
            quota_refresh: true,
            ..ProviderPoolCapabilities::default()
        }
    }

    fn quota_exhausted(&self, input: &ProviderPoolMemberInput<'_>) -> bool {
        if let Some(exhausted) =
            provider_pool_quota_snapshot_exhausted_decision(input.key, input.provider_type)
        {
            return exhausted;
        }
        provider_pool_metadata_bucket(input.key.upstream_metadata.as_ref(), input.provider_type)
            .is_some_and(quota_exhausted_from_bucket)
    }

    fn quota_refresh_endpoint(
        &self,
        endpoints: &[StoredProviderCatalogEndpoint],
        include_inactive: bool,
    ) -> Option<StoredProviderCatalogEndpoint> {
        provider_pool_matching_endpoint(endpoints, include_inactive, |endpoint| {
            provider_pool_endpoint_format_matches(endpoint, "openai:image")
        })
    }

    fn quota_refresh_missing_endpoint_message(&self) -> String {
        "找不到有效的 openai:image 端点".to_string()
    }
}

pub fn build_chatgpt_web_pool_quota_request(
    key_id: &str,
    endpoint_base_url: &str,
    authorization: (String, String),
) -> ProviderPoolQuotaRequestSpec {
    let base_url = chatgpt_web_base_url(endpoint_base_url);
    let device_id = Uuid::new_v4().to_string();
    let session_id = Uuid::new_v4().to_string();
    let mut headers = BTreeMap::from([
        ("accept".to_string(), "application/json".to_string()),
        ("content-type".to_string(), "application/json".to_string()),
        ("user-agent".to_string(), CHATGPT_WEB_USER_AGENT.to_string()),
        ("origin".to_string(), base_url.clone()),
        ("referer".to_string(), format!("{base_url}/")),
        (
            "accept-language".to_string(),
            "zh-CN,zh;q=0.9,en;q=0.8,en-US;q=0.7".to_string(),
        ),
        ("cache-control".to_string(), "no-cache".to_string()),
        ("pragma".to_string(), "no-cache".to_string()),
        ("priority".to_string(), "u=1, i".to_string()),
        ("sec-ch-ua".to_string(), CHATGPT_WEB_SEC_CH_UA.to_string()),
        ("sec-ch-ua-arch".to_string(), r#""x86""#.to_string()),
        ("sec-ch-ua-bitness".to_string(), r#""64""#.to_string()),
        ("sec-ch-ua-mobile".to_string(), "?0".to_string()),
        ("sec-ch-ua-model".to_string(), r#""""#.to_string()),
        ("sec-ch-ua-platform".to_string(), r#""Windows""#.to_string()),
        (
            "sec-ch-ua-platform-version".to_string(),
            r#""19.0.0""#.to_string(),
        ),
        ("sec-fetch-dest".to_string(), "empty".to_string()),
        ("sec-fetch-mode".to_string(), "cors".to_string()),
        ("sec-fetch-site".to_string(), "same-origin".to_string()),
        ("oai-device-id".to_string(), device_id),
        ("oai-session-id".to_string(), session_id),
        ("oai-language".to_string(), "zh-CN".to_string()),
        (
            "oai-client-version".to_string(),
            CHATGPT_WEB_CLIENT_VERSION.to_string(),
        ),
        (
            "oai-client-build-number".to_string(),
            CHATGPT_WEB_BUILD_NUMBER.to_string(),
        ),
        (
            "x-openai-target-path".to_string(),
            CHATGPT_WEB_CONVERSATION_INIT_PATH.to_string(),
        ),
        (
            "x-openai-target-route".to_string(),
            CHATGPT_WEB_CONVERSATION_INIT_PATH.to_string(),
        ),
    ]);
    headers.insert(authorization.0.to_ascii_lowercase(), authorization.1);

    ProviderPoolQuotaRequestSpec {
        request_id: format!("chatgpt-web-quota:{key_id}"),
        provider_name: "chatgpt_web".to_string(),
        quota_kind: "chatgpt_web".to_string(),
        method: "POST".to_string(),
        url: format!("{base_url}{CHATGPT_WEB_CONVERSATION_INIT_PATH}"),
        headers,
        content_type: Some("application/json".to_string()),
        json_body: Some(json!({
            "gizmo_id": Value::Null,
            "requested_default_model": Value::Null,
            "conversation_id": Value::Null,
            "timezone_offset_min": -480,
            "system_hints": ["picture_v2"],
        })),
        client_api_format: "openai:image".to_string(),
        provider_api_format: "chatgpt_web:conversation_init".to_string(),
        model_name: Some("chatgpt-web-conversation-init".to_string()),
        accept_invalid_certs: true,
    }
}

fn chatgpt_web_base_url(endpoint_base_url: &str) -> String {
    let base_url = endpoint_base_url.trim().trim_end_matches('/');
    if base_url.is_empty() {
        CHATGPT_WEB_DEFAULT_BASE_URL.to_string()
    } else {
        base_url.to_string()
    }
}

pub fn enrich_chatgpt_web_quota_metadata(metadata: &mut Value, auth_config: Option<&Value>) {
    let Some(object) = metadata.as_object_mut() else {
        return;
    };
    for (target, fields) in [
        ("plan_type", &["plan_type", "tier", "plan"][..]),
        ("email", &["email"][..]),
        ("account_id", &["account_id", "accountId"][..]),
        ("account_user_id", &["account_user_id", "accountUserId"][..]),
        ("user_id", &["user_id", "userId"][..]),
    ] {
        if object.contains_key(target) {
            continue;
        }
        if let Some(value) = chatgpt_web_auth_config_string(auth_config, fields) {
            object.insert(target.to_string(), json!(value));
        }
    }
}

pub fn normalize_chatgpt_web_image_quota_limit(
    metadata: &mut Value,
    upstream_metadata: Option<&Value>,
) {
    let existing_limit = existing_chatgpt_web_image_quota_limit(upstream_metadata);
    let Some(object) = metadata.as_object_mut() else {
        return;
    };

    let remaining = provider_pool_json_f64(object.get("image_quota_remaining"));
    let explicit_limit =
        provider_pool_json_f64(object.get("image_quota_total")).filter(|value| *value > 0.0);
    let plan_type = chatgpt_web_json_string(object.get("plan_type"));
    let is_free_plan = plan_type.is_some_and(|value| value.trim().eq_ignore_ascii_case("free"));
    let limit = if is_free_plan {
        Some(CHATGPT_WEB_FREE_IMAGE_QUOTA_LIMIT)
    } else {
        explicit_limit
            .or_else(|| infer_chatgpt_web_image_quota_limit(plan_type, remaining, existing_limit))
    };

    if let Some(limit) = limit {
        object.insert("image_quota_total".to_string(), json!(limit));

        if !object.contains_key("image_quota_used") {
            if let Some(remaining) = remaining {
                object.insert(
                    "image_quota_used".to_string(),
                    json!((limit - remaining).max(0.0)),
                );
            } else if object.get("image_quota_blocked").and_then(Value::as_bool) == Some(true) {
                object.insert("image_quota_used".to_string(), json!(limit));
            }
        }
    }
}

fn chatgpt_web_auth_config_string(auth_config: Option<&Value>, fields: &[&str]) -> Option<String> {
    let object = auth_config.and_then(Value::as_object)?;
    fields.iter().find_map(|field| {
        object
            .get(*field)
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    })
}

fn chatgpt_web_json_string(value: Option<&Value>) -> Option<&str> {
    value
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn existing_chatgpt_web_image_quota_limit(upstream_metadata: Option<&Value>) -> Option<f64> {
    upstream_metadata
        .and_then(Value::as_object)
        .and_then(|metadata| metadata.get("chatgpt_web"))
        .and_then(Value::as_object)
        .and_then(|bucket| provider_pool_json_f64(bucket.get("image_quota_total")))
        .filter(|value| *value > 0.0)
}

fn infer_chatgpt_web_image_quota_limit(
    plan_type: Option<&str>,
    remaining: Option<f64>,
    existing_limit: Option<f64>,
) -> Option<f64> {
    let normalized_plan = plan_type.unwrap_or_default().trim().to_ascii_lowercase();
    if normalized_plan == "free" {
        return Some(CHATGPT_WEB_FREE_IMAGE_QUOTA_LIMIT);
    }

    if let Some(existing_limit) = existing_limit.filter(|value| *value > 0.0) {
        return Some(existing_limit);
    }

    remaining.filter(|value| *value > 0.0)
}

pub(crate) fn quota_exhausted_from_bucket(bucket: &Map<String, Value>) -> bool {
    if provider_pool_json_bool(bucket.get("image_quota_blocked")) == Some(true) {
        return true;
    }
    if provider_pool_json_f64(bucket.get("image_quota_remaining")).is_some_and(|value| value <= 0.0)
    {
        return true;
    }
    match (
        provider_pool_json_f64(bucket.get("image_quota_total")),
        provider_pool_json_f64(bucket.get("image_quota_used")),
    ) {
        (Some(limit), Some(used)) if limit > 0.0 => used >= limit,
        _ => false,
    }
}
