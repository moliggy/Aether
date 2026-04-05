use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use serde_json::{json, Value};

use super::oauth_refresh::{
    CachedOAuthEntry, LocalOAuthRefreshAdapter, LocalOAuthRefreshError,
    LocalResolvedOAuthRequestAuth,
};
use super::snapshot::GatewayProviderTransportSnapshot;

const AUTH_HEADER_NAME: &str = "authorization";
const OAUTH_REFRESH_SKEW_SECS: u64 = 120;
const PLACEHOLDER_API_KEY: &str = "__placeholder__";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GenericOAuthTemplate {
    provider_type: &'static str,
    token_url: &'static str,
    client_id: &'static str,
    client_secret: &'static str,
    scopes: &'static [&'static str],
    uses_json_payload: bool,
}

const GENERIC_OAUTH_TEMPLATES: &[GenericOAuthTemplate] = &[
    GenericOAuthTemplate {
        provider_type: "claude_code",
        token_url: "https://console.anthropic.com/v1/oauth/token",
        client_id: "9d1c250a-e61b-44d9-88ed-5944d1962f5e",
        client_secret: "",
        scopes: &["org:create_api_key", "user:profile", "user:inference"],
        uses_json_payload: true,
    },
    GenericOAuthTemplate {
        provider_type: "codex",
        token_url: "https://auth.openai.com/oauth/token",
        client_id: "app_EMoamEEZ73f0CkXaXp7hrann",
        client_secret: "",
        scopes: &["openid", "email", "profile", "offline_access"],
        uses_json_payload: false,
    },
    GenericOAuthTemplate {
        provider_type: "gemini_cli",
        token_url: "https://oauth2.googleapis.com/token",
        client_id: "681255809395-oo8ft2oprdrnp9e3aqf6av3hmdib135j.apps.googleusercontent.com",
        client_secret: "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",
        scopes: &[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/userinfo.profile",
        ],
        uses_json_payload: false,
    },
    GenericOAuthTemplate {
        provider_type: "antigravity",
        token_url: "https://oauth2.googleapis.com/token",
        client_id: "1071006060591-tmhssin2h21lcre235vtolojh4g403ep.apps.googleusercontent.com",
        client_secret: "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",
        scopes: &[
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/userinfo.email",
            "https://www.googleapis.com/auth/userinfo.profile",
            "https://www.googleapis.com/auth/cclog",
            "https://www.googleapis.com/auth/experimentsandconfigs",
        ],
        uses_json_payload: false,
    },
];

pub fn supports_local_generic_oauth_request_auth_resolution(
    transport: &GatewayProviderTransportSnapshot,
) -> bool {
    transport.key.auth_type.trim().eq_ignore_ascii_case("oauth")
        && template_for_provider_type(transport.provider.provider_type.as_str()).is_some()
}

#[derive(Debug, Clone, Default)]
pub struct GenericOAuthRefreshAdapter {
    token_url_overrides: BTreeMap<String, String>,
}

impl GenericOAuthRefreshAdapter {
    pub fn with_token_url_for_tests(
        mut self,
        provider_type: &str,
        token_url: impl Into<String>,
    ) -> Self {
        self.token_url_overrides
            .insert(provider_type.trim().to_ascii_lowercase(), token_url.into());
        self
    }

    fn token_url_for_template(&self, template: GenericOAuthTemplate) -> String {
        self.token_url_overrides
            .get(template.provider_type)
            .cloned()
            .unwrap_or_else(|| template.token_url.to_string())
    }

    fn auth_config_from_transport(transport: &GatewayProviderTransportSnapshot) -> Option<Value> {
        transport
            .key
            .decrypted_auth_config
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .and_then(|value| serde_json::from_str::<Value>(value).ok())
    }

    fn auth_config_from_entry(
        transport: &GatewayProviderTransportSnapshot,
        entry: &CachedOAuthEntry,
    ) -> Option<Value> {
        entry
            .metadata
            .as_ref()
            .filter(|_| {
                entry
                    .provider_type
                    .eq_ignore_ascii_case(transport.provider.provider_type.as_str())
            })
            .cloned()
    }

    fn base_auth_config(
        &self,
        transport: &GatewayProviderTransportSnapshot,
        entry: Option<&CachedOAuthEntry>,
    ) -> Option<Value> {
        entry
            .and_then(|cached| Self::auth_config_from_entry(transport, cached))
            .or_else(|| Self::auth_config_from_transport(transport))
    }

    fn resolve_direct_header(
        &self,
        transport: &GatewayProviderTransportSnapshot,
    ) -> Option<LocalResolvedOAuthRequestAuth> {
        if !supports_local_generic_oauth_request_auth_resolution(transport) {
            return None;
        }

        let secret = transport.key.decrypted_api_key.trim();
        if secret.is_empty() || secret == PLACEHOLDER_API_KEY {
            return None;
        }

        let auth_config = Self::auth_config_from_transport(transport);
        let refreshable = auth_config
            .as_ref()
            .and_then(refresh_token_from_auth_config)
            .is_some();
        if refreshable && auth_config_expires_soon(auth_config.as_ref()) {
            return None;
        }

        Some(LocalResolvedOAuthRequestAuth::Header {
            name: AUTH_HEADER_NAME.to_string(),
            value: format!("Bearer {secret}"),
        })
    }

    fn build_cached_entry(
        &self,
        template: GenericOAuthTemplate,
        access_token: &str,
        metadata: Value,
        expires_at_unix_secs: Option<u64>,
    ) -> CachedOAuthEntry {
        CachedOAuthEntry {
            provider_type: template.provider_type.to_string(),
            auth_header_name: AUTH_HEADER_NAME.to_string(),
            auth_header_value: format!("Bearer {access_token}"),
            expires_at_unix_secs,
            metadata: Some(metadata),
        }
    }
}

#[async_trait]
impl LocalOAuthRefreshAdapter for GenericOAuthRefreshAdapter {
    fn provider_type(&self) -> &'static str {
        "generic_oauth"
    }

    fn supports(&self, transport: &GatewayProviderTransportSnapshot) -> bool {
        supports_local_generic_oauth_request_auth_resolution(transport)
    }

    fn resolve_cached(
        &self,
        transport: &GatewayProviderTransportSnapshot,
        entry: &CachedOAuthEntry,
    ) -> Option<LocalResolvedOAuthRequestAuth> {
        if !entry
            .provider_type
            .eq_ignore_ascii_case(transport.provider.provider_type.as_str())
        {
            return None;
        }
        if expires_at_requires_refresh(entry.expires_at_unix_secs) {
            return None;
        }

        let name = entry.auth_header_name.trim();
        let value = entry.auth_header_value.trim();
        if name.is_empty() || value.is_empty() {
            return None;
        }

        Some(LocalResolvedOAuthRequestAuth::Header {
            name: name.to_ascii_lowercase(),
            value: value.to_string(),
        })
    }

    fn resolve_without_refresh(
        &self,
        transport: &GatewayProviderTransportSnapshot,
    ) -> Option<LocalResolvedOAuthRequestAuth> {
        self.resolve_direct_header(transport)
    }

    fn should_refresh(
        &self,
        transport: &GatewayProviderTransportSnapshot,
        entry: Option<&CachedOAuthEntry>,
    ) -> bool {
        if !supports_local_generic_oauth_request_auth_resolution(transport) {
            return false;
        }
        if entry
            .and_then(|cached| self.resolve_cached(transport, cached))
            .is_some()
            || self.resolve_direct_header(transport).is_some()
        {
            return false;
        }

        self.base_auth_config(transport, entry)
            .as_ref()
            .and_then(refresh_token_from_auth_config)
            .is_some()
    }

    async fn refresh(
        &self,
        client: &reqwest::Client,
        transport: &GatewayProviderTransportSnapshot,
        entry: Option<&CachedOAuthEntry>,
    ) -> Result<Option<CachedOAuthEntry>, LocalOAuthRefreshError> {
        let Some(template) = template_for_provider_type(transport.provider.provider_type.as_str())
        else {
            return Ok(None);
        };
        let mut metadata = self
            .base_auth_config(transport, entry)
            .and_then(|value| value.as_object().cloned())
            .unwrap_or_default();
        let Some(refresh_token) = metadata.get("refresh_token").and_then(non_empty_string) else {
            return Ok(None);
        };

        let token_url = self.token_url_for_template(template);
        let scope = (!template.scopes.is_empty()).then(|| template.scopes.join(" "));
        let request = client.post(token_url);
        let response = if template.uses_json_payload {
            let mut body = serde_json::Map::from_iter([
                (
                    "grant_type".to_string(),
                    Value::String("refresh_token".to_string()),
                ),
                (
                    "client_id".to_string(),
                    Value::String(template.client_id.to_string()),
                ),
                (
                    "refresh_token".to_string(),
                    Value::String(refresh_token.clone()),
                ),
            ]);
            if let Some(scope) = scope.as_ref() {
                body.insert("scope".to_string(), Value::String(scope.clone()));
            }
            request
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .json(&Value::Object(body))
                .send()
                .await
        } else {
            let mut form = vec![
                ("grant_type", "refresh_token".to_string()),
                ("client_id", template.client_id.to_string()),
                ("refresh_token", refresh_token.clone()),
            ];
            if let Some(scope) = scope.as_ref() {
                form.push(("scope", scope.clone()));
            }
            if !template.client_secret.trim().is_empty() {
                form.push(("client_secret", template.client_secret.to_string()));
            }
            request
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("Accept", "application/json")
                .form(&form)
                .send()
                .await
        }
        .map_err(|source| LocalOAuthRefreshError::Transport {
            provider_type: template.provider_type,
            source,
        })?;

        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|source| LocalOAuthRefreshError::Transport {
                provider_type: template.provider_type,
                source,
            })?;
        if !status.is_success() {
            return Err(LocalOAuthRefreshError::HttpStatus {
                provider_type: template.provider_type,
                status_code: status.as_u16(),
                body_excerpt: truncate_body(&body),
            });
        }

        let payload: Value =
            serde_json::from_str(&body).map_err(|_| LocalOAuthRefreshError::InvalidResponse {
                provider_type: template.provider_type,
                message: "generic oauth refresh returned non-json body".to_string(),
            })?;
        let Some(access_token) = payload.get("access_token").and_then(non_empty_string) else {
            return Err(LocalOAuthRefreshError::InvalidResponse {
                provider_type: template.provider_type,
                message: "generic oauth refresh returned empty access_token".to_string(),
            });
        };

        let expires_at_unix_secs = resolve_expires_at(payload.get("expires_in"));
        metadata.insert(
            "provider_type".to_string(),
            Value::String(template.provider_type.to_string()),
        );
        metadata.insert("updated_at".to_string(), json!(current_unix_secs()));
        if let Some(refresh_token) = payload.get("refresh_token").and_then(non_empty_string) {
            metadata.insert("refresh_token".to_string(), Value::String(refresh_token));
        }
        if let Some(token_type) = payload.get("token_type").and_then(non_empty_string) {
            metadata.insert("token_type".to_string(), Value::String(token_type));
        }
        if let Some(scope) = payload.get("scope").and_then(non_empty_string) {
            metadata.insert("scope".to_string(), Value::String(scope));
        }
        match expires_at_unix_secs {
            Some(expires_at_unix_secs) => {
                metadata.insert("expires_at".to_string(), json!(expires_at_unix_secs));
            }
            None => {
                metadata.remove("expires_at");
            }
        }

        Ok(Some(self.build_cached_entry(
            template,
            access_token.as_str(),
            Value::Object(metadata),
            expires_at_unix_secs,
        )))
    }
}

fn template_for_provider_type(provider_type: &str) -> Option<GenericOAuthTemplate> {
    let normalized = provider_type.trim();
    GENERIC_OAUTH_TEMPLATES
        .iter()
        .find(|template| normalized.eq_ignore_ascii_case(template.provider_type))
        .copied()
}

fn refresh_token_from_auth_config(auth_config: &Value) -> Option<String> {
    auth_config
        .as_object()
        .and_then(|object| object.get("refresh_token"))
        .and_then(non_empty_string)
}

fn auth_config_expires_soon(auth_config: Option<&Value>) -> bool {
    expires_at_requires_refresh(
        auth_config
            .and_then(|value| value.as_object())
            .and_then(|object| object.get("expires_at"))
            .and_then(|value| parse_u64_value(Some(value))),
    )
}

fn expires_at_requires_refresh(expires_at_unix_secs: Option<u64>) -> bool {
    expires_at_unix_secs
        .map(|expires_at_unix_secs| {
            current_unix_secs() >= expires_at_unix_secs.saturating_sub(OAUTH_REFRESH_SKEW_SECS)
        })
        .unwrap_or(false)
}

fn resolve_expires_at(expires_in: Option<&Value>) -> Option<u64> {
    parse_u64_value(expires_in).map(|expires_in| current_unix_secs().saturating_add(expires_in))
}

fn parse_u64_value(value: Option<&Value>) -> Option<u64> {
    match value? {
        Value::Number(number) => number.as_u64(),
        Value::String(string) => string.trim().parse::<u64>().ok(),
        _ => None,
    }
}

fn non_empty_string(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|value| value.as_secs())
        .unwrap_or_default()
}

fn truncate_body(body: &str) -> String {
    let body = body.trim();
    if body.is_empty() {
        return String::from("-");
    }
    body.chars().take(500).collect()
}
