use std::collections::BTreeMap;

use aether_contracts::{ExecutionPlan, ProxySnapshot, RequestBody};
use aether_provider_transport::auth::{
    resolve_local_gemini_auth, resolve_local_openai_chat_auth, resolve_local_standard_auth,
};
use aether_provider_transport::url::build_passthrough_path_url;
use aether_provider_transport::vertex::resolve_local_vertex_api_key_query_auth;
use aether_provider_transport::{
    apply_local_header_rules, ensure_upstream_auth_header, resolve_transport_execution_timeouts,
    resolve_transport_tls_profile, GatewayProviderTransportSnapshot, LocalResolvedOAuthRequestAuth,
};
use async_trait::async_trait;
use serde_json::json;

use crate::build_models_fetch_url;

#[async_trait]
pub trait ModelFetchTransportRuntime: Send + Sync {
    async fn resolve_local_oauth_request_auth(
        &self,
        transport: &GatewayProviderTransportSnapshot,
    ) -> Result<Option<LocalResolvedOAuthRequestAuth>, String>;

    async fn resolve_model_fetch_proxy(
        &self,
        transport: &GatewayProviderTransportSnapshot,
    ) -> Option<ProxySnapshot>;
}

pub async fn build_models_fetch_execution_plan(
    runtime: &(impl ModelFetchTransportRuntime + ?Sized),
    transport: &GatewayProviderTransportSnapshot,
) -> Result<ExecutionPlan, String> {
    let (upstream_url, provider_api_format) = build_models_fetch_url(
        &transport.provider.provider_type,
        &transport.endpoint.api_format,
        &transport.endpoint.base_url,
    )
    .ok_or_else(|| "Rust models fetch does not support this provider format yet".to_string())?;
    let (auth_header_name, auth_header_value) = resolve_models_fetch_auth(runtime, transport)
        .await?
        .ok_or_else(|| {
            "Rust models fetch auth resolution is not supported for this key".to_string()
        })?;

    let mut headers = BTreeMap::from([(auth_header_name.clone(), auth_header_value.clone())]);
    if !apply_local_header_rules(
        &mut headers,
        transport.endpoint.header_rules.as_ref(),
        &[auth_header_name.as_str()],
        &json!({}),
        None,
    ) {
        return Err("Endpoint header_rules application failed".to_string());
    }
    ensure_upstream_auth_header(&mut headers, &auth_header_name, &auth_header_value);

    Ok(ExecutionPlan {
        request_id: format!("req-model-fetch-{}", transport.key.id),
        candidate_id: None,
        provider_name: Some(transport.provider.name.clone()),
        provider_id: transport.provider.id.clone(),
        endpoint_id: transport.endpoint.id.clone(),
        key_id: transport.key.id.clone(),
        method: "GET".to_string(),
        url: upstream_url,
        headers,
        content_type: None,
        content_encoding: None,
        body: RequestBody {
            json_body: None,
            body_bytes_b64: None,
            body_ref: None,
        },
        stream: false,
        client_api_format: provider_api_format.clone(),
        provider_api_format,
        model_name: None,
        proxy: runtime.resolve_model_fetch_proxy(transport).await,
        tls_profile: resolve_transport_tls_profile(transport),
        timeouts: resolve_transport_execution_timeouts(transport),
    })
}

async fn resolve_models_fetch_auth(
    runtime: &(impl ModelFetchTransportRuntime + ?Sized),
    transport: &GatewayProviderTransportSnapshot,
) -> Result<Option<(String, String)>, String> {
    if transport.key.auth_type.trim().eq_ignore_ascii_case("oauth")
        || transport.key.auth_type.trim().eq_ignore_ascii_case("kiro")
    {
        return match runtime.resolve_local_oauth_request_auth(transport).await {
            Ok(Some(LocalResolvedOAuthRequestAuth::Header { name, value })) => {
                Ok(Some((name, value)))
            }
            Ok(Some(LocalResolvedOAuthRequestAuth::Kiro(_))) => Ok(None),
            Ok(None) => Ok(None),
            Err(err) => Err(err),
        };
    }

    if let Some(auth) = resolve_local_openai_chat_auth(transport) {
        return Ok(Some(auth));
    }
    if let Some(auth) = resolve_local_standard_auth(transport) {
        return Ok(Some(auth));
    }
    if let Some(auth) = resolve_local_gemini_auth(transport) {
        return Ok(Some(auth));
    }
    if let Some(query_auth) = resolve_local_vertex_api_key_query_auth(transport) {
        let url = build_passthrough_path_url(
            &transport.endpoint.base_url,
            "/v1/publishers/google/models",
            Some(&format!("{}={}", query_auth.name, query_auth.value)),
            &[],
        );
        if url.is_some() {
            return Ok(None);
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use aether_contracts::ProxySnapshot;
    use aether_provider_transport::snapshot::{
        GatewayProviderTransportEndpoint, GatewayProviderTransportKey,
        GatewayProviderTransportProvider, GatewayProviderTransportSnapshot,
    };
    use async_trait::async_trait;

    use super::{build_models_fetch_execution_plan, ModelFetchTransportRuntime};

    struct TestRuntime {
        oauth_auth: Option<aether_provider_transport::LocalResolvedOAuthRequestAuth>,
        proxy: Option<ProxySnapshot>,
    }

    #[async_trait]
    impl ModelFetchTransportRuntime for TestRuntime {
        async fn resolve_local_oauth_request_auth(
            &self,
            _transport: &GatewayProviderTransportSnapshot,
        ) -> Result<Option<aether_provider_transport::LocalResolvedOAuthRequestAuth>, String>
        {
            Ok(self.oauth_auth.clone())
        }

        async fn resolve_model_fetch_proxy(
            &self,
            _transport: &GatewayProviderTransportSnapshot,
        ) -> Option<ProxySnapshot> {
            self.proxy.clone()
        }
    }

    fn sample_transport(api_format: &str, auth_type: &str) -> GatewayProviderTransportSnapshot {
        GatewayProviderTransportSnapshot {
            provider: GatewayProviderTransportProvider {
                id: "provider-1".to_string(),
                name: "Provider One".to_string(),
                provider_type: "openai".to_string(),
                website: None,
                is_active: true,
                keep_priority_on_conversion: false,
                enable_format_conversion: false,
                concurrent_limit: None,
                max_retries: None,
                proxy: None,
                request_timeout_secs: Some(30.0),
                stream_first_byte_timeout_secs: Some(5.0),
                config: None,
            },
            endpoint: GatewayProviderTransportEndpoint {
                id: "endpoint-1".to_string(),
                provider_id: "provider-1".to_string(),
                api_format: api_format.to_string(),
                api_family: None,
                endpoint_kind: None,
                is_active: true,
                base_url: "https://example.com".to_string(),
                header_rules: None,
                body_rules: None,
                max_retries: None,
                custom_path: None,
                config: None,
                format_acceptance_config: None,
                proxy: None,
            },
            key: GatewayProviderTransportKey {
                id: "key-1".to_string(),
                provider_id: "provider-1".to_string(),
                name: "key".to_string(),
                auth_type: auth_type.to_string(),
                is_active: true,
                api_formats: None,
                allowed_models: None,
                capabilities: None,
                rate_multipliers: None,
                global_priority_by_format: None,
                expires_at_unix_secs: None,
                proxy: None,
                fingerprint: None,
                decrypted_api_key: "secret".to_string(),
                decrypted_auth_config: None,
            },
        }
    }

    #[tokio::test]
    async fn builds_openai_models_fetch_plan_from_transport_snapshot() {
        let runtime = TestRuntime {
            oauth_auth: None,
            proxy: None,
        };
        let plan = build_models_fetch_execution_plan(
            &runtime,
            &sample_transport("openai:chat", "api_key"),
        )
        .await
        .expect("plan");

        assert_eq!(plan.method, "GET");
        assert_eq!(plan.url, "https://example.com/v1/models");
        assert_eq!(
            plan.headers.get("authorization").map(String::as_str),
            Some("Bearer secret")
        );
        assert_eq!(plan.provider_api_format, "openai:chat");
    }

    #[tokio::test]
    async fn builds_oauth_models_fetch_plan_from_runtime_auth() {
        let runtime = TestRuntime {
            oauth_auth: Some(
                aether_provider_transport::LocalResolvedOAuthRequestAuth::Header {
                    name: "authorization".to_string(),
                    value: "Bearer oauth-token".to_string(),
                },
            ),
            proxy: Some(ProxySnapshot {
                enabled: Some(true),
                mode: Some("fixed".to_string()),
                node_id: None,
                label: None,
                url: Some("http://proxy.internal".to_string()),
                extra: None,
            }),
        };
        let plan =
            build_models_fetch_execution_plan(&runtime, &sample_transport("openai:chat", "oauth"))
                .await
                .expect("plan");

        assert_eq!(
            plan.headers.get("authorization").map(String::as_str),
            Some("Bearer oauth-token")
        );
        assert_eq!(
            plan.proxy.as_ref().and_then(|proxy| proxy.url.as_deref()),
            Some("http://proxy.internal")
        );
    }
}
