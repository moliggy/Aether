use crate::provider_transport::snapshot::GatewayProviderTransportSnapshot;

pub(crate) const VERTEX_API_KEY_QUERY_PARAM: &str = "key";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VertexApiKeyQueryAuth {
    pub(crate) name: &'static str,
    pub(crate) value: String,
}

pub(crate) fn resolve_local_vertex_api_key_query_auth(
    transport: &GatewayProviderTransportSnapshot,
) -> Option<VertexApiKeyQueryAuth> {
    if !transport
        .provider
        .provider_type
        .trim()
        .eq_ignore_ascii_case(super::PROVIDER_TYPE)
    {
        return None;
    }

    if transport.key.decrypted_auth_config.is_some() {
        return None;
    }

    if !transport
        .key
        .auth_type
        .trim()
        .eq_ignore_ascii_case("api_key")
    {
        return None;
    }

    let secret = transport.key.decrypted_api_key.trim();
    if secret.is_empty() {
        return None;
    }

    Some(VertexApiKeyQueryAuth {
        name: VERTEX_API_KEY_QUERY_PARAM,
        value: secret.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use crate::provider_transport::snapshot::{
        GatewayProviderTransportEndpoint, GatewayProviderTransportKey,
        GatewayProviderTransportProvider, GatewayProviderTransportSnapshot,
    };

    use super::{resolve_local_vertex_api_key_query_auth, VERTEX_API_KEY_QUERY_PARAM};

    fn sample_transport() -> GatewayProviderTransportSnapshot {
        GatewayProviderTransportSnapshot {
            provider: GatewayProviderTransportProvider {
                id: "provider-1".to_string(),
                name: "Vertex".to_string(),
                provider_type: "vertex_ai".to_string(),
                website: None,
                is_active: true,
                keep_priority_on_conversion: false,
                enable_format_conversion: false,
                concurrent_limit: None,
                max_retries: None,
                proxy: None,
                request_timeout_secs: None,
                stream_first_byte_timeout_secs: None,
                config: None,
            },
            endpoint: GatewayProviderTransportEndpoint {
                id: "endpoint-1".to_string(),
                provider_id: "provider-1".to_string(),
                api_format: "gemini:chat".to_string(),
                api_family: Some("gemini".to_string()),
                endpoint_kind: Some("chat".to_string()),
                is_active: true,
                base_url: "https://aiplatform.googleapis.com".to_string(),
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
                auth_type: "api_key".to_string(),
                is_active: true,
                api_formats: Some(vec!["gemini:chat".to_string()]),
                allowed_models: None,
                capabilities: None,
                rate_multipliers: None,
                global_priority_by_format: None,
                expires_at_unix_secs: None,
                proxy: None,
                fingerprint: None,
                decrypted_api_key: "vertex-secret".to_string(),
                decrypted_auth_config: None,
            },
        }
    }

    #[test]
    fn resolves_query_auth_for_vertex_api_key_subset() {
        let auth = resolve_local_vertex_api_key_query_auth(&sample_transport())
            .expect("vertex api key query auth should resolve");
        assert_eq!(auth.name, VERTEX_API_KEY_QUERY_PARAM);
        assert_eq!(auth.value, "vertex-secret");
    }

    #[test]
    fn rejects_non_api_key_transport() {
        let mut transport = sample_transport();
        transport.key.auth_type = "service_account".to_string();
        assert!(resolve_local_vertex_api_key_query_auth(&transport).is_none());
    }

    #[test]
    fn rejects_vertex_auth_config_transport() {
        let mut transport = sample_transport();
        transport.key.decrypted_auth_config = Some("{\"project_id\":\"demo-project\"}".to_string());
        assert!(resolve_local_vertex_api_key_query_auth(&transport).is_none());
    }
}
