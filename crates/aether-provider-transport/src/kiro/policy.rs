use super::super::snapshot::GatewayProviderTransportSnapshot;
use super::super::{resolve_transport_tls_profile, transport_proxy_is_locally_supported};
use super::{supports_local_kiro_request_auth_resolution, supports_local_kiro_request_shape};

pub fn supports_local_kiro_request_transport(transport: &GatewayProviderTransportSnapshot) -> bool {
    if !transport.provider.is_active || !transport.endpoint.is_active || !transport.key.is_active {
        return false;
    }
    if !transport
        .endpoint
        .api_format
        .trim()
        .eq_ignore_ascii_case("claude:cli")
    {
        return false;
    }
    if !supports_local_kiro_request_auth_resolution(transport) {
        return false;
    }
    if !supports_local_kiro_request_shape(
        transport.endpoint.header_rules.as_ref(),
        transport.endpoint.body_rules.as_ref(),
    ) {
        return false;
    }

    true
}

pub fn supports_local_kiro_request_transport_with_network(
    transport: &GatewayProviderTransportSnapshot,
) -> bool {
    supports_local_kiro_request_transport(transport)
        && transport_proxy_is_locally_supported(transport)
        && (transport.key.fingerprint.is_none()
            || resolve_transport_tls_profile(transport).is_some())
}

#[cfg(test)]
mod tests {
    use super::super::super::snapshot::{
        GatewayProviderTransportEndpoint, GatewayProviderTransportKey,
        GatewayProviderTransportProvider, GatewayProviderTransportSnapshot,
    };
    use super::{
        supports_local_kiro_request_transport, supports_local_kiro_request_transport_with_network,
    };

    fn sample_transport() -> GatewayProviderTransportSnapshot {
        GatewayProviderTransportSnapshot {
            provider: GatewayProviderTransportProvider {
                id: "provider-1".to_string(),
                name: "Kiro".to_string(),
                provider_type: "kiro".to_string(),
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
                api_format: "claude:cli".to_string(),
                api_family: Some("claude".to_string()),
                endpoint_kind: Some("cli".to_string()),
                is_active: true,
                base_url: "https://kiro.example".to_string(),
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
                auth_type: "bearer".to_string(),
                is_active: true,
                api_formats: Some(vec!["claude:cli".to_string()]),
                allowed_models: None,
                capabilities: None,
                rate_multipliers: None,
                global_priority_by_format: None,
                expires_at_unix_secs: None,
                proxy: None,
                fingerprint: None,
                decrypted_api_key: "__placeholder__".to_string(),
                decrypted_auth_config: Some(
                    r#"{
                        "access_token":"cached-token",
                        "refresh_token":"rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr",
                        "machine_id":"123e4567-e89b-12d3-a456-426614174000"
                    }"#
                    .to_string(),
                ),
            },
        }
    }

    #[test]
    fn supports_kiro_request_transport_when_cached_access_token_exists() {
        assert!(supports_local_kiro_request_transport(&sample_transport()));
        assert!(supports_local_kiro_request_transport_with_network(
            &sample_transport()
        ));
    }

    #[test]
    fn supports_kiro_request_transport_when_refresh_only_auth_exists() {
        let mut transport = sample_transport();
        transport.key.decrypted_api_key = "__placeholder__".to_string();
        transport.key.decrypted_auth_config = Some(
            r#"{
                "refresh_token":"rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr"
            }"#
            .to_string(),
        );

        assert!(supports_local_kiro_request_transport(&transport));
        assert!(supports_local_kiro_request_transport_with_network(
            &transport
        ));
    }
}
