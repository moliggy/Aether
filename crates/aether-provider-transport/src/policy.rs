use super::provider_types::{
    provider_type_supports_local_openai_chat_transport,
    provider_type_supports_local_same_format_transport,
};
use super::snapshot::GatewayProviderTransportSnapshot;
use super::{
    body_rules_are_locally_supported, header_rules_are_locally_supported,
    resolve_transport_tls_profile, supports_local_oauth_request_auth_resolution,
    transport_proxy_is_locally_supported,
};

pub fn supports_local_openai_chat_transport(transport: &GatewayProviderTransportSnapshot) -> bool {
    if !transport.provider.is_active || !transport.endpoint.is_active || !transport.key.is_active {
        return false;
    }
    if !transport
        .endpoint
        .api_format
        .trim()
        .eq_ignore_ascii_case("openai:chat")
    {
        return false;
    }
    if !header_rules_are_locally_supported(transport.endpoint.header_rules.as_ref())
        || !body_rules_are_locally_supported(transport.endpoint.body_rules.as_ref())
    {
        return false;
    }
    if transport.key.decrypted_auth_config.is_some()
        && !supports_local_oauth_request_auth_resolution(transport)
    {
        return false;
    }
    if !transport_proxy_is_locally_supported(transport) {
        return false;
    }
    if transport.key.fingerprint.is_some() && resolve_transport_tls_profile(transport).is_none() {
        return false;
    }

    if !provider_type_supports_local_openai_chat_transport(&transport.provider.provider_type) {
        return false;
    }

    true
}

pub fn supports_local_standard_transport(
    transport: &GatewayProviderTransportSnapshot,
    api_format: &str,
) -> bool {
    supports_local_same_format_transport(transport, api_format, false)
}

pub fn supports_local_gemini_transport(
    transport: &GatewayProviderTransportSnapshot,
    api_format: &str,
) -> bool {
    supports_local_same_format_transport(transport, api_format, false)
}

pub fn supports_local_standard_transport_with_network(
    transport: &GatewayProviderTransportSnapshot,
    api_format: &str,
) -> bool {
    supports_local_same_format_transport(transport, api_format, true)
}

pub fn supports_local_gemini_transport_with_network(
    transport: &GatewayProviderTransportSnapshot,
    api_format: &str,
) -> bool {
    supports_local_same_format_transport(transport, api_format, true)
}

fn supports_local_same_format_transport(
    transport: &GatewayProviderTransportSnapshot,
    api_format: &str,
    allow_network_passthrough: bool,
) -> bool {
    if !transport.provider.is_active || !transport.endpoint.is_active || !transport.key.is_active {
        return false;
    }
    if !transport
        .endpoint
        .api_format
        .trim()
        .eq_ignore_ascii_case(api_format.trim())
    {
        return false;
    }
    if !header_rules_are_locally_supported(transport.endpoint.header_rules.as_ref())
        || !body_rules_are_locally_supported(transport.endpoint.body_rules.as_ref())
    {
        return false;
    }
    if transport.key.decrypted_auth_config.is_some()
        && !supports_local_oauth_request_auth_resolution(transport)
    {
        return false;
    }
    let has_custom_path = transport
        .endpoint
        .custom_path
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty());
    if has_custom_path && !allow_network_passthrough {
        return false;
    }
    if allow_network_passthrough {
        if !transport_proxy_is_locally_supported(transport) {
            return false;
        }
        if transport.key.fingerprint.is_some() && resolve_transport_tls_profile(transport).is_none()
        {
            return false;
        }
    } else if transport.provider.proxy.is_some()
        || transport.endpoint.proxy.is_some()
        || transport.key.proxy.is_some()
        || transport
            .key
            .fingerprint
            .as_ref()
            .and_then(|value| value.get("tls_profile"))
            .and_then(|value| value.as_str())
            .is_some_and(|value| !value.trim().is_empty())
    {
        return false;
    }

    if !provider_type_supports_local_same_format_transport(&transport.provider.provider_type) {
        return false;
    }

    true
}
