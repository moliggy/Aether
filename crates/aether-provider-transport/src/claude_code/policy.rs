use super::super::snapshot::GatewayProviderTransportSnapshot;
use super::super::{
    body_rules_are_locally_supported, header_rules_are_locally_supported,
    resolve_transport_tls_profile, supports_local_oauth_request_auth_resolution,
    transport_proxy_is_locally_supported,
};
use super::auth::supports_local_claude_code_auth;

pub fn supports_local_claude_code_transport_with_network(
    transport: &GatewayProviderTransportSnapshot,
    api_format: &str,
) -> bool {
    if !transport.provider.is_active || !transport.endpoint.is_active || !transport.key.is_active {
        return false;
    }
    if !transport
        .provider
        .provider_type
        .trim()
        .eq_ignore_ascii_case("claude_code")
    {
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
    if !supports_local_claude_code_auth(transport) {
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

    true
}
