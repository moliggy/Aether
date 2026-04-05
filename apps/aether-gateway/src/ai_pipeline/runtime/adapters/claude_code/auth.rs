use crate::provider_transport::auth::resolve_local_standard_auth;
use crate::provider_transport::snapshot::GatewayProviderTransportSnapshot;
use crate::provider_transport::supports_local_oauth_request_auth_resolution;

pub(crate) fn supports_local_claude_code_auth(
    transport: &GatewayProviderTransportSnapshot,
) -> bool {
    resolve_local_standard_auth(transport).is_some()
        || supports_local_oauth_request_auth_resolution(transport)
}
