use super::super::shared::{
    build_provider_quota_execution_plan, default_provider_quota_execution_timeouts,
    execute_provider_quota_plan, ProviderQuotaExecutionOutcome,
};
use crate::handlers::admin::request::{
    AdminAppState, AdminGatewayProviderTransportSnapshot, AdminKiroRequestAuth,
};
use crate::GatewayError;
use aether_contracts::ProxySnapshot;
use aether_provider_pool::{build_kiro_pool_quota_request, KiroPoolQuotaAuthInput};

pub(super) async fn execute_kiro_quota_plan(
    state: &AdminAppState<'_>,
    transport: &AdminGatewayProviderTransportSnapshot,
    auth: &AdminKiroRequestAuth,
    proxy_override: Option<&ProxySnapshot>,
) -> Result<ProviderQuotaExecutionOutcome, GatewayError> {
    let proxy = match proxy_override {
        Some(proxy) => Some(proxy.clone()),
        None => {
            state
                .resolve_transport_proxy_snapshot_with_tunnel_affinity(transport)
                .await
        }
    };
    let timeouts = state
        .resolve_transport_execution_timeouts(transport)
        .or(Some(default_provider_quota_execution_timeouts(
            proxy.as_ref(),
        )));
    let spec = build_kiro_pool_quota_request(
        &transport.key.id,
        &KiroPoolQuotaAuthInput {
            authorization_value: auth.value.clone(),
            api_region: auth.auth_config.effective_api_region().to_string(),
            kiro_version: auth.auth_config.effective_kiro_version().to_string(),
            machine_id: auth.machine_id.clone(),
            profile_arn: auth
                .auth_config
                .profile_arn_for_payload()
                .map(str::to_string),
        },
    );
    let plan = build_provider_quota_execution_plan(
        transport,
        spec,
        proxy,
        state.resolve_transport_profile(transport),
        timeouts,
    );

    execute_provider_quota_plan(state, transport, plan, "kiro").await
}
