use axum::body::Body;
use axum::http::{self, Response};
use tracing::{info, warn};

use crate::control::GatewayControlDecision;

#[derive(Debug, Clone)]
pub(crate) struct AdminAuditEvent {
    pub(crate) event_name: &'static str,
    pub(crate) action: &'static str,
    pub(crate) target_type: &'static str,
    pub(crate) target_id: String,
}

pub(crate) fn attach_admin_audit_event(
    response: &mut Response<Body>,
    event_name: &'static str,
    action: &'static str,
    target_type: &'static str,
    target_id: impl Into<String>,
) {
    response.extensions_mut().insert(AdminAuditEvent {
        event_name,
        action,
        target_type,
        target_id: target_id.into(),
    });
}

pub(crate) fn emit_admin_audit(
    response: &mut Response<Body>,
    trace_id: &str,
    method: &http::Method,
    path_and_query: &str,
    control_decision: Option<&GatewayControlDecision>,
) {
    let Some(decision) = control_decision else {
        return;
    };
    let Some(admin_principal) = decision.admin_principal.as_ref() else {
        return;
    };

    let attached_event = response.extensions_mut().remove::<AdminAuditEvent>();
    let route_family = decision.route_family.as_deref().unwrap_or("unknown");
    let route_kind = decision.route_kind.as_deref().unwrap_or("unknown");
    let status_code = response.status().as_u16();
    if attached_event.is_none() && !is_admin_mutation_method(method) {
        return;
    }
    let (event_name, action, target_type, target_id) = if let Some(event) = attached_event {
        (
            event.event_name,
            event.action,
            event.target_type,
            event.target_id,
        )
    } else {
        (
            if response.status().is_success() {
                "admin_mutation_completed"
            } else {
                "admin_mutation_failed"
            },
            route_kind,
            default_target_type(route_family),
            path_and_query.to_string(),
        )
    };

    let audit_status = if response.status().is_success() {
        "completed"
    } else {
        "failed"
    };
    if response.status().is_success() {
        info!(
            event_name,
            log_type = "audit",
            status = audit_status,
            status_code,
            trace_id = %trace_id,
            admin_user_id = admin_principal.user_id.as_str(),
            admin_user_role = admin_principal.user_role.as_str(),
            admin_session_id = admin_principal.session_id.as_deref().unwrap_or("-"),
            admin_management_token_id = admin_principal.management_token_id.as_deref().unwrap_or("-"),
            route_family,
            route_kind,
            method = %method,
            path = %path_and_query,
            action,
            target_type,
            target_id = %target_id,
            "admin mutation audit event"
        );
    } else {
        warn!(
            event_name,
            log_type = "audit",
            status = audit_status,
            status_code,
            trace_id = %trace_id,
            admin_user_id = admin_principal.user_id.as_str(),
            admin_user_role = admin_principal.user_role.as_str(),
            admin_session_id = admin_principal.session_id.as_deref().unwrap_or("-"),
            admin_management_token_id = admin_principal.management_token_id.as_deref().unwrap_or("-"),
            route_family,
            route_kind,
            method = %method,
            path = %path_and_query,
            action,
            target_type,
            target_id = %target_id,
            "admin mutation audit event"
        );
    }
}

fn default_target_type(route_family: &str) -> &str {
    route_family
        .strip_suffix("_manage")
        .unwrap_or(route_family)
        .trim()
}

fn is_admin_mutation_method(method: &http::Method) -> bool {
    matches!(
        *method,
        http::Method::POST | http::Method::PUT | http::Method::PATCH | http::Method::DELETE
    )
}
