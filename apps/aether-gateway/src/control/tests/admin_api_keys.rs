use http::Uri;

use crate::control::GatewayPublicRequestContext;
use crate::handlers::shared::local_proxy_route_requires_buffered_body;

use super::{classify_control_route, headers};

#[test]
fn classifies_admin_api_keys_list_as_admin_proxy_route() {
    let headers = headers(&[]);
    let uri: Uri = "/api/admin/api-keys?limit=100"
        .parse()
        .expect("uri should parse");
    let decision =
        classify_control_route(&http::Method::GET, &uri, &headers).expect("route should classify");

    assert_eq!(decision.route_class.as_deref(), Some("admin_proxy"));
    assert_eq!(decision.route_family.as_deref(), Some("api_keys_manage"));
    assert_eq!(decision.route_kind.as_deref(), Some("list_api_keys"));
    assert_eq!(
        decision.auth_endpoint_signature.as_deref(),
        Some("admin:api_keys")
    );
    assert!(!decision.is_execution_runtime_candidate());
}

#[test]
fn classifies_admin_api_keys_create_as_admin_proxy_route() {
    let headers = headers(&[]);
    let uri: Uri = "/api/admin/api-keys".parse().expect("uri should parse");
    let decision =
        classify_control_route(&http::Method::POST, &uri, &headers).expect("route should classify");

    assert_eq!(decision.route_class.as_deref(), Some("admin_proxy"));
    assert_eq!(decision.route_family.as_deref(), Some("api_keys_manage"));
    assert_eq!(decision.route_kind.as_deref(), Some("create_api_key"));
    assert_eq!(
        decision.auth_endpoint_signature.as_deref(),
        Some("admin:api_keys")
    );
    assert!(!decision.is_execution_runtime_candidate());
}

#[test]
fn classifies_admin_api_key_install_session_create_as_admin_proxy_route() {
    let headers = headers(&[]);
    for path in [
        "/api/admin/api-keys/key-123/install-sessions",
        "/api/admin/api-keys/key-123/install-sessions/",
    ] {
        let uri: Uri = path.parse().expect("uri should parse");
        let decision = classify_control_route(&http::Method::POST, &uri, &headers)
            .expect("route should classify");

        assert_eq!(decision.route_class.as_deref(), Some("admin_proxy"));
        assert_eq!(decision.route_family.as_deref(), Some("api_keys_manage"));
        assert_eq!(
            decision.route_kind.as_deref(),
            Some("create_api_key_install_session")
        );
        assert_eq!(
            decision.auth_endpoint_signature.as_deref(),
            Some("admin:api_keys")
        );
        assert!(!decision.is_execution_runtime_candidate());
    }
}

#[test]
fn admin_api_key_install_session_create_buffers_request_body() {
    let headers = headers(&[]);
    let uri: Uri = "/api/admin/api-keys/key-123/install-sessions"
        .parse()
        .expect("uri should parse");
    let decision =
        classify_control_route(&http::Method::POST, &uri, &headers).expect("route should classify");
    let context = GatewayPublicRequestContext::from_request_parts(
        "trace-admin-install-session",
        &http::Method::POST,
        &uri,
        &headers,
        Some(decision),
    );

    assert!(local_proxy_route_requires_buffered_body(&context));
}

#[test]
fn classifies_admin_api_keys_detail_as_admin_proxy_route() {
    let headers = headers(&[]);
    let uri: Uri = "/api/admin/api-keys/key-123"
        .parse()
        .expect("uri should parse");
    let decision =
        classify_control_route(&http::Method::GET, &uri, &headers).expect("route should classify");

    assert_eq!(decision.route_class.as_deref(), Some("admin_proxy"));
    assert_eq!(decision.route_family.as_deref(), Some("api_keys_manage"));
    assert_eq!(decision.route_kind.as_deref(), Some("api_key_detail"));
    assert_eq!(
        decision.auth_endpoint_signature.as_deref(),
        Some("admin:api_keys")
    );
    assert!(!decision.is_execution_runtime_candidate());
}

#[test]
fn classifies_admin_api_keys_update_as_admin_proxy_route() {
    let headers = headers(&[]);
    let uri: Uri = "/api/admin/api-keys/key-123"
        .parse()
        .expect("uri should parse");
    let decision =
        classify_control_route(&http::Method::PUT, &uri, &headers).expect("route should classify");

    assert_eq!(decision.route_class.as_deref(), Some("admin_proxy"));
    assert_eq!(decision.route_family.as_deref(), Some("api_keys_manage"));
    assert_eq!(decision.route_kind.as_deref(), Some("update_api_key"));
    assert_eq!(
        decision.auth_endpoint_signature.as_deref(),
        Some("admin:api_keys")
    );
    assert!(!decision.is_execution_runtime_candidate());
}

#[test]
fn classifies_admin_api_keys_toggle_as_admin_proxy_route() {
    let headers = headers(&[]);
    let uri: Uri = "/api/admin/api-keys/key-123"
        .parse()
        .expect("uri should parse");
    let decision = classify_control_route(&http::Method::PATCH, &uri, &headers)
        .expect("route should classify");

    assert_eq!(decision.route_class.as_deref(), Some("admin_proxy"));
    assert_eq!(decision.route_family.as_deref(), Some("api_keys_manage"));
    assert_eq!(decision.route_kind.as_deref(), Some("toggle_api_key"));
    assert_eq!(
        decision.auth_endpoint_signature.as_deref(),
        Some("admin:api_keys")
    );
    assert!(!decision.is_execution_runtime_candidate());
}

#[test]
fn classifies_admin_api_keys_delete_as_admin_proxy_route() {
    let headers = headers(&[]);
    let uri: Uri = "/api/admin/api-keys/key-123"
        .parse()
        .expect("uri should parse");
    let decision = classify_control_route(&http::Method::DELETE, &uri, &headers)
        .expect("route should classify");

    assert_eq!(decision.route_class.as_deref(), Some("admin_proxy"));
    assert_eq!(decision.route_family.as_deref(), Some("api_keys_manage"));
    assert_eq!(decision.route_kind.as_deref(), Some("delete_api_key"));
    assert_eq!(
        decision.auth_endpoint_signature.as_deref(),
        Some("admin:api_keys")
    );
    assert!(!decision.is_execution_runtime_candidate());
}
