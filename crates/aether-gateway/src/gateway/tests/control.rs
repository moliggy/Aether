use super::*;

#[tokio::test]
async fn gateway_consults_control_api_for_ai_routes_and_propagates_decision_headers() {
    #[derive(Debug, Clone)]
    struct SeenControlRequest {
        auth_endpoint_signature: String,
        query_string: String,
        trace_id: String,
    }

    #[derive(Debug, Clone)]
    struct SeenPublicRequest {
        control_route_class: String,
        control_route_family: String,
        control_route_kind: String,
        control_executor_candidate: String,
        control_endpoint_signature: String,
        trusted_user_id: String,
        trusted_api_key_id: String,
        trusted_balance_remaining: String,
        trusted_access_allowed: String,
        trace_id: String,
    }

    let seen_control = Arc::new(Mutex::new(None::<SeenControlRequest>));
    let seen_control_clone = Arc::clone(&seen_control);
    let seen_public = Arc::new(Mutex::new(None::<SeenPublicRequest>));
    let seen_public_clone = Arc::clone(&seen_public);

    let upstream = Router::new()
        .route(
            "/api/internal/gateway/auth-context",
            any(move |request: Request| {
                let seen_control_inner = Arc::clone(&seen_control_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    let payload: serde_json::Value =
                        serde_json::from_slice(&raw_body).expect("control payload should parse");
                    *seen_control_inner.lock().expect("mutex should lock") =
                        Some(SeenControlRequest {
                            auth_endpoint_signature: payload
                                .get("auth_endpoint_signature")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            query_string: payload
                                .get("query_string")
                                .and_then(|value| value.as_str())
                                .unwrap_or_default()
                                .to_string(),
                            trace_id: parts
                                .headers
                                .get(TRACE_ID_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    Json(json!({
                        "auth_context": {
                            "user_id": "user-123",
                            "api_key_id": "key-123",
                            "balance_remaining": 42.5,
                            "access_allowed": true
                        }
                    }))
                }
            }),
        )
        .route(
            "/v1/chat/completions",
            any(move |request: Request| {
                let seen_public_inner = Arc::clone(&seen_public_clone);
                async move {
                    *seen_public_inner.lock().expect("mutex should lock") =
                        Some(SeenPublicRequest {
                            control_route_class: request
                                .headers()
                                .get(CONTROL_ROUTE_CLASS_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            control_route_family: request
                                .headers()
                                .get(CONTROL_ROUTE_FAMILY_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            control_route_kind: request
                                .headers()
                                .get(CONTROL_ROUTE_KIND_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            control_executor_candidate: request
                                .headers()
                                .get(CONTROL_EXECUTOR_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            control_endpoint_signature: request
                                .headers()
                                .get(CONTROL_ENDPOINT_SIGNATURE_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            trusted_user_id: request
                                .headers()
                                .get(TRUSTED_AUTH_USER_ID_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            trusted_api_key_id: request
                                .headers()
                                .get(TRUSTED_AUTH_API_KEY_ID_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            trusted_balance_remaining: request
                                .headers()
                                .get(TRUSTED_AUTH_BALANCE_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            trusted_access_allowed: request
                                .headers()
                                .get(TRUSTED_AUTH_ACCESS_ALLOWED_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                            trace_id: request
                                .headers()
                                .get(TRACE_ID_HEADER)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_string(),
                        });
                    (
                        StatusCode::OK,
                        [(GATEWAY_HEADER, "python-upstream")],
                        Body::from("proxied"),
                    )
                }
            }),
        );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router_with_control(upstream_url.clone(), Some(upstream_url))
        .expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .post(format!("{gateway_url}/v1/chat/completions?stream=true"))
        .header(TRACE_ID_HEADER, "trace-control-123")
        .body("{\"hello\":\"world\"}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(CONTROL_ROUTE_CLASS_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("ai_public")
    );
    assert_eq!(
        response
            .headers()
            .get(CONTROL_EXECUTOR_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("true")
    );

    let seen_control_request = seen_control
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("control request should be captured");
    assert_eq!(seen_control_request.auth_endpoint_signature, "openai:chat");
    assert_eq!(seen_control_request.query_string, "stream=true");
    assert_eq!(seen_control_request.trace_id, "trace-control-123");

    let seen_public_request = seen_public
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("public request should be captured");
    assert_eq!(seen_public_request.control_route_class, "ai_public");
    assert_eq!(seen_public_request.control_route_family, "openai");
    assert_eq!(seen_public_request.control_route_kind, "chat");
    assert_eq!(seen_public_request.control_executor_candidate, "true");
    assert_eq!(
        seen_public_request.control_endpoint_signature,
        "openai:chat"
    );
    assert_eq!(seen_public_request.trusted_user_id, "user-123");
    assert_eq!(seen_public_request.trusted_api_key_id, "key-123");
    assert_eq!(seen_public_request.trusted_balance_remaining, "42.5");
    assert_eq!(seen_public_request.trusted_access_allowed, "true");
    assert_eq!(seen_public_request.trace_id, "trace-control-123");

    gateway_handle.abort();
    upstream_handle.abort();
}
