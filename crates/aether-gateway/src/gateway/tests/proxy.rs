use super::*;

#[tokio::test]
async fn gateway_proxies_method_path_body_and_generates_trace_id() {
    #[derive(Debug, Clone)]
    struct SeenRequest {
        method: String,
        path: String,
        trace_id: String,
        execution_path: String,
        host: String,
        forwarded_for: String,
        body: String,
    }

    let seen = Arc::new(Mutex::new(None::<SeenRequest>));
    let seen_clone = Arc::clone(&seen);
    let upstream = Router::new()
        .route("/", any(|| async { StatusCode::OK }))
        .route(
            "/{*path}",
            any(move |request: Request| {
                let seen_inner = Arc::clone(&seen_clone);
                async move {
                    let (parts, body) = request.into_parts();
                    let raw_body = to_bytes(body, usize::MAX).await.expect("body should read");
                    *seen_inner.lock().expect("mutex should lock") = Some(SeenRequest {
                        method: parts.method.to_string(),
                        path: parts
                            .uri
                            .path_and_query()
                            .map(|value| value.as_str())
                            .unwrap_or("/")
                            .to_string(),
                        trace_id: parts
                            .headers
                            .get(TRACE_ID_HEADER)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_string(),
                        execution_path: parts
                            .headers
                            .get(EXECUTION_PATH_HEADER)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_string(),
                        host: parts
                            .headers
                            .get(http::header::HOST)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_string(),
                        forwarded_for: parts
                            .headers
                            .get(FORWARDED_FOR_HEADER)
                            .and_then(|value| value.to_str().ok())
                            .unwrap_or_default()
                            .to_string(),
                        body: String::from_utf8(raw_body.to_vec()).expect("utf-8 body"),
                    });
                    (
                        StatusCode::CREATED,
                        [(GATEWAY_HEADER, "python-upstream")],
                        Body::from("proxied"),
                    )
                }
            }),
        );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router(upstream_url).expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{gateway_url}/v1/chat/completions?stream=true"))
        .header(http::header::HOST, "api.example.com")
        .body("{\"hello\":\"world\"}")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(
        response
            .headers()
            .get(GATEWAY_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("python-upstream")
    );
    assert_eq!(
        response
            .headers()
            .get(EXECUTION_PATH_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some(EXECUTION_PATH_PUBLIC_PROXY_PASSTHROUGH)
    );

    let response_trace_id = response
        .headers()
        .get(TRACE_ID_HEADER)
        .and_then(|value| value.to_str().ok())
        .expect("response trace id should exist")
        .to_string();
    assert_eq!(response.text().await.expect("body should read"), "proxied");

    let seen_request = seen
        .lock()
        .expect("mutex should lock")
        .clone()
        .expect("upstream request should be captured");
    assert_eq!(seen_request.method, "POST");
    assert_eq!(seen_request.path, "/v1/chat/completions?stream=true");
    assert_eq!(seen_request.body, "{\"hello\":\"world\"}");
    assert_eq!(
        seen_request.execution_path,
        EXECUTION_PATH_PUBLIC_PROXY_PASSTHROUGH
    );
    assert_eq!(seen_request.host, "api.example.com");
    assert_eq!(seen_request.forwarded_for, "127.0.0.1");
    assert_eq!(seen_request.trace_id, response_trace_id);
    assert!(!seen_request.trace_id.is_empty());

    gateway_handle.abort();
    upstream_handle.abort();
}

#[tokio::test]
async fn gateway_preserves_existing_trace_id_and_streams_response() {
    let upstream = Router::new().route(
        "/{*path}",
        any(|request: Request| async move {
            let incoming_trace_id = request
                .headers()
                .get(TRACE_ID_HEADER)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string();
            let stream = futures_util::stream::iter([
                Ok::<_, Infallible>(Bytes::from_static(b"chunk-1")),
                Ok::<_, Infallible>(Bytes::from_static(b"chunk-2")),
            ]);
            let mut response = Response::builder()
                .status(StatusCode::OK)
                .body(Body::from_stream(stream))
                .expect("response should build");
            response.headers_mut().insert(
                HeaderName::from_static(TRACE_ID_HEADER),
                HeaderValue::from_str(&incoming_trace_id).expect("trace id header"),
            );
            response
        }),
    );

    let (upstream_url, upstream_handle) = start_server(upstream).await;
    let gateway = build_router(upstream_url).expect("gateway should build");
    let (gateway_url, gateway_handle) = start_server(gateway).await;

    let response = reqwest::Client::new()
        .get(format!("{gateway_url}/v1/messages"))
        .header(TRACE_ID_HEADER, "trace-123")
        .send()
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get(TRACE_ID_HEADER)
            .and_then(|value| value.to_str().ok()),
        Some("trace-123")
    );
    assert_eq!(
        response.bytes().await.expect("bytes should read"),
        Bytes::from_static(b"chunk-1chunk-2")
    );

    gateway_handle.abort();
    upstream_handle.abort();
}
