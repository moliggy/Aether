use std::time::Instant;

use axum::body::Body;
use axum::extract::Request;
use axum::http::header::{HeaderName, HeaderValue};
use axum::middleware::Next;
use axum::response::Response;
use tracing::{info, warn};

use crate::constants::{
    CONTROL_REQUEST_ID_HEADER, CONTROL_ROUTE_CLASS_HEADER, EXECUTION_PATH_HEADER, TRACE_ID_HEADER,
};
use crate::headers::extract_or_generate_trace_id;

#[derive(Debug, Clone, Copy)]
pub(crate) struct RequestLogEmitted;

pub(crate) async fn access_log_middleware(request: Request<Body>, next: Next) -> Response {
    let started_at = Instant::now();
    let method = request.method().clone();
    let path = request
        .uri()
        .path_and_query()
        .map(|value| value.as_str().to_string())
        .unwrap_or_else(|| "/".to_string());
    let trace_id = extract_or_generate_trace_id(request.headers());
    info!(
        event_name = "http_request_started",
        log_type = "access",
        status = "started",
        trace_id = %trace_id,
        request_id = "-",
        method = %method,
        path = %path,
        route_class = "pending",
        execution_path = "pending",
        "gateway request started"
    );
    let mut response = next.run(request).await;
    if !response.headers().contains_key(TRACE_ID_HEADER) {
        response.headers_mut().insert(
            HeaderName::from_static(TRACE_ID_HEADER),
            HeaderValue::from_str(&trace_id).expect("trace id should be a valid header value"),
        );
    }
    if response.extensions().get::<RequestLogEmitted>().is_none() {
        let route_class = response
            .headers()
            .get(CONTROL_ROUTE_CLASS_HEADER)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("local");
        let execution_path = response
            .headers()
            .get(EXECUTION_PATH_HEADER)
            .and_then(|value| value.to_str().ok())
            .unwrap_or("local_route");
        let request_id = response
            .headers()
            .get(CONTROL_REQUEST_ID_HEADER)
            .and_then(|value| value.to_str().ok())
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("-");
        let status_code = response.status().as_u16();
        let elapsed_ms = started_at.elapsed().as_millis() as u64;
        if response.status().is_server_error() {
            warn!(
                event_name = "http_request_failed",
                log_type = "access",
                status = "failed",
                status_code,
                trace_id = %trace_id,
                request_id,
                method = %method,
                path = %path,
                route_class,
                execution_path,
                elapsed_ms,
                "gateway request failed"
            );
        } else {
            info!(
                event_name = "http_request_completed",
                log_type = "access",
                status = "completed",
                status_code,
                trace_id = %trace_id,
                request_id,
                method = %method,
                path = %path,
                route_class,
                execution_path,
                elapsed_ms,
                "gateway completed request"
            );
        }
    }
    response
}

#[cfg(test)]
mod tests {
    use super::access_log_middleware;
    use crate::constants::{
        CONTROL_REQUEST_ID_HEADER, CONTROL_ROUTE_CLASS_HEADER, EXECUTION_PATH_HEADER,
        TRACE_ID_HEADER,
    };
    use axum::body::Body;
    use axum::http::{Request, Response, StatusCode};
    use axum::routing::get;
    use axum::Router;
    use bytes::Bytes;
    use futures_util::stream;
    use std::sync::{Arc, Mutex};
    use tower::ServiceExt;
    use tracing_subscriber::prelude::*;

    #[derive(Clone, Default)]
    struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

    struct SharedBufferWriter(Arc<Mutex<Vec<u8>>>);

    impl SharedBuffer {
        fn lines(&self) -> Vec<serde_json::Value> {
            String::from_utf8(self.0.lock().expect("buffer should lock").clone())
                .expect("buffer should contain valid utf-8")
                .lines()
                .filter(|line| !line.trim().is_empty())
                .map(|line| serde_json::from_str(line).expect("json log line should parse"))
                .collect()
        }
    }

    impl std::io::Write for SharedBufferWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("buffer should lock")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::writer::MakeWriter<'a> for SharedBuffer {
        type Writer = SharedBufferWriter;

        fn make_writer(&'a self) -> Self::Writer {
            SharedBufferWriter(Arc::clone(&self.0))
        }
    }

    #[tokio::test]
    async fn access_log_emits_started_and_completed_events() {
        let writer = SharedBuffer::default();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .json()
                .flatten_event(true)
                .with_current_span(false)
                .with_span_list(false)
                .with_writer(writer.clone()),
        );
        let dispatch = tracing::Dispatch::new(subscriber);
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let app = Router::new()
            .route(
                "/ok",
                get(|| async {
                    let mut response = Response::new(Body::empty());
                    response.headers_mut().insert(
                        CONTROL_ROUTE_CLASS_HEADER,
                        "local".parse().expect("header should parse"),
                    );
                    response.headers_mut().insert(
                        EXECUTION_PATH_HEADER,
                        "local_route".parse().expect("header should parse"),
                    );
                    response.headers_mut().insert(
                        CONTROL_REQUEST_ID_HEADER,
                        "req-123".parse().expect("header should parse"),
                    );
                    response
                }),
            )
            .layer(axum::middleware::from_fn(access_log_middleware));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/ok")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert!(response.headers().contains_key(TRACE_ID_HEADER));

        let logs = writer.lines();
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0]["event_name"], "http_request_started");
        assert_eq!(logs[0]["status"], "started");
        assert_eq!(logs[1]["event_name"], "http_request_completed");
        assert_eq!(logs[1]["status"], "completed");
        assert_eq!(logs[1]["status_code"], 200);
        assert_eq!(logs[1]["request_id"], "req-123");
        assert_eq!(logs[1]["route_class"], "local");
        assert_eq!(logs[1]["execution_path"], "local_route");
    }

    #[tokio::test]
    async fn access_log_emits_started_and_failed_events_for_server_errors() {
        let writer = SharedBuffer::default();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .json()
                .flatten_event(true)
                .with_current_span(false)
                .with_span_list(false)
                .with_writer(writer.clone()),
        );
        let dispatch = tracing::Dispatch::new(subscriber);
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let app = Router::new()
            .route(
                "/fail",
                get(|| async {
                    Response::builder()
                        .status(StatusCode::BAD_GATEWAY)
                        .header(CONTROL_ROUTE_CLASS_HEADER, "passthrough")
                        .header(EXECUTION_PATH_HEADER, "execution_runtime_sync")
                        .body(Body::empty())
                        .expect("response should build")
                }),
            )
            .layer(axum::middleware::from_fn(access_log_middleware));

        let _response = app
            .oneshot(
                Request::builder()
                    .uri("/fail")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        let logs = writer.lines();
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0]["event_name"], "http_request_started");
        assert_eq!(logs[1]["event_name"], "http_request_failed");
        assert_eq!(logs[1]["status"], "failed");
        assert_eq!(logs[1]["status_code"], 502);
        assert_eq!(logs[1]["route_class"], "passthrough");
        assert_eq!(logs[1]["execution_path"], "execution_runtime_sync");
    }

    #[tokio::test]
    async fn access_log_treats_client_errors_as_completed_events() {
        let writer = SharedBuffer::default();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .json()
                .flatten_event(true)
                .with_current_span(false)
                .with_span_list(false)
                .with_writer(writer.clone()),
        );
        let dispatch = tracing::Dispatch::new(subscriber);
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let app = Router::new()
            .route(
                "/missing",
                get(|| async {
                    Response::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .header(CONTROL_ROUTE_CLASS_HEADER, "auth")
                        .header(EXECUTION_PATH_HEADER, "local_auth_denied")
                        .body(Body::empty())
                        .expect("response should build")
                }),
            )
            .layer(axum::middleware::from_fn(access_log_middleware));

        let _response = app
            .oneshot(
                Request::builder()
                    .uri("/missing")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        let logs = writer.lines();
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0]["event_name"], "http_request_started");
        assert_eq!(logs[1]["event_name"], "http_request_completed");
        assert_eq!(logs[1]["status"], "completed");
        assert_eq!(logs[1]["status_code"], 401);
        assert_eq!(logs[1]["route_class"], "auth");
        assert_eq!(logs[1]["execution_path"], "local_auth_denied");
    }

    #[tokio::test]
    async fn access_log_emits_completed_events_for_streaming_responses() {
        let writer = SharedBuffer::default();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .json()
                .flatten_event(true)
                .with_current_span(false)
                .with_span_list(false)
                .with_writer(writer.clone()),
        );
        let dispatch = tracing::Dispatch::new(subscriber);
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let app = Router::new()
            .route(
                "/stream",
                get(|| async {
                    let body = Body::from_stream(stream::iter(vec![
                        Ok::<Bytes, std::convert::Infallible>(Bytes::from("chunk-1")),
                        Ok::<Bytes, std::convert::Infallible>(Bytes::from("chunk-2")),
                    ]));
                    Response::builder()
                        .status(StatusCode::OK)
                        .header(CONTROL_ROUTE_CLASS_HEADER, "ai_public")
                        .header(EXECUTION_PATH_HEADER, "execution_runtime_stream")
                        .header(CONTROL_REQUEST_ID_HEADER, "req-stream")
                        .body(body)
                        .expect("response should build")
                }),
            )
            .layer(axum::middleware::from_fn(access_log_middleware));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/stream")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("request should succeed");

        assert_eq!(response.status(), StatusCode::OK);

        let logs = writer.lines();
        assert_eq!(logs.len(), 2);
        assert_eq!(logs[0]["event_name"], "http_request_started");
        assert_eq!(logs[1]["event_name"], "http_request_completed");
        assert_eq!(logs[1]["status_code"], 200);
        assert_eq!(logs[1]["request_id"], "req-stream");
        assert_eq!(logs[1]["route_class"], "ai_public");
        assert_eq!(logs[1]["execution_path"], "execution_runtime_stream");
    }
}
