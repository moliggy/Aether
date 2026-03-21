#[path = "constants.rs"]
mod constants;
#[path = "control.rs"]
mod control;
#[path = "error.rs"]
mod error;
#[path = "executor.rs"]
mod executor;
#[path = "handlers.rs"]
mod handlers;
#[path = "headers.rs"]
mod headers;
#[path = "response.rs"]
mod response;

use axum::http::header::{HeaderName, HeaderValue};
use axum::routing::{any, get};
use axum::Router;

pub(crate) use control::{
    maybe_execute_via_control, resolve_control_route, GatewayControlAuthContext,
    GatewayControlDecision,
};
pub(crate) use error::GatewayError;
pub(crate) use executor::{maybe_execute_via_executor_stream, maybe_execute_via_executor_sync};
use handlers::{health, proxy_request};
pub(crate) use response::{build_client_response, build_client_response_from_parts};

#[derive(Debug, Clone)]
pub struct AppState {
    upstream_base_url: String,
    control_base_url: Option<String>,
    executor_base_url: Option<String>,
    client: reqwest::Client,
}

impl AppState {
    pub fn new(
        upstream_base_url: impl Into<String>,
        control_base_url: Option<String>,
    ) -> Result<Self, reqwest::Error> {
        Self::new_with_executor(upstream_base_url, control_base_url, None)
    }

    pub fn new_with_executor(
        upstream_base_url: impl Into<String>,
        control_base_url: Option<String>,
        executor_base_url: Option<String>,
    ) -> Result<Self, reqwest::Error> {
        let client = reqwest::Client::builder()
            .http2_adaptive_window(true)
            .connect_timeout(std::time::Duration::from_secs(10))
            .build()?;
        Ok(Self {
            upstream_base_url: normalize_upstream_base_url(upstream_base_url.into()),
            control_base_url: control_base_url
                .map(normalize_upstream_base_url)
                .filter(|value| !value.is_empty()),
            executor_base_url: executor_base_url
                .map(normalize_upstream_base_url)
                .filter(|value| !value.is_empty()),
            client,
        })
    }
}

pub fn build_router(upstream_base_url: impl Into<String>) -> Result<Router, reqwest::Error> {
    build_router_with_control(upstream_base_url, None)
}

pub fn build_router_with_control(
    upstream_base_url: impl Into<String>,
    control_base_url: Option<String>,
) -> Result<Router, reqwest::Error> {
    Ok(build_router_with_state(AppState::new(
        upstream_base_url,
        control_base_url,
    )?))
}

pub fn build_router_with_endpoints(
    upstream_base_url: impl Into<String>,
    control_base_url: Option<String>,
    executor_base_url: Option<String>,
) -> Result<Router, reqwest::Error> {
    Ok(build_router_with_state(AppState::new_with_executor(
        upstream_base_url,
        control_base_url,
        executor_base_url,
    )?))
}

pub fn build_router_with_state(state: AppState) -> Router {
    Router::new()
        .route("/_gateway/health", get(health))
        .route("/", any(proxy_request))
        .route("/{*path}", any(proxy_request))
        .with_state(state)
}

pub async fn serve_tcp(
    bind: &str,
    upstream_base_url: &str,
    control_base_url: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    serve_tcp_with_endpoints(bind, upstream_base_url, control_base_url, None).await
}

pub async fn serve_tcp_with_endpoints(
    bind: &str,
    upstream_base_url: &str,
    control_base_url: Option<&str>,
    executor_base_url: Option<&str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    let router = build_router_with_endpoints(
        upstream_base_url.to_string(),
        control_base_url.map(ToOwned::to_owned),
        executor_base_url.map(ToOwned::to_owned),
    )?;
    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    Ok(())
}

fn normalize_upstream_base_url(upstream_base_url: String) -> String {
    upstream_base_url.trim_end_matches('/').to_string()
}

fn insert_header_if_missing(
    headers: &mut http::HeaderMap,
    key: &'static str,
    value: &str,
) -> Result<(), GatewayError> {
    if headers.contains_key(key) {
        return Ok(());
    }
    let name = HeaderName::from_static(key);
    let value =
        HeaderValue::from_str(value).map_err(|err| GatewayError::Internal(err.to_string()))?;
    headers.insert(name, value);
    Ok(())
}

#[cfg(test)]
mod tests;
