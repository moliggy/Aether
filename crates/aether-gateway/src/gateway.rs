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
#[path = "kiro_stream.rs"]
mod kiro_stream;
#[path = "local_finalize.rs"]
mod local_finalize;
#[path = "local_stream.rs"]
mod local_stream;
#[path = "response.rs"]
mod response;
#[path = "video_tasks.rs"]
mod video_tasks;

use aether_contracts::ExecutionResult;
use axum::http::header::{HeaderName, HeaderValue};
use axum::routing::{any, get};
use axum::Router;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tracing::warn;

pub(crate) use control::{
    cache_executor_auth_context, maybe_execute_via_control, resolve_control_route,
    resolve_executor_auth_context, GatewayControlAuthContext, GatewayControlDecision,
};
pub(crate) use error::GatewayError;
pub(crate) use executor::{maybe_execute_via_executor_stream, maybe_execute_via_executor_sync};
use handlers::{health, proxy_request};
pub(crate) use response::{build_client_response, build_client_response_from_parts};
pub(crate) use video_tasks::VideoTaskService;
pub use video_tasks::VideoTaskTruthSourceMode;

#[derive(Debug, Clone)]
pub(crate) struct CachedAuthContextEntry {
    pub(crate) auth_context: GatewayControlAuthContext,
    pub(crate) cached_at: Instant,
}

#[derive(Debug, Clone)]
pub struct AppState {
    upstream_base_url: String,
    control_base_url: Option<String>,
    executor_base_url: Option<String>,
    video_tasks: Arc<VideoTaskService>,
    video_task_poller: Option<VideoTaskPollerConfig>,
    client: reqwest::Client,
    auth_context_cache: Arc<Mutex<HashMap<String, CachedAuthContextEntry>>>,
    direct_plan_bypass_cache: Arc<Mutex<HashMap<String, Instant>>>,
}

#[derive(Debug, Clone, Copy)]
pub struct VideoTaskPollerConfig {
    interval: Duration,
    batch_size: usize,
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
            .timeout(std::time::Duration::from_secs(300))
            .build()?;
        Ok(Self {
            upstream_base_url: normalize_upstream_base_url(upstream_base_url.into()),
            control_base_url: control_base_url
                .map(normalize_upstream_base_url)
                .filter(|value| !value.is_empty()),
            executor_base_url: executor_base_url
                .map(normalize_upstream_base_url)
                .filter(|value| !value.is_empty()),
            video_tasks: Arc::new(VideoTaskService::new(
                VideoTaskTruthSourceMode::PythonSyncReport,
            )),
            video_task_poller: None,
            client,
            auth_context_cache: Arc::new(Mutex::new(HashMap::new())),
            direct_plan_bypass_cache: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn with_video_task_truth_source_mode(mut self, mode: VideoTaskTruthSourceMode) -> Self {
        self.video_tasks = Arc::new(VideoTaskService::new(mode));
        self
    }

    pub fn with_video_task_poller_config(mut self, interval: Duration, batch_size: usize) -> Self {
        self.video_task_poller = Some(VideoTaskPollerConfig {
            interval,
            batch_size: batch_size.max(1),
        });
        self
    }

    pub fn with_video_task_store_path(
        mut self,
        path: impl Into<std::path::PathBuf>,
    ) -> std::io::Result<Self> {
        self.video_tasks = Arc::new(VideoTaskService::with_file_store(
            self.video_tasks.truth_source_mode(),
            path,
        )?);
        Ok(self)
    }

    pub fn spawn_background_tasks(&self) -> Vec<JoinHandle<()>> {
        let mut tasks = Vec::new();
        if let Some(handle) = self.spawn_video_task_poller() {
            tasks.push(handle);
        }
        tasks
    }

    pub(crate) async fn execute_video_task_refresh_plan(
        &self,
        executor_base_url: &str,
        refresh_plan: &video_tasks::LocalVideoTaskReadRefreshPlan,
    ) -> Result<bool, GatewayError> {
        let response = match self
            .client
            .post(format!("{executor_base_url}/v1/execute/sync"))
            .json(&refresh_plan.plan)
            .send()
            .await
        {
            Ok(response) => response,
            Err(err) => {
                warn!(error = %err, "gateway local video task refresh executor unavailable");
                return Ok(false);
            }
        };

        if response.status() != http::StatusCode::OK {
            return Ok(false);
        }

        let result: ExecutionResult = response
            .json()
            .await
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        if result.status_code >= 400 {
            return Ok(false);
        }
        let provider_body = result
            .body
            .and_then(|body| body.json_body)
            .and_then(|body| body.as_object().cloned());
        let Some(provider_body) = provider_body else {
            return Ok(false);
        };

        Ok(self
            .video_tasks
            .apply_read_refresh_projection(refresh_plan, &provider_body))
    }

    async fn poll_video_tasks_once(&self, batch_size: usize) -> Result<usize, GatewayError> {
        if !self.video_tasks.is_rust_authoritative() {
            return Ok(0);
        }
        let Some(executor_base_url) = self.executor_base_url.as_deref() else {
            return Ok(0);
        };

        let refresh_plans = self
            .video_tasks
            .prepare_poll_refresh_batch(batch_size, "video-task-poller");
        let mut refreshed = 0usize;
        for refresh_plan in refresh_plans {
            if self
                .execute_video_task_refresh_plan(executor_base_url, &refresh_plan)
                .await?
            {
                refreshed += 1;
            }
        }
        Ok(refreshed)
    }

    fn spawn_video_task_poller(&self) -> Option<JoinHandle<()>> {
        let config = self.video_task_poller?;
        if !self.video_tasks.is_rust_authoritative() || self.executor_base_url.is_none() {
            return None;
        }

        let state = self.clone();
        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await;
            loop {
                interval.tick().await;
                if let Err(err) = state.poll_video_tasks_once(config.batch_size).await {
                    warn!(error = ?err, "gateway video task poller tick failed");
                }
            }
        }))
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
