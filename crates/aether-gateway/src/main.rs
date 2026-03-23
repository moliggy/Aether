use clap::{Parser, ValueEnum};
use tracing::info;

use aether_gateway::{build_router_with_state, AppState, VideoTaskTruthSourceMode};

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum VideoTaskTruthSourceArg {
    PythonSyncReport,
    RustAuthoritative,
}

impl From<VideoTaskTruthSourceArg> for VideoTaskTruthSourceMode {
    fn from(value: VideoTaskTruthSourceArg) -> Self {
        match value {
            VideoTaskTruthSourceArg::PythonSyncReport => VideoTaskTruthSourceMode::PythonSyncReport,
            VideoTaskTruthSourceArg::RustAuthoritative => {
                VideoTaskTruthSourceMode::RustAuthoritative
            }
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "aether-gateway",
    about = "Phase 3a Rust ingress gateway for Aether"
)]
struct Args {
    #[arg(long, env = "AETHER_GATEWAY_BIND", default_value = "0.0.0.0:8084")]
    bind: String,

    #[arg(
        long,
        env = "AETHER_GATEWAY_UPSTREAM",
        default_value = "http://127.0.0.1:18084"
    )]
    upstream: String,

    #[arg(long, env = "AETHER_GATEWAY_CONTROL_URL")]
    control_url: Option<String>,

    #[arg(long, env = "AETHER_GATEWAY_EXECUTOR_URL")]
    executor_url: Option<String>,

    #[arg(
        long,
        env = "AETHER_GATEWAY_VIDEO_TASK_TRUTH_SOURCE_MODE",
        value_enum,
        default_value = "python-sync-report"
    )]
    video_task_truth_source_mode: VideoTaskTruthSourceArg,

    #[arg(
        long,
        env = "AETHER_GATEWAY_VIDEO_TASK_POLLER_INTERVAL_MS",
        default_value_t = 5000
    )]
    video_task_poller_interval_ms: u64,

    #[arg(
        long,
        env = "AETHER_GATEWAY_VIDEO_TASK_POLLER_BATCH_SIZE",
        default_value_t = 32
    )]
    video_task_poller_batch_size: usize,

    #[arg(long, env = "AETHER_GATEWAY_VIDEO_TASK_STORE_PATH")]
    video_task_store_path: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "aether_gateway=info".into()),
        )
        .init();

    let args = Args::parse();
    let control_url = args
        .control_url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let executor_url = args
        .executor_url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    info!(
        bind = %args.bind,
        upstream = %args.upstream,
        control_url = control_url.unwrap_or("-"),
        executor_url = executor_url.unwrap_or("-"),
        video_task_truth_source_mode = ?args.video_task_truth_source_mode,
        video_task_poller_interval_ms = args.video_task_poller_interval_ms,
        video_task_poller_batch_size = args.video_task_poller_batch_size,
        video_task_store_path = args.video_task_store_path.as_deref().unwrap_or("-"),
        "aether-gateway started"
    );
    let mut state = AppState::new_with_executor(
        args.upstream,
        control_url.map(ToOwned::to_owned),
        executor_url.map(ToOwned::to_owned),
    )?
    .with_video_task_truth_source_mode(args.video_task_truth_source_mode.into());
    if matches!(
        args.video_task_truth_source_mode,
        VideoTaskTruthSourceArg::RustAuthoritative
    ) {
        state = state.with_video_task_poller_config(
            std::time::Duration::from_millis(args.video_task_poller_interval_ms.max(1)),
            args.video_task_poller_batch_size.max(1),
        );
    }
    if let Some(path) = args
        .video_task_store_path
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        state = state.with_video_task_store_path(path)?;
    }
    let background_tasks = state.spawn_background_tasks();
    let listener = tokio::net::TcpListener::bind(&args.bind).await?;
    let router = build_router_with_state(state);
    axum::serve(
        listener,
        router.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .await?;
    for handle in background_tasks {
        handle.abort();
    }
    Ok(())
}
