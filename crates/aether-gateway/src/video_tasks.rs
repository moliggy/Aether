use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use aether_contracts::{ExecutionPlan, ExecutionTimeouts, ProxySnapshot, RequestBody};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use uuid::Uuid;

use super::GatewayControlAuthContext;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VideoTaskSyncReportMode {
    InlineSync,
    Background,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum VideoTaskTruthSourceMode {
    #[default]
    PythonSyncReport,
    #[allow(dead_code)] // reserved for the next migration step when Rust owns video_tasks writes
    RustAuthoritative,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LocalVideoTaskSuccessPlan {
    seed: LocalVideoTaskSeed,
    report_mode: VideoTaskSyncReportMode,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LocalVideoTaskFollowUpPlan {
    pub(crate) plan: ExecutionPlan,
    pub(crate) report_kind: Option<String>,
    pub(crate) report_context: Option<Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct LocalVideoTaskReadRefreshPlan {
    pub(crate) plan: ExecutionPlan,
    projection_target: LocalVideoTaskProjectionTarget,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum LocalVideoTaskContentAction {
    Immediate { status_code: u16, body_json: Value },
    StreamPlan(ExecutionPlan),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum LocalVideoTaskProjectionTarget {
    OpenAi { task_id: String },
    Gemini { short_id: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) enum LocalVideoTaskSnapshot {
    OpenAi(OpenAiVideoTaskSeed),
    Gemini(GeminiVideoTaskSeed),
}

#[allow(dead_code)] // projection states are staged for upcoming poller/store wiring
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum LocalVideoTaskStatus {
    Submitted,
    Queued,
    Processing,
    Completed,
    Failed,
    Cancelled,
    Expired,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocalVideoTaskReadResponse {
    pub(crate) status_code: u16,
    pub(crate) body_json: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LocalVideoTaskRegistryMutation {
    OpenAiCancelled { task_id: String },
    OpenAiDeleted { task_id: String },
    GeminiCancelled { short_id: String },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct VideoTaskRegistry {
    openai: BTreeMap<String, LocalVideoTaskSnapshot>,
    gemini: BTreeMap<String, LocalVideoTaskSnapshot>,
}

trait VideoTaskStore: std::fmt::Debug + Send + Sync {
    fn insert(&self, snapshot: LocalVideoTaskSnapshot);
    fn read_openai(&self, task_id: &str) -> Option<LocalVideoTaskReadResponse>;
    fn read_gemini(&self, short_id: &str) -> Option<LocalVideoTaskReadResponse>;
    fn clone_openai(&self, task_id: &str) -> Option<OpenAiVideoTaskSeed>;
    fn clone_gemini(&self, short_id: &str) -> Option<GeminiVideoTaskSeed>;
    fn list_active_snapshots(&self, limit: usize) -> Vec<LocalVideoTaskSnapshot>;
    fn apply_mutation(&self, mutation: LocalVideoTaskRegistryMutation);
    fn project_openai(&self, task_id: &str, provider_body: &Map<String, Value>) -> bool;
    fn project_gemini(&self, short_id: &str, provider_body: &Map<String, Value>) -> bool;
}

#[derive(Debug, Default)]
struct InMemoryVideoTaskStore {
    registry: Mutex<VideoTaskRegistry>,
}

#[derive(Debug)]
struct FileVideoTaskStore {
    path: PathBuf,
    registry: Mutex<VideoTaskRegistry>,
}

#[derive(Debug)]
pub(crate) struct VideoTaskService {
    truth_source_mode: VideoTaskTruthSourceMode,
    store: Arc<dyn VideoTaskStore>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum LocalVideoTaskSeed {
    OpenAiCreate(OpenAiVideoTaskSeed),
    OpenAiRemix(OpenAiVideoTaskSeed),
    GeminiCreate(GeminiVideoTaskSeed),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct LocalVideoTaskTransport {
    upstream_base_url: String,
    provider_name: Option<String>,
    provider_id: String,
    endpoint_id: String,
    key_id: String,
    headers: BTreeMap<String, String>,
    content_type: Option<String>,
    model_name: Option<String>,
    proxy: Option<ProxySnapshot>,
    tls_profile: Option<String>,
    timeouts: Option<ExecutionTimeouts>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct OpenAiVideoTaskSeed {
    local_task_id: String,
    upstream_task_id: String,
    created_at_unix_secs: u64,
    user_id: Option<String>,
    api_key_id: Option<String>,
    model: Option<String>,
    prompt: Option<String>,
    size: Option<String>,
    seconds: Option<String>,
    remixed_from_video_id: Option<String>,
    status: LocalVideoTaskStatus,
    progress_percent: u16,
    completed_at_unix_secs: Option<u64>,
    expires_at_unix_secs: Option<u64>,
    error_code: Option<String>,
    error_message: Option<String>,
    video_url: Option<String>,
    transport: LocalVideoTaskTransport,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct GeminiVideoTaskSeed {
    local_short_id: String,
    upstream_operation_name: String,
    user_id: Option<String>,
    api_key_id: Option<String>,
    model: String,
    status: LocalVideoTaskStatus,
    progress_percent: u16,
    error_code: Option<String>,
    error_message: Option<String>,
    metadata: Value,
    transport: LocalVideoTaskTransport,
}

impl LocalVideoTaskSeed {
    pub(crate) fn from_sync_finalize(
        report_kind: &str,
        provider_body: &Map<String, Value>,
        report_context: &Map<String, Value>,
        plan: &ExecutionPlan,
    ) -> Option<Self> {
        let transport = LocalVideoTaskTransport::from_plan(plan)?;
        match report_kind {
            "openai_video_create_sync_finalize" => {
                let upstream_id = provider_body.get("id").and_then(Value::as_str)?.trim();
                if upstream_id.is_empty() {
                    return None;
                }

                Some(Self::OpenAiCreate(OpenAiVideoTaskSeed {
                    local_task_id: context_text(report_context, "local_task_id")
                        .unwrap_or_else(|| Uuid::new_v4().to_string()),
                    upstream_task_id: upstream_id.to_string(),
                    created_at_unix_secs: context_u64(report_context, "local_created_at")
                        .unwrap_or_else(current_unix_timestamp_secs),
                    user_id: context_text(report_context, "user_id"),
                    api_key_id: context_text(report_context, "api_key_id"),
                    model: context_text(report_context, "model")
                        .or_else(|| request_body_text(report_context, "model")),
                    prompt: request_body_text(report_context, "prompt"),
                    size: request_body_text(report_context, "size"),
                    seconds: request_body_text(report_context, "seconds"),
                    remixed_from_video_id: None,
                    status: LocalVideoTaskStatus::Submitted,
                    progress_percent: 0,
                    completed_at_unix_secs: None,
                    expires_at_unix_secs: None,
                    error_code: None,
                    error_message: None,
                    video_url: None,
                    transport: transport.clone(),
                }))
            }
            "openai_video_remix_sync_finalize" => {
                let upstream_id = provider_body.get("id").and_then(Value::as_str)?.trim();
                if upstream_id.is_empty() {
                    return None;
                }

                Some(Self::OpenAiRemix(OpenAiVideoTaskSeed {
                    local_task_id: context_text(report_context, "local_task_id")
                        .unwrap_or_else(|| Uuid::new_v4().to_string()),
                    upstream_task_id: upstream_id.to_string(),
                    created_at_unix_secs: context_u64(report_context, "local_created_at")
                        .unwrap_or_else(current_unix_timestamp_secs),
                    user_id: context_text(report_context, "user_id"),
                    api_key_id: context_text(report_context, "api_key_id"),
                    model: context_text(report_context, "model")
                        .or_else(|| request_body_text(report_context, "model")),
                    prompt: request_body_text(report_context, "prompt"),
                    size: request_body_text(report_context, "size"),
                    seconds: request_body_text(report_context, "seconds"),
                    remixed_from_video_id: context_text(report_context, "task_id")
                        .or_else(|| request_body_text(report_context, "remix_video_id")),
                    status: LocalVideoTaskStatus::Submitted,
                    progress_percent: 0,
                    completed_at_unix_secs: None,
                    expires_at_unix_secs: None,
                    error_code: None,
                    error_message: None,
                    video_url: None,
                    transport: transport.clone(),
                }))
            }
            "gemini_video_create_sync_finalize" => {
                let operation_name = provider_body
                    .get("name")
                    .or_else(|| provider_body.get("id"))
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty());
                operation_name?;

                Some(Self::GeminiCreate(GeminiVideoTaskSeed {
                    local_short_id: context_text(report_context, "local_short_id")
                        .unwrap_or_else(generate_local_short_id),
                    upstream_operation_name: operation_name?.to_string(),
                    user_id: context_text(report_context, "user_id"),
                    api_key_id: context_text(report_context, "api_key_id"),
                    model: context_text(report_context, "model")
                        .or_else(|| context_text(report_context, "model_name"))
                        .unwrap_or_else(|| "unknown".to_string()),
                    status: LocalVideoTaskStatus::Submitted,
                    progress_percent: 0,
                    error_code: None,
                    error_message: None,
                    metadata: json!({}),
                    transport,
                }))
            }
            _ => None,
        }
    }

    pub(crate) fn success_report_kind(&self) -> &'static str {
        match self {
            Self::OpenAiCreate(_) => "openai_video_create_sync_success",
            Self::OpenAiRemix(_) => "openai_video_remix_sync_success",
            Self::GeminiCreate(_) => "gemini_video_create_sync_success",
        }
    }

    pub(crate) fn apply_to_report_context(&self, report_context: &mut Map<String, Value>) {
        match self {
            Self::OpenAiCreate(seed) | Self::OpenAiRemix(seed) => {
                report_context.insert(
                    "local_task_id".to_string(),
                    Value::String(seed.local_task_id.clone()),
                );
                report_context.insert(
                    "local_created_at".to_string(),
                    Value::Number(seed.created_at_unix_secs.into()),
                );
            }
            Self::GeminiCreate(seed) => {
                report_context.insert(
                    "local_short_id".to_string(),
                    Value::String(seed.local_short_id.clone()),
                );
            }
        }
    }

    pub(crate) fn client_body_json(&self) -> Value {
        match self {
            Self::OpenAiCreate(seed) | Self::OpenAiRemix(seed) => seed.client_body_json(),
            Self::GeminiCreate(seed) => seed.client_body_json(),
        }
    }
}

impl VideoTaskTruthSourceMode {
    fn prepare_sync_success(
        self,
        report_kind: &str,
        provider_body: &Map<String, Value>,
        report_context: &Map<String, Value>,
        plan: &ExecutionPlan,
    ) -> Option<LocalVideoTaskSuccessPlan> {
        let seed = LocalVideoTaskSeed::from_sync_finalize(
            report_kind,
            provider_body,
            report_context,
            plan,
        )?;
        let report_mode = match self {
            Self::PythonSyncReport => VideoTaskSyncReportMode::InlineSync,
            Self::RustAuthoritative => VideoTaskSyncReportMode::Background,
        };
        Some(LocalVideoTaskSuccessPlan { seed, report_mode })
    }
}

impl LocalVideoTaskSuccessPlan {
    pub(crate) fn success_report_kind(&self) -> &'static str {
        self.seed.success_report_kind()
    }

    pub(crate) fn report_mode(&self) -> VideoTaskSyncReportMode {
        self.report_mode
    }

    pub(crate) fn apply_to_report_context(&self, report_context: &mut Map<String, Value>) {
        self.seed.apply_to_report_context(report_context);
    }

    pub(crate) fn client_body_json(&self) -> Value {
        self.seed.client_body_json()
    }

    pub(crate) fn to_snapshot(&self) -> LocalVideoTaskSnapshot {
        match &self.seed {
            LocalVideoTaskSeed::OpenAiCreate(seed) | LocalVideoTaskSeed::OpenAiRemix(seed) => {
                LocalVideoTaskSnapshot::OpenAi(seed.clone())
            }
            LocalVideoTaskSeed::GeminiCreate(seed) => LocalVideoTaskSnapshot::Gemini(seed.clone()),
        }
    }
}

impl OpenAiVideoTaskSeed {
    fn build_content_stream_action(
        &self,
        query_string: Option<&str>,
        trace_id: &str,
    ) -> Option<LocalVideoTaskContentAction> {
        match self.status {
            LocalVideoTaskStatus::Submitted
            | LocalVideoTaskStatus::Queued
            | LocalVideoTaskStatus::Processing => {
                return Some(LocalVideoTaskContentAction::Immediate {
                    status_code: 202,
                    body_json: json!({
                        "detail": format!(
                            "Video is still processing (status: {})",
                            map_openai_task_status(self.status)
                        )
                    }),
                });
            }
            LocalVideoTaskStatus::Failed | LocalVideoTaskStatus::Expired => {
                return Some(LocalVideoTaskContentAction::Immediate {
                    status_code: 422,
                    body_json: json!({
                        "detail": format!(
                            "Video generation failed: {}",
                            self.error_message
                                .clone()
                                .unwrap_or_else(|| "Unknown error".to_string())
                        )
                    }),
                });
            }
            LocalVideoTaskStatus::Cancelled => {
                return Some(LocalVideoTaskContentAction::Immediate {
                    status_code: 404,
                    body_json: json!({"detail": "Video task was cancelled"}),
                });
            }
            LocalVideoTaskStatus::Deleted => {
                return Some(LocalVideoTaskContentAction::Immediate {
                    status_code: 404,
                    body_json: json!({"detail": "Video task not found"}),
                });
            }
            LocalVideoTaskStatus::Completed => {}
        }

        let variant = parse_video_content_variant(query_string)?;
        let (url, headers) = if variant == "video" {
            if let Some(video_url) = self
                .video_url
                .clone()
                .filter(|value| value.starts_with("http://") || value.starts_with("https://"))
            {
                (video_url, BTreeMap::new())
            } else {
                let mut headers = self.transport.headers.clone();
                headers.remove("content-type");
                headers.remove("content-length");
                (
                    format!(
                        "{}/v1/videos/{}/content",
                        self.transport.upstream_base_url.trim_end_matches('/'),
                        self.upstream_task_id
                    ),
                    headers,
                )
            }
        } else {
            let mut headers = self.transport.headers.clone();
            headers.remove("content-type");
            headers.remove("content-length");
            (
                format!(
                    "{}/v1/videos/{}/content?variant={variant}",
                    self.transport.upstream_base_url.trim_end_matches('/'),
                    self.upstream_task_id
                ),
                headers,
            )
        };

        Some(LocalVideoTaskContentAction::StreamPlan(ExecutionPlan {
            request_id: trace_id.to_string(),
            candidate_id: None,
            provider_name: self.transport.provider_name.clone(),
            provider_id: self.transport.provider_id.clone(),
            endpoint_id: self.transport.endpoint_id.clone(),
            key_id: self.transport.key_id.clone(),
            method: "GET".to_string(),
            url,
            headers,
            content_type: None,
            content_encoding: None,
            body: RequestBody {
                json_body: None,
                body_bytes_b64: None,
                body_ref: None,
            },
            stream: true,
            client_api_format: "openai:video".to_string(),
            provider_api_format: "openai:video".to_string(),
            model_name: self
                .model
                .clone()
                .or_else(|| self.transport.model_name.clone()),
            proxy: self.transport.proxy.clone(),
            tls_profile: self.transport.tls_profile.clone(),
            timeouts: self.transport.timeouts.clone(),
        }))
    }

    fn build_delete_follow_up_plan(
        &self,
        auth_context: Option<&GatewayControlAuthContext>,
        trace_id: &str,
    ) -> Option<LocalVideoTaskFollowUpPlan> {
        if !matches!(
            self.status,
            LocalVideoTaskStatus::Completed | LocalVideoTaskStatus::Failed
        ) {
            return None;
        }
        let (user_id, api_key_id) = resolve_follow_up_auth(
            self.user_id.as_deref(),
            self.api_key_id.as_deref(),
            auth_context,
        )?;

        let mut headers = self.transport.headers.clone();
        headers.remove("content-type");
        headers.remove("content-length");

        Some(LocalVideoTaskFollowUpPlan {
            plan: ExecutionPlan {
                request_id: trace_id.to_string(),
                candidate_id: None,
                provider_name: self.transport.provider_name.clone(),
                provider_id: self.transport.provider_id.clone(),
                endpoint_id: self.transport.endpoint_id.clone(),
                key_id: self.transport.key_id.clone(),
                method: "DELETE".to_string(),
                url: format!(
                    "{}/v1/videos/{}",
                    self.transport.upstream_base_url.trim_end_matches('/'),
                    self.upstream_task_id
                ),
                headers,
                content_type: None,
                content_encoding: None,
                body: RequestBody {
                    json_body: None,
                    body_bytes_b64: None,
                    body_ref: None,
                },
                stream: false,
                client_api_format: "openai:video".to_string(),
                provider_api_format: "openai:video".to_string(),
                model_name: self
                    .model
                    .clone()
                    .or_else(|| self.transport.model_name.clone()),
                proxy: self.transport.proxy.clone(),
                tls_profile: self.transport.tls_profile.clone(),
                timeouts: self.transport.timeouts.clone(),
            },
            report_kind: Some("openai_video_delete_sync_finalize".to_string()),
            report_context: Some(build_video_follow_up_report_context(
                &user_id,
                &api_key_id,
                &self.local_task_id,
                self.model
                    .clone()
                    .or_else(|| self.transport.model_name.clone()),
                &self.transport,
                "openai:video",
                "openai:video",
            )),
        })
    }

    fn build_get_follow_up_plan(&self, trace_id: &str) -> Option<ExecutionPlan> {
        if !matches!(
            self.status,
            LocalVideoTaskStatus::Submitted
                | LocalVideoTaskStatus::Queued
                | LocalVideoTaskStatus::Processing
        ) {
            return None;
        }

        let mut headers = self.transport.headers.clone();
        headers.remove("content-type");
        headers.remove("content-length");

        Some(ExecutionPlan {
            request_id: trace_id.to_string(),
            candidate_id: None,
            provider_name: self.transport.provider_name.clone(),
            provider_id: self.transport.provider_id.clone(),
            endpoint_id: self.transport.endpoint_id.clone(),
            key_id: self.transport.key_id.clone(),
            method: "GET".to_string(),
            url: format!(
                "{}/v1/videos/{}",
                self.transport.upstream_base_url.trim_end_matches('/'),
                self.upstream_task_id
            ),
            headers,
            content_type: None,
            content_encoding: None,
            body: RequestBody {
                json_body: None,
                body_bytes_b64: None,
                body_ref: None,
            },
            stream: false,
            client_api_format: "openai:video".to_string(),
            provider_api_format: "openai:video".to_string(),
            model_name: self
                .model
                .clone()
                .or_else(|| self.transport.model_name.clone()),
            proxy: self.transport.proxy.clone(),
            tls_profile: self.transport.tls_profile.clone(),
            timeouts: self.transport.timeouts.clone(),
        })
    }

    fn build_cancel_follow_up_plan(
        &self,
        auth_context: Option<&GatewayControlAuthContext>,
        trace_id: &str,
    ) -> Option<LocalVideoTaskFollowUpPlan> {
        if !matches!(
            self.status,
            LocalVideoTaskStatus::Submitted
                | LocalVideoTaskStatus::Queued
                | LocalVideoTaskStatus::Processing
        ) {
            return None;
        }
        let (user_id, api_key_id) = resolve_follow_up_auth(
            self.user_id.as_deref(),
            self.api_key_id.as_deref(),
            auth_context,
        )?;

        let mut headers = self.transport.headers.clone();
        headers.remove("content-type");
        headers.remove("content-length");

        Some(LocalVideoTaskFollowUpPlan {
            plan: ExecutionPlan {
                request_id: trace_id.to_string(),
                candidate_id: None,
                provider_name: self.transport.provider_name.clone(),
                provider_id: self.transport.provider_id.clone(),
                endpoint_id: self.transport.endpoint_id.clone(),
                key_id: self.transport.key_id.clone(),
                method: "DELETE".to_string(),
                url: format!(
                    "{}/v1/videos/{}",
                    self.transport.upstream_base_url.trim_end_matches('/'),
                    self.upstream_task_id
                ),
                headers,
                content_type: None,
                content_encoding: None,
                body: RequestBody {
                    json_body: None,
                    body_bytes_b64: None,
                    body_ref: None,
                },
                stream: false,
                client_api_format: "openai:video".to_string(),
                provider_api_format: "openai:video".to_string(),
                model_name: self
                    .model
                    .clone()
                    .or_else(|| self.transport.model_name.clone()),
                proxy: self.transport.proxy.clone(),
                tls_profile: self.transport.tls_profile.clone(),
                timeouts: self.transport.timeouts.clone(),
            },
            report_kind: Some("openai_video_cancel_sync_finalize".to_string()),
            report_context: Some(build_video_follow_up_report_context(
                &user_id,
                &api_key_id,
                &self.local_task_id,
                self.model
                    .clone()
                    .or_else(|| self.transport.model_name.clone()),
                &self.transport,
                "openai:video",
                "openai:video",
            )),
        })
    }
}

impl GeminiVideoTaskSeed {
    fn build_get_follow_up_plan(&self, trace_id: &str) -> Option<ExecutionPlan> {
        if !matches!(
            self.status,
            LocalVideoTaskStatus::Submitted
                | LocalVideoTaskStatus::Queued
                | LocalVideoTaskStatus::Processing
        ) {
            return None;
        }

        let operation_path = self.resolve_operation_path()?;
        let mut headers = self.transport.headers.clone();
        headers.remove("content-type");
        headers.remove("content-length");

        Some(ExecutionPlan {
            request_id: trace_id.to_string(),
            candidate_id: None,
            provider_name: self.transport.provider_name.clone(),
            provider_id: self.transport.provider_id.clone(),
            endpoint_id: self.transport.endpoint_id.clone(),
            key_id: self.transport.key_id.clone(),
            method: "GET".to_string(),
            url: format!(
                "{}/v1beta/{}",
                self.transport.upstream_base_url.trim_end_matches('/'),
                operation_path
            ),
            headers,
            content_type: None,
            content_encoding: None,
            body: RequestBody {
                json_body: None,
                body_bytes_b64: None,
                body_ref: None,
            },
            stream: false,
            client_api_format: "gemini:video".to_string(),
            provider_api_format: "gemini:video".to_string(),
            model_name: Some(self.model.clone()),
            proxy: self.transport.proxy.clone(),
            tls_profile: self.transport.tls_profile.clone(),
            timeouts: self.transport.timeouts.clone(),
        })
    }

    fn resolve_operation_path(&self) -> Option<String> {
        if self.upstream_operation_name.starts_with("models/") {
            Some(self.upstream_operation_name.clone())
        } else if self.upstream_operation_name.starts_with("operations/") && !self.model.is_empty()
        {
            Some(format!(
                "models/{}/{}",
                self.model, self.upstream_operation_name
            ))
        } else {
            None
        }
    }

    fn build_cancel_follow_up_plan(
        &self,
        auth_context: Option<&GatewayControlAuthContext>,
        trace_id: &str,
    ) -> Option<LocalVideoTaskFollowUpPlan> {
        if !matches!(
            self.status,
            LocalVideoTaskStatus::Submitted
                | LocalVideoTaskStatus::Queued
                | LocalVideoTaskStatus::Processing
        ) {
            return None;
        }
        let (user_id, api_key_id) = resolve_follow_up_auth(
            self.user_id.as_deref(),
            self.api_key_id.as_deref(),
            auth_context,
        )?;

        let operation_path = self.resolve_operation_path()?;

        let mut headers = self.transport.headers.clone();
        let content_type = self
            .transport
            .content_type
            .clone()
            .unwrap_or_else(|| "application/json".to_string());
        headers
            .entry("content-type".to_string())
            .or_insert_with(|| content_type.clone());

        Some(LocalVideoTaskFollowUpPlan {
            plan: ExecutionPlan {
                request_id: trace_id.to_string(),
                candidate_id: None,
                provider_name: self.transport.provider_name.clone(),
                provider_id: self.transport.provider_id.clone(),
                endpoint_id: self.transport.endpoint_id.clone(),
                key_id: self.transport.key_id.clone(),
                method: "POST".to_string(),
                url: format!(
                    "{}/v1beta/{}:cancel",
                    self.transport.upstream_base_url.trim_end_matches('/'),
                    operation_path
                ),
                headers,
                content_type: Some(content_type),
                content_encoding: None,
                body: RequestBody::from_json(json!({})),
                stream: false,
                client_api_format: "gemini:video".to_string(),
                provider_api_format: "gemini:video".to_string(),
                model_name: Some(self.model.clone()),
                proxy: self.transport.proxy.clone(),
                tls_profile: self.transport.tls_profile.clone(),
                timeouts: self.transport.timeouts.clone(),
            },
            report_kind: Some("gemini_video_cancel_sync_finalize".to_string()),
            report_context: Some(build_video_follow_up_report_context(
                &user_id,
                &api_key_id,
                &self.local_short_id,
                Some(self.model.clone()),
                &self.transport,
                "gemini:video",
                "gemini:video",
            )),
        })
    }
}

impl OpenAiVideoTaskSeed {
    fn client_body_json(&self) -> Value {
        let mut body = json!({
            "id": self.local_task_id,
            "object": "video",
            "status": map_openai_task_status(self.status),
            "progress": self.progress_percent,
            "created_at": self.created_at_unix_secs,
        });

        if let Some(model) = &self.model {
            body["model"] = Value::String(model.clone());
        }
        if let Some(prompt) = &self.prompt {
            body["prompt"] = Value::String(prompt.clone());
        }
        if let Some(size) = &self.size {
            body["size"] = Value::String(size.clone());
        }
        if let Some(seconds) = &self.seconds {
            body["seconds"] = Value::String(seconds.clone());
        }
        if let Some(remixed_from_video_id) = &self.remixed_from_video_id {
            body["remixed_from_video_id"] = Value::String(remixed_from_video_id.clone());
        }
        if let Some(completed_at) = self.completed_at_unix_secs {
            body["completed_at"] = Value::Number(completed_at.into());
        }
        if let Some(expires_at) = self.expires_at_unix_secs {
            body["expires_at"] = Value::Number(expires_at.into());
        }
        if self.status == LocalVideoTaskStatus::Failed
            || self.status == LocalVideoTaskStatus::Expired
        {
            body["error"] = json!({
                "code": self.error_code.clone().unwrap_or_else(|| "unknown".to_string()),
                "message": self
                    .error_message
                    .clone()
                    .unwrap_or_else(|| "Video generation failed".to_string()),
            });
        }

        body
    }
}

impl GeminiVideoTaskSeed {
    fn client_body_json(&self) -> Value {
        let operation_name = format!("models/{}/operations/{}", self.model, self.local_short_id);
        match self.status {
            LocalVideoTaskStatus::Completed => json!({
                "name": operation_name,
                "done": true,
                "response": {
                    "generateVideoResponse": {
                        "generatedSamples": [
                            {
                                "video": {
                                    "uri": format!("/v1beta/files/aev_{}:download?alt=media", self.local_short_id),
                                    "mimeType": "video/mp4"
                                }
                            }
                        ]
                    }
                }
            }),
            LocalVideoTaskStatus::Failed | LocalVideoTaskStatus::Expired => json!({
                "name": operation_name,
                "done": true,
                "error": {
                    "code": self.error_code.clone().unwrap_or_else(|| "UNKNOWN".to_string()),
                    "message": self
                        .error_message
                        .clone()
                        .unwrap_or_else(|| "Video generation failed".to_string()),
                }
            }),
            _ => json!({
                "name": operation_name,
                "done": false,
                "metadata": self.metadata.clone(),
            }),
        }
    }
}

impl LocalVideoTaskSnapshot {
    pub(crate) fn read_response(&self) -> LocalVideoTaskReadResponse {
        match self {
            Self::OpenAi(seed) => match seed.status {
                LocalVideoTaskStatus::Cancelled => LocalVideoTaskReadResponse {
                    status_code: 404,
                    body_json: json!({"detail": "Video task was cancelled"}),
                },
                LocalVideoTaskStatus::Deleted => LocalVideoTaskReadResponse {
                    status_code: 404,
                    body_json: json!({"detail": "Video task not found"}),
                },
                _ => LocalVideoTaskReadResponse {
                    status_code: 200,
                    body_json: seed.client_body_json(),
                },
            },
            Self::Gemini(seed) => match seed.status {
                LocalVideoTaskStatus::Cancelled => LocalVideoTaskReadResponse {
                    status_code: 404,
                    body_json: json!({"detail": "Video task was cancelled"}),
                },
                LocalVideoTaskStatus::Deleted => LocalVideoTaskReadResponse {
                    status_code: 404,
                    body_json: json!({"detail": "Video task not found"}),
                },
                _ => LocalVideoTaskReadResponse {
                    status_code: 200,
                    body_json: seed.client_body_json(),
                },
            },
        }
    }

    fn is_active_for_refresh(&self) -> bool {
        match self {
            Self::OpenAi(seed) => matches!(
                seed.status,
                LocalVideoTaskStatus::Submitted
                    | LocalVideoTaskStatus::Queued
                    | LocalVideoTaskStatus::Processing
            ),
            Self::Gemini(seed) => matches!(
                seed.status,
                LocalVideoTaskStatus::Submitted
                    | LocalVideoTaskStatus::Queued
                    | LocalVideoTaskStatus::Processing
            ),
        }
    }
}

impl VideoTaskRegistry {
    fn insert(&mut self, snapshot: LocalVideoTaskSnapshot) {
        match &snapshot {
            LocalVideoTaskSnapshot::OpenAi(seed) => {
                self.openai.insert(seed.local_task_id.clone(), snapshot);
            }
            LocalVideoTaskSnapshot::Gemini(seed) => {
                self.gemini.insert(seed.local_short_id.clone(), snapshot);
            }
        }
    }

    fn read_openai(&self, task_id: &str) -> Option<LocalVideoTaskReadResponse> {
        self.openai
            .get(task_id)
            .map(LocalVideoTaskSnapshot::read_response)
    }

    fn read_gemini(&self, short_id: &str) -> Option<LocalVideoTaskReadResponse> {
        self.gemini
            .get(short_id)
            .map(LocalVideoTaskSnapshot::read_response)
    }

    fn list_active_snapshots(&self, limit: usize) -> Vec<LocalVideoTaskSnapshot> {
        self.openai
            .values()
            .chain(self.gemini.values())
            .filter(|snapshot| snapshot.is_active_for_refresh())
            .take(limit)
            .cloned()
            .collect()
    }

    fn apply_mutation(&mut self, mutation: LocalVideoTaskRegistryMutation) {
        match mutation {
            LocalVideoTaskRegistryMutation::OpenAiCancelled { task_id } => {
                if let Some(LocalVideoTaskSnapshot::OpenAi(seed)) = self.openai.get_mut(&task_id) {
                    seed.status = LocalVideoTaskStatus::Cancelled;
                }
            }
            LocalVideoTaskRegistryMutation::OpenAiDeleted { task_id } => {
                if let Some(LocalVideoTaskSnapshot::OpenAi(seed)) = self.openai.get_mut(&task_id) {
                    seed.status = LocalVideoTaskStatus::Deleted;
                }
            }
            LocalVideoTaskRegistryMutation::GeminiCancelled { short_id } => {
                if let Some(LocalVideoTaskSnapshot::Gemini(seed)) = self.gemini.get_mut(&short_id) {
                    seed.status = LocalVideoTaskStatus::Cancelled;
                }
            }
        }
    }

    #[allow(dead_code)] // used by staged Rust-side status projection tests before runtime wiring
    fn project_openai(&mut self, task_id: &str, provider_body: &Map<String, Value>) -> bool {
        let Some(LocalVideoTaskSnapshot::OpenAi(seed)) = self.openai.get_mut(task_id) else {
            return false;
        };
        let raw_status = provider_body
            .get("status")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or_default();
        seed.status = match raw_status {
            "queued" => LocalVideoTaskStatus::Queued,
            "processing" => LocalVideoTaskStatus::Processing,
            "completed" => LocalVideoTaskStatus::Completed,
            "failed" => LocalVideoTaskStatus::Failed,
            "cancelled" => LocalVideoTaskStatus::Cancelled,
            "expired" => LocalVideoTaskStatus::Expired,
            _ => LocalVideoTaskStatus::Submitted,
        };
        seed.progress_percent = provider_body
            .get("progress")
            .and_then(Value::as_u64)
            .and_then(|value| u16::try_from(value).ok())
            .unwrap_or(match seed.status {
                LocalVideoTaskStatus::Completed => 100,
                LocalVideoTaskStatus::Processing => 50,
                _ => seed.progress_percent,
            });
        seed.completed_at_unix_secs = provider_body.get("completed_at").and_then(Value::as_u64);
        seed.expires_at_unix_secs = provider_body.get("expires_at").and_then(Value::as_u64);
        let error = provider_body.get("error").and_then(Value::as_object);
        seed.error_code = error
            .and_then(|value| value.get("code"))
            .and_then(Value::as_str)
            .map(str::to_string);
        seed.error_message = error
            .and_then(|value| value.get("message"))
            .and_then(Value::as_str)
            .map(str::to_string);
        seed.video_url = provider_body
            .get("video_url")
            .or_else(|| provider_body.get("url"))
            .or_else(|| provider_body.get("result_url"))
            .and_then(Value::as_str)
            .map(str::to_string);
        true
    }

    #[allow(dead_code)] // used by staged Rust-side status projection tests before runtime wiring
    fn project_gemini(&mut self, short_id: &str, provider_body: &Map<String, Value>) -> bool {
        let Some(LocalVideoTaskSnapshot::Gemini(seed)) = self.gemini.get_mut(short_id) else {
            return false;
        };
        let done = provider_body
            .get("done")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if done {
            let error = provider_body.get("error").and_then(Value::as_object);
            if let Some(error) = error {
                seed.status = LocalVideoTaskStatus::Failed;
                seed.progress_percent = 100;
                seed.error_code = error
                    .get("code")
                    .and_then(Value::as_str)
                    .map(str::to_string);
                seed.error_message = error
                    .get("message")
                    .and_then(Value::as_str)
                    .map(str::to_string);
            } else {
                seed.status = LocalVideoTaskStatus::Completed;
                seed.progress_percent = 100;
                seed.error_code = None;
                seed.error_message = None;
            }
            seed.metadata = json!({});
            return true;
        }

        seed.status = LocalVideoTaskStatus::Processing;
        seed.progress_percent = 50;
        seed.error_code = None;
        seed.error_message = None;
        seed.metadata = provider_body
            .get("metadata")
            .cloned()
            .unwrap_or_else(|| json!({}));
        true
    }
}

impl VideoTaskStore for InMemoryVideoTaskStore {
    fn insert(&self, snapshot: LocalVideoTaskSnapshot) {
        if let Ok(mut registry) = self.registry.lock() {
            registry.insert(snapshot);
        }
    }

    fn read_openai(&self, task_id: &str) -> Option<LocalVideoTaskReadResponse> {
        let registry = self.registry.lock().ok()?;
        registry.read_openai(task_id)
    }

    fn read_gemini(&self, short_id: &str) -> Option<LocalVideoTaskReadResponse> {
        let registry = self.registry.lock().ok()?;
        registry.read_gemini(short_id)
    }

    fn clone_openai(&self, task_id: &str) -> Option<OpenAiVideoTaskSeed> {
        let registry = self.registry.lock().ok()?;
        let LocalVideoTaskSnapshot::OpenAi(seed) = registry.openai.get(task_id)?.clone() else {
            return None;
        };
        Some(seed)
    }

    fn clone_gemini(&self, short_id: &str) -> Option<GeminiVideoTaskSeed> {
        let registry = self.registry.lock().ok()?;
        let LocalVideoTaskSnapshot::Gemini(seed) = registry.gemini.get(short_id)?.clone() else {
            return None;
        };
        Some(seed)
    }

    fn list_active_snapshots(&self, limit: usize) -> Vec<LocalVideoTaskSnapshot> {
        let Ok(registry) = self.registry.lock() else {
            return Vec::new();
        };
        registry.list_active_snapshots(limit)
    }

    fn apply_mutation(&self, mutation: LocalVideoTaskRegistryMutation) {
        if let Ok(mut registry) = self.registry.lock() {
            registry.apply_mutation(mutation);
        }
    }

    fn project_openai(&self, task_id: &str, provider_body: &Map<String, Value>) -> bool {
        let Ok(mut registry) = self.registry.lock() else {
            return false;
        };
        registry.project_openai(task_id, provider_body)
    }

    fn project_gemini(&self, short_id: &str, provider_body: &Map<String, Value>) -> bool {
        let Ok(mut registry) = self.registry.lock() else {
            return false;
        };
        registry.project_gemini(short_id, provider_body)
    }
}

impl FileVideoTaskStore {
    fn new(path: impl Into<PathBuf>) -> std::io::Result<Self> {
        let path = path.into();
        let registry = Self::load_registry(&path)?;
        Ok(Self {
            path,
            registry: Mutex::new(registry),
        })
    }

    fn load_registry(path: &Path) -> std::io::Result<VideoTaskRegistry> {
        if !path.exists() {
            return Ok(VideoTaskRegistry::default());
        }
        let bytes = std::fs::read(path)?;
        if bytes.is_empty() {
            return Ok(VideoTaskRegistry::default());
        }
        serde_json::from_slice(&bytes)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
    }

    fn persist_registry(&self, registry: &VideoTaskRegistry) -> std::io::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let bytes = serde_json::to_vec_pretty(registry)
            .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
        let temp_path = self.path.with_extension("tmp");
        std::fs::write(&temp_path, bytes)?;
        std::fs::rename(temp_path, &self.path)?;
        Ok(())
    }

    fn mutate_registry(&self, mutator: impl FnOnce(&mut VideoTaskRegistry) -> bool) -> bool {
        let Ok(mut registry) = self.registry.lock() else {
            return false;
        };
        if !mutator(&mut registry) {
            return false;
        }
        self.persist_registry(&registry).is_ok()
    }
}

impl VideoTaskStore for FileVideoTaskStore {
    fn insert(&self, snapshot: LocalVideoTaskSnapshot) {
        let _ = self.mutate_registry(|registry| {
            registry.insert(snapshot);
            true
        });
    }

    fn read_openai(&self, task_id: &str) -> Option<LocalVideoTaskReadResponse> {
        let registry = self.registry.lock().ok()?;
        registry.read_openai(task_id)
    }

    fn read_gemini(&self, short_id: &str) -> Option<LocalVideoTaskReadResponse> {
        let registry = self.registry.lock().ok()?;
        registry.read_gemini(short_id)
    }

    fn clone_openai(&self, task_id: &str) -> Option<OpenAiVideoTaskSeed> {
        let registry = self.registry.lock().ok()?;
        let LocalVideoTaskSnapshot::OpenAi(seed) = registry.openai.get(task_id)?.clone() else {
            return None;
        };
        Some(seed)
    }

    fn clone_gemini(&self, short_id: &str) -> Option<GeminiVideoTaskSeed> {
        let registry = self.registry.lock().ok()?;
        let LocalVideoTaskSnapshot::Gemini(seed) = registry.gemini.get(short_id)?.clone() else {
            return None;
        };
        Some(seed)
    }

    fn list_active_snapshots(&self, limit: usize) -> Vec<LocalVideoTaskSnapshot> {
        let Ok(registry) = self.registry.lock() else {
            return Vec::new();
        };
        registry.list_active_snapshots(limit)
    }

    fn apply_mutation(&self, mutation: LocalVideoTaskRegistryMutation) {
        let _ = self.mutate_registry(|registry| {
            registry.apply_mutation(mutation);
            true
        });
    }

    fn project_openai(&self, task_id: &str, provider_body: &Map<String, Value>) -> bool {
        self.mutate_registry(|registry| registry.project_openai(task_id, provider_body))
    }

    fn project_gemini(&self, short_id: &str, provider_body: &Map<String, Value>) -> bool {
        self.mutate_registry(|registry| registry.project_gemini(short_id, provider_body))
    }
}

impl VideoTaskService {
    pub(crate) fn new(mode: VideoTaskTruthSourceMode) -> Self {
        Self::with_store(mode, Arc::new(InMemoryVideoTaskStore::default()))
    }

    pub(crate) fn with_file_store(
        mode: VideoTaskTruthSourceMode,
        path: impl Into<PathBuf>,
    ) -> std::io::Result<Self> {
        Ok(Self::with_store(
            mode,
            Arc::new(FileVideoTaskStore::new(path)?),
        ))
    }

    fn with_store(mode: VideoTaskTruthSourceMode, store: Arc<dyn VideoTaskStore>) -> Self {
        Self {
            truth_source_mode: mode,
            store,
        }
    }

    pub(crate) fn prepare_sync_success(
        &self,
        report_kind: &str,
        provider_body: &Map<String, Value>,
        report_context: &Map<String, Value>,
        plan: &ExecutionPlan,
    ) -> Option<LocalVideoTaskSuccessPlan> {
        self.truth_source_mode.prepare_sync_success(
            report_kind,
            provider_body,
            report_context,
            plan,
        )
    }

    pub(crate) fn record_snapshot(&self, snapshot: LocalVideoTaskSnapshot) {
        self.store.insert(snapshot);
    }

    pub(crate) fn is_rust_authoritative(&self) -> bool {
        self.truth_source_mode == VideoTaskTruthSourceMode::RustAuthoritative
    }

    pub(crate) fn truth_source_mode(&self) -> VideoTaskTruthSourceMode {
        self.truth_source_mode
    }

    pub(crate) fn read_response(
        &self,
        route_family: Option<&str>,
        request_path: &str,
    ) -> Option<LocalVideoTaskReadResponse> {
        if self.truth_source_mode != VideoTaskTruthSourceMode::RustAuthoritative {
            return None;
        }
        match route_family {
            Some("openai") => extract_openai_task_id_from_path(request_path)
                .and_then(|task_id| self.store.read_openai(task_id)),
            Some("gemini") => extract_gemini_short_id_from_path(request_path)
                .and_then(|short_id| self.store.read_gemini(short_id)),
            _ => None,
        }
    }

    pub(crate) fn apply_finalize_mutation(&self, request_path: &str, report_kind: &str) {
        let Some(mutation) = resolve_local_video_registry_mutation(
            self.truth_source_mode,
            request_path,
            report_kind,
        ) else {
            return;
        };
        self.store.apply_mutation(mutation);
    }

    pub(crate) fn project_openai_task_response(
        &self,
        task_id: &str,
        provider_body: &Map<String, Value>,
    ) -> bool {
        if self.truth_source_mode != VideoTaskTruthSourceMode::RustAuthoritative {
            return false;
        }
        self.store.project_openai(task_id, provider_body)
    }

    pub(crate) fn project_gemini_task_response(
        &self,
        short_id: &str,
        provider_body: &Map<String, Value>,
    ) -> bool {
        if self.truth_source_mode != VideoTaskTruthSourceMode::RustAuthoritative {
            return false;
        }
        self.store.project_gemini(short_id, provider_body)
    }

    pub(crate) fn prepare_follow_up_sync_plan(
        &self,
        plan_kind: &str,
        request_path: &str,
        auth_context: Option<&GatewayControlAuthContext>,
        trace_id: &str,
    ) -> Option<LocalVideoTaskFollowUpPlan> {
        if self.truth_source_mode != VideoTaskTruthSourceMode::RustAuthoritative {
            return None;
        }
        match plan_kind {
            "openai_video_delete_sync" => {
                let task_id = extract_openai_task_id_from_path(request_path)?;
                let seed = self.store.clone_openai(task_id)?;
                seed.build_delete_follow_up_plan(auth_context, trace_id)
            }
            "openai_video_cancel_sync" => {
                let task_id = extract_openai_task_id_from_cancel_path(request_path)?;
                let seed = self.store.clone_openai(task_id)?;
                seed.build_cancel_follow_up_plan(auth_context, trace_id)
            }
            "gemini_video_cancel_sync" => {
                let short_id = extract_gemini_short_id_from_cancel_path(request_path)?;
                let seed = self.store.clone_gemini(short_id)?;
                seed.build_cancel_follow_up_plan(auth_context, trace_id)
            }
            _ => None,
        }
    }

    pub(crate) fn prepare_read_refresh_sync_plan(
        &self,
        route_family: Option<&str>,
        request_path: &str,
        trace_id: &str,
    ) -> Option<LocalVideoTaskReadRefreshPlan> {
        if self.truth_source_mode != VideoTaskTruthSourceMode::RustAuthoritative {
            return None;
        }
        match route_family {
            Some("openai") => {
                let task_id = extract_openai_task_id_from_path(request_path)?;
                let seed = self.store.clone_openai(task_id)?;
                Some(LocalVideoTaskReadRefreshPlan {
                    plan: seed.build_get_follow_up_plan(trace_id)?,
                    projection_target: LocalVideoTaskProjectionTarget::OpenAi {
                        task_id: task_id.to_string(),
                    },
                })
            }
            Some("gemini") => {
                let short_id = extract_gemini_short_id_from_path(request_path)?;
                let seed = self.store.clone_gemini(short_id)?;
                Some(LocalVideoTaskReadRefreshPlan {
                    plan: seed.build_get_follow_up_plan(trace_id)?,
                    projection_target: LocalVideoTaskProjectionTarget::Gemini {
                        short_id: short_id.to_string(),
                    },
                })
            }
            _ => None,
        }
    }

    pub(crate) fn prepare_poll_refresh_batch(
        &self,
        limit: usize,
        trace_prefix: &str,
    ) -> Vec<LocalVideoTaskReadRefreshPlan> {
        if self.truth_source_mode != VideoTaskTruthSourceMode::RustAuthoritative || limit == 0 {
            return Vec::new();
        }

        self.store
            .list_active_snapshots(limit)
            .into_iter()
            .enumerate()
            .filter_map(|(index, snapshot)| {
                let trace_id = format!("{trace_prefix}-{index}");
                match snapshot {
                    LocalVideoTaskSnapshot::OpenAi(seed) => Some(LocalVideoTaskReadRefreshPlan {
                        plan: seed.build_get_follow_up_plan(&trace_id)?,
                        projection_target: LocalVideoTaskProjectionTarget::OpenAi {
                            task_id: seed.local_task_id.clone(),
                        },
                    }),
                    LocalVideoTaskSnapshot::Gemini(seed) => Some(LocalVideoTaskReadRefreshPlan {
                        plan: seed.build_get_follow_up_plan(&trace_id)?,
                        projection_target: LocalVideoTaskProjectionTarget::Gemini {
                            short_id: seed.local_short_id.clone(),
                        },
                    }),
                }
            })
            .collect()
    }

    pub(crate) fn prepare_openai_content_stream_action(
        &self,
        request_path: &str,
        query_string: Option<&str>,
        trace_id: &str,
    ) -> Option<LocalVideoTaskContentAction> {
        if self.truth_source_mode != VideoTaskTruthSourceMode::RustAuthoritative {
            return None;
        }
        let task_id = extract_openai_task_id_from_content_path(request_path)?;
        let seed = self.store.clone_openai(task_id)?;
        seed.build_content_stream_action(query_string, trace_id)
    }

    pub(crate) fn apply_read_refresh_projection(
        &self,
        refresh_plan: &LocalVideoTaskReadRefreshPlan,
        provider_body: &Map<String, Value>,
    ) -> bool {
        match &refresh_plan.projection_target {
            LocalVideoTaskProjectionTarget::OpenAi { task_id } => {
                self.project_openai_task_response(task_id, provider_body)
            }
            LocalVideoTaskProjectionTarget::Gemini { short_id } => {
                self.project_gemini_task_response(short_id, provider_body)
            }
        }
    }
}

impl LocalVideoTaskTransport {
    fn from_plan(plan: &ExecutionPlan) -> Option<Self> {
        let upstream_base_url = match plan.provider_api_format.as_str() {
            "openai:video" => plan.url.split("/v1/videos").next()?.to_string(),
            "gemini:video" => plan.url.split("/v1beta/").next()?.to_string(),
            _ => return None,
        };
        if upstream_base_url.is_empty() {
            return None;
        }
        Some(Self {
            upstream_base_url,
            provider_name: plan.provider_name.clone(),
            provider_id: plan.provider_id.clone(),
            endpoint_id: plan.endpoint_id.clone(),
            key_id: plan.key_id.clone(),
            headers: plan.headers.clone(),
            content_type: plan.content_type.clone(),
            model_name: plan.model_name.clone(),
            proxy: plan.proxy.clone(),
            tls_profile: plan.tls_profile.clone(),
            timeouts: plan.timeouts.clone(),
        })
    }
}

pub(crate) fn extract_openai_task_id_from_path(path: &str) -> Option<&str> {
    let suffix = path.strip_prefix("/v1/videos/")?;
    if suffix.is_empty()
        || suffix.contains('/')
        || suffix.ends_with(":cancel")
        || suffix.ends_with(":delete")
    {
        return None;
    }
    Some(suffix)
}

pub(crate) fn extract_gemini_short_id_from_path(path: &str) -> Option<&str> {
    let operations_index = path.find("/operations/")?;
    let suffix = &path[(operations_index + "/operations/".len())..];
    if suffix.is_empty() || suffix.contains('/') || suffix.ends_with(":cancel") {
        return None;
    }
    Some(suffix)
}

pub(crate) fn extract_openai_task_id_from_cancel_path(path: &str) -> Option<&str> {
    let suffix = path.strip_prefix("/v1/videos/")?;
    suffix
        .strip_suffix("/cancel")
        .filter(|value| !value.is_empty())
}

pub(crate) fn extract_openai_task_id_from_content_path(path: &str) -> Option<&str> {
    let suffix = path.strip_prefix("/v1/videos/")?;
    suffix
        .strip_suffix("/content")
        .filter(|value| !value.is_empty())
}

pub(crate) fn extract_gemini_short_id_from_cancel_path(path: &str) -> Option<&str> {
    let operations_index = path.find("/operations/")?;
    let suffix = &path[(operations_index + "/operations/".len())..];
    let short_id = suffix.strip_suffix(":cancel")?;
    if short_id.is_empty() || short_id.contains('/') {
        return None;
    }
    Some(short_id)
}

fn resolve_local_video_registry_mutation(
    truth_source_mode: VideoTaskTruthSourceMode,
    request_path: &str,
    report_kind: &str,
) -> Option<LocalVideoTaskRegistryMutation> {
    if truth_source_mode != VideoTaskTruthSourceMode::RustAuthoritative {
        return None;
    }

    match report_kind {
        "openai_video_delete_sync_finalize" => {
            let task_id = extract_openai_task_id_from_path(request_path)?;
            Some(LocalVideoTaskRegistryMutation::OpenAiDeleted {
                task_id: task_id.to_string(),
            })
        }
        "openai_video_cancel_sync_finalize" => {
            let task_id = extract_openai_task_id_from_cancel_path(request_path)?;
            Some(LocalVideoTaskRegistryMutation::OpenAiCancelled {
                task_id: task_id.to_string(),
            })
        }
        "gemini_video_cancel_sync_finalize" => {
            let short_id = extract_gemini_short_id_from_cancel_path(request_path)?;
            Some(LocalVideoTaskRegistryMutation::GeminiCancelled {
                short_id: short_id.to_string(),
            })
        }
        _ => None,
    }
}

fn current_unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn generate_local_short_id() -> String {
    Uuid::new_v4()
        .simple()
        .to_string()
        .chars()
        .take(12)
        .collect()
}

fn parse_video_content_variant(query_string: Option<&str>) -> Option<&'static str> {
    let mut variant = "video";
    if let Some(query_string) = query_string {
        for (key, value) in url::form_urlencoded::parse(query_string.as_bytes()) {
            if key == "variant" {
                variant = match value.as_ref() {
                    "video" => "video",
                    "thumbnail" => "thumbnail",
                    "spritesheet" => "spritesheet",
                    _ => return None,
                };
            }
        }
    }
    Some(variant)
}

fn context_text(context: &Map<String, Value>, key: &str) -> Option<String> {
    context
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn context_u64(context: &Map<String, Value>, key: &str) -> Option<u64> {
    let value = context.get(key)?;
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.trim().parse().ok(),
        _ => None,
    }
}

fn request_body_text(context: &Map<String, Value>, key: &str) -> Option<String> {
    context
        .get("original_request_body")
        .and_then(Value::as_object)
        .and_then(|body| body.get(key))
        .and_then(|value| match value {
            Value::String(text) => Some(text.trim().to_string()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        })
        .filter(|value| !value.is_empty())
}

fn build_video_follow_up_report_context(
    user_id: &str,
    api_key_id: &str,
    task_id: &str,
    model_name: Option<String>,
    transport: &LocalVideoTaskTransport,
    client_api_format: &str,
    provider_api_format: &str,
) -> Value {
    let mut context = Map::new();
    context.insert("user_id".to_string(), Value::String(user_id.to_string()));
    context.insert(
        "api_key_id".to_string(),
        Value::String(api_key_id.to_string()),
    );
    context.insert("task_id".to_string(), Value::String(task_id.to_string()));
    context.insert(
        "provider_id".to_string(),
        Value::String(transport.provider_id.clone()),
    );
    context.insert(
        "endpoint_id".to_string(),
        Value::String(transport.endpoint_id.clone()),
    );
    context.insert(
        "key_id".to_string(),
        Value::String(transport.key_id.clone()),
    );
    context.insert(
        "client_api_format".to_string(),
        Value::String(client_api_format.to_string()),
    );
    context.insert(
        "provider_api_format".to_string(),
        Value::String(provider_api_format.to_string()),
    );
    if let Some(provider_name) = transport.provider_name.clone() {
        context.insert("provider_name".to_string(), Value::String(provider_name));
    }
    if let Some(model_name) = model_name.filter(|value| !value.is_empty()) {
        context.insert("model".to_string(), Value::String(model_name));
    }
    Value::Object(context)
}

fn map_openai_task_status(status: LocalVideoTaskStatus) -> &'static str {
    match status {
        LocalVideoTaskStatus::Submitted | LocalVideoTaskStatus::Queued => "queued",
        LocalVideoTaskStatus::Processing => "processing",
        LocalVideoTaskStatus::Completed => "completed",
        LocalVideoTaskStatus::Failed
        | LocalVideoTaskStatus::Cancelled
        | LocalVideoTaskStatus::Expired => "failed",
        LocalVideoTaskStatus::Deleted => "deleted",
    }
}

fn resolve_follow_up_auth(
    user_id: Option<&str>,
    api_key_id: Option<&str>,
    auth_context: Option<&GatewayControlAuthContext>,
) -> Option<(String, String)> {
    let resolved_user_id = user_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| auth_context.map(|value| value.user_id.clone()))?;
    let resolved_api_key_id = api_key_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| auth_context.map(|value| value.api_key_id.clone()))?;
    Some((resolved_user_id, resolved_api_key_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openai_video_seed_preserves_existing_local_identity() {
        let provider_body = json!({"id": "ext-video-task-123"});
        let report_context = json!({
            "local_task_id": "task-local-123",
            "local_created_at": 1712345678u64,
            "model": "sora-2",
            "original_request_body": {
                "prompt": "hello",
                "size": "1024x1024",
                "seconds": "8"
            }
        });

        let seed = LocalVideoTaskSeed::from_sync_finalize(
            "openai_video_create_sync_finalize",
            provider_body
                .as_object()
                .expect("provider body should be object"),
            report_context
                .as_object()
                .expect("report context should be object"),
            &sample_plan("https://api.openai.example/v1/videos", "openai:video"),
        )
        .expect("seed should build");

        let mut patched_context = report_context
            .as_object()
            .expect("report context should be object")
            .clone();
        seed.apply_to_report_context(&mut patched_context);
        let client_body = seed.client_body_json();

        assert_eq!(
            seed.success_report_kind(),
            "openai_video_create_sync_success"
        );
        assert_eq!(
            patched_context.get("local_task_id").and_then(Value::as_str),
            Some("task-local-123")
        );
        assert_eq!(
            patched_context
                .get("local_created_at")
                .and_then(Value::as_u64),
            Some(1712345678)
        );
        assert_eq!(
            client_body.get("id").and_then(Value::as_str),
            Some("task-local-123")
        );
        assert_eq!(
            client_body.get("created_at").and_then(Value::as_u64),
            Some(1712345678)
        );
        assert_eq!(
            client_body.get("model").and_then(Value::as_str),
            Some("sora-2")
        );
        assert_eq!(
            client_body.get("prompt").and_then(Value::as_str),
            Some("hello")
        );
    }

    #[test]
    fn openai_video_remix_seed_carries_source_task() {
        let provider_body = json!({"id": "ext-remix-task-123"});
        let report_context = json!({
            "task_id": "task-source-123",
            "original_request_body": {
                "prompt": "remix it"
            }
        });

        let seed = LocalVideoTaskSeed::from_sync_finalize(
            "openai_video_remix_sync_finalize",
            provider_body
                .as_object()
                .expect("provider body should be object"),
            report_context
                .as_object()
                .expect("report context should be object"),
            &sample_plan(
                "https://api.openai.example/v1/videos/ext-source/remix",
                "openai:video",
            ),
        )
        .expect("seed should build");
        let client_body = seed.client_body_json();

        assert_eq!(
            seed.success_report_kind(),
            "openai_video_remix_sync_success"
        );
        assert_eq!(
            client_body
                .get("remixed_from_video_id")
                .and_then(Value::as_str),
            Some("task-source-123")
        );
    }

    #[test]
    fn gemini_video_seed_generates_short_id_and_pending_operation_name() {
        let provider_body = json!({"name": "operations/ext-video-task-123"});
        let report_context = json!({
            "model": "veo-3"
        });

        let seed = LocalVideoTaskSeed::from_sync_finalize(
            "gemini_video_create_sync_finalize",
            provider_body
                .as_object()
                .expect("provider body should be object"),
            report_context
                .as_object()
                .expect("report context should be object"),
            &sample_plan(
                "https://generativelanguage.googleapis.com/v1beta/models/veo-3:predictLongRunning",
                "gemini:video",
            ),
        )
        .expect("seed should build");
        let mut patched_context = Map::new();
        seed.apply_to_report_context(&mut patched_context);
        let client_body = seed.client_body_json();
        let local_short_id = patched_context
            .get("local_short_id")
            .and_then(Value::as_str)
            .expect("local_short_id should exist");

        assert_eq!(
            seed.success_report_kind(),
            "gemini_video_create_sync_success"
        );
        assert_eq!(local_short_id.len(), 12);
        assert_eq!(
            client_body.get("name").and_then(Value::as_str),
            Some(format!("models/veo-3/operations/{local_short_id}").as_str())
        );
        assert_eq!(
            client_body.get("done").and_then(Value::as_bool),
            Some(false)
        );
    }

    #[test]
    fn python_backed_video_truth_source_requires_inline_sync_report() {
        let provider_body = json!({"id": "ext-video-task-123"});
        let report_context = json!({});

        let plan = VideoTaskTruthSourceMode::PythonSyncReport
            .prepare_sync_success(
                "openai_video_create_sync_finalize",
                provider_body
                    .as_object()
                    .expect("provider body should be object"),
                report_context
                    .as_object()
                    .expect("report context should be object"),
                &sample_plan("https://api.openai.example/v1/videos", "openai:video"),
            )
            .expect("plan should build");

        assert_eq!(plan.report_mode(), VideoTaskSyncReportMode::InlineSync);
        assert_eq!(
            plan.success_report_kind(),
            "openai_video_create_sync_success"
        );
    }

    #[test]
    fn rust_authoritative_video_truth_source_can_background_success_report() {
        let provider_body = json!({"name": "operations/ext-video-task-123"});
        let report_context = json!({
            "model": "veo-3"
        });

        let plan = VideoTaskTruthSourceMode::RustAuthoritative
            .prepare_sync_success(
                "gemini_video_create_sync_finalize",
                provider_body
                    .as_object()
                    .expect("provider body should be object"),
                report_context
                    .as_object()
                    .expect("report context should be object"),
                &sample_plan(
                    "https://generativelanguage.googleapis.com/v1beta/models/veo-3:predictLongRunning",
                    "gemini:video",
                ),
            )
            .expect("plan should build");

        assert_eq!(plan.report_mode(), VideoTaskSyncReportMode::Background);
        assert_eq!(
            plan.success_report_kind(),
            "gemini_video_create_sync_success"
        );
    }

    #[test]
    fn rust_authoritative_service_reads_openai_task_from_local_registry() {
        let service = VideoTaskService::new(VideoTaskTruthSourceMode::RustAuthoritative);
        let snapshot = LocalVideoTaskSnapshot::OpenAi(OpenAiVideoTaskSeed {
            local_task_id: "task-local-123".to_string(),
            upstream_task_id: "ext-video-task-123".to_string(),
            created_at_unix_secs: 1712345678,
            user_id: Some("user-123".to_string()),
            api_key_id: Some("key-123".to_string()),
            model: Some("sora-2".to_string()),
            prompt: Some("hello".to_string()),
            size: None,
            seconds: None,
            remixed_from_video_id: None,
            status: LocalVideoTaskStatus::Submitted,
            progress_percent: 0,
            completed_at_unix_secs: None,
            expires_at_unix_secs: None,
            error_code: None,
            error_message: None,
            video_url: None,
            transport: sample_transport("https://api.openai.example", "openai:video"),
        });
        service.record_snapshot(snapshot);

        let response = service
            .read_response(Some("openai"), "/v1/videos/task-local-123")
            .expect("read response should exist");

        assert_eq!(response.status_code, 200);
        assert_eq!(
            response.body_json.get("id").and_then(Value::as_str),
            Some("task-local-123")
        );
        assert_eq!(
            response.body_json.get("status").and_then(Value::as_str),
            Some("queued")
        );
    }

    #[test]
    fn rust_authoritative_service_applies_cancel_and_delete_mutations() {
        let service = VideoTaskService::new(VideoTaskTruthSourceMode::RustAuthoritative);
        service.record_snapshot(LocalVideoTaskSnapshot::OpenAi(OpenAiVideoTaskSeed {
            local_task_id: "task-local-123".to_string(),
            upstream_task_id: "ext-video-task-123".to_string(),
            created_at_unix_secs: 1712345678,
            user_id: Some("user-123".to_string()),
            api_key_id: Some("key-123".to_string()),
            model: Some("sora-2".to_string()),
            prompt: None,
            size: None,
            seconds: None,
            remixed_from_video_id: None,
            status: LocalVideoTaskStatus::Submitted,
            progress_percent: 0,
            completed_at_unix_secs: None,
            expires_at_unix_secs: None,
            error_code: None,
            error_message: None,
            video_url: None,
            transport: sample_transport("https://api.openai.example", "openai:video"),
        }));
        service.record_snapshot(LocalVideoTaskSnapshot::Gemini(GeminiVideoTaskSeed {
            local_short_id: "short12345678".to_string(),
            upstream_operation_name: "operations/ext-video-task-123".to_string(),
            user_id: Some("user-123".to_string()),
            api_key_id: Some("key-123".to_string()),
            model: "veo-3".to_string(),
            status: LocalVideoTaskStatus::Submitted,
            progress_percent: 0,
            error_code: None,
            error_message: None,
            metadata: json!({}),
            transport: sample_transport(
                "https://generativelanguage.googleapis.com",
                "gemini:video",
            ),
        }));

        service.apply_finalize_mutation(
            "/v1/videos/task-local-123/cancel",
            "openai_video_cancel_sync_finalize",
        );
        let cancelled_openai = service
            .read_response(Some("openai"), "/v1/videos/task-local-123")
            .expect("openai read response should exist");
        assert_eq!(cancelled_openai.status_code, 404);
        assert_eq!(
            cancelled_openai.body_json,
            json!({"detail": "Video task was cancelled"})
        );

        service.apply_finalize_mutation(
            "/v1beta/models/veo-3/operations/short12345678:cancel",
            "gemini_video_cancel_sync_finalize",
        );
        let cancelled_gemini = service
            .read_response(
                Some("gemini"),
                "/v1beta/models/veo-3/operations/short12345678",
            )
            .expect("gemini read response should exist");
        assert_eq!(cancelled_gemini.status_code, 404);
        assert_eq!(
            cancelled_gemini.body_json,
            json!({"detail": "Video task was cancelled"})
        );

        service.apply_finalize_mutation(
            "/v1/videos/task-local-123",
            "openai_video_delete_sync_finalize",
        );
        let deleted_openai = service
            .read_response(Some("openai"), "/v1/videos/task-local-123")
            .expect("deleted openai read response should exist");
        assert_eq!(deleted_openai.status_code, 404);
        assert_eq!(
            deleted_openai.body_json,
            json!({"detail": "Video task not found"})
        );
    }

    #[test]
    fn seed_captures_transport_metadata_from_execution_plan() {
        let provider_body = json!({"id": "ext-video-task-123"});
        let report_context = json!({});

        let seed = LocalVideoTaskSeed::from_sync_finalize(
            "openai_video_create_sync_finalize",
            provider_body
                .as_object()
                .expect("provider body should be object"),
            report_context
                .as_object()
                .expect("report context should be object"),
            &sample_plan("https://api.openai.example/v1/videos", "openai:video"),
        )
        .expect("seed should build");

        let LocalVideoTaskSeed::OpenAiCreate(seed) = seed else {
            panic!("seed should be openai");
        };
        assert_eq!(
            seed.transport.upstream_base_url,
            "https://api.openai.example"
        );
        assert_eq!(seed.transport.provider_id, "provider-123");
        assert_eq!(seed.transport.endpoint_id, "endpoint-123");
        assert_eq!(seed.transport.key_id, "key-123");
        assert_eq!(
            seed.transport
                .headers
                .get("authorization")
                .map(String::as_str),
            Some("Bearer upstream-key")
        );
    }

    #[test]
    fn rust_authoritative_service_builds_openai_cancel_follow_up_plan() {
        let service = VideoTaskService::new(VideoTaskTruthSourceMode::RustAuthoritative);
        service.record_snapshot(LocalVideoTaskSnapshot::OpenAi(OpenAiVideoTaskSeed {
            local_task_id: "task-local-123".to_string(),
            upstream_task_id: "ext-video-task-123".to_string(),
            created_at_unix_secs: 1712345678,
            user_id: Some("user-123".to_string()),
            api_key_id: Some("key-123".to_string()),
            model: Some("sora-2".to_string()),
            prompt: None,
            size: None,
            seconds: None,
            remixed_from_video_id: None,
            status: LocalVideoTaskStatus::Submitted,
            progress_percent: 0,
            completed_at_unix_secs: None,
            expires_at_unix_secs: None,
            error_code: None,
            error_message: None,
            video_url: None,
            transport: sample_transport("https://api.openai.example", "openai:video"),
        }));

        let follow_up = service
            .prepare_follow_up_sync_plan(
                "openai_video_cancel_sync",
                "/v1/videos/task-local-123/cancel",
                Some(&sample_auth_context()),
                "trace-openai-cancel-123",
            )
            .expect("follow-up plan should build");

        assert_eq!(follow_up.plan.method, "DELETE");
        assert_eq!(
            follow_up.plan.url,
            "https://api.openai.example/v1/videos/ext-video-task-123"
        );
        assert_eq!(follow_up.plan.provider_api_format, "openai:video");
        assert!(follow_up.plan.body.json_body.is_none());
        assert_eq!(
            follow_up.report_kind.as_deref(),
            Some("openai_video_cancel_sync_finalize")
        );
        assert_eq!(
            follow_up
                .report_context
                .as_ref()
                .and_then(Value::as_object)
                .and_then(|value| value.get("task_id"))
                .and_then(Value::as_str),
            Some("task-local-123")
        );
    }

    #[test]
    fn rust_authoritative_service_builds_openai_delete_follow_up_plan() {
        let service = VideoTaskService::new(VideoTaskTruthSourceMode::RustAuthoritative);
        service.record_snapshot(LocalVideoTaskSnapshot::OpenAi(OpenAiVideoTaskSeed {
            local_task_id: "task-local-123".to_string(),
            upstream_task_id: "ext-video-task-123".to_string(),
            created_at_unix_secs: 1712345678,
            user_id: Some("user-123".to_string()),
            api_key_id: Some("key-123".to_string()),
            model: Some("sora-2".to_string()),
            prompt: None,
            size: None,
            seconds: None,
            remixed_from_video_id: None,
            status: LocalVideoTaskStatus::Completed,
            progress_percent: 100,
            completed_at_unix_secs: Some(1712345688),
            expires_at_unix_secs: None,
            error_code: None,
            error_message: None,
            video_url: None,
            transport: sample_transport("https://api.openai.example", "openai:video"),
        }));

        let follow_up = service
            .prepare_follow_up_sync_plan(
                "openai_video_delete_sync",
                "/v1/videos/task-local-123",
                Some(&sample_auth_context()),
                "trace-openai-delete-123",
            )
            .expect("follow-up plan should build");

        assert_eq!(follow_up.plan.method, "DELETE");
        assert_eq!(
            follow_up.plan.url,
            "https://api.openai.example/v1/videos/ext-video-task-123"
        );
        assert_eq!(follow_up.plan.provider_api_format, "openai:video");
        assert!(follow_up.plan.body.json_body.is_none());
        assert_eq!(
            follow_up.report_kind.as_deref(),
            Some("openai_video_delete_sync_finalize")
        );
        assert_eq!(
            follow_up
                .report_context
                .as_ref()
                .and_then(Value::as_object)
                .and_then(|value| value.get("task_id"))
                .and_then(Value::as_str),
            Some("task-local-123")
        );
    }

    #[test]
    fn rust_authoritative_service_builds_gemini_cancel_follow_up_plan() {
        let service = VideoTaskService::new(VideoTaskTruthSourceMode::RustAuthoritative);
        service.record_snapshot(LocalVideoTaskSnapshot::Gemini(GeminiVideoTaskSeed {
            local_short_id: "localshort123".to_string(),
            upstream_operation_name: "operations/ext-video-123".to_string(),
            user_id: Some("user-123".to_string()),
            api_key_id: Some("key-123".to_string()),
            model: "veo-3".to_string(),
            status: LocalVideoTaskStatus::Submitted,
            progress_percent: 0,
            error_code: None,
            error_message: None,
            metadata: json!({}),
            transport: sample_transport(
                "https://generativelanguage.googleapis.com",
                "gemini:video",
            ),
        }));

        let follow_up = service
            .prepare_follow_up_sync_plan(
                "gemini_video_cancel_sync",
                "/v1beta/models/veo-3/operations/localshort123:cancel",
                Some(&sample_auth_context()),
                "trace-gemini-cancel-123",
            )
            .expect("follow-up plan should build");

        assert_eq!(follow_up.plan.method, "POST");
        assert_eq!(
            follow_up.plan.url,
            "https://generativelanguage.googleapis.com/v1beta/models/veo-3/operations/ext-video-123:cancel"
        );
        assert_eq!(follow_up.plan.provider_api_format, "gemini:video");
        assert_eq!(follow_up.plan.body.json_body, Some(json!({})));
        assert_eq!(
            follow_up.report_kind.as_deref(),
            Some("gemini_video_cancel_sync_finalize")
        );
        assert_eq!(
            follow_up
                .report_context
                .as_ref()
                .and_then(Value::as_object)
                .and_then(|value| value.get("task_id"))
                .and_then(Value::as_str),
            Some("localshort123")
        );
    }

    #[test]
    fn rust_authoritative_service_builds_openai_read_refresh_plan() {
        let service = VideoTaskService::new(VideoTaskTruthSourceMode::RustAuthoritative);
        service.record_snapshot(LocalVideoTaskSnapshot::OpenAi(OpenAiVideoTaskSeed {
            local_task_id: "task-local-123".to_string(),
            upstream_task_id: "ext-video-task-123".to_string(),
            created_at_unix_secs: 1712345678,
            user_id: Some("user-123".to_string()),
            api_key_id: Some("key-123".to_string()),
            model: Some("sora-2".to_string()),
            prompt: None,
            size: None,
            seconds: None,
            remixed_from_video_id: None,
            status: LocalVideoTaskStatus::Submitted,
            progress_percent: 0,
            completed_at_unix_secs: None,
            expires_at_unix_secs: None,
            error_code: None,
            error_message: None,
            video_url: None,
            transport: sample_transport("https://api.openai.example", "openai:video"),
        }));

        let refresh = service
            .prepare_read_refresh_sync_plan(
                Some("openai"),
                "/v1/videos/task-local-123",
                "trace-openai-read-123",
            )
            .expect("read refresh plan should build");

        assert_eq!(refresh.plan.method, "GET");
        assert_eq!(
            refresh.plan.url,
            "https://api.openai.example/v1/videos/ext-video-task-123"
        );
        assert!(refresh.plan.body.json_body.is_none());
    }

    #[test]
    fn rust_authoritative_service_builds_gemini_read_refresh_plan() {
        let service = VideoTaskService::new(VideoTaskTruthSourceMode::RustAuthoritative);
        service.record_snapshot(LocalVideoTaskSnapshot::Gemini(GeminiVideoTaskSeed {
            local_short_id: "localshort123".to_string(),
            upstream_operation_name: "operations/ext-video-123".to_string(),
            user_id: Some("user-123".to_string()),
            api_key_id: Some("key-123".to_string()),
            model: "veo-3".to_string(),
            status: LocalVideoTaskStatus::Submitted,
            progress_percent: 0,
            error_code: None,
            error_message: None,
            metadata: json!({}),
            transport: sample_transport(
                "https://generativelanguage.googleapis.com",
                "gemini:video",
            ),
        }));

        let refresh = service
            .prepare_read_refresh_sync_plan(
                Some("gemini"),
                "/v1beta/models/veo-3/operations/localshort123",
                "trace-gemini-read-123",
            )
            .expect("read refresh plan should build");

        assert_eq!(refresh.plan.method, "GET");
        assert_eq!(
            refresh.plan.url,
            "https://generativelanguage.googleapis.com/v1beta/models/veo-3/operations/ext-video-123"
        );
        assert!(refresh.plan.body.json_body.is_none());
    }

    #[test]
    fn rust_authoritative_service_builds_poll_refresh_batch_for_active_tasks_only() {
        let service = VideoTaskService::new(VideoTaskTruthSourceMode::RustAuthoritative);
        service.record_snapshot(LocalVideoTaskSnapshot::OpenAi(OpenAiVideoTaskSeed {
            local_task_id: "task-active-123".to_string(),
            upstream_task_id: "ext-video-task-123".to_string(),
            created_at_unix_secs: 1712345678,
            user_id: Some("user-123".to_string()),
            api_key_id: Some("key-123".to_string()),
            model: Some("sora-2".to_string()),
            prompt: None,
            size: None,
            seconds: None,
            remixed_from_video_id: None,
            status: LocalVideoTaskStatus::Processing,
            progress_percent: 42,
            completed_at_unix_secs: None,
            expires_at_unix_secs: None,
            error_code: None,
            error_message: None,
            video_url: None,
            transport: sample_transport("https://api.openai.example", "openai:video"),
        }));
        service.record_snapshot(LocalVideoTaskSnapshot::OpenAi(OpenAiVideoTaskSeed {
            local_task_id: "task-completed-123".to_string(),
            upstream_task_id: "ext-video-task-999".to_string(),
            created_at_unix_secs: 1712345678,
            user_id: Some("user-123".to_string()),
            api_key_id: Some("key-123".to_string()),
            model: Some("sora-2".to_string()),
            prompt: None,
            size: None,
            seconds: None,
            remixed_from_video_id: None,
            status: LocalVideoTaskStatus::Completed,
            progress_percent: 100,
            completed_at_unix_secs: Some(1712345688),
            expires_at_unix_secs: None,
            error_code: None,
            error_message: None,
            video_url: Some("https://cdn.example.com/ext-video-task-999.mp4".to_string()),
            transport: sample_transport("https://api.openai.example", "openai:video"),
        }));

        let batch = service.prepare_poll_refresh_batch(10, "trace-poller");

        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].plan.method, "GET");
        assert_eq!(
            batch[0].plan.url,
            "https://api.openai.example/v1/videos/ext-video-task-123"
        );
    }

    #[test]
    fn file_video_task_store_persists_snapshots_across_service_rebuilds() {
        let store_path =
            std::env::temp_dir().join(format!("aether-video-task-store-{}.json", Uuid::new_v4()));
        let service = VideoTaskService::with_file_store(
            VideoTaskTruthSourceMode::RustAuthoritative,
            &store_path,
        )
        .expect("file-backed service should build");
        service.record_snapshot(LocalVideoTaskSnapshot::OpenAi(OpenAiVideoTaskSeed {
            local_task_id: "task-file-123".to_string(),
            upstream_task_id: "ext-video-task-123".to_string(),
            created_at_unix_secs: 1712345678,
            user_id: Some("user-123".to_string()),
            api_key_id: Some("key-123".to_string()),
            model: Some("sora-2".to_string()),
            prompt: Some("hello".to_string()),
            size: None,
            seconds: None,
            remixed_from_video_id: None,
            status: LocalVideoTaskStatus::Submitted,
            progress_percent: 0,
            completed_at_unix_secs: None,
            expires_at_unix_secs: None,
            error_code: None,
            error_message: None,
            video_url: None,
            transport: sample_transport("https://api.openai.example", "openai:video"),
        }));
        drop(service);

        let reopened = VideoTaskService::with_file_store(
            VideoTaskTruthSourceMode::RustAuthoritative,
            &store_path,
        )
        .expect("reopened file-backed service should build");
        let response = reopened
            .read_response(Some("openai"), "/v1/videos/task-file-123")
            .expect("persisted read response should exist");
        assert_eq!(response.status_code, 200);
        assert_eq!(
            response.body_json.get("id").and_then(Value::as_str),
            Some("task-file-123")
        );

        let _ = std::fs::remove_file(store_path);
    }

    #[test]
    fn rust_authoritative_service_projects_openai_status_into_local_read_response() {
        let service = VideoTaskService::new(VideoTaskTruthSourceMode::RustAuthoritative);
        service.record_snapshot(LocalVideoTaskSnapshot::OpenAi(OpenAiVideoTaskSeed {
            local_task_id: "task-local-123".to_string(),
            upstream_task_id: "ext-video-task-123".to_string(),
            created_at_unix_secs: 1712345678,
            user_id: Some("user-123".to_string()),
            api_key_id: Some("key-123".to_string()),
            model: Some("sora-2".to_string()),
            prompt: Some("hello".to_string()),
            size: None,
            seconds: None,
            remixed_from_video_id: None,
            status: LocalVideoTaskStatus::Submitted,
            progress_percent: 0,
            completed_at_unix_secs: None,
            expires_at_unix_secs: None,
            error_code: None,
            error_message: None,
            video_url: None,
            transport: sample_transport("https://api.openai.example", "openai:video"),
        }));

        assert!(service.project_openai_task_response(
            "task-local-123",
            json!({
                "status": "processing",
                "progress": 42
            })
            .as_object()
            .expect("provider body should be object"),
        ));
        let processing = service
            .read_response(Some("openai"), "/v1/videos/task-local-123")
            .expect("processing read response should exist");
        assert_eq!(processing.status_code, 200);
        assert_eq!(
            processing.body_json.get("status"),
            Some(&json!("processing"))
        );
        assert_eq!(processing.body_json.get("progress"), Some(&json!(42)));

        assert!(service.project_openai_task_response(
            "task-local-123",
            json!({
                "status": "failed",
                "progress": 100,
                "completed_at": 1712345688u64,
                "error": {
                    "code": "upstream_failed",
                    "message": "provider failed"
                }
            })
            .as_object()
            .expect("provider body should be object"),
        ));
        let failed = service
            .read_response(Some("openai"), "/v1/videos/task-local-123")
            .expect("failed read response should exist");
        assert_eq!(failed.status_code, 200);
        assert_eq!(failed.body_json.get("status"), Some(&json!("failed")));
        assert_eq!(
            failed.body_json.get("completed_at"),
            Some(&json!(1712345688u64))
        );
        assert_eq!(
            failed
                .body_json
                .get("error")
                .and_then(Value::as_object)
                .and_then(|value| value.get("code")),
            Some(&json!("upstream_failed"))
        );
    }

    #[test]
    fn rust_authoritative_service_builds_openai_content_stream_plan_from_direct_video_url() {
        let service = VideoTaskService::new(VideoTaskTruthSourceMode::RustAuthoritative);
        service.record_snapshot(LocalVideoTaskSnapshot::OpenAi(OpenAiVideoTaskSeed {
            local_task_id: "task-local-123".to_string(),
            upstream_task_id: "ext-video-task-123".to_string(),
            created_at_unix_secs: 1712345678,
            user_id: Some("user-123".to_string()),
            api_key_id: Some("key-123".to_string()),
            model: Some("sora-2".to_string()),
            prompt: None,
            size: None,
            seconds: None,
            remixed_from_video_id: None,
            status: LocalVideoTaskStatus::Completed,
            progress_percent: 100,
            completed_at_unix_secs: Some(1712345688),
            expires_at_unix_secs: None,
            error_code: None,
            error_message: None,
            video_url: Some("https://cdn.example.com/ext-video-task-123.mp4".to_string()),
            transport: sample_transport("https://api.openai.example", "openai:video"),
        }));

        let action = service
            .prepare_openai_content_stream_action(
                "/v1/videos/task-local-123/content",
                Some("variant=video"),
                "trace-openai-content-123",
            )
            .expect("content action should exist");

        let LocalVideoTaskContentAction::StreamPlan(plan) = action else {
            panic!("content action should be stream plan");
        };
        assert_eq!(plan.method, "GET");
        assert_eq!(plan.url, "https://cdn.example.com/ext-video-task-123.mp4");
        assert!(plan.headers.is_empty());
    }

    #[test]
    fn rust_authoritative_service_returns_processing_content_response_for_pending_openai_task() {
        let service = VideoTaskService::new(VideoTaskTruthSourceMode::RustAuthoritative);
        service.record_snapshot(LocalVideoTaskSnapshot::OpenAi(OpenAiVideoTaskSeed {
            local_task_id: "task-local-123".to_string(),
            upstream_task_id: "ext-video-task-123".to_string(),
            created_at_unix_secs: 1712345678,
            user_id: Some("user-123".to_string()),
            api_key_id: Some("key-123".to_string()),
            model: Some("sora-2".to_string()),
            prompt: None,
            size: None,
            seconds: None,
            remixed_from_video_id: None,
            status: LocalVideoTaskStatus::Processing,
            progress_percent: 42,
            completed_at_unix_secs: None,
            expires_at_unix_secs: None,
            error_code: None,
            error_message: None,
            video_url: None,
            transport: sample_transport("https://api.openai.example", "openai:video"),
        }));

        let action = service
            .prepare_openai_content_stream_action(
                "/v1/videos/task-local-123/content",
                Some("variant=video"),
                "trace-openai-content-processing-123",
            )
            .expect("content action should exist");

        let LocalVideoTaskContentAction::Immediate {
            status_code,
            body_json,
        } = action
        else {
            panic!("content action should be immediate response");
        };
        assert_eq!(status_code, 202);
        assert_eq!(
            body_json,
            json!({"detail": "Video is still processing (status: processing)"})
        );
    }

    #[test]
    fn rust_authoritative_service_projects_gemini_status_into_local_read_response() {
        let service = VideoTaskService::new(VideoTaskTruthSourceMode::RustAuthoritative);
        service.record_snapshot(LocalVideoTaskSnapshot::Gemini(GeminiVideoTaskSeed {
            local_short_id: "localshort123".to_string(),
            upstream_operation_name: "operations/ext-video-123".to_string(),
            user_id: Some("user-123".to_string()),
            api_key_id: Some("key-123".to_string()),
            model: "veo-3".to_string(),
            status: LocalVideoTaskStatus::Submitted,
            progress_percent: 0,
            error_code: None,
            error_message: None,
            metadata: json!({}),
            transport: sample_transport(
                "https://generativelanguage.googleapis.com",
                "gemini:video",
            ),
        }));

        assert!(service.project_gemini_task_response(
            "localshort123",
            json!({
                "done": false,
                "metadata": {
                    "state": "PROCESSING"
                }
            })
            .as_object()
            .expect("provider body should be object"),
        ));
        let processing = service
            .read_response(
                Some("gemini"),
                "/v1beta/models/veo-3/operations/localshort123",
            )
            .expect("processing read response should exist");
        assert_eq!(processing.status_code, 200);
        assert_eq!(processing.body_json.get("done"), Some(&json!(false)));
        assert_eq!(
            processing.body_json.get("metadata"),
            Some(&json!({"state": "PROCESSING"}))
        );

        assert!(service.project_gemini_task_response(
            "localshort123",
            json!({
                "done": true,
                "response": {
                    "generateVideoResponse": {
                        "generatedSamples": [
                            {"video": {"uri": "https://example.invalid/video.mp4"}}
                        ]
                    }
                }
            })
            .as_object()
            .expect("provider body should be object"),
        ));
        let completed = service
            .read_response(
                Some("gemini"),
                "/v1beta/models/veo-3/operations/localshort123",
            )
            .expect("completed read response should exist");
        assert_eq!(completed.status_code, 200);
        assert_eq!(completed.body_json.get("done"), Some(&json!(true)));
        assert_eq!(
            completed
                .body_json
                .get("response")
                .and_then(|value| value.get("generateVideoResponse"))
                .and_then(|value| value.get("generatedSamples"))
                .and_then(Value::as_array)
                .and_then(|value| value.first())
                .and_then(|value| value.get("video"))
                .and_then(|value| value.get("uri")),
            Some(&json!("/v1beta/files/aev_localshort123:download?alt=media"))
        );
    }

    fn sample_transport(base_url: &str, provider_api_format: &str) -> LocalVideoTaskTransport {
        let url = match provider_api_format {
            "openai:video" => format!("{base_url}/v1/videos"),
            "gemini:video" => {
                format!("{base_url}/v1beta/models/veo-3:predictLongRunning")
            }
            _ => panic!("unsupported provider api format"),
        };
        LocalVideoTaskTransport::from_plan(&sample_plan(&url, provider_api_format))
            .expect("transport should build")
    }

    fn sample_auth_context() -> GatewayControlAuthContext {
        GatewayControlAuthContext {
            user_id: "user-123".to_string(),
            api_key_id: "key-123".to_string(),
            balance_remaining: None,
            access_allowed: true,
        }
    }

    fn sample_plan(url: &str, provider_api_format: &str) -> ExecutionPlan {
        ExecutionPlan {
            request_id: "req-123".to_string(),
            candidate_id: None,
            provider_name: Some(
                provider_api_format
                    .split(':')
                    .next()
                    .expect("provider name should exist")
                    .to_string(),
            ),
            provider_id: "provider-123".to_string(),
            endpoint_id: "endpoint-123".to_string(),
            key_id: "key-123".to_string(),
            method: "POST".to_string(),
            url: url.to_string(),
            headers: BTreeMap::from([(
                "authorization".to_string(),
                "Bearer upstream-key".to_string(),
            )]),
            content_type: Some("application/json".to_string()),
            content_encoding: None,
            body: RequestBody::from_json(json!({})),
            stream: false,
            client_api_format: provider_api_format.to_string(),
            provider_api_format: provider_api_format.to_string(),
            model_name: Some(
                if provider_api_format == "gemini:video" {
                    "veo-3"
                } else {
                    "sora-2"
                }
                .to_string(),
            ),
            proxy: Some(ProxySnapshot {
                enabled: Some(false),
                mode: Some("direct".to_string()),
                node_id: None,
                label: None,
                url: None,
                extra: None,
            }),
            tls_profile: Some("chrome".to_string()),
            timeouts: Some(ExecutionTimeouts {
                connect_ms: Some(10_000),
                read_ms: Some(30_000),
                first_byte_ms: None,
                write_ms: Some(30_000),
                pool_ms: Some(10_000),
                total_ms: Some(300_000),
            }),
        }
    }
}
