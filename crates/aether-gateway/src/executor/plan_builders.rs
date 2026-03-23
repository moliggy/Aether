use super::*;

pub(crate) struct LocalSyncPlanAndReport {
    pub(crate) plan: ExecutionPlan,
    pub(crate) report_kind: Option<String>,
    pub(crate) report_context: Option<serde_json::Value>,
}

pub(crate) struct LocalStreamPlanAndReport {
    pub(crate) plan: ExecutionPlan,
    pub(crate) report_kind: Option<String>,
    pub(crate) report_context: Option<serde_json::Value>,
}

#[path = "plan_builders/shared.rs"]
mod shared;
#[path = "plan_builders/stream.rs"]
mod stream;
#[path = "plan_builders/sync.rs"]
mod sync;

pub(super) use shared::{
    build_direct_plan_bypass_cache_key, is_matching_stream_request, mark_direct_plan_bypass,
    resolve_direct_executor_stream_plan_kind, resolve_direct_executor_sync_plan_kind,
    should_skip_direct_plan,
};
pub(crate) use stream::{
    build_gemini_stream_plan_from_decision, build_openai_cli_stream_plan_from_decision,
    build_standard_stream_plan_from_decision,
};
pub(super) use stream::{
    build_openai_chat_stream_plan_from_decision, build_passthrough_stream_plan_from_decision,
};
pub(super) use sync::build_openai_chat_sync_plan_from_decision;
pub(crate) use sync::{
    build_gemini_sync_plan_from_decision, build_openai_cli_sync_plan_from_decision,
    build_passthrough_sync_plan_from_decision, build_standard_sync_plan_from_decision,
};
