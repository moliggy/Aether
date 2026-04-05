use std::collections::BTreeMap;

use aether_contracts::ExecutionPlan;

pub(crate) use crate::ai_pipeline::contracts::generic_decision_missing_exact_provider_request;
use crate::{GatewayControlSyncDecisionResponse, GatewayError};

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

#[path = "standard/gemini/plan_builders.rs"]
mod gemini_builders;
#[path = "standard/openai/plan_builders.rs"]
mod openai_builders;
#[path = "passthrough/plan_builders.rs"]
mod passthrough_builders;
#[path = "standard/plan_builders.rs"]
mod standard_builders;

pub(crate) use gemini_builders::{
    build_gemini_stream_plan_from_decision, build_gemini_sync_plan_from_decision,
};
pub(crate) use openai_builders::{
    build_openai_chat_stream_plan_from_decision, build_openai_chat_sync_plan_from_decision,
    build_openai_cli_stream_plan_from_decision, build_openai_cli_sync_plan_from_decision,
};
pub(crate) use passthrough_builders::{
    build_passthrough_stream_plan_from_decision, build_passthrough_sync_plan_from_decision,
};
pub(crate) use standard_builders::{
    build_standard_stream_plan_from_decision, build_standard_sync_plan_from_decision,
};

pub(super) fn augment_sync_report_context(
    report_context: Option<serde_json::Value>,
    provider_request_headers: &BTreeMap<String, String>,
    provider_request_body: &serde_json::Value,
) -> Result<Option<serde_json::Value>, GatewayError> {
    let mut report_context = match report_context {
        Some(serde_json::Value::Object(map)) => map,
        Some(_) => serde_json::Map::new(),
        None => serde_json::Map::new(),
    };

    report_context.insert(
        "provider_request_headers".to_string(),
        serde_json::to_value(provider_request_headers)
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
    );
    report_context.insert(
        "provider_request_body".to_string(),
        provider_request_body.clone(),
    );

    Ok(Some(serde_json::Value::Object(report_context)))
}
