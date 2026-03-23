use std::collections::BTreeMap;

use axum::body::Body;
use axum::http::Response;
use serde_json::{json, Map, Value};

use crate::gateway::{build_client_response_from_parts, GatewayControlDecision, GatewayError};

use super::executor::GatewaySyncReportRequest;

#[path = "local_finalize/chat.rs"]
mod chat;
#[path = "local_finalize/cli.rs"]
mod cli;
#[path = "local_finalize/common.rs"]
mod common;

use chat::{
    maybe_build_local_claude_stream_sync_response, maybe_build_local_gemini_stream_sync_response,
    maybe_build_local_openai_chat_cross_format_stream_sync_response,
    maybe_build_local_openai_chat_cross_format_sync_response,
    maybe_build_local_openai_chat_stream_sync_response,
};
use cli::{
    maybe_build_local_claude_cli_stream_sync_response,
    maybe_build_local_gemini_cli_stream_sync_response,
    maybe_build_local_openai_cli_cross_format_stream_sync_response,
    maybe_build_local_openai_cli_cross_format_sync_response,
    maybe_build_local_openai_cli_stream_sync_response,
};
use common::LocalCoreSyncFinalizeOutcome;

pub(super) use chat::{
    aggregate_claude_stream_sync_response, aggregate_gemini_stream_sync_response,
};
pub(super) use cli::{
    convert_claude_cli_response_to_openai_cli, convert_gemini_cli_response_to_openai_cli,
};
pub(crate) fn maybe_build_local_core_sync_finalize_response(
    trace_id: &str,
    decision: &GatewayControlDecision,
    payload: &GatewaySyncReportRequest,
) -> Result<Option<LocalCoreSyncFinalizeOutcome>, GatewayError> {
    if let Some(response) =
        maybe_build_local_openai_chat_stream_sync_response(trace_id, decision, payload)?
    {
        return Ok(Some(response));
    }
    if let Some(response) = maybe_build_local_openai_chat_cross_format_stream_sync_response(
        trace_id, decision, payload,
    )? {
        return Ok(Some(response));
    }
    if let Some(response) =
        maybe_build_local_openai_cli_stream_sync_response(trace_id, decision, payload)?
    {
        return Ok(Some(response));
    }
    if let Some(response) =
        maybe_build_local_openai_cli_cross_format_stream_sync_response(trace_id, decision, payload)?
    {
        return Ok(Some(response));
    }
    if let Some(response) =
        maybe_build_local_claude_cli_stream_sync_response(trace_id, decision, payload)?
    {
        return Ok(Some(response));
    }
    if let Some(response) =
        maybe_build_local_gemini_cli_stream_sync_response(trace_id, decision, payload)?
    {
        return Ok(Some(response));
    }
    if let Some(response) =
        maybe_build_local_claude_stream_sync_response(trace_id, decision, payload)?
    {
        return Ok(Some(response));
    }
    if let Some(response) =
        maybe_build_local_gemini_stream_sync_response(trace_id, decision, payload)?
    {
        return Ok(Some(response));
    }
    if let Some(response) =
        maybe_build_local_openai_chat_cross_format_sync_response(trace_id, decision, payload)?
    {
        return Ok(Some(response));
    }
    if let Some(response) =
        maybe_build_local_openai_cli_cross_format_sync_response(trace_id, decision, payload)?
    {
        return Ok(Some(response));
    }
    Ok(None)
}

#[cfg(test)]
#[path = "local_finalize/tests.rs"]
mod tests;
