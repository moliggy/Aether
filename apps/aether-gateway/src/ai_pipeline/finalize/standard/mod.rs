//! Standard finalize surface for standard contract sync/stream compilation.

use serde_json::Value;

mod claude;
mod gemini;
mod openai;
#[path = "stream_core/mod.rs"]
mod stream;

pub(crate) use crate::ai_pipeline::conversion::response::{
    build_openai_cli_response, convert_openai_chat_response_to_openai_cli,
};
pub(crate) use claude::*;
pub(crate) use gemini::*;
pub(crate) use openai::*;
pub(crate) use stream::*;

pub(crate) fn aggregate_standard_chat_stream_sync_response(
    body: &[u8],
    provider_api_format: &str,
) -> Option<Value> {
    match provider_api_format.trim().to_ascii_lowercase().as_str() {
        "openai:chat" => aggregate_openai_chat_stream_sync_response(body),
        "openai:cli" | "openai:compact" => aggregate_openai_cli_stream_sync_response(body),
        "claude:chat" | "claude:cli" => aggregate_claude_stream_sync_response(body),
        "gemini:chat" | "gemini:cli" => aggregate_gemini_stream_sync_response(body),
        _ => None,
    }
}

pub(crate) fn convert_standard_chat_response(
    body_json: &Value,
    provider_api_format: &str,
    client_api_format: &str,
    report_context: &Value,
) -> Option<Value> {
    let canonical = match provider_api_format.trim().to_ascii_lowercase().as_str() {
        "openai:chat" => body_json.clone(),
        "openai:cli" | "openai:compact" => {
            convert_openai_cli_response_to_openai_chat(body_json, report_context)?
        }
        "claude:chat" | "claude:cli" => {
            convert_claude_chat_response_to_openai_chat(body_json, report_context)?
        }
        "gemini:chat" | "gemini:cli" => {
            convert_gemini_chat_response_to_openai_chat(body_json, report_context)?
        }
        _ => return None,
    };

    match client_api_format.trim().to_ascii_lowercase().as_str() {
        "openai:chat" => Some(canonical),
        "claude:chat" => convert_openai_chat_response_to_claude_chat(&canonical, report_context),
        "gemini:chat" => convert_openai_chat_response_to_gemini_chat(&canonical, report_context),
        _ => None,
    }
}

pub(crate) fn aggregate_standard_cli_stream_sync_response(
    body: &[u8],
    provider_api_format: &str,
) -> Option<Value> {
    aggregate_standard_chat_stream_sync_response(body, provider_api_format)
}

pub(crate) fn convert_standard_cli_response(
    body_json: &Value,
    provider_api_format: &str,
    client_api_format: &str,
    report_context: &Value,
) -> Option<Value> {
    let canonical = match provider_api_format.trim().to_ascii_lowercase().as_str() {
        "openai:cli" | "openai:compact" => {
            convert_openai_cli_response_to_openai_chat(body_json, report_context)?
        }
        _ => convert_standard_chat_response(
            body_json,
            provider_api_format,
            "openai:chat",
            report_context,
        )?,
    };

    match client_api_format.trim().to_ascii_lowercase().as_str() {
        "openai:cli" => {
            convert_openai_chat_response_to_openai_cli(&canonical, report_context, false)
        }
        "openai:compact" => {
            convert_openai_chat_response_to_openai_cli(&canonical, report_context, true)
        }
        "claude:cli" => convert_openai_chat_response_to_claude_chat(&canonical, report_context),
        "gemini:cli" => convert_openai_chat_response_to_gemini_chat(&canonical, report_context),
        _ => None,
    }
}
