#[path = "cli/claude.rs"]
mod claude;
#[path = "cli/gemini.rs"]
mod gemini;
#[path = "cli/openai.rs"]
mod openai;

pub(crate) use claude::convert_claude_cli_response_to_openai_cli;
pub(super) use claude::maybe_build_local_claude_cli_stream_sync_response;
pub(crate) use gemini::convert_gemini_cli_response_to_openai_cli;
pub(super) use gemini::maybe_build_local_gemini_cli_stream_sync_response;
#[cfg(test)]
pub(crate) use openai::aggregate_openai_cli_stream_sync_response;
pub(super) use openai::{
    maybe_build_local_openai_cli_cross_format_stream_sync_response,
    maybe_build_local_openai_cli_cross_format_sync_response,
    maybe_build_local_openai_cli_stream_sync_response,
};
