#[path = "chat/claude.rs"]
mod claude;
#[path = "chat/gemini.rs"]
mod gemini;
#[path = "chat/openai.rs"]
mod openai;

pub(super) use claude::maybe_build_local_claude_stream_sync_response;
pub(crate) use claude::{
    aggregate_claude_stream_sync_response, convert_claude_chat_response_to_openai_chat,
};
pub(super) use gemini::maybe_build_local_gemini_stream_sync_response;
pub(crate) use gemini::{
    aggregate_gemini_stream_sync_response, convert_gemini_chat_response_to_openai_chat,
};
#[cfg(test)]
pub(crate) use openai::aggregate_openai_chat_stream_sync_response;
pub(super) use openai::{
    maybe_build_local_openai_chat_cross_format_stream_sync_response,
    maybe_build_local_openai_chat_cross_format_sync_response,
    maybe_build_local_openai_chat_stream_sync_response,
};
