mod chat;
mod cli;

pub(crate) use chat::{
    aggregate_openai_chat_stream_sync_response, convert_openai_cli_response_to_openai_chat,
    maybe_build_local_openai_chat_cross_format_stream_sync_response,
    maybe_build_local_openai_chat_cross_format_sync_response,
    maybe_build_local_openai_chat_stream_sync_response,
    maybe_build_local_openai_chat_sync_response,
};
pub(crate) use cli::{
    aggregate_openai_cli_stream_sync_response, build_openai_cli_response,
    maybe_build_local_openai_cli_cross_format_stream_sync_response,
    maybe_build_local_openai_cli_cross_format_sync_response,
    maybe_build_local_openai_cli_stream_sync_response,
};
