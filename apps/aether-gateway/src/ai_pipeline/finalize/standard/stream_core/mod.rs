//! Standard finalize streaming conversion helpers.

use serde_json::Value;

use crate::GatewayError;

use super::claude::stream::{ClaudeClientEmitter, ClaudeProviderState};
use super::gemini::stream::{GeminiClientEmitter, GeminiProviderState};
use super::openai::stream::{
    OpenAIChatClientEmitter, OpenAIChatProviderState, OpenAICliClientEmitter,
    OpenAICliProviderState,
};

pub(crate) mod common;
mod orchestrator;

use common::CanonicalStreamFrame;

pub(crate) enum ProviderStreamParser {
    OpenAIChat(OpenAIChatProviderState),
    OpenAICli(OpenAICliProviderState),
    Claude(ClaudeProviderState),
    Gemini(GeminiProviderState),
}

impl ProviderStreamParser {
    pub(crate) fn for_api_format(provider_api_format: &str) -> Option<Self> {
        Some(match provider_api_format {
            "openai:chat" => Self::OpenAIChat(OpenAIChatProviderState::default()),
            "openai:cli" | "openai:compact" => Self::OpenAICli(OpenAICliProviderState::default()),
            "claude:chat" | "claude:cli" => Self::Claude(ClaudeProviderState::default()),
            "gemini:chat" | "gemini:cli" => Self::Gemini(GeminiProviderState::default()),
            _ => return None,
        })
    }

    pub(crate) fn push_line(
        &mut self,
        report_context: &Value,
        line: Vec<u8>,
    ) -> Result<Vec<CanonicalStreamFrame>, GatewayError> {
        match self {
            ProviderStreamParser::OpenAIChat(state) => state.push_line(report_context, line),
            ProviderStreamParser::OpenAICli(state) => state.push_line(report_context, line),
            ProviderStreamParser::Claude(state) => state.push_line(report_context, line),
            ProviderStreamParser::Gemini(state) => state.push_line(report_context, line),
        }
    }

    pub(crate) fn finish(
        &mut self,
        report_context: &Value,
    ) -> Result<Vec<CanonicalStreamFrame>, GatewayError> {
        match self {
            ProviderStreamParser::OpenAIChat(state) => state.finish(report_context),
            ProviderStreamParser::OpenAICli(state) => state.finish(report_context),
            ProviderStreamParser::Claude(state) => state.finish(report_context),
            ProviderStreamParser::Gemini(state) => state.finish(report_context),
        }
    }
}

pub(crate) enum ClientStreamEmitter {
    OpenAIChat(OpenAIChatClientEmitter),
    OpenAICli(OpenAICliClientEmitter),
    Claude(ClaudeClientEmitter),
    Gemini(GeminiClientEmitter),
}

impl ClientStreamEmitter {
    pub(crate) fn for_api_format(client_api_format: &str) -> Option<Self> {
        Some(match client_api_format {
            "openai:chat" => Self::OpenAIChat(OpenAIChatClientEmitter::default()),
            "openai:cli" | "openai:compact" => Self::OpenAICli(OpenAICliClientEmitter::default()),
            "claude:chat" | "claude:cli" => Self::Claude(ClaudeClientEmitter::default()),
            "gemini:chat" | "gemini:cli" => Self::Gemini(GeminiClientEmitter::default()),
            _ => return None,
        })
    }

    pub(crate) fn emit(&mut self, frame: CanonicalStreamFrame) -> Result<Vec<u8>, GatewayError> {
        match self {
            ClientStreamEmitter::OpenAIChat(state) => state.emit(frame),
            ClientStreamEmitter::OpenAICli(state) => state.emit(frame),
            ClientStreamEmitter::Claude(state) => state.emit(frame),
            ClientStreamEmitter::Gemini(state) => state.emit(frame),
        }
    }

    pub(crate) fn finish(&mut self) -> Result<Vec<u8>, GatewayError> {
        match self {
            ClientStreamEmitter::OpenAIChat(state) => state.finish(),
            ClientStreamEmitter::OpenAICli(state) => state.finish(),
            ClientStreamEmitter::Claude(state) => state.finish(),
            ClientStreamEmitter::Gemini(state) => state.finish(),
        }
    }
}

pub(crate) use orchestrator::StreamingStandardConversionState;
