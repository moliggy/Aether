use serde_json::Value;

use crate::formats::openai::image::stream::OpenAiImageStreamState;
use crate::formats::shared::model_directives::model_directive_display_model_from_report_context;
use crate::formats::shared::stream_core::StreamingStandardFormatMatrix;
use crate::formats::shared::AiSurfaceFinalizeError;
use crate::provider_compat::kiro_stream::KiroToClaudeCliStreamState;
use crate::provider_compat::private_envelope::transform_provider_private_stream_line;
use crate::provider_compat::surfaces::{
    provider_adaptation_should_unwrap_stream_envelope, KIRO_ENVELOPE_NAME,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalizeStreamRewriteMode {
    EnvelopeUnwrap,
    ModelDirectiveDisplay,
    OpenAiImage,
    Standard,
    KiroToClaudeCli,
    KiroToClaudeCliThenStandard,
}

pub fn resolve_finalize_stream_rewrite_mode(
    report_context: &Value,
) -> Option<FinalizeStreamRewriteMode> {
    let needs_conversion = report_context
        .get("needs_conversion")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let envelope_name = report_context
        .get("envelope_name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();
    let client_api_format = report_context
        .get("client_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();

    if needs_conversion
        && envelope_name.eq_ignore_ascii_case(KIRO_ENVELOPE_NAME)
        && provider_api_format == "claude:messages"
    {
        return supports_standard_stream_rewrite(
            provider_api_format.as_str(),
            client_api_format.as_str(),
        )
        .then_some(FinalizeStreamRewriteMode::KiroToClaudeCliThenStandard);
    }

    if needs_conversion {
        return supports_standard_stream_rewrite(
            provider_api_format.as_str(),
            client_api_format.as_str(),
        )
        .then_some(FinalizeStreamRewriteMode::Standard);
    }

    if provider_api_format == "openai:image" && client_api_format == "openai:image" {
        return Some(FinalizeStreamRewriteMode::OpenAiImage);
    }

    if envelope_name.eq_ignore_ascii_case(KIRO_ENVELOPE_NAME) {
        return (provider_api_format == "claude:messages"
            && client_api_format == "claude:messages")
            .then_some(FinalizeStreamRewriteMode::KiroToClaudeCli);
    }

    if model_directive_display_model_from_report_context(report_context).is_some()
        && provider_api_format == client_api_format
        && is_standard_provider_api_format(provider_api_format.as_str())
        && !provider_adaptation_should_unwrap_stream_envelope(
            envelope_name.as_str(),
            provider_api_format.as_str(),
        )
    {
        return Some(FinalizeStreamRewriteMode::ModelDirectiveDisplay);
    }

    (provider_api_format == client_api_format
        && provider_adaptation_should_unwrap_stream_envelope(
            envelope_name.as_str(),
            provider_api_format.as_str(),
        ))
    .then_some(FinalizeStreamRewriteMode::EnvelopeUnwrap)
}

enum AiSurfaceStreamRewriteState {
    EnvelopeUnwrap,
    ModelDirectiveDisplay,
    OpenAiImage(Box<OpenAiImageStreamState>),
    Standard(Box<StreamingStandardFormatMatrix>),
    KiroToClaudeCli(Box<KiroToClaudeCliStreamState>),
    KiroToClaudeCliThenStandard {
        kiro: Box<KiroToClaudeCliStreamState>,
        standard: Box<StreamingStandardFormatMatrix>,
    },
}

pub struct AiSurfaceStreamRewriter<'a> {
    report_context: &'a Value,
    buffered: Vec<u8>,
    state: AiSurfaceStreamRewriteState,
}

pub fn maybe_build_ai_surface_stream_rewriter<'a>(
    report_context: Option<&'a Value>,
) -> Option<AiSurfaceStreamRewriter<'a>> {
    let report_context = report_context?;
    let state = match resolve_finalize_stream_rewrite_mode(report_context)? {
        FinalizeStreamRewriteMode::EnvelopeUnwrap => AiSurfaceStreamRewriteState::EnvelopeUnwrap,
        FinalizeStreamRewriteMode::ModelDirectiveDisplay => {
            AiSurfaceStreamRewriteState::ModelDirectiveDisplay
        }
        FinalizeStreamRewriteMode::OpenAiImage => {
            AiSurfaceStreamRewriteState::OpenAiImage(Box::<OpenAiImageStreamState>::default())
        }
        FinalizeStreamRewriteMode::Standard => {
            AiSurfaceStreamRewriteState::Standard(Box::<StreamingStandardFormatMatrix>::default())
        }
        FinalizeStreamRewriteMode::KiroToClaudeCli => AiSurfaceStreamRewriteState::KiroToClaudeCli(
            Box::new(KiroToClaudeCliStreamState::new(report_context)),
        ),
        FinalizeStreamRewriteMode::KiroToClaudeCliThenStandard => {
            AiSurfaceStreamRewriteState::KiroToClaudeCliThenStandard {
                kiro: Box::new(KiroToClaudeCliStreamState::new(report_context)),
                standard: Box::<StreamingStandardFormatMatrix>::default(),
            }
        }
    };

    Some(AiSurfaceStreamRewriter {
        report_context,
        buffered: Vec::new(),
        state,
    })
}

impl AiSurfaceStreamRewriter<'_> {
    pub fn push_chunk(&mut self, chunk: &[u8]) -> Result<Vec<u8>, AiSurfaceFinalizeError> {
        match &mut self.state {
            AiSurfaceStreamRewriteState::OpenAiImage(state) => {
                state.push_chunk(self.report_context, chunk)
            }
            AiSurfaceStreamRewriteState::KiroToClaudeCli(state) => {
                state.push_chunk(self.report_context, chunk)
            }
            AiSurfaceStreamRewriteState::KiroToClaudeCliThenStandard { kiro, standard } => {
                let claude_bytes = kiro.push_chunk(self.report_context, chunk)?;
                transform_standard_bytes(standard, self.report_context, claude_bytes)
            }
            AiSurfaceStreamRewriteState::EnvelopeUnwrap
            | AiSurfaceStreamRewriteState::ModelDirectiveDisplay
            | AiSurfaceStreamRewriteState::Standard(_) => {
                self.buffered.extend_from_slice(chunk);
                let mut output = Vec::new();
                while let Some(line_end) = self.buffered.iter().position(|byte| *byte == b'\n') {
                    let line = self.buffered.drain(..=line_end).collect::<Vec<_>>();
                    output.extend(self.transform_line(line)?);
                }
                Ok(output)
            }
        }
    }

    pub fn finish(&mut self) -> Result<Vec<u8>, AiSurfaceFinalizeError> {
        match &mut self.state {
            AiSurfaceStreamRewriteState::OpenAiImage(state) => state.finish(self.report_context),
            AiSurfaceStreamRewriteState::KiroToClaudeCli(state) => {
                state.finish(self.report_context)
            }
            AiSurfaceStreamRewriteState::KiroToClaudeCliThenStandard { kiro, standard } => {
                let mut output = transform_standard_bytes(
                    standard,
                    self.report_context,
                    kiro.finish(self.report_context)?,
                )?;
                output.extend(standard.finish(self.report_context)?);
                Ok(output)
            }
            AiSurfaceStreamRewriteState::EnvelopeUnwrap
            | AiSurfaceStreamRewriteState::ModelDirectiveDisplay
            | AiSurfaceStreamRewriteState::Standard(_) => {
                if self.buffered.is_empty() {
                    if let AiSurfaceStreamRewriteState::Standard(state) = &mut self.state {
                        return state.finish(self.report_context);
                    }
                    return Ok(Vec::new());
                }
                let line = std::mem::take(&mut self.buffered);
                let mut output = self.transform_line(line)?;
                if let AiSurfaceStreamRewriteState::Standard(state) = &mut self.state {
                    output.extend(state.finish(self.report_context)?);
                }
                Ok(output)
            }
        }
    }

    fn transform_line(&mut self, line: Vec<u8>) -> Result<Vec<u8>, AiSurfaceFinalizeError> {
        match &mut self.state {
            AiSurfaceStreamRewriteState::EnvelopeUnwrap => {
                let output = transform_provider_private_stream_line(self.report_context, line)
                    .map_err(AiSurfaceFinalizeError::from)?;
                rewrite_model_directive_stream_line(self.report_context, output)
            }
            AiSurfaceStreamRewriteState::ModelDirectiveDisplay => {
                rewrite_model_directive_stream_line(self.report_context, line)
            }
            AiSurfaceStreamRewriteState::Standard(state) => {
                transform_standard_line(state, self.report_context, line)
            }
            AiSurfaceStreamRewriteState::OpenAiImage(_)
            | AiSurfaceStreamRewriteState::KiroToClaudeCli(_)
            | AiSurfaceStreamRewriteState::KiroToClaudeCliThenStandard { .. } => Ok(Vec::new()),
        }
    }
}

fn rewrite_model_directive_stream_line(
    report_context: &Value,
    line: Vec<u8>,
) -> Result<Vec<u8>, AiSurfaceFinalizeError> {
    let Some(display_model) = model_directive_display_model_from_report_context(report_context)
    else {
        return Ok(line);
    };
    let text = match std::str::from_utf8(&line) {
        Ok(text) => text,
        Err(_) => return Ok(line),
    };
    let trimmed_line_end = text.trim_end_matches(['\r', '\n']);
    let trailing = &text[trimmed_line_end.len()..];
    let Some((prefix, payload)) = trimmed_line_end.split_once(':') else {
        return Ok(line);
    };
    if prefix.trim() != "data" {
        return Ok(line);
    }
    let payload = payload.trim_start();
    if payload.is_empty() || payload == "[DONE]" {
        return Ok(line);
    }
    let mut value = match serde_json::from_str::<Value>(payload) {
        Ok(value) => value,
        Err(_) => return Ok(line),
    };
    if !rewrite_stream_payload_model(&mut value, &display_model) {
        return Ok(line);
    }
    let mut output = Vec::new();
    output.extend_from_slice(b"data: ");
    output.extend(serde_json::to_vec(&value)?);
    output.extend_from_slice(trailing.as_bytes());
    Ok(output)
}

fn rewrite_stream_payload_model(value: &mut Value, display_model: &str) -> bool {
    let Some(object) = value.as_object_mut() else {
        return false;
    };
    let mut changed = false;
    for key in ["model", "modelVersion"] {
        if object.get(key).and_then(Value::as_str).is_some() {
            object.insert(key.to_string(), Value::String(display_model.to_string()));
            changed = true;
        }
    }
    for key in ["response", "message"] {
        if let Some(nested) = object.get_mut(key) {
            changed |= rewrite_stream_payload_model(nested, display_model);
        }
    }
    changed
}

fn transform_standard_bytes(
    standard: &mut StreamingStandardFormatMatrix,
    report_context: &Value,
    bytes: Vec<u8>,
) -> Result<Vec<u8>, AiSurfaceFinalizeError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let mut output = Vec::new();
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        output.extend(transform_standard_line(
            standard,
            report_context,
            line.to_vec(),
        )?);
    }
    Ok(output)
}

fn transform_standard_line(
    standard: &mut StreamingStandardFormatMatrix,
    report_context: &Value,
    line: Vec<u8>,
) -> Result<Vec<u8>, AiSurfaceFinalizeError> {
    let line = if should_unwrap_envelope(report_context) {
        transform_provider_private_stream_line(report_context, line)?
    } else {
        line
    };
    if line.is_empty() {
        return Ok(Vec::new());
    }
    standard.transform_line(report_context, line)
}

fn should_unwrap_envelope(report_context: &Value) -> bool {
    let envelope_name = report_context
        .get("envelope_name")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let provider_api_format = report_context
        .get("provider_api_format")
        .and_then(Value::as_str)
        .unwrap_or_default();
    provider_adaptation_should_unwrap_stream_envelope(envelope_name, provider_api_format)
}

fn supports_standard_stream_rewrite(provider_api_format: &str, client_api_format: &str) -> bool {
    is_standard_provider_api_format(provider_api_format)
        && (is_standard_chat_client_api_format(client_api_format)
            || is_standard_cli_client_api_format(client_api_format))
}

fn is_standard_provider_api_format(api_format: &str) -> bool {
    matches!(
        aether_ai_formats::normalize_api_format_alias(api_format).as_str(),
        "openai:chat"
            | "openai:responses"
            | "openai:responses:compact"
            | "claude:messages"
            | "gemini:generate_content"
    )
}

fn is_standard_chat_client_api_format(api_format: &str) -> bool {
    matches!(
        api_format,
        "openai:chat" | "claude:messages" | "gemini:generate_content"
    )
}

fn is_standard_cli_client_api_format(api_format: &str) -> bool {
    matches!(
        aether_ai_formats::normalize_api_format_alias(api_format).as_str(),
        "openai:responses"
            | "openai:responses:compact"
            | "claude:messages"
            | "gemini:generate_content"
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        maybe_build_ai_surface_stream_rewriter, resolve_finalize_stream_rewrite_mode,
        FinalizeStreamRewriteMode,
    };

    #[test]
    fn resolves_standard_mode_for_cross_format_standard_streams() {
        let report_context = json!({
            "provider_api_format": "claude:messages",
            "client_api_format": "openai:chat",
            "needs_conversion": true,
        });
        assert_eq!(
            resolve_finalize_stream_rewrite_mode(&report_context),
            Some(FinalizeStreamRewriteMode::Standard)
        );
    }

    #[test]
    fn resolves_envelope_unwrap_for_same_format_private_envelopes() {
        let report_context = json!({
            "provider_api_format": "gemini:generate_content",
            "client_api_format": "gemini:generate_content",
            "envelope_name": "antigravity:v1internal",
            "needs_conversion": false,
        });
        assert_eq!(
            resolve_finalize_stream_rewrite_mode(&report_context),
            Some(FinalizeStreamRewriteMode::EnvelopeUnwrap)
        );
    }

    #[test]
    fn resolves_kiro_same_format_streams_to_kiro_mode() {
        let report_context = json!({
            "provider_api_format": "claude:messages",
            "client_api_format": "claude:messages",
            "envelope_name": "kiro:generateAssistantResponse",
            "needs_conversion": false,
        });
        assert_eq!(
            resolve_finalize_stream_rewrite_mode(&report_context),
            Some(FinalizeStreamRewriteMode::KiroToClaudeCli)
        );
    }

    #[test]
    fn rejects_unsupported_non_conversion_streams() {
        let report_context = json!({
            "provider_api_format": "openai:chat",
            "client_api_format": "openai:chat",
            "needs_conversion": false,
        });
        assert_eq!(resolve_finalize_stream_rewrite_mode(&report_context), None);
    }

    #[test]
    fn resolves_model_directive_display_mode_for_same_format_standard_streams() {
        let report_context = json!({
            "provider_api_format": "openai:responses",
            "client_api_format": "openai:responses",
            "model": "gpt-5.5-xhigh",
            "mapped_model": "gpt-5.5",
            "needs_conversion": false,
        });
        assert_eq!(
            resolve_finalize_stream_rewrite_mode(&report_context),
            Some(FinalizeStreamRewriteMode::ModelDirectiveDisplay)
        );
    }

    #[test]
    fn model_directive_display_mode_does_not_displace_kiro_stream_bridge() {
        let report_context = json!({
            "provider_api_format": "claude:messages",
            "client_api_format": "claude:messages",
            "envelope_name": "kiro:generateAssistantResponse",
            "model": "claude-sonnet-4.5-high",
            "mapped_model": "claude-sonnet-4.5",
            "needs_conversion": false,
        });
        assert_eq!(
            resolve_finalize_stream_rewrite_mode(&report_context),
            Some(FinalizeStreamRewriteMode::KiroToClaudeCli)
        );
    }

    #[test]
    fn envelope_unwrap_rewriter_restores_model_directive_display_model() {
        let report_context = json!({
            "provider_api_format": "gemini:generate_content",
            "client_api_format": "gemini:generate_content",
            "envelope_name": "gemini_cli:v1internal",
            "model": "gemini-2.5-pro-high",
            "mapped_model": "gemini-2.5-pro",
            "needs_conversion": false,
        });
        let mut rewriter = maybe_build_ai_surface_stream_rewriter(Some(&report_context))
            .expect("rewriter should exist");
        let output = rewriter
            .push_chunk(
                b"data: {\"response\":{\"modelVersion\":\"gemini-2.5-pro\",\"candidates\":[]}}\n\n",
            )
            .expect("rewrite should succeed");
        let output = String::from_utf8(output).expect("output should be utf8");

        assert!(output.contains("\"modelVersion\":\"gemini-2.5-pro-high\""));
        assert!(!output.contains("\"modelVersion\":\"gemini-2.5-pro\""));
    }

    #[test]
    fn model_directive_display_rewriter_restores_response_model() {
        let report_context = json!({
            "provider_api_format": "openai:responses",
            "client_api_format": "openai:responses",
            "model": "gpt-5.5-xhigh",
            "mapped_model": "gpt-5.5",
            "needs_conversion": false,
        });
        let mut rewriter = maybe_build_ai_surface_stream_rewriter(Some(&report_context))
            .expect("rewriter should exist");
        let output = rewriter
            .push_chunk(
                b"event: response.created\n\
data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_123\",\"object\":\"response\",\"model\":\"gpt-5.5\",\"status\":\"in_progress\"}}\n\n",
            )
            .expect("rewrite should succeed");
        let output = String::from_utf8(output).expect("output should be utf8");

        assert!(output.contains("event: response.created"));
        assert!(output.contains("\"model\":\"gpt-5.5-xhigh\""));
        assert!(!output.contains("\"model\":\"gpt-5.5\""));
    }

    #[test]
    fn standard_rewriter_converts_openai_responses_reasoning_delta_to_chat() {
        let report_context = json!({
            "provider_api_format": "openai:responses",
            "client_api_format": "openai:chat",
            "needs_conversion": true,
            "mapped_model": "gpt-5.4",
        });
        let mut rewriter = maybe_build_ai_surface_stream_rewriter(Some(&report_context))
            .expect("rewriter should exist");
        let output = rewriter
            .push_chunk(
                b"event: response.reasoning_summary_text.delta\n\
data: {\"type\":\"response.reasoning_summary_text.delta\",\"response_id\":\"resp_reasoning_stream_123\",\"item_id\":\"rs_123\",\"output_index\":0,\"summary_index\":0,\"delta\":\"Need to inspect first.\"}\n\n",
            )
            .expect("rewrite should succeed");
        let output = String::from_utf8(output).expect("output should be utf8");

        assert!(output.contains("\"object\":\"chat.completion.chunk\""));
        assert!(output.contains("\"reasoning_content\":\"Need to inspect first.\""));
        assert!(!output.contains("\"content\""));
        assert!(!output.contains("data: [DONE]"));
    }

    #[test]
    fn resolves_openai_image_mode_for_same_format_image_streams() {
        let report_context = json!({
            "provider_api_format": "openai:image",
            "client_api_format": "openai:image",
            "needs_conversion": false,
        });
        assert_eq!(
            resolve_finalize_stream_rewrite_mode(&report_context),
            Some(FinalizeStreamRewriteMode::OpenAiImage)
        );
    }
}
