use serde_json::Value;

use crate::ai_pipeline::adaptation::private_envelope::transform_provider_private_stream_line as transform_envelope_line;
use crate::ai_pipeline::adaptation::KiroToClaudeCliStreamState;
use crate::ai_pipeline::finalize::sse::encode_json_sse;
use crate::ai_pipeline::finalize::standard::StreamingStandardConversionState;
use crate::ai_pipeline::{resolve_finalize_stream_rewrite_mode, FinalizeStreamRewriteMode};
use crate::GatewayError;

enum RewriteMode {
    EnvelopeUnwrap,
    OpenAiImage(OpenAiImageStreamState),
    Standard(StreamingStandardConversionState),
    KiroToClaudeCli(KiroToClaudeCliStreamState),
}

pub(crate) struct LocalStreamRewriter<'a> {
    report_context: &'a Value,
    buffered: Vec<u8>,
    mode: RewriteMode,
}

pub(crate) fn maybe_build_local_stream_rewriter<'a>(
    report_context: Option<&'a Value>,
) -> Option<LocalStreamRewriter<'a>> {
    let report_context = report_context?;
    let mode = match resolve_finalize_stream_rewrite_mode(report_context)? {
        FinalizeStreamRewriteMode::EnvelopeUnwrap => RewriteMode::EnvelopeUnwrap,
        FinalizeStreamRewriteMode::OpenAiImage => {
            RewriteMode::OpenAiImage(OpenAiImageStreamState::default())
        }
        FinalizeStreamRewriteMode::Standard => {
            RewriteMode::Standard(StreamingStandardConversionState::default())
        }
        FinalizeStreamRewriteMode::KiroToClaudeCli => {
            RewriteMode::KiroToClaudeCli(KiroToClaudeCliStreamState::new(report_context))
        }
    };

    Some(LocalStreamRewriter {
        report_context,
        buffered: Vec::new(),
        mode,
    })
}

impl LocalStreamRewriter<'_> {
    pub(crate) fn push_chunk(&mut self, chunk: &[u8]) -> Result<Vec<u8>, GatewayError> {
        if let RewriteMode::OpenAiImage(state) = &mut self.mode {
            return state.push_chunk(self.report_context, chunk);
        }
        if let RewriteMode::KiroToClaudeCli(state) = &mut self.mode {
            return state.push_chunk(self.report_context, chunk);
        }
        self.buffered.extend_from_slice(chunk);
        let mut output = Vec::new();
        while let Some(line_end) = self.buffered.iter().position(|byte| *byte == b'\n') {
            let line = self.buffered.drain(..=line_end).collect::<Vec<_>>();
            output.extend(self.transform_line(line)?);
        }
        Ok(output)
    }

    pub(crate) fn finish(&mut self) -> Result<Vec<u8>, GatewayError> {
        if let RewriteMode::OpenAiImage(state) = &mut self.mode {
            return state.finish(self.report_context);
        }
        if let RewriteMode::KiroToClaudeCli(state) = &mut self.mode {
            return state.finish(self.report_context);
        }
        if self.buffered.is_empty() {
            match &mut self.mode {
                RewriteMode::Standard(state) => return state.finish(self.report_context),
                RewriteMode::OpenAiImage(_) => {}
                RewriteMode::KiroToClaudeCli(_) => {}
                RewriteMode::EnvelopeUnwrap => {}
            }
            return Ok(Vec::new());
        }
        let line = std::mem::take(&mut self.buffered);
        let mut output = self.transform_line(line)?;
        match &mut self.mode {
            RewriteMode::Standard(state) => {
                output.extend(state.finish(self.report_context)?);
            }
            RewriteMode::OpenAiImage(_) => {}
            RewriteMode::KiroToClaudeCli(_) => {}
            RewriteMode::EnvelopeUnwrap => {}
        }
        Ok(output)
    }

    fn transform_line(&mut self, line: Vec<u8>) -> Result<Vec<u8>, GatewayError> {
        match &mut self.mode {
            RewriteMode::EnvelopeUnwrap => transform_envelope_line(self.report_context, line)
                .map_err(|err| GatewayError::Internal(err.to_string())),
            RewriteMode::OpenAiImage(_) => Ok(Vec::new()),
            RewriteMode::Standard(state) => state.transform_line(self.report_context, line),
            RewriteMode::KiroToClaudeCli(_) => Ok(Vec::new()),
        }
    }
}

#[derive(Default)]
struct OpenAiImageStreamState {
    buffered: Vec<u8>,
    latest_image: Option<OpenAiImageFrame>,
    emitted_partial_count: u64,
}

#[derive(Clone)]
struct OpenAiImageFrame {
    b64_json: String,
}

impl OpenAiImageStreamState {
    fn push_chunk(
        &mut self,
        report_context: &Value,
        chunk: &[u8],
    ) -> Result<Vec<u8>, GatewayError> {
        self.buffered.extend_from_slice(chunk);
        let mut output = Vec::new();
        while let Some(block_end) = find_sse_block_end(&self.buffered) {
            let block = self.buffered.drain(..block_end).collect::<Vec<_>>();
            output.extend(self.transform_block(report_context, &block)?);
            drain_sse_separator(&mut self.buffered);
        }
        Ok(output)
    }

    fn finish(&mut self, report_context: &Value) -> Result<Vec<u8>, GatewayError> {
        if self.buffered.is_empty() {
            return Ok(Vec::new());
        }
        let block = std::mem::take(&mut self.buffered);
        self.transform_block(report_context, &block)
    }

    fn transform_block(
        &mut self,
        report_context: &Value,
        block: &[u8],
    ) -> Result<Vec<u8>, GatewayError> {
        let text =
            std::str::from_utf8(block).map_err(|err| GatewayError::Internal(err.to_string()))?;
        let mut event_name = None::<String>;
        let mut data_lines = Vec::new();
        for raw_line in text.lines() {
            let line = raw_line.trim_end_matches('\r');
            if let Some(value) = line.strip_prefix("event:") {
                event_name = Some(value.trim().to_string());
            } else if let Some(value) = line.strip_prefix("data:") {
                data_lines.push(value.trim().to_string());
            }
        }
        let data = data_lines.join("\n");
        if data.is_empty() || data == "[DONE]" {
            return Ok(Vec::new());
        }
        let event: Value =
            serde_json::from_str(&data).map_err(|err| GatewayError::Internal(err.to_string()))?;
        let event_type = event
            .get("type")
            .and_then(Value::as_str)
            .or(event_name.as_deref())
            .unwrap_or_default();
        match event_type {
            "response.output_item.done" => self.handle_output_item_done(report_context, &event),
            "response.completed" => self.handle_completed(report_context, &event),
            _ => Ok(Vec::new()),
        }
    }

    fn handle_output_item_done(
        &mut self,
        report_context: &Value,
        event: &Value,
    ) -> Result<Vec<u8>, GatewayError> {
        let Some(item) = event.get("item").and_then(Value::as_object) else {
            return Ok(Vec::new());
        };
        if item.get("type").and_then(Value::as_str) != Some("image_generation_call") {
            return Ok(Vec::new());
        }
        let Some(result) = item.get("result").and_then(Value::as_str).map(str::trim) else {
            return Ok(Vec::new());
        };
        if result.is_empty() {
            return Ok(Vec::new());
        }
        self.latest_image = Some(OpenAiImageFrame {
            b64_json: result.to_string(),
        });

        if requested_partial_images(report_context) == 0 {
            return Ok(Vec::new());
        }

        let partial_image_index = event
            .get("output_index")
            .and_then(Value::as_u64)
            .unwrap_or(self.emitted_partial_count);
        self.emitted_partial_count = partial_image_index.saturating_add(1);

        encode_json_sse(
            Some(image_partial_event_name(report_context)),
            &serde_json::json!({
                "type": image_partial_event_name(report_context),
                "b64_json": result,
                "partial_image_index": partial_image_index,
            }),
        )
    }

    fn handle_completed(
        &mut self,
        report_context: &Value,
        event: &Value,
    ) -> Result<Vec<u8>, GatewayError> {
        let Some(latest_image) = self.latest_image.clone() else {
            return Ok(Vec::new());
        };
        let usage = event
            .get("response")
            .and_then(Value::as_object)
            .and_then(|response| {
                response
                    .get("tool_usage")
                    .and_then(|value| value.get("image_gen"))
                    .cloned()
                    .or_else(|| response.get("usage").cloned())
            })
            .unwrap_or(Value::Null);

        encode_json_sse(
            Some(image_completed_event_name(report_context)),
            &serde_json::json!({
                "type": image_completed_event_name(report_context),
                "b64_json": latest_image.b64_json,
                "usage": usage,
            }),
        )
    }
}

fn requested_partial_images(report_context: &Value) -> u64 {
    report_context
        .get("image_request")
        .and_then(|value| value.get("partial_images"))
        .and_then(Value::as_u64)
        .unwrap_or(0)
}

fn image_partial_event_name(report_context: &Value) -> &'static str {
    if image_request_operation(report_context) == Some("edit") {
        "image_edit.partial_image"
    } else {
        "image_generation.partial_image"
    }
}

fn image_completed_event_name(report_context: &Value) -> &'static str {
    if image_request_operation(report_context) == Some("edit") {
        "image_edit.completed"
    } else {
        "image_generation.completed"
    }
}

fn image_request_operation(report_context: &Value) -> Option<&str> {
    report_context
        .get("image_request")
        .and_then(|value| value.get("operation"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn find_sse_block_end(buffer: &[u8]) -> Option<usize> {
    buffer
        .windows(2)
        .position(|window| window == b"\n\n")
        .map(|index| index + 2)
        .or_else(|| {
            buffer
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .map(|index| index + 4)
        })
}

fn drain_sse_separator(buffer: &mut Vec<u8>) {
    while matches!(buffer.first(), Some(b'\n' | b'\r')) {
        buffer.remove(0);
    }
}

#[cfg(test)]
#[path = "../tests_stream.rs"]
mod tests;
