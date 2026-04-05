use serde_json::Value;

use crate::ai_pipeline::adaptation::private_envelope::transform_provider_private_stream_line as transform_envelope_line;
use crate::ai_pipeline::adaptation::surfaces::provider_adaptation_should_unwrap_stream_envelope;
use crate::GatewayError;

use super::common::CanonicalStreamFrame;
use super::{ClientStreamEmitter, ProviderStreamParser};

#[derive(Default)]
pub(crate) struct StreamingStandardConversionState {
    provider: Option<ProviderStreamParser>,
    client: Option<ClientStreamEmitter>,
}

impl StreamingStandardConversionState {
    pub(crate) fn transform_line(
        &mut self,
        report_context: &Value,
        line: Vec<u8>,
    ) -> Result<Vec<u8>, GatewayError> {
        self.ensure_initialized(report_context)?;
        let line = if should_unwrap_envelope(report_context) {
            transform_envelope_line(report_context, line)?
        } else {
            line
        };
        if line.is_empty() {
            return Ok(Vec::new());
        }
        let Some(provider) = self.provider.as_mut() else {
            return Ok(Vec::new());
        };
        let frames = provider.push_line(report_context, line)?;
        self.emit_frames(frames)
    }

    pub(crate) fn finish(&mut self, report_context: &Value) -> Result<Vec<u8>, GatewayError> {
        self.ensure_initialized(report_context)?;
        let Some(provider) = self.provider.as_mut() else {
            return Ok(Vec::new());
        };
        let frames = provider.finish(report_context)?;
        let mut out = self.emit_frames(frames)?;
        if let Some(client) = self.client.as_mut() {
            out.extend(client.finish()?);
        }
        Ok(out)
    }

    fn ensure_initialized(&mut self, report_context: &Value) -> Result<(), GatewayError> {
        if self.provider.is_some() && self.client.is_some() {
            return Ok(());
        }

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

        self.provider = ProviderStreamParser::for_api_format(provider_api_format.as_str());
        self.client = ClientStreamEmitter::for_api_format(client_api_format.as_str());

        Ok(())
    }

    fn emit_frames(&mut self, frames: Vec<CanonicalStreamFrame>) -> Result<Vec<u8>, GatewayError> {
        let Some(client) = self.client.as_mut() else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for frame in frames {
            out.extend(client.emit(frame)?);
        }
        Ok(out)
    }
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
