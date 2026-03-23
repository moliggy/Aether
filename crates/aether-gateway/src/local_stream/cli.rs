use serde_json::{json, Value};

use crate::gateway::GatewayError;

use super::common::encode_json_sse;

#[derive(Default)]
pub(super) struct BufferedCliConversionStreamState {
    raw: Vec<u8>,
}

impl BufferedCliConversionStreamState {
    pub(super) fn transform_line(&mut self, line: Vec<u8>) -> Result<Vec<u8>, GatewayError> {
        self.raw.extend_from_slice(&line);
        Ok(Vec::new())
    }

    pub(super) fn finish<AggregateFn, ConvertFn>(
        &mut self,
        report_context: &Value,
        aggregate: AggregateFn,
        convert: ConvertFn,
    ) -> Result<Vec<u8>, GatewayError>
    where
        AggregateFn: Fn(&[u8]) -> Option<Value>,
        ConvertFn: Fn(&Value, &Value) -> Option<Value>,
    {
        if self.raw.is_empty() {
            return Ok(Vec::new());
        }
        let aggregated = aggregate(&self.raw);
        self.raw.clear();
        let Some(aggregated) = aggregated else {
            return Ok(Vec::new());
        };
        let Some(response) = convert(&aggregated, report_context) else {
            return Ok(Vec::new());
        };
        let event = json!({
            "type": "response.completed",
            "response": response,
        });
        encode_json_sse(Some("response.completed"), &event)
    }
}
