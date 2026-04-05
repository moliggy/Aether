use std::collections::BTreeMap;

use aether_contracts::ExecutionResult;
use base64::Engine as _;

use crate::GatewayError;

type DecodedBody = (Vec<u8>, Option<serde_json::Value>, Option<String>);

pub(super) fn decode_execution_result_body(
    result: &ExecutionResult,
    headers: &mut BTreeMap<String, String>,
) -> Result<DecodedBody, GatewayError> {
    let Some(body) = result.body.as_ref() else {
        return Ok((Vec::new(), None, None));
    };

    if let Some(json_body) = body.json_body.clone() {
        headers
            .entry("content-type".to_string())
            .or_insert_with(|| "application/json".to_string());
        let bytes = serde_json::to_vec(&json_body)
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        headers.insert("content-length".to_string(), bytes.len().to_string());
        return Ok((bytes, Some(json_body), None));
    }

    if let Some(body_bytes_b64) = body.body_bytes_b64.clone() {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&body_bytes_b64)
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        return Ok((bytes, None, Some(body_bytes_b64)));
    }

    Ok((Vec::new(), None, None))
}
