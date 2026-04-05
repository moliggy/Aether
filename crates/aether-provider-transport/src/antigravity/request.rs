use serde_json::{Map, Value};

use super::auth::{AntigravityRequestAuth, ANTIGRAVITY_REQUEST_USER_AGENT};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AntigravityEnvelopeRequestType {
    Agent,
    EndpointTest,
}

impl AntigravityEnvelopeRequestType {
    fn as_str(self) -> &'static str {
        match self {
            Self::Agent => "agent",
            Self::EndpointTest => "endpoint_test",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum AntigravityRequestEnvelopeSupport {
    Supported(Value),
    Unsupported(AntigravityRequestEnvelopeUnsupportedReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AntigravityRequestEnvelopeUnsupportedReason {
    NonObjectBody,
    MissingContents,
    MissingRequestId,
    MissingModel,
    ComplexEnvelopeTransform,
}

pub fn classify_antigravity_safe_request_body(
    request_body: &Value,
) -> Result<(), AntigravityRequestEnvelopeUnsupportedReason> {
    let Value::Object(map) = request_body else {
        return Err(AntigravityRequestEnvelopeUnsupportedReason::NonObjectBody);
    };
    if !map.contains_key("contents") {
        return Err(AntigravityRequestEnvelopeUnsupportedReason::MissingContents);
    }
    if contains_blocked_request_features(request_body) {
        return Err(AntigravityRequestEnvelopeUnsupportedReason::ComplexEnvelopeTransform);
    }

    Ok(())
}

pub fn build_antigravity_safe_v1internal_request(
    auth: &AntigravityRequestAuth,
    request_id: &str,
    model: &str,
    request_body: &Value,
    request_type: AntigravityEnvelopeRequestType,
) -> AntigravityRequestEnvelopeSupport {
    if request_id.trim().is_empty() {
        return AntigravityRequestEnvelopeSupport::Unsupported(
            AntigravityRequestEnvelopeUnsupportedReason::MissingRequestId,
        );
    }
    if model.trim().is_empty() {
        return AntigravityRequestEnvelopeSupport::Unsupported(
            AntigravityRequestEnvelopeUnsupportedReason::MissingModel,
        );
    }
    if let Err(reason) = classify_antigravity_safe_request_body(request_body) {
        return AntigravityRequestEnvelopeSupport::Unsupported(reason);
    }

    let Value::Object(source) = request_body else {
        return AntigravityRequestEnvelopeSupport::Unsupported(
            AntigravityRequestEnvelopeUnsupportedReason::NonObjectBody,
        );
    };

    let mut inner_request: Map<String, Value> = source.clone();
    inner_request.remove("model");
    inner_request.remove("safetySettings");
    inner_request.remove("safety_settings");

    AntigravityRequestEnvelopeSupport::Supported(serde_json::json!({
        "project": auth.project_id,
        "requestId": request_id,
        "request": Value::Object(inner_request),
        "model": model,
        "userAgent": ANTIGRAVITY_REQUEST_USER_AGENT,
        "requestType": request_type.as_str(),
    }))
}

fn contains_blocked_request_features(value: &Value) -> bool {
    match value {
        Value::Object(map) => map.iter().any(|(key, inner)| {
            is_blocked_request_key(key.as_str()) || contains_blocked_request_features(inner)
        }),
        Value::Array(items) => items.iter().any(contains_blocked_request_features),
        _ => false,
    }
}

fn is_blocked_request_key(key: &str) -> bool {
    matches!(
        key.trim(),
        "systemInstruction"
            | "system_instruction"
            | "tools"
            | "toolConfig"
            | "tool_config"
            | "thinkingConfig"
            | "thinking_config"
            | "imageConfig"
            | "image_config"
            | "functionCall"
            | "function_call"
            | "functionResponse"
            | "function_response"
    )
}
