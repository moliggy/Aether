use serde_json::{Map, Value};

pub fn context_text(context: &Map<String, Value>, key: &str) -> Option<String> {
    context
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub fn context_u64(context: &Map<String, Value>, key: &str) -> Option<u64> {
    let value = context.get(key)?;
    match value {
        Value::Number(number) => number.as_u64(),
        Value::String(text) => text.trim().parse().ok(),
        _ => None,
    }
}

pub fn request_body_text(context: &Map<String, Value>, key: &str) -> Option<String> {
    context
        .get("original_request_body")
        .and_then(Value::as_object)
        .and_then(|body| body.get(key))
        .and_then(|value| match value {
            Value::String(text) => Some(text.trim().to_string()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        })
        .filter(|value| !value.is_empty())
}

pub fn request_body_string(body: &Value, key: &str) -> Option<String> {
    body.as_object()
        .and_then(|map| map.get(key))
        .and_then(|value| match value {
            Value::String(text) => Some(text.trim().to_string()),
            Value::Number(number) => Some(number.to_string()),
            _ => None,
        })
        .filter(|value| !value.is_empty())
}

pub fn request_body_u32(body: &Value, key: &str) -> Option<u32> {
    body.as_object()
        .and_then(|map| map.get(key))
        .and_then(|value| match value {
            Value::Number(number) => number.as_u64().and_then(|value| u32::try_from(value).ok()),
            Value::String(text) => text.trim().parse().ok(),
            _ => None,
        })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{
        context_text, context_u64, request_body_string, request_body_text, request_body_u32,
    };

    #[test]
    fn context_helpers_trim_and_parse_values() {
        let context = json!({
            "request_id": "  req_123  ",
            "local_created_at": "42",
        });

        let context = context.as_object().expect("object");
        assert_eq!(
            context_text(context, "request_id").as_deref(),
            Some("req_123")
        );
        assert_eq!(context_u64(context, "local_created_at"), Some(42));
    }

    #[test]
    fn request_body_helpers_read_nested_original_request_body() {
        let context = json!({
            "original_request_body": {
                "prompt": "  hello  ",
                "seconds": "8",
            }
        });

        let context = context.as_object().expect("object");
        assert_eq!(
            request_body_text(context, "prompt").as_deref(),
            Some("hello")
        );
        assert_eq!(request_body_text(context, "seconds").as_deref(), Some("8"));
    }

    #[test]
    fn request_body_helpers_read_flat_json_values() {
        let body = json!({
            "prompt": "  hello  ",
            "seconds": 8,
        });

        assert_eq!(
            request_body_string(&body, "prompt").as_deref(),
            Some("hello")
        );
        assert_eq!(request_body_u32(&body, "seconds"), Some(8));
    }
}
