use serde_json::{Map, Value};

pub fn build_video_follow_up_report_context(
    request_id: &str,
    user_id: &str,
    api_key_id: &str,
    task_id: &str,
    provider_id: &str,
    endpoint_id: &str,
    key_id: &str,
    provider_name: Option<&str>,
    model_name: Option<&str>,
    client_api_format: &str,
    provider_api_format: &str,
) -> Value {
    let mut context = Map::new();
    context.insert(
        "request_id".to_string(),
        Value::String(request_id.to_string()),
    );
    context.insert("user_id".to_string(), Value::String(user_id.to_string()));
    context.insert(
        "api_key_id".to_string(),
        Value::String(api_key_id.to_string()),
    );
    context.insert("task_id".to_string(), Value::String(task_id.to_string()));
    context.insert(
        "provider_id".to_string(),
        Value::String(provider_id.to_string()),
    );
    context.insert(
        "endpoint_id".to_string(),
        Value::String(endpoint_id.to_string()),
    );
    context.insert("key_id".to_string(), Value::String(key_id.to_string()));
    context.insert(
        "client_api_format".to_string(),
        Value::String(client_api_format.to_string()),
    );
    context.insert(
        "provider_api_format".to_string(),
        Value::String(provider_api_format.to_string()),
    );
    if let Some(provider_name) = provider_name.filter(|value| !value.is_empty()) {
        context.insert(
            "provider_name".to_string(),
            Value::String(provider_name.to_string()),
        );
    }
    if let Some(model_name) = model_name.filter(|value| !value.is_empty()) {
        context.insert("model".to_string(), Value::String(model_name.to_string()));
    }
    Value::Object(context)
}

pub fn resolve_follow_up_auth(
    user_id: Option<&str>,
    api_key_id: Option<&str>,
    fallback_user_id: Option<&str>,
    fallback_api_key_id: Option<&str>,
) -> Option<(String, String)> {
    let resolved_user_id = user_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| {
            fallback_user_id
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        })?;
    let resolved_api_key_id = api_key_id
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| {
            fallback_api_key_id
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        })?;
    Some((resolved_user_id, resolved_api_key_id))
}

#[cfg(test)]
mod tests {
    use super::{build_video_follow_up_report_context, resolve_follow_up_auth};

    #[test]
    fn builds_follow_up_report_context_with_transport_metadata() {
        let context = build_video_follow_up_report_context(
            "req_123",
            "user_123",
            "key_123",
            "task_123",
            "provider_123",
            "endpoint_123",
            "transport_key_123",
            Some("provider-name"),
            Some("model-name"),
            "openai:video",
            "openai:video",
        );

        assert_eq!(context["request_id"].as_str(), Some("req_123"));
        assert_eq!(context["provider_id"].as_str(), Some("provider_123"));
        assert_eq!(context["provider_name"].as_str(), Some("provider-name"));
        assert_eq!(context["model"].as_str(), Some("model-name"));
    }

    #[test]
    fn resolves_follow_up_auth_from_primary_or_fallback_values() {
        assert_eq!(
            resolve_follow_up_auth(Some(" user_123 "), Some(" key_123 "), None, None),
            Some(("user_123".to_string(), "key_123".to_string()))
        );
        assert_eq!(
            resolve_follow_up_auth(None, None, Some("user_fallback"), Some("key_fallback")),
            Some(("user_fallback".to_string(), "key_fallback".to_string()))
        );
        assert_eq!(resolve_follow_up_auth(None, None, None, None), None);
    }
}
