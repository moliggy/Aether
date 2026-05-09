use aether_ai_formats::formats::conversion::request::{
    convert_openai_chat_request_to_claude_request, convert_openai_chat_request_to_gemini_request,
    convert_openai_chat_request_to_openai_responses_request,
    normalize_openai_responses_request_to_openai_chat_request,
};
use aether_ai_formats::{request_conversion_kind, RequestConversionKind};
use serde_json::{json, Value};

use crate::formats::shared::model_directives::apply_model_directive_overrides_from_request;

pub fn build_local_openai_chat_request_body(
    body_json: &Value,
    mapped_model: &str,
    upstream_is_stream: bool,
) -> Option<Value> {
    build_local_openai_chat_request_body_with_model_directives(
        body_json,
        mapped_model,
        upstream_is_stream,
        false,
    )
}

pub fn build_local_openai_chat_request_body_with_model_directives(
    body_json: &Value,
    mapped_model: &str,
    upstream_is_stream: bool,
    enable_model_directives: bool,
) -> Option<Value> {
    let request_body_object = body_json.as_object()?;
    let mut provider_request_body = serde_json::Map::from_iter(
        request_body_object
            .iter()
            .map(|(key, value)| (key.clone(), value.clone())),
    );
    provider_request_body.insert("model".to_string(), Value::String(mapped_model.to_string()));
    if upstream_is_stream {
        provider_request_body.insert("stream".to_string(), Value::Bool(true));
        match provider_request_body.get_mut("stream_options") {
            Some(Value::Object(stream_options)) => {
                stream_options.insert("include_usage".to_string(), Value::Bool(true));
            }
            _ => {
                provider_request_body.insert(
                    "stream_options".to_string(),
                    json!({
                        "include_usage": true,
                    }),
                );
            }
        }
    }
    let mut provider_request_body = with_model_directive_overrides(
        Value::Object(provider_request_body),
        "openai:chat",
        mapped_model,
        body_json,
        None,
        enable_model_directives,
    );
    let require_body_stream_field = body_json
        .as_object()
        .is_some_and(|object| object.contains_key("stream"));
    crate::formats::shared::request::enforce_request_body_stream_field(
        &mut provider_request_body,
        "openai:chat",
        upstream_is_stream,
        require_body_stream_field,
    );
    Some(provider_request_body)
}

pub fn build_cross_format_openai_chat_request_body(
    body_json: &Value,
    mapped_model: &str,
    provider_api_format: &str,
    upstream_is_stream: bool,
) -> Option<Value> {
    build_cross_format_openai_chat_request_body_with_model_directives(
        body_json,
        mapped_model,
        provider_api_format,
        upstream_is_stream,
        false,
    )
}

pub fn build_cross_format_openai_chat_request_body_with_model_directives(
    body_json: &Value,
    mapped_model: &str,
    provider_api_format: &str,
    upstream_is_stream: bool,
    enable_model_directives: bool,
) -> Option<Value> {
    let conversion_kind = request_conversion_kind("openai:chat", provider_api_format)?;
    let provider_request_body = match conversion_kind {
        RequestConversionKind::ToClaudeStandard => convert_openai_chat_request_to_claude_request(
            body_json,
            mapped_model,
            upstream_is_stream,
        )?,
        RequestConversionKind::ToGeminiStandard => convert_openai_chat_request_to_gemini_request(
            body_json,
            mapped_model,
            upstream_is_stream,
        )?,
        RequestConversionKind::ToOpenAiResponses => {
            convert_openai_chat_request_to_openai_responses_request(
                body_json,
                mapped_model,
                upstream_is_stream,
                false,
            )?
        }
        _ => return None,
    };
    let mut provider_request_body = with_model_directive_overrides(
        provider_request_body,
        provider_api_format,
        mapped_model,
        body_json,
        None,
        enable_model_directives,
    );
    let require_body_stream_field = body_json
        .as_object()
        .is_some_and(|object| object.contains_key("stream"));
    crate::formats::shared::request::enforce_request_body_stream_field(
        &mut provider_request_body,
        provider_api_format,
        upstream_is_stream,
        require_body_stream_field,
    );
    Some(provider_request_body)
}

pub fn build_local_openai_responses_request_body(
    body_json: &Value,
    mapped_model: &str,
    require_streaming: bool,
) -> Option<Value> {
    build_local_openai_responses_request_body_with_model_directives(
        body_json,
        mapped_model,
        require_streaming,
        false,
    )
}

pub fn build_local_openai_responses_request_body_with_model_directives(
    body_json: &Value,
    mapped_model: &str,
    require_streaming: bool,
    enable_model_directives: bool,
) -> Option<Value> {
    let request_body_object = body_json.as_object()?;
    let mut provider_request_body = serde_json::Map::from_iter(
        request_body_object
            .iter()
            .map(|(key, value)| (key.clone(), value.clone())),
    );
    provider_request_body.insert("model".to_string(), Value::String(mapped_model.to_string()));
    if require_streaming {
        provider_request_body.insert("stream".to_string(), Value::Bool(true));
    }
    let mut provider_request_body = with_model_directive_overrides(
        Value::Object(provider_request_body),
        "openai:responses",
        mapped_model,
        body_json,
        None,
        enable_model_directives,
    );
    let require_body_stream_field = body_json
        .as_object()
        .is_some_and(|object| object.contains_key("stream"));
    crate::formats::shared::request::enforce_request_body_stream_field(
        &mut provider_request_body,
        "openai:responses",
        require_streaming,
        require_body_stream_field,
    );
    Some(provider_request_body)
}

pub fn build_cross_format_openai_responses_request_body(
    body_json: &Value,
    mapped_model: &str,
    client_api_format: &str,
    provider_api_format: &str,
    upstream_is_stream: bool,
) -> Option<Value> {
    build_cross_format_openai_responses_request_body_with_model_directives(
        body_json,
        mapped_model,
        client_api_format,
        provider_api_format,
        upstream_is_stream,
        false,
    )
}

pub fn build_cross_format_openai_responses_request_body_with_model_directives(
    body_json: &Value,
    mapped_model: &str,
    client_api_format: &str,
    provider_api_format: &str,
    upstream_is_stream: bool,
    enable_model_directives: bool,
) -> Option<Value> {
    let chat_like_request = normalize_openai_responses_request_to_openai_chat_request(body_json)?;
    let conversion_kind = request_conversion_kind(client_api_format, provider_api_format)?;
    let provider_request_body = match conversion_kind {
        RequestConversionKind::ToOpenAIChat => {
            build_local_openai_chat_request_body_with_model_directives(
                &chat_like_request,
                mapped_model,
                upstream_is_stream,
                enable_model_directives,
            )?
        }
        RequestConversionKind::ToOpenAiResponses => {
            convert_openai_chat_request_to_openai_responses_request(
                &chat_like_request,
                mapped_model,
                upstream_is_stream,
                false,
            )?
        }
        RequestConversionKind::ToClaudeStandard => convert_openai_chat_request_to_claude_request(
            &chat_like_request,
            mapped_model,
            upstream_is_stream,
        )?,
        RequestConversionKind::ToGeminiStandard => convert_openai_chat_request_to_gemini_request(
            &chat_like_request,
            mapped_model,
            upstream_is_stream,
        )?,
    };
    let mut provider_request_body = with_model_directive_overrides(
        provider_request_body,
        provider_api_format,
        mapped_model,
        body_json,
        None,
        enable_model_directives,
    );
    let require_body_stream_field = body_json
        .as_object()
        .is_some_and(|object| object.contains_key("stream"));
    crate::formats::shared::request::enforce_request_body_stream_field(
        &mut provider_request_body,
        provider_api_format,
        upstream_is_stream,
        require_body_stream_field,
    );
    Some(provider_request_body)
}

fn with_model_directive_overrides(
    mut provider_request_body: Value,
    provider_api_format: &str,
    provider_model: &str,
    request_body: &Value,
    request_path: Option<&str>,
    enable_model_directives: bool,
) -> Value {
    if enable_model_directives {
        apply_model_directive_overrides_from_request(
            &mut provider_request_body,
            provider_api_format,
            provider_model,
            request_body,
            request_path,
        );
    }
    provider_request_body
}

#[cfg(test)]
mod tests {
    use super::build_local_openai_responses_request_body;
    use super::{
        build_cross_format_openai_chat_request_body_with_model_directives,
        build_cross_format_openai_responses_request_body, build_local_openai_chat_request_body,
        build_local_openai_chat_request_body_with_model_directives,
        build_local_openai_responses_request_body_with_model_directives,
    };
    use serde_json::{json, Value};

    fn object_keys(value: &Value) -> Vec<&str> {
        value
            .as_object()
            .expect("json object")
            .keys()
            .map(String::as_str)
            .collect()
    }

    #[test]
    fn builds_openai_chat_cross_format_request_body_from_openai_responses_source() {
        let body_json = json!({
            "model": "gpt-5",
            "input": "hello",
        });

        let provider_request_body = build_cross_format_openai_responses_request_body(
            &body_json,
            "gpt-5-upstream",
            "openai:responses",
            "openai:chat",
            false,
        )
        .expect("openai responses to openai chat body should build");

        assert_eq!(provider_request_body["model"], "gpt-5-upstream");
        assert_eq!(provider_request_body["messages"][0]["role"], "user");
        assert_eq!(provider_request_body["messages"][0]["content"], "hello");
    }

    #[test]
    fn local_openai_responses_request_body_preserves_original_field_order() {
        let body_json: Value = serde_json::from_str(
            r#"{
                "model": "gpt-5",
                "include": ["reasoning.encrypted_content"],
                "input": [],
                "instructions": "Keep order"
            }"#,
        )
        .expect("request json should parse");

        let provider_request_body =
            build_local_openai_responses_request_body(&body_json, "gpt-5-upstream", false)
                .expect("openai responses body should build");

        assert_eq!(
            object_keys(&provider_request_body),
            vec!["model", "include", "input", "instructions"]
        );
        assert_eq!(provider_request_body["model"], "gpt-5-upstream");
    }

    #[test]
    fn builds_streaming_local_openai_chat_request_body_with_include_usage() {
        let body_json = json!({
            "model": "gpt-5",
            "messages": [{
                "role": "user",
                "content": "hello"
            }]
        });

        let provider_request_body =
            build_local_openai_chat_request_body(&body_json, "gpt-5-upstream", true)
                .expect("openai chat body should build");

        assert_eq!(provider_request_body["model"], "gpt-5-upstream");
        assert_eq!(provider_request_body["stream"], true);
        assert_eq!(
            provider_request_body["stream_options"]["include_usage"],
            true
        );
    }

    #[test]
    fn local_openai_chat_request_body_overrides_client_stream_for_non_stream_upstream() {
        let body_json = json!({
            "model": "gpt-5",
            "messages": [{
                "role": "user",
                "content": "hello"
            }],
            "stream": true
        });

        let provider_request_body =
            build_local_openai_chat_request_body(&body_json, "gpt-5-upstream", false)
                .expect("openai chat body should build");

        assert_eq!(provider_request_body["model"], "gpt-5-upstream");
        assert_eq!(provider_request_body["stream"], false);
    }

    #[test]
    fn local_openai_responses_request_body_overrides_client_stream_for_non_stream_upstream() {
        let body_json = json!({
            "model": "gpt-5",
            "input": "hello",
            "stream": true
        });

        let provider_request_body =
            build_local_openai_responses_request_body(&body_json, "gpt-5-upstream", false)
                .expect("openai responses body should build");

        assert_eq!(provider_request_body["model"], "gpt-5-upstream");
        assert_eq!(provider_request_body["stream"], false);
    }

    #[test]
    fn cross_format_openai_chat_request_body_overrides_client_stream_for_non_stream_upstream() {
        let body_json = json!({
            "model": "gpt-5",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": true
        });

        let claude = build_cross_format_openai_chat_request_body_with_model_directives(
            &body_json,
            "claude-sonnet-4-5",
            "claude:messages",
            false,
            false,
        )
        .expect("claude body should build");
        assert_eq!(claude["stream"], false);

        let responses = build_cross_format_openai_chat_request_body_with_model_directives(
            &body_json,
            "gpt-5-upstream",
            "openai:responses",
            false,
            false,
        )
        .expect("responses body should build");
        assert_eq!(responses["stream"], false);
    }

    #[test]
    fn cross_format_openai_chat_request_body_does_not_add_stream_false_for_plain_sync_body() {
        let body_json = json!({
            "model": "gpt-5",
            "messages": [{"role": "user", "content": "hello"}]
        });

        let claude = build_cross_format_openai_chat_request_body_with_model_directives(
            &body_json,
            "claude-sonnet-4-5",
            "claude:messages",
            false,
            false,
        )
        .expect("claude body should build");
        assert!(claude.get("stream").is_none());
    }

    #[test]
    fn cross_format_openai_responses_body_overrides_client_stream_for_non_stream_upstream() {
        let body_json = json!({
            "model": "gpt-5",
            "input": "hello",
            "stream": true
        });

        let provider_request_body = build_cross_format_openai_responses_request_body(
            &body_json,
            "claude-sonnet-4-5",
            "openai:responses",
            "claude:messages",
            false,
        )
        .expect("claude body should build");

        assert_eq!(provider_request_body["stream"], false);
    }

    #[test]
    fn local_openai_chat_request_body_applies_reasoning_effort_suffix() {
        let body_json = json!({
            "model": "gpt-5.4-xhigh",
            "messages": [{"role": "user", "content": "hello"}],
            "reasoning_effort": "low"
        });

        let provider_request_body = build_local_openai_chat_request_body_with_model_directives(
            &body_json,
            "gpt-5-upstream",
            false,
            true,
        )
        .expect("openai chat body should build");

        assert_eq!(provider_request_body["model"], "gpt-5-upstream");
        assert_eq!(provider_request_body["reasoning_effort"], "xhigh");
    }

    #[test]
    fn local_openai_chat_request_body_leaves_model_directive_disabled_by_default() {
        let body_json = json!({
            "model": "gpt-5.4-xhigh",
            "messages": [{"role": "user", "content": "hello"}],
            "reasoning_effort": "low"
        });

        let provider_request_body =
            build_local_openai_chat_request_body(&body_json, "gpt-5-upstream", false)
                .expect("openai chat body should build");

        assert_eq!(provider_request_body["model"], "gpt-5-upstream");
        assert_eq!(provider_request_body["reasoning_effort"], "low");
    }

    #[test]
    fn local_openai_responses_request_body_applies_reasoning_effort_suffix() {
        let body_json = json!({
            "model": "gpt-5.4-max",
            "input": "hello",
            "reasoning": {"effort": "low", "summary": "auto"}
        });

        let provider_request_body =
            build_local_openai_responses_request_body_with_model_directives(
                &body_json,
                "gpt-5-upstream",
                false,
                true,
            )
            .expect("openai responses body should build");

        assert_eq!(provider_request_body["model"], "gpt-5-upstream");
        assert_eq!(provider_request_body["reasoning"]["summary"], "auto");
        assert_eq!(provider_request_body["reasoning"]["effort"], "xhigh");
    }

    #[test]
    fn cross_format_request_body_applies_reasoning_effort_suffix() {
        let body_json = json!({
            "model": "gpt-5.4-high",
            "messages": [{"role": "user", "content": "hello"}],
            "reasoning_effort": "low"
        });

        let provider_request_body =
            build_cross_format_openai_chat_request_body_with_model_directives(
                &body_json,
                "claude-sonnet-4-5",
                "claude:messages",
                false,
                true,
            )
            .expect("claude body should build");

        assert_eq!(provider_request_body["model"], "claude-sonnet-4-5");
        assert_eq!(provider_request_body["output_config"]["effort"], "high");
        assert_eq!(provider_request_body["thinking"]["budget_tokens"], 4096);
    }

    #[test]
    fn streaming_local_openai_chat_request_body_preserves_stream_options_while_forcing_include_usage(
    ) {
        let body_json = json!({
            "model": "gpt-5",
            "messages": [{
                "role": "user",
                "content": "hello"
            }],
            "stream_options": {
                "include_usage": false,
                "extra": "keep-me"
            }
        });

        let provider_request_body =
            build_local_openai_chat_request_body(&body_json, "gpt-5-upstream", true)
                .expect("openai chat body should build");

        assert_eq!(
            provider_request_body["stream_options"]["include_usage"],
            true
        );
        assert_eq!(provider_request_body["stream_options"]["extra"], "keep-me");
    }
}
