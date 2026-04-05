use std::collections::BTreeMap;

use crate::StandardizedUsage;

pub struct UsageMapper;

impl UsageMapper {
    pub fn map(
        raw_usage: &serde_json::Value,
        api_format: &str,
        extra_mapping: Option<&BTreeMap<String, String>>,
    ) -> StandardizedUsage {
        if !raw_usage.is_object() {
            return StandardizedUsage::new();
        }

        let mut usage = StandardizedUsage::new();
        let mut mapping = base_mapping(api_format);
        if let Some(extra_mapping) = extra_mapping {
            mapping.extend(extra_mapping.clone());
        }

        for (source_path, target_field) in mapping {
            if let Some(value) = get_nested_value(raw_usage, &source_path) {
                usage.set(&target_field, value.clone());
            }
        }

        usage
    }

    pub fn map_from_response(response: &serde_json::Value, api_format: &str) -> StandardizedUsage {
        let family = api_family(api_format);
        let usage_value = if family == "gemini" {
            response
                .get("usageMetadata")
                .or_else(|| {
                    response
                        .get("candidates")
                        .and_then(|v| v.get(0))
                        .and_then(|v| v.get("usageMetadata"))
                })
                .unwrap_or(&serde_json::Value::Null)
        } else {
            response.get("usage").unwrap_or(&serde_json::Value::Null)
        };
        Self::map(usage_value, api_format, None)
    }
}

pub fn map_usage(raw_usage: &serde_json::Value, api_format: &str) -> StandardizedUsage {
    UsageMapper::map(raw_usage, api_format, None)
}

pub fn map_usage_from_response(
    response: &serde_json::Value,
    api_format: &str,
) -> StandardizedUsage {
    UsageMapper::map_from_response(response, api_format)
}

fn api_family(api_format: &str) -> String {
    api_format
        .split(':')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

fn base_mapping(api_format: &str) -> BTreeMap<String, String> {
    let mut mapping = BTreeMap::new();
    match api_family(api_format).as_str() {
        "openai" => {
            mapping.insert("prompt_tokens".to_string(), "input_tokens".to_string());
            mapping.insert("completion_tokens".to_string(), "output_tokens".to_string());
            mapping.insert(
                "prompt_tokens_details.cached_tokens".to_string(),
                "cache_read_tokens".to_string(),
            );
            mapping.insert(
                "completion_tokens_details.reasoning_tokens".to_string(),
                "reasoning_tokens".to_string(),
            );
        }
        "gemini" => {
            mapping.insert("promptTokenCount".to_string(), "input_tokens".to_string());
            mapping.insert(
                "candidatesTokenCount".to_string(),
                "output_tokens".to_string(),
            );
            mapping.insert(
                "cachedContentTokenCount".to_string(),
                "cache_read_tokens".to_string(),
            );
            mapping.insert(
                "usageMetadata.promptTokenCount".to_string(),
                "input_tokens".to_string(),
            );
            mapping.insert(
                "usageMetadata.candidatesTokenCount".to_string(),
                "output_tokens".to_string(),
            );
            mapping.insert(
                "usageMetadata.cachedContentTokenCount".to_string(),
                "cache_read_tokens".to_string(),
            );
        }
        _ => {
            mapping.insert("input_tokens".to_string(), "input_tokens".to_string());
            mapping.insert("output_tokens".to_string(), "output_tokens".to_string());
            mapping.insert(
                "cache_creation_input_tokens".to_string(),
                "cache_creation_tokens".to_string(),
            );
            mapping.insert(
                "cache_read_input_tokens".to_string(),
                "cache_read_tokens".to_string(),
            );
        }
    }
    mapping
}

fn get_nested_value<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let mut current = value;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::{map_usage, map_usage_from_response};

    #[test]
    fn maps_openai_usage() {
        let usage = map_usage(
            &serde_json::json!({
                "prompt_tokens": 12,
                "completion_tokens": 8,
                "prompt_tokens_details": { "cached_tokens": 2 },
                "completion_tokens_details": { "reasoning_tokens": 3 }
            }),
            "openai:chat",
        );

        assert_eq!(usage.input_tokens, 12);
        assert_eq!(usage.output_tokens, 8);
        assert_eq!(usage.cache_read_tokens, 2);
        assert_eq!(usage.reasoning_tokens, 3);
    }

    #[test]
    fn maps_claude_usage() {
        let usage = map_usage(
            &serde_json::json!({
                "input_tokens": 10,
                "output_tokens": 5,
                "cache_creation_input_tokens": 4,
                "cache_read_input_tokens": 1
            }),
            "claude:chat",
        );

        assert_eq!(usage.input_tokens, 10);
        assert_eq!(usage.output_tokens, 5);
        assert_eq!(usage.cache_creation_tokens, 4);
        assert_eq!(usage.cache_read_tokens, 1);
    }

    #[test]
    fn maps_gemini_usage_from_response() {
        let usage = map_usage_from_response(
            &serde_json::json!({
                "usageMetadata": {
                    "promptTokenCount": 14,
                    "candidatesTokenCount": 6,
                    "cachedContentTokenCount": 2
                }
            }),
            "gemini:chat",
        );

        assert_eq!(usage.input_tokens, 14);
        assert_eq!(usage.output_tokens, 6);
        assert_eq!(usage.cache_read_tokens, 2);
    }
}
