use std::collections::{BTreeMap, HashSet};

use regex::Regex;
use serde_json::{Map, Value};

const ORIGINAL_PLACEHOLDER: &str = "{{$original}}";
const CONDITION_SOURCES: &[&str] = &["current", "original"];
const CONDITION_TYPE_VALUES: &[&str] = &["string", "number", "boolean", "array", "object", "null"];

#[derive(Debug, Clone, PartialEq, Eq)]
enum BodyPathSegment {
    Key(String),
    Index(isize),
}

pub fn header_rules_are_locally_supported(rules: Option<&Value>) -> bool {
    let Some(rules) = rules else {
        return true;
    };
    let Some(rules) = rules.as_array() else {
        return false;
    };

    rules.iter().all(|rule| {
        let Some(rule) = rule.as_object() else {
            return false;
        };
        if rule
            .get("condition")
            .is_some_and(|value| !value.is_null() && !condition_is_locally_supported(value))
        {
            return false;
        }

        match rule
            .get("action")
            .and_then(Value::as_str)
            .map(str::trim)
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("set") => {
                rule.get("key")
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.trim().is_empty())
                    && rule.get("value").is_some_and(Value::is_string)
            }
            Some("drop") => rule
                .get("key")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.trim().is_empty()),
            Some("rename") => {
                rule.get("from")
                    .and_then(Value::as_str)
                    .is_some_and(|value| !value.trim().is_empty())
                    && rule
                        .get("to")
                        .and_then(Value::as_str)
                        .is_some_and(|value| !value.trim().is_empty())
            }
            _ => false,
        }
    })
}

pub fn apply_local_header_rules(
    headers: &mut BTreeMap<String, String>,
    rules: Option<&Value>,
    protected_keys: &[&str],
    body: &Value,
    original_body: Option<&Value>,
) -> bool {
    let Some(rules) = rules else {
        return true;
    };
    let Some(rules) = rules.as_array() else {
        return false;
    };
    let protected_keys: HashSet<String> = protected_keys
        .iter()
        .map(|value| value.trim().to_ascii_lowercase())
        .collect();

    for rule in rules {
        let Some(rule) = rule.as_object() else {
            return false;
        };
        if let Some(condition) = rule.get("condition").filter(|value| !value.is_null()) {
            if !condition_is_locally_supported(condition) {
                return false;
            }
            if !evaluate_local_condition(body, condition, original_body) {
                continue;
            }
        }

        match rule
            .get("action")
            .and_then(Value::as_str)
            .map(str::trim)
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("set") => {
                let Some(key) = rule.get("key").and_then(Value::as_str).map(str::trim) else {
                    return false;
                };
                let Some(value) = rule.get("value").and_then(Value::as_str) else {
                    return false;
                };
                let key = key.to_ascii_lowercase();
                if !protected_keys.contains(&key) {
                    headers.insert(key, value.to_string());
                }
            }
            Some("drop") => {
                let Some(key) = rule.get("key").and_then(Value::as_str).map(str::trim) else {
                    return false;
                };
                let key = key.to_ascii_lowercase();
                if !protected_keys.contains(&key) {
                    headers.remove(&key);
                }
            }
            Some("rename") => {
                let Some(from) = rule.get("from").and_then(Value::as_str).map(str::trim) else {
                    return false;
                };
                let Some(to) = rule.get("to").and_then(Value::as_str).map(str::trim) else {
                    return false;
                };
                let from = from.to_ascii_lowercase();
                let to = to.to_ascii_lowercase();
                if protected_keys.contains(&from) || protected_keys.contains(&to) {
                    continue;
                }
                if let Some(value) = headers.remove(&from) {
                    headers.insert(to, value);
                }
            }
            _ => return false,
        }
    }

    true
}

pub fn body_rules_are_locally_supported(rules: Option<&Value>) -> bool {
    let Some(rules) = rules else {
        return true;
    };
    let Some(rules) = rules.as_array() else {
        return false;
    };

    rules.iter().all(|rule| {
        let Some(rule) = rule.as_object() else {
            return false;
        };
        if rule
            .get("condition")
            .is_some_and(|value| !value.is_null() && !condition_is_locally_supported(value))
        {
            return false;
        }

        match rule
            .get("action")
            .and_then(Value::as_str)
            .map(str::trim)
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("set") => {
                rule.get("path")
                    .and_then(Value::as_str)
                    .and_then(parse_body_path)
                    .is_some()
                    && !rule.get("value").is_some_and(contains_original_placeholder)
            }
            Some("drop") => rule
                .get("path")
                .and_then(Value::as_str)
                .and_then(parse_body_path)
                .is_some(),
            Some("rename") => {
                rule.get("from")
                    .and_then(Value::as_str)
                    .and_then(parse_body_path)
                    .is_some()
                    && rule
                        .get("to")
                        .and_then(Value::as_str)
                        .and_then(parse_body_path)
                        .is_some()
            }
            _ => false,
        }
    })
}

pub fn apply_local_body_rules(
    body: &mut Value,
    rules: Option<&Value>,
    original_body: Option<&Value>,
) -> bool {
    let Some(rules) = rules else {
        return true;
    };
    let Some(rules) = rules.as_array() else {
        return false;
    };

    for rule in rules {
        let Some(rule) = rule.as_object() else {
            return false;
        };
        if let Some(condition) = rule.get("condition").filter(|value| !value.is_null()) {
            if !condition_is_locally_supported(condition) {
                return false;
            }
            if !evaluate_local_condition(body, condition, original_body) {
                continue;
            }
        }

        match rule
            .get("action")
            .and_then(Value::as_str)
            .map(str::trim)
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("set") => {
                let Some(path) = rule
                    .get("path")
                    .and_then(Value::as_str)
                    .and_then(parse_body_path)
                else {
                    return false;
                };
                let value = rule.get("value").cloned().unwrap_or(Value::Null);
                if contains_original_placeholder(&value) {
                    return false;
                }
                let _ = set_nested_value(body, &path, value);
            }
            Some("drop") => {
                let Some(path) = rule
                    .get("path")
                    .and_then(Value::as_str)
                    .and_then(parse_body_path)
                else {
                    return false;
                };
                let _ = delete_nested_value(body, &path);
            }
            Some("rename") => {
                let Some(from) = rule
                    .get("from")
                    .and_then(Value::as_str)
                    .and_then(parse_body_path)
                else {
                    return false;
                };
                let Some(to) = rule
                    .get("to")
                    .and_then(Value::as_str)
                    .and_then(parse_body_path)
                else {
                    return false;
                };
                let _ = rename_nested_value(body, &from, &to);
            }
            _ => return false,
        }
    }

    true
}

fn condition_is_locally_supported(condition: &Value) -> bool {
    let Some(condition) = condition.as_object() else {
        return false;
    };

    if let Some(children) = condition.get("all").and_then(Value::as_array) {
        return !children.is_empty() && children.iter().all(condition_is_locally_supported);
    }
    if let Some(children) = condition.get("any").and_then(Value::as_array) {
        return !children.is_empty() && children.iter().all(condition_is_locally_supported);
    }

    let source = condition
        .get("source")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("current");
    if !CONDITION_SOURCES.contains(&source) {
        return false;
    }

    let Some(op) = condition.get("op").and_then(Value::as_str).map(str::trim) else {
        return false;
    };
    let Some(path) = condition
        .get("path")
        .and_then(Value::as_str)
        .map(str::trim)
        .and_then(parse_body_path)
    else {
        return false;
    };
    if path.is_empty() {
        return false;
    }

    match op {
        "exists" | "not_exists" | "eq" | "neq" => true,
        "gt" | "lt" | "gte" | "lte" => condition
            .get("value")
            .is_some_and(|value| value.as_f64().is_some() && !value.is_boolean()),
        "starts_with" | "ends_with" | "matches" => condition
            .get("value")
            .and_then(Value::as_str)
            .is_some_and(|value| {
                if op == "matches" {
                    Regex::new(value).is_ok()
                } else {
                    true
                }
            }),
        "contains" => condition.get("value").is_some(),
        "in" => condition.get("value").is_some_and(Value::is_array),
        "type_is" => condition
            .get("value")
            .and_then(Value::as_str)
            .is_some_and(|value| CONDITION_TYPE_VALUES.contains(&value)),
        _ => false,
    }
}

fn evaluate_local_condition(
    body: &Value,
    condition: &Value,
    original_body: Option<&Value>,
) -> bool {
    let Some(condition) = condition.as_object() else {
        return false;
    };

    if let Some(children) = condition.get("all").and_then(Value::as_array) {
        return !children.is_empty()
            && children
                .iter()
                .all(|child| evaluate_local_condition(body, child, original_body));
    }
    if let Some(children) = condition.get("any").and_then(Value::as_array) {
        return !children.is_empty()
            && children
                .iter()
                .any(|child| evaluate_local_condition(body, child, original_body));
    }

    let source = condition
        .get("source")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("current");
    let target = if source.eq_ignore_ascii_case("original") {
        original_body.unwrap_or(body)
    } else {
        body
    };

    let Some(op) = condition.get("op").and_then(Value::as_str).map(str::trim) else {
        return false;
    };
    let Some(path) = condition
        .get("path")
        .and_then(Value::as_str)
        .map(str::trim)
        .and_then(parse_body_path)
    else {
        return false;
    };

    let current_value = get_nested_value(target, &path);
    if op == "exists" {
        return current_value.is_some();
    }
    if op == "not_exists" {
        return current_value.is_none();
    }

    let Some(current_value) = current_value else {
        return false;
    };
    let expected = condition.get("value");

    match op {
        "eq" => expected == Some(&current_value),
        "neq" => expected != Some(&current_value),
        "gt" | "lt" | "gte" | "lte" => {
            let Some(current) = json_number(&current_value) else {
                return false;
            };
            let Some(expected) = expected.and_then(json_number) else {
                return false;
            };
            match op {
                "gt" => current > expected,
                "lt" => current < expected,
                "gte" => current >= expected,
                "lte" => current <= expected,
                _ => false,
            }
        }
        "starts_with" => current_value
            .as_str()
            .zip(expected.and_then(Value::as_str))
            .is_some_and(|(current, expected)| current.starts_with(expected)),
        "ends_with" => current_value
            .as_str()
            .zip(expected.and_then(Value::as_str))
            .is_some_and(|(current, expected)| current.ends_with(expected)),
        "contains" => match (current_value, expected) {
            (Value::String(current), Some(Value::String(expected))) => current.contains(expected),
            (Value::Array(current), Some(expected)) => {
                current.iter().any(|value| value == expected)
            }
            _ => false,
        },
        "matches" => current_value
            .as_str()
            .zip(expected.and_then(Value::as_str))
            .is_some_and(|(current, expected)| {
                Regex::new(expected)
                    .map(|pattern| pattern.is_match(current))
                    .unwrap_or(false)
            }),
        "in" => expected
            .and_then(Value::as_array)
            .is_some_and(|values| values.iter().any(|value| value == &current_value)),
        "type_is" => expected
            .and_then(Value::as_str)
            .is_some_and(|expected| match expected {
                "string" => current_value.is_string(),
                "number" => current_value.as_f64().is_some() && !current_value.is_boolean(),
                "boolean" => current_value.is_boolean(),
                "array" => current_value.is_array(),
                "object" => current_value.is_object(),
                "null" => current_value.is_null(),
                _ => false,
            }),
        _ => false,
    }
}

fn json_number(value: &Value) -> Option<f64> {
    value.as_f64().filter(|_| !value.is_boolean())
}

fn parse_body_path(path: &str) -> Option<Vec<BodyPathSegment>> {
    let raw = path.trim();
    if raw.is_empty() {
        return None;
    }

    let chars: Vec<char> = raw.chars().collect();
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut expect_key = true;
    let mut index = 0usize;

    while index < chars.len() {
        let ch = chars[index];
        if ch == '\\' && chars.get(index + 1).copied() == Some('.') {
            current.push('.');
            expect_key = false;
            index += 2;
            continue;
        }

        if ch == '.' {
            if !current.is_empty() {
                parts.push(BodyPathSegment::Key(std::mem::take(&mut current)));
            } else if expect_key {
                return None;
            }
            expect_key = true;
            index += 1;
            continue;
        }

        if ch == '[' {
            if !current.is_empty() {
                parts.push(BodyPathSegment::Key(std::mem::take(&mut current)));
            }

            let mut close_index = index + 1;
            while close_index < chars.len() && chars[close_index] != ']' {
                close_index += 1;
            }
            if close_index >= chars.len() {
                return None;
            }

            let inner = chars[index + 1..close_index]
                .iter()
                .collect::<String>()
                .trim()
                .to_string();
            if inner.is_empty() || inner == "*" {
                return None;
            }
            let Ok(index_value) = inner.parse::<isize>() else {
                return None;
            };
            parts.push(BodyPathSegment::Index(index_value));
            expect_key = false;
            index = close_index + 1;
            continue;
        }

        current.push(ch);
        expect_key = false;
        index += 1;
    }

    if !current.is_empty() {
        parts.push(BodyPathSegment::Key(current));
    } else if expect_key {
        return None;
    }

    (!parts.is_empty()).then_some(parts)
}

fn contains_original_placeholder(value: &Value) -> bool {
    match value {
        Value::String(value) => value.contains(ORIGINAL_PLACEHOLDER),
        Value::Array(items) => items.iter().any(contains_original_placeholder),
        Value::Object(items) => items.values().any(contains_original_placeholder),
        _ => false,
    }
}

fn resolve_index(len: usize, index: isize) -> Option<usize> {
    if index >= 0 {
        ((index as usize) < len).then_some(index as usize)
    } else {
        let resolved = len as isize + index;
        (resolved >= 0).then_some(resolved as usize)
    }
}

fn get_nested_value(value: &Value, path: &[BodyPathSegment]) -> Option<Value> {
    let mut current = value;
    for segment in path {
        match segment {
            BodyPathSegment::Key(key) => {
                current = current.as_object()?.get(key)?;
            }
            BodyPathSegment::Index(index) => {
                let values = current.as_array()?;
                let resolved = resolve_index(values.len(), *index)?;
                current = values.get(resolved)?;
            }
        }
    }
    Some(current.clone())
}

fn get_existing_child_mut<'a>(
    current: &'a mut Value,
    segment: &BodyPathSegment,
) -> Option<&'a mut Value> {
    match segment {
        BodyPathSegment::Key(key) => current.as_object_mut()?.get_mut(key),
        BodyPathSegment::Index(index) => {
            let values = current.as_array_mut()?;
            let resolved = resolve_index(values.len(), *index)?;
            values.get_mut(resolved)
        }
    }
}

fn set_nested_value(current: &mut Value, path: &[BodyPathSegment], value: Value) -> bool {
    let Some((last, parents)) = path.split_last() else {
        return false;
    };
    let mut current = current;

    for (offset, segment) in parents.iter().enumerate() {
        let next = &path[offset + 1];
        match segment {
            BodyPathSegment::Key(key) => {
                let Some(object) = current.as_object_mut() else {
                    return false;
                };
                match next {
                    BodyPathSegment::Key(_) => {
                        let child = object
                            .entry(key.clone())
                            .or_insert_with(|| Value::Object(Map::new()));
                        if !child.is_object() {
                            *child = Value::Object(Map::new());
                        }
                        current = child;
                    }
                    BodyPathSegment::Index(_) => {
                        let Some(child) = object.get_mut(key) else {
                            return false;
                        };
                        if !child.is_array() {
                            return false;
                        }
                        current = child;
                    }
                }
            }
            BodyPathSegment::Index(index) => {
                let Some(values) = current.as_array_mut() else {
                    return false;
                };
                let Some(resolved) = resolve_index(values.len(), *index) else {
                    return false;
                };
                current = &mut values[resolved];
            }
        }
    }

    match last {
        BodyPathSegment::Key(key) => {
            let Some(object) = current.as_object_mut() else {
                return false;
            };
            object.insert(key.clone(), value);
            true
        }
        BodyPathSegment::Index(index) => {
            let Some(values) = current.as_array_mut() else {
                return false;
            };
            let Some(resolved) = resolve_index(values.len(), *index) else {
                return false;
            };
            values[resolved] = value;
            true
        }
    }
}

fn delete_nested_value(current: &mut Value, path: &[BodyPathSegment]) -> bool {
    let Some((last, parents)) = path.split_last() else {
        return false;
    };
    let mut current = current;
    for segment in parents {
        let Some(child) = get_existing_child_mut(current, segment) else {
            return false;
        };
        current = child;
    }

    match last {
        BodyPathSegment::Key(key) => current
            .as_object_mut()
            .and_then(|object| object.remove(key))
            .is_some(),
        BodyPathSegment::Index(index) => {
            let Some(values) = current.as_array_mut() else {
                return false;
            };
            let Some(resolved) = resolve_index(values.len(), *index) else {
                return false;
            };
            values.remove(resolved);
            true
        }
    }
}

fn rename_nested_value(
    current: &mut Value,
    from: &[BodyPathSegment],
    to: &[BodyPathSegment],
) -> bool {
    if from == to {
        return get_nested_value(current, from).is_some();
    }

    let Some(value) = get_nested_value(current, from) else {
        return false;
    };
    if !set_nested_value(current, to, value) {
        return false;
    }
    delete_nested_value(current, from)
}

#[cfg(test)]
mod tests {
    use super::{
        apply_local_body_rules, apply_local_header_rules, body_rules_are_locally_supported,
        header_rules_are_locally_supported,
    };

    #[test]
    fn header_rules_allow_simple_set_drop_and_rename() {
        let rules = serde_json::json!([
            {"action":"set","key":"x-added","value":"1"},
            {"action":"drop","key":"x-drop"},
            {"action":"rename","from":"x-old","to":"x-new"}
        ]);
        assert!(header_rules_are_locally_supported(Some(&rules)));

        let mut headers = std::collections::BTreeMap::from([
            ("x-drop".to_string(), "drop-me".to_string()),
            ("x-old".to_string(), "old-value".to_string()),
            ("authorization".to_string(), "Bearer keep".to_string()),
        ]);
        assert!(apply_local_header_rules(
            &mut headers,
            Some(&rules),
            &["authorization", "content-type"],
            &serde_json::json!({}),
            None,
        ));
        assert_eq!(headers.get("x-added").map(String::as_str), Some("1"));
        assert!(!headers.contains_key("x-drop"));
        assert_eq!(headers.get("x-new").map(String::as_str), Some("old-value"));
        assert_eq!(
            headers.get("authorization").map(String::as_str),
            Some("Bearer keep")
        );
    }

    #[test]
    fn header_rules_allow_simple_conditions() {
        let rules = serde_json::json!([
            {"action":"set","key":"x-added","value":"1","condition":{"path":"metadata.mode","op":"eq","value":"safe"}},
            {"action":"set","key":"x-from-original","value":"1","condition":{"path":"metadata.client","op":"exists","source":"original"}}
        ]);
        assert!(header_rules_are_locally_supported(Some(&rules)));

        let mut headers = std::collections::BTreeMap::new();
        assert!(apply_local_header_rules(
            &mut headers,
            Some(&rules),
            &[],
            &serde_json::json!({"metadata":{"mode":"safe"}}),
            Some(&serde_json::json!({"metadata":{"client":"desktop"}})),
        ));
        assert_eq!(headers.get("x-added").map(String::as_str), Some("1"));
        assert_eq!(
            headers.get("x-from-original").map(String::as_str),
            Some("1")
        );
    }

    #[test]
    fn body_rules_allow_simple_nested_set_drop_and_rename() {
        let rules = serde_json::json!([
            {"action":"set","path":"metadata.mode","value":"safe"},
            {"action":"drop","path":"tools[1]"},
            {"action":"rename","from":"messages[0].content","to":"messages[0].text"}
        ]);
        assert!(body_rules_are_locally_supported(Some(&rules)));

        let mut body = serde_json::json!({
            "messages": [{"content":"hello"}],
            "tools": [{"name":"a"},{"name":"b"}]
        });
        assert!(apply_local_body_rules(&mut body, Some(&rules), None));
        assert_eq!(body["metadata"]["mode"], "safe");
        assert_eq!(body["tools"], serde_json::json!([{"name":"a"}]));
        assert_eq!(body["messages"][0]["text"], "hello");
        assert!(body["messages"][0].get("content").is_none());
    }

    #[test]
    fn body_rules_allow_simple_conditions() {
        let rules = serde_json::json!([
            {"action":"set","path":"instructions","value":"You are GPT-5.","condition":{"path":"instructions","op":"not_exists"}},
            {"action":"set","path":"metadata.origin","value":"desktop","condition":{"path":"metadata.client","op":"exists","source":"original"}}
        ]);
        assert!(body_rules_are_locally_supported(Some(&rules)));

        let original = serde_json::json!({
            "metadata": {
                "client": "desktop"
            }
        });
        let mut body = serde_json::json!({
            "metadata": {
                "mode": "safe"
            }
        });
        assert!(apply_local_body_rules(
            &mut body,
            Some(&rules),
            Some(&original)
        ));
        assert_eq!(body["instructions"], "You are GPT-5.");
        assert_eq!(body["metadata"]["origin"], "desktop");
    }

    #[test]
    fn body_rules_reject_placeholder_and_wildcard_paths() {
        let placeholder_rules =
            serde_json::json!([{"action":"set","path":"model","value":"{{$original}}"}]);
        assert!(!body_rules_are_locally_supported(Some(&placeholder_rules)));

        let wildcard_rules = serde_json::json!([{"action":"drop","path":"items[*].value"}]);
        assert!(!body_rules_are_locally_supported(Some(&wildcard_rules)));
    }
}
