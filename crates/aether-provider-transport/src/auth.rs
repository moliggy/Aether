use std::collections::BTreeMap;

use super::headers::should_skip_upstream_passthrough_header;
use super::snapshot::GatewayProviderTransportSnapshot;

fn collect_passthrough_headers(
    headers: &http::HeaderMap,
    extra_headers: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for (name, value) in headers.iter() {
        let Ok(value) = value.to_str() else {
            continue;
        };
        let key = name.as_str().to_ascii_lowercase();
        if should_skip_upstream_passthrough_header(&key) {
            continue;
        }
        let value = value.trim();
        if value.is_empty() {
            continue;
        }
        out.insert(key, value.to_string());
    }

    for (key, value) in extra_headers {
        let normalized_key = key.to_ascii_lowercase();
        let value = value.trim();
        if value.is_empty() {
            continue;
        }
        out.insert(normalized_key, value.to_string());
    }

    out
}

pub fn build_passthrough_headers(
    headers: &http::HeaderMap,
    extra_headers: &BTreeMap<String, String>,
    content_type: Option<&str>,
) -> BTreeMap<String, String> {
    let mut out = collect_passthrough_headers(headers, extra_headers);
    out.entry("content-type".to_string()).or_insert_with(|| {
        content_type
            .filter(|value| !value.trim().is_empty())
            .unwrap_or("application/json")
            .trim()
            .to_string()
    });
    out.remove("content-length");
    out
}

pub fn build_openai_passthrough_headers(
    headers: &http::HeaderMap,
    auth_header: &str,
    auth_value: &str,
    extra_headers: &BTreeMap<String, String>,
    content_type: Option<&str>,
) -> BTreeMap<String, String> {
    let mut out = build_passthrough_headers(headers, extra_headers, content_type);
    ensure_upstream_auth_header(&mut out, auth_header, auth_value);
    out
}

pub fn build_passthrough_headers_with_auth(
    headers: &http::HeaderMap,
    auth_header: &str,
    auth_value: &str,
    extra_headers: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut out = collect_passthrough_headers(headers, extra_headers);
    ensure_upstream_auth_header(&mut out, auth_header, auth_value);
    out.remove("content-length");
    out
}

pub fn ensure_upstream_auth_header(
    headers: &mut BTreeMap<String, String>,
    auth_header: &str,
    auth_value: &str,
) {
    let header_name = auth_header.trim().to_ascii_lowercase();
    let header_value = auth_value.trim();
    if header_name.is_empty() || header_value.is_empty() {
        return;
    }

    if headers
        .get(&header_name)
        .map(|value| value.trim().is_empty())
        .unwrap_or(true)
    {
        headers.insert(header_name, header_value.to_string());
    }
}

pub fn resolve_local_openai_chat_auth(
    transport: &GatewayProviderTransportSnapshot,
) -> Option<(String, String)> {
    let auth_type = transport.key.auth_type.trim().to_ascii_lowercase();
    if !matches!(auth_type.as_str(), "api_key" | "bearer") {
        return None;
    }
    let secret = transport.key.decrypted_api_key.trim();
    if secret.is_empty() {
        return None;
    }

    Some(("authorization".to_string(), format!("Bearer {secret}")))
}

pub fn resolve_local_standard_auth(
    transport: &GatewayProviderTransportSnapshot,
) -> Option<(String, String)> {
    let auth_type = transport.key.auth_type.trim().to_ascii_lowercase();
    let secret = transport.key.decrypted_api_key.trim();
    if secret.is_empty() {
        return None;
    }

    match auth_type.as_str() {
        "api_key" => Some(("x-api-key".to_string(), secret.to_string())),
        "bearer" => Some(("authorization".to_string(), format!("Bearer {secret}"))),
        _ => None,
    }
}

pub fn resolve_local_gemini_auth(
    transport: &GatewayProviderTransportSnapshot,
) -> Option<(String, String)> {
    let auth_type = transport.key.auth_type.trim().to_ascii_lowercase();
    let secret = transport.key.decrypted_api_key.trim();
    if secret.is_empty() {
        return None;
    }

    match auth_type.as_str() {
        "api_key" => Some(("x-goog-api-key".to_string(), secret.to_string())),
        "bearer" => Some(("authorization".to_string(), format!("Bearer {secret}"))),
        _ => None,
    }
}
