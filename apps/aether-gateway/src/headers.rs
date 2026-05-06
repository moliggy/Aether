use std::{collections::BTreeMap, net::SocketAddr};

use crate::constants::*;
use serde_json::{Map, Value};
use uuid::Uuid;

pub(crate) fn extract_or_generate_trace_id(headers: &http::HeaderMap) -> String {
    header_value_str(headers, TRACE_ID_HEADER).unwrap_or_else(|| Uuid::new_v4().to_string())
}

pub(crate) fn header_value_str(headers: &http::HeaderMap, key: &str) -> Option<String> {
    headers
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

pub(crate) fn header_value_u64(headers: &http::HeaderMap, key: &str) -> Option<u64> {
    header_value_str(headers, key).and_then(|value| value.parse::<u64>().ok())
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct RequestOrigin {
    pub(crate) client_ip: Option<String>,
    pub(crate) user_agent: Option<String>,
}

pub(crate) fn request_origin_from_headers(headers: &http::HeaderMap) -> RequestOrigin {
    RequestOrigin {
        client_ip: client_ip_from_headers(headers),
        user_agent: header_value_str(headers, http::header::USER_AGENT.as_str())
            .map(|value| truncate_chars(value.as_str(), 1_000)),
    }
}

pub(crate) fn request_origin_from_headers_and_remote_addr(
    headers: &http::HeaderMap,
    remote_addr: &SocketAddr,
) -> RequestOrigin {
    let mut origin = request_origin_from_headers(headers);
    if origin.client_ip.is_none() {
        origin.client_ip = Some(remote_addr.ip().to_string());
    }
    origin
}

pub(crate) fn request_origin_from_parts(parts: &http::request::Parts) -> RequestOrigin {
    parts
        .extensions
        .get::<RequestOrigin>()
        .cloned()
        .unwrap_or_else(|| request_origin_from_headers(&parts.headers))
}

pub(crate) fn tls_fingerprint_from_headers(headers: &http::HeaderMap) -> Option<Value> {
    let mut object = Map::new();

    copy_tls_header(headers, &mut object, "x-aether-tls-ja3", "ja3");
    copy_tls_header(headers, &mut object, "x-aether-tls-ja3-hash", "ja3_hash");
    copy_tls_header(headers, &mut object, "x-aether-tls-ja4", "ja4");
    copy_tls_header(headers, &mut object, "x-aether-tls-protocol", "protocol");
    copy_tls_header(headers, &mut object, "x-aether-tls-version", "tls_version");
    copy_tls_header(headers, &mut object, "x-aether-tls-cipher", "cipher");
    copy_tls_header(headers, &mut object, "x-aether-tls-sni", "sni");
    copy_tls_header(headers, &mut object, "x-aether-tls-alpn", "alpn");

    if object.is_empty() {
        return None;
    }

    let source = header_value_str(headers, "x-aether-tls-source")
        .unwrap_or_else(|| "forwarded_header".to_string());
    object.insert("source".to_string(), Value::String(source));

    Some(Value::Object(object))
}

fn copy_tls_header(
    headers: &http::HeaderMap,
    object: &mut Map<String, Value>,
    header_name: &str,
    field_name: &str,
) {
    let Some(value) = header_value_str(headers, header_name) else {
        return;
    };
    object.insert(
        field_name.to_string(),
        Value::String(truncate_chars(&value, 512)),
    );
}

fn client_ip_from_headers(headers: &http::HeaderMap) -> Option<String> {
    header_value_str(headers, "x-forwarded-for")
        .and_then(|value| {
            value
                .split(',')
                .map(str::trim)
                .find(|segment| !segment.is_empty() && !segment.eq_ignore_ascii_case("unknown"))
                .map(|segment| truncate_chars(segment, 45))
        })
        .or_else(|| {
            header_value_str(headers, "x-real-ip").and_then(|value| {
                let value = value.trim();
                (!value.is_empty() && !value.eq_ignore_ascii_case("unknown"))
                    .then(|| truncate_chars(value, 45))
            })
        })
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

pub(crate) fn should_skip_request_header(name: &str) -> bool {
    crate::provider_transport::should_skip_request_header(name)
}

pub(crate) fn should_skip_upstream_passthrough_header(name: &str) -> bool {
    crate::provider_transport::should_skip_upstream_passthrough_header(name)
}

pub(crate) fn should_skip_response_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "proxy-connection"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "x-aether-control-executed"
            | "x-aether-control-action"
    )
}

pub(crate) fn collect_control_headers(headers: &http::HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_ascii_lowercase(), value.trim().to_string()))
        })
        .collect()
}

pub(crate) fn is_json_request(headers: &http::HeaderMap) -> bool {
    header_value_str(headers, http::header::CONTENT_TYPE.as_str())
        .map(|value| value.to_ascii_lowercase().contains("application/json"))
        .unwrap_or(false)
}

pub(crate) fn header_equals(
    headers: &reqwest::header::HeaderMap,
    key: &'static str,
    expected: &str,
) -> bool {
    headers
        .get(key)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.eq_ignore_ascii_case(expected))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::{
        request_origin_from_headers, request_origin_from_headers_and_remote_addr,
        tls_fingerprint_from_headers, RequestOrigin,
    };
    use http::{HeaderMap, HeaderValue};
    use serde_json::json;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[test]
    fn request_origin_prefers_first_forwarded_for_ip() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            HeaderValue::from_static(" 203.0.113.8, 10.0.0.1 "),
        );
        headers.insert("x-real-ip", HeaderValue::from_static("198.51.100.4"));
        headers.insert(
            http::header::USER_AGENT,
            HeaderValue::from_static("Claude-Code/1.0"),
        );

        assert_eq!(
            request_origin_from_headers(&headers),
            RequestOrigin {
                client_ip: Some("203.0.113.8".to_string()),
                user_agent: Some("Claude-Code/1.0".to_string()),
            }
        );
    }

    #[test]
    fn request_origin_uses_real_ip_after_empty_forwarded_for_segments() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", HeaderValue::from_static(" , unknown "));
        headers.insert("x-real-ip", HeaderValue::from_static("198.51.100.4"));

        assert_eq!(
            request_origin_from_headers(&headers).client_ip.as_deref(),
            Some("198.51.100.4")
        );
    }

    #[test]
    fn request_origin_falls_back_to_remote_addr() {
        let headers = HeaderMap::new();
        let remote_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 10)), 443);

        assert_eq!(
            request_origin_from_headers_and_remote_addr(&headers, &remote_addr)
                .client_ip
                .as_deref(),
            Some("192.0.2.10")
        );
    }

    #[test]
    fn tls_fingerprint_from_headers_collects_forwarded_tls_fields() {
        let mut headers = HeaderMap::new();
        headers.insert("x-aether-tls-ja3", HeaderValue::from_static("ja3-value"));
        headers.insert(
            "x-aether-tls-ja3-hash",
            HeaderValue::from_static("ja3-hash"),
        );
        headers.insert("x-aether-tls-ja4", HeaderValue::from_static("ja4-value"));
        headers.insert("x-aether-tls-protocol", HeaderValue::from_static("TLSv1.3"));
        headers.insert(
            "x-aether-tls-cipher",
            HeaderValue::from_static("TLS_AES_128_GCM_SHA256"),
        );
        headers.insert(
            "x-aether-tls-sni",
            HeaderValue::from_static("api.example.com"),
        );
        headers.insert("x-aether-tls-alpn", HeaderValue::from_static("h2"));
        headers.insert("x-aether-tls-source", HeaderValue::from_static("nginx"));

        assert_eq!(
            tls_fingerprint_from_headers(&headers),
            Some(json!({
                "source": "nginx",
                "ja3": "ja3-value",
                "ja3_hash": "ja3-hash",
                "ja4": "ja4-value",
                "protocol": "TLSv1.3",
                "cipher": "TLS_AES_128_GCM_SHA256",
                "sni": "api.example.com",
                "alpn": "h2"
            }))
        );
    }
}
