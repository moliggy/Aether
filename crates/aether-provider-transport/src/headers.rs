pub fn should_skip_request_header(name: &str) -> bool {
    let normalized = name.to_ascii_lowercase();
    matches!(
        normalized.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "proxy-connection"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
            | "x-aether-execution-path"
            | "x-aether-dependency-reason"
            | "x-aether-control-execute-fallback"
            | "x-aether-rate-limit-preflight"
    )
}

pub fn should_skip_upstream_passthrough_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "authorization"
            | "x-api-key"
            | "x-goog-api-key"
            | "host"
            | "content-length"
            | "transfer-encoding"
            | "connection"
            | "accept-encoding"
            | "content-encoding"
            | "x-real-ip"
            | "x-real-proto"
            | "x-forwarded-for"
            | "x-forwarded-proto"
            | "x-forwarded-scheme"
            | "x-forwarded-host"
            | "x-forwarded-port"
    ) || should_skip_request_header(name)
}
