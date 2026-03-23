use super::*;

pub(super) fn augment_sync_report_context(
    report_context: Option<serde_json::Value>,
    provider_request_headers: &BTreeMap<String, String>,
    provider_request_body: &serde_json::Value,
) -> Result<Option<serde_json::Value>, GatewayError> {
    let mut report_context = match report_context {
        Some(serde_json::Value::Object(map)) => map,
        Some(_) => serde_json::Map::new(),
        None => serde_json::Map::new(),
    };

    report_context.insert(
        "provider_request_headers".to_string(),
        serde_json::to_value(provider_request_headers)
            .map_err(|err| GatewayError::Internal(err.to_string()))?,
    );
    report_context.insert(
        "provider_request_body".to_string(),
        provider_request_body.clone(),
    );

    Ok(Some(serde_json::Value::Object(report_context)))
}

pub(super) fn build_openai_passthrough_headers(
    headers: &http::HeaderMap,
    auth_header: &str,
    auth_value: &str,
    extra_headers: &BTreeMap<String, String>,
    content_type: Option<&str>,
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
        if value.trim().is_empty() {
            continue;
        }
        out.insert(normalized_key, value.trim().to_string());
    }

    out.insert(
        auth_header.trim().to_ascii_lowercase(),
        auth_value.trim().to_string(),
    );
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

pub(super) fn build_openai_chat_url(upstream_base_url: &str, query: Option<&str>) -> String {
    let trimmed = upstream_base_url.trim_end_matches('/');
    let mut url = if trimmed.ends_with("/v1") {
        format!("{trimmed}/chat/completions")
    } else {
        format!("{trimmed}/v1/chat/completions")
    };
    if let Some(query) = query.filter(|value| !value.trim().is_empty()) {
        if url.contains('?') {
            url.push('&');
        } else {
            url.push('?');
        }
        url.push_str(query);
    }
    url
}

pub(super) fn build_openai_cli_url(
    upstream_base_url: &str,
    query: Option<&str>,
    compact: bool,
) -> String {
    let trimmed = upstream_base_url.trim_end_matches('/');
    let suffix = if compact {
        "/responses/compact"
    } else {
        "/responses"
    };
    let mut url = if trimmed.contains("/backend-api/codex")
        || trimmed.ends_with("/codex")
        || trimmed.ends_with("/v1")
    {
        format!("{trimmed}{suffix}")
    } else {
        format!("{trimmed}/v1{suffix}")
    };
    if let Some(query) = query.filter(|value| !value.trim().is_empty()) {
        if url.contains('?') {
            url.push('&');
        } else {
            url.push('?');
        }
        url.push_str(query);
    }
    url
}

pub(crate) fn build_direct_plan_bypass_cache_key(
    plan_kind: &str,
    parts: &http::request::Parts,
    body_bytes: &Bytes,
    decision: &GatewayControlDecision,
) -> String {
    let mut hasher = DefaultHasher::new();
    plan_kind.hash(&mut hasher);
    parts.method.as_str().hash(&mut hasher);
    parts.uri.path().hash(&mut hasher);
    parts.uri.query().unwrap_or_default().hash(&mut hasher);
    decision
        .route_family
        .as_deref()
        .unwrap_or_default()
        .hash(&mut hasher);
    decision
        .route_kind
        .as_deref()
        .unwrap_or_default()
        .hash(&mut hasher);
    decision
        .auth_endpoint_signature
        .as_deref()
        .unwrap_or_default()
        .hash(&mut hasher);
    header_value_str(&parts.headers, http::header::AUTHORIZATION.as_str())
        .unwrap_or_default()
        .hash(&mut hasher);
    header_value_str(&parts.headers, "x-api-key")
        .unwrap_or_default()
        .hash(&mut hasher);
    header_value_str(&parts.headers, "api-key")
        .unwrap_or_default()
        .hash(&mut hasher);
    header_value_str(&parts.headers, http::header::CONTENT_TYPE.as_str())
        .unwrap_or_default()
        .hash(&mut hasher);
    body_bytes.hash(&mut hasher);
    format!("{plan_kind}:{:x}", hasher.finish())
}

pub(crate) fn should_skip_direct_plan(state: &AppState, cache_key: &str) -> bool {
    let Ok(mut cache) = state.direct_plan_bypass_cache.lock() else {
        return false;
    };
    let Some(cached_at) = cache.get(cache_key).copied() else {
        return false;
    };
    if cached_at.elapsed() > DIRECT_PLAN_BYPASS_TTL {
        cache.remove(cache_key);
        return false;
    }
    true
}

pub(crate) fn mark_direct_plan_bypass(state: &AppState, cache_key: String) {
    let Ok(mut cache) = state.direct_plan_bypass_cache.lock() else {
        return;
    };
    cache.retain(|_, cached_at| cached_at.elapsed() <= DIRECT_PLAN_BYPASS_TTL);
    if cache.len() >= DIRECT_PLAN_BYPASS_MAX_ENTRIES {
        if let Some(oldest_key) = cache
            .iter()
            .min_by_key(|(_, cached_at)| *cached_at)
            .map(|(key, _)| key.clone())
        {
            cache.remove(&oldest_key);
        }
    }
    cache.insert(cache_key, Instant::now());
}

pub(crate) fn resolve_direct_executor_stream_plan_kind(
    parts: &http::request::Parts,
    decision: &GatewayControlDecision,
) -> Option<&'static str> {
    if decision.route_class.as_deref() != Some("ai_public") {
        return None;
    }

    if decision.route_family.as_deref() == Some("gemini")
        && decision.route_kind.as_deref() == Some("files")
        && parts.method == http::Method::GET
        && parts.uri.path().ends_with(":download")
    {
        return Some(GEMINI_FILES_DOWNLOAD_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("chat")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/chat/completions"
    {
        return Some(OPENAI_CHAT_STREAM_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("claude")
        && decision.route_kind.as_deref() == Some("chat")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/messages"
    {
        return Some(CLAUDE_CHAT_STREAM_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("claude")
        && decision.route_kind.as_deref() == Some("cli")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/messages"
    {
        return Some(CLAUDE_CLI_STREAM_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("gemini")
        && decision.route_kind.as_deref() == Some("chat")
        && parts.method == http::Method::POST
        && parts.uri.path().ends_with(":streamGenerateContent")
    {
        return Some(GEMINI_CHAT_STREAM_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("gemini")
        && decision.route_kind.as_deref() == Some("cli")
        && parts.method == http::Method::POST
        && parts.uri.path().ends_with(":streamGenerateContent")
    {
        return Some(GEMINI_CLI_STREAM_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("cli")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/responses"
    {
        return Some(OPENAI_CLI_STREAM_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("compact")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/responses/compact"
    {
        return Some(OPENAI_COMPACT_STREAM_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("video")
        && parts.method == http::Method::GET
        && parts.uri.path().ends_with("/content")
    {
        return Some(OPENAI_VIDEO_CONTENT_PLAN_KIND);
    }

    None
}

pub(crate) fn resolve_direct_executor_sync_plan_kind(
    parts: &http::request::Parts,
    decision: &GatewayControlDecision,
) -> Option<&'static str> {
    if decision.route_class.as_deref() != Some("ai_public") {
        return None;
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("video")
        && parts.method == http::Method::POST
        && parts.uri.path().starts_with("/v1/videos/")
        && parts.uri.path().ends_with("/cancel")
    {
        return Some(OPENAI_VIDEO_CANCEL_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("video")
        && parts.method == http::Method::POST
        && parts.uri.path().starts_with("/v1/videos/")
        && parts.uri.path().ends_with("/remix")
    {
        return Some(OPENAI_VIDEO_REMIX_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("video")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/videos"
    {
        return Some(OPENAI_VIDEO_CREATE_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("video")
        && parts.method == http::Method::DELETE
        && parts.uri.path().starts_with("/v1/videos/")
    {
        return Some(OPENAI_VIDEO_DELETE_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("gemini")
        && decision.route_kind.as_deref() == Some("video")
        && parts.method == http::Method::POST
        && parts.uri.path().ends_with(":cancel")
    {
        return Some(GEMINI_VIDEO_CANCEL_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("gemini")
        && decision.route_kind.as_deref() == Some("video")
        && parts.method == http::Method::POST
        && parts.uri.path().ends_with(":predictLongRunning")
    {
        return Some(GEMINI_VIDEO_CREATE_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("chat")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/chat/completions"
    {
        return Some(OPENAI_CHAT_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("cli")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/responses"
    {
        return Some(OPENAI_CLI_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("compact")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/responses/compact"
    {
        return Some(OPENAI_COMPACT_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("claude")
        && decision.route_kind.as_deref() == Some("chat")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/messages"
    {
        return Some(CLAUDE_CHAT_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("claude")
        && decision.route_kind.as_deref() == Some("cli")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/messages"
    {
        return Some(CLAUDE_CLI_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("gemini")
        && decision.route_kind.as_deref() == Some("chat")
        && parts.method == http::Method::POST
        && parts.uri.path().ends_with(":generateContent")
    {
        return Some(GEMINI_CHAT_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("gemini")
        && decision.route_kind.as_deref() == Some("cli")
        && parts.method == http::Method::POST
        && parts.uri.path().ends_with(":generateContent")
    {
        return Some(GEMINI_CLI_SYNC_PLAN_KIND);
    }

    if decision.route_family.as_deref() == Some("gemini")
        && decision.route_kind.as_deref() == Some("files")
    {
        if parts.method == http::Method::POST && parts.uri.path() == "/upload/v1beta/files" {
            return Some(GEMINI_FILES_UPLOAD_PLAN_KIND);
        }
        if parts.method == http::Method::GET && parts.uri.path() == "/v1beta/files" {
            return Some(GEMINI_FILES_LIST_PLAN_KIND);
        }
        if parts.method == http::Method::GET
            && parts.uri.path().starts_with("/v1beta/files/")
            && !parts.uri.path().ends_with(":download")
        {
            return Some(GEMINI_FILES_GET_PLAN_KIND);
        }
        if parts.method == http::Method::DELETE
            && parts.uri.path().starts_with("/v1beta/files/")
            && !parts.uri.path().ends_with(":download")
        {
            return Some(GEMINI_FILES_DELETE_PLAN_KIND);
        }
    }

    None
}

pub(crate) fn is_matching_stream_request(
    plan_kind: &str,
    parts: &http::request::Parts,
    body_json: &serde_json::Value,
) -> bool {
    match plan_kind {
        OPENAI_CHAT_STREAM_PLAN_KIND
        | CLAUDE_CHAT_STREAM_PLAN_KIND
        | OPENAI_CLI_STREAM_PLAN_KIND
        | OPENAI_COMPACT_STREAM_PLAN_KIND
        | CLAUDE_CLI_STREAM_PLAN_KIND => body_json
            .get("stream")
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
        GEMINI_CHAT_STREAM_PLAN_KIND | GEMINI_CLI_STREAM_PLAN_KIND => {
            parts.uri.path().ends_with(":streamGenerateContent")
        }
        _ => true,
    }
}
