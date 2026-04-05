use crate::control::GatewayControlDecision;

pub(crate) fn resolve_execution_runtime_stream_plan_kind(
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
        return Some("gemini_files_download");
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("chat")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/chat/completions"
    {
        return Some("openai_chat_stream");
    }

    if decision.route_family.as_deref() == Some("claude")
        && decision.route_kind.as_deref() == Some("chat")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/messages"
    {
        return Some("claude_chat_stream");
    }

    if decision.route_family.as_deref() == Some("claude")
        && decision.route_kind.as_deref() == Some("cli")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/messages"
    {
        return Some("claude_cli_stream");
    }

    if decision.route_family.as_deref() == Some("gemini")
        && decision.route_kind.as_deref() == Some("chat")
        && parts.method == http::Method::POST
        && parts.uri.path().ends_with(":streamGenerateContent")
    {
        return Some("gemini_chat_stream");
    }

    if decision.route_family.as_deref() == Some("gemini")
        && decision.route_kind.as_deref() == Some("cli")
        && parts.method == http::Method::POST
        && parts.uri.path().ends_with(":streamGenerateContent")
    {
        return Some("gemini_cli_stream");
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("cli")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/responses"
    {
        return Some("openai_cli_stream");
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("compact")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/responses/compact"
    {
        return Some("openai_compact_stream");
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("video")
        && parts.method == http::Method::GET
        && parts.uri.path().ends_with("/content")
    {
        return Some("openai_video_content");
    }

    None
}

pub(crate) fn resolve_execution_runtime_sync_plan_kind(
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
        return Some("openai_video_cancel_sync");
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("video")
        && parts.method == http::Method::POST
        && parts.uri.path().starts_with("/v1/videos/")
        && parts.uri.path().ends_with("/remix")
    {
        return Some("openai_video_remix_sync");
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("video")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/videos"
    {
        return Some("openai_video_create_sync");
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("video")
        && parts.method == http::Method::DELETE
        && parts.uri.path().starts_with("/v1/videos/")
    {
        return Some("openai_video_delete_sync");
    }

    if decision.route_family.as_deref() == Some("gemini")
        && decision.route_kind.as_deref() == Some("video")
        && parts.method == http::Method::POST
        && parts.uri.path().ends_with(":cancel")
    {
        return Some("gemini_video_cancel_sync");
    }

    if decision.route_family.as_deref() == Some("gemini")
        && decision.route_kind.as_deref() == Some("video")
        && parts.method == http::Method::POST
        && parts.uri.path().ends_with(":predictLongRunning")
    {
        return Some("gemini_video_create_sync");
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("chat")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/chat/completions"
    {
        return Some("openai_chat_sync");
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("cli")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/responses"
    {
        return Some("openai_cli_sync");
    }

    if decision.route_family.as_deref() == Some("openai")
        && decision.route_kind.as_deref() == Some("compact")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/responses/compact"
    {
        return Some("openai_compact_sync");
    }

    if decision.route_family.as_deref() == Some("claude")
        && decision.route_kind.as_deref() == Some("chat")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/messages"
    {
        return Some("claude_chat_sync");
    }

    if decision.route_family.as_deref() == Some("claude")
        && decision.route_kind.as_deref() == Some("cli")
        && parts.method == http::Method::POST
        && parts.uri.path() == "/v1/messages"
    {
        return Some("claude_cli_sync");
    }

    if decision.route_family.as_deref() == Some("gemini")
        && decision.route_kind.as_deref() == Some("chat")
        && parts.method == http::Method::POST
        && parts.uri.path().ends_with(":generateContent")
    {
        return Some("gemini_chat_sync");
    }

    if decision.route_family.as_deref() == Some("gemini")
        && decision.route_kind.as_deref() == Some("cli")
        && parts.method == http::Method::POST
        && parts.uri.path().ends_with(":generateContent")
    {
        return Some("gemini_cli_sync");
    }

    if decision.route_family.as_deref() == Some("gemini")
        && decision.route_kind.as_deref() == Some("files")
    {
        if parts.method == http::Method::POST && parts.uri.path() == "/upload/v1beta/files" {
            return Some("gemini_files_upload");
        }
        if parts.method == http::Method::GET && parts.uri.path() == "/v1beta/files" {
            return Some("gemini_files_list");
        }
        if parts.method == http::Method::GET
            && parts.uri.path().starts_with("/v1beta/files/")
            && !parts.uri.path().ends_with(":download")
        {
            return Some("gemini_files_get");
        }
        if parts.method == http::Method::DELETE
            && parts.uri.path().starts_with("/v1beta/files/")
            && !parts.uri.path().ends_with(":download")
        {
            return Some("gemini_files_delete");
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
        "openai_chat_stream"
        | "claude_chat_stream"
        | "openai_cli_stream"
        | "openai_compact_stream"
        | "claude_cli_stream" => body_json
            .get("stream")
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
        "gemini_chat_stream" | "gemini_cli_stream" => {
            parts.uri.path().ends_with(":streamGenerateContent")
        }
        _ => true,
    }
}

pub(crate) fn supports_sync_scheduler_decision_kind(plan_kind: &str) -> bool {
    matches!(
        plan_kind,
        "openai_chat_sync"
            | "openai_cli_sync"
            | "openai_compact_sync"
            | "claude_chat_sync"
            | "claude_cli_sync"
            | "gemini_chat_sync"
            | "gemini_cli_sync"
            | "gemini_files_upload"
            | "openai_video_create_sync"
            | "openai_video_remix_sync"
            | "openai_video_cancel_sync"
            | "openai_video_delete_sync"
            | "gemini_video_create_sync"
            | "gemini_video_cancel_sync"
            | "gemini_files_get"
            | "gemini_files_list"
            | "gemini_files_delete"
    )
}

pub(crate) fn supports_stream_scheduler_decision_kind(plan_kind: &str) -> bool {
    matches!(
        plan_kind,
        "openai_chat_stream"
            | "claude_chat_stream"
            | "gemini_chat_stream"
            | "openai_cli_stream"
            | "openai_compact_stream"
            | "claude_cli_stream"
            | "gemini_cli_stream"
            | "gemini_files_download"
            | "openai_video_content"
    )
}

#[cfg(test)]
mod tests {
    use axum::http::{Method, Request};

    use super::{
        is_matching_stream_request, resolve_execution_runtime_stream_plan_kind,
        resolve_execution_runtime_sync_plan_kind, supports_stream_scheduler_decision_kind,
        supports_sync_scheduler_decision_kind,
    };
    use crate::control::GatewayControlDecision;

    fn sample_decision(route_family: &str, route_kind: &str) -> GatewayControlDecision {
        GatewayControlDecision {
            public_path: "/".to_string(),
            public_query_string: None,
            route_class: Some("ai_public".to_string()),
            route_family: Some(route_family.to_string()),
            route_kind: Some(route_kind.to_string()),
            auth_context: None,
            admin_principal: None,
            auth_endpoint_signature: None,
            execution_runtime_candidate: true,
            local_auth_rejection: None,
        }
    }

    #[test]
    fn resolves_openai_chat_plan_kinds() {
        let request = Request::builder()
            .method(Method::POST)
            .uri("/v1/chat/completions")
            .body(())
            .expect("request should build");
        let (parts, _) = request.into_parts();
        let decision = sample_decision("openai", "chat");

        assert_eq!(
            resolve_execution_runtime_sync_plan_kind(&parts, &decision),
            Some("openai_chat_sync")
        );
        assert_eq!(
            resolve_execution_runtime_stream_plan_kind(&parts, &decision),
            Some("openai_chat_stream")
        );
    }

    #[test]
    fn stream_matching_requires_openai_stream_flag() {
        let request = Request::builder()
            .method(Method::POST)
            .uri("/v1/chat/completions")
            .body(())
            .expect("request should build");
        let (parts, _) = request.into_parts();

        assert!(!is_matching_stream_request(
            "openai_chat_stream",
            &parts,
            &serde_json::json!({"stream": false}),
        ));
        assert!(is_matching_stream_request(
            "openai_chat_stream",
            &parts,
            &serde_json::json!({"stream": true}),
        ));
        assert!(supports_sync_scheduler_decision_kind("openai_chat_sync"));
        assert!(supports_stream_scheduler_decision_kind(
            "openai_chat_stream"
        ));
    }
}
