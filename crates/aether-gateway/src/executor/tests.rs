use axum::http::Request;
use serde_json::json;

use super::{
    plan_builders::{
        build_gemini_stream_plan_from_decision, build_gemini_sync_plan_from_decision,
        build_openai_cli_stream_plan_from_decision, build_openai_cli_sync_plan_from_decision,
        build_passthrough_sync_plan_from_decision, build_standard_stream_plan_from_decision,
        build_standard_sync_plan_from_decision,
    },
    submission::resolve_core_error_background_report_kind,
    sync::{
        resolve_local_sync_error_background_report_kind,
        resolve_local_sync_success_background_report_kind,
    },
    GatewayControlSyncDecisionResponse,
};

fn test_parts() -> http::request::Parts {
    let request = Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .body(())
        .expect("request");
    let (parts, _) = request.into_parts();
    parts
}

fn missing_exact_provider_request_payload(
    decision_kind: &str,
) -> GatewayControlSyncDecisionResponse {
    GatewayControlSyncDecisionResponse {
        action: "direct_executor".to_string(),
        decision_kind: Some(decision_kind.to_string()),
        request_id: Some("req_123".to_string()),
        candidate_id: Some("cand_123".to_string()),
        provider_name: Some("provider".to_string()),
        provider_id: Some("provider_id".to_string()),
        endpoint_id: Some("endpoint_id".to_string()),
        key_id: Some("key_id".to_string()),
        upstream_base_url: Some("https://example.com".to_string()),
        upstream_url: Some("https://example.com/v1/messages".to_string()),
        provider_request_method: None,
        auth_header: Some("authorization".to_string()),
        auth_value: Some("Bearer test".to_string()),
        provider_api_format: Some("anthropic".to_string()),
        client_api_format: Some("openai".to_string()),
        model_name: Some("model".to_string()),
        mapped_model: Some("model".to_string()),
        prompt_cache_key: None,
        extra_headers: Default::default(),
        provider_request_headers: Default::default(),
        provider_request_body: None,
        content_type: Some("application/json".to_string()),
        proxy: None,
        tls_profile: None,
        timeouts: None,
        upstream_is_stream: false,
        report_kind: Some("openai_chat_sync_success".to_string()),
        report_context: Some(json!({})),
        auth_context: None,
    }
}

#[test]
fn resolve_core_error_background_report_kind_maps_all_core_finalize_kinds() {
    let cases = [
        ("openai_chat_sync_finalize", Some("openai_chat_sync_error")),
        ("claude_chat_sync_finalize", Some("claude_chat_sync_error")),
        ("gemini_chat_sync_finalize", Some("gemini_chat_sync_error")),
        ("openai_cli_sync_finalize", Some("openai_cli_sync_error")),
        (
            "openai_compact_sync_finalize",
            Some("openai_compact_sync_error"),
        ),
        ("claude_cli_sync_finalize", Some("claude_cli_sync_error")),
        ("gemini_cli_sync_finalize", Some("gemini_cli_sync_error")),
        ("openai_video_create_sync_finalize", None),
        ("gemini_video_cancel_sync_finalize", None),
        ("unknown_finalize_kind", None),
    ];

    for (report_kind, expected) in cases {
        assert_eq!(
            resolve_core_error_background_report_kind(report_kind),
            expected.map(str::to_string),
            "unexpected mapping for {report_kind}"
        );
    }
}

#[test]
fn resolve_local_sync_success_background_report_kind_maps_video_finalize_kinds() {
    let cases = [
        (
            "openai_video_delete_sync_finalize",
            Some("openai_video_delete_sync_success"),
        ),
        (
            "openai_video_cancel_sync_finalize",
            Some("openai_video_cancel_sync_success"),
        ),
        (
            "gemini_video_cancel_sync_finalize",
            Some("gemini_video_cancel_sync_success"),
        ),
        ("openai_video_create_sync_finalize", None),
        ("unknown_finalize_kind", None),
    ];

    for (report_kind, expected) in cases {
        assert_eq!(
            resolve_local_sync_success_background_report_kind(report_kind),
            expected.map(str::to_string),
            "unexpected mapping for {report_kind}"
        );
    }
}

#[test]
fn resolve_local_sync_error_background_report_kind_maps_video_finalize_kinds() {
    let cases = [
        (
            "openai_video_create_sync_finalize",
            Some("openai_video_create_sync_error"),
        ),
        (
            "openai_video_remix_sync_finalize",
            Some("openai_video_remix_sync_error"),
        ),
        (
            "gemini_video_create_sync_finalize",
            Some("gemini_video_create_sync_error"),
        ),
        (
            "openai_video_delete_sync_finalize",
            Some("openai_video_delete_sync_error"),
        ),
        (
            "openai_video_cancel_sync_finalize",
            Some("openai_video_cancel_sync_error"),
        ),
        (
            "gemini_video_cancel_sync_finalize",
            Some("gemini_video_cancel_sync_error"),
        ),
        ("unknown_finalize_kind", None),
    ];

    for (report_kind, expected) in cases {
        assert_eq!(
            resolve_local_sync_error_background_report_kind(report_kind),
            expected.map(str::to_string),
            "unexpected mapping for {report_kind}"
        );
    }
}

#[test]
fn generic_decision_builders_require_exact_provider_request() {
    let parts = test_parts();
    let body_json = json!({"messages":[{"role":"user","content":"hi"}]});

    assert!(build_openai_cli_stream_plan_from_decision(
        &parts,
        &body_json,
        missing_exact_provider_request_payload("openai_cli_stream"),
        false,
    )
    .expect("builder should not error")
    .is_none());
    assert!(build_openai_cli_stream_plan_from_decision(
        &parts,
        &body_json,
        missing_exact_provider_request_payload("openai_compact_stream"),
        true,
    )
    .expect("builder should not error")
    .is_none());
    assert!(build_standard_stream_plan_from_decision(
        &parts,
        &body_json,
        missing_exact_provider_request_payload("claude_chat_stream"),
        false,
    )
    .expect("builder should not error")
    .is_none());
    assert!(build_gemini_stream_plan_from_decision(
        &parts,
        &body_json,
        missing_exact_provider_request_payload("gemini_chat_stream"),
    )
    .expect("builder should not error")
    .is_none());
    assert!(build_openai_cli_sync_plan_from_decision(
        &parts,
        &body_json,
        missing_exact_provider_request_payload("openai_cli_sync"),
        false,
    )
    .expect("builder should not error")
    .is_none());
    assert!(build_standard_sync_plan_from_decision(
        &parts,
        &body_json,
        missing_exact_provider_request_payload("claude_chat_sync"),
    )
    .expect("builder should not error")
    .is_none());
    assert!(build_gemini_sync_plan_from_decision(
        &parts,
        &body_json,
        missing_exact_provider_request_payload("gemini_chat_sync"),
    )
    .expect("builder should not error")
    .is_none());
}

#[test]
fn passthrough_sync_plan_uses_provider_request_method_override() {
    let request = Request::builder()
        .method("POST")
        .uri("/v1/videos/task-123/cancel")
        .body(())
        .expect("request");
    let (parts, _) = request.into_parts();

    let mut payload = missing_exact_provider_request_payload("openai_video_cancel_sync");
    payload.provider_name = Some("openai".to_string());
    payload.provider_api_format = Some("openai:video".to_string());
    payload.client_api_format = Some("openai:video".to_string());
    payload.model_name = Some("sora-2".to_string());
    payload.upstream_url = Some("https://api.openai.example/v1/videos/ext-123".to_string());
    payload.provider_request_method = Some("DELETE".to_string());
    payload.provider_request_headers = [(
        "authorization".to_string(),
        "Bearer upstream-key".to_string(),
    )]
    .into_iter()
    .collect();

    let plan_and_report = build_passthrough_sync_plan_from_decision(&parts, payload)
        .expect("builder should not error")
        .expect("plan should be built");

    assert_eq!(plan_and_report.plan.method, "DELETE");
    assert_eq!(
        plan_and_report.plan.url,
        "https://api.openai.example/v1/videos/ext-123"
    );
}
