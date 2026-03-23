use super::*;

pub(super) fn should_fallback_to_control_sync(
    plan_kind: &str,
    result: &ExecutionResult,
    body_json: Option<&serde_json::Value>,
    has_body_bytes: bool,
    explicit_finalize: bool,
    mapped_error_finalize: bool,
) -> bool {
    if explicit_finalize
        && matches!(
            plan_kind,
            OPENAI_VIDEO_DELETE_SYNC_PLAN_KIND
                | OPENAI_VIDEO_CANCEL_SYNC_PLAN_KIND
                | GEMINI_VIDEO_CANCEL_SYNC_PLAN_KIND
        )
    {
        return false;
    }

    if !matches!(
        plan_kind,
        OPENAI_VIDEO_CREATE_SYNC_PLAN_KIND
            | OPENAI_VIDEO_REMIX_SYNC_PLAN_KIND
            | GEMINI_VIDEO_CREATE_SYNC_PLAN_KIND
            | OPENAI_CHAT_SYNC_PLAN_KIND
            | OPENAI_CLI_SYNC_PLAN_KIND
            | OPENAI_COMPACT_SYNC_PLAN_KIND
            | CLAUDE_CHAT_SYNC_PLAN_KIND
            | GEMINI_CHAT_SYNC_PLAN_KIND
            | CLAUDE_CLI_SYNC_PLAN_KIND
            | GEMINI_CLI_SYNC_PLAN_KIND
    ) {
        return false;
    }

    if explicit_finalize {
        return result.status_code < 400 && body_json.is_none() && !has_body_bytes;
    }

    if mapped_error_finalize {
        return false;
    }

    if result.status_code >= 400 {
        return true;
    }

    let Some(body_json) = body_json else {
        return true;
    };

    body_json.get("error").is_some()
}

pub(super) fn should_finalize_sync_response(report_kind: Option<&str>) -> bool {
    report_kind.is_some_and(|kind| kind.ends_with("_finalize"))
}

pub(super) fn resolve_core_sync_error_finalize_report_kind(
    plan_kind: &str,
    result: &ExecutionResult,
    body_json: Option<&serde_json::Value>,
) -> Option<String> {
    let has_embedded_error = body_json.is_some_and(|value| value.get("error").is_some());
    if result.status_code < 400 && !has_embedded_error {
        return None;
    }

    let report_kind = match plan_kind {
        OPENAI_CHAT_SYNC_PLAN_KIND => "openai_chat_sync_finalize",
        OPENAI_CLI_SYNC_PLAN_KIND => "openai_cli_sync_finalize",
        OPENAI_COMPACT_SYNC_PLAN_KIND => "openai_compact_sync_finalize",
        CLAUDE_CHAT_SYNC_PLAN_KIND => "claude_chat_sync_finalize",
        GEMINI_CHAT_SYNC_PLAN_KIND => "gemini_chat_sync_finalize",
        CLAUDE_CLI_SYNC_PLAN_KIND => "claude_cli_sync_finalize",
        GEMINI_CLI_SYNC_PLAN_KIND => "gemini_cli_sync_finalize",
        _ => return None,
    };

    Some(report_kind.to_string())
}

type DecodedBody = (Vec<u8>, Option<serde_json::Value>, Option<String>);

pub(super) fn decode_execution_result_body(
    result: &ExecutionResult,
    headers: &mut BTreeMap<String, String>,
) -> Result<DecodedBody, GatewayError> {
    let Some(body) = result.body.as_ref() else {
        return Ok((Vec::new(), None, None));
    };

    if let Some(json_body) = body.json_body.clone() {
        headers
            .entry("content-type".to_string())
            .or_insert_with(|| "application/json".to_string());
        let bytes = serde_json::to_vec(&json_body)
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        headers.insert("content-length".to_string(), bytes.len().to_string());
        return Ok((bytes, Some(json_body), None));
    }

    if let Some(body_bytes_b64) = body.body_bytes_b64.clone() {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(&body_bytes_b64)
            .map_err(|err| GatewayError::Internal(err.to_string()))?;
        return Ok((bytes, None, Some(body_bytes_b64)));
    }

    Ok((Vec::new(), None, None))
}
