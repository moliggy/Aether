use base64::Engine as _;
use futures_util::TryStreamExt;

use super::super::submission::{
    maybe_build_local_core_error_response, resolve_core_error_background_report_kind,
    spawn_sync_finalize, spawn_sync_report, submit_stream_report, submit_sync_finalize,
};
use super::super::*;
use super::error::{
    build_executor_error_response, collect_error_body, decode_stream_error_body,
    inspect_prefetched_stream_body, read_next_frame,
    resolve_core_stream_direct_finalize_report_kind,
    resolve_core_stream_error_finalize_report_kind, should_fallback_to_control_stream,
    StreamPrefetchInspection,
};

#[allow(clippy::too_many_arguments)] // internal function, grouping would add unnecessary indirection
pub(super) async fn execute_executor_stream(
    state: &AppState,
    control_base_url: &str,
    executor_base_url: &str,
    plan: ExecutionPlan,
    trace_id: &str,
    decision: &GatewayControlDecision,
    plan_kind: &str,
    report_kind: Option<String>,
    report_context: Option<serde_json::Value>,
) -> Result<Option<Response<Body>>, GatewayError> {
    let response = match state
        .client
        .post(format!("{executor_base_url}/v1/execute/stream"))
        .header(TRACE_ID_HEADER, trace_id)
        .json(&plan)
        .send()
        .await
    {
        Ok(response) => response,
        Err(err) => {
            warn!(trace_id = %trace_id, error = %err, "gateway direct executor stream unavailable");
            return Ok(None);
        }
    };

    if response.status() != http::StatusCode::OK {
        return Ok(Some(build_client_response(
            response,
            trace_id,
            Some(decision),
        )?));
    }

    let stream = response
        .bytes_stream()
        .map_err(|err| IoError::other(err.to_string()));
    let reader = StreamReader::new(stream);
    let mut lines = FramedRead::new(reader, LinesCodec::new());

    let first_frame = read_next_frame(&mut lines).await?.ok_or_else(|| {
        GatewayError::Internal("executor stream ended before headers frame".to_string())
    })?;
    let StreamFramePayload::Headers {
        status_code,
        mut headers,
    } = first_frame.payload
    else {
        return Err(GatewayError::Internal(
            "executor stream must start with headers frame".to_string(),
        ));
    };

    let stream_error_finalize_kind =
        resolve_core_stream_error_finalize_report_kind(plan_kind, status_code);

    if should_fallback_to_control_stream(
        plan_kind,
        status_code,
        stream_error_finalize_kind.is_some(),
    ) {
        return Ok(None);
    }

    if status_code >= 400 {
        let error_body = collect_error_body(&mut lines).await?;
        if let Some(report_kind) = stream_error_finalize_kind {
            let (body_json, body_base64) = decode_stream_error_body(&headers, &error_body);
            let payload = GatewaySyncReportRequest {
                trace_id: trace_id.to_string(),
                report_kind,
                report_context,
                status_code,
                headers: headers.clone(),
                body_json,
                client_body_json: None,
                body_base64,
                telemetry: None,
            };
            if let Some(response) =
                maybe_build_local_core_error_response(trace_id, decision, &payload)?
            {
                if let Some(error_report_kind) =
                    resolve_core_error_background_report_kind(payload.report_kind.as_str())
                {
                    let mut report_payload = payload.clone();
                    report_payload.report_kind = error_report_kind;
                    spawn_sync_report(
                        state.clone(),
                        control_base_url.to_string(),
                        trace_id.to_string(),
                        report_payload,
                    );
                } else {
                    spawn_sync_finalize(
                        state.clone(),
                        control_base_url.to_string(),
                        trace_id.to_string(),
                        payload,
                    );
                }
                return Ok(Some(response));
            }
            let response = submit_sync_finalize(state, control_base_url, trace_id, payload).await?;
            return Ok(Some(build_client_response(
                response,
                trace_id,
                Some(decision),
            )?));
        }
        return Ok(Some(build_executor_error_response(
            trace_id,
            decision,
            plan_kind,
            status_code,
            headers,
            error_body,
        )?));
    }

    let direct_stream_finalize_kind = resolve_core_stream_direct_finalize_report_kind(plan_kind);
    let mut local_stream_rewriter = maybe_build_local_stream_rewriter(report_context.as_ref());
    if local_stream_rewriter.is_some() {
        headers.remove("content-encoding");
        headers.remove("content-length");
        headers.insert("content-type".to_string(), "text/event-stream".to_string());
    }
    let mut prefetched_chunks: Vec<Bytes> = Vec::new();
    let mut prefetched_body = Vec::new();
    let mut prefetched_inspection_body = Vec::new();
    let mut prefetched_telemetry: Option<ExecutionTelemetry> = None;
    let mut reached_eof = false;
    if let Some(ref report_kind) = direct_stream_finalize_kind {
        while prefetched_chunks.len() < MAX_STREAM_PREFETCH_FRAMES
            && prefetched_inspection_body.len() < MAX_STREAM_PREFETCH_BYTES
        {
            let Some(frame) = read_next_frame(&mut lines).await? else {
                reached_eof = true;
                break;
            };
            match frame.payload {
                StreamFramePayload::Data { chunk_b64, text } => {
                    let chunk = if let Some(chunk_b64) = chunk_b64 {
                        match base64::engine::general_purpose::STANDARD.decode(chunk_b64) {
                            Ok(decoded) => decoded,
                            Err(err) => {
                                warn!(trace_id = %trace_id, error = %err, "gateway failed to decode executor chunk");
                                break;
                            }
                        }
                    } else if let Some(text) = text {
                        text.into_bytes()
                    } else {
                        Vec::new()
                    };

                    if chunk.is_empty() {
                        continue;
                    }

                    prefetched_inspection_body.extend_from_slice(&chunk);

                    let inspection =
                        inspect_prefetched_stream_body(&headers, &prefetched_inspection_body);
                    match inspection {
                        StreamPrefetchInspection::EmbeddedError(body_json) => {
                            let payload = GatewaySyncReportRequest {
                                trace_id: trace_id.to_string(),
                                report_kind: report_kind.clone(),
                                report_context: report_context.clone(),
                                status_code,
                                headers: headers.clone(),
                                body_json: Some(body_json),
                                client_body_json: None,
                                body_base64: None,
                                telemetry: prefetched_telemetry.clone(),
                            };
                            if let Some(response) =
                                maybe_build_local_core_error_response(trace_id, decision, &payload)?
                            {
                                if let Some(error_report_kind) =
                                    resolve_core_error_background_report_kind(
                                        payload.report_kind.as_str(),
                                    )
                                {
                                    let mut report_payload = payload.clone();
                                    report_payload.report_kind = error_report_kind;
                                    spawn_sync_report(
                                        state.clone(),
                                        control_base_url.to_string(),
                                        trace_id.to_string(),
                                        report_payload,
                                    );
                                } else {
                                    spawn_sync_finalize(
                                        state.clone(),
                                        control_base_url.to_string(),
                                        trace_id.to_string(),
                                        payload,
                                    );
                                }
                                return Ok(Some(response));
                            }
                            let response =
                                submit_sync_finalize(state, control_base_url, trace_id, payload)
                                    .await?;
                            return Ok(Some(build_client_response(
                                response,
                                trace_id,
                                Some(decision),
                            )?));
                        }
                        StreamPrefetchInspection::NeedMore => {}
                        StreamPrefetchInspection::NonError => {}
                    }

                    let rewritten_chunk = if let Some(rewriter) = local_stream_rewriter.as_mut() {
                        rewriter.push_chunk(&chunk)?
                    } else {
                        chunk
                    };
                    if !rewritten_chunk.is_empty() {
                        prefetched_body.extend_from_slice(&rewritten_chunk);
                        prefetched_chunks.push(Bytes::from(rewritten_chunk));
                    }

                    if matches!(inspection, StreamPrefetchInspection::NonError) {
                        break;
                    }
                }
                StreamFramePayload::Telemetry {
                    telemetry: frame_telemetry,
                } => {
                    prefetched_telemetry = Some(frame_telemetry);
                }
                StreamFramePayload::Eof { .. } => {
                    reached_eof = true;
                    break;
                }
                StreamFramePayload::Error { error } => {
                    warn!(trace_id = %trace_id, error = %error.message, "executor stream emitted error frame during prefetch");
                    break;
                }
                StreamFramePayload::Headers { .. } => {}
            }
        }
    }

    let (tx, mut rx) = mpsc::channel::<Result<Bytes, IoError>>(16);
    let state_for_report = state.clone();
    let trace_id_owned = trace_id.to_string();
    let control_base_url_owned = control_base_url.to_string();
    let headers_for_report = headers.clone();
    let report_kind_owned = report_kind.clone();
    let report_context_owned = report_context.clone();
    let prefetched_body_for_report = prefetched_body.clone();
    let prefetched_chunks_for_body = prefetched_chunks.clone();
    let initial_telemetry = prefetched_telemetry.clone();
    let initial_reached_eof = reached_eof;
    tokio::spawn(async move {
        let mut buffered_body = prefetched_body_for_report;
        let mut telemetry: Option<ExecutionTelemetry> = initial_telemetry;
        let reached_eof = initial_reached_eof;

        if !reached_eof {
            loop {
                let next_frame = match read_next_frame(&mut lines).await {
                    Ok(frame) => frame,
                    Err(err) => {
                        warn!(trace_id = %trace_id_owned, error = ?err, "gateway failed to decode executor stream frame");
                        break;
                    }
                };
                let Some(frame) = next_frame else {
                    break;
                };
                match frame.payload {
                    StreamFramePayload::Data { chunk_b64, text } => {
                        let chunk = if let Some(chunk_b64) = chunk_b64 {
                            match base64::engine::general_purpose::STANDARD.decode(chunk_b64) {
                                Ok(decoded) => decoded,
                                Err(err) => {
                                    warn!(trace_id = %trace_id_owned, error = %err, "gateway failed to decode executor chunk");
                                    break;
                                }
                            }
                        } else if let Some(text) = text {
                            text.into_bytes()
                        } else {
                            Vec::new()
                        };

                        if chunk.is_empty() {
                            continue;
                        }

                        let rewritten_chunk = if let Some(rewriter) = local_stream_rewriter.as_mut()
                        {
                            match rewriter.push_chunk(&chunk) {
                                Ok(rewritten_chunk) => rewritten_chunk,
                                Err(err) => {
                                    warn!(trace_id = %trace_id_owned, error = ?err, "gateway failed to rewrite executor stream chunk");
                                    break;
                                }
                            }
                        } else {
                            chunk
                        };

                        if rewritten_chunk.is_empty() {
                            continue;
                        }

                        buffered_body.extend_from_slice(&rewritten_chunk);
                        if tx.send(Ok(Bytes::from(rewritten_chunk))).await.is_err() {
                            warn!(
                                trace_id = %trace_id_owned,
                                "gateway stream downstream dropped; stopping executor stream forwarding"
                            );
                            break;
                        }
                    }
                    StreamFramePayload::Telemetry {
                        telemetry: frame_telemetry,
                    } => {
                        telemetry = Some(frame_telemetry);
                    }
                    StreamFramePayload::Eof { .. } => {
                        break;
                    }
                    StreamFramePayload::Error { error } => {
                        warn!(trace_id = %trace_id_owned, error = %error.message, "executor stream emitted error frame");
                        break;
                    }
                    StreamFramePayload::Headers { .. } => {}
                }
            }
        }

        if let Some(rewriter) = local_stream_rewriter.as_mut() {
            match rewriter.finish() {
                Ok(flushed_chunk) if !flushed_chunk.is_empty() => {
                    buffered_body.extend_from_slice(&flushed_chunk);
                    if tx.send(Ok(Bytes::from(flushed_chunk))).await.is_err() {
                        warn!(
                            trace_id = %trace_id_owned,
                            "gateway stream downstream dropped while flushing local stream rewrite"
                        );
                    }
                }
                Ok(_) => {}
                Err(err) => {
                    warn!(trace_id = %trace_id_owned, error = ?err, "gateway failed to flush local stream rewrite");
                }
            }
        }

        drop(tx);

        if let Some(report_kind) = report_kind_owned {
            let report = GatewayStreamReportRequest {
                trace_id: trace_id_owned.clone(),
                report_kind,
                report_context: report_context_owned,
                status_code,
                headers: headers_for_report,
                body_base64: (!buffered_body.is_empty())
                    .then(|| base64::engine::general_purpose::STANDARD.encode(&buffered_body)),
                telemetry,
            };
            if let Err(err) = submit_stream_report(
                &state_for_report,
                &control_base_url_owned,
                &trace_id_owned,
                report,
            )
            .await
            {
                warn!(trace_id = %trace_id_owned, error = ?err, "gateway failed to submit stream execution report");
            }
        }
    });

    let body_stream = stream! {
        for chunk in prefetched_chunks_for_body {
            yield Ok(chunk);
        }
        while let Some(item) = rx.recv().await {
            yield item;
        }
    };

    Ok(Some(build_client_response_from_parts(
        status_code,
        &headers,
        Body::from_stream(body_stream),
        trace_id,
        Some(decision),
    )?))
}
