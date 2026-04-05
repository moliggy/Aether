use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{AppState, GatewayError};

const DEFAULT_RECENT_LIMIT: usize = 20;
const MAX_RECENT_LIMIT: usize = 200;

#[derive(Debug, Deserialize)]
pub(crate) struct ListRecentShadowResultsQuery {
    pub(crate) limit: Option<usize>,
}

#[derive(Debug, Serialize)]
pub(crate) struct ShadowResultStatusCounts {
    pub(crate) pending: usize,
    pub(crate) r#match: usize,
    pub(crate) mismatch: usize,
    pub(crate) error: usize,
}

#[derive(Debug, Serialize)]
pub(crate) struct ListRecentShadowResultsResponse {
    pub(crate) items: Vec<aether_data::repository::shadow_results::StoredShadowResult>,
    pub(crate) limit_applied: usize,
    pub(crate) counts: ShadowResultStatusCounts,
}

pub(crate) async fn list_recent_shadow_results(
    State(state): State<AppState>,
    Query(query): Query<ListRecentShadowResultsQuery>,
) -> Result<Json<ListRecentShadowResultsResponse>, GatewayError> {
    let limit = query
        .limit
        .unwrap_or(DEFAULT_RECENT_LIMIT)
        .clamp(1, MAX_RECENT_LIMIT);
    let items = state.list_recent_shadow_results(limit).await?;

    let mut counts = ShadowResultStatusCounts {
        pending: 0,
        r#match: 0,
        mismatch: 0,
        error: 0,
    };
    for item in &items {
        match item.match_status {
            aether_data::repository::shadow_results::ShadowResultMatchStatus::Pending => {
                counts.pending += 1
            }
            aether_data::repository::shadow_results::ShadowResultMatchStatus::Match => {
                counts.r#match += 1
            }
            aether_data::repository::shadow_results::ShadowResultMatchStatus::Mismatch => {
                counts.mismatch += 1
            }
            aether_data::repository::shadow_results::ShadowResultMatchStatus::Error => {
                counts.error += 1
            }
        }
    }

    Ok(Json(ListRecentShadowResultsResponse {
        items,
        limit_applied: limit,
        counts,
    }))
}

#[derive(Debug, Deserialize)]
pub(crate) struct GetRequestCandidateTraceQuery {
    pub(crate) attempted_only: Option<bool>,
}

pub(crate) async fn get_request_candidate_trace(
    State(state): State<AppState>,
    Path(request_id): Path<String>,
    Query(query): Query<GetRequestCandidateTraceQuery>,
) -> Result<
    Json<crate::data::candidates::RequestCandidateTrace>,
    axum::response::Response,
> {
    let attempted_only = query.attempted_only.unwrap_or(false);
    let trace = state
        .read_request_candidate_trace(&request_id, attempted_only)
        .await
        .map_err(IntoResponse::into_response)?;

    match trace {
        Some(trace) => Ok(Json(trace)),
        None => Err((
            axum::http::StatusCode::NOT_FOUND,
            Json(json!({
                "error": {
                    "message": "Request not found",
                }
            })),
        )
            .into_response()),
    }
}

pub(crate) async fn get_decision_trace(
    State(state): State<AppState>,
    Path(request_id): Path<String>,
    Query(query): Query<GetRequestCandidateTraceQuery>,
) -> Result<
    Json<crate::data::decision_trace::DecisionTrace>,
    axum::response::Response,
> {
    let attempted_only = query.attempted_only.unwrap_or(false);
    let trace = state
        .read_decision_trace(&request_id, attempted_only)
        .await
        .map_err(IntoResponse::into_response)?;

    match trace {
        Some(trace) => Ok(Json(trace)),
        None => Err((
            axum::http::StatusCode::NOT_FOUND,
            Json(json!({
                "error": {
                    "message": "Decision trace not found",
                }
            })),
        )
            .into_response()),
    }
}

pub(crate) async fn get_auth_api_key_snapshot(
    State(state): State<AppState>,
    Path((user_id, api_key_id)): Path<(String, String)>,
) -> Result<
    Json<crate::data::auth::GatewayAuthApiKeySnapshot>,
    axum::response::Response,
> {
    let snapshot = state
        .read_auth_api_key_snapshot(&user_id, &api_key_id, current_unix_secs())
        .await
        .map_err(IntoResponse::into_response)?;

    match snapshot {
        Some(snapshot) => Ok(Json(snapshot)),
        None => Err((
            axum::http::StatusCode::NOT_FOUND,
            Json(json!({
                "error": {
                    "message": "Auth snapshot not found",
                }
            })),
        )
            .into_response()),
    }
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
