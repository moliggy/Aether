use std::time::{SystemTime, UNIX_EPOCH};

use aether_contracts::ExecutionPlan;
use aether_data::repository::candidates::RequestCandidateStatus;
use aether_scheduler_core::{
    build_execution_request_candidate_seed, build_local_request_candidate_status_record,
    build_report_request_candidate_status_record,
    finalize_execution_request_candidate_report_context, parse_request_candidate_report_context,
    resolve_report_request_candidate_slot as resolve_report_request_candidate_slot_from_candidates,
    SchedulerResolvedReportRequestCandidateSlot,
};
use serde_json::Value;
use tracing::warn;
use uuid::Uuid;

use super::request_candidate_state::SchedulerRequestCandidateRuntimeState;

pub(crate) use aether_scheduler_core::execution_error_details;

pub(crate) async fn record_local_request_candidate_status(
    state: &(impl SchedulerRequestCandidateRuntimeState + ?Sized),
    plan: &ExecutionPlan,
    report_context: Option<&Value>,
    status: RequestCandidateStatus,
    status_code: Option<u16>,
    error_type: Option<String>,
    error_message: Option<String>,
    latency_ms: Option<u64>,
    started_at_unix_secs: Option<u64>,
    finished_at_unix_secs: Option<u64>,
) {
    let Some(record) = build_local_request_candidate_status_record(
        plan,
        report_context,
        status,
        status_code,
        error_type,
        error_message,
        latency_ms,
        started_at_unix_secs,
        finished_at_unix_secs,
    ) else {
        return;
    };
    let candidate_id = record.id.clone();

    if let Err(err) = state.upsert_request_candidate(record).await {
        warn!(
            event_name = "request_candidate_status_persist_failed",
            log_type = "event",
            request_id = %plan.request_id,
            candidate_id = %candidate_id,
            error = ?err,
            "gateway failed to persist request candidate status update"
        );
    }
}

pub(crate) async fn record_report_request_candidate_status(
    state: &(impl SchedulerRequestCandidateRuntimeState + ?Sized),
    report_context: Option<&Value>,
    status: RequestCandidateStatus,
    status_code: Option<u16>,
    error_type: Option<String>,
    error_message: Option<String>,
    latency_ms: Option<u64>,
    started_at_unix_secs: Option<u64>,
    finished_at_unix_secs: Option<u64>,
) {
    let Some(slot) = resolve_report_request_candidate_slot(state, report_context).await else {
        return;
    };
    let request_id = slot.request_id.clone();
    let candidate_index = slot.candidate_index;
    let retry_index = slot.retry_index;
    let record = build_report_request_candidate_status_record(
        slot,
        status,
        status_code,
        error_type,
        error_message,
        latency_ms,
        started_at_unix_secs,
        finished_at_unix_secs,
        current_unix_secs(),
    );

    if let Err(err) = state.upsert_request_candidate(record).await {
        warn!(
            event_name = "request_candidate_report_status_persist_failed",
            log_type = "event",
            request_id = %request_id,
            candidate_index,
            retry_index,
            error = ?err,
            "gateway failed to persist report-driven request candidate status update"
        );
    }
}

pub(crate) async fn ensure_execution_request_candidate_slot(
    state: &(impl SchedulerRequestCandidateRuntimeState + ?Sized),
    plan: &mut ExecutionPlan,
    report_context: &mut Option<Value>,
) {
    if !state.has_request_candidate_data_writer() {
        return;
    }
    if plan
        .candidate_id
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| !value.is_empty())
    {
        return;
    }

    let seed = build_execution_request_candidate_seed(
        plan,
        report_context.as_ref(),
        current_unix_secs(),
        Uuid::new_v4().to_string(),
    );
    let generated_candidate_id = seed.upsert_record.id.clone();

    let candidate_id = match state.upsert_request_candidate(seed.upsert_record).await {
        Ok(Some(stored)) => stored.id,
        Ok(None) => generated_candidate_id,
        Err(err) => {
            warn!(
                event_name = "request_candidate_slot_seed_failed",
                log_type = "event",
                request_id = %plan.request_id,
                error = ?err,
                "gateway failed to seed execution request candidate slot"
            );
            return;
        }
    };

    plan.candidate_id = Some(candidate_id.clone());
    *report_context = Some(finalize_execution_request_candidate_report_context(
        seed.report_context,
        &candidate_id,
    ));
}

pub(crate) fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

async fn resolve_report_request_candidate_slot(
    state: &(impl SchedulerRequestCandidateRuntimeState + ?Sized),
    report_context: Option<&Value>,
) -> Option<SchedulerResolvedReportRequestCandidateSlot> {
    let metadata = parse_request_candidate_report_context(report_context)?;
    let request_id = metadata.request_id.clone()?;
    let existing_candidates = state
        .read_request_candidates_by_request_id(request_id.as_str())
        .await
        .ok()
        .unwrap_or_default();
    resolve_report_request_candidate_slot_from_candidates(
        &existing_candidates,
        metadata,
        current_unix_secs(),
        Uuid::new_v4().to_string(),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use aether_contracts::{ExecutionPlan, RequestBody};
    use aether_data::repository::candidates::{
        InMemoryRequestCandidateRepository, RequestCandidateReadRepository, RequestCandidateStatus,
        StoredRequestCandidate,
    };
    use aether_data::repository::usage::InMemoryUsageReadRepository;
    use serde_json::json;

    use super::{ensure_execution_request_candidate_slot, record_report_request_candidate_status};
    use crate::data::GatewayDataState;
    use crate::AppState;

    fn build_test_state(repository: Arc<InMemoryRequestCandidateRepository>) -> AppState {
        AppState::new()
            .expect("gateway state should build")
            .with_data_state_for_tests(
                GatewayDataState::with_request_candidate_and_usage_repository_for_tests(
                    repository,
                    Arc::new(InMemoryUsageReadRepository::default()),
                ),
            )
    }

    fn sample_plan() -> ExecutionPlan {
        ExecutionPlan {
            request_id: "req-request-candidate-seed-123".to_string(),
            candidate_id: None,
            provider_name: Some("openai".to_string()),
            provider_id: "provider-request-candidate-seed-123".to_string(),
            endpoint_id: "endpoint-request-candidate-seed-123".to_string(),
            key_id: "key-request-candidate-seed-123".to_string(),
            method: "POST".to_string(),
            url: "https://api.openai.example/v1/chat/completions".to_string(),
            headers: BTreeMap::new(),
            content_type: Some("application/json".to_string()),
            content_encoding: None,
            body: RequestBody::from_json(json!({"model": "gpt-5", "messages": []})),
            stream: false,
            client_api_format: "openai:chat".to_string(),
            provider_api_format: "openai:chat".to_string(),
            model_name: Some("gpt-5".to_string()),
            proxy: None,
            tls_profile: None,
            timeouts: None,
        }
    }

    #[tokio::test]
    async fn seeds_execution_request_candidate_slot_for_plan_without_candidate_id() {
        let repository = Arc::new(InMemoryRequestCandidateRepository::default());
        let state = build_test_state(Arc::clone(&repository));
        let mut plan = sample_plan();
        let mut report_context = Some(json!({
            "request_id": "req-request-candidate-seed-123",
            "client_api_format": "openai:chat"
        }));

        ensure_execution_request_candidate_slot(&state, &mut plan, &mut report_context).await;

        let candidate_id = plan
            .candidate_id
            .clone()
            .expect("candidate id should be seeded");
        let report_context = report_context.expect("report context should be populated");
        assert_eq!(
            report_context
                .get("candidate_id")
                .and_then(|value| value.as_str()),
            Some(candidate_id.as_str())
        );
        assert_eq!(
            report_context
                .get("candidate_index")
                .and_then(|value| value.as_u64()),
            Some(0)
        );
        assert_eq!(
            report_context
                .get("provider_id")
                .and_then(|value| value.as_str()),
            Some("provider-request-candidate-seed-123")
        );

        let stored = repository
            .list_by_request_id("req-request-candidate-seed-123")
            .await
            .expect("request candidates should read");
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].id, candidate_id);
        assert_eq!(stored[0].status, RequestCandidateStatus::Pending);
        assert_eq!(
            stored[0].provider_id.as_deref(),
            Some("provider-request-candidate-seed-123")
        );
        assert_eq!(
            stored[0].endpoint_id.as_deref(),
            Some("endpoint-request-candidate-seed-123")
        );
        assert_eq!(
            stored[0].key_id.as_deref(),
            Some("key-request-candidate-seed-123")
        );
    }

    #[tokio::test]
    async fn does_not_reseed_execution_request_candidate_slot_when_plan_already_has_candidate_id() {
        let repository = Arc::new(InMemoryRequestCandidateRepository::default());
        let state = build_test_state(Arc::clone(&repository));
        let mut plan = sample_plan();
        plan.candidate_id = Some("cand-existing-123".to_string());
        let mut report_context = Some(json!({
            "request_id": "req-request-candidate-seed-123"
        }));

        ensure_execution_request_candidate_slot(&state, &mut plan, &mut report_context).await;

        assert_eq!(plan.candidate_id.as_deref(), Some("cand-existing-123"));
        let stored = repository
            .list_by_request_id("req-request-candidate-seed-123")
            .await
            .expect("request candidates should read");
        assert!(stored.is_empty());
        assert_eq!(
            report_context
                .as_ref()
                .and_then(|value| value.get("candidate_id"))
                .and_then(|value| value.as_str()),
            None
        );
    }

    #[tokio::test]
    async fn records_report_request_candidate_status_for_existing_slot() {
        let repository = Arc::new(InMemoryRequestCandidateRepository::seed(vec![
            StoredRequestCandidate::new(
                "cand-report-123".to_string(),
                "req-report-123".to_string(),
                Some("user-1".to_string()),
                Some("api-key-1".to_string()),
                None,
                None,
                0,
                0,
                Some("provider-report-123".to_string()),
                Some("endpoint-report-123".to_string()),
                Some("key-report-123".to_string()),
                RequestCandidateStatus::Pending,
                None,
                false,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                100,
                Some(100),
                None,
            )
            .expect("request candidate should build"),
        ]));
        let state = build_test_state(Arc::clone(&repository));
        let report_context = json!({
            "request_id": "req-report-123",
            "candidate_id": "cand-report-123",
            "candidate_index": 0,
            "retry_index": 0,
            "provider_id": "provider-report-123",
            "endpoint_id": "endpoint-report-123",
            "key_id": "key-report-123"
        });

        record_report_request_candidate_status(
            &state,
            Some(&report_context),
            RequestCandidateStatus::Success,
            Some(200),
            None,
            None,
            Some(25),
            Some(101),
            Some(102),
        )
        .await;

        let stored = repository
            .list_by_request_id("req-report-123")
            .await
            .expect("request candidates should read");
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].id, "cand-report-123");
        assert_eq!(stored[0].status, RequestCandidateStatus::Success);
        assert_eq!(stored[0].status_code, Some(200));
        assert_eq!(stored[0].latency_ms, Some(25));
        assert_eq!(stored[0].started_at_unix_secs, Some(101));
        assert_eq!(stored[0].finished_at_unix_secs, Some(102));
    }
}
