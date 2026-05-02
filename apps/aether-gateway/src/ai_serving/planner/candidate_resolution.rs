use std::sync::Arc;

use aether_ai_serving::{
    run_ai_candidate_resolution, AiCandidateResolutionMode, AiCandidateResolutionPort,
    AiCandidateResolutionRequest,
};
use async_trait::async_trait;
use std::convert::Infallible;
use tracing::warn;

use aether_scheduler_core::{SchedulerMinimalCandidateSelectionCandidate, SchedulerRankingOutcome};

use crate::ai_serving::{
    candidate_common_transport_skip_reason, candidate_transport_pair_skip_reason,
    CandidateTransportPolicyFacts, GatewayAuthApiKeySnapshot, GatewayProviderTransportSnapshot,
    PlannerAppState,
};
use crate::orchestration::LocalExecutionCandidateMetadata;

use super::candidate_ranking::rank_eligible_local_execution_candidates;
use super::pool_scheduler::apply_local_execution_pool_scheduler;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EligibleLocalExecutionCandidate {
    pub(crate) candidate: SchedulerMinimalCandidateSelectionCandidate,
    pub(crate) transport: Arc<GatewayProviderTransportSnapshot>,
    pub(crate) provider_api_format: String,
    pub(crate) orchestration: LocalExecutionCandidateMetadata,
    pub(crate) ranking: Option<SchedulerRankingOutcome>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SkippedLocalExecutionCandidate {
    pub(crate) candidate: SchedulerMinimalCandidateSelectionCandidate,
    pub(crate) skip_reason: &'static str,
    pub(crate) transport: Option<Arc<GatewayProviderTransportSnapshot>>,
    pub(crate) ranking: Option<SchedulerRankingOutcome>,
    pub(crate) extra_data: Option<serde_json::Value>,
}

impl SkippedLocalExecutionCandidate {
    pub(crate) fn transport_ref(&self) -> Option<&GatewayProviderTransportSnapshot> {
        self.transport.as_deref()
    }
}

struct GatewayLocalCandidateResolutionPort<'a> {
    state: PlannerAppState<'a>,
    requested_model: Option<&'a str>,
    auth_snapshot: Option<&'a GatewayAuthApiKeySnapshot>,
    required_capabilities: Option<&'a serde_json::Value>,
    sticky_session_token: Option<&'a str>,
}

#[async_trait]
impl AiCandidateResolutionPort for GatewayLocalCandidateResolutionPort<'_> {
    type Candidate = SchedulerMinimalCandidateSelectionCandidate;
    type Transport = GatewayProviderTransportSnapshot;
    type Eligible = EligibleLocalExecutionCandidate;
    type Skipped = SkippedLocalExecutionCandidate;
    type Error = Infallible;

    async fn read_candidate_transport(
        &self,
        candidate: &Self::Candidate,
    ) -> Result<Option<Self::Transport>, Self::Error> {
        Ok(read_candidate_transport_snapshot(self.state, candidate).await)
    }

    fn build_missing_transport_skipped_candidate(
        &self,
        candidate: Self::Candidate,
    ) -> Self::Skipped {
        SkippedLocalExecutionCandidate {
            candidate,
            skip_reason: "transport_snapshot_missing",
            transport: None,
            ranking: None,
            extra_data: None,
        }
    }

    fn candidate_common_skip_reason(
        &self,
        candidate: &Self::Candidate,
        transport: &Self::Transport,
        requested_model: Option<&str>,
    ) -> Option<&'static str> {
        candidate_common_transport_skip_reason(
            transport,
            candidate_transport_policy_facts(candidate),
            requested_model,
        )
    }

    fn candidate_transport_pair_skip_reason(
        &self,
        candidate: &Self::Candidate,
        transport: &Self::Transport,
        normalized_client_api_format: &str,
        requested_model: &str,
    ) -> Option<&'static str> {
        let _ = (candidate, requested_model);
        candidate_transport_pair_skip_reason(transport, normalized_client_api_format)
    }

    fn build_skipped_candidate(
        &self,
        candidate: Self::Candidate,
        transport: Self::Transport,
        skip_reason: &'static str,
    ) -> Self::Skipped {
        SkippedLocalExecutionCandidate {
            candidate,
            skip_reason,
            transport: Some(Arc::new(transport)),
            ranking: None,
            extra_data: None,
        }
    }

    fn build_eligible_candidate(
        &self,
        candidate: Self::Candidate,
        transport: Self::Transport,
    ) -> Self::Eligible {
        let provider_api_format = transport.endpoint.api_format.trim().to_ascii_lowercase();
        EligibleLocalExecutionCandidate {
            candidate,
            transport: Arc::new(transport),
            provider_api_format,
            orchestration: LocalExecutionCandidateMetadata::default(),
            ranking: None,
        }
    }

    async fn rank_eligible_candidates(
        &self,
        candidates: Vec<Self::Eligible>,
        normalized_client_api_format: &str,
    ) -> Result<Vec<Self::Eligible>, Self::Error> {
        Ok(rank_eligible_local_execution_candidates(
            self.state,
            candidates,
            normalized_client_api_format,
            self.requested_model,
            self.auth_snapshot,
            self.required_capabilities,
        )
        .await)
    }

    async fn apply_pool_scheduler(
        &self,
        candidates: Vec<Self::Eligible>,
    ) -> Result<(Vec<Self::Eligible>, Vec<Self::Skipped>), Self::Error> {
        Ok(
            apply_local_execution_pool_scheduler(self.state, candidates, self.sticky_session_token)
                .await,
        )
    }
}

pub(crate) async fn resolve_and_rank_local_execution_candidates(
    state: PlannerAppState<'_>,
    candidates: Vec<SchedulerMinimalCandidateSelectionCandidate>,
    client_api_format: &str,
    requested_model: &str,
    auth_snapshot: Option<&GatewayAuthApiKeySnapshot>,
    required_capabilities: Option<&serde_json::Value>,
    sticky_session_token: Option<&str>,
) -> (
    Vec<EligibleLocalExecutionCandidate>,
    Vec<SkippedLocalExecutionCandidate>,
) {
    let requested_model = requested_model.trim();
    resolve_and_rank_local_execution_candidates_with_mode(
        state,
        candidates,
        client_api_format,
        Some(requested_model),
        auth_snapshot,
        required_capabilities,
        sticky_session_token,
        AiCandidateResolutionMode::Standard,
    )
    .await
}

pub(crate) async fn resolve_and_rank_local_execution_candidates_without_transport_pair_gate(
    state: PlannerAppState<'_>,
    candidates: Vec<SchedulerMinimalCandidateSelectionCandidate>,
    client_api_format: &str,
    requested_model: Option<&str>,
    auth_snapshot: Option<&GatewayAuthApiKeySnapshot>,
    required_capabilities: Option<&serde_json::Value>,
    sticky_session_token: Option<&str>,
) -> (
    Vec<EligibleLocalExecutionCandidate>,
    Vec<SkippedLocalExecutionCandidate>,
) {
    let requested_model = requested_model.map(str::trim);
    resolve_and_rank_local_execution_candidates_with_mode(
        state,
        candidates,
        client_api_format,
        requested_model,
        auth_snapshot,
        required_capabilities,
        sticky_session_token,
        AiCandidateResolutionMode::WithoutTransportPairGate,
    )
    .await
}

async fn resolve_and_rank_local_execution_candidates_with_mode(
    state: PlannerAppState<'_>,
    candidates: Vec<SchedulerMinimalCandidateSelectionCandidate>,
    client_api_format: &str,
    requested_model: Option<&str>,
    auth_snapshot: Option<&GatewayAuthApiKeySnapshot>,
    required_capabilities: Option<&serde_json::Value>,
    sticky_session_token: Option<&str>,
    mode: AiCandidateResolutionMode,
) -> (
    Vec<EligibleLocalExecutionCandidate>,
    Vec<SkippedLocalExecutionCandidate>,
) {
    let port = GatewayLocalCandidateResolutionPort {
        state,
        requested_model,
        auth_snapshot,
        required_capabilities,
        sticky_session_token,
    };

    let request = AiCandidateResolutionRequest {
        client_api_format,
        requested_model,
        mode,
    };

    match run_ai_candidate_resolution(&port, candidates, request).await {
        Ok(outcome) => (outcome.eligible_candidates, outcome.skipped_candidates),
        Err(error) => match error {},
    }
}

fn candidate_transport_policy_facts(
    candidate: &SchedulerMinimalCandidateSelectionCandidate,
) -> CandidateTransportPolicyFacts<'_> {
    CandidateTransportPolicyFacts {
        endpoint_api_format: candidate.endpoint_api_format.as_str(),
        global_model_name: candidate.global_model_name.as_str(),
        selected_provider_model_name: candidate.selected_provider_model_name.as_str(),
        mapping_matched_model: candidate.mapping_matched_model.as_deref(),
    }
}

pub(crate) async fn read_candidate_transport_snapshot(
    state: PlannerAppState<'_>,
    candidate: &SchedulerMinimalCandidateSelectionCandidate,
) -> Option<GatewayProviderTransportSnapshot> {
    match state
        .read_provider_transport_snapshot(
            &candidate.provider_id,
            &candidate.endpoint_id,
            &candidate.key_id,
        )
        .await
    {
        Ok(Some(transport)) => Some(transport),
        Ok(None) => None,
        Err(error) => {
            warn!(
                event_name = "candidate_resolution_transport_load_failed",
                log_type = "event",
                provider_id = %candidate.provider_id,
                endpoint_id = %candidate.endpoint_id,
                key_id = %candidate.key_id,
                error = ?error,
                "failed to load provider transport while evaluating local candidate eligibility"
            );
            None
        }
    }
}
