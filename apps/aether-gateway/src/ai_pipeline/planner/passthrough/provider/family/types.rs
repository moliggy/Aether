#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocalSameFormatProviderFamily {
    Standard,
    Gemini,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct LocalSameFormatProviderSpec {
    pub(crate) api_format: &'static str,
    pub(crate) decision_kind: &'static str,
    pub(crate) report_kind: &'static str,
    pub(crate) family: LocalSameFormatProviderFamily,
    pub(crate) require_streaming: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct LocalSameFormatProviderDecisionInput {
    pub(crate) auth_context: crate::control::GatewayControlAuthContext,
    pub(crate) requested_model: String,
    pub(crate) auth_snapshot: crate::data::auth::GatewayAuthApiKeySnapshot,
}

#[derive(Debug, Clone)]
pub(crate) struct LocalSameFormatProviderCandidateAttempt {
    pub(crate) candidate: crate::scheduler::GatewayMinimalCandidateSelectionCandidate,
    pub(crate) candidate_index: u32,
    pub(crate) candidate_id: String,
}
