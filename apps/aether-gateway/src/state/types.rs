#[derive(Debug, Clone, Default)]
pub(crate) struct LocalProviderDeleteTaskState {
    pub task_id: String,
    pub provider_id: String,
    pub status: String,
    pub stage: String,
    pub total_keys: usize,
    pub deleted_keys: usize,
    pub total_endpoints: usize,
    pub deleted_endpoints: usize,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum LocalMutationOutcome<T> {
    Applied(T),
    NotFound,
    Invalid(String),
    Unavailable,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct LocalExecutionRuntimeMissDiagnostic {
    pub(crate) reason: String,
    pub(crate) route_family: Option<String>,
    pub(crate) route_kind: Option<String>,
    pub(crate) public_path: Option<String>,
    pub(crate) plan_kind: Option<String>,
    pub(crate) requested_model: Option<String>,
    pub(crate) candidate_count: Option<usize>,
    pub(crate) skipped_candidate_count: Option<usize>,
    pub(crate) skip_reasons: std::collections::BTreeMap<String, usize>,
}

impl LocalExecutionRuntimeMissDiagnostic {
    pub(crate) fn skip_reasons_summary(&self) -> Option<String> {
        if self.skip_reasons.is_empty() {
            return None;
        }
        Some(
            self.skip_reasons
                .iter()
                .map(|(reason, count)| format!("{reason}={count}"))
                .collect::<Vec<_>>()
                .join(","),
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum AdminWalletMutationOutcome<T> {
    Applied(T),
    NotFound,
    Invalid(String),
    Unavailable,
}
