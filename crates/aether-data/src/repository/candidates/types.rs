use std::collections::BTreeMap;

use async_trait::async_trait;

use crate::repository::provider_catalog::{
    StoredProviderCatalogEndpoint, StoredProviderCatalogKey, StoredProviderCatalogProvider,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestCandidateStatus {
    Available,
    Unused,
    Pending,
    Streaming,
    Success,
    Failed,
    Cancelled,
    Skipped,
}

impl RequestCandidateStatus {
    pub fn from_database(value: &str) -> Result<Self, crate::DataLayerError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "available" => Ok(Self::Available),
            "unused" => Ok(Self::Unused),
            "pending" => Ok(Self::Pending),
            "streaming" => Ok(Self::Streaming),
            "success" => Ok(Self::Success),
            "failed" => Ok(Self::Failed),
            "cancelled" => Ok(Self::Cancelled),
            "skipped" => Ok(Self::Skipped),
            other => Err(crate::DataLayerError::UnexpectedValue(format!(
                "unsupported request_candidates.status: {other}"
            ))),
        }
    }

    pub fn is_attempted(self, started_at_unix_secs: Option<u64>) -> bool {
        match self {
            Self::Available | Self::Unused | Self::Skipped => false,
            Self::Pending => started_at_unix_secs.is_some(),
            Self::Streaming | Self::Success | Self::Failed | Self::Cancelled => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StoredRequestCandidate {
    pub id: String,
    pub request_id: String,
    pub user_id: Option<String>,
    pub api_key_id: Option<String>,
    pub username: Option<String>,
    pub api_key_name: Option<String>,
    pub candidate_index: u32,
    pub retry_index: u32,
    pub provider_id: Option<String>,
    pub endpoint_id: Option<String>,
    pub key_id: Option<String>,
    pub status: RequestCandidateStatus,
    pub skip_reason: Option<String>,
    pub is_cached: bool,
    pub status_code: Option<u16>,
    pub error_type: Option<String>,
    pub error_message: Option<String>,
    pub latency_ms: Option<u64>,
    pub concurrent_requests: Option<u32>,
    pub extra_data: Option<serde_json::Value>,
    pub required_capabilities: Option<serde_json::Value>,
    pub created_at_unix_secs: u64,
    pub started_at_unix_secs: Option<u64>,
    pub finished_at_unix_secs: Option<u64>,
}

impl StoredRequestCandidate {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        request_id: String,
        user_id: Option<String>,
        api_key_id: Option<String>,
        username: Option<String>,
        api_key_name: Option<String>,
        candidate_index: i32,
        retry_index: i32,
        provider_id: Option<String>,
        endpoint_id: Option<String>,
        key_id: Option<String>,
        status: RequestCandidateStatus,
        skip_reason: Option<String>,
        is_cached: bool,
        status_code: Option<i32>,
        error_type: Option<String>,
        error_message: Option<String>,
        latency_ms: Option<i32>,
        concurrent_requests: Option<i32>,
        extra_data: Option<serde_json::Value>,
        required_capabilities: Option<serde_json::Value>,
        created_at_unix_secs: i64,
        started_at_unix_secs: Option<i64>,
        finished_at_unix_secs: Option<i64>,
    ) -> Result<Self, crate::DataLayerError> {
        let candidate_index = u32::try_from(candidate_index).map_err(|_| {
            crate::DataLayerError::UnexpectedValue(format!(
                "invalid request_candidates.candidate_index: {candidate_index}"
            ))
        })?;
        let retry_index = u32::try_from(retry_index).map_err(|_| {
            crate::DataLayerError::UnexpectedValue(format!(
                "invalid request_candidates.retry_index: {retry_index}"
            ))
        })?;
        let status_code = status_code
            .map(|value| {
                u16::try_from(value).map_err(|_| {
                    crate::DataLayerError::UnexpectedValue(format!(
                        "invalid request_candidates.status_code: {value}"
                    ))
                })
            })
            .transpose()?;
        let latency_ms = latency_ms
            .map(|value| {
                u64::try_from(value).map_err(|_| {
                    crate::DataLayerError::UnexpectedValue(format!(
                        "invalid request_candidates.latency_ms: {value}"
                    ))
                })
            })
            .transpose()?;
        let concurrent_requests = concurrent_requests
            .map(|value| {
                u32::try_from(value).map_err(|_| {
                    crate::DataLayerError::UnexpectedValue(format!(
                        "invalid request_candidates.concurrent_requests: {value}"
                    ))
                })
            })
            .transpose()?;
        let created_at_unix_secs = u64::try_from(created_at_unix_secs).map_err(|_| {
            crate::DataLayerError::UnexpectedValue(format!(
                "invalid request_candidates.created_at_unix_secs: {created_at_unix_secs}"
            ))
        })?;
        let started_at_unix_secs = started_at_unix_secs
            .map(|value| {
                u64::try_from(value).map_err(|_| {
                    crate::DataLayerError::UnexpectedValue(format!(
                        "invalid request_candidates.started_at_unix_secs: {value}"
                    ))
                })
            })
            .transpose()?;
        let finished_at_unix_secs = finished_at_unix_secs
            .map(|value| {
                u64::try_from(value).map_err(|_| {
                    crate::DataLayerError::UnexpectedValue(format!(
                        "invalid request_candidates.finished_at_unix_secs: {value}"
                    ))
                })
            })
            .transpose()?;

        Ok(Self {
            id,
            request_id,
            user_id,
            api_key_id,
            username,
            api_key_name,
            candidate_index,
            retry_index,
            provider_id,
            endpoint_id,
            key_id,
            status,
            skip_reason,
            is_cached,
            status_code,
            error_type,
            error_message,
            latency_ms,
            concurrent_requests,
            extra_data,
            required_capabilities,
            created_at_unix_secs,
            started_at_unix_secs,
            finished_at_unix_secs,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestCandidateFinalStatus {
    Success,
    Failed,
    Cancelled,
    Streaming,
    Pending,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RequestCandidateTrace {
    pub request_id: String,
    pub total_candidates: usize,
    pub final_status: RequestCandidateFinalStatus,
    pub total_latency_ms: u64,
    pub candidates: Vec<StoredRequestCandidate>,
}

impl RequestCandidateTrace {
    pub fn from_candidates(
        request_id: impl Into<String>,
        all_candidates: Vec<StoredRequestCandidate>,
        attempted_only: bool,
    ) -> Option<Self> {
        if all_candidates.is_empty() {
            return None;
        }

        let candidates = if attempted_only {
            all_candidates
                .iter()
                .filter(|candidate| {
                    candidate
                        .status
                        .is_attempted(candidate.started_at_unix_secs)
                })
                .cloned()
                .collect::<Vec<_>>()
        } else {
            all_candidates.clone()
        };

        let total_latency_ms = candidates
            .iter()
            .filter(|candidate| {
                matches!(
                    candidate.status,
                    RequestCandidateStatus::Success
                        | RequestCandidateStatus::Failed
                        | RequestCandidateStatus::Cancelled
                ) && candidate.latency_ms.is_some()
            })
            .map(|candidate| candidate.latency_ms.unwrap_or(0))
            .sum();
        let final_status_source = if attempted_only && candidates.is_empty() {
            &all_candidates
        } else {
            &candidates
        };

        Some(Self {
            request_id: request_id.into(),
            total_candidates: candidates.len(),
            final_status: derive_request_candidate_final_status(final_status_source),
            total_latency_ms,
            candidates,
        })
    }
}

pub fn derive_request_candidate_final_status(
    candidates: &[StoredRequestCandidate],
) -> RequestCandidateFinalStatus {
    let has_success = candidates.iter().any(|candidate| {
        candidate.status == RequestCandidateStatus::Success
            || matches!(candidate.status_code, Some(status_code) if (200..300).contains(&status_code))
    });
    if has_success {
        return RequestCandidateFinalStatus::Success;
    }

    if candidates
        .iter()
        .any(|candidate| candidate.status == RequestCandidateStatus::Streaming)
    {
        return RequestCandidateFinalStatus::Streaming;
    }

    if candidates
        .iter()
        .any(|candidate| candidate.status == RequestCandidateStatus::Pending)
    {
        return RequestCandidateFinalStatus::Pending;
    }

    let has_cancelled = candidates
        .iter()
        .any(|candidate| candidate.status == RequestCandidateStatus::Cancelled);
    let has_failed = candidates
        .iter()
        .any(|candidate| candidate.status == RequestCandidateStatus::Failed);
    if has_cancelled && !has_failed {
        return RequestCandidateFinalStatus::Cancelled;
    }

    RequestCandidateFinalStatus::Failed
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DecisionTraceCandidate {
    #[serde(flatten)]
    pub candidate: StoredRequestCandidate,
    pub provider_name: Option<String>,
    pub provider_website: Option<String>,
    pub provider_type: Option<String>,
    pub endpoint_api_format: Option<String>,
    pub endpoint_api_family: Option<String>,
    pub endpoint_kind: Option<String>,
    pub provider_key_name: Option<String>,
    pub provider_key_auth_type: Option<String>,
    pub provider_key_capabilities: Option<serde_json::Value>,
    pub provider_key_is_active: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DecisionTrace {
    pub request_id: String,
    pub total_candidates: usize,
    pub final_status: RequestCandidateFinalStatus,
    pub total_latency_ms: u64,
    pub candidates: Vec<DecisionTraceCandidate>,
}

pub fn build_decision_trace(
    trace: RequestCandidateTrace,
    providers: Vec<StoredProviderCatalogProvider>,
    endpoints: Vec<StoredProviderCatalogEndpoint>,
    keys: Vec<StoredProviderCatalogKey>,
) -> DecisionTrace {
    let provider_map = providers
        .into_iter()
        .map(|item| (item.id.clone(), item))
        .collect::<BTreeMap<_, _>>();
    let endpoint_map = endpoints
        .into_iter()
        .map(|item| (item.id.clone(), item))
        .collect::<BTreeMap<_, _>>();
    let key_map = keys
        .into_iter()
        .map(|item| (item.id.clone(), item))
        .collect::<BTreeMap<_, _>>();

    DecisionTrace {
        request_id: trace.request_id,
        total_candidates: trace.total_candidates,
        final_status: trace.final_status,
        total_latency_ms: trace.total_latency_ms,
        candidates: trace
            .candidates
            .into_iter()
            .map(|candidate| {
                enrich_decision_trace_candidate(candidate, &provider_map, &endpoint_map, &key_map)
            })
            .collect(),
    }
}

fn enrich_decision_trace_candidate(
    candidate: StoredRequestCandidate,
    provider_map: &BTreeMap<String, StoredProviderCatalogProvider>,
    endpoint_map: &BTreeMap<String, StoredProviderCatalogEndpoint>,
    key_map: &BTreeMap<String, StoredProviderCatalogKey>,
) -> DecisionTraceCandidate {
    let provider = candidate
        .provider_id
        .as_ref()
        .and_then(|provider_id| provider_map.get(provider_id));
    let endpoint = candidate
        .endpoint_id
        .as_ref()
        .and_then(|endpoint_id| endpoint_map.get(endpoint_id));
    let provider_key = candidate
        .key_id
        .as_ref()
        .and_then(|key_id| key_map.get(key_id));

    DecisionTraceCandidate {
        provider_name: provider.map(|item| item.name.clone()),
        provider_website: provider.and_then(|item| item.website.clone()),
        provider_type: provider.map(|item| item.provider_type.clone()),
        endpoint_api_format: endpoint.map(|item| item.api_format.clone()),
        endpoint_api_family: endpoint.and_then(|item| item.api_family.clone()),
        endpoint_kind: endpoint.and_then(|item| item.endpoint_kind.clone()),
        provider_key_name: provider_key
            .map(|item| item.name.clone())
            .or_else(|| candidate.api_key_name.clone()),
        provider_key_auth_type: provider_key.map(|item| item.auth_type.clone()),
        provider_key_capabilities: provider_key.and_then(|item| item.capabilities.clone()),
        provider_key_is_active: provider_key.map(|item| item.is_active),
        candidate,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PublicHealthStatusCount {
    pub endpoint_id: String,
    pub status: RequestCandidateStatus,
    pub count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PublicHealthTimelineBucket {
    pub endpoint_id: String,
    pub segment_idx: u32,
    pub total_count: u64,
    pub success_count: u64,
    pub failed_count: u64,
    pub min_created_at_unix_secs: Option<u64>,
    pub max_created_at_unix_secs: Option<u64>,
}

#[async_trait]
pub trait RequestCandidateReadRepository: Send + Sync {
    async fn list_by_request_id(
        &self,
        request_id: &str,
    ) -> Result<Vec<StoredRequestCandidate>, crate::DataLayerError>;

    async fn list_recent(
        &self,
        limit: usize,
    ) -> Result<Vec<StoredRequestCandidate>, crate::DataLayerError>;

    async fn list_by_provider_id(
        &self,
        provider_id: &str,
        limit: usize,
    ) -> Result<Vec<StoredRequestCandidate>, crate::DataLayerError>;

    async fn list_finalized_by_endpoint_ids_since(
        &self,
        endpoint_ids: &[String],
        since_unix_secs: u64,
        limit: usize,
    ) -> Result<Vec<StoredRequestCandidate>, crate::DataLayerError>;

    async fn count_finalized_statuses_by_endpoint_ids_since(
        &self,
        endpoint_ids: &[String],
        since_unix_secs: u64,
    ) -> Result<Vec<PublicHealthStatusCount>, crate::DataLayerError>;

    async fn aggregate_finalized_timeline_by_endpoint_ids_since(
        &self,
        endpoint_ids: &[String],
        since_unix_secs: u64,
        until_unix_secs: u64,
        segments: u32,
    ) -> Result<Vec<PublicHealthTimelineBucket>, crate::DataLayerError>;
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct UpsertRequestCandidateRecord {
    pub id: String,
    pub request_id: String,
    pub user_id: Option<String>,
    pub api_key_id: Option<String>,
    pub username: Option<String>,
    pub api_key_name: Option<String>,
    pub candidate_index: u32,
    pub retry_index: u32,
    pub provider_id: Option<String>,
    pub endpoint_id: Option<String>,
    pub key_id: Option<String>,
    pub status: RequestCandidateStatus,
    pub skip_reason: Option<String>,
    pub is_cached: Option<bool>,
    pub status_code: Option<u16>,
    pub error_type: Option<String>,
    pub error_message: Option<String>,
    pub latency_ms: Option<u64>,
    pub concurrent_requests: Option<u32>,
    pub extra_data: Option<serde_json::Value>,
    pub required_capabilities: Option<serde_json::Value>,
    pub created_at_unix_secs: Option<u64>,
    pub started_at_unix_secs: Option<u64>,
    pub finished_at_unix_secs: Option<u64>,
}

impl UpsertRequestCandidateRecord {
    pub fn validate(&self) -> Result<(), crate::DataLayerError> {
        if self.id.trim().is_empty() {
            return Err(crate::DataLayerError::InvalidInput(
                "request candidate upsert id cannot be empty".to_string(),
            ));
        }
        if self.request_id.trim().is_empty() {
            return Err(crate::DataLayerError::InvalidInput(
                "request candidate upsert request_id cannot be empty".to_string(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
pub trait RequestCandidateWriteRepository: Send + Sync {
    async fn upsert(
        &self,
        candidate: UpsertRequestCandidateRecord,
    ) -> Result<StoredRequestCandidate, crate::DataLayerError>;

    async fn delete_created_before(
        &self,
        created_before_unix_secs: u64,
        limit: usize,
    ) -> Result<usize, crate::DataLayerError>;
}

pub trait RequestCandidateRepository:
    RequestCandidateReadRepository + RequestCandidateWriteRepository + Send + Sync
{
}

impl<T> RequestCandidateRepository for T where
    T: RequestCandidateReadRepository + RequestCandidateWriteRepository + Send + Sync
{
}

#[cfg(test)]
mod tests {
    use super::{
        build_decision_trace, derive_request_candidate_final_status, DecisionTrace,
        DecisionTraceCandidate, RequestCandidateFinalStatus, RequestCandidateStatus,
        RequestCandidateTrace, StoredRequestCandidate, UpsertRequestCandidateRecord,
    };
    use crate::repository::provider_catalog::{
        StoredProviderCatalogEndpoint, StoredProviderCatalogKey, StoredProviderCatalogProvider,
    };

    #[test]
    fn parses_status_from_database_text() {
        assert_eq!(
            RequestCandidateStatus::from_database("streaming").expect("status should parse"),
            RequestCandidateStatus::Streaming
        );
    }

    #[test]
    fn rejects_invalid_database_status() {
        assert!(RequestCandidateStatus::from_database("mystery").is_err());
    }

    #[test]
    fn rejects_negative_candidate_index() {
        assert!(StoredRequestCandidate::new(
            "cand-1".to_string(),
            "req-1".to_string(),
            None,
            None,
            None,
            None,
            -1,
            0,
            None,
            None,
            None,
            RequestCandidateStatus::Pending,
            None,
            false,
            Some(200),
            None,
            None,
            Some(10),
            Some(1),
            None,
            None,
            100,
            None,
            None,
        )
        .is_err());
    }

    fn sample_candidate(
        id: &str,
        request_id: &str,
        candidate_index: i32,
        status: RequestCandidateStatus,
        started_at_unix_secs: Option<i64>,
        latency_ms: Option<i32>,
        status_code: Option<i32>,
    ) -> StoredRequestCandidate {
        StoredRequestCandidate::new(
            id.to_string(),
            request_id.to_string(),
            Some("user-1".to_string()),
            Some("api-key-1".to_string()),
            Some("alice".to_string()),
            Some("default".to_string()),
            candidate_index,
            0,
            Some("provider-1".to_string()),
            Some("endpoint-1".to_string()),
            Some("provider-key-1".to_string()),
            status,
            None,
            false,
            status_code,
            None,
            None,
            latency_ms,
            Some(1),
            None,
            None,
            100 + i64::from(candidate_index),
            started_at_unix_secs,
            started_at_unix_secs.map(|value| value + 1),
        )
        .expect("candidate should build")
    }

    #[test]
    fn derives_request_candidate_final_status_preferring_success() {
        let candidates = vec![sample_candidate(
            "cand-1",
            "req-1",
            0,
            RequestCandidateStatus::Success,
            Some(100),
            Some(25),
            Some(200),
        )];

        assert_eq!(
            derive_request_candidate_final_status(&candidates),
            RequestCandidateFinalStatus::Success
        );
    }

    #[test]
    fn request_candidate_trace_filters_attempted_rows() {
        let trace = RequestCandidateTrace::from_candidates(
            "req-1",
            vec![
                sample_candidate(
                    "cand-1",
                    "req-1",
                    0,
                    RequestCandidateStatus::Pending,
                    None,
                    None,
                    None,
                ),
                sample_candidate(
                    "cand-2",
                    "req-1",
                    1,
                    RequestCandidateStatus::Failed,
                    Some(101),
                    Some(33),
                    Some(502),
                ),
            ],
            true,
        )
        .expect("trace should exist");

        assert_eq!(trace.total_candidates, 1);
        assert_eq!(trace.candidates[0].id, "cand-2");
        assert_eq!(trace.final_status, RequestCandidateFinalStatus::Failed);
        assert_eq!(trace.total_latency_ms, 33);
    }

    fn sample_provider() -> StoredProviderCatalogProvider {
        StoredProviderCatalogProvider::new(
            "provider-1".to_string(),
            "OpenAI".to_string(),
            Some("https://openai.com".to_string()),
            "custom".to_string(),
        )
        .expect("provider should build")
    }

    fn sample_endpoint() -> StoredProviderCatalogEndpoint {
        StoredProviderCatalogEndpoint::new(
            "endpoint-1".to_string(),
            "provider-1".to_string(),
            "openai:chat".to_string(),
            Some("openai".to_string()),
            Some("chat".to_string()),
            true,
        )
        .expect("endpoint should build")
    }

    fn sample_key() -> StoredProviderCatalogKey {
        StoredProviderCatalogKey::new(
            "provider-key-1".to_string(),
            "provider-1".to_string(),
            "prod-key".to_string(),
            "api_key".to_string(),
            Some(serde_json::json!({"cache_1h": true})),
            true,
        )
        .expect("key should build")
    }

    #[test]
    fn build_decision_trace_enriches_candidate_with_provider_catalog_metadata() {
        let trace = RequestCandidateTrace::from_candidates(
            "req-1",
            vec![sample_candidate(
                "cand-1",
                "req-1",
                0,
                RequestCandidateStatus::Failed,
                Some(101),
                Some(37),
                Some(502),
            )],
            true,
        )
        .expect("trace should exist");

        assert_eq!(
            build_decision_trace(
                trace,
                vec![sample_provider()],
                vec![sample_endpoint()],
                vec![sample_key()],
            ),
            DecisionTrace {
                request_id: "req-1".to_string(),
                total_candidates: 1,
                final_status: RequestCandidateFinalStatus::Failed,
                total_latency_ms: 37,
                candidates: vec![DecisionTraceCandidate {
                    candidate: sample_candidate(
                        "cand-1",
                        "req-1",
                        0,
                        RequestCandidateStatus::Failed,
                        Some(101),
                        Some(37),
                        Some(502),
                    ),
                    provider_name: Some("OpenAI".to_string()),
                    provider_website: Some("https://openai.com".to_string()),
                    provider_type: Some("custom".to_string()),
                    endpoint_api_format: Some("openai:chat".to_string()),
                    endpoint_api_family: Some("openai".to_string()),
                    endpoint_kind: Some("chat".to_string()),
                    provider_key_name: Some("prod-key".to_string()),
                    provider_key_auth_type: Some("api_key".to_string()),
                    provider_key_capabilities: Some(serde_json::json!({"cache_1h": true})),
                    provider_key_is_active: Some(true),
                }],
            }
        );
    }

    #[test]
    fn rejects_negative_created_at() {
        assert!(StoredRequestCandidate::new(
            "cand-1".to_string(),
            "req-1".to_string(),
            None,
            None,
            None,
            None,
            0,
            0,
            None,
            None,
            None,
            RequestCandidateStatus::Pending,
            None,
            false,
            Some(200),
            None,
            None,
            Some(10),
            Some(1),
            None,
            None,
            -1,
            None,
            None,
        )
        .is_err());
    }

    #[test]
    fn pending_without_started_at_is_not_attempted() {
        assert!(!RequestCandidateStatus::Pending.is_attempted(None));
        assert!(RequestCandidateStatus::Pending.is_attempted(Some(1)));
    }

    #[test]
    fn rejects_invalid_upsert_payload() {
        assert!(UpsertRequestCandidateRecord {
            id: "".to_string(),
            request_id: "".to_string(),
            user_id: None,
            api_key_id: None,
            username: None,
            api_key_name: None,
            candidate_index: 0,
            retry_index: 0,
            provider_id: None,
            endpoint_id: None,
            key_id: None,
            status: RequestCandidateStatus::Available,
            skip_reason: None,
            is_cached: None,
            status_code: None,
            error_type: None,
            error_message: None,
            latency_ms: None,
            concurrent_requests: None,
            extra_data: None,
            required_capabilities: None,
            created_at_unix_secs: None,
            started_at_unix_secs: None,
            finished_at_unix_secs: None,
        }
        .validate()
        .is_err());
    }
}
