use aether_data::repository::candidates::{StoredRequestCandidate, UpsertRequestCandidateRecord};
use async_trait::async_trait;

use crate::GatewayError;

#[async_trait]
pub(crate) trait SchedulerRequestCandidateRuntimeState {
    fn has_request_candidate_data_writer(&self) -> bool;

    async fn read_request_candidates_by_request_id(
        &self,
        request_id: &str,
    ) -> Result<Vec<StoredRequestCandidate>, GatewayError>;

    async fn upsert_request_candidate(
        &self,
        candidate: UpsertRequestCandidateRecord,
    ) -> Result<Option<StoredRequestCandidate>, GatewayError>;
}
