mod memory;
mod sql;

#[allow(unused_imports)]
pub(crate) use aether_data_contracts::repository::candidate_selection::{
    MinimalCandidateSelectionReadRepository, MinimalCandidateSelectionRepository,
    StoredMinimalCandidateSelectionRow, StoredPoolKeyCandidateRowsQuery,
    StoredProviderModelMapping, StoredRequestedModelCandidateRowsQuery,
};
pub use memory::InMemoryMinimalCandidateSelectionReadRepository;
pub use sql::SqlxMinimalCandidateSelectionReadRepository;
