mod scheduler;
mod scoring;

pub use scheduler::{
    normalize_enabled_pool_presets, run_pool_scheduler, PoolCandidateFacts, PoolCandidateInput,
    PoolCandidateOrchestration, PoolMemberSignals, PoolRuntimeState, PoolScheduledCandidate,
    PoolSchedulerOutcome, PoolSchedulingConfig, PoolSchedulingPreset, PoolSkippedCandidate,
    POOL_ACCOUNT_BLOCKED_SKIP_REASON, POOL_ACCOUNT_EXHAUSTED_SKIP_REASON,
    POOL_COOLDOWN_SKIP_REASON, POOL_COST_LIMIT_REACHED_SKIP_REASON,
};
pub use scoring::{
    probe_freshness_score, probe_freshness_score_with_ttl, score_pool_member,
    score_pool_member_with_rules, PoolMemberScoreInput, PoolMemberScoreOutput,
    PoolMemberScoreRules, PoolMemberScoreWeights, POOL_SCORE_VERSION,
    PROBE_FAILURE_COOLDOWN_THRESHOLD, PROBE_FAILURE_PENALTY, PROBE_FRESHNESS_TTL_SECONDS,
    REQUEST_FAILURE_PENALTY, UNSCHEDULABLE_SCORE_CAP,
};
