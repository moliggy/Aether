use aether_data_contracts::repository::pool_scores::{
    PoolMemberHardState, PoolMemberIdentity, PoolMemberProbeStatus, PoolScoreScope,
};
use serde_json::{json, Value};

pub const POOL_SCORE_VERSION: u64 = 1;
pub const PROBE_FRESHNESS_TTL_SECONDS: u64 = 30 * 60;

#[derive(Debug, Clone)]
pub struct PoolMemberScoreInput {
    pub identity: PoolMemberIdentity,
    pub scope: PoolScoreScope,
    pub internal_priority: i32,
    pub is_active: bool,
    pub health_score: Option<f64>,
    pub quota_usage_ratio: Option<f64>,
    pub quota_exhausted: bool,
    pub account_blocked: bool,
    pub oauth_invalid_reason: Option<String>,
    pub circuit_open: bool,
    pub success_count: u64,
    pub error_count: u64,
    pub total_response_time_ms: u64,
    pub total_tokens: u64,
    pub total_cost_usd: f64,
    pub last_used_at: Option<u64>,
    pub last_probe_success_at: Option<u64>,
    pub probe_status: PoolMemberProbeStatus,
    pub now_unix_secs: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PoolMemberScoreOutput {
    pub score: f64,
    pub hard_state: PoolMemberHardState,
    pub score_reason: Value,
}

pub fn score_pool_member(input: &PoolMemberScoreInput) -> PoolMemberScoreOutput {
    let hard_state = derive_hard_state(input);
    let manual_priority = manual_priority_score(input.internal_priority);
    let health = input.health_score.unwrap_or(0.5).clamp(0.0, 1.0);
    let probe_freshness = probe_freshness_score(
        input.last_probe_success_at,
        input.probe_status,
        input.now_unix_secs,
    );
    let quota_remaining = input
        .quota_usage_ratio
        .map(|ratio| 1.0 - ratio.clamp(0.0, 1.0))
        .unwrap_or(0.5);
    let latency = latency_score(input.success_count, input.total_response_time_ms);
    let cost_lru = cost_lru_score(input.total_cost_usd, input.total_tokens, input.last_used_at);

    let mut score = manual_priority * 0.30
        + health * 0.20
        + probe_freshness * 0.15
        + quota_remaining * 0.15
        + latency * 0.10
        + cost_lru * 0.10;
    if !hard_state.schedulable() {
        score = score.min(0.05);
    }
    score = score.clamp(0.0, 1.0);

    PoolMemberScoreOutput {
        score,
        hard_state,
        score_reason: json!({
            "weights": {
                "manual_priority": 0.30,
                "health": 0.20,
                "probe_freshness": 0.15,
                "quota_remaining": 0.15,
                "latency": 0.10,
                "cost_lru": 0.10
            },
            "factors": {
                "manual_priority": manual_priority,
                "health": health,
                "probe_freshness": probe_freshness,
                "quota_remaining": quota_remaining,
                "latency": latency,
                "cost_lru": cost_lru
            },
            "hard_state": hard_state.as_database(),
            "score_version": POOL_SCORE_VERSION
        }),
    }
}

fn derive_hard_state(input: &PoolMemberScoreInput) -> PoolMemberHardState {
    if !input.is_active {
        return PoolMemberHardState::Inactive;
    }
    if let Some(reason) = input.oauth_invalid_reason.as_deref() {
        let reason = reason.to_ascii_lowercase();
        if reason.contains("ban") || reason.contains("blocked") || reason.contains("suspended") {
            return PoolMemberHardState::Banned;
        }
        return PoolMemberHardState::AuthInvalid;
    }
    if input.account_blocked {
        return PoolMemberHardState::Banned;
    }
    if input.quota_exhausted {
        return PoolMemberHardState::QuotaExhausted;
    }
    if input.circuit_open {
        return PoolMemberHardState::Cooldown;
    }
    if input.health_score.is_some() || input.probe_status == PoolMemberProbeStatus::Ok {
        PoolMemberHardState::Available
    } else {
        PoolMemberHardState::Unknown
    }
}

fn manual_priority_score(internal_priority: i32) -> f64 {
    (1.0 - (f64::from(internal_priority).clamp(0.0, 100.0) / 100.0)).clamp(0.0, 1.0)
}

pub fn probe_freshness_score(
    last_probe_success_at: Option<u64>,
    probe_status: PoolMemberProbeStatus,
    now_unix_secs: u64,
) -> f64 {
    if probe_status != PoolMemberProbeStatus::Ok {
        return 0.0;
    }
    let Some(success_at) = last_probe_success_at else {
        return 0.0;
    };
    let age = now_unix_secs.saturating_sub(success_at);
    if age >= PROBE_FRESHNESS_TTL_SECONDS {
        0.0
    } else {
        1.0 - (age as f64 / PROBE_FRESHNESS_TTL_SECONDS as f64)
    }
}

fn latency_score(success_count: u64, total_response_time_ms: u64) -> f64 {
    if success_count == 0 || total_response_time_ms == 0 {
        return 0.5;
    }
    let avg = total_response_time_ms as f64 / success_count as f64;
    if avg <= 500.0 {
        1.0
    } else if avg >= 60_000.0 {
        0.0
    } else {
        1.0 - ((avg - 500.0) / 59_500.0)
    }
}

fn cost_lru_score(total_cost_usd: f64, total_tokens: u64, last_used_at: Option<u64>) -> f64 {
    let cost_penalty = if total_cost_usd.is_finite() {
        (total_cost_usd.max(0.0) / 100.0).min(0.5)
    } else {
        0.0
    };
    let token_penalty = (total_tokens as f64 / 10_000_000.0).min(0.25);
    let lru_bonus = if last_used_at.unwrap_or(0) == 0 {
        0.25
    } else {
        0.0
    };
    (0.75 - cost_penalty - token_penalty + lru_bonus).clamp(0.0, 1.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use aether_data_contracts::repository::pool_scores::{
        POOL_KIND_PROVIDER_KEY_POOL, POOL_MEMBER_KIND_PROVIDER_API_KEY, POOL_SCORE_SCOPE_KIND_MODEL,
    };

    fn input() -> PoolMemberScoreInput {
        PoolMemberScoreInput {
            identity: PoolMemberIdentity {
                pool_kind: POOL_KIND_PROVIDER_KEY_POOL.to_string(),
                pool_id: "provider-1".to_string(),
                member_kind: POOL_MEMBER_KIND_PROVIDER_API_KEY.to_string(),
                member_id: "key-1".to_string(),
            },
            scope: PoolScoreScope {
                capability: "openai:responses".to_string(),
                scope_kind: POOL_SCORE_SCOPE_KIND_MODEL.to_string(),
                scope_id: Some("model-1".to_string()),
            },
            internal_priority: 10,
            is_active: true,
            health_score: Some(1.0),
            quota_usage_ratio: Some(0.1),
            quota_exhausted: false,
            account_blocked: false,
            oauth_invalid_reason: None,
            circuit_open: false,
            success_count: 10,
            error_count: 0,
            total_response_time_ms: 2_000,
            total_tokens: 10,
            total_cost_usd: 0.01,
            last_used_at: None,
            last_probe_success_at: Some(1_000),
            probe_status: PoolMemberProbeStatus::Ok,
            now_unix_secs: 1_000,
        }
    }

    #[test]
    fn hard_state_caps_unavailable_member_score() {
        let mut input = input();
        input.oauth_invalid_reason = Some("token invalid".to_string());

        let output = score_pool_member(&input);

        assert_eq!(output.hard_state, PoolMemberHardState::AuthInvalid);
        assert!(output.score <= 0.05);
    }

    #[test]
    fn probe_freshness_has_ttl() {
        assert_eq!(
            probe_freshness_score(Some(1_000), PoolMemberProbeStatus::Ok, 1_000),
            1.0
        );
        assert_eq!(
            probe_freshness_score(
                Some(1_000),
                PoolMemberProbeStatus::Ok,
                1_000 + PROBE_FRESHNESS_TTL_SECONDS
            ),
            0.0
        );
    }
}
