use aether_ai_serving::{score_pool_member, PoolMemberScoreInput, POOL_SCORE_VERSION};
use aether_data_contracts::repository::pool_scores::{
    PoolMemberIdentity, PoolMemberProbeStatus, PoolScoreScope, UpsertPoolMemberScore,
    POOL_SCORE_SCOPE_KIND_MODEL,
};
use aether_data_contracts::repository::provider_catalog::StoredProviderCatalogKey;
use serde_json::Value;

use crate::handlers::shared::{provider_key_health_summary, provider_key_status_snapshot_payload};

pub(crate) fn build_provider_key_pool_score_upsert(
    key: &StoredProviderCatalogKey,
    provider_type: &str,
    api_format: &str,
    model_id: Option<&str>,
    existing: Option<&aether_data_contracts::repository::pool_scores::StoredPoolMemberScore>,
    now_unix_secs: u64,
) -> UpsertPoolMemberScore {
    let identity = PoolMemberIdentity::provider_api_key(key.provider_id.clone(), key.id.clone());
    let scope = provider_key_pool_score_scope(api_format, model_id);
    let input = provider_key_score_input(
        key,
        provider_type,
        identity.clone(),
        scope.clone(),
        existing,
        now_unix_secs,
    );
    let output = score_pool_member(&input);
    UpsertPoolMemberScore {
        id: provider_key_pool_score_id(&identity, &scope),
        identity,
        scope,
        score: output.score,
        hard_state: output.hard_state,
        score_version: POOL_SCORE_VERSION,
        score_reason: output.score_reason,
        last_ranked_at: Some(now_unix_secs),
        last_scheduled_at: existing.and_then(|score| score.last_scheduled_at),
        last_success_at: existing.and_then(|score| score.last_success_at),
        last_failure_at: existing.and_then(|score| score.last_failure_at),
        failure_count: existing.map(|score| score.failure_count).unwrap_or(0),
        last_probe_attempt_at: existing.and_then(|score| score.last_probe_attempt_at),
        last_probe_success_at: existing.and_then(|score| score.last_probe_success_at),
        last_probe_failure_at: existing.and_then(|score| score.last_probe_failure_at),
        probe_failure_count: existing.map(|score| score.probe_failure_count).unwrap_or(0),
        probe_status: existing
            .map(|score| score.probe_status)
            .unwrap_or(PoolMemberProbeStatus::Never),
        updated_at: now_unix_secs,
    }
}

pub(crate) fn provider_key_pool_score_scope(
    api_format: &str,
    model_id: Option<&str>,
) -> PoolScoreScope {
    PoolScoreScope {
        capability: api_format.trim().to_ascii_lowercase(),
        scope_kind: POOL_SCORE_SCOPE_KIND_MODEL.to_string(),
        scope_id: model_id
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
    }
}

pub(crate) fn provider_key_pool_score_id(
    identity: &PoolMemberIdentity,
    scope: &PoolScoreScope,
) -> String {
    let raw = format!(
        "{}:{}:{}:{}:{}:{}:{}",
        identity.pool_kind,
        identity.pool_id,
        identity.member_kind,
        identity.member_id,
        scope.capability,
        scope.scope_kind,
        scope.scope_id.as_deref().unwrap_or("*")
    );
    format!(
        "pms-{:016x}-{:016x}",
        stable_hash(raw.as_bytes()),
        stable_hash(identity.member_id.as_bytes())
    )
}

fn provider_key_score_input(
    key: &StoredProviderCatalogKey,
    provider_type: &str,
    identity: PoolMemberIdentity,
    scope: PoolScoreScope,
    existing: Option<&aether_data_contracts::repository::pool_scores::StoredPoolMemberScore>,
    now_unix_secs: u64,
) -> PoolMemberScoreInput {
    let status_snapshot = provider_key_status_snapshot_payload(key, provider_type);
    let quota_snapshot = status_snapshot
        .as_object()
        .and_then(|snapshot| snapshot.get("quota"))
        .and_then(Value::as_object);
    let account_snapshot = status_snapshot
        .as_object()
        .and_then(|snapshot| snapshot.get("account"))
        .and_then(Value::as_object);
    let (health_score, _, _, any_circuit_open, _) = provider_key_health_summary(key);
    let health_score = key
        .health_by_format
        .as_ref()
        .and_then(Value::as_object)
        .filter(|payload| !payload.is_empty())
        .map(|_| health_score);

    PoolMemberScoreInput {
        identity,
        scope: scope.clone(),
        internal_priority: key.internal_priority,
        is_active: key.is_active,
        health_score,
        quota_usage_ratio: quota_snapshot
            .and_then(|quota| quota.get("usage_ratio"))
            .and_then(json_f64)
            .map(|value| value.clamp(0.0, 1.0)),
        quota_exhausted: quota_snapshot
            .and_then(|quota| quota.get("exhausted"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
        account_blocked: account_snapshot
            .and_then(|account| account.get("blocked"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
        oauth_invalid_reason: key.oauth_invalid_reason.clone(),
        circuit_open: any_circuit_open
            || circuit_open_for_scope(key.circuit_breaker_by_format.as_ref(), &scope),
        success_count: key.success_count.unwrap_or(0).into(),
        error_count: key.error_count.unwrap_or(0).into(),
        total_response_time_ms: key.total_response_time_ms.unwrap_or(0).into(),
        total_tokens: key.total_tokens,
        total_cost_usd: key.total_cost_usd,
        last_used_at: key.last_used_at_unix_secs,
        last_probe_success_at: existing.and_then(|score| score.last_probe_success_at),
        probe_status: existing
            .map(|score| score.probe_status)
            .unwrap_or(PoolMemberProbeStatus::Never),
        now_unix_secs,
    }
}

fn circuit_open_for_scope(circuit_by_format: Option<&Value>, scope: &PoolScoreScope) -> bool {
    let Some(formats) = circuit_by_format.and_then(Value::as_object) else {
        return false;
    };
    let keys = api_format_lookup_keys(&scope.capability);
    keys.iter().any(|key| {
        formats
            .get(key)
            .and_then(Value::as_object)
            .and_then(|value| value.get("open"))
            .and_then(Value::as_bool)
            .unwrap_or(false)
    })
}

fn json_f64(value: &Value) -> Option<f64> {
    value.as_f64().or_else(|| {
        value
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .and_then(|value| value.parse::<f64>().ok())
    })
}

fn api_format_lookup_keys(api_format: &str) -> Vec<String> {
    let normalized = aether_ai_formats::normalize_api_format_alias(api_format);
    let mut keys = aether_ai_formats::api_format_storage_aliases(&normalized);
    if !keys.iter().any(|value| value == &normalized) {
        keys.push(normalized);
    }
    keys.sort();
    keys.dedup();
    keys
}

fn stable_hash(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
