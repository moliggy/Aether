use std::fs;
use std::path::{Path, PathBuf};

fn collect_rust_files(root: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(root).expect("directory should be readable") {
        let entry = entry.expect("directory entry should be readable");
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, files);
            continue;
        }
        if path.extension().and_then(|value| value.to_str()) == Some("rs") {
            files.push(path);
        }
    }
}

fn assert_no_sqlx_queries(root_relative_path: &str) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(root_relative_path);
    let mut files = Vec::new();
    collect_rust_files(&root, &mut files);

    let patterns = [
        "sqlx::query(",
        "sqlx::query_scalar",
        "query_scalar::<",
        "QueryBuilder<",
    ];
    let violations = files
        .into_iter()
        .filter_map(|path| {
            let source = fs::read_to_string(&path).expect("source file should be readable");
            let hits = patterns
                .iter()
                .filter(|pattern| source.contains(**pattern))
                .copied()
                .collect::<Vec<_>>();
            if hits.is_empty() {
                None
            } else {
                Some(format!("{} -> {}", path.display(), hits.join(", ")))
            }
        })
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "disallowed SQL ownership violations:\n{}",
        violations.join("\n")
    );
}

fn assert_no_sensitive_log_patterns(root_relative_path: &str, patterns: &[&str]) {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(root_relative_path);
    let mut files = Vec::new();
    collect_rust_files(&root, &mut files);

    let violations = files
        .into_iter()
        .filter_map(|path| {
            let source = fs::read_to_string(&path).expect("source file should be readable");
            let hits = patterns
                .iter()
                .filter(|pattern| source.contains(**pattern))
                .copied()
                .collect::<Vec<_>>();
            if hits.is_empty() {
                None
            } else {
                Some(format!("{} -> {}", path.display(), hits.join(", ")))
            }
        })
        .collect::<Vec<_>>();

    assert!(
        violations.is_empty(),
        "disallowed sensitive logging patterns:\n{}",
        violations.join("\n")
    );
}

fn read_workspace_file(path: &str) -> String {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root should resolve");
    fs::read_to_string(workspace_root.join(path)).expect("source file should be readable")
}

#[test]
fn handlers_do_not_inline_sql_queries() {
    assert_no_sqlx_queries("src/handlers");
}

#[test]
fn gateway_runtime_does_not_inline_sql_queries() {
    assert_no_sqlx_queries("src/state/runtime");
}

#[test]
fn wallet_repository_does_not_reexport_settlement_types() {
    let wallet_mod = read_workspace_file("crates/aether-data/src/repository/wallet/mod.rs");
    let wallet_types = read_workspace_file("crates/aether-data/src/repository/wallet/types.rs");
    let wallet_sql = read_workspace_file("crates/aether-data/src/repository/wallet/sql.rs");
    let wallet_memory = read_workspace_file("crates/aether-data/src/repository/wallet/memory.rs");

    assert!(
        !wallet_mod.contains("StoredUsageSettlement"),
        "wallet/mod.rs should not export StoredUsageSettlement"
    );
    assert!(
        !wallet_mod.contains("UsageSettlementInput"),
        "wallet/mod.rs should not export UsageSettlementInput"
    );
    assert!(
        !wallet_types.contains("pub use crate::repository::settlement"),
        "wallet/types.rs should not re-export settlement types"
    );
    assert!(
        !wallet_types.contains("async fn settle_usage("),
        "wallet/types.rs should not own settlement entrypoints"
    );
    assert!(
        !wallet_sql.contains("impl SettlementWriteRepository"),
        "wallet/sql.rs should not implement SettlementWriteRepository"
    );
    assert!(
        !wallet_memory.contains("impl SettlementWriteRepository"),
        "wallet/memory.rs should not implement SettlementWriteRepository"
    );
}

#[test]
fn usage_runtime_paths_depend_on_shared_crates_not_app_runtime_shims() {
    for path in [
        "apps/aether-gateway/src/usage/runtime.rs",
        "apps/aether-gateway/src/usage/worker.rs",
        "apps/aether-gateway/src/async_task/runtime.rs",
    ] {
        let source = read_workspace_file(path);
        assert!(
            source.contains("aether_usage_runtime"),
            "{path} should depend on aether_usage_runtime"
        );
        assert!(
            !source.contains("wallet_runtime"),
            "{path} should not depend on wallet_runtime"
        );
    }

    for path in ["apps/aether-gateway/src/async_task/runtime.rs"] {
        let source = read_workspace_file(path);
        assert!(
            source.contains("aether_billing"),
            "{path} should depend on aether_billing"
        );
        assert!(
            !source.contains("billing_runtime::enrich_usage_event_with_billing"),
            "{path} should not depend on billing_runtime compat re-export"
        );
        assert!(
            !source.contains("settlement_runtime::settle_usage_if_needed"),
            "{path} should not depend on settlement_runtime compat re-export"
        );
    }

    let usage_runtime = read_workspace_file("apps/aether-gateway/src/usage/runtime.rs");
    assert!(
        !usage_runtime.contains("GatewayDataState"),
        "usage/runtime.rs should not own GatewayDataState integration impls anymore"
    );
    assert!(
        !usage_runtime.contains("UsageBillingEventEnricher"),
        "usage/runtime.rs should not own UsageBillingEventEnricher impl anymore"
    );
    assert!(
        !usage_runtime.contains("UsageRuntimeAccess"),
        "usage/runtime.rs should not own UsageRuntimeAccess impl anymore"
    );

    let usage_worker = read_workspace_file("apps/aether-gateway/src/usage/worker.rs");
    let runtime_usage_worker = usage_worker
        .split("#[cfg(test)]")
        .next()
        .unwrap_or(usage_worker.as_str());
    assert!(
        !runtime_usage_worker.contains("GatewayDataState"),
        "usage/worker.rs runtime path should not own GatewayDataState integration impls anymore"
    );
    assert!(
        !runtime_usage_worker.contains("UsageRecordWriter"),
        "usage/worker.rs runtime path should not own UsageRecordWriter impl anymore"
    );

    let integrations =
        read_workspace_file("apps/aether-gateway/src/data/state/integrations.rs");
    for pattern in [
        "UsageBillingEventEnricher for GatewayDataState",
        "UsageRuntimeAccess for GatewayDataState",
        "UsageRecordWriter for GatewayDataState",
        "UsageSettlementWriter for GatewayDataState",
    ] {
        assert!(
            integrations.contains(pattern),
            "data/state/integrations.rs should centralize {pattern}"
        );
    }

    let usage_reporting_context =
        read_workspace_file("apps/aether-gateway/src/usage/reporting/context.rs");
    assert!(
        usage_reporting_context.contains("aether_usage_runtime"),
        "usage/reporting/context.rs should depend on aether_usage_runtime"
    );
    assert!(
        usage_reporting_context.contains("resolve_video_task_report_lookup"),
        "usage/reporting/context.rs should depend on shared video task report lookup helper"
    );
    for pattern in [
        "build_locally_actionable_report_context_from_request_candidate",
        "build_locally_actionable_report_context_from_video_task",
        "report_context_is_locally_actionable",
    ] {
        assert!(
            usage_reporting_context.contains(pattern),
            "usage/reporting/context.rs should depend on shared usage helper {pattern}"
        );
    }
    assert!(
        usage_reporting_context.contains("VideoTaskReportLookup::TaskIdOrExternal"),
        "usage/reporting/context.rs should keep app-local external task fallback orchestration"
    );
    for pattern in [
        "context\n        .get(\"local_task_id\")",
        "context\n        .get(\"local_short_id\")",
        "context\n        .get(\"task_id\")",
        "VideoTaskLookupKey::ShortId(short_id)",
        "fn insert_missing_string_value(",
        "fn insert_missing_optional_string_value(",
        "fn has_non_empty_str(",
        "fn has_u64(",
    ] {
        assert!(
            !usage_reporting_context.contains(pattern),
            "usage/reporting/context.rs should not own video task report lookup parsing {pattern}"
        );
    }

    let usage_reporting_mod = read_workspace_file("apps/aether-gateway/src/usage/reporting/mod.rs");
    assert!(
        usage_reporting_mod.contains("aether_usage_runtime"),
        "usage/reporting/mod.rs should depend on aether_usage_runtime"
    );
    for pattern in [
        "is_local_ai_sync_report_kind",
        "is_local_ai_stream_report_kind",
        "sync_report_represents_failure",
        "extract_gemini_file_mapping_entries",
        "gemini_file_mapping_cache_key",
        "normalize_gemini_file_name",
        "report_request_id",
        "should_handle_local_sync_report",
        "should_handle_local_stream_report",
        "GEMINI_FILE_MAPPING_TTL_SECONDS",
    ] {
        assert!(
            usage_reporting_mod.contains(pattern),
            "usage/reporting/mod.rs should depend on shared usage helper {pattern}"
        );
    }
    for pattern in [
        "fn is_local_ai_sync_report_kind(",
        "fn is_local_ai_stream_report_kind(",
        "fn sync_report_represents_failure(",
        "fn extract_gemini_file_mapping_entries(",
        "fn maybe_push_local_gemini_file_mapping_entry(",
        "fn extract_sync_report_body_json(",
        "fn content_type_starts_with(",
        "fn normalize_file_name(",
        "const GEMINI_FILE_MAPPING_TTL_SECONDS",
        "const GEMINI_FILE_MAPPING_CACHE_PREFIX",
        "fn gemini_file_mapping_cache_key(",
        "fn report_request_id(",
        "fn should_handle_local_sync_report(",
        "fn should_handle_local_stream_report(",
        "\"openai_video_delete_sync_success\" && payload.status_code == 404",
    ] {
        assert!(
            !usage_reporting_mod.contains(pattern),
            "usage/reporting/mod.rs should not own local report classification logic {pattern}"
        );
    }
}

#[test]
fn gateway_system_config_types_are_owned_by_aether_data() {
    let state_mod = read_workspace_file("apps/aether-gateway/src/data/state/mod.rs");
    assert!(
        state_mod.contains("aether_data::repository::system"),
        "data/state/mod.rs should depend on aether-data system types"
    );
    assert!(
        !state_mod.contains("pub(crate) struct StoredSystemConfigEntry"),
        "data/state/mod.rs should not define StoredSystemConfigEntry locally"
    );

    let state_core = read_workspace_file("apps/aether-gateway/src/data/state/core.rs");
    for pattern in [
        "backend.list_system_config_entries().await",
        "upsert_system_config_entry(key, value, description)",
        "AdminSystemStats::default()",
    ] {
        assert!(
            state_core.contains(pattern),
            "data/state/core.rs should use shared system DTO path {pattern}"
        );
    }
    for pattern in [
        "|(key, value, description, updated_at_unix_secs)|",
        "Ok((0, 0, 0, 0))",
    ] {
        assert!(
            !state_core.contains(pattern),
            "data/state/core.rs should not own local system DTO projection {pattern}"
        );
    }

    let system_types = read_workspace_file("crates/aether-data/src/repository/system.rs");
    for pattern in [
        "pub struct StoredSystemConfigEntry",
        "pub struct AdminSystemStats",
        "pub struct AdminSecurityBlacklistEntry",
    ] {
        assert!(
            system_types.contains(pattern),
            "aether-data system module should own {pattern}"
        );
    }

    let admin_types = read_workspace_file("apps/aether-gateway/src/state/admin_types.rs");
    assert!(
        admin_types.contains("aether_data::repository::system::AdminSecurityBlacklistEntry"),
        "state/admin_types.rs should re-export AdminSecurityBlacklistEntry from aether-data"
    );
    assert!(
        !admin_types.contains("struct AdminSecurityBlacklistEntry"),
        "state/admin_types.rs should not define AdminSecurityBlacklistEntry locally"
    );

    let runtime_mod = read_workspace_file("apps/aether-gateway/src/state/runtime/mod.rs");
    assert!(
        !runtime_mod.contains("AdminSecurityBlacklistEntryPayload"),
        "state/runtime/mod.rs should not keep the unused blacklist payload wrapper"
    );
}

#[test]
fn gateway_auth_snapshot_type_is_owned_by_aether_data() {
    let gateway_auth = read_workspace_file("apps/aether-gateway/src/data/auth.rs");
    assert!(
        gateway_auth.contains("aether_data::repository::auth"),
        "data/auth.rs should depend on aether-data auth snapshot types"
    );
    assert!(
        gateway_auth.contains("ResolvedAuthApiKeySnapshot as GatewayAuthApiKeySnapshot"),
        "data/auth.rs should expose the shared resolved auth snapshot type under the gateway-facing name"
    );
    for pattern in [
        "pub(crate) struct GatewayAuthApiKeySnapshot",
        "pub(crate) async fn read_auth_api_key_snapshot(",
        "pub(crate) async fn read_auth_api_key_snapshot_by_key_hash(",
        "fn effective_allowed_providers(",
        "fn effective_allowed_api_formats(",
        "fn effective_allowed_models(",
    ] {
        assert!(
            !gateway_auth.contains(pattern),
            "data/auth.rs should not own local auth snapshot logic {pattern}"
        );
    }

    let auth_types = read_workspace_file("crates/aether-data/src/repository/auth/types.rs");
    for pattern in [
        "pub struct ResolvedAuthApiKeySnapshot",
        "pub trait ResolvedAuthApiKeySnapshotReader",
        "pub async fn read_resolved_auth_api_key_snapshot(",
        "pub async fn read_resolved_auth_api_key_snapshot_by_key_hash(",
        "pub async fn read_resolved_auth_api_key_snapshot_by_user_api_key_ids(",
        "pub fn effective_allowed_providers(&self)",
        "pub fn effective_allowed_api_formats(&self)",
        "pub fn effective_allowed_models(&self)",
    ] {
        assert!(
            auth_types.contains(pattern),
            "aether-data auth types should own {pattern}"
        );
    }
}

#[test]
fn gateway_auth_data_layer_does_not_keep_ldap_row_wrapper() {
    let gateway_auth_state =
        read_workspace_file("apps/aether-gateway/src/data/state/auth.rs");
    for pattern in [
        "struct StoredLdapAuthUserRow",
        "fn map_ldap_user_auth_row(",
        "Result<Option<StoredLdapAuthUserRow>, DataLayerError>",
        "existing.user.",
    ] {
        assert!(
            !gateway_auth_state.contains(pattern),
            "data/state/auth.rs should not keep ldap row wrapper {pattern}"
        );
    }

    for pattern in [
        "Result<Option<StoredUserAuthRecord>, DataLayerError>",
        "return map_user_auth_row(row).map(Some);",
    ] {
        assert!(
            gateway_auth_state.contains(pattern),
            "data/state/auth.rs should use shared user auth record directly via {pattern}"
        );
    }
}

#[test]
fn non_admin_handlers_do_not_depend_on_admin_stats_module() {
    let handlers_mod = read_workspace_file("apps/aether-gateway/src/handlers/mod.rs");
    for pattern in [
        "pub(crate) use admin::stats::{",
        "admin_stats_bad_request_response",
        "list_usage_for_optional_range",
        "parse_bounded_u32",
        "round_to",
        "AdminStatsTimeRange",
        "AdminStatsUsageFilter",
    ] {
        assert!(
            handlers_mod.contains(pattern),
            "handlers/mod.rs should re-export shared app-local stats helper {pattern}"
        );
    }

    for path in [
        "apps/aether-gateway/src/handlers/public/support/user_me.rs",
        "apps/aether-gateway/src/handlers/public/support/wallet/reads.rs",
        "apps/aether-gateway/src/handlers/admin/monitoring/cache_store.rs",
    ] {
        let file = read_workspace_file(path);
        assert!(
            !file.contains("handlers::admin::stats::"),
            "{path} should not depend directly on admin::stats"
        );
    }
}

#[test]
fn admin_monitoring_snapshots_stay_app_local() {
    let monitoring_common =
        read_workspace_file("apps/aether-gateway/src/handlers/admin/monitoring/common.rs");
    for pattern in [
        "pub(super) struct AdminMonitoringCacheSnapshot",
        "pub(super) struct AdminMonitoringCacheAffinityRecord",
        "pub(super) struct AdminMonitoringResilienceSnapshot",
    ] {
        assert!(
            monitoring_common.contains(pattern),
            "monitoring/common.rs should own {pattern}"
        );
    }

    let monitoring_resilience =
        read_workspace_file("apps/aether-gateway/src/handlers/admin/monitoring/resilience.rs");
    assert!(
        monitoring_resilience.contains("AdminMonitoringResilienceSnapshot"),
        "monitoring/resilience.rs should depend on shared monitoring snapshot type"
    );
    assert!(
        !monitoring_resilience.contains("struct AdminMonitoringResilienceSnapshot"),
        "monitoring/resilience.rs should not define AdminMonitoringResilienceSnapshot locally"
    );

    let data_system = read_workspace_file("crates/aether-data/src/repository/system.rs");
    assert!(
        !data_system.contains("AdminMonitoringCacheSnapshot")
            && !data_system.contains("AdminMonitoringResilienceSnapshot"),
        "monitoring snapshots are admin view DTOs and should not move into aether-data"
    );
}

#[test]
fn gateway_provider_oauth_storage_types_are_owned_by_aether_data() {
    let provider_oauth_state =
        read_workspace_file("apps/aether-gateway/src/handlers/admin/provider_oauth/state.rs");
    assert!(
        provider_oauth_state.contains("aether_data::repository::provider_oauth"),
        "provider_oauth/state.rs should depend on aether-data provider oauth storage types"
    );
    for pattern in [
        "pub(crate) struct StoredAdminProviderOAuthDeviceSession",
        "pub(crate) struct StoredAdminProviderOAuthState",
        "const KIRO_DEVICE_AUTH_SESSION_PREFIX",
        "fn provider_oauth_device_session_key(",
        "fn build_provider_oauth_batch_task_status_payload(",
        "fn provider_oauth_batch_task_key(",
        "const PROVIDER_OAUTH_BATCH_TASK_TTL_SECS",
        "format!(\"provider_oauth_state:{nonce}\")",
    ] {
        assert!(
            !provider_oauth_state.contains(pattern),
            "provider_oauth/state.rs should not own local storage helper {pattern}"
        );
    }

    let dispatch_device = read_workspace_file(
        "apps/aether-gateway/src/handlers/admin/provider_oauth/dispatch/device.rs",
    );
    assert!(
        dispatch_device.contains("aether_data::repository::provider_oauth"),
        "provider_oauth/dispatch/device.rs should use shared provider oauth storage DTOs"
    );

    let shared_provider_oauth =
        read_workspace_file("crates/aether-data/src/repository/provider_oauth.rs");
    for pattern in [
        "pub struct StoredAdminProviderOAuthDeviceSession",
        "pub struct StoredAdminProviderOAuthState",
        "pub fn provider_oauth_device_session_storage_key(",
        "pub fn provider_oauth_state_storage_key(",
        "pub fn provider_oauth_batch_task_storage_key(",
        "pub fn build_provider_oauth_batch_task_status_payload(",
        "pub const KIRO_DEVICE_AUTH_SESSION_TTL_BUFFER_SECS: u64 = 60;",
        "pub const PROVIDER_OAUTH_BATCH_TASK_TTL_SECS: u64 = 24 * 60 * 60;",
        "pub const PROVIDER_OAUTH_STATE_TTL_SECS: u64 = 600;",
    ] {
        assert!(
            shared_provider_oauth.contains(pattern),
            "aether-data provider oauth storage module should own {pattern}"
        );
    }
}

#[test]
fn gateway_request_candidate_trace_type_is_owned_by_aether_data() {
    let gateway_candidates =
        read_workspace_file("apps/aether-gateway/src/data/candidates.rs");
    assert!(
        gateway_candidates.contains("aether_data::repository::candidates"),
        "data/candidates.rs should depend on aether-data request candidate types"
    );
    assert!(
        gateway_candidates.contains("RequestCandidateTrace::from_candidates"),
        "data/candidates.rs should build traces through shared candidate trace helper"
    );
    for pattern in [
        "pub(crate) enum RequestCandidateFinalStatus",
        "pub(crate) struct RequestCandidateTrace",
        "fn derive_final_status(",
    ] {
        assert!(
            !gateway_candidates.contains(pattern),
            "data/candidates.rs should not own local request candidate trace logic {pattern}"
        );
    }

    let candidate_types =
        read_workspace_file("crates/aether-data/src/repository/candidates/types.rs");
    for pattern in [
        "pub enum RequestCandidateFinalStatus",
        "pub struct RequestCandidateTrace",
        "pub fn derive_request_candidate_final_status(",
        "pub fn from_candidates(",
    ] {
        assert!(
            candidate_types.contains(pattern),
            "aether-data candidate types should own {pattern}"
        );
    }
}

#[test]
fn gateway_decision_trace_type_is_owned_by_aether_data() {
    let gateway_decision_trace =
        read_workspace_file("apps/aether-gateway/src/data/decision_trace.rs");
    assert!(
        gateway_decision_trace.contains("aether_data::repository::candidates"),
        "data/decision_trace.rs should depend on aether-data candidate trace types"
    );
    assert!(
        gateway_decision_trace.contains("build_decision_trace"),
        "data/decision_trace.rs should build enriched traces through shared decision trace helper"
    );
    for pattern in [
        "pub(crate) struct DecisionTraceCandidate",
        "pub(crate) struct DecisionTrace",
        "fn enrich_candidate(",
    ] {
        assert!(
            !gateway_decision_trace.contains(pattern),
            "data/decision_trace.rs should not own local decision trace logic {pattern}"
        );
    }

    let candidate_types =
        read_workspace_file("crates/aether-data/src/repository/candidates/types.rs");
    for pattern in [
        "pub struct DecisionTraceCandidate",
        "pub struct DecisionTrace",
        "pub fn build_decision_trace(",
    ] {
        assert!(
            candidate_types.contains(pattern),
            "aether-data candidate types should own {pattern}"
        );
    }
}

#[test]
fn scheduler_candidate_runtime_paths_depend_on_scheduler_core_and_state_trait() {
    let candidate_mod = read_workspace_file("apps/aether-gateway/src/scheduler/candidate/mod.rs");
    assert!(
        candidate_mod.contains("SchedulerMinimalCandidateSelectionCandidate"),
        "candidate/mod.rs should depend on core minimal candidate DTO"
    );
    assert!(
        candidate_mod.contains("build_minimal_candidate_selection"),
        "candidate/mod.rs should depend on core minimal candidate builder"
    );
    assert!(
        candidate_mod.contains("collect_global_model_names_for_required_capability"),
        "candidate/mod.rs should depend on core capability model-name collector"
    );
    assert!(
        candidate_mod.contains("collect_selectable_candidates_from_keys"),
        "candidate/mod.rs should depend on core selectable-candidate collector"
    );
    assert!(
        candidate_mod.contains("auth_api_key_concurrency_limit_reached"),
        "candidate/mod.rs should depend on core auth api key concurrency helper"
    );
    assert!(
        !candidate_mod.contains("pub(crate) struct GatewayMinimalCandidateSelectionCandidate"),
        "candidate/mod.rs should not own the minimal candidate DTO"
    );
    for pattern in [
        "resolve_provider_model_name(&row",
        "extract_global_priority_for_format(",
        "compare_affinity_order(",
        "row_supports_required_capability(&row",
        "selected.push(candidate);",
        "if let Some(target) = cached_affinity_target",
        "count_recent_active_requests_for_api_key(",
    ] {
        assert!(
            !candidate_mod.contains(pattern),
            "candidate/mod.rs should not own {pattern}"
        );
    }

    let selection = read_workspace_file("apps/aether-gateway/src/scheduler/candidate/selection.rs");
    assert!(
        selection.contains("SchedulerRuntimeState"),
        "selection.rs should depend on SchedulerRuntimeState"
    );
    assert!(
        selection.contains("aether_scheduler_core::{")
            && selection.contains("SchedulerAffinityTarget"),
        "selection.rs should depend on core SchedulerAffinityTarget"
    );
    assert!(
        !selection.contains("use crate::{AppState"),
        "selection.rs should not depend on AppState directly"
    );
    assert!(
        !selection.contains("crate::cache::SchedulerAffinityTarget"),
        "selection.rs should not depend on gateway-local SchedulerAffinityTarget"
    );
    assert!(
        selection.contains("reorder_candidates_by_scheduler_health_in_core"),
        "selection.rs should depend on core candidate reorder helper"
    );
    assert!(
        selection.contains("candidate_is_selectable_with_runtime_state"),
        "selection.rs should depend on core selectable predicate helper"
    );
    for pattern in [
        "fn compare_provider_key_health_order(",
        "fn candidate_provider_key_health_bucket(",
        "fn candidate_provider_key_health_score(",
        "count_recent_active_requests_for_provider(",
        "is_candidate_in_recent_failure_cooldown(",
        "provider_key_health_score(",
        "provider_key_rpm_allows_request_since(",
    ] {
        assert!(
            !selection.contains(pattern),
            "selection.rs should not own {pattern}"
        );
    }

    let affinity = read_workspace_file("apps/aether-gateway/src/scheduler/candidate/affinity.rs");
    assert!(
        affinity.contains("SchedulerRuntimeState"),
        "affinity.rs should depend on SchedulerRuntimeState"
    );
    assert!(
        affinity.contains("aether_scheduler_core::{")
            && affinity.contains("SchedulerAffinityTarget"),
        "affinity.rs should depend on core SchedulerAffinityTarget"
    );
    for pattern in [
        "candidate_affinity_hash",
        "compare_affinity_order",
        "matches_affinity_target",
        "candidate_key",
    ] {
        assert!(
            affinity.contains(pattern),
            "affinity.rs should depend on core affinity helper {pattern}"
        );
    }
    assert!(
        !affinity.contains("use crate::AppState"),
        "affinity.rs should not depend on AppState directly"
    );
    assert!(
        !affinity.contains("crate::cache::SchedulerAffinityTarget"),
        "affinity.rs should not depend on gateway-local SchedulerAffinityTarget"
    );
    assert!(
        !affinity.contains("use sha2::{Digest, Sha256};"),
        "affinity.rs should not own affinity hashing implementation anymore"
    );
    for pattern in [
        "fn compare_affinity_order(",
        "fn candidate_affinity_hash(",
        "fn matches_affinity_target(",
        "fn candidate_key(",
    ] {
        assert!(
            !affinity.contains(pattern),
            "affinity.rs should not own {pattern}"
        );
    }

    let model = read_workspace_file("apps/aether-gateway/src/scheduler/candidate/model.rs");
    assert!(
        model.contains("SchedulerAuthConstraints"),
        "model.rs should depend on SchedulerAuthConstraints"
    );
    assert!(
        model.contains("SchedulerCandidateSelectionRowSource"),
        "model.rs should depend on SchedulerCandidateSelectionRowSource"
    );
    assert!(
        model.contains("auth_constraints_allow_provider"),
        "model.rs should depend on core auth helpers"
    );
    assert!(
        model.contains("candidate_supports_required_capability"),
        "model.rs should depend on core candidate capability helper"
    );
    assert!(
        !model.contains("use regex::Regex;"),
        "model.rs should not own regex model-matching implementation anymore"
    );
    assert!(
        !model.contains("GatewayDataState"),
        "model.rs should not depend on GatewayDataState directly"
    );
    assert!(
        !model.contains("fn candidate_supports_required_capability("),
        "model.rs should not own candidate capability matching anymore"
    );

    let affinity_cache =
        read_workspace_file("apps/aether-gateway/src/cache/scheduler_affinity.rs");
    assert!(
        affinity_cache.contains("aether_scheduler_core::SchedulerAffinityTarget"),
        "scheduler affinity cache should reuse core SchedulerAffinityTarget"
    );

    let candidate_state =
        read_workspace_file("apps/aether-gateway/src/scheduler/candidate/state.rs");
    assert!(
        candidate_state.contains("pub(crate) trait SchedulerRuntimeState"),
        "candidate/state.rs should host SchedulerRuntimeState"
    );
    assert!(
        candidate_state
            .contains("pub(crate) trait SchedulerCandidateSelectionRowSource"),
        "candidate/state.rs should host SchedulerCandidateSelectionRowSource"
    );
    for pattern in [
        "impl SchedulerCandidateSelectionRowSource for GatewayDataState",
        "impl SchedulerCandidateSelectionRowSource for AppState",
        "impl SchedulerRuntimeState for AppState",
    ] {
        assert!(
            !candidate_state.contains(pattern),
            "candidate/state.rs should not host {pattern} anymore"
        );
    }

    let state_integrations =
        read_workspace_file("apps/aether-gateway/src/state/integrations.rs");
    for pattern in [
        "impl SchedulerCandidateSelectionRowSource for AppState",
        "impl SchedulerRuntimeState for AppState",
    ] {
        assert!(
            state_integrations.contains(pattern),
            "state/integrations.rs should host {pattern}"
        );
    }

    let data_state_integrations =
        read_workspace_file("apps/aether-gateway/src/data/state/integrations.rs");
    assert!(
        data_state_integrations.contains(
            "impl SchedulerCandidateSelectionRowSource for GatewayDataState"
        ),
        "data/state/integrations.rs should host SchedulerCandidateSelectionRowSource for GatewayDataState"
    );
}

#[test]
fn gateway_request_audit_bundle_type_is_owned_by_aether_data() {
    let usage_http = read_workspace_file("apps/aether-gateway/src/usage/http.rs");
    assert!(
        usage_http.contains("aether_data::repository::audit::RequestAuditBundle"),
        "usage/http.rs should depend on aether-data request audit bundle type"
    );
    assert!(
        usage_http.contains("aether_data::repository::usage::StoredRequestUsageAudit"),
        "usage/http.rs should depend on aether-data usage audit type"
    );

    let gateway_audit =
        read_workspace_file("apps/aether-gateway/src/state/runtime/audit.rs");
    assert!(
        gateway_audit.contains("aether_data::repository::audit::RequestAuditBundle"),
        "state/runtime/audit.rs should depend on the shared request audit bundle type"
    );
    assert!(
        gateway_audit.contains("aether_data::repository::usage::StoredRequestUsageAudit"),
        "state/runtime/audit.rs should depend on the shared usage audit type"
    );

    let usage_mod = read_workspace_file("apps/aether-gateway/src/usage/mod.rs");
    for pattern in ["mod bundle;", "mod read;"] {
        assert!(
            !usage_mod.contains(pattern),
            "usage/mod.rs should not keep local audit compatibility modules {pattern}"
        );
    }

    let audit_types = read_workspace_file("crates/aether-data/src/repository/audit.rs");
    for pattern in [
        "pub struct RequestAuditBundle",
        "pub trait RequestAuditReader",
        "pub async fn read_request_audit_bundle(",
    ] {
        assert!(
            audit_types.contains(pattern),
            "aether-data request audit module should own {pattern}"
        );
    }
}

#[test]
fn scheduler_request_candidate_runtime_paths_depend_on_scheduler_core() {
    let request_candidates =
        read_workspace_file("apps/aether-gateway/src/scheduler/request_candidates.rs");
    let runtime_request_candidates = request_candidates
        .split("#[cfg(test)]")
        .next()
        .unwrap_or(request_candidates.as_str());
    assert!(
        request_candidates.contains("aether_scheduler_core"),
        "request_candidates.rs should depend on aether-scheduler-core"
    );
    assert!(
        request_candidates.contains("SchedulerRequestCandidateRuntimeState"),
        "request_candidates.rs should depend on SchedulerRequestCandidateRuntimeState"
    );
    for pattern in [
        "parse_request_candidate_report_context",
        "resolve_report_request_candidate_slot_from_candidates",
        "execution_error_details",
        "build_execution_request_candidate_seed",
        "finalize_execution_request_candidate_report_context",
        "build_local_request_candidate_status_record",
        "build_report_request_candidate_status_record",
    ] {
        assert!(
            runtime_request_candidates.contains(pattern),
            "request_candidates.rs should depend on shared helper {pattern}"
        );
    }
    for pattern in [
        "fn match_existing_report_candidate(",
        "fn next_candidate_index(",
        "fn build_report_candidate_extra_data(",
        "fn is_terminal_candidate_status(",
        "parse_report_context(report_context)?",
        "StoredRequestCandidate",
        "let mut context = report_context",
        "context.insert(\"request_id\"",
        "UpsertRequestCandidateRecord {",
        "use crate::AppState",
    ] {
        assert!(
            !runtime_request_candidates.contains(pattern),
            "request_candidates.rs should not own {pattern}"
        );
    }

    let request_candidate_state =
        read_workspace_file("apps/aether-gateway/src/scheduler/request_candidate_state.rs");
    assert!(
        request_candidate_state
            .contains("pub(crate) trait SchedulerRequestCandidateRuntimeState"),
        "request_candidate_state.rs should host SchedulerRequestCandidateRuntimeState"
    );
    assert!(
        !request_candidate_state
            .contains("impl SchedulerRequestCandidateRuntimeState for AppState"),
        "request_candidate_state.rs should not host AppState integration impl anymore"
    );

    let state_integrations =
        read_workspace_file("apps/aether-gateway/src/state/integrations.rs");
    assert!(
        state_integrations.contains("impl SchedulerRequestCandidateRuntimeState for AppState"),
        "state/integrations.rs should host SchedulerRequestCandidateRuntimeState for AppState"
    );
}

#[test]
fn gateway_data_state_does_not_depend_on_scheduler_candidate_selection() {
    let state_mod = read_workspace_file("apps/aether-gateway/src/data/state/mod.rs");
    let state_runtime =
        read_workspace_file("apps/aether-gateway/src/data/state/runtime.rs");
    let app_audit = read_workspace_file("apps/aether-gateway/src/state/runtime/audit.rs");

    assert!(
        !state_mod.contains("read_minimal_candidate_selection"),
        "data/state/mod.rs should not import scheduler candidate selection entrypoints"
    );
    assert!(
        !state_runtime.contains("pub(crate) async fn read_minimal_candidate_selection("),
        "data/state/runtime.rs should not own scheduler minimal candidate derived read"
    );
    assert!(
        app_audit.contains("scheduler::read_minimal_candidate_selection("),
        "state/runtime/audit.rs should call scheduler boundary directly"
    );
}

#[test]
fn model_fetch_runtime_paths_depend_on_shared_crates_not_local_pure_helpers() {
    let runtime = read_workspace_file("apps/aether-gateway/src/model_fetch/runtime.rs");
    assert!(
        runtime.contains("aether_model_fetch"),
        "model_fetch/runtime.rs should depend on aether_model_fetch"
    );
    assert!(
        runtime.contains("ModelFetchRuntimeState"),
        "model_fetch/runtime.rs should depend on ModelFetchRuntimeState"
    );
    assert!(
        runtime.contains("build_models_fetch_execution_plan"),
        "model_fetch/runtime.rs should depend on shared models fetch plan builder"
    );
    for pattern in [
        "fn apply_model_filters(",
        "fn aggregate_models_for_cache(",
        "fn build_models_fetch_url(",
        "fn build_models_fetch_execution_plan(",
        "fn parse_models_response(",
        "fn select_models_fetch_endpoint(",
        "fn model_fetch_interval_minutes(",
        "fn resolve_models_fetch_auth(",
        "state.data.has_provider_catalog_reader()",
        "execute_execution_runtime_sync_plan(state, None, &plan)",
        "resolve_local_standard_auth(",
        "resolve_local_gemini_auth(",
        "resolve_local_openai_chat_auth(",
        "resolve_local_vertex_api_key_query_auth(",
        "apply_local_header_rules(",
        "ensure_upstream_auth_header(",
    ] {
        assert!(
            !runtime.contains(pattern),
            "model_fetch/runtime.rs should not own {pattern}"
        );
    }

    assert!(
        runtime.contains("sync_provider_model_whitelist_associations"),
        "model_fetch/runtime.rs should call shared whitelist sync helper"
    );
    assert!(
        !runtime.contains("mod association_sync;"),
        "model_fetch/runtime.rs should not keep a local association_sync module"
    );
    assert!(
        !runtime.contains("fn sync_provider_model_whitelist_associations("),
        "model_fetch/runtime.rs should not own whitelist sync logic"
    );

    let runtime_state = read_workspace_file("apps/aether-gateway/src/model_fetch/runtime/state.rs");
    assert!(
        runtime_state.contains("pub(crate) trait ModelFetchRuntimeState"),
        "model_fetch/runtime/state.rs should host the runtime state trait definition"
    );
    for pattern in [
        "impl ModelFetchTransportRuntime for AppState",
        "impl ModelFetchRuntimeState for AppState",
        "impl ModelFetchAssociationStore for AppState",
    ] {
        assert!(
            !runtime_state.contains(pattern),
            "model_fetch/runtime/state.rs should not host {pattern} anymore"
        );
    }

    let state_integrations =
        read_workspace_file("apps/aether-gateway/src/state/integrations.rs");
    for pattern in [
        "impl provider_transport::TransportTunnelAffinityLookup for AppState",
        "impl ModelFetchTransportRuntime for AppState",
        "impl ModelFetchRuntimeState for AppState",
        "impl ModelFetchAssociationStore for AppState",
    ] {
        assert!(
            state_integrations.contains(pattern),
            "state/integrations.rs should host {pattern}"
        );
    }

    let app_state = read_workspace_file("apps/aether-gateway/src/state/app.rs");
    assert!(
        !app_state.contains("impl provider_transport::TransportTunnelAffinityLookup for AppState"),
        "state/app.rs should not host provider transport integration impls anymore"
    );

    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root should resolve");
    let association_sync_path =
        workspace_root.join("apps/aether-gateway/src/model_fetch/runtime/association_sync.rs");
    assert!(
        !association_sync_path.exists(),
        "model_fetch/runtime/association_sync.rs should be removed after extraction"
    );
}

#[test]
fn video_task_helpers_depend_on_shared_core_crate() {
    let types = read_workspace_file("apps/aether-gateway/src/video_tasks/types.rs");
    assert!(
        types.contains("aether_video_tasks_core"),
        "video_tasks/types.rs should depend on aether-video-tasks-core"
    );
    for pattern in [
        "pub(crate) struct LocalVideoTaskTransport",
        "pub(crate) struct LocalVideoTaskPersistence",
        "pub(crate) struct OpenAiVideoTaskSeed",
        "pub(crate) struct GeminiVideoTaskSeed",
        "pub(crate) enum LocalVideoTaskSnapshot",
    ] {
        assert!(
            !types.contains(pattern),
            "video_tasks/types.rs should not own {pattern}"
        );
    }

    let body = read_workspace_file("apps/aether-gateway/src/video_tasks/helpers/body.rs");
    assert!(
        body.contains("aether_video_tasks_core"),
        "video_tasks/helpers/body.rs should depend on aether-video-tasks-core"
    );
    for pattern in [
        "fn context_text(",
        "fn context_u64(",
        "fn request_body_text(",
        "fn request_body_string(",
        "fn request_body_u32(",
    ] {
        assert!(
            !body.contains(pattern),
            "video_tasks/helpers/body.rs should not own {pattern}"
        );
    }

    let path = read_workspace_file("apps/aether-gateway/src/video_tasks/helpers/path.rs");
    assert!(
        path.contains("aether_video_tasks_core"),
        "video_tasks/helpers/path.rs should depend on aether-video-tasks-core"
    );
    for pattern in [
        "fn extract_openai_task_id_from_path(",
        "fn extract_gemini_short_id_from_path(",
        "fn extract_openai_task_id_from_cancel_path(",
        "fn extract_openai_task_id_from_remix_path(",
        "fn extract_openai_task_id_from_content_path(",
        "fn extract_gemini_short_id_from_cancel_path(",
        "fn resolve_video_task_read_lookup_key(",
        "fn resolve_video_task_hydration_lookup_key(",
        "fn current_unix_timestamp_secs(",
        "fn generate_local_short_id(",
    ] {
        assert!(
            !path.contains(pattern),
            "video_tasks/helpers/path.rs should not own {pattern}"
        );
    }

    let util = read_workspace_file("apps/aether-gateway/src/video_tasks/helpers/util.rs");
    assert!(
        util.contains("aether_video_tasks_core"),
        "video_tasks/helpers/util.rs should depend on aether-video-tasks-core"
    );
    assert!(
        !util.contains("fn non_empty_owned("),
        "video_tasks/helpers/util.rs should not own non_empty_owned"
    );

    let helpers = read_workspace_file("apps/aether-gateway/src/video_tasks/helpers.rs");
    assert!(
        !helpers.contains("mod transport;"),
        "video_tasks/helpers.rs should not keep a local transport bridge module"
    );
    assert!(
        !helpers.contains("transport_from_provider_transport"),
        "video_tasks/helpers.rs should not re-export a local transport bridge"
    );
}

#[test]
fn video_task_store_depends_on_shared_core_crate() {
    let store = read_workspace_file("apps/aether-gateway/src/video_tasks/store.rs");
    assert!(
        store.contains("aether_video_tasks_core"),
        "video_tasks/store.rs should depend on aether-video-tasks-core"
    );
    for pattern in [
        "trait VideoTaskStore",
        "struct InMemoryVideoTaskStore",
        "struct FileVideoTaskStore",
        "mod backend;",
        "mod registry;",
    ] {
        assert!(
            !store.contains(pattern),
            "video_tasks/store.rs should not own {pattern}"
        );
    }
}

#[test]
fn video_task_service_depends_on_shared_core_crate() {
    let service = read_workspace_file("apps/aether-gateway/src/video_tasks/service.rs");
    assert!(
        service.contains("aether_video_tasks_core::VideoTaskService"),
        "video_tasks/service.rs should wrap shared VideoTaskService"
    );
    for pattern in [
        "truth_source_mode:",
        "store:",
        "mod follow_up;",
        "mod lifecycle;",
        "mod read;",
        "mod refresh;",
    ] {
        assert!(
            !service.contains(pattern),
            "video_tasks/service.rs should not own {pattern}"
        );
    }
}

#[test]
fn video_task_state_is_split_between_data_and_runtime_crates() {
    let store = read_workspace_file("apps/aether-gateway/src/video_tasks/store.rs");
    assert!(
        store.contains("aether_video_tasks_core"),
        "video_tasks/store.rs should keep runtime store ownership in aether-video-tasks-core"
    );
    assert!(
        !store.contains("aether_data::repository::video_tasks"),
        "video_tasks/store.rs should not own persistent video task repository types"
    );

    let state_video = read_workspace_file("apps/aether-gateway/src/state/video.rs");
    assert!(
        state_video.contains("aether_data::repository::video_tasks::"),
        "state/video.rs should use aether-data video task repository types for persistence"
    );
    assert!(
        state_video.contains("reconstruct_local_video_task_snapshot"),
        "state/video.rs should reuse shared runtime snapshot reconstruction"
    );
    for pattern in [
        "InMemoryVideoTaskStore",
        "FileVideoTaskStore",
        "trait VideoTaskStore",
    ] {
        assert!(
            !state_video.contains(pattern),
            "state/video.rs should not own runtime store implementation {pattern}"
        );
    }
}

#[test]
fn data_backed_video_task_rebuild_uses_shared_provider_transport() {
    let state_video = read_workspace_file("apps/aether-gateway/src/state/video.rs");
    assert!(
        state_video.contains("reconstruct_local_video_task_snapshot"),
        "state/video.rs should rebuild snapshots through shared provider transport helper"
    );
    assert!(
        state_video.contains("resolve_video_task_hydration_lookup_key"),
        "state/video.rs should resolve hydrate lookup through shared video task helper"
    );
    assert!(
        !state_video.contains("impl crate::provider_transport::VideoTaskTransportSnapshotLookup for AppState"),
        "state/video.rs should not host video task transport lookup integration impl anymore"
    );
    assert!(
        !state_video.contains("resolve_local_video_task_transport"),
        "state/video.rs should not manually rebuild local video transport"
    );
    for pattern in [
        "extract_openai_task_id_from_path(",
        "extract_openai_task_id_from_cancel_path(",
        "extract_openai_task_id_from_remix_path(",
        "extract_openai_task_id_from_content_path(",
        "extract_gemini_short_id_from_path(",
        "extract_gemini_short_id_from_cancel_path(",
    ] {
        assert!(
            !state_video.contains(pattern),
            "state/video.rs should not inline path extractor {pattern}"
        );
    }
    assert!(
        !state_video.contains("self.data\n            .read_provider_transport_snapshot"),
        "state/video.rs should not inline provider transport snapshot reads in the rebuild path"
    );

    let video_mod = read_workspace_file("apps/aether-gateway/src/video_tasks/mod.rs");
    assert!(
        !video_mod.contains("transport_from_provider_transport"),
        "video_tasks/mod.rs should not export a local provider transport bridge"
    );

    let state_integrations =
        read_workspace_file("apps/aether-gateway/src/state/integrations.rs");
    assert!(
        state_integrations
            .contains("impl provider_transport::VideoTaskTransportSnapshotLookup for AppState"),
        "state/integrations.rs should host VideoTaskTransportSnapshotLookup for AppState"
    );

    let data_video = read_workspace_file("apps/aether-gateway/src/data/state/runtime.rs");
    assert!(
        data_video.contains("aether_video_tasks_core"),
        "data/state/runtime.rs should depend on aether-video-tasks-core"
    );
    assert!(
        data_video.contains("read_data_backed_video_task_response"),
        "data/state/runtime.rs should delegate data-backed read orchestration to shared video task helper"
    );
    for pattern in [
        "read_openai_video_task_response(",
        "read_gemini_video_task_response(",
        "resolve_video_task_read_lookup_key",
        "map_openai_stored_task_to_read_response",
        "map_gemini_stored_task_to_read_response",
    ] {
        assert!(
            !data_video.contains(pattern),
            "data/state/runtime.rs should not own data-backed video read orchestration {pattern}"
        );
    }

    let core_read_side = read_workspace_file("crates/aether-video-tasks-core/src/read_side.rs");
    for pattern in [
        "pub trait StoredVideoTaskReadSide",
        "pub async fn read_data_backed_video_task_response(",
        "resolve_video_task_read_lookup_key",
        "map_openai_stored_task_to_read_response",
        "map_gemini_stored_task_to_read_response",
    ] {
        assert!(
            core_read_side.contains(pattern),
            "aether-video-tasks-core read_side.rs should own {pattern}"
        );
    }

    let gateway_data_mod = read_workspace_file("apps/aether-gateway/src/data/mod.rs");
    for pattern in ["mod openai;", "mod gemini;", "mod video_tasks;"] {
        assert!(
            !gateway_data_mod.contains(pattern),
            "data/mod.rs should not keep local video task projection wrapper {pattern}"
        );
    }
}

#[test]
fn provider_transport_cache_helpers_live_in_shared_crate() {
    let state_cache = read_workspace_file("apps/aether-gateway/src/state/cache.rs");
    assert!(
        state_cache.contains("pub(crate) struct CachedProviderTransportSnapshot"),
        "state/cache.rs should keep the app-local cached snapshot wrapper"
    );
    for pattern in [
        "struct ProviderTransportSnapshotCacheKey",
        "fn provider_transport_snapshot_looks_refreshed(",
    ] {
        assert!(
            !state_cache.contains(pattern),
            "state/cache.rs should not own provider transport cache helper {pattern}"
        );
    }

    let state_mod = read_workspace_file("apps/aether-gateway/src/state/mod.rs");
    assert!(
        state_mod.contains("super::provider_transport::ProviderTransportSnapshotCacheKey"),
        "state/mod.rs should re-export ProviderTransportSnapshotCacheKey from shared provider transport"
    );
    assert!(
        state_mod
            .contains("super::provider_transport::provider_transport_snapshot_looks_refreshed"),
        "state/mod.rs should import refresh detection from shared provider transport"
    );

    let transport_cache = read_workspace_file("crates/aether-provider-transport/src/cache.rs");
    for pattern in [
        "pub struct ProviderTransportSnapshotCacheKey",
        "pub fn provider_transport_snapshot_looks_refreshed(",
    ] {
        assert!(
            transport_cache.contains(pattern),
            "aether-provider-transport cache helper should own {pattern}"
        );
    }
}

#[test]
fn gateway_provider_transport_transition_copies_are_removed() {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root should resolve");
    let transition_dir = workspace_root.join("apps/aether-gateway/src/provider_transport");
    if !transition_dir.exists() {
        return;
    }
    let mut rust_files = Vec::new();
    collect_rust_files(&transition_dir, &mut rust_files);
    assert!(
        rust_files.is_empty(),
        "apps/aether-gateway/src/provider_transport should not retain Rust transition copies after provider transport extraction"
    );
}

#[test]
fn gateway_billing_and_settlement_runtime_transition_copies_are_removed() {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root should resolve");

    for relative in [
        "apps/aether-gateway/src/billing_runtime",
        "apps/aether-gateway/src/settlement_runtime",
    ] {
        let transition_dir = workspace_root.join(relative);
        if !transition_dir.exists() {
            continue;
        }
        let mut rust_files = Vec::new();
        collect_rust_files(&transition_dir, &mut rust_files);
        assert!(
            rust_files.is_empty(),
            "{relative} should not retain Rust transition copies after usage extraction"
        );
    }
}

#[test]
fn usage_reporting_does_not_log_raw_report_context() {
    let source = read_workspace_file("apps/aether-gateway/src/usage/reporting/mod.rs");
    assert!(
        !source.contains("report_context = ?payload.report_context"),
        "usage/reporting/mod.rs should not log raw report_context"
    );
}

#[test]
fn proxy_registration_client_does_not_log_raw_management_response_body() {
    let source = read_workspace_file("apps/aether-proxy/src/registration/client.rs");
    assert!(
        !source.contains("error!(body = %text"),
        "registration/client.rs should not log raw management response bodies"
    );
    assert!(
        !source.contains("register failed (HTTP {}): {}"),
        "registration/client.rs should not bubble raw register response bodies into logs"
    );
    assert!(
        !source.contains("unregister failed: {}"),
        "registration/client.rs should not bubble raw unregister response bodies into logs"
    );
}

#[test]
fn hotspot_modules_do_not_log_sensitive_payload_like_fields() {
    let patterns = [
        "report_context = ?",
        "payload = ?",
        "headers = ?",
        "original_request_body = ?",
        "provider_request_body = ?",
        "request_body = ?",
        "response_body = ?",
    ];

    for root in [
        "src/ai_pipeline",
        "src/execution_runtime",
        "src/usage",
        "src/async_task",
    ] {
        assert_no_sensitive_log_patterns(root, &patterns);
    }
}

#[test]
fn execution_runtime_video_finalize_paths_depend_on_shared_video_task_core() {
    let response =
        read_workspace_file("apps/aether-gateway/src/execution_runtime/sync/execution/response.rs");
    for pattern in [
        "build_local_sync_finalize_read_response",
        "resolve_local_sync_error_background_report_kind",
        "resolve_local_sync_success_background_report_kind",
    ] {
        assert!(
            response.contains(pattern),
            "execution/runtime response path should depend on shared video helper {pattern}"
        );
    }
    for pattern in [
        "fn resolve_local_sync_success_background_report_kind(",
        "fn resolve_local_sync_error_background_report_kind(",
        "\"openai_video_delete_sync_success\"",
        "\"openai_video_cancel_sync_success\"",
        "\"gemini_video_cancel_sync_success\"",
        "\"openai_video_create_sync_error\"",
        "\"openai_video_remix_sync_error\"",
        "\"gemini_video_create_sync_error\"",
    ] {
        assert!(
            !response.contains(pattern),
            "execution/runtime response path should not own video finalize mapping {pattern}"
        );
    }

    let internal_gateway =
        read_workspace_file("apps/aether-gateway/src/handlers/internal/gateway_helpers.rs");
    assert!(
        internal_gateway.contains("build_local_sync_finalize_request_path"),
        "internal gateway finalize path should depend on shared video finalize request-path helper"
    );
    for pattern in [
        "build_internal_finalize_video_plan",
        "infer_internal_finalize_signature",
        "resolve_internal_finalize_route",
    ] {
        assert!(
            internal_gateway.contains(pattern),
            "internal gateway finalize path should depend on shared helper {pattern}"
        );
    }
    assert!(
        !internal_gateway.contains("fn build_internal_finalize_video_request_path("),
        "internal gateway finalize path should not own local finalize request-path builder"
    );
    assert!(
        !internal_gateway.contains("fn build_internal_finalize_video_plan("),
        "internal gateway finalize path should not own local finalize video plan builder"
    );
    assert!(
        !internal_gateway.contains("fn infer_internal_finalize_signature("),
        "internal gateway finalize path should not own local finalize signature inference"
    );
}
