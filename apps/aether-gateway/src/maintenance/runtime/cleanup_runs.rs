use std::time::Instant;

use aether_data::repository::system::AdminSystemPurgeSummary;
use aether_data_contracts::DataLayerError;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{info, warn};

use crate::data::GatewayDataState;
use crate::{AppState, GatewayError};

use super::{now_unix_secs, system_config_usize};

const CLEANUP_RUN_HISTORY_KEY: &str = "admin_cleanup_run_history";
const CLEANUP_RUN_HISTORY_LIMIT: usize = 50;
const REQUEST_BODY_PROGRESS_UPDATE_BATCHES: usize = 10;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct AdminCleanupRunRecord {
    pub(crate) id: String,
    pub(crate) kind: String,
    pub(crate) trigger: String,
    pub(crate) status: String,
    pub(crate) message: String,
    pub(crate) started_at_unix_secs: u64,
    pub(crate) completed_at_unix_secs: Option<u64>,
    pub(crate) duration_ms: Option<u64>,
    pub(crate) summary: Value,
    pub(crate) error: Option<String>,
}

pub(crate) async fn list_admin_cleanup_run_records(
    data: &GatewayDataState,
) -> Result<Vec<AdminCleanupRunRecord>, DataLayerError> {
    let Some(value) = data
        .find_system_config_value(CLEANUP_RUN_HISTORY_KEY)
        .await?
    else {
        return Ok(Vec::new());
    };
    Ok(parse_cleanup_run_records(value))
}

pub(crate) async fn start_admin_request_body_cleanup_task(
    app: AppState,
) -> Result<AdminCleanupRunRecord, GatewayError> {
    let data = app.data.clone();
    let records = list_admin_cleanup_run_records(&data)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;
    if let Some(existing) = records
        .iter()
        .find(|record| record.kind == "request_bodies" && record.status == "processing")
        .cloned()
    {
        return Ok(existing);
    }

    let batch_size = system_config_usize(&data, "cleanup_batch_size", 1_000)
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?
        .max(1);
    let started_at = now_unix_secs();
    let record = AdminCleanupRunRecord {
        id: uuid::Uuid::new_v4().to_string(),
        kind: "request_bodies".to_string(),
        trigger: "manual".to_string(),
        status: "processing".to_string(),
        message: format!("请求/响应体后台清理已开始，每批 {batch_size} 条"),
        started_at_unix_secs: started_at,
        completed_at_unix_secs: None,
        duration_ms: None,
        summary: json!({
            "batch_size": batch_size,
            "batches": 0,
            "cleaned": {},
        }),
        error: None,
    };
    record_cleanup_run(&data, record.clone())
        .await
        .map_err(|err| GatewayError::Internal(err.to_string()))?;

    tokio::spawn(run_request_body_cleanup_task(
        data,
        record.clone(),
        batch_size,
    ));
    Ok(record)
}

pub(crate) async fn record_completed_cleanup_run(
    data: &GatewayDataState,
    kind: &str,
    trigger: &str,
    started_at_unix_secs: u64,
    started_at: Instant,
    summary: Value,
    message: impl Into<String>,
) {
    let record = AdminCleanupRunRecord {
        id: uuid::Uuid::new_v4().to_string(),
        kind: kind.to_string(),
        trigger: trigger.to_string(),
        status: "completed".to_string(),
        message: message.into(),
        started_at_unix_secs,
        completed_at_unix_secs: Some(now_unix_secs()),
        duration_ms: Some(
            started_at
                .elapsed()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
        ),
        summary,
        error: None,
    };
    if let Err(err) = record_cleanup_run(data, record).await {
        warn!(error = %err, kind, "failed to record cleanup run");
    }
}

pub(crate) async fn record_failed_cleanup_run(
    data: &GatewayDataState,
    kind: &str,
    trigger: &str,
    started_at_unix_secs: u64,
    started_at: Instant,
    error: &DataLayerError,
) {
    let record = AdminCleanupRunRecord {
        id: uuid::Uuid::new_v4().to_string(),
        kind: kind.to_string(),
        trigger: trigger.to_string(),
        status: "failed".to_string(),
        message: "清理执行失败".to_string(),
        started_at_unix_secs,
        completed_at_unix_secs: Some(now_unix_secs()),
        duration_ms: Some(
            started_at
                .elapsed()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
        ),
        summary: json!({}),
        error: Some(error.to_string()),
    };
    if let Err(err) = record_cleanup_run(data, record).await {
        warn!(error = %err, kind, "failed to record failed cleanup run");
    }
}

async fn run_request_body_cleanup_task(
    data: std::sync::Arc<GatewayDataState>,
    initial_record: AdminCleanupRunRecord,
    batch_size: usize,
) {
    let started_at = Instant::now();
    let mut total = AdminSystemPurgeSummary::default();
    let mut batches = 0usize;

    loop {
        match data.purge_admin_request_bodies_batch(batch_size).await {
            Ok(batch) if batch.total() == 0 => break,
            Ok(batch) => {
                batches = batches.saturating_add(1);
                total.merge(&batch);
                if batches.is_multiple_of(REQUEST_BODY_PROGRESS_UPDATE_BATCHES) {
                    let progress = request_body_cleanup_record(
                        &initial_record,
                        "processing",
                        format!(
                            "请求/响应体后台清理中，已处理 {} 批，影响 {} 行",
                            batches,
                            total.total()
                        ),
                        batches,
                        batch_size,
                        &total,
                        Some(started_at),
                        None,
                    );
                    if let Err(err) = record_cleanup_run(&data, progress).await {
                        warn!(error = %err, "failed to update request body cleanup progress");
                    }
                }
                tokio::task::yield_now().await;
            }
            Err(err) => {
                let failed = request_body_cleanup_record(
                    &initial_record,
                    "failed",
                    "请求/响应体后台清理失败".to_string(),
                    batches,
                    batch_size,
                    &total,
                    Some(started_at),
                    Some(err.to_string()),
                );
                if let Err(record_err) = record_cleanup_run(&data, failed).await {
                    warn!(error = %record_err, "failed to record request body cleanup failure");
                }
                warn!(error = %err, "request body cleanup task failed");
                return;
            }
        }
    }

    let completed = request_body_cleanup_record(
        &initial_record,
        "completed",
        format!(
            "请求/响应体后台清理完成，影响 {} 行，共 {} 批",
            total.total(),
            batches
        ),
        batches,
        batch_size,
        &total,
        Some(started_at),
        None,
    );
    if let Err(err) = record_cleanup_run(&data, completed).await {
        warn!(error = %err, "failed to record request body cleanup completion");
    }
    info!(
        event_name = "request_body_cleanup_task_completed",
        log_type = "ops",
        worker = "request_body_cleanup_task",
        batches,
        affected = total.total(),
        "gateway finished request body cleanup task"
    );
}

fn request_body_cleanup_record(
    initial: &AdminCleanupRunRecord,
    status: &str,
    message: String,
    batches: usize,
    batch_size: usize,
    total: &AdminSystemPurgeSummary,
    started_at: Option<Instant>,
    error: Option<String>,
) -> AdminCleanupRunRecord {
    let completed = matches!(status, "completed" | "failed");
    AdminCleanupRunRecord {
        id: initial.id.clone(),
        kind: initial.kind.clone(),
        trigger: initial.trigger.clone(),
        status: status.to_string(),
        message,
        started_at_unix_secs: initial.started_at_unix_secs,
        completed_at_unix_secs: completed.then(now_unix_secs),
        duration_ms: started_at
            .map(|value| value.elapsed().as_millis().try_into().unwrap_or(u64::MAX)),
        summary: json!({
            "batch_size": batch_size,
            "batches": batches,
            "cleaned": total.affected,
            "total": total.total(),
        }),
        error,
    }
}

async fn record_cleanup_run(
    data: &GatewayDataState,
    record: AdminCleanupRunRecord,
) -> Result<(), DataLayerError> {
    let mut records = list_admin_cleanup_run_records(data).await?;
    records.retain(|existing| existing.id != record.id);
    records.insert(0, record);
    records.truncate(CLEANUP_RUN_HISTORY_LIMIT);
    let value = serde_json::to_value(records).map_err(|err| {
        DataLayerError::UnexpectedValue(format!("invalid cleanup run history: {err}"))
    })?;
    data.upsert_system_config_entry(
        CLEANUP_RUN_HISTORY_KEY,
        &value,
        Some("最近的系统清理执行记录"),
    )
    .await?;
    Ok(())
}

fn parse_cleanup_run_records(value: Value) -> Vec<AdminCleanupRunRecord> {
    value
        .as_array()
        .into_iter()
        .flat_map(|items| items.iter())
        .filter_map(|item| serde_json::from_value::<AdminCleanupRunRecord>(item.clone()).ok())
        .collect()
}
