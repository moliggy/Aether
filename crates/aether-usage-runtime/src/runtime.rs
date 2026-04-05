use std::sync::Arc;

use aether_contracts::{ExecutionPlan, ExecutionTelemetry};
use aether_data::redis::RedisStreamRunner;
use aether_data::DataLayerError;
use async_trait::async_trait;
use tracing::warn;

use crate::{
    build_pending_usage_record, build_stream_terminal_usage_outcome, build_streaming_usage_record,
    build_sync_terminal_usage_outcome, build_terminal_usage_event_from_outcome,
    build_upsert_usage_record_from_event, build_usage_queue_worker, settle_usage_if_needed,
    GatewayStreamReportRequest, GatewaySyncReportRequest, UsageEvent, UsageQueue,
    UsageRecordWriter, UsageRuntimeConfig, UsageSettlementWriter, UsageTerminalState,
};

#[async_trait]
pub trait UsageBillingEventEnricher: Send + Sync {
    async fn enrich_usage_event(&self, event: &mut UsageEvent) -> Result<(), DataLayerError>;
}

pub trait UsageRuntimeAccess:
    UsageRecordWriter + UsageSettlementWriter + UsageBillingEventEnricher + Send + Sync
{
    fn has_usage_writer(&self) -> bool;
    fn has_usage_worker_runner(&self) -> bool;
    fn usage_worker_runner(&self) -> Option<RedisStreamRunner>;
}

#[derive(Debug, Clone)]
pub struct UsageRuntime {
    config: UsageRuntimeConfig,
}

impl Default for UsageRuntime {
    fn default() -> Self {
        Self::disabled()
    }
}

impl UsageRuntime {
    pub fn disabled() -> Self {
        Self {
            config: UsageRuntimeConfig::disabled(),
        }
    }

    pub fn new(config: UsageRuntimeConfig) -> Result<Self, DataLayerError> {
        config.validate()?;
        Ok(Self { config })
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    pub fn can_spawn_worker<T>(&self, data: &T) -> bool
    where
        T: UsageRuntimeAccess,
    {
        self.is_enabled() && data.has_usage_writer() && data.has_usage_worker_runner()
    }

    pub fn spawn_worker<T>(&self, data: Arc<T>) -> Option<tokio::task::JoinHandle<()>>
    where
        T: UsageRuntimeAccess + 'static,
    {
        if !self.can_spawn_worker(data.as_ref()) {
            return None;
        }
        let runner = data.usage_worker_runner()?;
        let worker = build_usage_queue_worker(runner, data, self.config.clone()).ok()?;
        Some(worker.spawn())
    }

    pub async fn record_pending<T>(
        &self,
        data: &T,
        plan: &ExecutionPlan,
        report_context: Option<&serde_json::Value>,
    ) where
        T: UsageRuntimeAccess,
    {
        if !self.is_enabled() {
            return;
        }
        let now_unix_secs = now_unix_secs();
        match build_pending_usage_record(plan, report_context, now_unix_secs) {
            Ok(record) => {
                if let Err(err) = data.upsert_usage_record(record).await {
                    warn!(
                        event_name = "usage_pending_record_failed",
                        log_type = "event",
                        request_id = %plan.request_id,
                        error = %err,
                        "usage runtime failed to record sync pending usage"
                    );
                }
            }
            Err(err) => {
                warn!(
                    event_name = "usage_pending_build_failed",
                    log_type = "event",
                    request_id = %plan.request_id,
                    error = %err,
                    "usage runtime failed to build sync pending usage"
                )
            }
        }
    }

    pub async fn record_stream_started<T>(
        &self,
        data: &T,
        plan: &ExecutionPlan,
        report_context: Option<&serde_json::Value>,
        status_code: u16,
        headers: &std::collections::BTreeMap<String, String>,
        telemetry: Option<&ExecutionTelemetry>,
    ) where
        T: UsageRuntimeAccess,
    {
        if !self.is_enabled() {
            return;
        }
        let now_unix_secs = now_unix_secs();
        match build_streaming_usage_record(
            plan,
            report_context,
            status_code,
            headers,
            telemetry,
            now_unix_secs,
        ) {
            Ok(record) => {
                if let Err(err) = data.upsert_usage_record(record).await {
                    warn!(
                        event_name = "usage_stream_record_failed",
                        log_type = "event",
                        request_id = %plan.request_id,
                        error = %err,
                        "usage runtime failed to record stream usage"
                    );
                }
            }
            Err(err) => {
                warn!(
                    event_name = "usage_stream_build_failed",
                    log_type = "event",
                    request_id = %plan.request_id,
                    error = %err,
                    "usage runtime failed to build stream usage"
                )
            }
        }
    }

    pub async fn record_sync_terminal<T>(
        &self,
        data: &T,
        plan: &ExecutionPlan,
        report_context: Option<&serde_json::Value>,
        payload: &GatewaySyncReportRequest,
    ) where
        T: UsageRuntimeAccess,
    {
        if !self.is_enabled() {
            return;
        }
        match build_terminal_usage_event_from_outcome(build_sync_terminal_usage_outcome(
            plan,
            report_context,
            payload,
        )) {
            Ok(mut event) => {
                if let Err(err) = data.enrich_usage_event(&mut event).await {
                    warn!(
                        event_name = "usage_sync_terminal_billing_enrichment_failed",
                        log_type = "event",
                        request_id = %plan.request_id,
                        error = %err,
                        "usage runtime failed to enrich sync usage event with billing"
                    );
                }
                self.enqueue_or_write_terminal(data, event).await
            }
            Err(err) => {
                warn!(
                    event_name = "usage_sync_terminal_build_failed",
                    log_type = "event",
                    request_id = %plan.request_id,
                    error = %err,
                    "usage runtime failed to build sync terminal usage event"
                )
            }
        }
    }

    pub async fn record_stream_terminal<T>(
        &self,
        data: &T,
        plan: &ExecutionPlan,
        report_context: Option<&serde_json::Value>,
        payload: &GatewayStreamReportRequest,
        cancelled: bool,
    ) where
        T: UsageRuntimeAccess,
    {
        if !self.is_enabled() {
            return;
        }
        let mut outcome = build_stream_terminal_usage_outcome(plan, report_context, payload);
        if cancelled {
            outcome.terminal_state = UsageTerminalState::Cancelled;
        }
        match build_terminal_usage_event_from_outcome(outcome) {
            Ok(mut event) => {
                if let Err(err) = data.enrich_usage_event(&mut event).await {
                    warn!(
                        event_name = "usage_stream_terminal_billing_enrichment_failed",
                        log_type = "event",
                        request_id = %plan.request_id,
                        error = %err,
                        "usage runtime failed to enrich stream usage event with billing"
                    );
                }
                self.enqueue_or_write_terminal(data, event).await
            }
            Err(err) => {
                warn!(
                    event_name = "usage_stream_terminal_build_failed",
                    log_type = "event",
                    request_id = %plan.request_id,
                    error = %err,
                    "usage runtime failed to build stream terminal usage event"
                )
            }
        }
    }

    async fn enqueue_or_write_terminal<T>(&self, data: &T, event: UsageEvent)
    where
        T: UsageRuntimeAccess,
    {
        if let Some(runner) = data.usage_worker_runner() {
            match UsageQueue::new(runner, self.config.clone()) {
                Ok(queue) => match queue.enqueue(&event).await {
                    Ok(_) => return,
                    Err(err) => {
                        warn!(
                            event_name = "usage_terminal_enqueue_failed",
                            log_type = "event",
                            request_id = %event.request_id,
                            fallback = "direct_write",
                            error = %err,
                            "usage runtime failed to enqueue terminal usage event; falling back to direct write"
                        )
                    }
                },
                Err(err) => {
                    warn!(
                        event_name = "usage_terminal_queue_init_failed",
                        log_type = "event",
                        request_id = %event.request_id,
                        fallback = "direct_write",
                        error = %err,
                        "usage runtime failed to build queue; falling back to direct write"
                    )
                }
            }
        }

        match build_upsert_usage_record_from_event(&event) {
            Ok(record) => match data.upsert_usage_record(record).await {
                Ok(Some(stored)) => {
                    if let Err(err) = settle_usage_if_needed(data, &stored).await {
                        warn!(
                            event_name = "usage_terminal_settlement_failed",
                            log_type = "event",
                            request_id = %event.request_id,
                            error = %err,
                            "usage runtime failed to settle terminal usage directly"
                        );
                    }
                }
                Ok(None) => {}
                Err(err) => {
                    warn!(
                        event_name = "usage_terminal_upsert_failed",
                        log_type = "event",
                        request_id = %event.request_id,
                        error = %err,
                        "usage runtime failed to upsert terminal usage directly"
                    );
                }
            },
            Err(err) => {
                warn!(
                    event_name = "usage_terminal_upsert_build_failed",
                    log_type = "event",
                    request_id = %event.request_id,
                    error = %err,
                    "usage runtime failed to build terminal usage upsert"
                )
            }
        }
    }
}

fn now_unix_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
