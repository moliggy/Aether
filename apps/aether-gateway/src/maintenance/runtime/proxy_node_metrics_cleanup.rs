use aether_data::repository::proxy_nodes::ProxyNodeMetricsCleanupSummary;
use aether_data_contracts::DataLayerError;

use crate::data::GatewayDataState;

use super::{now_unix_secs, system_config_bool, system_config_u64, system_config_usize};

const SECS_PER_DAY: u64 = 24 * 60 * 60;
const PROXY_NODE_METRICS_1M_RETENTION_DAYS_DEFAULT: u64 = 30;
const PROXY_NODE_METRICS_1H_RETENTION_DAYS_DEFAULT: u64 = 180;
const PROXY_NODE_METRICS_RETENTION_DAYS_MIN: u64 = 1;
const PROXY_NODE_METRICS_1M_RETENTION_DAYS_MAX: u64 = 365;
const PROXY_NODE_METRICS_1H_RETENTION_DAYS_MAX: u64 = 1_095;
const PROXY_NODE_METRICS_CLEANUP_BATCH_SIZE_DEFAULT: usize = 5_000;
const PROXY_NODE_METRICS_CLEANUP_BATCH_SIZE_MAX: usize = 50_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ProxyNodeMetricsCleanupSettings {
    pub retain_1m_days: u64,
    pub retain_1h_days: u64,
    pub batch_size: usize,
}

pub(super) async fn proxy_node_metrics_cleanup_settings(
    data: &GatewayDataState,
) -> Result<ProxyNodeMetricsCleanupSettings, DataLayerError> {
    let retain_1m_days = system_config_u64(
        data,
        "proxy_node_metrics_1m_retention_days",
        PROXY_NODE_METRICS_1M_RETENTION_DAYS_DEFAULT,
    )
    .await?
    .clamp(
        PROXY_NODE_METRICS_RETENTION_DAYS_MIN,
        PROXY_NODE_METRICS_1M_RETENTION_DAYS_MAX,
    );
    let retain_1h_days = system_config_u64(
        data,
        "proxy_node_metrics_1h_retention_days",
        PROXY_NODE_METRICS_1H_RETENTION_DAYS_DEFAULT,
    )
    .await?
    .clamp(retain_1m_days, PROXY_NODE_METRICS_1H_RETENTION_DAYS_MAX);
    let cleanup_batch_size =
        system_config_usize(data, "proxy_node_metrics_cleanup_batch_size", 0).await?;
    let fallback_batch_size = system_config_usize(
        data,
        "cleanup_batch_size",
        PROXY_NODE_METRICS_CLEANUP_BATCH_SIZE_DEFAULT,
    )
    .await?;
    let batch_size = (if cleanup_batch_size > 0 {
        cleanup_batch_size
    } else {
        fallback_batch_size
    })
    .clamp(1, PROXY_NODE_METRICS_CLEANUP_BATCH_SIZE_MAX);

    Ok(ProxyNodeMetricsCleanupSettings {
        retain_1m_days,
        retain_1h_days,
        batch_size,
    })
}

pub(super) async fn cleanup_proxy_node_metrics_once(
    data: &GatewayDataState,
) -> Result<ProxyNodeMetricsCleanupSummary, DataLayerError> {
    cleanup_proxy_node_metrics_at(data, now_unix_secs()).await
}

pub(super) async fn cleanup_proxy_node_metrics_at(
    data: &GatewayDataState,
    now_unix_secs: u64,
) -> Result<ProxyNodeMetricsCleanupSummary, DataLayerError> {
    if !system_config_bool(data, "enable_auto_cleanup", true).await? {
        return Ok(ProxyNodeMetricsCleanupSummary::default());
    }

    let settings = proxy_node_metrics_cleanup_settings(data).await?;
    let retain_1m_from_unix_secs =
        now_unix_secs.saturating_sub(settings.retain_1m_days.saturating_mul(SECS_PER_DAY));
    let retain_1h_from_unix_secs =
        now_unix_secs.saturating_sub(settings.retain_1h_days.saturating_mul(SECS_PER_DAY));
    let mut summary = ProxyNodeMetricsCleanupSummary::default();

    loop {
        let deleted = data
            .cleanup_proxy_node_metrics(
                retain_1m_from_unix_secs,
                retain_1h_from_unix_secs,
                settings.batch_size,
            )
            .await?;
        summary.deleted_1m_rows = summary
            .deleted_1m_rows
            .saturating_add(deleted.deleted_1m_rows);
        summary.deleted_1h_rows = summary
            .deleted_1h_rows
            .saturating_add(deleted.deleted_1h_rows);
        if deleted.deleted_1m_rows < settings.batch_size
            && deleted.deleted_1h_rows < settings.batch_size
        {
            break;
        }
    }

    Ok(summary)
}
