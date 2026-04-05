use std::sync::Arc;
use std::time::Duration;

use tracing::warn;

use crate::data::GatewayDataState;

const QUOTA_RESET_INTERVAL: Duration = Duration::from_secs(60 * 60);

pub(crate) async fn reset_due_provider_quotas_once(
    data: &GatewayDataState,
) -> Result<usize, aether_data::DataLayerError> {
    let now_unix_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    data.reset_due_provider_quotas(now_unix_secs).await
}

pub(crate) fn spawn_provider_quota_reset_worker(
    data: Arc<GatewayDataState>,
) -> Option<tokio::task::JoinHandle<()>> {
    if !data.has_provider_quota_writer() {
        return None;
    }

    Some(tokio::spawn(async move {
        if let Err(err) = reset_due_provider_quotas_once(&data).await {
            warn!(error = %err, "gateway provider quota reset startup failed");
        }
        let mut interval = tokio::time::interval(QUOTA_RESET_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(err) = reset_due_provider_quotas_once(&data).await {
                warn!(error = %err, "gateway provider quota reset tick failed");
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use aether_data::repository::quota::{
        InMemoryProviderQuotaRepository, ProviderQuotaReadRepository, StoredProviderQuotaSnapshot,
    };

    use super::{reset_due_provider_quotas_once, spawn_provider_quota_reset_worker};
    use crate::data::GatewayDataState;

    #[tokio::test]
    async fn resets_due_provider_quotas_from_runtime() {
        let repository = Arc::new(InMemoryProviderQuotaRepository::seed(vec![
            StoredProviderQuotaSnapshot::new(
                "provider-1".to_string(),
                "monthly_quota".to_string(),
                Some(20.0),
                4.0,
                Some(1),
                Some(1),
                None,
                true,
            )
            .expect("quota should build"),
        ]));
        let data = GatewayDataState::with_provider_quota_repository_for_tests(repository.clone());

        let reset = reset_due_provider_quotas_once(&data)
            .await
            .expect("quota reset should succeed");
        assert_eq!(reset, 1);

        let stored = repository
            .find_by_provider_id("provider-1")
            .await
            .expect("quota lookup should succeed")
            .expect("quota should exist");
        assert_eq!(stored.monthly_used_usd, 0.0);
    }

    #[tokio::test]
    async fn spawned_worker_resets_due_provider_quotas_immediately() {
        let repository = Arc::new(InMemoryProviderQuotaRepository::seed(vec![
            StoredProviderQuotaSnapshot::new(
                "provider-1".to_string(),
                "monthly_quota".to_string(),
                Some(20.0),
                4.0,
                Some(1),
                Some(1),
                None,
                true,
            )
            .expect("quota should build"),
        ]));
        let data = Arc::new(GatewayDataState::with_provider_quota_repository_for_tests(
            repository.clone(),
        ));
        let handle = spawn_provider_quota_reset_worker(data).expect("worker should spawn");

        let stored = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let stored = repository
                    .find_by_provider_id("provider-1")
                    .await
                    .expect("quota lookup should succeed")
                    .expect("quota should exist");
                if stored.monthly_used_usd == 0.0 {
                    break stored;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("worker should reset quota on startup");

        handle.abort();

        assert_eq!(stored.monthly_used_usd, 0.0);
        assert!(stored.quota_last_reset_at_unix_secs.unwrap_or_default() > 1);
    }
}
