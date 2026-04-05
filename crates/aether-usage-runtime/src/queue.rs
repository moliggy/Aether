use serde_json::json;

use aether_data::redis::{
    RedisConsumerGroup, RedisConsumerName, RedisStreamEntry, RedisStreamName,
    RedisStreamReclaimConfig, RedisStreamRunner, RedisStreamRunnerConfig,
};
use aether_data::DataLayerError;

use super::config::UsageRuntimeConfig;
use super::event::UsageEvent;

#[derive(Debug, Clone)]
pub struct UsageQueue {
    runner: RedisStreamRunner,
    config: UsageRuntimeConfig,
    stream: RedisStreamName,
    group: RedisConsumerGroup,
    dlq_stream: RedisStreamName,
}

impl UsageQueue {
    pub fn new(
        runner: RedisStreamRunner,
        config: UsageRuntimeConfig,
    ) -> Result<Self, DataLayerError> {
        config.validate()?;
        let tuned_runner = RedisStreamRunner::new(
            runner.client().clone(),
            runner.keyspace().clone(),
            usage_stream_runner_config(&config),
        )?;
        Ok(Self {
            runner: tuned_runner,
            stream: RedisStreamName(config.stream_key.clone()),
            group: RedisConsumerGroup(config.consumer_group.clone()),
            dlq_stream: RedisStreamName(config.dlq_stream_key.clone()),
            config,
        })
    }

    pub async fn ensure_consumer_group(&self) -> Result<(), DataLayerError> {
        self.runner
            .ensure_consumer_group(&self.stream, &self.group, "0-0")
            .await
    }

    pub async fn enqueue(&self, event: &UsageEvent) -> Result<String, DataLayerError> {
        let fields = event.to_stream_fields()?;
        self.runner
            .append_fields_with_maxlen(&self.stream, &fields, Some(self.config.stream_maxlen))
            .await
    }

    pub async fn read_group(
        &self,
        consumer: &RedisConsumerName,
    ) -> Result<Vec<RedisStreamEntry>, DataLayerError> {
        self.runner
            .read_group(&self.stream, &self.group, consumer)
            .await
    }

    pub async fn claim_stale(
        &self,
        consumer: &RedisConsumerName,
        start_id: &str,
    ) -> Result<Vec<RedisStreamEntry>, DataLayerError> {
        Ok(self
            .runner
            .claim_stale(
                &self.stream,
                &self.group,
                consumer,
                start_id,
                RedisStreamReclaimConfig {
                    min_idle_ms: self.config.reclaim_idle_ms,
                    count: self.config.reclaim_count,
                },
            )
            .await?
            .entries)
    }

    pub async fn ack_and_delete(&self, ids: &[String]) -> Result<(), DataLayerError> {
        self.runner.ack(&self.stream, &self.group, ids).await?;
        self.runner.delete(&self.stream, ids).await?;
        Ok(())
    }

    pub async fn push_dead_letter(
        &self,
        entry: &RedisStreamEntry,
        error: &str,
    ) -> Result<String, DataLayerError> {
        self.runner
            .append_json(
                &self.dlq_stream,
                "payload",
                &json!({
                    "entry_id": entry.id,
                    "fields": entry.fields,
                    "error": error,
                }),
            )
            .await
    }
}

fn usage_stream_runner_config(config: &UsageRuntimeConfig) -> RedisStreamRunnerConfig {
    let read_block_ms = config.consumer_block_ms.max(1);
    let command_timeout_ms = read_block_ms.saturating_add(2_000).max(5_000);
    RedisStreamRunnerConfig {
        command_timeout_ms: Some(command_timeout_ms),
        read_block_ms: Some(read_block_ms),
        read_count: config.consumer_batch_size.max(1),
    }
}

#[cfg(test)]
mod tests {
    use super::{usage_stream_runner_config, UsageQueue};
    use crate::UsageRuntimeConfig;
    use aether_data::redis::{RedisClientConfig, RedisClientFactory, RedisStreamRunner};

    fn sample_runner() -> RedisStreamRunner {
        let config = RedisClientConfig {
            url: "redis://127.0.0.1/0".to_string(),
            key_prefix: Some("aether".to_string()),
        };
        let client = RedisClientFactory::new(config.clone())
            .expect("factory should build")
            .connect_lazy()
            .expect("client should build");

        RedisStreamRunner::new(
            client,
            config.keyspace(),
            aether_data::redis::RedisStreamRunnerConfig::default(),
        )
        .expect("runner should build")
    }

    #[test]
    fn usage_queue_applies_runtime_block_and_batch_settings() {
        let config = UsageRuntimeConfig {
            enabled: true,
            consumer_block_ms: 750,
            consumer_batch_size: 123,
            ..UsageRuntimeConfig::default()
        };
        let queue = UsageQueue::new(sample_runner(), config)
            .expect("usage queue should build from runtime config");

        assert_eq!(
            queue.runner.config(),
            usage_stream_runner_config(&queue.config)
        );
        assert_eq!(queue.runner.config().read_block_ms, Some(750));
        assert_eq!(queue.runner.config().read_count, 123);
        assert_eq!(queue.runner.config().command_timeout_ms, Some(5_000));
    }
}
