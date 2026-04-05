mod leases;
mod locks;
mod postgres;
mod read;
mod redis;
mod transactions;
mod workers;
mod write;

pub use leases::DataLeaseBackends;
pub use locks::DataLockBackends;
pub use postgres::PostgresBackend;
pub use read::DataReadRepositories;
pub use redis::RedisBackend;
pub use transactions::DataTransactionBackends;
pub use workers::DataWorkerBackends;
pub use write::DataWriteRepositories;

use crate::{DataLayerConfig, DataLayerError};

#[derive(Debug, Clone, Default)]
pub struct DataBackends {
    config: DataLayerConfig,
    postgres: Option<PostgresBackend>,
    redis: Option<RedisBackend>,
    leases: DataLeaseBackends,
    locks: DataLockBackends,
    read: DataReadRepositories,
    transactions: DataTransactionBackends,
    workers: DataWorkerBackends,
    write: DataWriteRepositories,
}

impl DataBackends {
    pub fn from_config(config: DataLayerConfig) -> Result<Self, DataLayerError> {
        config.validate()?;

        let postgres = config
            .postgres
            .clone()
            .map(PostgresBackend::from_config)
            .transpose()?;
        let redis = config
            .redis
            .clone()
            .map(RedisBackend::from_config)
            .transpose()?;
        let leases = DataLeaseBackends::from_postgres(postgres.as_ref())?;
        let locks = DataLockBackends::from_redis(redis.as_ref())?;
        let read = DataReadRepositories::from_postgres(postgres.as_ref());
        let transactions = DataTransactionBackends::from_postgres(postgres.as_ref());
        let workers = DataWorkerBackends::from_redis(redis.as_ref())?;
        let write = DataWriteRepositories::from_postgres(postgres.as_ref());

        Ok(Self {
            config,
            postgres,
            redis,
            leases,
            locks,
            read,
            transactions,
            workers,
            write,
        })
    }

    pub fn config(&self) -> &DataLayerConfig {
        &self.config
    }

    pub fn postgres(&self) -> Option<&PostgresBackend> {
        self.postgres.as_ref()
    }

    pub fn redis(&self) -> Option<&RedisBackend> {
        self.redis.as_ref()
    }

    pub fn read(&self) -> &DataReadRepositories {
        &self.read
    }

    pub fn leases(&self) -> &DataLeaseBackends {
        &self.leases
    }

    pub fn locks(&self) -> &DataLockBackends {
        &self.locks
    }

    pub fn transactions(&self) -> &DataTransactionBackends {
        &self.transactions
    }

    pub fn workers(&self) -> &DataWorkerBackends {
        &self.workers
    }

    pub fn write(&self) -> &DataWriteRepositories {
        &self.write
    }

    pub fn has_runtime_backends(&self) -> bool {
        self.postgres.is_some()
            || self.redis.is_some()
            || self.leases.has_any()
            || self.locks.has_any()
            || self.read.has_any()
            || self.transactions.has_any()
            || self.workers.has_any()
            || self.write.has_any()
    }
}

#[cfg(test)]
mod tests {
    use super::DataBackends;
    use crate::{postgres::PostgresPoolConfig, DataLayerConfig};

    #[test]
    fn builds_empty_backends_from_default_config() {
        let backends = DataBackends::from_config(DataLayerConfig::default())
            .expect("empty config should be accepted");

        assert!(!backends.has_runtime_backends());
        assert!(backends.postgres().is_none());
        assert!(backends.redis().is_none());
        assert!(backends.leases().postgres().is_none());
        assert!(backends.locks().redis().is_none());
        assert!(backends.read().auth_api_keys().is_none());
        assert!(backends.read().auth_modules().is_none());
        assert!(backends.read().billing().is_none());
        assert!(backends.read().gemini_file_mappings().is_none());
        assert!(backends.read().global_models().is_none());
        assert!(backends.read().management_tokens().is_none());
        assert!(backends.read().oauth_providers().is_none());
        assert!(backends.read().proxy_nodes().is_none());
        assert!(backends.read().minimal_candidate_selection().is_none());
        assert!(backends.read().request_candidates().is_none());
        assert!(backends.read().provider_catalog().is_none());
        assert!(backends.read().usage().is_none());
        assert!(backends.read().video_tasks().is_none());
        assert!(backends.read().shadow_results().is_none());
        assert!(backends.transactions().postgres().is_none());
        assert!(backends.workers().redis().is_none());
        assert!(backends.write().shadow_results().is_none());
        assert!(backends.write().settlement().is_none());
        assert!(backends.write().usage().is_none());
    }

    #[tokio::test]
    async fn builds_postgres_backend_from_config() {
        let backends = DataBackends::from_config(DataLayerConfig {
            postgres: Some(PostgresPoolConfig {
                database_url: "postgres://localhost/aether".to_string(),
                min_connections: 1,
                max_connections: 4,
                acquire_timeout_ms: 1_000,
                idle_timeout_ms: 5_000,
                max_lifetime_ms: 30_000,
                statement_cache_capacity: 64,
                require_ssl: false,
            }),
            redis: None,
        })
        .expect("postgres backend should build");

        assert!(backends.has_runtime_backends());
        assert!(backends.postgres().is_some());
        assert!(backends.leases().postgres().is_some());
        assert!(backends.read().auth_api_keys().is_some());
        assert!(backends.read().auth_modules().is_some());
        assert!(backends.read().billing().is_some());
        assert!(backends.read().gemini_file_mappings().is_some());
        assert!(backends.read().global_models().is_some());
        assert!(backends.read().management_tokens().is_some());
        assert!(backends.read().oauth_providers().is_some());
        assert!(backends.read().proxy_nodes().is_some());
        assert!(backends.read().minimal_candidate_selection().is_some());
        assert!(backends.read().request_candidates().is_some());
        assert!(backends.read().provider_catalog().is_some());
        assert!(backends.read().provider_quotas().is_some());
        assert!(backends.read().usage().is_some());
        assert!(backends.read().video_tasks().is_some());
        assert!(backends.read().wallets().is_some());
        assert!(backends.read().shadow_results().is_some());
        assert!(backends.transactions().postgres().is_some());
        assert!(backends.write().shadow_results().is_some());
        assert!(backends.write().auth_modules().is_some());
        assert!(backends.write().gemini_file_mappings().is_some());
        assert!(backends.write().management_tokens().is_some());
        assert!(backends.write().oauth_providers().is_some());
        assert!(backends.write().proxy_nodes().is_some());
        assert!(backends.write().provider_catalog().is_some());
        assert!(backends.write().provider_quotas().is_some());
        assert!(backends.write().settlement().is_some());
        assert!(backends.write().usage().is_some());
        assert!(backends.write().wallets().is_some());
        assert!(backends.config().postgres.is_some());
    }

    #[test]
    fn builds_redis_backend_from_config() {
        let backends = DataBackends::from_config(DataLayerConfig {
            postgres: None,
            redis: Some(crate::redis::RedisClientConfig {
                url: "redis://127.0.0.1/0".to_string(),
                key_prefix: Some("aether".to_string()),
            }),
        })
        .expect("redis backend should build");

        assert!(backends.has_runtime_backends());
        assert!(backends.postgres().is_none());
        assert!(backends.redis().is_some());
        assert!(backends.leases().postgres().is_none());
        assert!(backends.locks().redis().is_some());
        assert!(backends.workers().redis().is_some());
        assert!(backends.read().auth_api_keys().is_none());
        assert!(backends.read().auth_modules().is_none());
        assert!(backends.read().global_models().is_none());
        assert!(backends.read().oauth_providers().is_none());
        assert!(backends.transactions().postgres().is_none());
        assert!(backends.write().shadow_results().is_none());
        assert!(backends.write().settlement().is_none());
        assert!(backends.write().usage().is_none());
        assert!(backends.config().redis.is_some());
    }
}
