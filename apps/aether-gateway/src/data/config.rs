use aether_data::postgres::PostgresPoolConfig;
use aether_data::redis::RedisClientConfig;
use aether_data::DataLayerConfig;
use std::fmt;

#[derive(Clone, Default)]
pub struct GatewayDataConfig {
    postgres: Option<PostgresPoolConfig>,
    redis: Option<RedisClientConfig>,
    encryption_key: Option<String>,
}

impl fmt::Debug for GatewayDataConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatewayDataConfig")
            .field("postgres", &self.postgres)
            .field("redis", &self.redis)
            .field("has_encryption_key", &self.encryption_key.is_some())
            .finish()
    }
}

impl GatewayDataConfig {
    pub fn disabled() -> Self {
        Self::default()
    }

    pub fn from_postgres_config(postgres: PostgresPoolConfig) -> Self {
        Self {
            postgres: Some(postgres),
            redis: None,
            encryption_key: None,
        }
    }

    pub fn from_postgres_url(database_url: impl Into<String>, require_ssl: bool) -> Self {
        let mut postgres = PostgresPoolConfig::default();
        postgres.database_url = database_url.into();
        postgres.require_ssl = require_ssl;
        Self::from_postgres_config(postgres)
    }

    pub fn postgres(&self) -> Option<&PostgresPoolConfig> {
        self.postgres.as_ref()
    }

    pub fn redis(&self) -> Option<&RedisClientConfig> {
        self.redis.as_ref()
    }

    pub fn with_redis_config(mut self, redis: RedisClientConfig) -> Self {
        self.redis = Some(redis);
        self
    }

    pub fn with_redis_url(
        self,
        url: impl Into<String>,
        key_prefix: Option<impl Into<String>>,
    ) -> Self {
        self.with_redis_config(RedisClientConfig {
            url: url.into(),
            key_prefix: key_prefix.map(Into::into),
        })
    }

    pub fn with_encryption_key(mut self, encryption_key: impl Into<String>) -> Self {
        let encryption_key = encryption_key.into();
        let encryption_key = encryption_key.trim();
        self.encryption_key = if encryption_key.is_empty() {
            None
        } else {
            Some(encryption_key.to_string())
        };
        self
    }

    pub fn encryption_key(&self) -> Option<&str> {
        self.encryption_key.as_deref()
    }

    pub fn is_enabled(&self) -> bool {
        self.postgres.is_some() || self.redis.is_some()
    }

    pub fn to_data_layer_config(&self) -> DataLayerConfig {
        DataLayerConfig {
            postgres: self.postgres.clone(),
            redis: self.redis.clone(),
        }
    }
}
