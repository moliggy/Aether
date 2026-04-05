use std::time::Duration;

use aether_cache::ExpiringMap;

use crate::control::GatewayControlAuthContext;

#[derive(Debug, Default)]
pub(crate) struct AuthContextCache {
    entries: ExpiringMap<String, GatewayControlAuthContext>,
}

impl AuthContextCache {
    pub(crate) fn get_fresh(
        &self,
        cache_key: &str,
        ttl: Duration,
    ) -> Option<GatewayControlAuthContext> {
        self.entries.get_fresh(&cache_key.to_string(), ttl)
    }

    pub(crate) fn insert(
        &self,
        cache_key: String,
        auth_context: GatewayControlAuthContext,
        ttl: Duration,
        max_entries: usize,
    ) {
        self.entries
            .insert(cache_key, auth_context, ttl, max_entries);
    }
}
