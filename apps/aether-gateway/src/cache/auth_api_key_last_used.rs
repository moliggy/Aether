use std::time::Duration;

use aether_cache::ExpiringMap;

#[derive(Debug, Default)]
pub(crate) struct AuthApiKeyLastUsedCache {
    entries: ExpiringMap<String, ()>,
}

impl AuthApiKeyLastUsedCache {
    pub(crate) fn should_touch(&self, api_key_id: &str, ttl: Duration, max_entries: usize) -> bool {
        let key = api_key_id.to_string();
        if self.entries.contains_fresh(&key, ttl) {
            return false;
        }
        self.entries.insert(key, (), ttl, max_entries);
        true
    }
}
