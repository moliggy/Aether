use std::time::Duration;

use aether_cache::ExpiringMap;
pub(crate) use aether_scheduler_core::SchedulerAffinityTarget;

#[derive(Debug, Default)]
pub(crate) struct SchedulerAffinityCache {
    entries: ExpiringMap<String, SchedulerAffinityTarget>,
}

impl SchedulerAffinityCache {
    pub(crate) fn get_fresh(
        &self,
        cache_key: &str,
        ttl: Duration,
    ) -> Option<SchedulerAffinityTarget> {
        self.entries.get_fresh(&cache_key.to_string(), ttl)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn insert(
        &self,
        cache_key: String,
        target: SchedulerAffinityTarget,
        ttl: Duration,
        max_entries: usize,
    ) {
        self.entries.insert(cache_key, target, ttl, max_entries);
    }

    pub(crate) fn remove(&self, cache_key: &str) -> Option<SchedulerAffinityTarget> {
        self.entries.remove(&cache_key.to_string())
    }
}
