#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StoredSystemConfigEntry {
    pub key: String,
    pub value: serde_json::Value,
    pub description: Option<String>,
    pub updated_at_unix_secs: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AdminSecurityBlacklistEntry {
    pub ip_address: String,
    pub reason: String,
    pub ttl_seconds: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub struct AdminSystemStats {
    pub total_users: u64,
    pub active_users: u64,
    pub total_api_keys: u64,
    pub total_requests: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminSystemPurgeTarget {
    Config,
    Users,
    Usage,
    AuditLogs,
    RequestBodies,
    Stats,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct AdminSystemPurgeSummary {
    pub affected: std::collections::BTreeMap<String, u64>,
}

impl AdminSystemPurgeSummary {
    pub fn add(&mut self, key: impl Into<String>, count: u64) {
        *self.affected.entry(key.into()).or_insert(0) += count;
    }

    pub fn merge(&mut self, other: &Self) {
        for (key, count) in &other.affected {
            self.add(key.clone(), *count);
        }
    }

    pub fn total(&self) -> u64 {
        self.affected.values().copied().sum()
    }
}
