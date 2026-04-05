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
