CREATE TABLE IF NOT EXISTS users (
    id TEXT PRIMARY KEY,
    external_id TEXT,
    email TEXT UNIQUE,
    username TEXT UNIQUE,
    password_hash TEXT,
    role TEXT,
    auth_source TEXT NOT NULL DEFAULT 'local',
    email_verified INTEGER NOT NULL DEFAULT 0,
    is_active INTEGER NOT NULL DEFAULT 1,
    is_deleted INTEGER NOT NULL DEFAULT 0,
    allowed_models TEXT,
    allowed_providers TEXT,
    allowed_api_formats TEXT,
    model_capability_settings TEXT,
    rate_limit INTEGER,
    metadata TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    last_login_at INTEGER
);

CREATE TABLE IF NOT EXISTS api_keys (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    key_hash TEXT NOT NULL UNIQUE,
    key_encrypted TEXT,
    name TEXT,
    key_prefix TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    allowed_models TEXT,
    allowed_providers TEXT,
    allowed_api_formats TEXT,
    rate_limit INTEGER DEFAULT 100,
    concurrent_limit INTEGER,
    force_capabilities TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    is_locked INTEGER NOT NULL DEFAULT 0,
    is_standalone INTEGER NOT NULL DEFAULT 0,
    auto_delete_on_expiry INTEGER NOT NULL DEFAULT 0,
    total_requests INTEGER NOT NULL DEFAULT 0,
    total_tokens INTEGER NOT NULL DEFAULT 0,
    total_cost_usd REAL NOT NULL DEFAULT 0,
    metadata TEXT,
    expires_at INTEGER,
    last_used_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS api_keys_user_id_idx ON api_keys (user_id);

CREATE TABLE IF NOT EXISTS audit_logs (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    user_id TEXT,
    api_key_id TEXT,
    description TEXT NOT NULL,
    ip_address TEXT,
    user_agent TEXT,
    request_id TEXT,
    event_metadata TEXT,
    status_code INTEGER,
    error_message TEXT,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS audit_logs_created_at_idx ON audit_logs (created_at);
CREATE INDEX IF NOT EXISTS audit_logs_event_type_idx ON audit_logs (event_type);
CREATE INDEX IF NOT EXISTS audit_logs_request_id_idx ON audit_logs (request_id);
CREATE INDEX IF NOT EXISTS audit_logs_user_id_idx ON audit_logs (user_id);

CREATE TABLE IF NOT EXISTS announcements (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    content TEXT NOT NULL,
    type TEXT NOT NULL DEFAULT 'info',
    priority INTEGER NOT NULL DEFAULT 0,
    author_id TEXT,
    is_active INTEGER NOT NULL DEFAULT 1,
    is_pinned INTEGER NOT NULL DEFAULT 0,
    start_time INTEGER,
    end_time INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS announcements_author_id_idx ON announcements (author_id);
CREATE INDEX IF NOT EXISTS announcements_created_at_idx ON announcements (created_at);
CREATE INDEX IF NOT EXISTS announcements_is_active_idx ON announcements (is_active);

CREATE TABLE IF NOT EXISTS announcement_reads (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    announcement_id TEXT NOT NULL,
    read_at INTEGER NOT NULL,
    UNIQUE (user_id, announcement_id)
);
CREATE INDEX IF NOT EXISTS announcement_reads_announcement_id_idx ON announcement_reads (announcement_id);
CREATE INDEX IF NOT EXISTS announcement_reads_user_id_idx ON announcement_reads (user_id);

CREATE TABLE IF NOT EXISTS management_tokens (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    token_hash TEXT NOT NULL UNIQUE,
    token_prefix TEXT,
    allowed_ips TEXT,
    expires_at INTEGER,
    last_used_at INTEGER,
    last_used_ip TEXT,
    usage_count INTEGER NOT NULL DEFAULT 0,
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (user_id, name)
);
CREATE INDEX IF NOT EXISTS management_tokens_user_id_idx ON management_tokens (user_id);
CREATE TABLE IF NOT EXISTS billing_rules (
    id TEXT PRIMARY KEY,
    global_model_id TEXT,
    model_id TEXT,
    name TEXT NOT NULL,
    task_type TEXT NOT NULL DEFAULT 'chat',
    expression TEXT NOT NULL,
    variables TEXT NOT NULL DEFAULT '{}',
    dimension_mappings TEXT NOT NULL DEFAULT '{}',
    is_enabled INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    CHECK (
        (global_model_id IS NOT NULL AND model_id IS NULL)
        OR (global_model_id IS NULL AND model_id IS NOT NULL)
    )
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_billing_rules_global_model_task
    ON billing_rules (global_model_id, task_type)
    WHERE is_enabled = 1 AND global_model_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_billing_rules_model_task
    ON billing_rules (model_id, task_type)
    WHERE is_enabled = 1 AND model_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS dimension_collectors (
    id TEXT PRIMARY KEY,
    api_format TEXT NOT NULL,
    task_type TEXT NOT NULL,
    dimension_name TEXT NOT NULL,
    source_type TEXT NOT NULL,
    source_path TEXT,
    value_type TEXT NOT NULL DEFAULT 'float',
    transform_expression TEXT,
    default_value TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    is_enabled INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    CHECK (
        (source_type = 'computed' AND source_path IS NULL AND transform_expression IS NOT NULL)
        OR (source_type <> 'computed' AND source_path IS NOT NULL)
    )
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_dimension_collectors_enabled
    ON dimension_collectors (api_format, task_type, dimension_name, priority)
    WHERE is_enabled = 1;

CREATE TABLE IF NOT EXISTS providers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    website TEXT,
    provider_type TEXT NOT NULL,
    billing_type TEXT,
    monthly_quota_usd REAL,
    monthly_used_usd REAL,
    quota_reset_day INTEGER,
    quota_last_reset_at INTEGER,
    quota_expires_at INTEGER,
    enabled INTEGER NOT NULL DEFAULT 1,
    is_active INTEGER NOT NULL DEFAULT 1,
    priority INTEGER NOT NULL DEFAULT 0,
    provider_priority INTEGER NOT NULL DEFAULT 100,
    keep_priority_on_conversion INTEGER NOT NULL DEFAULT 0,
    enable_format_conversion INTEGER NOT NULL DEFAULT 1,
    concurrent_limit INTEGER,
    max_retries INTEGER,
    proxy TEXT,
    request_timeout REAL,
    stream_first_byte_timeout REAL,
    config TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS provider_api_keys (
    id TEXT PRIMARY KEY,
    provider_id TEXT NOT NULL,
    name TEXT NOT NULL,
    api_key TEXT,
    encrypted_key TEXT,
    auth_type TEXT NOT NULL DEFAULT 'api_key',
    auth_config TEXT,
    note TEXT,
    internal_priority INTEGER NOT NULL DEFAULT 50,
    capabilities TEXT,
    api_formats TEXT,
    auth_type_by_format TEXT,
    allow_auth_channel_mismatch_formats TEXT,
    rate_multipliers TEXT,
    global_priority_by_format TEXT,
    allowed_models TEXT,
    expires_at INTEGER,
    cache_ttl_minutes INTEGER NOT NULL DEFAULT 5,
    max_probe_interval_minutes INTEGER NOT NULL DEFAULT 32,
    proxy TEXT,
    fingerprint TEXT,
    concurrent_limit INTEGER,
    learned_rpm_limit INTEGER,
    concurrent_429_count INTEGER NOT NULL DEFAULT 0,
    rpm_429_count INTEGER NOT NULL DEFAULT 0,
    last_429_at INTEGER,
    last_429_type TEXT,
    adjustment_history TEXT,
    utilization_samples TEXT,
    last_probe_increase_at INTEGER,
    last_rpm_peak INTEGER,
    request_count INTEGER NOT NULL DEFAULT 0,
    total_tokens INTEGER NOT NULL DEFAULT 0,
    total_cost_usd REAL NOT NULL DEFAULT 0,
    success_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    total_response_time_ms INTEGER NOT NULL DEFAULT 0,
    last_used_at INTEGER,
    auto_fetch_models INTEGER NOT NULL DEFAULT 0,
    last_models_fetch_at INTEGER,
    last_models_fetch_error TEXT,
    locked_models TEXT,
    model_include_patterns TEXT,
    model_exclude_patterns TEXT,
    upstream_metadata TEXT,
    oauth_invalid_at INTEGER,
    oauth_invalid_reason TEXT,
    status_snapshot TEXT,
    health_by_format TEXT,
    circuit_breaker_by_format TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    is_active INTEGER NOT NULL DEFAULT 1,
    weight INTEGER NOT NULL DEFAULT 1,
    rpm_limit INTEGER,
    metadata TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS provider_api_keys_provider_id_idx ON provider_api_keys (provider_id);

CREATE TABLE IF NOT EXISTS gemini_file_mappings (
    id TEXT PRIMARY KEY,
    file_name TEXT NOT NULL UNIQUE,
    key_id TEXT NOT NULL,
    user_id TEXT,
    display_name TEXT,
    mime_type TEXT,
    source_hash TEXT,
    created_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS gemini_file_mappings_key_id_idx ON gemini_file_mappings (key_id);
CREATE INDEX IF NOT EXISTS gemini_file_mappings_user_id_idx ON gemini_file_mappings (user_id);
CREATE INDEX IF NOT EXISTS gemini_file_mappings_expires_at_idx ON gemini_file_mappings (expires_at);
CREATE INDEX IF NOT EXISTS gemini_file_mappings_source_hash_idx ON gemini_file_mappings (source_hash);

CREATE TABLE IF NOT EXISTS request_candidates (
    id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL,
    user_id TEXT,
    api_key_id TEXT,
    username TEXT,
    api_key_name TEXT,
    candidate_index INTEGER NOT NULL,
    retry_index INTEGER NOT NULL DEFAULT 0,
    provider_id TEXT,
    endpoint_id TEXT,
    key_id TEXT,
    status TEXT NOT NULL,
    skip_reason TEXT,
    is_cached INTEGER NOT NULL DEFAULT 0,
    status_code INTEGER,
    error_type TEXT,
    error_message TEXT,
    latency_ms INTEGER,
    concurrent_requests INTEGER,
    extra_data TEXT,
    required_capabilities TEXT,
    created_at INTEGER NOT NULL,
    started_at INTEGER,
    finished_at INTEGER,
    UNIQUE (request_id, candidate_index, retry_index)
);
CREATE INDEX IF NOT EXISTS request_candidates_request_id_idx ON request_candidates (request_id);
CREATE INDEX IF NOT EXISTS request_candidates_provider_id_idx ON request_candidates (provider_id);
CREATE INDEX IF NOT EXISTS request_candidates_endpoint_id_idx ON request_candidates (endpoint_id);
CREATE INDEX IF NOT EXISTS request_candidates_status_idx ON request_candidates (status);
CREATE INDEX IF NOT EXISTS request_candidates_created_at_idx ON request_candidates (created_at);
CREATE INDEX IF NOT EXISTS request_candidates_endpoint_status_created_idx ON request_candidates (endpoint_id, status, created_at);

CREATE TABLE IF NOT EXISTS video_tasks (
    id TEXT PRIMARY KEY,
    short_id TEXT UNIQUE,
    request_id TEXT NOT NULL UNIQUE,
    user_id TEXT,
    api_key_id TEXT,
    username TEXT,
    api_key_name TEXT,
    external_task_id TEXT,
    provider_id TEXT,
    endpoint_id TEXT,
    key_id TEXT,
    client_api_format TEXT,
    provider_api_format TEXT,
    format_converted INTEGER NOT NULL DEFAULT 0,
    model TEXT,
    prompt TEXT,
    original_request_body TEXT,
    duration_seconds INTEGER,
    resolution TEXT,
    aspect_ratio TEXT,
    size TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    progress_percent INTEGER NOT NULL DEFAULT 0,
    progress_message TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0,
    poll_interval_seconds INTEGER NOT NULL DEFAULT 10,
    next_poll_at INTEGER,
    poll_count INTEGER NOT NULL DEFAULT 0,
    max_poll_count INTEGER NOT NULL DEFAULT 360,
    created_at INTEGER NOT NULL,
    submitted_at INTEGER,
    completed_at INTEGER,
    updated_at INTEGER NOT NULL,
    error_code TEXT,
    error_message TEXT,
    video_url TEXT,
    request_metadata TEXT
);
CREATE INDEX IF NOT EXISTS video_tasks_external_id_idx ON video_tasks (external_task_id);
CREATE INDEX IF NOT EXISTS video_tasks_next_poll_idx ON video_tasks (next_poll_at);
CREATE INDEX IF NOT EXISTS video_tasks_request_id_idx ON video_tasks (request_id);
CREATE INDEX IF NOT EXISTS video_tasks_user_status_idx ON video_tasks (user_id, status);
CREATE INDEX IF NOT EXISTS video_tasks_api_key_id_idx ON video_tasks (api_key_id);
CREATE INDEX IF NOT EXISTS video_tasks_provider_id_idx ON video_tasks (provider_id);
CREATE INDEX IF NOT EXISTS video_tasks_endpoint_id_idx ON video_tasks (endpoint_id);
CREATE INDEX IF NOT EXISTS video_tasks_key_id_idx ON video_tasks (key_id);

CREATE TABLE IF NOT EXISTS provider_endpoints (
    id TEXT PRIMARY KEY,
    provider_id TEXT NOT NULL,
    name TEXT NOT NULL,
    base_url TEXT NOT NULL,
    api_format TEXT,
    api_family TEXT,
    endpoint_kind TEXT,
    enabled INTEGER NOT NULL DEFAULT 1,
    is_active INTEGER NOT NULL DEFAULT 1,
    health_score REAL NOT NULL DEFAULT 1.0,
    weight INTEGER NOT NULL DEFAULT 1,
    header_rules TEXT,
    body_rules TEXT,
    max_retries INTEGER,
    custom_path TEXT,
    metadata TEXT,
    config TEXT,
    format_acceptance_config TEXT,
    proxy TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS provider_endpoints_provider_id_idx ON provider_endpoints (provider_id);

CREATE TABLE IF NOT EXISTS models (
    id TEXT PRIMARY KEY,
    provider_id TEXT NOT NULL,
    global_model_id TEXT,
    provider_model_name TEXT NOT NULL,
    global_model_name TEXT,
    api_format TEXT,
    enabled INTEGER NOT NULL DEFAULT 1,
    is_active INTEGER NOT NULL DEFAULT 1,
    is_available INTEGER NOT NULL DEFAULT 1,
    price_per_request REAL,
    tiered_pricing TEXT,
    supports_vision INTEGER,
    supports_function_calling INTEGER,
    supports_streaming INTEGER,
    supports_extended_thinking INTEGER,
    supports_image_generation INTEGER,
    provider_model_mappings TEXT,
    config TEXT,
    metadata TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS models_provider_id_idx ON models (provider_id);

CREATE TABLE IF NOT EXISTS global_models (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    display_name TEXT,
    enabled INTEGER NOT NULL DEFAULT 1,
    is_active INTEGER NOT NULL DEFAULT 1,
    default_price_per_request REAL,
    default_tiered_pricing TEXT,
    supported_capabilities TEXT,
    usage_count INTEGER NOT NULL DEFAULT 0,
    config TEXT,
    metadata TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS system_configs (
    id TEXT PRIMARY KEY,
    key TEXT NOT NULL UNIQUE,
    value TEXT NOT NULL,
    description TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS auth_modules (
    id TEXT PRIMARY KEY,
    module_type TEXT NOT NULL UNIQUE,
    enabled INTEGER NOT NULL DEFAULT 1,
    config TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS oauth_providers (
    provider_type TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    client_id TEXT NOT NULL,
    client_secret_encrypted TEXT,
    authorization_url_override TEXT,
    token_url_override TEXT,
    userinfo_url_override TEXT,
    scopes TEXT,
    redirect_uri TEXT NOT NULL,
    frontend_callback_url TEXT NOT NULL,
    attribute_mapping TEXT,
    extra_config TEXT,
    is_enabled INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS ldap_configs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    server_url TEXT NOT NULL,
    bind_dn TEXT NOT NULL,
    bind_password_encrypted TEXT,
    base_dn TEXT NOT NULL,
    user_search_filter TEXT DEFAULT '(uid={username})' NOT NULL,
    username_attr TEXT DEFAULT 'uid' NOT NULL,
    email_attr TEXT DEFAULT 'mail' NOT NULL,
    display_name_attr TEXT DEFAULT 'cn' NOT NULL,
    is_enabled INTEGER NOT NULL DEFAULT 0,
    is_exclusive INTEGER NOT NULL DEFAULT 0,
    use_starttls INTEGER NOT NULL DEFAULT 0,
    connect_timeout INTEGER NOT NULL DEFAULT 10,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS user_oauth_links (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    provider_type TEXT NOT NULL,
    provider_user_id TEXT NOT NULL,
    provider_username TEXT,
    provider_email TEXT,
    extra_data TEXT,
    linked_at INTEGER NOT NULL,
    last_login_at INTEGER
);
CREATE INDEX IF NOT EXISTS user_oauth_links_provider_type_idx ON user_oauth_links (provider_type);
CREATE INDEX IF NOT EXISTS user_oauth_links_user_id_idx ON user_oauth_links (user_id);
CREATE TABLE IF NOT EXISTS proxy_nodes (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    ip TEXT NOT NULL,
    port INTEGER NOT NULL,
    region TEXT,
    status TEXT NOT NULL DEFAULT 'online',
    registered_by TEXT,
    last_heartbeat_at INTEGER,
    heartbeat_interval INTEGER NOT NULL DEFAULT 30,
    active_connections INTEGER NOT NULL DEFAULT 0,
    total_requests INTEGER NOT NULL DEFAULT 0,
    avg_latency_ms REAL,
    is_manual INTEGER NOT NULL DEFAULT 0,
    proxy_url TEXT,
    proxy_username TEXT,
    proxy_password TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    remote_config TEXT,
    config_version INTEGER NOT NULL DEFAULT 0,
    hardware_info TEXT,
    estimated_max_concurrency INTEGER,
    tunnel_mode INTEGER NOT NULL DEFAULT 0,
    tunnel_connected INTEGER NOT NULL DEFAULT 0,
    tunnel_connected_at INTEGER,
    failed_requests INTEGER NOT NULL DEFAULT 0,
    dns_failures INTEGER NOT NULL DEFAULT 0,
    stream_errors INTEGER NOT NULL DEFAULT 0,
    proxy_metadata TEXT
);

CREATE TABLE IF NOT EXISTS proxy_node_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    detail TEXT,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS wallets (
    id TEXT PRIMARY KEY,
    user_id TEXT UNIQUE,
    api_key_id TEXT UNIQUE,
    balance REAL NOT NULL DEFAULT 0,
    gift_balance REAL NOT NULL DEFAULT 0,
    limit_mode TEXT NOT NULL DEFAULT 'finite',
    currency TEXT NOT NULL DEFAULT 'USD',
    status TEXT NOT NULL DEFAULT 'active',
    total_recharged REAL NOT NULL DEFAULT 0,
    total_consumed REAL NOT NULL DEFAULT 0,
    total_refunded REAL NOT NULL DEFAULT 0,
    total_adjusted REAL NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS wallets_api_key_id_idx ON wallets (api_key_id);
CREATE INDEX IF NOT EXISTS wallets_user_id_idx ON wallets (user_id);

CREATE TABLE IF NOT EXISTS wallet_transactions (
    id TEXT PRIMARY KEY,
    wallet_id TEXT NOT NULL,
    category TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    amount REAL NOT NULL,
    balance_before REAL NOT NULL,
    balance_after REAL NOT NULL,
    recharge_balance_before REAL NOT NULL,
    recharge_balance_after REAL NOT NULL,
    gift_balance_before REAL NOT NULL,
    gift_balance_after REAL NOT NULL,
    link_type TEXT,
    link_id TEXT,
    operator_id TEXT,
    description TEXT,
    created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wallet_tx_wallet_created
    ON wallet_transactions (wallet_id, created_at);
CREATE INDEX IF NOT EXISTS idx_wallet_tx_category_created
    ON wallet_transactions (category, created_at);
CREATE INDEX IF NOT EXISTS idx_wallet_tx_reason_created
    ON wallet_transactions (reason_code, created_at);
CREATE INDEX IF NOT EXISTS idx_wallet_tx_link
    ON wallet_transactions (link_type, link_id);
CREATE INDEX IF NOT EXISTS ix_wallet_transactions_operator_id
    ON wallet_transactions (operator_id);

CREATE TABLE IF NOT EXISTS wallet_daily_usage_ledgers (
    id TEXT PRIMARY KEY,
    wallet_id TEXT NOT NULL,
    billing_date TEXT NOT NULL,
    billing_timezone TEXT NOT NULL,
    total_cost_usd REAL NOT NULL DEFAULT 0,
    total_requests INTEGER NOT NULL DEFAULT 0,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    cache_creation_tokens INTEGER NOT NULL DEFAULT 0,
    cache_read_tokens INTEGER NOT NULL DEFAULT 0,
    first_finalized_at INTEGER,
    last_finalized_at INTEGER,
    aggregated_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_wallet_daily_usage_wallet_date
    ON wallet_daily_usage_ledgers (wallet_id, billing_timezone, billing_date);

CREATE TABLE IF NOT EXISTS payment_orders (
    id TEXT PRIMARY KEY,
    order_no TEXT NOT NULL UNIQUE,
    wallet_id TEXT NOT NULL,
    user_id TEXT,
    amount_usd REAL NOT NULL,
    pay_amount REAL,
    pay_currency TEXT,
    exchange_rate REAL,
    refunded_amount_usd REAL NOT NULL DEFAULT 0,
    refundable_amount_usd REAL NOT NULL DEFAULT 0,
    payment_method TEXT NOT NULL,
    gateway_order_id TEXT,
    gateway_response TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at INTEGER NOT NULL,
    paid_at INTEGER,
    credited_at INTEGER,
    expires_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_payment_orders_wallet_created
    ON payment_orders (wallet_id, created_at);
CREATE INDEX IF NOT EXISTS idx_payment_orders_user_created
    ON payment_orders (user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_payment_orders_status
    ON payment_orders (status);
CREATE INDEX IF NOT EXISTS idx_payment_orders_gateway_order_id
    ON payment_orders (gateway_order_id);

CREATE TABLE IF NOT EXISTS payment_callbacks (
    id TEXT PRIMARY KEY,
    payment_order_id TEXT,
    payment_method TEXT NOT NULL,
    callback_key TEXT NOT NULL UNIQUE,
    order_no TEXT,
    gateway_order_id TEXT,
    payload_hash TEXT,
    signature_valid INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'received',
    payload TEXT,
    error_message TEXT,
    created_at INTEGER NOT NULL,
    processed_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_payment_callbacks_order
    ON payment_callbacks (order_no);
CREATE INDEX IF NOT EXISTS idx_payment_callbacks_gateway_order
    ON payment_callbacks (gateway_order_id);
CREATE INDEX IF NOT EXISTS idx_payment_callbacks_created
    ON payment_callbacks (created_at);
CREATE INDEX IF NOT EXISTS ix_payment_callbacks_payment_order_id
    ON payment_callbacks (payment_order_id);

CREATE TABLE IF NOT EXISTS refund_requests (
    id TEXT PRIMARY KEY,
    refund_no TEXT NOT NULL UNIQUE,
    wallet_id TEXT NOT NULL,
    user_id TEXT,
    payment_order_id TEXT,
    source_type TEXT NOT NULL,
    source_id TEXT,
    refund_mode TEXT NOT NULL,
    amount_usd REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending_approval',
    reason TEXT,
    requested_by TEXT,
    approved_by TEXT,
    processed_by TEXT,
    gateway_refund_id TEXT,
    payout_method TEXT,
    payout_reference TEXT,
    payout_proof TEXT,
    failure_reason TEXT,
    idempotency_key TEXT UNIQUE,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    processed_at INTEGER,
    completed_at INTEGER
);
CREATE INDEX IF NOT EXISTS idx_refund_wallet_created
    ON refund_requests (wallet_id, created_at);
CREATE INDEX IF NOT EXISTS idx_refund_user_created
    ON refund_requests (user_id, created_at);
CREATE INDEX IF NOT EXISTS idx_refund_status
    ON refund_requests (status);
CREATE INDEX IF NOT EXISTS ix_refund_requests_payment_order_id
    ON refund_requests (payment_order_id);
CREATE INDEX IF NOT EXISTS ix_refund_requests_requested_by
    ON refund_requests (requested_by);
CREATE INDEX IF NOT EXISTS ix_refund_requests_approved_by
    ON refund_requests (approved_by);
CREATE INDEX IF NOT EXISTS ix_refund_requests_processed_by
    ON refund_requests (processed_by);

CREATE TABLE IF NOT EXISTS redeem_code_batches (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    amount_usd REAL NOT NULL,
    currency TEXT NOT NULL DEFAULT 'USD',
    balance_bucket TEXT NOT NULL DEFAULT 'gift',
    total_count INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    description TEXT,
    created_by TEXT,
    expires_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_redeem_code_batches_status
    ON redeem_code_batches (status, created_at);

CREATE TABLE IF NOT EXISTS redeem_codes (
    id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    code_hash TEXT NOT NULL UNIQUE,
    code_prefix TEXT NOT NULL,
    code_suffix TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    redeemed_by_user_id TEXT,
    redeemed_wallet_id TEXT,
    redeemed_payment_order_id TEXT,
    redeemed_at INTEGER,
    disabled_by TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_redeem_codes_batch_created
    ON redeem_codes (batch_id, created_at);
CREATE INDEX IF NOT EXISTS idx_redeem_codes_status
    ON redeem_codes (status, updated_at);
CREATE INDEX IF NOT EXISTS idx_redeem_codes_redeemed_user
    ON redeem_codes (redeemed_by_user_id, redeemed_at);
CREATE INDEX IF NOT EXISTS idx_redeem_codes_redeemed_order
    ON redeem_codes (redeemed_payment_order_id);
CREATE TABLE IF NOT EXISTS "usage" (
    request_id TEXT PRIMARY KEY,
    id TEXT,
    user_id TEXT,
    api_key_id TEXT,
    provider_name TEXT NOT NULL DEFAULT 'unknown',
    model TEXT NOT NULL DEFAULT 'unknown',
    target_model TEXT,
    provider_id TEXT,
    provider_endpoint_id TEXT,
    provider_api_key_id TEXT,
    request_type TEXT,
    api_format TEXT,
    api_family TEXT,
    endpoint_kind TEXT,
    endpoint_api_format TEXT,
    provider_api_family TEXT,
    provider_endpoint_kind TEXT,
    has_format_conversion INTEGER NOT NULL DEFAULT 0,
    is_stream INTEGER NOT NULL DEFAULT 0,
    input_tokens INTEGER NOT NULL DEFAULT 0,
    output_tokens INTEGER NOT NULL DEFAULT 0,
    total_tokens INTEGER NOT NULL DEFAULT 0,
    cache_creation_input_tokens INTEGER NOT NULL DEFAULT 0,
    cache_creation_ephemeral_5m_input_tokens INTEGER NOT NULL DEFAULT 0,
    cache_creation_ephemeral_1h_input_tokens INTEGER NOT NULL DEFAULT 0,
    cache_read_input_tokens INTEGER NOT NULL DEFAULT 0,
    cache_creation_cost_usd REAL NOT NULL DEFAULT 0,
    cache_read_cost_usd REAL NOT NULL DEFAULT 0,
    output_price_per_1m REAL,
    status_code INTEGER,
    error_message TEXT,
    error_category TEXT,
    response_time_ms INTEGER,
    first_byte_time_ms INTEGER,
    wallet_id TEXT,
    status TEXT NOT NULL DEFAULT 'completed',
    billing_status TEXT NOT NULL DEFAULT 'pending',
    total_cost_usd REAL NOT NULL DEFAULT 0,
    actual_total_cost_usd REAL NOT NULL DEFAULT 0,
    request_metadata TEXT,
    candidate_id TEXT,
    candidate_index INTEGER,
    key_name TEXT,
    planner_kind TEXT,
    route_family TEXT,
    route_kind TEXT,
    execution_path TEXT,
    local_execution_runtime_miss_reason TEXT,
    wallet_balance_before REAL,
    wallet_balance_after REAL,
    wallet_recharge_balance_before REAL,
    wallet_recharge_balance_after REAL,
    wallet_gift_balance_before REAL,
    wallet_gift_balance_after REAL,
    finalized_at INTEGER,
    created_at_unix_ms INTEGER NOT NULL DEFAULT 0,
    updated_at_unix_secs INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS usage_api_key_id_idx ON "usage" (api_key_id);
CREATE INDEX IF NOT EXISTS usage_billing_status_idx ON "usage" (billing_status);
CREATE INDEX IF NOT EXISTS usage_created_at_idx ON "usage" (created_at_unix_ms);
CREATE INDEX IF NOT EXISTS usage_provider_api_key_id_idx ON "usage" (provider_api_key_id);
CREATE INDEX IF NOT EXISTS usage_provider_id_idx ON "usage" (provider_id);
CREATE INDEX IF NOT EXISTS usage_request_id_idx ON "usage" (request_id);
CREATE INDEX IF NOT EXISTS usage_user_id_idx ON "usage" (user_id);
CREATE INDEX IF NOT EXISTS usage_wallet_id_idx ON "usage" (wallet_id);

CREATE TABLE IF NOT EXISTS usage_settlement_snapshots (
    request_id TEXT PRIMARY KEY,
    billing_status TEXT NOT NULL,
    wallet_id TEXT,
    wallet_balance_before REAL,
    wallet_balance_after REAL,
    wallet_recharge_balance_before REAL,
    wallet_recharge_balance_after REAL,
    wallet_gift_balance_before REAL,
    wallet_gift_balance_after REAL,
    provider_monthly_used_usd REAL,
    finalized_at INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS usage_settlement_snapshots_billing_status_idx
    ON usage_settlement_snapshots (billing_status);
CREATE INDEX IF NOT EXISTS usage_settlement_snapshots_wallet_id_idx
    ON usage_settlement_snapshots (wallet_id);
