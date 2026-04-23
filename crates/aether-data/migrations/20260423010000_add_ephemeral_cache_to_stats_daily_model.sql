ALTER TABLE public.stats_daily_model
    ADD COLUMN IF NOT EXISTS cache_creation_ephemeral_5m_tokens bigint DEFAULT '0'::bigint NOT NULL,
    ADD COLUMN IF NOT EXISTS cache_creation_ephemeral_1h_tokens bigint DEFAULT '0'::bigint NOT NULL;
