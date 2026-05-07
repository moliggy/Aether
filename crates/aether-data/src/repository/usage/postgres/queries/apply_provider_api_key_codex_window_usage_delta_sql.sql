WITH target_key AS (
  SELECT
    id,
    COALESCE(status_snapshot::jsonb, '{}'::jsonb) AS snapshot
  FROM provider_api_keys
  WHERE id = $1
    AND jsonb_typeof((status_snapshot::jsonb) -> 'quota' -> 'windows') = 'array'
    AND lower(BTRIM(COALESCE((status_snapshot::jsonb) -> 'quota' ->> 'provider_type', ''))) = 'codex'
  FOR UPDATE
),
window_items AS (
  SELECT
    target_key.id,
    window_rows.window_item,
    window_rows.window_ordinality
  FROM target_key
  CROSS JOIN LATERAL jsonb_array_elements(target_key.snapshot -> 'quota' -> 'windows')
    WITH ORDINALITY AS window_rows(window_item, window_ordinality)
),
parsed_windows AS (
  SELECT
    window_items.id,
    window_items.window_item,
    window_items.window_ordinality,
    lower(BTRIM(COALESCE(window_items.window_item ->> 'code', ''))) AS window_code,
    CASE
      WHEN text_values.reset_at_text ~ '^[0-9]+$'
           AND (
             length(text_values.reset_at_text) < 19
             OR (
               length(text_values.reset_at_text) = 19
               AND text_values.reset_at_text <= '9223372036854775807'
             )
           )
      THEN text_values.reset_at_text::BIGINT
      ELSE NULL
    END AS reset_at,
    CASE
      WHEN text_values.window_minutes_text ~ '^[0-9]+$'
           AND (
             length(text_values.window_minutes_text) < 19
             OR (
               length(text_values.window_minutes_text) = 19
               AND text_values.window_minutes_text <= '9223372036854775807'
             )
           )
      THEN text_values.window_minutes_text::BIGINT
      ELSE NULL
    END AS window_minutes,
    CASE
      WHEN text_values.usage_reset_at_text ~ '^[0-9]+$'
           AND (
             length(text_values.usage_reset_at_text) < 19
             OR (
               length(text_values.usage_reset_at_text) = 19
               AND text_values.usage_reset_at_text <= '9223372036854775807'
             )
           )
      THEN text_values.usage_reset_at_text::BIGINT
      ELSE NULL
    END AS usage_reset_at,
    CASE
      WHEN text_values.request_count_text ~ '^[0-9]+$'
           AND (
             length(text_values.request_count_text) < 19
             OR (
               length(text_values.request_count_text) = 19
               AND text_values.request_count_text <= '9223372036854775807'
             )
           )
      THEN text_values.request_count_text::BIGINT
      ELSE 0
    END AS current_request_count,
    CASE
      WHEN text_values.total_tokens_text ~ '^[0-9]+$'
           AND (
             length(text_values.total_tokens_text) < 19
             OR (
               length(text_values.total_tokens_text) = 19
               AND text_values.total_tokens_text <= '9223372036854775807'
             )
           )
      THEN text_values.total_tokens_text::BIGINT
      ELSE 0
    END AS current_total_tokens,
    CASE
      WHEN text_values.total_cost_usd_text ~ '^[-+]?[0-9]+([.][0-9]+)?$'
      THEN text_values.total_cost_usd_text::DOUBLE PRECISION
      ELSE 0
    END AS current_total_cost_usd
  FROM window_items
  CROSS JOIN LATERAL (
    SELECT
      BTRIM(COALESCE(window_items.window_item ->> 'reset_at', '')) AS reset_at_text,
      BTRIM(COALESCE(window_items.window_item ->> 'window_minutes', '')) AS window_minutes_text,
      BTRIM(COALESCE(window_items.window_item ->> 'usage_reset_at', '')) AS usage_reset_at_text,
      BTRIM(COALESCE(window_items.window_item -> 'usage' ->> 'request_count', '')) AS request_count_text,
      BTRIM(COALESCE(window_items.window_item -> 'usage' ->> 'total_tokens', '')) AS total_tokens_text,
      BTRIM(COALESCE(window_items.window_item -> 'usage' ->> 'total_cost_usd', '')) AS total_cost_usd_text
  ) AS text_values
),
window_usage AS (
  SELECT
    parsed_windows.*,
    CASE
      WHEN parsed_windows.window_minutes BETWEEN 0 AND 153722867280912930
      THEN parsed_windows.window_minutes * 60
      ELSE NULL
    END AS window_seconds
  FROM parsed_windows
),
updated_windows AS (
  SELECT
    window_usage.id,
    jsonb_agg(
      CASE
        WHEN window_usage.window_code IN ('5h', 'weekly')
             AND window_usage.reset_at IS NOT NULL
             AND window_usage.window_seconds IS NOT NULL
             AND window_usage.reset_at >= window_usage.window_seconds
             AND window_usage.reset_at > $2
             AND GREATEST(
               window_usage.reset_at - window_usage.window_seconds,
               COALESCE(window_usage.usage_reset_at, 0)
             ) <= $2
        THEN jsonb_set(
          window_usage.window_item,
          '{usage}',
          jsonb_build_object(
            'request_count',
            LEAST(
              GREATEST(window_usage.current_request_count::NUMERIC + $3::NUMERIC, 0),
              9223372036854775807
            )::BIGINT,
            'total_tokens',
            LEAST(
              GREATEST(window_usage.current_total_tokens::NUMERIC + $4::NUMERIC, 0),
              9223372036854775807
            )::BIGINT,
            'total_cost_usd',
            to_char(
              GREATEST(COALESCE(window_usage.current_total_cost_usd, 0) + $5, 0),
              'FM999999999999999990.00000000'
            )
          ),
          true
        )
        ELSE window_usage.window_item
      END
      ORDER BY window_usage.window_ordinality
    ) AS windows
  FROM window_usage
  GROUP BY window_usage.id
)
UPDATE provider_api_keys AS keys
SET
  status_snapshot = jsonb_set(
    target_key.snapshot,
    '{quota,windows}',
    updated_windows.windows,
    true
  )::json,
  updated_at = NOW()
FROM target_key
JOIN updated_windows ON updated_windows.id = target_key.id
WHERE keys.id = target_key.id
