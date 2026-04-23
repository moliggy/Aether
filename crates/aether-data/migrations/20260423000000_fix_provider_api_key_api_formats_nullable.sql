ALTER TABLE public.provider_api_keys
    ALTER COLUMN api_formats DROP DEFAULT,
    ALTER COLUMN api_formats DROP NOT NULL;

UPDATE public.provider_api_keys AS pak
SET
    api_formats = NULL,
    updated_at = NOW()
FROM public.providers AS p
WHERE p.id = pak.provider_id
  AND pak.api_formats IS NOT NULL
  AND pak.api_formats::jsonb = '[]'::jsonb
  AND (
    (
      LOWER(BTRIM(p.provider_type)) IN (
        'claude_code',
        'codex',
        'gemini_cli',
        'vertex_ai',
        'antigravity'
      )
      AND LOWER(BTRIM(pak.auth_type)) = 'oauth'
    )
    OR (
      LOWER(BTRIM(p.provider_type)) = 'kiro'
      AND (
        LOWER(BTRIM(pak.auth_type)) = 'oauth'
        OR (
          LOWER(BTRIM(pak.auth_type)) = 'bearer'
          AND COALESCE(BTRIM(pak.auth_config), '') <> ''
        )
      )
    )
  );
