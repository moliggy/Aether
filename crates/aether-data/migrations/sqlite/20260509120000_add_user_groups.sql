ALTER TABLE users ADD COLUMN allowed_providers_mode TEXT NOT NULL DEFAULT 'unrestricted';
ALTER TABLE users ADD COLUMN allowed_api_formats_mode TEXT NOT NULL DEFAULT 'unrestricted';
ALTER TABLE users ADD COLUMN allowed_models_mode TEXT NOT NULL DEFAULT 'unrestricted';
ALTER TABLE users ADD COLUMN rate_limit_mode TEXT NOT NULL DEFAULT 'system';

UPDATE users
SET allowed_providers_mode = CASE WHEN allowed_providers IS NULL THEN 'unrestricted' ELSE 'specific' END
WHERE allowed_providers_mode = 'unrestricted';

UPDATE users
SET allowed_api_formats_mode = CASE WHEN allowed_api_formats IS NULL THEN 'unrestricted' ELSE 'specific' END
WHERE allowed_api_formats_mode = 'unrestricted';

UPDATE users
SET allowed_models_mode = CASE WHEN allowed_models IS NULL THEN 'unrestricted' ELSE 'specific' END
WHERE allowed_models_mode = 'unrestricted';

UPDATE users
SET rate_limit_mode = CASE WHEN rate_limit IS NULL THEN 'system' ELSE 'custom' END
WHERE rate_limit_mode = 'system';

CREATE TABLE IF NOT EXISTS user_groups (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    normalized_name TEXT NOT NULL UNIQUE,
    description TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    allowed_providers TEXT,
    allowed_providers_mode TEXT NOT NULL DEFAULT 'inherit',
    allowed_api_formats TEXT,
    allowed_api_formats_mode TEXT NOT NULL DEFAULT 'inherit',
    allowed_models TEXT,
    allowed_models_mode TEXT NOT NULL DEFAULT 'inherit',
    rate_limit INTEGER,
    rate_limit_mode TEXT NOT NULL DEFAULT 'inherit',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS user_group_members (
    group_id TEXT NOT NULL REFERENCES user_groups(id) ON DELETE CASCADE,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (group_id, user_id)
);

CREATE INDEX IF NOT EXISTS user_group_members_user_id_idx
    ON user_group_members (user_id);

CREATE INDEX IF NOT EXISTS user_groups_priority_name_idx
    ON user_groups (priority DESC, name ASC, id ASC);
