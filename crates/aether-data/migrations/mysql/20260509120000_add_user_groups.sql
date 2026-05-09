ALTER TABLE users
    ADD COLUMN allowed_providers_mode VARCHAR(32) NOT NULL DEFAULT 'unrestricted',
    ADD COLUMN allowed_api_formats_mode VARCHAR(32) NOT NULL DEFAULT 'unrestricted',
    ADD COLUMN allowed_models_mode VARCHAR(32) NOT NULL DEFAULT 'unrestricted',
    ADD COLUMN rate_limit_mode VARCHAR(32) NOT NULL DEFAULT 'system';

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
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    normalized_name VARCHAR(100) NOT NULL,
    description TEXT,
    priority INT NOT NULL DEFAULT 0,
    allowed_providers TEXT,
    allowed_providers_mode VARCHAR(32) NOT NULL DEFAULT 'inherit',
    allowed_api_formats TEXT,
    allowed_api_formats_mode VARCHAR(32) NOT NULL DEFAULT 'inherit',
    allowed_models TEXT,
    allowed_models_mode VARCHAR(32) NOT NULL DEFAULT 'inherit',
    rate_limit INT,
    rate_limit_mode VARCHAR(32) NOT NULL DEFAULT 'inherit',
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    UNIQUE KEY user_groups_normalized_name_key (normalized_name),
    KEY user_groups_priority_name_idx (priority, name, id)
);

CREATE TABLE IF NOT EXISTS user_group_members (
    group_id VARCHAR(64) NOT NULL,
    user_id VARCHAR(64) NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (group_id, user_id),
    KEY user_group_members_user_id_idx (user_id),
    CONSTRAINT user_group_members_group_id_fk
        FOREIGN KEY (group_id) REFERENCES user_groups(id) ON DELETE CASCADE,
    CONSTRAINT user_group_members_user_id_fk
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
);
