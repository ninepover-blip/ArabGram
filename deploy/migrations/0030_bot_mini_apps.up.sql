CREATE TABLE bot_apps (
    id BIGINT PRIMARY KEY,
    bot_user_id BIGINT NOT NULL REFERENCES bots(bot_user_id) ON DELETE CASCADE,
    short_name VARCHAR(64) NOT NULL,
    title VARCHAR(128) NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    url VARCHAR(512) NOT NULL,
    photo_id BIGINT NOT NULL DEFAULT 0,
    document_id BIGINT NOT NULL DEFAULT 0,
    access_hash BIGINT NOT NULL,
    hash BIGINT NOT NULL,
    inactive BOOLEAN NOT NULL DEFAULT FALSE,
    request_write_access BOOLEAN NOT NULL DEFAULT FALSE,
    has_settings BOOLEAN NOT NULL DEFAULT FALSE,
    is_main BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT bot_apps_short_name_nonempty CHECK (short_name <> ''),
    CONSTRAINT bot_apps_title_nonempty CHECK (title <> ''),
    CONSTRAINT bot_apps_url_nonempty CHECK (url <> '')
);

CREATE UNIQUE INDEX bot_apps_bot_short_name_unique_idx ON bot_apps (bot_user_id, lower(short_name));
CREATE UNIQUE INDEX bot_apps_bot_main_unique_idx ON bot_apps (bot_user_id) WHERE is_main;
CREATE INDEX bot_apps_bot_idx ON bot_apps (bot_user_id, is_main, short_name);

CREATE TABLE bot_app_settings (
    bot_user_id BIGINT PRIMARY KEY REFERENCES bots(bot_user_id) ON DELETE CASCADE,
    placeholder_path BYTEA NOT NULL DEFAULT '\x',
    background_color INTEGER,
    background_dark_color INTEGER,
    header_color INTEGER,
    header_dark_color INTEGER,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE bot_app_preview_media (
    id BIGSERIAL PRIMARY KEY,
    bot_user_id BIGINT NOT NULL REFERENCES bots(bot_user_id) ON DELETE CASCADE,
    app_id BIGINT NOT NULL REFERENCES bot_apps(id) ON DELETE CASCADE,
    position INTEGER NOT NULL,
    photo_id BIGINT NOT NULL DEFAULT 0,
    document_id BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT bot_app_preview_media_one_media CHECK ((photo_id <> 0) <> (document_id <> 0))
);

CREATE UNIQUE INDEX bot_app_preview_media_position_unique_idx ON bot_app_preview_media (app_id, position);
CREATE INDEX bot_app_preview_media_bot_app_idx ON bot_app_preview_media (bot_user_id, app_id, position);

CREATE TABLE attach_menu_bots (
    bot_user_id BIGINT PRIMARY KEY REFERENCES bots(bot_user_id) ON DELETE CASCADE,
    app_id BIGINT REFERENCES bot_apps(id) ON DELETE SET NULL,
    short_name VARCHAR(64) NOT NULL,
    inactive BOOLEAN NOT NULL DEFAULT FALSE,
    has_settings BOOLEAN NOT NULL DEFAULT FALSE,
    request_write_access BOOLEAN NOT NULL DEFAULT FALSE,
    show_in_attach_menu BOOLEAN NOT NULL DEFAULT TRUE,
    show_in_side_menu BOOLEAN NOT NULL DEFAULT TRUE,
    side_menu_disclaimer_needed BOOLEAN NOT NULL DEFAULT FALSE,
    peer_types TEXT[] NOT NULL DEFAULT ARRAY['pm','chat','megagroup','broadcast']::text[],
    icons JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT attach_menu_bots_short_name_nonempty CHECK (short_name <> '')
);

CREATE INDEX attach_menu_bots_visible_idx ON attach_menu_bots (inactive, bot_user_id);

CREATE TABLE attach_menu_user_states (
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    bot_user_id BIGINT NOT NULL REFERENCES bots(bot_user_id) ON DELETE CASCADE,
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    write_allowed BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, bot_user_id),
    CONSTRAINT attach_menu_user_states_not_self CHECK (user_id <> bot_user_id)
);

CREATE INDEX attach_menu_user_states_bot_idx ON attach_menu_user_states (bot_user_id, user_id);

CREATE TABLE webview_requested_buttons (
    webapp_req_id TEXT PRIMARY KEY,
    bot_user_id BIGINT NOT NULL REFERENCES bots(bot_user_id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    button_id INTEGER NOT NULL,
    text TEXT NOT NULL DEFAULT '',
    peer_type TEXT NOT NULL,
    max_quantity INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT webview_requested_buttons_not_self CHECK (bot_user_id <> user_id)
);

CREATE INDEX webview_requested_buttons_lookup_idx ON webview_requested_buttons (bot_user_id, user_id, webapp_req_id);
CREATE INDEX webview_requested_buttons_expires_idx ON webview_requested_buttons (expires_at);

CREATE TABLE bot_emoji_status_permissions (
    bot_user_id BIGINT NOT NULL REFERENCES bots(bot_user_id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    allowed BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (bot_user_id, user_id),
    CONSTRAINT bot_emoji_status_permissions_not_self CHECK (bot_user_id <> user_id)
);

CREATE INDEX bot_emoji_status_permissions_user_idx ON bot_emoji_status_permissions (user_id, bot_user_id);

CREATE TABLE webview_custom_method_queries (
    id TEXT PRIMARY KEY,
    bot_user_id BIGINT NOT NULL REFERENCES bots(bot_user_id) ON DELETE CASCADE,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    custom_method TEXT NOT NULL,
    params JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    CONSTRAINT webview_custom_method_queries_not_self CHECK (bot_user_id <> user_id)
);

CREATE INDEX webview_custom_method_queries_bot_user_idx ON webview_custom_method_queries (bot_user_id, user_id, created_at DESC);
CREATE INDEX webview_custom_method_queries_expires_idx ON webview_custom_method_queries (expires_at);
