CREATE TABLE story_exposures (
    owner_peer_type TEXT NOT NULL,
    owner_peer_id BIGINT NOT NULL,
    story_id INTEGER NOT NULL,
    viewer_user_id BIGINT NOT NULL,
    date INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT story_exposures_peer_check CHECK (
        owner_peer_type IN ('user', 'channel')
        AND owner_peer_id > 0
        AND story_id > 0
        AND viewer_user_id > 0
    ),
    PRIMARY KEY (owner_peer_type, owner_peer_id, story_id, viewer_user_id)
);

CREATE INDEX story_exposures_viewer_idx
    ON story_exposures (viewer_user_id, owner_peer_type, owner_peer_id, story_id);
