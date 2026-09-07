-- Hard cut from mutable queue items/readiness mirrors to immutable items plus
-- durable account-stream lanes and fenced attempt evidence. This migration is
-- intentionally single-version and must run with Core/Egress stopped.

LOCK TABLE dispatch_outbox IN ACCESS EXCLUSIVE MODE;
LOCK TABLE edge_delivery_outbox IN ACCESS EXCLUSIVE MODE;

-- Auth-key identities are opaque MTProto 64-bit patterns, not signed SQL
-- integers. This migration-local conversion preserves every bit in protocol
-- byte order while changing the two existing columns to bytea. Runtime
-- producers pass raw eight-byte values and cannot call this helper.
CREATE FUNCTION outbox_auth_key_id_bytes(input_value bigint)
RETURNS bytea
LANGUAGE plpgsql
IMMUTABLE STRICT PARALLEL SAFE
AS $$
DECLARE
    network_bytes bytea := int8send(input_value);
    raw_bytes bytea := decode('0000000000000000', 'hex');
    byte_index integer;
BEGIN
    FOR byte_index IN 0..7 LOOP
        raw_bytes := set_byte(raw_bytes, byte_index, get_byte(network_bytes, 7 - byte_index));
    END LOOP;
    RETURN raw_bytes;
END;
$$;

ALTER TABLE dispatch_outbox
    DROP CONSTRAINT dispatch_outbox_exclusion_pair_check,
    ALTER COLUMN exclude_auth_key_id DROP DEFAULT,
    ALTER COLUMN exclude_auth_key_id TYPE bytea
        USING outbox_auth_key_id_bytes(exclude_auth_key_id),
    ALTER COLUMN exclude_auth_key_id SET DEFAULT decode('0000000000000000', 'hex'),
    ADD CONSTRAINT dispatch_outbox_auth_key_bytes_check
        CHECK (octet_length(exclude_auth_key_id) = 8),
    ADD CONSTRAINT dispatch_outbox_exclusion_pair_check CHECK (
        (exclude_auth_key_id = decode('0000000000000000', 'hex')) = (exclude_session_id = 0)
    );

ALTER TABLE edge_delivery_outbox
    DROP CONSTRAINT edge_delivery_outbox_exclusion_pair_check,
    ALTER COLUMN exclude_auth_key_id DROP DEFAULT,
    ALTER COLUMN exclude_auth_key_id TYPE bytea
        USING outbox_auth_key_id_bytes(exclude_auth_key_id),
    ALTER COLUMN exclude_auth_key_id SET DEFAULT decode('0000000000000000', 'hex'),
    ADD CONSTRAINT edge_delivery_outbox_auth_key_bytes_check
        CHECK (octet_length(exclude_auth_key_id) = 8),
    ADD CONSTRAINT edge_delivery_outbox_exclusion_pair_check CHECK (
        (exclude_auth_key_id = decode('0000000000000000', 'hex')) = (exclude_session_id = 0)
    );

DROP FUNCTION outbox_auth_key_id_bytes(bigint);

DROP TRIGGER IF EXISTS dispatch_outbox_insert_ready_notify ON dispatch_outbox;
DROP TRIGGER IF EXISTS dispatch_outbox_update_ready_notify ON dispatch_outbox;
DROP TRIGGER IF EXISTS dispatch_outbox_delete_ready_notify ON dispatch_outbox;
DROP FUNCTION IF EXISTS dispatch_outbox_notify_ready_heads();

DROP TRIGGER IF EXISTS dispatch_outbox_insert_user_head ON dispatch_outbox;
DROP TRIGGER IF EXISTS dispatch_outbox_update_user_head ON dispatch_outbox;
DROP TRIGGER IF EXISTS dispatch_outbox_delete_user_head ON dispatch_outbox;
DROP FUNCTION IF EXISTS dispatch_outbox_maintain_user_head();

DROP TRIGGER IF EXISTS edge_delivery_outbox_insert_ready_notify ON edge_delivery_outbox;
DROP TRIGGER IF EXISTS edge_delivery_outbox_update_ready_notify ON edge_delivery_outbox;
DROP TRIGGER IF EXISTS edge_delivery_outbox_delete_ready_notify ON edge_delivery_outbox;
DROP FUNCTION IF EXISTS edge_delivery_outbox_notify_ready();

DROP TABLE dispatch_outbox_user_heads;

DROP INDEX IF EXISTS dispatch_outbox_pending_ready_idx;
DROP INDEX IF EXISTS dispatch_outbox_dispatching_stale_ready_idx;
DROP INDEX IF EXISTS dispatch_outbox_failed_cleanup_ready_idx;
DROP INDEX IF EXISTS dispatch_outbox_logical_shard_head_idx;

DROP INDEX IF EXISTS edge_delivery_outbox_pending_shard_idx;
DROP INDEX IF EXISTS edge_delivery_outbox_dispatching_stale_idx;
DROP INDEX IF EXISTS edge_delivery_outbox_awaiting_ack_stale_idx;
DROP INDEX IF EXISTS edge_delivery_outbox_failed_cleanup_idx;

ALTER TABLE edge_delivery_outbox
    ADD COLUMN recovery_policy varchar(32) NOT NULL DEFAULT 'absolute_reload'
        CHECK (recovery_policy = 'absolute_reload'),
    ADD CONSTRAINT edge_delivery_outbox_payload_size_check
        CHECK (octet_length(payload) <= 16777216);

-- Fences come from non-cycling global queue sequences rather than lane-local
-- increments. Deleting and later recreating an empty stream can therefore
-- never reuse an old DeliveryRef fence. CACHE 1 is a correctness invariant:
-- PostgreSQL sequence caches are backend-local and larger caches can return a
-- lower fence after another pooled connection has already returned a higher one.
CREATE SEQUENCE dispatch_outbox_lease_fence_seq AS bigint
    MINVALUE 1 MAXVALUE 9223372036854775807 NO CYCLE CACHE 1;
CREATE SEQUENCE edge_delivery_outbox_lease_fence_seq AS bigint
    MINVALUE 1 MAXVALUE 9223372036854775807 NO CYCLE CACHE 1;

CREATE TABLE dispatch_outbox_lanes (
    stream_id bigint PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    head_item_id bigint NOT NULL,
    head_sequence bigint NOT NULL CHECK (head_sequence > 0),
    logical_shard smallint GENERATED ALWAYS AS (
        mod(stream_id, 256::bigint)::smallint
    ) STORED,
    state varchar(16) NOT NULL DEFAULT 'ready'
        CHECK (state IN ('ready', 'leased')),
    ready_at timestamptz NOT NULL DEFAULT now(),
    lease_fence bigint NOT NULL DEFAULT 0 CHECK (lease_fence >= 0),
    lease_owner text NOT NULL DEFAULT '',
    lease_until timestamptz,
    window_end_item_id bigint,
    window_end_sequence bigint,
    last_error text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (logical_shard >= 0 AND logical_shard < 256),
    CHECK ((window_end_item_id IS NULL) = (window_end_sequence IS NULL)),
    CONSTRAINT dispatch_outbox_lanes_head_fkey
        FOREIGN KEY (stream_id, head_item_id)
        REFERENCES dispatch_outbox (target_user_id, id)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX dispatch_outbox_lanes_ready_shard_idx
    ON dispatch_outbox_lanes (
        logical_shard, ready_at, stream_id, head_sequence, head_item_id
    ) WHERE state = 'ready';

CREATE INDEX dispatch_outbox_lanes_lease_shard_idx
    ON dispatch_outbox_lanes (
        logical_shard, lease_until, stream_id, head_sequence, head_item_id
    ) WHERE state = 'leased';

-- Attempts deliberately do not reference immutable items: finalization deletes
-- the item while retaining this row as the authoritative fenced tombstone.
CREATE TABLE dispatch_outbox_attempts (
    stream_id bigint NOT NULL,
    item_id bigint NOT NULL,
    sequence bigint NOT NULL CHECK (sequence > 0),
    lease_fence bigint NOT NULL CHECK (lease_fence > 0),
    attempt integer NOT NULL CHECK (attempt > 0),
    window_ordinal integer NOT NULL CHECK (window_ordinal >= 0),
    lease_owner text NOT NULL CHECK (lease_owner <> ''),
    delivery_source_instance_id text NOT NULL DEFAULT '',
    issued_at timestamptz NOT NULL DEFAULT now(),
    lease_until timestamptz NOT NULL,
    command_not_after timestamptz NOT NULL,
    evidence_deadline timestamptz NOT NULL,
    targets_bound boolean NOT NULL DEFAULT false,
    target_count integer NOT NULL DEFAULT 0 CHECK (target_count >= 0),
    empty_evidence_kind varchar(32)
        CHECK (empty_evidence_kind IS NULL OR empty_evidence_kind = 'authoritative_no_targets'),
    evidence_at timestamptz,
    resolution varchar(32)
        CHECK (resolution IS NULL OR resolution IN ('confirmed', 'retry', 'terminal_resync')),
    retry_at timestamptz,
    last_error text NOT NULL DEFAULT '',
    finalized_at timestamptz,
    finalization_outcome varchar(32)
        CHECK (finalization_outcome IS NULL OR finalization_outcome IN ('applied', 'scheduled_retry', 'terminal_resync', 'superseded')),
    PRIMARY KEY (item_id, lease_fence),
    UNIQUE (item_id, attempt),
    CHECK (targets_bound OR target_count = 0),
    CHECK (issued_at < command_not_after AND command_not_after < evidence_deadline AND evidence_deadline < lease_until),
    CHECK ((NOT targets_bound AND delivery_source_instance_id = '') OR
           (targets_bound AND delivery_source_instance_id <> '')),
    CHECK (empty_evidence_kind IS NULL OR (targets_bound AND target_count = 0)),
    CHECK ((resolution IS NOT DISTINCT FROM 'confirmed') = (evidence_at IS NOT NULL)),
    CHECK (resolution <> 'retry' OR retry_at IS NOT NULL),
    CHECK ((finalized_at IS NULL) = (finalization_outcome IS NULL)),
    CHECK (finalization_outcome IS NULL OR finalization_outcome = 'superseded'
           OR (finalization_outcome = 'applied' AND resolution = 'confirmed')
           OR (finalization_outcome = 'scheduled_retry' AND resolution = 'retry')
           OR finalization_outcome = resolution)
);

CREATE INDEX dispatch_outbox_attempts_finalize_idx
    ON dispatch_outbox_attempts (stream_id, lease_fence, sequence, item_id)
    WHERE resolution IS NOT NULL AND finalized_at IS NULL;

CREATE INDEX dispatch_outbox_attempts_evidence_deadline_idx
    ON dispatch_outbox_attempts (evidence_deadline, stream_id, item_id, lease_fence)
    WHERE resolution IS NULL AND finalized_at IS NULL;

CREATE INDEX dispatch_outbox_attempts_tombstone_cleanup_idx
    ON dispatch_outbox_attempts (finalized_at, item_id)
    WHERE finalized_at IS NOT NULL;

CREATE TABLE dispatch_outbox_attempt_targets (
    item_id bigint NOT NULL,
    lease_fence bigint NOT NULL,
    target_instance_id text NOT NULL CHECK (target_instance_id <> ''),
    target_user_id bigint NOT NULL CHECK (target_user_id > 0),
    batch_id bytea NOT NULL CHECK (octet_length(batch_id) = 16),
    command_id bytea NOT NULL CHECK (octet_length(command_id) = 16),
    evidence_kind varchar(32)
        CHECK (evidence_kind IS NULL OR evidence_kind IN ('edge_written', 'edge_no_eligible')),
    evidence_at timestamptz,
    eligible_sessions integer NOT NULL DEFAULT 0 CHECK (eligible_sessions >= 0),
    written_sessions integer NOT NULL DEFAULT 0 CHECK (written_sessions >= 0),
    physical_first_server_msg_id bigint NOT NULL DEFAULT 0,
    client_ack_at timestamptz,
    client_ack_auth_key_id bytea NOT NULL DEFAULT decode('0000000000000000', 'hex')
        CHECK (octet_length(client_ack_auth_key_id) = 8),
    client_ack_session_id bigint NOT NULL DEFAULT 0,
    client_ack_server_msg_id bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (item_id, lease_fence, target_instance_id, target_user_id, batch_id, command_id),
    UNIQUE (item_id, lease_fence, target_instance_id, target_user_id),
    CHECK (
        (evidence_kind IS NULL AND eligible_sessions = 0 AND written_sessions = 0
         AND physical_first_server_msg_id = 0)
        OR (evidence_kind = 'edge_no_eligible' AND eligible_sessions = 0 AND written_sessions = 0
            AND physical_first_server_msg_id = 0)
        OR (evidence_kind = 'edge_written' AND eligible_sessions > 0
            AND written_sessions = eligible_sessions AND physical_first_server_msg_id > 0)
    ),
    CHECK ((evidence_kind IS NULL) = (evidence_at IS NULL)),
    CHECK (
        (client_ack_at IS NULL
         AND client_ack_auth_key_id = decode('0000000000000000', 'hex')
         AND client_ack_session_id = 0 AND client_ack_server_msg_id = 0)
        OR
        (client_ack_at IS NOT NULL
         AND client_ack_auth_key_id <> decode('0000000000000000', 'hex')
         AND client_ack_session_id <> 0 AND client_ack_server_msg_id > 0)
    ),
    FOREIGN KEY (item_id, lease_fence)
        REFERENCES dispatch_outbox_attempts (item_id, lease_fence)
        ON DELETE CASCADE
);

CREATE INDEX dispatch_outbox_attempt_targets_pending_idx
    ON dispatch_outbox_attempt_targets (item_id, lease_fence)
    WHERE evidence_kind IS NULL;

CREATE TABLE edge_delivery_outbox_lanes (
    stream_id bigint PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    head_item_id bigint NOT NULL,
    head_sequence bigint NOT NULL CHECK (head_sequence > 0),
    logical_shard smallint GENERATED ALWAYS AS (
        mod(stream_id, 256::bigint)::smallint
    ) STORED,
    state varchar(16) NOT NULL DEFAULT 'ready'
        CHECK (state IN ('ready', 'leased')),
    ready_at timestamptz NOT NULL DEFAULT now(),
    lease_fence bigint NOT NULL DEFAULT 0 CHECK (lease_fence >= 0),
    lease_owner text NOT NULL DEFAULT '',
    lease_until timestamptz,
    window_end_item_id bigint,
    window_end_sequence bigint,
    last_error text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK (logical_shard >= 0 AND logical_shard < 256),
    CHECK ((window_end_item_id IS NULL) = (window_end_sequence IS NULL)),
    CONSTRAINT edge_delivery_outbox_lanes_head_fkey
        FOREIGN KEY (stream_id, head_item_id)
        REFERENCES edge_delivery_outbox (target_user_id, id)
        DEFERRABLE INITIALLY DEFERRED
);

CREATE INDEX edge_delivery_outbox_lanes_ready_shard_idx
    ON edge_delivery_outbox_lanes (
        logical_shard, ready_at, stream_id, head_item_id
    ) WHERE state = 'ready';

CREATE INDEX edge_delivery_outbox_lanes_lease_shard_idx
    ON edge_delivery_outbox_lanes (
        logical_shard, lease_until, stream_id, head_item_id
    ) WHERE state = 'leased';

-- Same tombstone rule as the PTS queue: item deletion must not erase attempt
-- identity, evidence, or the positive AlreadyFinalized proof.
CREATE TABLE edge_delivery_outbox_attempts (
    stream_id bigint NOT NULL,
    item_id bigint NOT NULL,
    sequence bigint NOT NULL DEFAULT 0 CHECK (sequence = 0),
    lease_fence bigint NOT NULL CHECK (lease_fence > 0),
    attempt integer NOT NULL CHECK (attempt > 0),
    window_ordinal integer NOT NULL CHECK (window_ordinal >= 0),
    lease_owner text NOT NULL CHECK (lease_owner <> ''),
    delivery_source_instance_id text NOT NULL DEFAULT '',
    issued_at timestamptz NOT NULL DEFAULT now(),
    lease_until timestamptz NOT NULL,
    command_not_after timestamptz NOT NULL,
    evidence_deadline timestamptz NOT NULL,
    targets_bound boolean NOT NULL DEFAULT false,
    target_count integer NOT NULL DEFAULT 0 CHECK (target_count >= 0),
    empty_evidence_kind varchar(32)
        CHECK (empty_evidence_kind IS NULL OR empty_evidence_kind = 'authoritative_no_targets'),
    evidence_at timestamptz,
    resolution varchar(32)
        CHECK (resolution IS NULL OR resolution IN ('confirmed', 'retry', 'abandoned')),
    retry_at timestamptz,
    last_error text NOT NULL DEFAULT '',
    finalized_at timestamptz,
    finalization_outcome varchar(32)
        CHECK (finalization_outcome IS NULL OR finalization_outcome IN ('applied', 'scheduled_retry', 'abandoned', 'superseded')),
    PRIMARY KEY (item_id, lease_fence),
    UNIQUE (item_id, attempt),
    CHECK (targets_bound OR target_count = 0),
    CHECK (issued_at < command_not_after AND command_not_after < evidence_deadline AND evidence_deadline < lease_until),
    CHECK ((NOT targets_bound AND delivery_source_instance_id = '') OR
           (targets_bound AND delivery_source_instance_id <> '')),
    CHECK (empty_evidence_kind IS NULL OR (targets_bound AND target_count = 0)),
    CHECK ((resolution IS NOT DISTINCT FROM 'confirmed') = (evidence_at IS NOT NULL)),
    CHECK (resolution <> 'retry' OR retry_at IS NOT NULL),
    CHECK ((finalized_at IS NULL) = (finalization_outcome IS NULL)),
    CHECK (finalization_outcome IS NULL OR finalization_outcome = 'superseded'
           OR (finalization_outcome = 'applied' AND resolution = 'confirmed')
           OR (finalization_outcome = 'scheduled_retry' AND resolution = 'retry')
           OR finalization_outcome = resolution)
);

CREATE INDEX edge_delivery_outbox_attempts_finalize_idx
    ON edge_delivery_outbox_attempts (stream_id, lease_fence, item_id)
    WHERE resolution IS NOT NULL AND finalized_at IS NULL;

CREATE INDEX edge_delivery_outbox_attempts_evidence_deadline_idx
    ON edge_delivery_outbox_attempts (evidence_deadline, stream_id, item_id, lease_fence)
    WHERE resolution IS NULL AND finalized_at IS NULL;

CREATE INDEX edge_delivery_outbox_attempts_tombstone_cleanup_idx
    ON edge_delivery_outbox_attempts (finalized_at, item_id)
    WHERE finalized_at IS NOT NULL;

CREATE TABLE edge_delivery_outbox_attempt_targets (
    item_id bigint NOT NULL,
    lease_fence bigint NOT NULL,
    target_instance_id text NOT NULL CHECK (target_instance_id <> ''),
    target_user_id bigint NOT NULL CHECK (target_user_id > 0),
    batch_id bytea NOT NULL CHECK (octet_length(batch_id) = 16),
    command_id bytea NOT NULL CHECK (octet_length(command_id) = 16),
    evidence_kind varchar(32)
        CHECK (evidence_kind IS NULL OR evidence_kind IN ('edge_written', 'edge_no_eligible')),
    evidence_at timestamptz,
    eligible_sessions integer NOT NULL DEFAULT 0 CHECK (eligible_sessions >= 0),
    written_sessions integer NOT NULL DEFAULT 0 CHECK (written_sessions >= 0),
    physical_first_server_msg_id bigint NOT NULL DEFAULT 0,
    client_ack_at timestamptz,
    client_ack_auth_key_id bytea NOT NULL DEFAULT decode('0000000000000000', 'hex')
        CHECK (octet_length(client_ack_auth_key_id) = 8),
    client_ack_session_id bigint NOT NULL DEFAULT 0,
    client_ack_server_msg_id bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (item_id, lease_fence, target_instance_id, target_user_id, batch_id, command_id),
    UNIQUE (item_id, lease_fence, target_instance_id, target_user_id),
    CHECK (
        (evidence_kind IS NULL AND eligible_sessions = 0 AND written_sessions = 0
         AND physical_first_server_msg_id = 0)
        OR (evidence_kind = 'edge_no_eligible' AND eligible_sessions = 0 AND written_sessions = 0
            AND physical_first_server_msg_id = 0)
        OR (evidence_kind = 'edge_written' AND eligible_sessions > 0
            AND written_sessions = eligible_sessions AND physical_first_server_msg_id > 0)
    ),
    CHECK ((evidence_kind IS NULL) = (evidence_at IS NULL)),
    CHECK (
        (client_ack_at IS NULL
         AND client_ack_auth_key_id = decode('0000000000000000', 'hex')
         AND client_ack_session_id = 0 AND client_ack_server_msg_id = 0)
        OR
        (client_ack_at IS NOT NULL
         AND client_ack_auth_key_id <> decode('0000000000000000', 'hex')
         AND client_ack_session_id <> 0 AND client_ack_server_msg_id > 0)
    ),
    FOREIGN KEY (item_id, lease_fence)
        REFERENCES edge_delivery_outbox_attempts (item_id, lease_fence)
        ON DELETE CASCADE
);

CREATE INDEX edge_delivery_outbox_attempt_targets_pending_idx
    ON edge_delivery_outbox_attempt_targets (item_id, lease_fence)
    WHERE evidence_kind IS NULL;

CREATE FUNCTION reject_outbox_attempt_target_identity_update()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% target identity is immutable', TG_TABLE_NAME
        USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER dispatch_outbox_attempt_target_identity_immutable
BEFORE UPDATE OF item_id, lease_fence, target_instance_id, target_user_id, batch_id, command_id
ON dispatch_outbox_attempt_targets
FOR EACH ROW EXECUTE FUNCTION reject_outbox_attempt_target_identity_update();

CREATE TRIGGER edge_delivery_outbox_attempt_target_identity_immutable
BEFORE UPDATE OF item_id, lease_fence, target_instance_id, target_user_id, batch_id, command_id
ON edge_delivery_outbox_attempt_targets
FOR EACH ROW EXECUTE FUNCTION reject_outbox_attempt_target_identity_update();

-- Binding is the only target-ledger mutation window: targets are inserted
-- while the parent attempt is unbound, then the single parent UPDATE below
-- freezes and validates the authoritative count. Once bound, direct target
-- INSERT/DELETE is forbidden. A missing parent is accepted only for DELETE so
-- the attempt tombstone retention worker can use the FK cascade.
CREATE FUNCTION guard_outbox_attempt_target_membership()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    checked_item_id bigint;
    checked_fence bigint;
    parent_bound boolean;
BEGIN
    IF TG_OP = 'INSERT' THEN
        checked_item_id := NEW.item_id;
        checked_fence := NEW.lease_fence;
    ELSE
        checked_item_id := OLD.item_id;
        checked_fence := OLD.lease_fence;
    END IF;

    parent_bound := NULL;
    EXECUTE format(
        'SELECT targets_bound FROM %I WHERE item_id = $1 AND lease_fence = $2',
        TG_ARGV[0]
    ) INTO parent_bound USING checked_item_id, checked_fence;

    IF parent_bound IS NULL THEN
        IF TG_OP = 'DELETE' THEN
            RETURN OLD;
        END IF;
        RAISE EXCEPTION 'missing parent attempt for target item %, fence %',
            checked_item_id, checked_fence USING ERRCODE = '23503';
    END IF;
    IF parent_bound THEN
        RAISE EXCEPTION 'frozen target membership for item %, fence % is immutable',
            checked_item_id, checked_fence USING ERRCODE = '55000';
    END IF;
    IF TG_OP = 'INSERT' THEN
        RETURN NEW;
    END IF;
    RETURN OLD;
END;
$$;

CREATE TRIGGER dispatch_outbox_attempt_target_membership_guard
BEFORE INSERT OR DELETE ON dispatch_outbox_attempt_targets
FOR EACH ROW EXECUTE FUNCTION guard_outbox_attempt_target_membership(
    'dispatch_outbox_attempts'
);

CREATE TRIGGER edge_delivery_outbox_attempt_target_membership_guard
BEFORE INSERT OR DELETE ON edge_delivery_outbox_attempt_targets
FOR EACH ROW EXECUTE FUNCTION guard_outbox_attempt_target_membership(
    'edge_delivery_outbox_attempts'
);

CREATE FUNCTION validate_outbox_attempt_target_count()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    checked_item_id bigint;
    checked_fence bigint;
    is_bound boolean;
    expected_count integer;
    actual_count integer;
BEGIN
    checked_item_id := COALESCE(NEW.item_id, OLD.item_id);
    checked_fence := COALESCE(NEW.lease_fence, OLD.lease_fence);
    EXECUTE format(
        'SELECT a.targets_bound, a.target_count, '
        '       (SELECT count(*)::int FROM %I t WHERE t.item_id = a.item_id AND t.lease_fence = a.lease_fence) '
        'FROM %I a WHERE a.item_id = $1 AND a.lease_fence = $2',
        TG_ARGV[1], TG_ARGV[0]
    ) INTO is_bound, expected_count, actual_count
      USING checked_item_id, checked_fence;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;
    IF (NOT is_bound AND actual_count <> 0)
       OR (is_bound AND actual_count <> expected_count) THEN
        RAISE EXCEPTION 'frozen target count mismatch for item %, fence %: bound %, expected %, actual %',
            checked_item_id, checked_fence, is_bound, expected_count, actual_count
            USING ERRCODE = '23514';
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER dispatch_outbox_attempt_target_count_from_attempt_bind
AFTER UPDATE OF targets_bound, target_count ON dispatch_outbox_attempts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_outbox_attempt_target_count(
    'dispatch_outbox_attempts', 'dispatch_outbox_attempt_targets'
);

CREATE CONSTRAINT TRIGGER edge_delivery_outbox_attempt_target_count_from_attempt_bind
AFTER UPDATE OF targets_bound, target_count ON edge_delivery_outbox_attempts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_outbox_attempt_target_count(
    'edge_delivery_outbox_attempts', 'edge_delivery_outbox_attempt_targets'
);

-- Existing mutable claims have no trustworthy cross-process receipt. Reset
-- them to a ready lane and let the first post-cut claim issue a fresh fence.
INSERT INTO dispatch_outbox_lanes (
    stream_id, head_item_id, head_sequence, state, ready_at, last_error, updated_at
)
SELECT DISTINCT ON (d.target_user_id)
    d.target_user_id,
    d.id,
    d.pts,
	'ready',
    CASE WHEN d.status = 'pending' THEN d.next_attempt_at ELSE now() END,
	'',
    now()
FROM dispatch_outbox d
ORDER BY d.target_user_id, d.pts, d.id;

INSERT INTO edge_delivery_outbox_lanes (
    stream_id, head_item_id, head_sequence, state, ready_at, last_error, updated_at
)
SELECT DISTINCT ON (d.target_user_id)
    d.target_user_id,
    d.id,
    d.id,
	'ready',
    CASE WHEN d.status = 'pending' THEN d.next_attempt_at ELSE now() END,
	'',
    now()
FROM edge_delivery_outbox d
ORDER BY d.target_user_id, d.id;

ALTER TABLE dispatch_outbox
    DROP COLUMN status,
    DROP COLUMN attempts,
    DROP COLUMN next_attempt_at,
    DROP COLUMN last_error,
    DROP COLUMN updated_at;

ALTER TABLE edge_delivery_outbox
    ALTER COLUMN recovery_policy DROP DEFAULT;

-- Producers only install a missing empty->nonempty marker. The conflict path
-- never updates an active lane, so append traffic does not contend on head.
CREATE FUNCTION dispatch_outbox_insert_lane_marker()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO dispatch_outbox_lanes (stream_id, head_item_id, head_sequence)
    SELECT DISTINCT ON (target_user_id) target_user_id, id, pts
    FROM new_rows
    ORDER BY target_user_id, pts, id
    ON CONFLICT (stream_id) DO NOTHING;
    RETURN NULL;
END;
$$;

CREATE TRIGGER dispatch_outbox_insert_lane_marker
AFTER INSERT ON dispatch_outbox
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION dispatch_outbox_insert_lane_marker();

CREATE FUNCTION edge_delivery_outbox_insert_lane_marker()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO edge_delivery_outbox_lanes (stream_id, head_item_id, head_sequence)
    SELECT DISTINCT ON (target_user_id) target_user_id, id, id
    FROM new_rows
    ORDER BY target_user_id, id
    ON CONFLICT (stream_id) DO NOTHING;
    RETURN NULL;
END;
$$;

CREATE TRIGGER edge_delivery_outbox_insert_lane_marker
AFTER INSERT ON edge_delivery_outbox
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION edge_delivery_outbox_insert_lane_marker();

CREATE FUNCTION reject_outbox_item_update()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION '% items are immutable', TG_TABLE_NAME
        USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER dispatch_outbox_reject_update
BEFORE UPDATE ON dispatch_outbox
FOR EACH ROW EXECUTE FUNCTION reject_outbox_item_update();

CREATE TRIGGER edge_delivery_outbox_reject_update
BEFORE UPDATE ON edge_delivery_outbox
FOR EACH ROW EXECUTE FUNCTION reject_outbox_item_update();

ALTER TABLE edge_delivery_outbox
    DROP COLUMN status,
    DROP COLUMN attempts,
    DROP COLUMN logical_shard,
    DROP COLUMN next_attempt_at,
    DROP COLUMN sent_sessions,
    DROP COLUMN last_error,
    DROP COLUMN updated_at;

CREATE FUNCTION dispatch_outbox_lanes_notify_ready()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE ready_shard smallint;
BEGIN
    FOR ready_shard IN
        SELECT DISTINCT logical_shard
        FROM new_rows
        WHERE state = 'ready' AND ready_at <= now()
    LOOP
        PERFORM pg_notify('telesrv_dispatch_outbox_ready', ready_shard::text);
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER dispatch_outbox_lanes_insert_ready_notify
AFTER INSERT ON dispatch_outbox_lanes
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION dispatch_outbox_lanes_notify_ready();

CREATE TRIGGER dispatch_outbox_lanes_update_ready_notify
AFTER UPDATE ON dispatch_outbox_lanes
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION dispatch_outbox_lanes_notify_ready();

CREATE FUNCTION edge_delivery_outbox_lanes_notify_ready()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE ready_shard smallint;
BEGIN
    FOR ready_shard IN
        SELECT DISTINCT logical_shard
        FROM new_rows
        WHERE state = 'ready' AND ready_at <= now()
    LOOP
        PERFORM pg_notify('telesrv_edge_delivery_outbox_ready', ready_shard::text);
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER edge_delivery_outbox_lanes_insert_ready_notify
AFTER INSERT ON edge_delivery_outbox_lanes
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION edge_delivery_outbox_lanes_notify_ready();

CREATE TRIGGER edge_delivery_outbox_lanes_update_ready_notify
AFTER UPDATE ON edge_delivery_outbox_lanes
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION edge_delivery_outbox_lanes_notify_ready();

CREATE FUNCTION dispatch_outbox_attempts_notify_finalize()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE ready_shard smallint;
BEGIN
    FOR ready_shard IN
        SELECT DISTINCT l.logical_shard
        FROM new_rows a
        JOIN dispatch_outbox_lanes l ON l.stream_id = a.stream_id
        WHERE a.resolution IS NOT NULL AND a.finalized_at IS NULL
    LOOP
        PERFORM pg_notify('telesrv_dispatch_outbox_finalize', ready_shard::text);
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER dispatch_outbox_attempts_finalize_notify
AFTER UPDATE ON dispatch_outbox_attempts
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION dispatch_outbox_attempts_notify_finalize();

CREATE FUNCTION edge_delivery_outbox_attempts_notify_finalize()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE ready_shard smallint;
BEGIN
    FOR ready_shard IN
        SELECT DISTINCT l.logical_shard
        FROM new_rows a
        JOIN edge_delivery_outbox_lanes l ON l.stream_id = a.stream_id
        WHERE a.resolution IS NOT NULL AND a.finalized_at IS NULL
    LOOP
        PERFORM pg_notify('telesrv_edge_delivery_outbox_finalize', ready_shard::text);
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER edge_delivery_outbox_attempts_finalize_notify
AFTER UPDATE ON edge_delivery_outbox_attempts
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION edge_delivery_outbox_attempts_notify_finalize();

-- A claim freezes the short evidence horizon before any projection or target
-- bind. Wake schedulers immediately so they arm a DB-deadline timer even if the
-- claiming process dies before BindAttemptTargets.
CREATE FUNCTION dispatch_outbox_attempts_notify_deadline()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE ready_shard smallint;
BEGIN
    FOR ready_shard IN
        SELECT DISTINCT l.logical_shard
        FROM new_rows a
        JOIN dispatch_outbox_lanes l ON l.stream_id = a.stream_id
        WHERE a.resolution IS NULL AND a.finalized_at IS NULL
    LOOP
        PERFORM pg_notify('telesrv_dispatch_outbox_ready', ready_shard::text);
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER dispatch_outbox_attempts_deadline_notify
AFTER INSERT ON dispatch_outbox_attempts
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION dispatch_outbox_attempts_notify_deadline();

CREATE FUNCTION edge_delivery_outbox_attempts_notify_deadline()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE ready_shard smallint;
BEGIN
    FOR ready_shard IN
        SELECT DISTINCT l.logical_shard
        FROM new_rows a
        JOIN edge_delivery_outbox_lanes l ON l.stream_id = a.stream_id
        WHERE a.resolution IS NULL AND a.finalized_at IS NULL
    LOOP
        PERFORM pg_notify('telesrv_edge_delivery_outbox_ready', ready_shard::text);
    END LOOP;
    RETURN NULL;
END;
$$;

CREATE TRIGGER edge_delivery_outbox_attempts_deadline_notify
AFTER INSERT ON edge_delivery_outbox_attempts
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT EXECUTE FUNCTION edge_delivery_outbox_attempts_notify_deadline();
