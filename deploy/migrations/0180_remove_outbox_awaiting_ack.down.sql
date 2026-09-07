ALTER TABLE edge_delivery_outbox
    DROP CONSTRAINT IF EXISTS edge_delivery_outbox_status_check;

ALTER TABLE edge_delivery_outbox
    ADD CONSTRAINT edge_delivery_outbox_status_check
    CHECK (status IN ('pending', 'dispatching', 'awaiting_ack', 'failed'));

CREATE INDEX IF NOT EXISTS edge_delivery_outbox_awaiting_ack_stale_idx
    ON edge_delivery_outbox (
        updated_at ASC,
        logical_shard ASC,
        target_user_id ASC,
        id ASC
    )
    WHERE status = 'awaiting_ack';

ALTER TABLE dispatch_outbox
    DROP CONSTRAINT IF EXISTS dispatch_outbox_status_check;

ALTER TABLE dispatch_outbox
    ADD CONSTRAINT dispatch_outbox_status_check
    CHECK (status IN ('pending', 'dispatching', 'awaiting_ack', 'failed'));

ALTER TABLE dispatch_outbox_user_heads
    DROP CONSTRAINT IF EXISTS dispatch_outbox_user_heads_status_check;

ALTER TABLE dispatch_outbox_user_heads
    ADD CONSTRAINT dispatch_outbox_user_heads_status_check
    CHECK (status IN ('pending', 'dispatching', 'awaiting_ack', 'failed'));

CREATE INDEX IF NOT EXISTS dispatch_outbox_user_heads_awaiting_ack_shard_idx
    ON dispatch_outbox_user_heads (
        updated_at ASC,
        logical_shard ASC,
        target_user_id ASC,
        head_pts ASC,
        head_id ASC
    )
    WHERE status = 'awaiting_ack';
