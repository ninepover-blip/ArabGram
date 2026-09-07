-- Add a durable waiting-for-client-ACK state for online outbox delivery.
-- Egress moves a physically written online update to awaiting_ack; Edge
-- client msgs_ack deletes the same fenced attempt. If Edge/Egress/ACK writer
-- crashes before ACK completion, claim treats stale awaiting_ack heads like
-- stale dispatching leases and re-dispatches them.

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

CREATE INDEX dispatch_outbox_user_heads_awaiting_ack_shard_idx
    ON dispatch_outbox_user_heads (
        updated_at ASC,
        logical_shard ASC,
        target_user_id ASC,
        head_pts ASC,
        head_id ASC
    )
    WHERE status = 'awaiting_ack';
