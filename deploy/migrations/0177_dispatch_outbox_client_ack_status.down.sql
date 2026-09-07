DROP INDEX IF EXISTS dispatch_outbox_user_heads_awaiting_ack_shard_idx;

UPDATE dispatch_outbox
SET status = 'pending',
    next_attempt_at = now(),
    updated_at = now()
WHERE status = 'awaiting_ack';

ALTER TABLE dispatch_outbox
    DROP CONSTRAINT IF EXISTS dispatch_outbox_status_check;

ALTER TABLE dispatch_outbox
    ADD CONSTRAINT dispatch_outbox_status_check
    CHECK (status IN ('pending', 'dispatching', 'failed'));

ALTER TABLE dispatch_outbox_user_heads
    DROP CONSTRAINT IF EXISTS dispatch_outbox_user_heads_status_check;

ALTER TABLE dispatch_outbox_user_heads
    ADD CONSTRAINT dispatch_outbox_user_heads_status_check
    CHECK (status IN ('pending', 'dispatching', 'failed'));
