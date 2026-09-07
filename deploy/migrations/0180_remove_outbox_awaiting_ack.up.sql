-- Remove the obsolete client-ACK waiting state from both durable online queues.
-- Client msgs_ack is only a late indeterminate-write signal; service-level Edge
-- confirmation is enough to complete online outbox rows.

UPDATE dispatch_outbox
SET status = 'dispatching',
    next_attempt_at = now(),
    updated_at = now() - interval '1 hour'
WHERE status = 'awaiting_ack';

UPDATE dispatch_outbox_user_heads
SET status = 'dispatching',
    next_attempt_at = now(),
    updated_at = now() - interval '1 hour'
WHERE status = 'awaiting_ack';

UPDATE edge_delivery_outbox
SET status = 'dispatching',
    next_attempt_at = now(),
    sent_sessions = 0,
    updated_at = now() - interval '1 hour'
WHERE status = 'awaiting_ack';

DROP INDEX IF EXISTS dispatch_outbox_user_heads_awaiting_ack_shard_idx;
DROP INDEX IF EXISTS edge_delivery_outbox_awaiting_ack_stale_idx;

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

ALTER TABLE edge_delivery_outbox
    DROP CONSTRAINT IF EXISTS edge_delivery_outbox_status_check;

ALTER TABLE edge_delivery_outbox
    ADD CONSTRAINT edge_delivery_outbox_status_check
    CHECK (status IN ('pending', 'dispatching', 'failed'));
