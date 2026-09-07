DROP TRIGGER IF EXISTS edge_delivery_outbox_delete_ready_notify ON edge_delivery_outbox;
DROP TRIGGER IF EXISTS edge_delivery_outbox_update_ready_notify ON edge_delivery_outbox;
DROP TRIGGER IF EXISTS edge_delivery_outbox_insert_ready_notify ON edge_delivery_outbox;
DROP FUNCTION IF EXISTS edge_delivery_outbox_notify_ready();

DROP INDEX IF EXISTS edge_delivery_outbox_failed_cleanup_idx;
DROP INDEX IF EXISTS edge_delivery_outbox_awaiting_ack_stale_idx;
DROP INDEX IF EXISTS edge_delivery_outbox_dispatching_stale_idx;
DROP INDEX IF EXISTS edge_delivery_outbox_pending_shard_idx;
DROP INDEX IF EXISTS edge_delivery_outbox_target_user_id_id_key;

DROP TABLE IF EXISTS edge_delivery_outbox;
