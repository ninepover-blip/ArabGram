DROP TRIGGER IF EXISTS dispatch_outbox_delete_ready_notify ON dispatch_outbox;
DROP TRIGGER IF EXISTS dispatch_outbox_update_ready_notify ON dispatch_outbox;
DROP TRIGGER IF EXISTS dispatch_outbox_insert_ready_notify ON dispatch_outbox;
DROP FUNCTION IF EXISTS dispatch_outbox_notify_ready_heads();
