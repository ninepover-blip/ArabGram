ALTER TABLE edge_delivery_outbox_lanes
    DROP CONSTRAINT IF EXISTS edge_delivery_outbox_lanes_window_order_check;
ALTER TABLE dispatch_outbox_lanes
    DROP CONSTRAINT IF EXISTS dispatch_outbox_lanes_window_order_check;
