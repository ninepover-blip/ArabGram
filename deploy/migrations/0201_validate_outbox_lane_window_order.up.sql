ALTER TABLE dispatch_outbox_lanes
    VALIDATE CONSTRAINT dispatch_outbox_lanes_window_order_check;
ALTER TABLE edge_delivery_outbox_lanes
    VALIDATE CONSTRAINT edge_delivery_outbox_lanes_window_order_check;
