-- Lane readiness is durable in the lane rows themselves. High-frequency
-- transactional NOTIFY serializes otherwise independent Core/Egress commits
-- on PostgreSQL's database-object lock, so the fixed Egress schedule actors
-- now discover ready heads and deadlines directly from those rows.

DROP TRIGGER IF EXISTS dispatch_outbox_lanes_insert_ready_notify
    ON public.dispatch_outbox_lanes;
DROP TRIGGER IF EXISTS dispatch_outbox_lanes_update_ready_notify
    ON public.dispatch_outbox_lanes;
DROP FUNCTION IF EXISTS public.dispatch_outbox_lanes_notify_ready();

DROP TRIGGER IF EXISTS edge_delivery_outbox_lanes_insert_ready_notify
    ON public.edge_delivery_outbox_lanes;
DROP TRIGGER IF EXISTS edge_delivery_outbox_lanes_update_ready_notify
    ON public.edge_delivery_outbox_lanes;
DROP FUNCTION IF EXISTS public.edge_delivery_outbox_lanes_notify_ready();

DROP TRIGGER IF EXISTS channel_delivery_lanes_insert_ready_notify
    ON public.channel_delivery_lanes;
DROP TRIGGER IF EXISTS channel_delivery_lanes_update_ready_notify
    ON public.channel_delivery_lanes;
DROP FUNCTION IF EXISTS public.channel_delivery_lanes_notify_ready();
