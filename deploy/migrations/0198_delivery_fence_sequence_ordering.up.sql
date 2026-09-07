-- Edge keeps a monotonic fence high-water mark per delivery domain. PostgreSQL
-- sequence caches are backend-local, so CACHE > 1 can return a lower value from
-- one pooled connection after another connection has returned a higher value.
-- Correct already-deployed v3 schemas in place; this changes no durable item,
-- attempt, event, watermark, or PTS fact.
ALTER SEQUENCE public.dispatch_outbox_lease_fence_seq CACHE 1;
ALTER SEQUENCE public.edge_delivery_outbox_lease_fence_seq CACHE 1;
ALTER SEQUENCE public.channel_delivery_lease_fence_seq CACHE 1;
