-- Attempt bind/evidence is a high-rate Egress-owned mutation. Its PostgreSQL
-- row remains authoritative, but transaction-level pg_notify serializes every
-- committing writer on the database object lock. Egress now transfers exact
-- committed attempt refs from its bounded mutation actors to bounded
-- finalization actors; process-start recovery reconstructs refs from these
-- authoritative rows after a crash.
--
-- New durable heads and lane ready transitions keep their transactional
-- notifications; only attempt INSERT/UPDATE notification triggers are removed.
DROP TRIGGER IF EXISTS dispatch_outbox_attempts_finalize_notify ON public.dispatch_outbox_attempts;
DROP TRIGGER IF EXISTS dispatch_outbox_attempts_deadline_notify ON public.dispatch_outbox_attempts;
DROP FUNCTION IF EXISTS public.dispatch_outbox_attempts_notify_finalize();
DROP FUNCTION IF EXISTS public.dispatch_outbox_attempts_notify_deadline();

DROP TRIGGER IF EXISTS edge_delivery_outbox_attempts_finalize_notify ON public.edge_delivery_outbox_attempts;
DROP TRIGGER IF EXISTS edge_delivery_outbox_attempts_deadline_notify ON public.edge_delivery_outbox_attempts;
DROP FUNCTION IF EXISTS public.edge_delivery_outbox_attempts_notify_finalize();
DROP FUNCTION IF EXISTS public.edge_delivery_outbox_attempts_notify_deadline();

DROP TRIGGER IF EXISTS channel_delivery_attempts_finalize_notify ON public.channel_delivery_attempts;
DROP TRIGGER IF EXISTS channel_delivery_attempts_deadline_notify ON public.channel_delivery_attempts;
DROP FUNCTION IF EXISTS public.channel_delivery_attempts_notify_finalize();
DROP FUNCTION IF EXISTS public.channel_delivery_attempts_notify_deadline();
