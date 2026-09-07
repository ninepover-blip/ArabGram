-- Claim supersedes only the previous still-open fence. Finalized attempt
-- tombstones are retained for late receipt classification and can outnumber
-- open attempts by several orders of magnitude, so they must never be scanned
-- by this hot lookup.
CREATE INDEX dispatch_outbox_attempts_open_fence_idx
    ON public.dispatch_outbox_attempts (stream_id, lease_fence, item_id)
    WHERE finalized_at IS NULL;

CREATE INDEX edge_delivery_outbox_attempts_open_fence_idx
    ON public.edge_delivery_outbox_attempts (stream_id, lease_fence, item_id)
    WHERE finalized_at IS NULL;

CREATE INDEX channel_delivery_attempts_open_fence_idx
    ON public.channel_delivery_attempts (channel_id, lease_fence, item_id)
    WHERE finalized_at IS NULL;
