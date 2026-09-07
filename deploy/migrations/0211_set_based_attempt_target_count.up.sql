-- BindAttemptTargets is the sole production membership writer. It inserts the
-- complete target set and freezes the parent count in one data-modifying CTE;
-- any row or constraint failure rolls the entire statement back. The existing
-- membership and identity guards continue to reject all post-bind mutation.
-- Re-counting every parent from a deferred row trigger therefore duplicates
-- the same invariant and adds one correlated SELECT per high-rate attempt.
DROP TRIGGER IF EXISTS dispatch_outbox_attempt_target_count_from_attempt_bind
    ON public.dispatch_outbox_attempts;
DROP TRIGGER IF EXISTS edge_delivery_outbox_attempt_target_count_from_attempt_bind
    ON public.edge_delivery_outbox_attempts;
DROP TRIGGER IF EXISTS channel_delivery_attempt_target_count_from_attempt_bind
    ON public.channel_delivery_attempts;
