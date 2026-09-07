CREATE CONSTRAINT TRIGGER dispatch_outbox_attempt_target_count_from_attempt_bind
AFTER UPDATE OF targets_bound, target_count ON public.dispatch_outbox_attempts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.validate_outbox_attempt_target_count(
    'dispatch_outbox_attempts', 'dispatch_outbox_attempt_targets'
);

CREATE CONSTRAINT TRIGGER edge_delivery_outbox_attempt_target_count_from_attempt_bind
AFTER UPDATE OF targets_bound, target_count ON public.edge_delivery_outbox_attempts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.validate_outbox_attempt_target_count(
    'edge_delivery_outbox_attempts', 'edge_delivery_outbox_attempt_targets'
);

CREATE CONSTRAINT TRIGGER channel_delivery_attempt_target_count_from_attempt_bind
AFTER UPDATE OF targets_bound, target_count ON public.channel_delivery_attempts
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.validate_outbox_attempt_target_count(
    'channel_delivery_attempts', 'channel_delivery_attempt_targets'
);
