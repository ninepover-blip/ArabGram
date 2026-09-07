-- Delivery attempts are live fencing state, not a 24-hour audit log.  This is
-- a stopped-world hard cut: an old binary must drain every open attempt before
-- the schema discards finalized diagnostic tombstones.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM public.dispatch_outbox_attempts WHERE finalized_at IS NULL)
     OR EXISTS (SELECT 1 FROM public.edge_delivery_outbox_attempts WHERE finalized_at IS NULL)
     OR EXISTS (SELECT 1 FROM public.channel_delivery_attempts WHERE finalized_at IS NULL) THEN
    RAISE EXCEPTION '0213 live-attempt cutover requires all open delivery attempts to be drained';
  END IF;
END
$$;

ALTER TABLE public.dispatch_outbox_lanes
  ADD COLUMN head_attempt integer NOT NULL DEFAULT 0,
  ADD CONSTRAINT dispatch_outbox_lanes_head_attempt_check CHECK (head_attempt >= 0);

ALTER TABLE public.edge_delivery_outbox_lanes
  ADD COLUMN head_attempt integer NOT NULL DEFAULT 0,
  ADD CONSTRAINT edge_delivery_outbox_lanes_head_attempt_check CHECK (head_attempt >= 0);

-- channel_delivery_lanes.retry_count already provides the same mutable
-- per-head counter. Immutable outbox/event rows remain byte-for-byte immutable.

UPDATE public.dispatch_outbox_lanes l
SET head_attempt = COALESCE((
  SELECT max(a.attempt)
  FROM public.dispatch_outbox_attempts a
  WHERE a.item_id = l.head_item_id
), 0);

UPDATE public.edge_delivery_outbox_lanes l
SET head_attempt = COALESCE((
  SELECT max(a.attempt)
  FROM public.edge_delivery_outbox_attempts a
  WHERE a.item_id = l.head_item_id
), 0);

TRUNCATE TABLE
  public.dispatch_outbox_attempt_targets,
  public.edge_delivery_outbox_attempt_targets,
  public.channel_delivery_attempt_targets,
  public.dispatch_outbox_attempts,
  public.edge_delivery_outbox_attempts,
  public.channel_delivery_attempts;

DROP INDEX public.dispatch_outbox_attempts_finalize_idx;
DROP INDEX public.dispatch_outbox_attempts_evidence_deadline_idx;
DROP INDEX public.dispatch_outbox_attempts_tombstone_cleanup_idx;
DROP INDEX public.dispatch_outbox_attempts_open_fence_idx;
ALTER TABLE public.dispatch_outbox_attempts
  DROP CONSTRAINT dispatch_outbox_attempts_item_id_attempt_key,
  DROP CONSTRAINT dispatch_outbox_attempts_check6,
  DROP CONSTRAINT dispatch_outbox_attempts_check7,
  DROP CONSTRAINT dispatch_outbox_attempts_finalization_outcome_check,
  DROP COLUMN finalized_at,
  DROP COLUMN finalization_outcome;
CREATE INDEX dispatch_outbox_attempts_finalize_idx
  ON public.dispatch_outbox_attempts (stream_id, lease_fence, sequence, item_id)
  WHERE resolution IS NOT NULL;
CREATE INDEX dispatch_outbox_attempts_evidence_deadline_idx
  ON public.dispatch_outbox_attempts (evidence_deadline, stream_id, item_id, lease_fence)
  WHERE resolution IS NULL;
CREATE INDEX dispatch_outbox_attempts_fence_idx
  ON public.dispatch_outbox_attempts (stream_id, lease_fence, item_id);

DROP INDEX public.edge_delivery_outbox_attempts_finalize_idx;
DROP INDEX public.edge_delivery_outbox_attempts_evidence_deadline_idx;
DROP INDEX public.edge_delivery_outbox_attempts_tombstone_cleanup_idx;
DROP INDEX public.edge_delivery_outbox_attempts_open_fence_idx;
ALTER TABLE public.edge_delivery_outbox_attempts
  DROP CONSTRAINT edge_delivery_outbox_attempts_item_id_attempt_key,
  DROP CONSTRAINT edge_delivery_outbox_attempts_check7,
  DROP CONSTRAINT edge_delivery_outbox_attempts_check8,
  DROP CONSTRAINT edge_delivery_outbox_attempts_finalization_outcome_check,
  DROP COLUMN finalized_at,
  DROP COLUMN finalization_outcome;
CREATE INDEX edge_delivery_outbox_attempts_finalize_idx
  ON public.edge_delivery_outbox_attempts (stream_id, lease_fence, item_id)
  WHERE resolution IS NOT NULL;
CREATE INDEX edge_delivery_outbox_attempts_evidence_deadline_idx
  ON public.edge_delivery_outbox_attempts (evidence_deadline, stream_id, item_id, lease_fence)
  WHERE resolution IS NULL;
CREATE INDEX edge_delivery_outbox_attempts_fence_idx
  ON public.edge_delivery_outbox_attempts (stream_id, lease_fence, item_id);

DROP INDEX public.channel_delivery_attempts_finalize_idx;
DROP INDEX public.channel_delivery_attempts_evidence_deadline_idx;
DROP INDEX public.channel_delivery_attempts_tombstone_cleanup_idx;
DROP INDEX public.channel_delivery_attempts_open_fence_idx;
ALTER TABLE public.channel_delivery_attempts
  DROP CONSTRAINT channel_delivery_attempts_item_attempt_key,
  DROP CONSTRAINT channel_delivery_attempts_finalized_check,
  DROP COLUMN finalized_at,
  DROP COLUMN final_outcome;
CREATE INDEX channel_delivery_attempts_finalize_idx
  ON public.channel_delivery_attempts (channel_id, max_pts, item_id, lease_fence)
  WHERE resolution <> 'pending';
CREATE INDEX channel_delivery_attempts_evidence_deadline_idx
  ON public.channel_delivery_attempts (evidence_deadline, channel_id, item_id, lease_fence)
  WHERE resolution = 'pending';
CREATE INDEX channel_delivery_attempts_fence_idx
  ON public.channel_delivery_attempts (channel_id, lease_fence, item_id);

ALTER TABLE public.dispatch_outbox_attempt_targets
  DROP CONSTRAINT dispatch_outbox_attempt_targets_check2,
  DROP CONSTRAINT dispatch_outbox_attempt_targets_client_ack_auth_key_id_check,
  DROP COLUMN client_ack_at,
  DROP COLUMN client_ack_auth_key_id,
  DROP COLUMN client_ack_session_id,
  DROP COLUMN client_ack_server_msg_id;

ALTER TABLE public.edge_delivery_outbox_attempt_targets
  DROP CONSTRAINT edge_delivery_outbox_attempt_targets_check2,
  DROP CONSTRAINT edge_delivery_outbox_attempt_targe_client_ack_auth_key_id_check,
  DROP COLUMN client_ack_at,
  DROP COLUMN client_ack_auth_key_id,
  DROP COLUMN client_ack_session_id,
  DROP COLUMN client_ack_server_msg_id;

ALTER TABLE public.channel_delivery_attempt_targets
  DROP CONSTRAINT channel_delivery_attempt_targets_auth_key_id_check,
  DROP CONSTRAINT channel_delivery_attempt_targets_client_ack_check,
  DROP COLUMN client_ack_auth_key_id,
  DROP COLUMN client_ack_session_id,
  DROP COLUMN client_ack_server_msg_id,
  DROP COLUMN client_ack_at;
