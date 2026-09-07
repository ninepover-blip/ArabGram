DROP INDEX IF EXISTS public.admin_audit_logs_peer_idx;

ALTER TABLE public.channels
	DROP COLUMN IF EXISTS verified;
