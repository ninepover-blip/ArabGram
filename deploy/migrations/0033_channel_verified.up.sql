ALTER TABLE public.channels
	ADD COLUMN IF NOT EXISTS verified boolean DEFAULT false NOT NULL;

CREATE INDEX IF NOT EXISTS admin_audit_logs_peer_idx
	ON public.admin_audit_logs (target_peer_type, target_peer_id, id DESC);
