CREATE TABLE IF NOT EXISTS account_send_restrictions (
	user_id bigint PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
	frozen boolean NOT NULL DEFAULT false,
	reason text NOT NULL DEFAULT '',
	actor text NOT NULL DEFAULT '',
	command_id text NOT NULL DEFAULT '',
	updated_at timestamptz NOT NULL DEFAULT now(),
	CONSTRAINT account_send_restrictions_user_id_check CHECK (user_id > 0)
);

CREATE INDEX IF NOT EXISTS account_send_restrictions_frozen_idx
	ON account_send_restrictions (user_id)
	WHERE frozen;

CREATE TABLE IF NOT EXISTS admin_commands (
	command_id text PRIMARY KEY,
	actor text NOT NULL,
	action varchar(96) NOT NULL,
	target_user_id bigint NOT NULL DEFAULT 0,
	target_peer_type varchar(16) NOT NULL DEFAULT '',
	target_peer_id bigint NOT NULL DEFAULT 0,
	dry_run boolean NOT NULL DEFAULT false,
	reason text NOT NULL,
	request jsonb NOT NULL DEFAULT '{}'::jsonb,
	result jsonb NOT NULL DEFAULT '{}'::jsonb,
	status varchar(16) NOT NULL DEFAULT 'running',
	error text NOT NULL DEFAULT '',
	created_at timestamptz NOT NULL DEFAULT now(),
	completed_at timestamptz,
	CONSTRAINT admin_commands_status_check CHECK (status IN ('running', 'completed', 'failed')),
	CONSTRAINT admin_commands_target_user_check CHECK (target_user_id >= 0),
	CONSTRAINT admin_commands_command_id_check CHECK (length(command_id) > 0 AND length(command_id) <= 128),
	CONSTRAINT admin_commands_actor_check CHECK (length(actor) > 0 AND length(actor) <= 128),
	CONSTRAINT admin_commands_reason_check CHECK (length(reason) > 0 AND length(reason) <= 1000)
);

CREATE INDEX IF NOT EXISTS admin_commands_target_idx
	ON admin_commands (target_user_id, action, created_at DESC);

CREATE TABLE IF NOT EXISTS admin_audit_logs (
	id bigserial PRIMARY KEY,
	command_id text NOT NULL UNIQUE REFERENCES admin_commands(command_id) ON DELETE RESTRICT,
	actor text NOT NULL,
	action varchar(96) NOT NULL,
	target_user_id bigint NOT NULL DEFAULT 0,
	target_peer_type varchar(16) NOT NULL DEFAULT '',
	target_peer_id bigint NOT NULL DEFAULT 0,
	dry_run boolean NOT NULL DEFAULT false,
	reason text NOT NULL,
	request jsonb NOT NULL DEFAULT '{}'::jsonb,
	result jsonb NOT NULL DEFAULT '{}'::jsonb,
	status varchar(16) NOT NULL,
	error text NOT NULL DEFAULT '',
	created_at timestamptz NOT NULL DEFAULT now(),
	CONSTRAINT admin_audit_logs_status_check CHECK (status IN ('running', 'completed', 'failed'))
);

CREATE INDEX IF NOT EXISTS admin_audit_logs_target_idx
	ON admin_audit_logs (target_user_id, id DESC);

CREATE INDEX IF NOT EXISTS admin_audit_logs_actor_idx
	ON admin_audit_logs (actor, id DESC);
