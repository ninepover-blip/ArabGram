-- Passkey(WebAuthn/FIDO2)凭据。每条 = 一个已注册的公钥。sign_count 用 bigint
-- 容纳 uint32;credential_id 为主键(原始字节,对外 base64url)。
CREATE TABLE public.passkey_credentials (
    credential_id bytea NOT NULL,
    user_id bigint NOT NULL,
    public_key bytea NOT NULL,
    sign_count bigint DEFAULT 0 NOT NULL,
    aaguid bytea DEFAULT '\x'::bytea NOT NULL,
    name character varying(128) DEFAULT ''::character varying NOT NULL,
    transports text[] DEFAULT '{}'::text[] NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    last_used_at timestamp with time zone,
    CONSTRAINT passkey_credentials_pkey PRIMARY KEY (credential_id),
    CONSTRAINT passkey_credentials_user_id_fkey FOREIGN KEY (user_id)
        REFERENCES public.users(id) ON DELETE CASCADE
);

CREATE INDEX passkey_credentials_user_id_idx ON public.passkey_credentials (user_id);
