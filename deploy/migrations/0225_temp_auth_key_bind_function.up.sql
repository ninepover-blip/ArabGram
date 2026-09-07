-- Collapse the identity-sensitive auth.bindTempAuthKey write boundary into one
-- database call. PostgreSQL remains the canonical temp -> permanent identity
-- store, while v2 keeps protocol-Layer identity/default state in Redis. This
-- function therefore never reads or writes PostgreSQL Layer mirrors.

CREATE OR REPLACE FUNCTION public.telesrv_bind_temp_auth_key(
    p_temp_auth_key_id bigint,
    p_perm_auth_key_id bigint,
    p_nonce bigint,
    p_temp_session_id bigint,
    p_expires_at integer,
    p_encrypted_message bytea
)
RETURNS TABLE (
    bind_status text,
    effective_layer integer,
    effective_observation_id bigint
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_temp_expiry integer;
    v_current_perm_id bigint;
    v_perm_expiry integer;
    v_updated integer;
BEGIN
    IF p_expires_at <= 0 OR p_temp_auth_key_id = p_perm_auth_key_id THEN
        RETURN QUERY SELECT 'binding_invalid'::text, 0, 0::bigint;
        RETURN;
    END IF;

    PERFORM pg_advisory_xact_lock(
        1096111176::integer,
        hashint8(p_perm_auth_key_id)::integer
    );

    SELECT key.expires_at
    INTO v_temp_expiry
    FROM public.auth_keys AS key
    WHERE key.auth_key_id = p_temp_auth_key_id
    FOR UPDATE;
    IF NOT FOUND OR v_temp_expiry <= 0 OR v_temp_expiry <> p_expires_at THEN
        RETURN QUERY SELECT 'binding_invalid'::text, 0, 0::bigint;
        RETURN;
    END IF;

    SELECT binding.perm_auth_key_id
    INTO v_current_perm_id
    FROM public.temp_auth_key_bindings AS binding
    WHERE binding.temp_auth_key_id = p_temp_auth_key_id;
    IF FOUND AND v_current_perm_id <> p_perm_auth_key_id THEN
        RETURN QUERY SELECT 'already_bound'::text, 0, 0::bigint;
        RETURN;
    END IF;

    SELECT key.expires_at
    INTO v_perm_expiry
    FROM public.auth_keys AS key
    WHERE key.auth_key_id = p_perm_auth_key_id
    FOR UPDATE;
    IF NOT FOUND OR v_perm_expiry <> 0 THEN
        RETURN QUERY SELECT 'binding_invalid'::text, 0, 0::bigint;
        RETURN;
    END IF;

    INSERT INTO public.temp_auth_key_bindings (
        temp_auth_key_id,
        perm_auth_key_id,
        nonce,
        temp_session_id,
        expires_at,
        encrypted_message
    ) VALUES (
        p_temp_auth_key_id,
        p_perm_auth_key_id,
        p_nonce,
        p_temp_session_id,
        p_expires_at,
        p_encrypted_message
    )
    ON CONFLICT (temp_auth_key_id) DO UPDATE SET
        nonce = EXCLUDED.nonce,
        temp_session_id = EXCLUDED.temp_session_id,
        expires_at = EXCLUDED.expires_at,
        encrypted_message = EXCLUDED.encrypted_message,
        created_at = now()
    WHERE public.temp_auth_key_bindings.perm_auth_key_id = EXCLUDED.perm_auth_key_id;
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    IF v_updated <> 1 THEN
        RETURN QUERY SELECT 'binding_invalid'::text, 0, 0::bigint;
        RETURN;
    END IF;

    -- The zero tuple is intentional: after this transaction commits, the v2
    -- service boundary atomically merges the two protocol identities in Redis
    -- and returns Redis' effective Layer tuple to the Router.
    RETURN QUERY SELECT 'ok'::text, 0, 0::bigint;
END;
$$;

COMMENT ON FUNCTION public.telesrv_bind_temp_auth_key(
    bigint, bigint, bigint, bigint, integer, bytea
) IS 'Atomically binds a temporary auth key while preserving v2 PostgreSQL identity lock order; protocol Layer state remains in Redis';
