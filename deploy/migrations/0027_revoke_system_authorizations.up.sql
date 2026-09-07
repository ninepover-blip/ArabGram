WITH target AS MATERIALIZED (
	SELECT auth_key_id
	FROM public.authorizations
	WHERE user_id IN (777000, 93372553)
), target_temp AS MATERIALIZED (
	SELECT temp_auth_key_id
	FROM public.temp_auth_key_bindings
	WHERE perm_auth_key_id IN (SELECT auth_key_id FROM target)
), deleted_update_states AS (
	DELETE FROM public.update_states
	WHERE user_id IN (777000, 93372553)
	   OR auth_key_id IN (SELECT auth_key_id FROM target)
	   OR auth_key_id IN (SELECT temp_auth_key_id FROM target_temp)
	RETURNING auth_key_id
), deleted_temp_bindings AS (
	DELETE FROM public.temp_auth_key_bindings
	WHERE perm_auth_key_id IN (SELECT auth_key_id FROM target)
	   OR temp_auth_key_id IN (SELECT temp_auth_key_id FROM target_temp)
	RETURNING temp_auth_key_id
), deleted_temp_keys AS (
	DELETE FROM public.auth_keys
	WHERE auth_key_id IN (SELECT temp_auth_key_id FROM target_temp)
	RETURNING auth_key_id
)
DELETE FROM public.auth_keys
WHERE auth_key_id IN (SELECT auth_key_id FROM target);
