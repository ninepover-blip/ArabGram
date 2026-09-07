DROP TRIGGER IF EXISTS collectible_phones_user_read_model_changed ON public.collectible_phones;
DROP TRIGGER IF EXISTS users_projection_fact_versions_changed ON public.users;
DROP FUNCTION IF EXISTS public.telesrv_maintain_user_projection_fact_versions();
DROP FUNCTION IF EXISTS public.telesrv_notify_user_collectible_phone_row();
DROP FUNCTION IF EXISTS public.telesrv_bump_user_collectible_phone(bigint);
DELETE FROM public.read_model_versions WHERE model = 'user_collectible_phone';
