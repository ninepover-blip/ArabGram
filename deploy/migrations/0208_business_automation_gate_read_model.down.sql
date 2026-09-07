DROP TRIGGER IF EXISTS business_connected_bots_automation_read_model_changed ON public.business_connected_bots;
DROP TRIGGER IF EXISTS user_business_profiles_automation_read_model_changed ON public.user_business_profiles;
DROP FUNCTION IF EXISTS public.telesrv_notify_business_automation_read_model();
DROP INDEX IF EXISTS public.read_model_versions_business_automation_publish_idx;
DROP FUNCTION IF EXISTS public.telesrv_bump_business_automation(bigint);
DELETE FROM public.read_model_versions WHERE model = 'business_automation';
