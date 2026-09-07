-- Business automation is a low-frequency account fact but its negative check is
-- on every ordinary private send. Keep PostgreSQL as the cold-build truth while
-- moving exact cross-Core invalidation through the existing low-frequency
-- PostgreSQL notification path. It must not contaminate the high-frequency
-- dialog relay's partial-index claim.

CREATE OR REPLACE FUNCTION public.telesrv_notify_business_automation_read_model()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    owner_id bigint;
BEGIN
    IF TG_TABLE_NAME = 'user_business_profiles' THEN
        owner_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.user_id ELSE NEW.user_id END;
    ELSE
        owner_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.owner_user_id ELSE NEW.owner_user_id END;
    END IF;
    PERFORM public.telesrv_bump_read_model_version(
        'business_automation', owner_id, 'user', owner_id
    );
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$;

DROP TRIGGER IF EXISTS user_business_profiles_automation_read_model_changed ON public.user_business_profiles;
CREATE TRIGGER user_business_profiles_automation_read_model_changed
AFTER INSERT OR UPDATE OR DELETE ON public.user_business_profiles
FOR EACH ROW
EXECUTE FUNCTION public.telesrv_notify_business_automation_read_model();

DROP TRIGGER IF EXISTS business_connected_bots_automation_read_model_changed ON public.business_connected_bots;
CREATE TRIGGER business_connected_bots_automation_read_model_changed
AFTER INSERT OR UPDATE OR DELETE ON public.business_connected_bots
FOR EACH ROW
EXECUTE FUNCTION public.telesrv_notify_business_automation_read_model();

DROP INDEX IF EXISTS public.read_model_versions_business_automation_publish_idx;
DROP FUNCTION IF EXISTS public.telesrv_bump_business_automation(bigint);
