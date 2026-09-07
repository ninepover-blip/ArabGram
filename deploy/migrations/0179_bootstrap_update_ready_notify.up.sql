CREATE FUNCTION bootstrap_update_jobs_notify_ready()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.status = 'ready'
       AND COALESCE(NEW.ready_at, NEW.created_at) <= now() THEN
        IF TG_OP = 'INSERT' THEN
            PERFORM pg_notify('telesrv_bootstrap_update_ready', NEW.user_id::text);
        ELSIF TG_OP = 'UPDATE'
              AND (OLD.status IS DISTINCT FROM NEW.status
                   OR OLD.ready_at IS DISTINCT FROM NEW.ready_at) THEN
            PERFORM pg_notify('telesrv_bootstrap_update_ready', NEW.user_id::text);
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER bootstrap_update_jobs_insert_ready_notify
AFTER INSERT ON bootstrap_update_jobs
FOR EACH ROW
EXECUTE FUNCTION bootstrap_update_jobs_notify_ready();

CREATE TRIGGER bootstrap_update_jobs_update_ready_notify
AFTER UPDATE OF status, ready_at ON bootstrap_update_jobs
FOR EACH ROW
EXECUTE FUNCTION bootstrap_update_jobs_notify_ready();
