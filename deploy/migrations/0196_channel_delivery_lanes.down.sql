DO $$
BEGIN
    RAISE EXCEPTION 'migration 0196 is irreversible; restore a coordinated database and binary backup instead of recreating the removed Core channel delivery mode';
END;
$$;
