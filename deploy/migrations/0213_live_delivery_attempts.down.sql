DO $$
BEGIN
  RAISE EXCEPTION '0213 live-attempt hard cut is not reversible; restore the pre-cutover database backup';
END
$$;
