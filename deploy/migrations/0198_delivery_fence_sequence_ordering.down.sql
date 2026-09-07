-- Restoring backend-local fence caches would reintroduce cross-connection
-- fence reordering and delayed delivery. Roll back the coordinated database
-- and matching binaries instead of weakening the fencing invariant in place.
DO $$
BEGIN
    RAISE EXCEPTION USING
        MESSAGE = 'migration 0198 is irreversible; delivery fence sequences must remain CACHE 1',
        HINT = 'Restore a coordinated pre-0198 database and matching binaries as one unit';
END
$$;
