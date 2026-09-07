-- This single-version hard cut is deliberately irreversible. Reconstructing
-- the removed mutable queue/head schema would revive an unsupported Egress
-- execution model and discard attempt evidence/fencing guarantees.
DO $$
BEGIN
    RAISE EXCEPTION USING
        MESSAGE = 'migration 0195 is irreversible; restore the coordinated database and binary backup',
        HINT = 'Restore the pre-0195 database and matching binaries as one unit; do not run an in-place schema downgrade.';
END
$$;
