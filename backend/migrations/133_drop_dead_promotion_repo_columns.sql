-- Drop the dead repositories.promotion_target_id / promotion_policy_id columns
-- (added by migration 047). They were never written by any code path and never
-- read for any logic: promotion resolves its release target from the
-- repository_config key 'release_repository_id' and its security policy per
-- repository_id via scan_policies/license_policies (migration 048). Every row
-- holds NULL in both columns, so there is no data impact.
--
-- promotion_target_id carries a self-FK to repositories(id) (recreated by
-- migration 125 as ON DELETE SET NULL). Drop that FK first, NAME-AGNOSTICALLY
-- (same pattern as migration 125), then drop both columns. Idempotent.

DO $$
DECLARE
    col text;
    fk  RECORD;
BEGIN
    FOREACH col IN ARRAY ARRAY['promotion_target_id', 'promotion_policy_id']
    LOOP
        -- Skip if the column is already gone (re-run / fresh database).
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'repositories'
              AND column_name = col
        ) THEN
            CONTINUE;
        END IF;

        -- Drop every single-column FK on this column, whatever it is named.
        FOR fk IN
            SELECT con.conname
            FROM pg_constraint con
            JOIN pg_attribute att
              ON att.attrelid = con.conrelid AND att.attnum = ANY (con.conkey)
            WHERE con.conrelid = 'public.repositories'::regclass
              AND con.contype = 'f'
              AND att.attname = col
              AND array_length(con.conkey, 1) = 1
        LOOP
            EXECUTE format('ALTER TABLE repositories DROP CONSTRAINT %I', fk.conname);
        END LOOP;

        EXECUTE format('ALTER TABLE repositories DROP COLUMN IF EXISTS %I', col);
    END LOOP;
END $$;
