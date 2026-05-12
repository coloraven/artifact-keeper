-- Validate the composite FK added NOT VALID in migration 087 (#1188-R3).
--
-- Lives in its own file because sqlx 0.8 wraps each migration in a
-- single transaction. Keeping VALIDATE in 087 would force the brief
-- SHARE ROW EXCLUSIVE lock from the NOT VALID add to be held for the
-- entire VALIDATE scan (3-8 minutes on a 30M-row scan_packages), which
-- blocks DML. Splitting the steps lets 087 commit immediately and lets
-- 088 acquire only the lighter SHARE UPDATE EXCLUSIVE lock.
--
-- VALIDATE CONSTRAINT semantics:
--   * Acquires SHARE UPDATE EXCLUSIVE on scan_packages (and ROW SHARE
--     on the referenced scan_results). Reads and writes proceed; only
--     other DDL / VALIDATE on the table queues.
--   * On a 30M-row scan_packages with ~100k probes/sec into the
--     scan_results PK, expect ~3-8 minutes wall time on commodity SSD.
--   * If a drifted row exists (a scan_packages row whose artifact_id
--     does not match its parent scan_results.artifact_id), this
--     migration errors out and the deploy stops. That is the desired
--     "fail loudly on inconsistent data" behaviour from PR #1188.
--   * Idempotent: VALIDATE CONSTRAINT on an already-validated
--     constraint is a documented Postgres no-op (checks the
--     pg_constraint.convalidated flag).

ALTER TABLE scan_packages
    VALIDATE CONSTRAINT scan_packages_scan_result_artifact_fk;
