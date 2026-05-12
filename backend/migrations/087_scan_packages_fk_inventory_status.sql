-- Hardening pass on the scan_packages / scan_results schema (#1154, #1157).
--
-- Two independent constraints land in the same file because they touch the
-- same two tables and ship in the same release, keeping the migration
-- numbering tight. Each section is self-contained and idempotent.
--
-- DEPLOY ORDERING (review #1188-R2 SRE):
--   * Migrate forward first, THEN roll the binary forward. The new code
--     paths (set_inventory_status, record_scan_inventory_success/failure)
--     reference columns added here; running a post-#1188 binary against
--     a pre-087 schema crashes scan persistence on every degraded scan.
--   * Binary rollback to pre-#1188 is safe against a post-087 schema:
--     the column has DEFAULT 'complete' and old code never reads it.
--   * Schema rollback (DROP COLUMN inventory_status, DROP CONSTRAINT
--     scan_packages_scan_result_artifact_fk) requires every replica to
--     already be on a pre-#1188 binary. Do NOT roll the DB back while
--     any new binary is still serving traffic.
--
-- EXPECTED MIGRATION DURATION:
--   * This file (087): all operations are catalog-only / metadata-only
--     or build against empty partial-index predicates. Total runtime
--     is milliseconds even on a 30M-row scan_packages, so the
--     auto-wrapped transaction commits immediately.
--   * Migration 088 (`VALIDATE CONSTRAINT scan_packages_scan_result_
--     artifact_fk`): 3-8 minutes on a 30M-row table under SHARE UPDATE
--     EXCLUSIVE (DML proceeds; only other DDL queues). See 088 header.
--
-- =========================================================================
-- 1. Composite FK enforcing scan_packages.artifact_id matches the parent
--    scan_results.artifact_id (#1154).
-- =========================================================================
--
-- scan_packages already carries `scan_result_id` and `artifact_id` as
-- independent FKs. The denormalised artifact_id is kept for read-path
-- performance (SBOM-for-artifact never joins through scan_results), but
-- without a composite constraint a future write that rewrites
-- scan_results.artifact_id (artifact merge, dedup re-attribution) would
-- leave scan_packages silently drifted.
--
-- Approach:
--   a. Ensure (id, artifact_id) is a unique pair on scan_results so it can
--      be the target of a composite FK. id is already PK so this constraint
--      is structurally trivial; Postgres still requires the explicit
--      UNIQUE declaration before allowing the FK to reference both columns.
--   b. Add a composite FK on scan_packages(scan_result_id, artifact_id)
--      pointing at scan_results(id, artifact_id). ON DELETE CASCADE matches
--      the existing per-column FKs so behaviour on scan_result/artifact
--      deletion is unchanged.
--
-- The old per-column FKs (scan_packages.scan_result_id -> scan_results.id
-- and scan_packages.artifact_id -> artifacts.id) are kept. The first is
-- subsumed by the composite FK, but dropping it would change the
-- foreign-key dependency graph (some tools surface "FK to scan_results"
-- by name); leaving it costs nothing on writes. The second still
-- protects against orphan rows when an artifact is hard-deleted directly
-- without its scan_results.

-- Step 1a: prerequisite UNIQUE on scan_results(id, artifact_id).
-- Note: this is technically redundant because id is the PK and unique on
-- its own, but a composite FK needs a UNIQUE/PK on EXACTLY the referenced
-- column set, not just a subset.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'scan_results_id_artifact_id_key'
          AND conrelid = 'scan_results'::regclass
    ) THEN
        ALTER TABLE scan_results
            ADD CONSTRAINT scan_results_id_artifact_id_key
            UNIQUE (id, artifact_id);
    END IF;
END
$$;

-- Step 1b: composite FK on scan_packages.
-- Wrapped in DO so re-running the migration on a partially-applied DB is
-- a no-op rather than an error.
--
-- Two-phase add (NOT VALID here, VALIDATE in migration 088) to keep
-- deploys online on large scan_packages tables. Review #1188-R3 (DB +
-- adversarial) found that putting VALIDATE in the same file would
-- defeat the split: sqlx 0.8 wraps each migration in a single
-- transaction (no per-file opt-out without an undocumented directive),
-- and the SHARE ROW EXCLUSIVE lock that NOT VALID briefly acquires
-- would be held until commit -- i.e. through the entire VALIDATE scan
-- -- blocking DML on scan_packages for 3-8 minutes on a 30M-row table.
--
-- Splitting into two files means each ALTER runs in its own
-- transaction:
--   * 087 (this file): NOT VALID add. Catalog-only, commits in ms.
--   * 088: VALIDATE CONSTRAINT. Holds SHARE UPDATE EXCLUSIVE for the
--     scan duration; SUE does NOT block reads or writes.
-- Drifted rows still fail loudly at VALIDATE time (migration 088
-- errors and the deploy stops), preserving the original "fail loudly
-- on inconsistent data" property.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'scan_packages_scan_result_artifact_fk'
          AND conrelid = 'scan_packages'::regclass
    ) THEN
        ALTER TABLE scan_packages
            ADD CONSTRAINT scan_packages_scan_result_artifact_fk
            FOREIGN KEY (scan_result_id, artifact_id)
            REFERENCES scan_results (id, artifact_id)
            ON DELETE CASCADE
            NOT VALID;
    END IF;
END
$$;

-- =========================================================================
-- 2. inventory_status column on scan_results (#1157).
-- =========================================================================
--
-- scanner_service::scan_artifact_with_prepared currently logs warn! and
-- continues when create_packages fails: the scan row is marked status =
-- 'completed' even though the SBOM is now incomplete. Operators have no
-- programmatic surface to alert on this state.
--
-- Adds an inventory_status column with the same CHECK-constraint pattern
-- used by the existing `status` and `severity_threshold` columns in
-- migration 022 (TEXT + CHECK rather than a Postgres enum, so future
-- values can be added without an ALTER TYPE step and so the value can be
-- read straight out of the DB driver without enum decoding).
--
-- Values:
--   complete  - scan succeeded AND inventory persisted in full
--   partial   - scan succeeded but at least one inventory write failed;
--               SBOM consumers should treat the package list as truncated
--   failed    - scan itself failed (status = 'failed' on the same row);
--               kept distinct from 'partial' so dashboards can split
--               "scanner crashed" from "scanner ran but inventory broken"
--
-- Default is 'complete' because every existing row is either pre-#903
-- (no inventory was ever attempted, so the inventory state is irrelevant
-- to those rows) or post-#903 with a successful write (the warn! path
-- never executed because the existing INSERT is per-row, not batched).
-- Operators querying the new column will see 'complete' for legacy rows;
-- that is consistent with the read-path fallback to scan_findings, which
-- only applies when scan_packages is genuinely empty.
--
-- Requires Postgres >= 11 (verified against project README minimum). On
-- PG 11+ ADD COLUMN ... NOT NULL DEFAULT <constant> is metadata-only
-- (no table rewrite); on PG <= 10 it would rewrite scan_results under
-- ACCESS EXCLUSIVE which is unacceptable on production-sized tables.
ALTER TABLE scan_results
    ADD COLUMN IF NOT EXISTS inventory_status TEXT NOT NULL DEFAULT 'complete'
    CHECK (inventory_status IN ('complete', 'partial', 'failed'));

-- Index for the operator-dashboard "show me scans with degraded SBOMs"
-- query. Partial index over the non-default value: keeps the index tiny
-- (the common case is 'complete') while supporting WHERE inventory_status
-- = 'partial' and WHERE inventory_status = 'failed' scans.
--
-- Build is online-safe even without CONCURRENTLY: the ADD COLUMN above
-- sets every existing row to 'complete', so this partial index has zero
-- entries to copy at create time. The SHARE lock is held only for the
-- empty-scan duration (milliseconds even on the largest scan_results).
CREATE INDEX IF NOT EXISTS idx_scan_results_inventory_status
    ON scan_results (inventory_status)
    WHERE inventory_status <> 'complete';
