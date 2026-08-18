-- Durable ferry ingest job progress (air-gap zip unpack).
-- Progress was previously only mirrored into artifact_metadata.ferry_ingest;
-- this table survives process restarts and supports cursor/lease updates.

CREATE TABLE IF NOT EXISTS ferry_ingest_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    repository_id UUID NOT NULL REFERENCES repositories(id) ON DELETE CASCADE,
    ferry_artifact_id UUID NOT NULL UNIQUE REFERENCES artifacts(id) ON DELETE CASCADE,
    user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'queued'
        CHECK (status IN ('queued', 'running', 'completed', 'failed', 'partial')),
    ecosystem VARCHAR(64),
    done BIGINT NOT NULL DEFAULT 0,
    skipped BIGINT NOT NULL DEFAULT 0,
    failed BIGINT NOT NULL DEFAULT 0,
    cursor_module_index INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    lease_owner TEXT,
    lease_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_ferry_ingest_jobs_repo_status
    ON ferry_ingest_jobs (repository_id, status);

CREATE INDEX IF NOT EXISTS idx_ferry_ingest_jobs_lease_until
    ON ferry_ingest_jobs (lease_until)
    WHERE status IN ('queued', 'running');

COMMENT ON TABLE ferry_ingest_jobs IS
    'Server-side ferry zip ingest job state (progress, cursor, lease)';
COMMENT ON COLUMN ferry_ingest_jobs.ferry_artifact_id IS
    'Uploaded ak-ferry/*.zip artifact being ingested';
COMMENT ON COLUMN ferry_ingest_jobs.cursor_module_index IS
    'Next module index to process (0-based); updated as modules complete';
COMMENT ON COLUMN ferry_ingest_jobs.lease_owner IS
    'Worker identity holding the ingest lease';
COMMENT ON COLUMN ferry_ingest_jobs.lease_until IS
    'Lease expiry; stale running jobs may be reclaimed';
