-- Track the user who created a repository (#authz-private-repo-membership).
--
-- New repositories record their creator and auto-grant that user a per-repo
-- role assignment (owner auto-grant), so the creator retains access once
-- private repositories are restricted to admins and explicitly-granted users.
--
-- Nullable: existing rows predate this column and keep NULL. ON DELETE SET NULL
-- so removing a user does not block deleting the repositories they created.
ALTER TABLE repositories
    ADD COLUMN IF NOT EXISTS created_by UUID REFERENCES users(id) ON DELETE SET NULL;
