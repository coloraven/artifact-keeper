-- Seed the baseline `user` role that federated role mapping assumes exists.
--
-- Federated role mapping (OIDC/LDAP/SAML) unconditionally appends a `user`
-- role to every federated user's resolved role set (the federated mapping
-- builder in `auth_service.rs` pushes `"user"` after applying group->role
-- mappings). Role assignment in `apply_role_mapping` only persists roles that
-- exist in the `roles` table, so when `user` is absent it is silently dropped.
--
-- The user's persisted roles can then NEVER equal the resolved mapping (the
-- mapping always carries `user`, the persisted set never can), so the
-- per-login privilege re-sync computes `privilege_changed = true` on EVERY
-- login. That bumps `users.privileges_changed_at = NOW()` and calls
-- `invalidate_user_tokens(user_id)` on every login, which -- combined with the
-- in-memory watermark race (#1911) -- invalidates the access token the same
-- login just minted, producing a 401 on `/auth/me` immediately after callback.
--
-- The default role seed (002_roles.sql) creates `admin`, `developer`, and
-- `reader`, but not `user`. This migration adds the missing baseline role so
-- federated role mapping converges and stops perpetually flagging a privilege
-- change. Idempotent via ON CONFLICT so it is safe on databases where a `user`
-- role was created manually.
INSERT INTO roles (name, description, is_system)
VALUES ('user', 'Baseline role for all authenticated users', true)
ON CONFLICT (name) DO NOTHING;
